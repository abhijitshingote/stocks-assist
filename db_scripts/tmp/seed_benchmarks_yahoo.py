import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from sqlalchemy import text

from models import init_db, Price, Stock, get_eastern_datetime


BENCHMARKS = {
    'SPX': '^GSPC',  # S&P 500 index (Yahoo)
    'QQQ': 'QQQ',    # Nasdaq 100 ETF (Yahoo)
}


def _dl(symbol: str) -> pd.DataFrame:
    """Download last ~1y of daily data from yfinance and normalize columns."""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period='1y', interval='1d', auto_adjust=False)
    except Exception as e:
        print(f"yfinance error for {symbol}: {e}")
        return pd.DataFrame()

    # Minimal inspection to aid debugging without noise
    print(f"yfinance raw [{symbol}] shape={df.shape} columns={list(df.columns)}")

    if df is None or df.empty:
        return pd.DataFrame()

    # yfinance returns DatetimeIndex in UTC; convert to date only
    df = df.rename(columns={'Open':'open','High':'high','Low':'low','Close':'close','Volume':'volume'})
    # Some builds include 'Adj Close' – we ignore for database schema
    df = df[['open','high','low','close','volume']]
    df = df.reset_index()
    # Ensure there is a 'Date' column after reset_index (could be 'Date' or 'Datetime')
    date_col = 'Date' if 'Date' in df.columns else ('Datetime' if 'Datetime' in df.columns else df.columns[0])
    df.rename(columns={date_col: 'date'}, inplace=True)
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df[['date','open','high','low','close','volume']]


def download_history(ticker_symbol: str) -> pd.DataFrame:
    df = _dl(ticker_symbol)
    return df


def upsert_stock(session, code: str, display_name: str):
    # Minimal fields for index proxy in ticker table
    existing = session.query(Stock).filter(Stock.ticker == code).first()
    if not existing:
        s = Stock(ticker=code, company_name=display_name, is_etf=False, is_actively_trading=True)
        session.add(s)
        session.commit()


def upsert_prices(session, code: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    engine = session.get_bind()
    # Overwrite semantics: delete existing rows for this ticker in the date window, then insert fresh
    min_date = df['date'].min()
    max_date = df['date'].max()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM price WHERE ticker = :t AND date BETWEEN :d1 AND :d2"), {
            't': code,
            'd1': min_date,
            'd2': max_date,
        })
        for _, r in df.iterrows():
            conn.execute(text(
                """
                INSERT INTO price (ticker, date, open_price, high_price, low_price, close_price, volume, last_traded_timestamp)
                VALUES (:ticker, :date, :open, :high, :low, :close, :volume, :ts)
                ON CONFLICT (ticker, date) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    last_traded_timestamp = EXCLUDED.last_traded_timestamp
                """
            ), {
                'ticker': code,
                'date': r['date'],
                'open': float(r['open']) if pd.notna(r['open']) else None,
                'high': float(r['high']) if pd.notna(r['high']) else None,
                'low': float(r['low']) if pd.notna(r['low']) else None,
                'close': float(r['close']) if pd.notna(r['close']) else None,
                'volume': float(r['volume']) if pd.notna(r['volume']) else None,
                'ts': get_eastern_datetime(),
            })
    return len(df)


def main():
    session = init_db()
    total_rows = 0
    for code, source_symbol in BENCHMARKS.items():
        print(f"Downloading {code} ({source_symbol}) via yfinance...")
        upsert_stock(session, code, f"Index: {code}")
        hist = download_history(source_symbol)
        n = upsert_prices(session, code, hist)
        total_rows += n
        print(f"{code}: {n} rows upserted")
    print(f"Done. Upserted {total_rows} total price rows.")


if __name__ == '__main__':
    main()


