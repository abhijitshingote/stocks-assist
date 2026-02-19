import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import find_peaks
from sklearn.linear_model import TheilSenRegressor
from sqlalchemy import create_engine, text

# ---- Postgres connection
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "stocks_db")
DB_HOST = os.getenv("POSTGRES_HOST", "stocks-assist-db-1")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# ---- helpers
def atr(df, period=14):
    tr = pd.concat([df['high']-df['low'],
                    abs(df['high']-df['close'].shift()),
                    abs(df['low']-df['close'].shift())], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def get_swing_highs(df, distance=5, prominence_atr=0.5):
    return find_peaks(df['high'].values, distance=distance, prominence=prominence_atr*atr(df).bfill().values)[0]

def fit_resistance_line(df, swing_high_idx):
    x = swing_high_idx.reshape(-1,1)
    y = df['high'].iloc[swing_high_idx].values
    model = TheilSenRegressor(random_state=42)
    model.fit(x, y)
    return model.coef_[0], model.intercept_

def detect_break(df, lookback=120, breakout_lookback=5, atr_mult=0.25, min_slope=-0.001):
    if len(df) < lookback: return False
    df = df.tail(lookback).copy(); df['ATR'] = atr(df)
    idx = get_swing_highs(df)
    if len(idx) < 4: return False
    slope, intercept = fit_resistance_line(df, idx)
    if slope >= min_slope: return False
    x = np.arange(len(df)); res = slope*x + intercept
    rc = df['close'].iloc[-breakout_lookback:].values
    rr = res[-breakout_lookback:]; ra = df['ATR'].iloc[-breakout_lookback:].values
    breakout = rc > (rr + atr_mult*ra)
    rv = df['volume'].iloc[-5:].mean(); pv = df['volume'].iloc[-30:-5].mean()
    if rv < 1.2*pv: return False
    return breakout.any()

def get_stock_data(ticker, lookback=120):
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT date, open, high, low, close, volume FROM ohlc WHERE ticker=:t ORDER BY date ASC"),
            conn, params={"t": ticker}
        )
    return df.tail(lookback).copy()

def plot_trendline(ticker, df, folder="charts"):
    os.makedirs(folder, exist_ok=True)
    swing_highs = get_swing_highs(df)
    slope, intercept = fit_resistance_line(df, swing_highs)
    x = np.arange(len(df)); resistance = slope*x + intercept

    plt.figure(figsize=(12,6))
    plt.plot(df['date'], df['close'], label="Close Price", color='blue')
    plt.plot(df['date'], resistance, label="Resistance Trendline", color='red', linestyle='--')
    plt.scatter(df['date'].iloc[swing_highs], df['high'].iloc[swing_highs], color='orange', label="Swing Highs")
    plt.title(f"{ticker} Price with Downtrend Resistance")
    plt.xlabel("Date"); plt.ylabel("Price")
    plt.xticks(rotation=45); plt.legend(); plt.tight_layout()
    filename = os.path.join(folder, f"{ticker}_trendline.png")
    plt.savefig(filename); plt.close()
    print(f"{ticker} chart saved as {filename}")

def screen_stocks():
    candidates = []
    with engine.connect() as conn:
        tickers = [t[0] for t in conn.execute(
            text("""
            SELECT t.ticker
            FROM tickers t
            JOIN ohlc o ON o.ticker = t.ticker
            GROUP BY t.ticker, t.market_cap
            ORDER BY t.market_cap DESC NULLS LAST
            """)
        ).fetchall()]
        for t in tickers:
            df = get_stock_data(t)
            if df.empty: continue
            try:
                if detect_break(df):
                    candidates.append((t, df))
            except: pass
    return candidates

if __name__ == "__main__":
    results = screen_stocks()
    if not results:
        print("No breakout candidates found.")
    for ticker, df in results:
        print(ticker)
        plot_trendline(ticker, df)
