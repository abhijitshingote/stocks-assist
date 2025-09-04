import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from models import init_db, Price, Stock, StockRSI, get_eastern_datetime


HORIZONS = {
    '1week': 5,
    '1month': 21,
    '3month': 63,
}


def fetch_close_prices(session) -> pd.DataFrame:
    """Load all available close prices for tickers and benchmarks SPX/QQQ."""
    # Load all prices
    q = session.query(Price.ticker, Price.date, Price.close_price)
    rows = q.all()
    df = pd.DataFrame(rows, columns=['ticker', 'date', 'close'])
    if df.empty:
        return df
    df = df.dropna(subset=['close'])
    df['date'] = pd.to_datetime(df['date'])
    # Diagnostics
    try:
        uniq = sorted(df['ticker'].unique().tolist())
        print(f"price rows={len(df):,}, unique_tickers={len(uniq)} (sample: {uniq[:8]})")
    except Exception:
        pass
    return df


def compute_returns(df: pd.DataFrame, window: int) -> pd.Series:
    """Compute simple windowed returns per ticker: close/close.shift(window) - 1."""
    # Use transform to maintain the original index (avoid MultiIndex from apply)
    return df.groupby('ticker')['close'].transform(lambda s: s / s.shift(window) - 1.0)


def make_rsi_table(df_prices: pd.DataFrame) -> pd.DataFrame:
    """Compute RSI-like ratios vs SPX and QQQ for 1w/1m/3m on the latest date available,
    plus cross-sectional percentiles for each RSI column across all tickers."""
    if df_prices.empty:
        return pd.DataFrame()

    # Use the last common trading date across all tickers
    last_date = df_prices['date'].max()

    # Filter to last ~6 months back to ensure we have enough data for 3-month returns (63 days + buffer)
    min_date = last_date - pd.Timedelta(days=180)
    df = df_prices[df_prices['date'] >= min_date].copy()

    # Pivot to ensure we have SPX and QQQ available
    tickers = df['ticker'].unique().tolist()
    if 'SPX' not in tickers or 'QQQ' not in tickers:
        raise RuntimeError("Benchmarks 'SPX' and 'QQQ' must exist in price table.")

    # Sort data once for all calculations
    df_sorted = df.sort_values(['ticker', 'date'])
    
    # Compute returns for each horizon
    ret = {}
    for name, win in HORIZONS.items():
        r = compute_returns(df_sorted, win)
        df_sorted[f'ret_{name}'] = r
        ret[name] = df_sorted.copy()

    # Grab the latest row per ticker for each horizon
    latest = {}
    for name, d in ret.items():
        latest_data = d.groupby('ticker').tail(1).set_index('ticker')
        latest[name] = latest_data
        
        # Validate we have benchmark data
        if 'SPX' not in latest_data.index or 'QQQ' not in latest_data.index:
            raise RuntimeError(f"Missing benchmark data for horizon {name}")
            
        # Check for NaN values in benchmark returns
        spx_ret = latest_data.loc['SPX'][f'ret_{name}']
        qqq_ret = latest_data.loc['QQQ'][f'ret_{name}']
        if pd.isna(spx_ret) or pd.isna(qqq_ret):
            print(f"WARNING: NaN benchmark returns for {name}")

    # Benchmarks
    spx = {name: latest[name].loc['SPX'][f'ret_{name}'] for name in HORIZONS.keys()}
    qqq = {name: latest[name].loc['QQQ'][f'ret_{name}'] for name in HORIZONS.keys()}
    

    
    results = []

    # Build output for all stocks except the benchmarks themselves
    universe = sorted(t for t in df['ticker'].unique() if t not in {'SPX', 'QQQ'})
    print(f"RSI window={HORIZONS} last_date={last_date.date()} universe_size={len(universe)}")
    if not universe:
        print("No non-benchmark tickers found in price table. Seed prices for stocks first.")
    for t in universe:
        row = {'ticker': t}
        missing_data = False
        try:
            for name in HORIZONS.keys():
                if t not in latest[name].index:
                    missing_data = True
                    continue
                    
                val = latest[name].loc[t][f'ret_{name}']
                
                # Calculate RSI values with validation
                if pd.isna(val) or pd.isna(spx[name]):
                    row[f'rsi_spx_{name}'] = None
                else:
                    rsi_spx = round(float((val - spx[name]) * 100), 2)
                    row[f'rsi_spx_{name}'] = rsi_spx
                    # Debug extreme values
                    # if abs(rsi_spx) > 500:
                    #     print(f"EXTREME RSI_SPX {t} {name}: stock_ret={val:.1%}, spx_ret={spx[name]:.1%}, rsi={rsi_spx:.1f}")
                
                if pd.isna(val) or pd.isna(qqq[name]):
                    row[f'rsi_qqq_{name}'] = None
                else:
                    rsi_qqq = round(float((val - qqq[name]) * 100), 2)
                    row[f'rsi_qqq_{name}'] = rsi_qqq
                    # Debug extreme values
                    # if abs(rsi_qqq) > 500:
                    #     print(f"EXTREME RSI_QQQ {t} {name}: stock_ret={val:.1%}, qqq_ret={qqq[name]:.1%}, rsi={rsi_qqq:.1f}")
                        
        except KeyError as e:
            # Missing data for this ticker at some horizon; leave as None
            # print(f"Missing data for ticker {t}: {e}")
            missing_data = True
            
        # if missing_data:
            # print(f"WARNING: Incomplete data for {t}")
            
        row['last_updated'] = get_eastern_datetime()
        results.append(row)

    df_out = pd.DataFrame(results)


    # --- Correction RSI ---
    corr_start = find_last_correction_start(df_prices)
    df_out['rsi_spx_correction'] = None

    if corr_start:
        print(f"Correction period starts from: {corr_start.date()}")
        df_corr = df_prices[df_prices['date'] >= corr_start].copy()
        

        
        # Get first and last prices per ticker in correction window
        first = df_corr.groupby('ticker').first()['close']
        last = df_corr.groupby('ticker').last()['close']
        # SPX return
        if 'SPX' in first.index and 'SPX' in last.index:
            spx_ret = last['SPX'] / first['SPX'] - 1
            if spx_ret != 0:
                for t in df_out['ticker']:
                    if t in first.index and t in last.index:
                        stock_ret = last[t] / first[t] - 1
                        rsi_val = (stock_ret - spx_ret) * 100
                        df_out.loc[df_out['ticker'] == t, 'rsi_spx_correction'] = round(rsi_val, 2)
                        

    else:
        print("No correction period found (SPX drawdown < 4%)")



    # Percentiles per column, across all tickers (exclude NaNs)
    for col in [
        'rsi_spx_1week', 'rsi_spx_1month', 'rsi_spx_3month',
        'rsi_qqq_1week', 'rsi_qqq_1month', 'rsi_qqq_3month',
        'rsi_spx_correction',
    ]:
        if col in df_out.columns:
            pct_col = f"{col}_percentile"
            s = df_out[col]
            # rank method='average' gives midpoint for ties; multiply by 100 for percentiles
            denom = s.notna().sum()
            if denom > 0:
                df_out[pct_col] = round(s.rank(pct=True, method='average') * 100, 1)
                # print(f"Percentiles for {col}: min={df_out[pct_col].min():.1f}%, max={df_out[pct_col].max():.1f}%, count={denom}")
            else:
                df_out[pct_col] = None
                # print(f"No valid data for {col} percentiles")

    return df_out


def upsert_rsi(session, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    # Use plain SQL upsert for speed and simplicity
    # Ensure table exists
    engine = session.get_bind()
    with engine.begin() as conn:
        # Create a temp table-like insert via ON CONFLICT on ticker
        for _, r in df.iterrows():
            conn.execute(text(
                """
                INSERT INTO stock_rsi (
                    ticker,
                    rsi_spx_1week, rsi_spx_1month, rsi_spx_3month,
                    rsi_qqq_1week, rsi_qqq_1month, rsi_qqq_3month,
                    rsi_spx_1week_percentile, rsi_spx_1month_percentile, rsi_spx_3month_percentile,
                    rsi_qqq_1week_percentile, rsi_qqq_1month_percentile, rsi_qqq_3month_percentile,
                    rsi_spx_correction, rsi_spx_correction_percentile,
                    last_updated
                ) VALUES (
                    :ticker,
                    :rsi_spx_1week, :rsi_spx_1month, :rsi_spx_3month,
                    :rsi_qqq_1week, :rsi_qqq_1month, :rsi_qqq_3month,
                    :rsi_spx_1week_percentile, :rsi_spx_1month_percentile, :rsi_spx_3month_percentile,
                    :rsi_qqq_1week_percentile, :rsi_qqq_1month_percentile, :rsi_qqq_3month_percentile,
                    :rsi_spx_correction, :rsi_spx_correction_percentile,
                    :last_updated
                )
                ON CONFLICT (ticker) DO UPDATE SET
                    rsi_spx_1week = EXCLUDED.rsi_spx_1week,
                    rsi_spx_1month = EXCLUDED.rsi_spx_1month,
                    rsi_spx_3month = EXCLUDED.rsi_spx_3month,
                    rsi_qqq_1week = EXCLUDED.rsi_qqq_1week,
                    rsi_qqq_1month = EXCLUDED.rsi_qqq_1month,
                    rsi_qqq_3month = EXCLUDED.rsi_qqq_3month,
                    rsi_spx_1week_percentile = EXCLUDED.rsi_spx_1week_percentile,
                    rsi_spx_1month_percentile = EXCLUDED.rsi_spx_1month_percentile,
                    rsi_spx_3month_percentile = EXCLUDED.rsi_spx_3month_percentile,
                    rsi_qqq_1week_percentile = EXCLUDED.rsi_qqq_1week_percentile,
                    rsi_qqq_1month_percentile = EXCLUDED.rsi_qqq_1month_percentile,
                    rsi_qqq_3month_percentile = EXCLUDED.rsi_qqq_3month_percentile,
                    rsi_spx_correction = EXCLUDED.rsi_spx_correction,
                    rsi_spx_correction_percentile = EXCLUDED.rsi_spx_correction_percentile,
                    last_updated = EXCLUDED.last_updated
                """
            ), {
                'ticker': r['ticker'],
                'rsi_spx_1week': r.get('rsi_spx_1week'),
                'rsi_spx_1month': r.get('rsi_spx_1month'),
                'rsi_spx_3month': r.get('rsi_spx_3month'),
                'rsi_qqq_1week': r.get('rsi_qqq_1week'),
                'rsi_qqq_1month': r.get('rsi_qqq_1month'),
                'rsi_qqq_3month': r.get('rsi_qqq_3month'),
                'rsi_spx_1week_percentile': r.get('rsi_spx_1week_percentile'),
                'rsi_spx_1month_percentile': r.get('rsi_spx_1month_percentile'),
                'rsi_spx_3month_percentile': r.get('rsi_spx_3month_percentile'),
                'rsi_qqq_1week_percentile': r.get('rsi_qqq_1week_percentile'),
                'rsi_qqq_1month_percentile': r.get('rsi_qqq_1month_percentile'),
                'rsi_qqq_3month_percentile': r.get('rsi_qqq_3month_percentile'),
                'rsi_spx_correction': r.get('rsi_spx_correction'),
                'rsi_spx_correction_percentile': r.get('rsi_spx_correction_percentile'),
                'last_updated': r.get('last_updated'),
            })
    return len(df)

def find_last_correction_start(df, threshold=0.04):
    """Return the date of the last SPX peak before the most recent drawdown >= threshold."""
    df_spx = df[df['ticker'] == 'SPX'].sort_values('date')
    if df_spx.empty:
        return None
    
    peak_close = df_spx['close'].iloc[0]
    peak_date = df_spx['date'].iloc[0]
    corr_start = None
    
    for _, row in df_spx.iterrows():
        # Update peak if we find a higher close
        if row['close'] > peak_close:
            peak_close = row['close']
            peak_date = row['date']
        
        # Calculate drawdown from current peak
        drawdown = (peak_close - row['close']) / peak_close
        
        # If we hit the threshold, record this peak as a correction start
        if drawdown >= threshold:
            corr_start = peak_date
    
    if corr_start:
        print(f"Correction period detected starting from: {corr_start.date()}")
    else:
        print(f"No correction >= {threshold:.1%} found")
        
    return corr_start


def main():
    session = init_db()
    df_prices = fetch_close_prices(session)
    if df_prices.empty:
        print("No price data found. Aborting RSI update.")
        return
    df_rsi = make_rsi_table(df_prices)
    n = upsert_rsi(session, df_rsi)
    print(f"RSI updated for {n} tickers at {get_eastern_datetime().strftime('%Y-%m-%d %H:%M:%S %Z')}.")


if __name__ == '__main__':
    main()


