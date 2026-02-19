import pandas as pd
import psycopg2
import mplfinance as mpf
import numpy as np
import matplotlib.pyplot as plt

# ----------------------
# Parameters
# ----------------------
TICKER = 'MSFT'
SWING_LOOKBACK = 10  # bars on each side to check for swing high/low (higher = fewer, more significant swings)
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'stocks_db',
    'user': 'postgres',       # replace with your DB username
    'password': 'postgres'    # replace with your DB password
}

# ----------------------
# Connect and load OHLC data
# ----------------------
conn = psycopg2.connect(**DB_PARAMS)

query = f"""
    SELECT date, open, high, low, close, volume
    FROM ohlc
    WHERE ticker = '{TICKER}'
    ORDER BY date
"""
df = pd.read_sql(query, conn)
conn.close()

df = df.sort_values("date").reset_index(drop=True)
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

# ----------------------
# Detect swing highs and lows using rolling window
# ----------------------
df['swing_high'] = False
df['swing_low'] = False

n = SWING_LOOKBACK
for i in range(n, len(df) - n):
    window_highs = df['high'].iloc[i-n:i+n+1]
    if df['high'].iloc[i] == window_highs.max():
        df.loc[df.index[i], 'swing_high'] = True
    
    window_lows = df['low'].iloc[i-n:i+n+1]
    if df['low'].iloc[i] == window_lows.min():
        df.loc[df.index[i], 'swing_low'] = True

# ----------------------
# Detect trend reversals
# Uptrend: Higher Highs (HH) + Higher Lows (HL)
# Downtrend: Lower Highs (LH) + Lower Lows (LL)
# Reversal: when the pattern breaks
# ----------------------

# Get swing points in chronological order with their type and price
swing_points = []
for idx in df.index:
    if df.loc[idx, 'swing_high']:
        swing_points.append({'date': idx, 'type': 'high', 'price': df.loc[idx, 'high']})
    if df.loc[idx, 'swing_low']:
        swing_points.append({'date': idx, 'type': 'low', 'price': df.loc[idx, 'low']})

swing_points.sort(key=lambda x: x['date'])

# Track pattern: compare each swing to previous swing of same type
reversals = []
prev_high = None
prev_low = None
trend = None  # 'up', 'down', or None

for i, point in enumerate(swing_points):
    if point['type'] == 'high':
        if prev_high is not None:
            if point['price'] > prev_high['price']:
                # Higher High
                if trend == 'down':
                    # Was in downtrend, now making HH = potential bullish reversal
                    reversals.append({
                        'date': point['date'],
                        'price': point['price'],
                        'reversal_type': 'BULLISH',
                        'label': 'HH (Bullish)',
                        'point_type': 'high'
                    })
                trend = 'up'
            else:
                # Lower High
                if trend == 'up':
                    # Was in uptrend, now making LH = potential bearish reversal
                    reversals.append({
                        'date': point['date'],
                        'price': point['price'],
                        'reversal_type': 'BEARISH',
                        'label': 'LH (Bearish)',
                        'point_type': 'high'
                    })
                trend = 'down'
        prev_high = point
    
    elif point['type'] == 'low':
        if prev_low is not None:
            if point['price'] > prev_low['price']:
                # Higher Low
                if trend == 'down':
                    # Was in downtrend, now making HL = potential bullish reversal
                    reversals.append({
                        'date': point['date'],
                        'price': point['price'],
                        'reversal_type': 'BULLISH',
                        'label': 'HL (Bullish)',
                        'point_type': 'low'
                    })
                trend = 'up'
            else:
                # Lower Low
                if trend == 'up':
                    # Was in uptrend, now making LL = potential bearish reversal
                    reversals.append({
                        'date': point['date'],
                        'price': point['price'],
                        'reversal_type': 'BEARISH',
                        'label': 'LL (Bearish)',
                        'point_type': 'low'
                    })
                trend = 'down'
        prev_low = point

print(f"Found {len(reversals)} potential reversals:")
for r in reversals:
    print(f"  {r['date'].strftime('%Y-%m-%d')}: {r['label']} at ${r['price']:.2f}")

# ----------------------
# Plot candlestick with annotations
# ----------------------
apds = []

# Swing high markers (small, subtle)
swing_high_markers = pd.Series(np.nan, index=df.index)
swing_high_markers[df['swing_high']] = df.loc[df['swing_high'], 'high']
if swing_high_markers.notna().any():
    apds.append(mpf.make_addplot(swing_high_markers, type='scatter', markersize=50, marker='^', color='gray', alpha=0.5))

# Swing low markers (small, subtle)
swing_low_markers = pd.Series(np.nan, index=df.index)
swing_low_markers[df['swing_low']] = df.loc[df['swing_low'], 'low']
if swing_low_markers.notna().any():
    apds.append(mpf.make_addplot(swing_low_markers, type='scatter', markersize=50, marker='v', color='gray', alpha=0.5))

# Bullish reversal markers (prominent)
bullish_markers = pd.Series(np.nan, index=df.index)
for r in reversals:
    if r['reversal_type'] == 'BULLISH':
        bullish_markers[r['date']] = r['price']
if bullish_markers.notna().any():
    apds.append(mpf.make_addplot(bullish_markers, type='scatter', markersize=200, marker='*', color='lime'))

# Bearish reversal markers (prominent)
bearish_markers = pd.Series(np.nan, index=df.index)
for r in reversals:
    if r['reversal_type'] == 'BEARISH':
        bearish_markers[r['date']] = r['price']
if bearish_markers.notna().any():
    apds.append(mpf.make_addplot(bearish_markers, type='scatter', markersize=200, marker='*', color='red'))

# Plot with returnfig to add text annotations
fig, axes = mpf.plot(
    df,
    type='candle',
    style='yahoo',
    title=f"{TICKER} - Swing Highs/Lows with Trend Reversals",
    volume=True,
    addplot=apds if apds else None,
    figsize=(16, 10),
    returnfig=True
)

# Add text labels for reversals
ax = axes[0]
for r in reversals:
    x_pos = df.index.get_loc(r['date'])
    y_offset = df['high'].max() * 0.02 if r['point_type'] == 'high' else -df['high'].max() * 0.02
    va = 'bottom' if r['point_type'] == 'high' else 'top'
    color = 'green' if r['reversal_type'] == 'BULLISH' else 'red'
    ax.annotate(
        r['label'],
        xy=(x_pos, r['price']),
        xytext=(x_pos, r['price'] + y_offset),
        fontsize=8,
        fontweight='bold',
        color=color,
        ha='center',
        va=va
    )

fig.savefig(f'{TICKER}_swing_hl.png', dpi=150, bbox_inches='tight')
print(f"Chart saved to {TICKER}_swing_hl.png")
