# Relative Strength Screener

A new screener page with adjustable multi-timeframe relative strength scoring and a chart-heavy layout.

Relative strength is simply **stock return minus SPY return** over a given timeframe. The feature computes RS across multiple windows and lets the user tune weights via sliders so they can experiment with how the screened stock list changes.

## Architecture

Follows the same pattern as the rest of the app: computation lives in **db_scripts**, served through **backend API**, rendered by **frontend**.

### db_scripts (new script: `rs_screener_update.py`)

Compute per-stock relative returns vs SPY for each timeframe window:

| Window | Trading Days |
|--------|-------------|
| 2D     | 2           |
| 5D     | 5           |
| 10D    | 10          |
| 20D    | 20          |
| 60D    | 60          |

For each stock and date:
```
rs_2d  = stock_ret_2d  - spy_ret_2d
rs_5d  = stock_ret_5d  - spy_ret_5d
rs_10d = stock_ret_10d - spy_ret_10d
rs_20d = stock_ret_20d - spy_ret_20d
rs_60d = stock_ret_60d - spy_ret_60d
```

Returns are computed from `ohlc` (stocks) and `index_prices` where `symbol = 'SPY'`, same source tables as `historical_rsi_update.py`.

Store these in a new table (or extend `stock_metrics`) so the backend can serve the raw per-timeframe RS values. The backend will need the raw values (not a single composite score) because the weighting happens client-side via sliders.

Also store the **percentile rank** of each RS value within the stock's market cap bucket (same NTILE(100) approach used in `historical_rsi_update.py`) so the frontend can display ranks alongside raw values.

Market cap buckets are the same as everywhere else:

| Bucket | Range |
|--------|-------|
| micro  | < $200M |
| small  | $200M – $2B |
| mid    | $2B – $20B |
| large  | $20B – $100B |
| mega   | ≥ $100B |

### Backend API

New endpoint(s) to serve the screener data. Accepts a market cap filter (All, MicroCap, SmallCap, MidCap, LargeCap, MegaCap) consistent with existing RSI endpoints.

Returns per-stock:
- ticker, company_name, sector, industry, market_cap
- rs_2d, rs_5d, rs_10d, rs_20d, rs_60d (raw excess return)
- rs_2d_rank, rs_5d_rank, rs_10d_rank, rs_20d_rank, rs_60d_rank (percentile within mktcap bucket)

The **composite score** is NOT computed server-side. The frontend applies the user's slider weights and sorts client-side so the list updates instantly when sliders move.

### Frontend

Frontend receives the raw RS values and ranks per stock. It applies weights from the sliders to produce a composite score:

```
composite = w_2d * rs_2d_rank + w_5d * rs_5d_rank + w_10d * rs_10d_rank + w_20d * rs_20d_rank + w_60d * rs_60d_rank
```

Weights default to equal (each 20%) and are adjustable via sliders that range 0–100. The stock list re-sorts on every slider change without a server round-trip.

## UI Layout

The layout for this page is different from other screener pages.

```
┌──────────────────────────────────────────────────────────┐
│  Navbar                                                  │
├──────────┬───────────────────────────────────────────────┤
│          │  Sliders (2D / 5D / 10D / 20D / 60D weights) │
│          ├───────────────────────────────────────────────┤
│  Stock   │                                               │
│  List    │              Stock Chart                      │
│  (15-20% │              (OHLC + SPY overlay)             │
│   width) │                                               │
│          │                                               │
│          ├───────────────────────────────────────────────┤
│          │  Stock info row (ticker, name, RS values)     │
├──────────┴───────────────────────────────────────────────┤
│  Market Cap Tabs (All | Micro | Small | Mid | Large | Mega)│
└──────────────────────────────────────────────────────────┘
```

### Stock List (left panel, ~15-20% width)
- Scrollable list of tickers sorted by composite score (descending).
- Each row: ticker symbol, composite score.
- Clicking a ticker loads its chart in the main panel.
- Bucketed by market cap using the same tab system as other pages.

### Sliders (top of right panel)
- Five horizontal sliders, one per timeframe (2D, 5D, 10D, 20D, 60D).
- Range 0–100, default all equal.
- Changing any slider immediately re-computes composite scores and re-sorts the stock list.

### Chart (main area, ~80-85% width)
- OHLC candlestick chart for the selected stock.
- **SPY overlay** so the user can visualize relative performance.
- Chart styling should match the existing stock chart in the main view (`stock-chart.js`), with the new addition of the SPY line.
- RSI MktCap subchart included below the candles, same as existing.

### Stock Info Row
- Below the chart, show the selected stock's RS values across all timeframes and its percentile ranks.

## Data Flow

```
ohlc + index_prices (SPY)
    → rs_screener_update.py  → rs_screener table (raw RS + percentile ranks)
    → backend API             → /api/rs-screener/<market_cap>
    → frontend                → sliders + sorted stock list + chart w/ SPY overlay
```

## Existing RS vs This Feature

The existing RSI system (`historical_rsi_update.py`) uses a single fixed-weight composite:
```
0.4 * (ret_4 - spy_ret_4) + 0.4 * (ret_13 - spy_ret_13) + 0.2 * (ret_20 - spy_ret_20)
```

This feature differs in three ways:
1. **Different timeframe windows** — 2D, 5D, 10D, 20D, 60D instead of 4D, 13D, 20D.
2. **No fixed weighting** — weights are user-adjustable via sliders, applied client-side.
3. **Chart-focused layout** — the main display area is the chart with SPY overlay rather than a data table.
