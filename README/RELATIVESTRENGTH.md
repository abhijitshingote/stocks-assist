# Relative Strength Screener (Slow / Fast)

A screener page with a chart-heavy layout and a **Slow RS / Fast RS mode toggle** at the top of the right panel.

Relative strength is simply **stock return minus SPY return** over a given timeframe. The page exposes two distinct rankings that share the same underlying data:

- **Slow RS** — the IBD/MarketSmith **RS Rating** (1–99), computed in the backend from a fixed-weight trailing-12-month strength (`0.4×63d + 0.2×126d + 0.2×189d + 0.2×252d`). This is the clean, canonical ranking. No sliders; the list is sorted purely by `rs_rating` (unrated tickers last, tie-broken by RS vs SPX).
- **Fast RS** — a fast, user-tunable ranking. The list is sorted **purely** by a slider-weighted blend of the multi-timeframe RS percentile ranks (2D/5D/10D/20D/60D). The sliders fully drive the ordering in this mode.

The mode toggle only changes how the left-hand list is ranked and labeled; the chart, SPY overlay, RSI subchart, metrics strip, and sector/industry/market-cap filters are shared across both modes.

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

Frontend receives the raw RS values, the per-timeframe percentile ranks, and the IBD `rs_rating` per stock. Ranking depends on the active mode:

- **Slow RS:** sort by `rs_rating` descending (unrated tickers last, tie-broken by `rs_vs_spy`). The right cell of each row shows the RS Rating badge. Sliders are hidden.
- **Fast RS:** sort by the normalized slider-weighted composite of the percentile ranks:

```
composite = Σ (w_tf / Σw) * rs_tf_rank   for tf in {2d, 5d, 10d, 20d, 60d}
```

Sliders range 0–100 (defaults 10/25/50/50/50). The stock list re-sorts on every slider change without a server round-trip, and the right cell of each row shows the composite score.

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
- Scrollable list of tickers, sorted by RS Rating (Slow) or composite score (Fast), descending.
- Each row: ticker symbol + the ranking value for the active mode (RS Rating badge in Slow, composite score in Fast). Blue dot (RS line new high) and 52W tags shown in both modes.
- Clicking a ticker loads its chart in the main panel.
- Bucketed by market cap using the same tab system as other pages.

### Mode toggle (top of right panel)
- Slow RS / Fast RS buttons. Slow is the default.
- Switching to Fast reveals the slider bar and re-labels the list header to "RS Score".

### Sliders (Fast RS only — top of right panel)
- Five horizontal sliders, one per timeframe (2D, 5D, 10D, 20D, 60D).
- Range 0–100, defaults 10/25/50/50/50, with a Reset button.
- Changing any slider immediately re-computes composite scores and re-sorts the stock list.
- Hidden entirely in Slow RS mode.

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
