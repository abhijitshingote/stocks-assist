# API & routes

Source of truth: `backend/app.py`, `frontend/app.py`.

## Base URLs

| Service | Container port | Host (docker-compose) |
|---------|----------------|-------------------------|
| Backend | 5000 | `http://localhost:5001` |
| Frontend | 5000 | `http://localhost:5002` |

Frontend pages and `/api/frontend/*` call the backend via `BACKEND_URL` (default `http://localhost:5001`). Direct backend calls skip the BFF layer.

Health check: `GET http://localhost:5001/api/health`

## Conventions

### Market-cap suffix

Most screener routes use `{Bucket}`:

| Suffix | `market_cap` range (USD) |
|--------|--------------------------|
| `MicroCap` | 0 – 200M |
| `SmallCap` | 200M – 2B |
| `MidCap` | 2B – 20B |
| `LargeCap` | 20B – 100B |
| `MegaCap` | ≥ 100B |
| `All` | no cap filter |

Frontend proxies use lowercase slugs: `all`, `micro`, `small`, `mid`, `large`, `mega`.

### Global filters (most `stock_metrics` / `main_view` screeners)

From `backend/app.py`:

- `industry NOT IN {'Biotechnology'}`
- `avg_vol_10d >= 50_000`
- `dollar_volume >= 10_000_000`
- `current_price >= 3`

### Return threshold constants (Return\* routes)

`RETURN_THRESHOLDS = {1: 5, 5: 10, 20: 15, 60: 20, 120: 30}` (%). Used by `get_momentum_stocks` (see note under Return scanners).

---

## Backend (`/api/*`)

### System

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/health` | DB connectivity + ticker count |
| GET | `/api/stats` | Row counts per base table |
| GET | `/api/latest_date` | Latest OHLC date + `sync_metadata` last sync |

### Single ticker

| Method | Path | Source |
|--------|------|--------|
| GET | `/api/stock/<ticker>` | `stock_metrics` + volspike/gapper + float |
| GET | `/api/ohlc/<ticker>` | `ohlc` ⋈ `ticker_moving_averages` (~400d); includes dma_50/200, ema_10/20 |
| GET | `/api/earnings-eps/<ticker>` | `earnings` actual vs estimate (chart annotations) |
| GET | `/api/volspike-events/<ticker>` | Spike/gap event dates from `stock_volspike_gapper` |
| GET | `/api/earnings/<ticker>` | Full earnings history |
| GET | `/api/analyst-estimates/<ticker>` | `analyst_estimates` |
| GET | `/api/ratios-ttm/<ticker>` | `ratios_ttm` |
| GET | `/api/company-profile/<ticker>` | `company_profiles` |
| GET | `/api/stock-news/<ticker>` | FMP + Yahoo RSS + Seeking Alpha RSS (merged) |

### Market / index (no ticker)

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/sector-performance` | ETF/index `dr_1/5/20/60` from `index_prices` |
| GET | `/api/homepage` | Main indices, commodities, risk-on/off sectors + 50/200 DMA distance |
| GET | `/api/market-breadth` | 1Y `market_breadth` series |
| GET | `/api/index-ohlc/<symbol>` | `index_prices` OHLC (~800d) for charts |
| GET | `/api/vix-latest` | `^VIX` latest close + change |
| GET | `/api/treasury-10y` | Scraped US10Y yield (CNBC) |

### RS screener

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/rs-screener` | All caps; `rs_*_rank` from `rs_screener` |
| GET | `/api/rs-screener/<market_cap>` | `micro`…`mega` slug (not `MicroCap` suffix) |

### Screener families (× `All` \| `MicroCap` \| … \| `MegaCap`)

Pattern: `GET /api/{Family}-{Bucket}` unless noted.

| Family | Filter / sort (summary) |
|--------|-------------------------|
| **Return** `Return{1\|5\|20\|60\|120}D-*` | Intended: `dr_N >= RETURN_THRESHOLDS[N]`; calls `get_momentum_stocks` (**handler missing in `app.py` — will 500**). Not proxied by frontend. |
| **Gapper** `Gapper-*` | Calls `get_gapper_stocks` (**missing**). Not proxied. |
| **Volume** `Volume-*` | Calls `get_volume_spike_stocks` (**missing**). Not proxied. |
| **TopPerformance** `TopPerformance-*` | Union: top 30 by `dr_1`, `dr_5`, `dr_20` each (deduped) |
| **BottomPerformance** `BottomPerformance-*` | Same windows; ascending sort |
| **VolspikeGapper** `VolspikeGapper-*` | `spike_day_count > 0 OR gapper_day_count > 0`; order `last_event_date DESC`. Optional `?lookback_days=N` keeps `last_event_date >= max(OHLC.date) − N` calendar days. Adds `adjusted_event_return = last_event_return×100 / (clip(mcap,$200M,$100B)/$100B)^-0.134` |
| **VolspikeGapper-Setup** | Dict keyed by ticker: nearest of `ema_10/ema_20/dma_50/dma_200`, distance (% and ATRs), 10-bar range in ATRs, close position in range, above-MA flags. Used by `/volspike-gapper-monthly` ranking |
| **StrongStocks** `StrongStocks-*` | Liquid + `ti65 IS NOT NULL` (biotech included). `adjusted_ti65 = (ti65−1) / (0.120 × (clip(mcap,$500M,$100B)/$100B)^-0.151)`. Order `adjusted_ti65 DESC` |
| **StrongStocks-Setup** | Same setupParts fields as VolspikeGapper-Setup, ticker universe = liquid + `ti65` |
| **MainView** `MainView-*` | Full `main_view` row |
| **HighSalesGrowth** `HighSalesGrowth-*` | `main_view.tags LIKE '%high_sales_growth%'`; order `rev_growth_t_plus_1 DESC` |
| **TechnicalScreener-Reversal** `TechnicalScreener-Reversal-*` | Latest day `(close-low)/low*100` reversal %; liquidity filters |

**MainView query param**

| Method | Path | Query |
|--------|------|-------|
| GET | `/api/MainView-ByTickers` | `tickers=AAPL,MSFT` (comma-separated) |

### News (Benzinga / external)

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/benzinga-news/<ticker>` | Cached `benzinga_articles` |
| POST | `/api/benzinga-news/<ticker>` | Refresh from Polygon |
| GET/POST | `/api/benzinga-news/market` | Market-wide Benzinga |
| GET | `/api/market-news/fmp` | FMP general news |
| GET | `/api/market-news/seeking-alpha` | SA RSS |

### User data (`user_data/*.json`)

| Method | Path | File |
|--------|------|------|
| GET/POST | `/api/abi-general-notes` | `abi_general_notes.json` |
| GET/PUT/DELETE | `/api/abi-general-notes/<note_id>` | |
| GET | `/api/abi-general-notes/tags` | |
| GET | `/api/abi-ticker-notes` | `abi_ticker_notes.json` |
| GET/PUT/DELETE | `/api/abi-ticker-notes/<ticker>` | |
| POST | `/api/abi-ticker-notes/batch-check` | body: `{ "tickers": [...] }` |
| GET/POST | `/api/abi-watchlist` | `abi_watchlist.json` |
| PUT/DELETE | `/api/abi-watchlist/<ticker>` | |
| POST | `/api/abi-watchlist/batch-check` | |
| GET | `/api/abi-watchlist/data` | watchlist + `stock_metrics` join |
| GET/POST | `/api/abi-dislikes` | `abi_dislikes.json` |
| DELETE | `/api/abi-dislikes/<ticker>` | |
| POST | `/api/abi-dislikes/batch-check` | |

### Daily screener (artifacts under `user_data/daily_screener/<date>/`)

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/daily-shortlist/dates` | |
| GET | `/api/daily-shortlist/<date>` | `05_audit.json` + funnel + watchlist/dislike enrichment |
| POST | `/api/daily-shortlist/run` | Spawns `daily_screener.run`; body: `max_tickers`, `from_stage`, `force_refresh_themes`, `date` |
| GET | `/api/daily-shortlist/feedback/<date>` | |
| GET | `/api/daily-shortlist/feedback` | All dates; optional `?limit=` |
| PUT/DELETE | `/api/daily-shortlist/feedback/<date>/<ticker>` | |
| GET | `/api/daily-shortlist/themes/dates` | |
| GET | `/api/daily-shortlist/themes/<date>` | `02a_market_themes.json`, `02b_user_themes.json` |

### Market brief (`user_data/market_brief/<date>/`)

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/market-brief/dates` | |
| GET | `/api/market-brief/<date_str>` | |
| GET | `/api/market-brief/<date_str>/pdf` | |
| GET | `/api/market-brief/<date_str>/costs` | `run_costs.json` polling |
| POST | `/api/market-brief/generate` | Steps 3–4 on existing source |
| POST | `/api/market-brief/run` | Full pipeline subprocess |
| POST | `/api/auto-commit` | `auto_commit.sh` on `user_data/` |

---

## Frontend

### HTML pages

| Path | Template / purpose |
|------|-------------------|
| `/` | Home |
| `/main-view` | `main_view` screener |
| `/top-performance`, `/top-losers` | Top / bottom return unions |
| `/volspike-gapper` | Vol spike + gapper |
| `/volspike-gapper-90d` | VSG events, last 90 calendar days |
| `/strong-stocks` | Liquid universe ranked by mcap-adjusted TI65 |
| `/technical-screener` | Reversal criterion |
| `/high-sales-growth` | Tagged main_view rows |
| `/sector-performance` | Sector/index ETF returns |
| `/rs-screener` | Relative strength |
| `/stock/<ticker>` | Detail + charts |
| `/themes` | `user_data/themes.json` editor |
| `/context`, `/context-2` | Macro context |
| `/abi-general-notes` | (`/abi-notes` → 301) |
| `/abi-watchlist`, `/abi-dislikes` | |
| `/daily-shortlist`, `/daily-themes` | Pipeline output |
| `/market-news`, `/market-brief` | |
| `/logs` | Log viewer |

### BFF: `/api/frontend/*`

Proxies to backend unless noted. Market-cap path segments use `all|micro|small|mid|large|mega`.

| Frontend | Backend / storage |
|----------|-----------------|
| `GET /api/frontend/stock/<ticker>` | `/api/stock/<ticker>` |
| `GET /api/frontend/ohlc/<ticker>` | `/api/ohlc/<ticker>` |
| `GET /api/frontend/earnings-eps/<ticker>` | `/api/earnings-eps/<ticker>` |
| `GET /api/frontend/volspike-events/<ticker>` | `/api/volspike-events/<ticker>` |
| `GET /api/frontend/earnings/<ticker>` | `/api/earnings/<ticker>` |
| `GET /api/frontend/analyst-estimates/<ticker>` | `/api/analyst-estimates/<ticker>` |
| `GET /api/frontend/ratios-ttm/<ticker>` | `/api/ratios-ttm/<ticker>` |
| `GET /api/frontend/company-profile/<ticker>` | `/api/company-profile/<ticker>` |
| `GET /api/frontend/stock-news/<ticker>` | `/api/stock-news/<ticker>` |
| `GET/POST /api/frontend/benzinga-news/...` | `/api/benzinga-news/...` |
| `GET /api/frontend/market-news/fmp` | `/api/market-news/fmp` |
| `GET /api/frontend/market-news/seeking-alpha` | `/api/market-news/seeking-alpha` |
| `GET /api/frontend/latest-date` | `/api/latest_date` |
| `GET /api/frontend/sector-performance` | `/api/sector-performance` |
| `GET /api/frontend/homepage` | `/api/homepage` |
| `GET /api/frontend/market-breadth` | `/api/market-breadth` |
| `GET /api/frontend/index-ohlc/<symbol>` | `/api/index-ohlc/<symbol>` |
| `GET /api/frontend/vix-latest` | `/api/vix-latest` |
| `GET /api/frontend/treasury-10y` | `/api/treasury-10y` |
| `GET /api/frontend/top-performance/<market_cap>` | `/api/TopPerformance-{Bucket}` |
| `GET /api/frontend/top-losers/<market_cap>` | `/api/BottomPerformance-{Bucket}` |
| `GET /api/frontend/volspike-gapper/<market_cap>` | `/api/VolspikeGapper-{Bucket}` |
| `GET /api/frontend/volspike-gapper-90d/<market_cap>` | `/api/VolspikeGapper-{Bucket}?lookback_days=90` |
| `GET /api/frontend/volspike-gapper-setup` | `/api/VolspikeGapper-Setup` |
| `GET /api/frontend/strong-stocks/<market_cap>` | `/api/StrongStocks-{Bucket}` |
| `GET /api/frontend/strong-stocks-setup` | `/api/StrongStocks-Setup` |
| `GET /api/frontend/main-view/<market_cap>` | `/api/MainView-{Bucket}` |
| `GET /api/frontend/main-view/by-tickers?tickers=` | `/api/MainView-ByTickers` |
| `GET /api/frontend/high-sales-growth/<market_cap>` | `/api/HighSalesGrowth-{Bucket}` |
| `GET /api/frontend/technical-screener/<criterion>/<market_cap>` | `/api/TechnicalScreener-{Criterion}-{Bucket}` (`criterion`: `reversal`) |
| `GET /api/frontend/rs-screener/<market_cap>` | `/api/rs-screener/<market_cap>` |
| `GET/PUT /api/frontend/themes` | **`user_data/themes.json`** (frontend only) |
| `GET /api/frontend/theme-proposals` | `/api/theme-proposals` (**no backend route registered**) |
| `POST /api/frontend/theme-proposals/apply` | `/api/theme-proposals/apply` (**no backend route**) |
| `GET/POST/PUT/DELETE /api/frontend/abi-*` | matching `/api/abi-*` |
| `GET/POST /api/frontend/daily-shortlist/...` | matching `/api/daily-shortlist/...` |
| `GET /api/frontend/daily-shortlist/feedback-all` | `/api/daily-shortlist/feedback` |
| `GET/POST /api/frontend/market-brief/...` | matching `/api/market-brief/...` |
| `POST /api/frontend/auto-commit` | `/api/auto-commit` |

### Frontend-only

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/frontend/logs` | Lists `logs/*.log` |
| GET | `/api/frontend/logs/<filename>` | Optional `?lines=N`; filename must match `^[\w\-\.]+\.log$` |

---

## Examples

```bash
# Backend
curl -s http://localhost:5001/api/latest_date | jq .
curl -s 'http://localhost:5001/api/MainView-ByTickers?tickers=AAPL,NVDA' | jq .
curl -s http://localhost:5001/api/rs-screener/mid | jq '.[0:3]'

# Frontend BFF
curl -s http://localhost:5002/api/frontend/homepage | jq .
curl -s http://localhost:5002/api/frontend/main-view/all | jq 'length'

# Mutations need JSON body
curl -s -X PUT http://localhost:5001/api/abi-ticker-notes/AAPL \
  -H 'Content-Type: application/json' \
  -d '{"notes":"test"}'
```

Non-GET routes require `curl`, Postman, or the UI — not the browser address bar alone.
