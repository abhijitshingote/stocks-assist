# Daily Screener

A daily shortlist screening agent that combines the existing momentum screens
(Top Returns / Vol Spike / Gappers) with per-ticker Perplexity news enrichment
and Claude-based judging on news materiality + thematic fit, derived from your
watchlist notes and curated themes.

**Mantra: STRONG MOMENTUM + STRONG NEWS + THEMATIC RELEVANCE.**

Every ticker in the daily universe is persisted with a stage + reason +
rationale, so you can QA exactly why something was — or wasn't — surfaced.

## Pipeline

```
[1] Universe  ─►  [2] Momentum  ─►  [3] Themes  ─►  [4] News (Perplexity)  ─►  [5] Judge (Claude)  ─►  [6] Audit
   (DB query)     (rule-based)      (Claude+Pplx)    (~30-60 calls)            (taste-calibrated)      (roll-up)
```

Every stage reads JSON in and writes JSON out under `outputs/<YYYY-MM-DD>/`,
so you can re-run any later stage without redoing earlier work.

## Layout

```
daily_screener/
  config.py              # all thresholds / model choices / weights
  pipeline.py            # orchestrator
  run.py                 # python -m daily_screener.run
  stages/
    s1_universe.py
    s2_momentum.py
    s3_themes.py
    s4_news.py
    s5_judge.py
    s6_audit.py
  prompts/               # plain markdown - edit and re-run
    market_themes.md
    user_theme_extraction.md
    ticker_news.md
    judge.md
  utils/
    db.py                # SQLAlchemy session, same env as db_scripts/
    perplexity.py
    claude.py
    io.py
  outputs/<YYYY-MM-DD>/
    00_universe.json
    01_momentum.json
    02_theme_vector.json
    02a_market_themes.json
    02b_user_themes.json
    03_news.json
    03_news/<ticker>.json
    04_judged.json
    05_audit.json          # SoT consumed by the UI
    run.log
    run_summary.json
```

## Run

Inside the backend container (recommended — gives you the right `DATABASE_URL`
and Python env):

```bash
# Full pipeline for today (US/Eastern)
docker-compose exec backend python -m daily_screener.run

# Cheap test: cap universe to 5 tickers
docker-compose exec backend python -m daily_screener.run --max-tickers 5 --verbose

# Re-run only the judge stage (after editing prompts/judge.md)
docker-compose exec backend python -m daily_screener.run --from-stage 5

# Run a specific date
docker-compose exec backend python -m daily_screener.run --date 2026-05-13

# Run a single stage end-to-end
docker-compose exec backend python -m daily_screener.stages.s2_momentum --verbose
```

CLI flags:

| Flag | Purpose |
|---|---|
| `--date YYYY-MM-DD` | Run a specific date (defaults to today in ET) |
| `--from-stage N` | Start at stage N (reuses earlier artifacts) |
| `--to-stage N` | Stop after stage N |
| `--max-tickers N` | Cap universe / LLM stages — handy for cheap testing |
| `--force-refresh-themes` | Ignore theme caches in stage 3 |
| `--verbose` | Debug logging |

## Stages

### 1. Universe (`s1_universe.py`)

Queries the DB directly (no HTTP) and pulls:

- Top 30 by `dr_1`, `dr_5`, `dr_20` from `stock_metrics`
- All `stock_volspike_gapper` rows with at least one spike or gap day

Applies the existing global liquidity filters (`avg_vol_10d`, `dollar_volume`,
`current_price`) and drops:

- Tickers in `user_data/abi_dislikes.json` (explicit thumbs-down list, drop
  reason: `user_dislike`). Watchlist `stars: 0` is NOT a veto — it just means
  "on the watchlist but not yet rated".
- Tickers in the excluded-industry list (default: Biotechnology)

Tags every ticker with the set of screens it appeared in (`top_1d`, `top_5d`,
`top_20d`, `volspike_gapper`).

### 2. Momentum (`s2_momentum.py`)

Deterministic 0-100 composite score with six factors. See
`MOMENTUM_FACTOR_WEIGHTS` in `config.py`:

- **multi_screen** (20pts): bonus for appearing in 2+ source screens
- **recent_event** (20pts): vol spike or gap within last N days
- **dr5_atr_norm** (20pts): ATR-normalized 5-day return strength
- **trend_alignment** (15pts): 20d and 60d returns both positive
- **vol_vs_avg** (10pts): elevated relative volume
- **rsi_mktcap** (15pts): RSI percentile within market-cap bucket

Tickers below `MOMENTUM_THRESHOLD` (default 50) are dropped with a reason
that's preserved through the audit. Top survivors are capped at
`MAX_LLM_GRADE` (default 60) to bound cost.

### 3. Themes (`s3_themes.py`)

Merges three sources into one weighted theme vector:

1. **Curated** themes from `user_data/themes.json` (your existing list)
2. **User-taste** themes derived by Claude from your watchlist notes — cached
   by watchlist content hash, only re-extracted when you've edited the list
3. **Hot market** themes from a single Perplexity call asking what's driving
   the market this week — cached daily

User-taste themes get the highest weight (you care most about your own taste);
hot-market themes are the lowest (signal but noisy).

### 4. News (`s4_news.py`)

For each Stage-2 survivor, one Perplexity `sonar` call with a structured
prompt asking for material catalysts in the last 30 days. Returns:

- `headline_drivers` — 1-3 short bullets
- `latest_material_date`
- `theme_tags` — model-suggested, constrained to the theme vector
- `summary` — 2-4 sentences

Runs concurrently (`NEWS_CONCURRENCY` workers, default 8). Per-ticker results
saved to `03_news/<ticker>.json` plus a consolidated `03_news.json`.

### 5. Judge (`s5_judge.py`)

For each ticker with news, the configured LLM (default Perplexity `sonar` —
see `JUDGE_MODEL` in `config.py`; set to a `claude-*` model to route through
Anthropic) receives:

- Momentum stats + per-factor breakdown
- News bullets + summary from stage 4
- The day's theme vector
- **The 2-3 most thematically-similar watchlist entries** — your stars and
  notes are the in-context calibration anchor

Returns JSON with:

- `news_materiality` in [0,3] + reason
- `theme_fit` in [0,3] + matched themes + reason
- `verdict` in {PICK, WATCH, SKIP} + 2-3 sentence rationale

Composite score = `momentum*0.4 + news/3*100*0.3 + theme/3*100*0.3`.

Final verdict is the more conservative of (composite-threshold verdict, LLM
verdict). Explicit dislikes never reach this stage; they are filtered in
Stage 1 from `user_data/abi_dislikes.json`.

### 6. Audit (`s6_audit.py`)

Rolls every ticker in the original universe into a single `05_audit.json`,
including:

- Survivors with their PICK/WATCH/SKIP verdict and rationale
- Dropped tickers with `dropped_at_stage` (1, 2, or 4) and `drop_reason`

This is the **SoT** the UI reads. Rejected tickers sort by composite
descending, so "almost made it" candidates are highest-priority for QA.

## QA Loop

1. Run today's pipeline.
2. Open `/daily-shortlist` in the web UI. Skim PICKs.
3. Open the **Rejected** tab — look at the top 10 by composite. If any should
   have been PICKs, the rationale tells you exactly why Claude said no.
4. Click **✎ Feedback** on any row to register a correction (e.g. "this should
   have been WATCH not PICK"). Saved to `user_data/daily_screener_feedback.json`.
5. Optionally edit `prompts/judge.md` (or `config.py` weights).
6. Re-run only stage 5: `python -m daily_screener.run --from-stage 5`. No
   Perplexity cost. The judge sees your corrections and recalibrates.
7. Repeat.

### Feedback loop ("fine-tuning")

The pipeline is calibrated in three layers, listed strongest → weakest:

1. **Veto layer** — anything in `user_data/abi_dislikes.json` is filtered in
   Stage 1 and never reaches the judge.
2. **In-context correction layer** — every entry in
   `user_data/daily_screener_feedback.json` (populated by the **✎ Feedback**
   button on `/daily-shortlist`) shows up in the judge prompt as a concrete
   "you said X, the trader said Y because Z" example. The judge is asked to
   generalize from these on the next run. Controlled by `FEEDBACK_LOOKBACK_DAYS`
   (default 30) and `FEEDBACK_MAX_EXAMPLES` (default 20) in `config.py`.
3. **Static layer** — `prompts/judge.md`, `config.py` weights, and curated
   themes (`user_data/themes.json`).

This is intentionally NOT a model-weights fine-tune — it's the cheap,
zero-latency version that takes effect on the very next run. If a correction
pattern is durable (e.g. "any RSI=100 momentum chase is a SKIP regardless
of news"), promote it into `prompts/judge.md` directly.

### Theme visualization

Open `/daily-themes` in the UI to see the day's theme vector laid out as
cards: weights, sources (user_taste / curated / hot_market), descriptions,
anti-patterns, and example tickers. Use the date picker to compare across
runs and confirm the theme extraction is doing what you expect.

## Cost

Per run, with default settings, ballpark:

- Stage 3: 1-2 Perplexity calls + 0-1 Claude calls (most days cache hits)
- Stage 4: ~30-60 Perplexity `sonar` calls (~$0.30-$1.00 total)
- Stage 5: ~30-60 Claude calls (Haiku, ~$0.10-$0.50 total)

Tune `MAX_LLM_GRADE` and `MOMENTUM_THRESHOLD` in `config.py` to control.
