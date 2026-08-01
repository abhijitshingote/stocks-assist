# How the Market Brief Works

Technical reference for the **current** pipeline (`market_brief.run_pipeline`): Benzinga ingest → one Anthropic Opus call → `02_brief.md`.

Legacy paths (`market_brief.pipeline`, Perplexity summarize, `01_summaries/`) exist but are **not** described here.

---

## Run

```bash
docker compose exec backend python -m market_brief.run_pipeline
docker compose exec backend python -m market_brief.run_pipeline --date 2026-06-03
```

**Output root:** `user_data/market_brief/<YYYY-MM-DD>/`

| Artifact | Role |
|----------|------|
| `source/` | Raw ingest snapshots (feeds Step 4) |
| `ingest_stats.json` | Fetch/dedupe counts |
| `02_brief.md` | Final brief |
| `run_costs.json` | Anthropic token/cost log |
| `status.json` | Run stage / completion |

**Flags:** `--skip-ingest` (reuse `source/`), `--skip-llm-summary` (ingest only), `--resume` (skip ingest, rerun Step 4 if brief missing), `--dry-run-step4` (prompt section counts, no API).

---

## Pipeline (2 stages)

```
INGEST (Polygon Benzinga)          SYNTHESIS (Anthropic Opus)
         │                                    │
         ▼                                    ▼
    source/                           02_brief.md
 general / channel / ticker
 ticker_universe/overview.md
```

Postgres `benzinga_articles` is a **7-day cache** for upserts during ingest. It does **not** feed the brief in this pipeline.

---

## Stage 1 — Ingest

Entry: `ingest.ingest_all` → `ingest.persist_source_snapshots`. Clears and rewrites `source/` each full run.

### Time windows (`ingest_window.py`, `trading_calendar.py`)

| Pull type | Window |
|-----------|--------|
| **General + channels** | Anchor session **5:00 AM ET** → run time (NYSE calendar rules for which session) |
| **Per-ticker** | Same end; start **24h earlier** (`TICKER_NEWS_EXTRA_HOURS`) |

Articles kept only if `published` falls in `[start, end]` (UTC). Rows without `published` are dropped.

### API pulls (`backend/benzinga_news.py`)

All use `GET /benzinga/v2/news`. Caps are **max rows returned**, not guaranteed counts after the time filter.

| Bucket | API params | Limit | On disk |
|--------|------------|-------|---------|
| **general** | No `tickers`, no `channels` | 100 | `source/general/articles.json` |
| **channel** | `channels=<slug>` | 50 each | `source/channel/<slug>/articles.json` |
| **ticker** | `tickers=<SYM>` | 25 each | `source/ticker/<SYM>/articles.json` |

**`general` is a pipeline label**, not a Benzinga channel tag. It means “unfiltered market-wide pull.” Individual articles may still carry Benzinga `channels: ["news", "earnings", …]` in JSON.

**Active channel slugs** (`config.GENERAL_CHANNEL_FETCHES`): `news`, `markets`, `equities`, `tech`, `commodities`, `earnings`, `movers`, `macro economic events` (9 × 50).

**Concurrency:** 5 parallel ticker fetches (`INGEST_CONCURRENCY`).

### Ticker universe (who gets per-ticker pulls)

From DB screener (`screener_universe.py`), **not** `themes.json`.

- **3 cap buckets:** `mega` (≥$100B), `large` ($20B–$100B), `mid_small` ($200M–$20B; micro excluded)
- **3 screens × 3 caps = 9 slices**, **top 10** each (`TICKER_UNIVERSE_TOP_N`)
- **Dedupe:** each symbol in **one** slice; priority `r1d` → `vol_spike_5d` (5d events) → `main_view_ti65`
- **Liquidity:** avg vol 10d ≥ 50k, dollar vol ≥ $10M, price ≥ $3; exclude industry `Biotechnology`

Human-readable tables: `source/ticker_universe/overview.md` + `lineage.json`.

Typical outcome: ~60–70 unique symbols after dedupe (≤90 raw slots).

---

## Stage 2 — Synthesis (Step 4)

Entry: `run_pipeline.run_step4` → `source_loader.build_step4_summaries_text` → `prompts_pipeline.step4_user_message` → `anthropic_client.complete` (Opus, `max_tokens=32_768`).

**Input is only `source/` + overview markdown** — no `01_summaries/`, no theme JSON, no DB reload corpus.

### Opus user message structure

```xml
<date>YYYY-MM-DD</date>

<ticker_universe>
  … full overview.md …
</ticker_universe>

<channel_summaries>
  … article blocks …
</channel_summaries>

<ticker_summaries>
  … article blocks …
</ticker_summaries>
```

### `<ticker_universe>` — screener table, not news

Markdown from `overview.md`: tickers, 1D/5D %, TI65, vol, cap, which screen (`r1d/mega`, etc.). Tells the model **who moved and why they are in the universe**. No Benzinga headlines or bodies.

### `<channel_summaries>` and `<ticker_summaries>` — raw articles

Each article becomes one block:

```text
[TICKER: NVDA, AMD] [news] Title
<body>
---
```

The bracket tag `[news]` / `[general]` is the **ingest bucket** (folder source), not necessarily the article’s Benzinga `channels` array (ticker-only blocks may use metadata channels).

### Global dedupe (`benzinga_id`)

Single `seen` set. **First processed bucket wins** (one copy of each story in the prompt).

**Order:**

1. **`general`** → `## Channel: general`
2. **Each `source/channel/<slug>/`** (slug order) → `## Channel: <slug>`
3. **`source/ticker/<SYM>/`** grouped into `## Ticker Group: …` batches parsed from `overview.md` (`r1d`, `vol_spike_5d`, TI65 mega/large vs mid/small, `other_tickers`)

Ticker sections only include articles **not** already placed in general or channel sections (“leftovers” from the longer per-ticker window).

**Design note:** General runs first for simple dedupe (broad pull overlaps channel/ticker). That means a story can appear as `[general]` even when it also exists under `earnings` or `ticker/NVDA/` — channel/ticker copies are dropped. Editorial “best bucket” is not used today.

### Coverage check

`audit_step4_source`: `unique_benzinga_ids` in all `source/` files should equal channel blocks + ticker blocks after dedupe (`coverage_ok`).

---

## What does *not* reach the brief

| Mechanism | Purpose |
|-----------|---------|
| Postgres reload (`load_articles_since`, limit 10k) | Funnel/QA stats, legacy `pipeline.py` topic snapshots — **not** `run_pipeline` |
| `themes.json` assignment | Counted in ingest funnel only; no `source/` folders for themes in current path |
| `01_summaries/` | Legacy Sonnet fact-extract path (not used by `run_pipeline`) |

---

## Key config (`market_brief/config.py`)

| Knob | Value |
|------|-------|
| `GENERAL_NEWS_LIMIT` | 100 |
| Channel fetch limit | 50 each (9 channels) |
| `PER_TICKER_LIMIT` | 25 |
| `TICKER_UNIVERSE_TOP_N` | 10 per slice |
| `TICKER_NEWS_EXTRA_HOURS` | 24 |
| `ARTICLE_RETENTION_DAYS` | 7 (DB purge) |
| `INGEST_CONCURRENCY` | 5 |

---

## Code map

| Concern | Module |
|---------|--------|
| CLI / orchestration | `run_pipeline.py` |
| Benzinga fetch + `source/` write | `ingest.py`, `benzinga_news.py` |
| Screener universe | `screener_universe.py` |
| Prompt assembly + dedupe | `source_loader.py` |
| Opus system/user prompts | `prompts_pipeline.py` |
| Time windows | `ingest_window.py`, `trading_calendar.py` |

---

## Mental model

1. **Ingest** fills `source/` with capped API pulls (general = unfiltered feed; channels = filtered; tickers = screener list, longer window).
2. **Overview** tells Opus which names matter and how they screened.
3. **Step 4** concatenates deduped article bodies into channel + ticker sections; Opus writes the structured brief from that single context.

**Upper bound on raw rows fetched** (before time filter and dedupe): 100 + 9×50 + N×25, where N ≈ unique screener symbols (~65). **Prompt article count** ≤ unique `benzinga_id`s after global dedupe, usually much lower.
