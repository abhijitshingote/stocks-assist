# Market Brief

Daily pre-market intelligence: **Benzinga news** (Polygon API) → **Perplexity summaries** per topic → **one edited brief** (`02_brief.md`) plus a calendar **watch** probe.

Not the same as `daily_screener` (that pipeline picks tickers to trade; this one builds morning context).

---

## End-to-end funnel

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. TOPICS          config.SECTORS (5) + user_data/themes.json (themes)   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. INGEST          Polygon Benzinga: general + channels + every ticker │
│                    in the fetch universe → dedupe → Postgres → corpus   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. ROUTE (files)   Split corpus into per-topic JSON under 00_news/      │
│                    + _unassigned.json for stories with no theme/sector   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. SUMMARIZE       Perplexity reads article bodies (no web) per bucket  │
│                    + optional "Unassigned" bucket + verified tape       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 5. WATCH           One Perplexity web probe: calendar next 24–48h       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 6. SYNTHESIZE      Perplexity merges all summary texts → 02_brief.md    │
│                    Second pass → 02_brief.json                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Important:** The final brief never sees raw articles. It only sees **markdown summaries** produced in step 4 (and the watch probe). If a story never gets summarized, it cannot appear in `02_brief.md`.

---

## Topics and tickers (three different uses)

Every run builds a list of **topics** (`topics.load_topics()`):

| Source | `kind` | Tickers on the topic row | Used for |
|--------|--------|---------------------------|----------|
| `config.SECTORS` | `sector` | Empty | Name + description in prompts; **ticker basket** from `tape.SECTOR_TICKERS[name]` |
| `user_data/themes.json` | `theme` | Your list in JSON | Same; basket is **only** those tickers |

The same symbols show up in three separate places:

1. **Fetch universe** — union of all theme tickers + all `SECTOR_TICKERS` baskets → one Polygon call per symbol (~100+ calls).
2. **Assignment** — which `00_news/<topic>.json` file an article lands in (see below).
3. **Tape** — prior-session % moves from local OHLC injected into summarize prompts (`tape.py`).

Sector names in `config.SECTORS` must **exactly match** keys in `tape.SECTOR_TICKERS` (e.g. `"Semiconductors"`). Theme names like `"AI Compute"` do **not** use a sector basket unless you add a matching key in `tape.py`.

---

## Step 2 — Ingest (what gets pulled)

**Window:** from **5:00 AM America/New_York** on the anchor NYSE session through run time (ET). Rules: weekday before 9:30 AM → prior session; Monday before 9:30 → prior Friday; weekend → last Friday; weekday after 9:30 → current session (holidays roll back). See `trading_calendar.py`.

**API pulls:**

| Pull | What |
|------|------|
| General | No filter, up to `GENERAL_NEWS_LIMIT` (100) |
| Channels | Each entry in `GENERAL_CHANNEL_FETCHES` (e.g. `news`, `markets`, `tech`) |
| Per ticker | Every symbol in the fetch universe, up to `PER_TICKER_LIMIT` (25) each |

**Merge rules (articles only, not tickers):**

- Append all rows from every pull into one list.
- `filter_published_window` — keep articles with `published` in [window start, window end] (UTC).
- `_dedupe_by_id` — keep one row per `benzinga_id`; drop rows with no id.
- Upsert into Postgres `benzinga_articles`; reload the window into the **corpus** used for the rest of the run.

**Tickers are never removed from the fetch list.** Duplicate **stories** are removed by `benzinga_id`. A symbol can return 0 articles (Benzinga had nothing in the window) — the call still happened.

Rediscover channel slugs:

```bash
docker compose exec backend python -m market_brief.discover_channels
```

---

## Step 3 — Route articles to `00_news/`

Routing uses **Benzinga’s `tickers` field on each article**, normalized (`X:NVDA` → `NVDA`). It does **not** use “which API call found this story.”

| File | Rule |
|------|------|
| `<sector_or_theme_slug>.json` | Article tickers overlap that topic’s ticker set |
| `_unassigned.json` | Article’s `benzinga_id` matched **no** theme and **no** sector file |
| `_macro.json` | Same payload as `_unassigned.json` (legacy filename) |

An article can appear in **multiple** topic files if its tags overlap several sets.

**Unassigned guarantee:** If a story is in the corpus but matched zero theme/sector buckets, it goes to `_unassigned.json` and, in step 4, gets its own summarize pass (`config.UNASSIGNED_TOPIC_NAME`). That summary **is** fed to synthesis so those stories are not silently dropped before the brief.

---

## Step 4 — Summarize

For each topic (sectors, themes, then unassigned if any):

1. Load matched articles from the corpus (not re-fetch).
2. Attach **verified tape** from `tape.py` (sector/theme tickers only; not for unassigned).
3. Chunk large bundles (`CHUNK_MAX_CHARS`), call Perplexity **`sonar-pro`** with `prompts.build_benzinga_chunk_prompt` (no web search).
4. Write `01_summaries/<slug>__benzinga.md`.

Empty topics still get a short stub (“no articles in window”) which is passed to synthesis.

---

## Step 5 — Watch

One Perplexity **web** call over the full fetch universe: earnings, conferences, etc. in the next 24–48h (`prompts.build_watch_tomorrow_prompt`).

---

## Step 6 — Synthesize

1. Concatenate all topic summary texts + watch → `prompts.build_synthesis_prompt` → `02_brief.md`.
2. Restructure markdown only → `02_brief.json`.

Synthesis may shorten or omit weak bullets; it does not re-fetch news. Stories only survive if they were in a summary block (including **Unassigned**).

---

## Output layout

```
user_data/market_brief/<YYYY-MM-DD>/
├── 00_news/           Raw JSON per topic + _unassigned.json
├── 01_summaries/      Per-topic Perplexity markdown
├── 02_brief.md        ← read this
├── 02_brief.json
├── ingest_stats.json
├── run.log
├── usage.json
└── qa_funnel.md       Only with --qa-log or MARKET_BRIEF_QA_LOG=1
```

Postgres `benzinga_articles` caches ingested rows; runs purge rows older than 7 days.

**`run.log`** includes:

- **INGEST** / **ROUTING** breakdown (per source, dedupe, per theme/sector/unassigned)
- **STAGE** banners (`INGEST`, `SUMMARIZE`, `WATCH`, `SYNTHESIZE`)
- Every **Perplexity** call: `PERPLEXITY START | … | task: …` and on failure `PERPLEXITY RETRY` / `PERPLEXITY FAILED` with the same label

Use `--qa-log` for the longer `qa_funnel.md` file.

---

## Run

Inside the **backend container** (`POLYGON_API_KEY`, `PERPLEXITY_API_KEY`):

```bash
docker compose exec backend python -m market_brief.run --dry-run
docker compose exec backend python -m market_brief.run
docker compose exec backend python -m market_brief.run --asof 2026-05-15
docker compose exec backend python -m market_brief.run --qa-log
```

UI **Run brief** → `POST /api/market-brief/run` (optional body `{"qa_log": true}`).

`--qa-log` writes `qa_funnel.md`: API pull counts, dedupe, per-topic assignment sizes, unassigned count, summarize stats, usage.

---

## What to edit

| File | Controls |
|------|----------|
| `config.py` | Sectors, ingest limits, channel list, models, `QA_LOG_ENABLED` |
| `user_data/themes.json` | Theme names, descriptions, ticker lists |
| `tape.py` | `SECTOR_TICKERS` baskets and index ETFs for verified tape |
| `prompts.py` | Summary and brief voice/structure |

**Legacy:** `MARKET_BRIEF_USE_WEB_PROBES=1` skips Benzinga ingest and uses old Perplexity web-search probes per topic instead.

---

## Module map

| Module | Role |
|--------|------|
| `run.py` | CLI |
| `pipeline.py` | Orchestrates stages |
| `topics.py` | Load sectors + themes |
| `ingest.py` | Fetch, dedupe, assign, snapshots |
| `summarize.py` | Per-topic + unassigned summaries |
| `tape.py` | OHLC tape for prompts |
| `prompts.py` | All LLM prompts |
| `persist.py` | Write `01_summaries/` |
| `funnel_log.py` | `qa_funnel.md` |
| `discover_channels.py` | Probe Polygon channel slugs |
| `backend/benzinga_news.py` | Polygon API + DB |
