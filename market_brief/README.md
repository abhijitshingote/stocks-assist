# Market Brief

Daily pre-market brief built from Benzinga news. Perplexity-sourced A/B variant: [Market Brief - Px](#market-brief---px-perplexity_briefpy). Run in the backend container:

```bash
docker compose exec backend python -m market_brief.run_pipeline
```

**Requirements (Anthropic pipeline):** `POLYGON_API_KEY`, `ANTHROPIC_API_KEY`, Postgres (`db` service). Legacy Perplexity path also needs `PERPLEXITY_API_KEY`.

---

## The process (four steps)

Everything for one day lives under `user_data/market_brief/<YYYY-MM-DD>/`.

```
  FETCH          GROUP BY CATEGORY    SUMMARIZE (LLM)      MERGE (LLM)
     │                  │                    │                  │
     ▼                  ▼                    ▼                  ▼
 source/            (step 2 TBD)       01_summaries/        02_brief.md
 (raw JSON)         group by category  (markdown)          (final brief)
```

| Step | Folder | What happens | LLM? |
|------|--------|--------------|------|
| **1. Fetch** | `source/` | Pull articles from Polygon/Benzinga; save full JSON by *how* we fetched (general feed, channel, or one ticker) | No |
| **2. Category tagging** | LLM instruction | Baked into Steps 3 & 4 — no code, no folder | No |
| **3. Fact extract** | `01_summaries/` | One Sonnet call per channel + ticker batch → structured facts | **Yes** (Anthropic Sonnet) |
| **4. Synthesize** | `02_brief.md` | One Opus call merges summaries + ticker universe into final brief | **Yes** (Anthropic Opus) |

`02_brief.md` never reads raw Benzinga bodies. It only sees the text in `01_summaries/`.

---

## Step 1 — Fetch (`source/`)

**Refresh** upserts the last 3 days into Postgres (`benzinga_articles`). **Synthesis** reads the DB using trading-calendar windows below.

| Step | What |
|------|------|
| **Refresh** | `refresh_benzinga_articles(end=brief run instant)` — 3 days ending at same ``asof`` 6 AM ET used for synthesis windows |
| **Brief prep** | `prepare_run()` — refresh + screener `source/ticker_universe/` + `metadata.json` |
| **Synthesis** | `source_loader` — three DB pulls: general window (untagged), each channel in `GENERAL_CHANNEL_FETCHES`, ticker window + universe |

**General/channel window** — NYSE session rules in `trading_calendar.py` (e.g. weekend → prior Friday 5 AM; weekday before 9:30 → prior session).

**Ticker universe** — who we fetch is **not** from `themes.json`. It comes from DB screener screens (`screener_universe.py`):

| Screen | Rule |
|--------|------|
| `r1d` | Top 10 by 1-day return per cap bucket |
| `vol_spike_5d` | Vol spike/gapper in last 5 days |
| `main_view_ti65` | Top 10 by TI65 |

Cap buckets: mega, large, mid_small ($200M–$20B; micro excluded). ~65 symbols after dedupe (each symbol in one screen only). Human-readable list: `source/ticker_universe/overview.md`.

---

## Step 2 — Category tagging

No separate code step. The category vocabulary below is passed to the LLM in Steps 3 and 4. During fact extraction (Step 3), Sonnet tags each ticker/article section with its category. During synthesis (Step 4), Opus uses those tags as the organizing lens for Narrative Threads.

Recognized categories: AI Compute · Memory & Interconnect · Optical & Photonics · Chip Equipment · Fab & Foundry · Wireless & Mobile · Analog & Mixed-Signal · Power & Wide-Bandgap · Test & Advanced Packaging · Specialty Materials & IP Licensing · Quantum Computing · EdgeAI · Semiconductors · AI Infrastructure · Software & SaaS · Internet & Platforms · Communications & Networking

The LLM may identify additional categories not on this list if the source material clearly supports one.
---

## Step 3 — Fact extraction (`01_summaries/`)

**First LLM step (Anthropic Sonnet).** Reads deduped articles per Benzinga channel and per ticker batch (grouped by screener screen from `overview.md`). Writes `channel_<slug>.md` and `tickers_<batch>.md`. Categories are inferred by the model during extraction — not pre-grouped in code.

Requires `ANTHROPIC_API_KEY`. Cost tracked in `run_costs.json`.

---

## Step 4 — Synthesize (`02_brief.md`)

**Second LLM step (Anthropic Opus).** Concatenates all `01_summaries/*.md` + `ticker_universe/overview.md` → final brief (`02_brief.md`).


---

## Folder map

```
user_data/market_brief/<YYYY-MM-DD>/
├── metadata.json              # fetch ids, dedupe, windows, ingest stats
├── source/ticker_universe/    # screener overview + lineage
├── 00_news/                   # legacy Perplexity pipeline snapshots
├── 01_summaries/              # Step 3 — LLM summaries per topic
├── 02_brief.md                # Step 4 — final brief
├── run_costs.json             # Anthropic API cost breakdown
└── run.log
```

---

## Commands

```bash
# Preview screener universe + topics (no API)
docker compose exec backend python -m market_brief.run --dry-run

# Full pipeline (deletes and rewrites source/, then Anthropic brief)
docker compose exec backend python -m market_brief.run --asof 2026-05-31

# Ingest only (rewrite source/; no LLM)
docker compose exec backend python -m market_brief.run --skip-llm-summary --asof 2026-05-31

# LLM only (existing source/)
docker compose exec backend python -m market_brief.run --skip-ingest --asof 2026-05-31

# Resume after partial failure (retry placeholders + Opus synthesis)
docker compose exec backend python -m market_brief.run --asof 2026-05-31 --resume

# Label a past calendar day
docker compose exec backend python -m market_brief.run --asof 2026-05-31
```

---

## Config highlights (`config.py`)

| Setting | Default | Effect |
|---------|---------|--------|
| `TICKER_UNIVERSE_TOP_N` | 10 | Names per screen × cap bucket |
| `TICKER_NEWS_EXTRA_HOURS` | 24 | Ticker fetch starts before general 5 AM |
| `PER_TICKER_LIMIT` | 25 | Max articles per ticker API call |
| `GENERAL_NEWS_LIMIT` | 100 | General feed cap |
| `MARKET_BRIEF_SUMMARIZE_BACKEND` | `perplexity` | `ollama` for local per-article summarize |

---

## Troubleshooting

| Problem | Likely cause |
|---------|----------------|
| Huge `00_news/` but thin brief | Summarize/synth failed or skipped; check `run.log` |
| Name only in `_unassigned.json` | Ticker not in any `themes.json` basket |
| Empty `source/ticker/SYM/` | No Benzinga stories in the extended window for that symbol |
| `too_many_prompt_tokens` on synth | Ollama summaries too large; use Perplexity summarize for production |

Channel slug probe: `docker compose exec backend python -m market_brief.discover_channels`

---

## Market Brief - Px (`perplexity_brief.py`)

A/B variant that replaces Benzinga ingest with Perplexity web search. Fully decoupled from the Benzinga run: own DB screener universe (`screener_universe.py`) + verified tape (`tape.py`: OHLC + index closes), own output root.

| Step | Calls | Model |
|---|---|---|
| Broad market: `broad_macro_cross_asset`, `broad_corporate_news`, `broad_calendar` | 3 | `sonar-pro` (search) |
| Ticker batches `tickers_NN`: universe (~62) flattened in slice priority, ~12/call, tape rows injected | ~6 | `sonar-pro` (search) |
| Synthesis `synthesis_<model>`: `STEP4_SYSTEM_PROMPT`, same output format as `02_brief.md` above | 1 | Sonnet (default) · `--synth haiku\|opus\|perplexity` |

Search filter: `search_after_date_filter` = session − 1d, `search_before_date_filter` = asof + 1d.

```bash
docker compose exec backend python -m market_brief.perplexity_brief                     # today
docker compose exec backend python -m market_brief.perplexity_brief --dry-run           # prompts only → 00_prompts/ (no run.log)
docker compose exec backend python -m market_brief.perplexity_brief --skip-research --synth haiku  # re-synth, no search
docker compose exec backend python -m market_brief.perplexity_brief --asof 2026-09-17 --universe baseline  # opt-in: copy Benzinga run's lineage.json
docker compose exec backend python -m market_brief.perplexity_brief --asof 2026-09-17 --compare-only      # rebuild compare.md
```

**UI:** `/market-brief-px`, `/m/market-brief-px` — same viewer as Market Brief via `brief_cfg` (`MARKET_BRIEF_PX_CFG` in `frontend/app.py`). API: `/api/market-brief-px/{dates,<date>,<date>/costs,<date>/pdf,generate}`.

**Status:** `status.json` stages `hydrate` → `research` (`detail`: `N/M Perplexity searches done`) → `synthesis_<model>` → `done`; `failed` + `error` on exception. All searches failed → run fails; partial failures → `detail` on the complete status.

**Artifacts** (`user_data/market_brief_perplexity/<date>/`): `status.json`, `tape.md`, `source/ticker_universe/`, `01_research/*.md` (facts + source URLs) / `*.json` (prompt + raw response), `02_synth_input.md`, `02_brief.md`, `run_costs.json` (Perplexity + Anthropic rows), `run.log`, `subprocess.log`, `compare.md`.

**Compare** (no LLM): parses Top Movers tables from `user_data/market_brief/<date>/02_brief.md` and the Px brief → ticker overlap, sign mismatches, Narrative Thread titles. Skipped if no Benzinga brief for that date.
