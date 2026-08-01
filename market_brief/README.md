# Market Brief

Daily pre-market brief built from Benzinga news. Run in the backend container:

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
