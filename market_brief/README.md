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

## Narratives (`narratives.py`)

Weekly roll-up of persistent storylines across briefs. Reads `02_brief.md` per date (Benzinga if it has Narrative Threads, else Px; last `--window-days` 120), parses `**Title** \`Category\` → new|ongoing` threads + one-liner in code, then **one Sonnet call** groups retitled threads into storylines (key, title, category, impact 1–5, arc, state, thread ids, turning points). Scoring in code: `intensity(date) = min(1.5, Σ max(0.35, 1 − 0.1·(rank−1)))`, `heat = impact/5 × Σ intensity × 0.5^(age/14d)`, `weight = impact/5 × Σ intensity`; status active < 3 briefs since last, fading < 8, else dormant.

~27K in / 11K out ≈ $0.25, ~4 min. Unparseable briefs (old `## TL;DR` format) are skipped.

```bash
docker compose exec backend python -m market_brief.narratives            # parse + cluster + score
docker compose exec backend python -m market_brief.narratives --dry-run  # parse + 02_llm_input.md only
docker compose exec backend python -m market_brief.narratives --rescore  # re-score latest run, no LLM
```

**Artifacts** (`user_data/market_narratives/<latest brief date>/`): `01_threads.json`, `02_llm_input.md`, `02_llm_response.{txt,json}`, `narratives.json`, `run_costs.json`. Global `user_data/market_narratives/status.json` + `subprocess.log` (UI Rebuild). **UI:** `/narratives`, `/m/narratives`. API: `/api/narratives`, `/api/narratives/generate`, `/api/narratives/prices?run=` (request-time, no LLM: equal-weight basket of the LLM `basket` tickers, else most-mentioned, max 6, from `ohlc` ∪ `index_prices`; % from the close before the first brief's session, plus SPY).

---

## Market Brief - Px (`perplexity_brief.py`)

A/B variant that replaces Benzinga ingest with Perplexity web search. Fully decoupled from the Benzinga run: own DB screener universe (`screener_universe.py`) + verified tape (`tape.py`: OHLC + index closes), own output root. Search plan + prompts: `px_prompts.py`.

| Phase | Files in `01_research/` | Calls | Model |
|---|---|---|---|
| A. Broad probes: one narrow topic each (`BROAD_PROBES`: rates, Fed context, econ data, oil, geopolitics, crypto, movers, pre-market, earnings, analyst actions, deals, FDA, AI capex, semis, AI platforms, software, AI policy, strategists, named investor calls, quantum/space, legal/IP, calendars, overnight/weekend corporate · analyst · megacap · policy, 10 sector company-news sweeps, earnings tonight/tomorrow, credit & housing) | `a_<slug>` | 42 | `sonar-pro` |
| A. Ticker batches: universe (~62) in slice priority, 4/call, tape rows injected | `tickers_NN` | ~16 | `sonar-pro` |
| Plan: reads A + tape → ≤12 follow-up threads (cross-ticker causal links, second-order winners/losers, contested catalysts, missing numbers) | `01b_plan.json` | 1 | Sonnet |
| B. Thread follow-ups | `b_<slug>` | ≤12 | `sonar-pro` |
| C. Gap fills: universe movers \|1D\| ≥ 3% (Mega ≥ 1.5%) still "No company-specific catalyst" | `c_<SYM>` | ≤15 | `sonar-pro` |
| Compact: dedupe channel/thread research into a fact ledger (groups macro · corporate · themes); ticker ledger built in code (cap tier, \|move\|, no-catalyst names collapsed) | `02_ledger/*.md` | 3 | Sonnet |
| Synthesis `synthesis_<model>`: `STEP4_SYSTEM_PROMPT` + `SYNTH_ADDENDUM` (length caps, causal chains, timing) | `02_brief_draft.md` | 1 | Opus (default) · `--synth sonnet\|haiku\|perplexity` |
| Coverage + revise: Sonnet lists material ledger stories the draft omits → synth model integrates them | `02_coverage.md` | 2 | Sonnet + synth model |
| Verify: Sonnet fact-checks numbers/weekdays/timestamps vs research + tape + calendar → exact find/replace fixes applied in code (opt-in `--verify`; off by default since 10/6 A/B: 2 fixes, $0.35) | `02_verify.json` → `02_brief.md` | 1 | Sonnet |
| Eval (opt-in `--eval`; off for normal/API runs): compare + grade Px brief vs Benzinga brief on a fixed per-date item list | `compare.md`, `eval_items.md`, `compare_nuance.md` | 1–2 | Sonnet |

~100 calls, ~$4, ~30–35 min. Search filters: `search_domain_filter` = `DOMAIN_DENYLIST` (social, promo, SEO aggregators); `search_after_date_filter` = session − 1d (pre-market probes: session), `search_before_date_filter` = asof + 1d. Filters are day-granular; `--cutoff HH:MM` ET (default now if today, else 09:00) is enforced in prompts. **Backdated runs leak post-cutoff news** (e.g. an evening deal on the brief date) — score live runs.

```bash
docker compose exec backend python -m market_brief.perplexity_brief                     # today, cutoff = now
docker compose exec backend python -m market_brief.perplexity_brief --asof 2026-09-24 --cutoff 08:37  # backdated
docker compose exec backend python -m market_brief.perplexity_brief --dry-run           # phase A prompts only → 00_prompts/ (no run.log)
docker compose exec backend python -m market_brief.perplexity_brief --skip-research     # re-compact + re-synth, no search
docker compose exec backend python -m market_brief.perplexity_brief --resume-followups  # reuse phase A, rerun plan + B/C onward
docker compose exec backend python -m market_brief.perplexity_brief --no-followups --no-revise --compact  # ablations (compaction is opt-in)
docker compose exec backend python -m market_brief.perplexity_brief --asof 2026-09-17 --universe baseline  # opt-in: copy Benzinga run's lineage.json
docker compose exec backend python -m market_brief.perplexity_brief --asof 2026-09-17 --compare-only      # rebuild compare.md
docker compose exec backend python -m market_brief.perplexity_brief --asof 2026-09-17 --eval-only         # rebuild compare_nuance.md
```

**UI:** `/market-brief-px`, `/m/market-brief-px` — same viewer as Market Brief via `brief_cfg` (`MARKET_BRIEF_PX_CFG` in `frontend/app.py`). API: `/api/market-brief-px/{dates,<date>,<date>/costs,<date>/pdf,generate}`.

**Status:** `status.json` stages `hydrate` → `research` (`detail`: `Phase A: N/M …`, `Planning follow-up threads`, `Phase B/C: N/M …`) → `compact` → `synthesis_<model>` → `coverage` → `revise` → `verify` (opt-in) → `eval` → `done`; `failed` + `error` on exception. All phase A searches failed → run fails; partial failures → `detail` on the complete status.

**Artifacts** (`user_data/market_brief_perplexity/<date>/`): `status.json`, `tape.md`, `source/ticker_universe/`, `01_research/*.md` (facts + source URLs) / `*.json` (job + prompt + raw response), `01b_plan.json`, `02_ledger/`, `02_synth_input.md`, `02_brief_draft.md`, `02_coverage.md`, `02_brief.md`, `run_costs.json` (Perplexity + Anthropic rows), `run.log`, `subprocess.log`, `compare.md`, `eval_items.md`, `compare_nuance.md`. Earlier pipeline versions for A/B: `user_data/market_brief_perplexity_v1/` (3 broad + 12/call), `_v2/` (before coverage/revise + extra probes).

**Compare** (no LLM): parses Top Movers tables from `user_data/market_brief/<date>/02_brief.md` and the Px brief → ticker overlap, sign mismatches, Narrative Thread titles. Skipped if no Benzinga brief for that date.

**Nuance eval** (LLM): `eval_items.md` = material items extracted once from the Benzinga brief (rebuilt when it changes); `compare_nuance.md` grades the Px brief per item (found / partial / missing / misattributed / baseline_error, major/minor tier, snapshot-timing numbers not graded) → score computed in code: recall = (found + baseline_error + 0.5 × partial) / N and story coverage = not-missing / N, plus Px-only material. Experiment log, gaps and next steps: `PX_EXPERIMENT_NOTES.md`.
