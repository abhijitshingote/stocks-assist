# Market Brief

Pre-market brief + R1D losers brief, both Perplexity-sourced (Benzinga/Massive retired 2026-10). Hydration (ticker universe, OHLC tape) comes from Postgres; news from Perplexity `sonar-pro` web search; writing from Anthropic.

**Requirements:** `PERPLEXITY_API_KEY`, `ANTHROPIC_API_KEY`, Postgres (`db` service). Run everything inside the backend container.

## Storage

| What | Where |
|---|---|
| Finished briefs | `app.briefs`, PK `(date, kind daily\|losers, source perplexity\|benzinga)`: `brief_md`, `brief_json`, `run_costs`, `metadata`. Benzinga rows are history (daily 2026-05-11 → 2026-10-15, losers 2026-06-04 → 2026-10-17). |
| Run working dir (scratch) | `user_data/market_brief_perplexity/<date>/`, `user_data/market_brief_perplexity_losers/<date>/`: `status.json`, logs, research, intermediates. Pipelines publish `02_brief.md` + `run_costs.json` to `app.briefs` at the end, before `status.json` → `complete`. |
| Narratives | `app.pipeline_runs` (`narratives`, `events.json`); scratch `user_data/market_narratives/` |

## Viewer

`/market-brief`, `/m/market-brief` (one viewer, `brief_cfg` = `MARKET_BRIEF_CFG` in `frontend/app.py`). Date list = union of `app.briefs` dates, scratch dirs with a `status.json`, and today. Each date shows source badges (`PX`, `BZ`, `L` = has losers brief); when a date has both sources a PX/BZ toggle appears (default: perplexity). The losers brief renders under the daily brief (perplexity preferred). `/market-brief-px` redirects here.

API (`backend/app.py`): `/api/market-brief/dates`, `/<date>?source=&losers_source=`, `/<date>/costs?kind=`, `/<date>/pdf?kind=&source=`, `POST /generate` (daily), `POST /api/market-brief-losers/generate`, `/api/market-brief-losers/<date>/costs`. Status per kind: `running` (scratch `status.json`, stale after 15 min without log activity → `failed`) › `complete` (row in `app.briefs`) › `failed` › `empty`.

## Daily brief (`perplexity_brief.py`)

DB screener universe (`screener_universe.py`: r1d, vol_spike_5d, main_view_ti65 × mega/large/mid_small) + verified tape (`tape.py`: OHLC + index closes). Search plan + prompts: `px_prompts.py`.

| Phase | Files in `01_research/` | Calls | Model |
|---|---|---|---|
| A. Broad probes: one narrow topic each (`BROAD_PROBES`: rates, Fed context, econ data, oil, geopolitics, crypto, movers, pre-market, earnings, analyst actions, deals, FDA, AI capex, semis, AI platforms, software, AI policy, strategists, named investor calls, quantum/space, legal/IP, calendars, overnight/weekend corporate · analyst · megacap · policy, 10 sector company-news sweeps, earnings tonight/tomorrow, credit & housing) | `a_<slug>` | 42 | `sonar-pro` |
| A. Ticker batches: universe (~62) in slice priority, 4/call, tape rows injected | `tickers_NN` | ~16 | `sonar-pro` |
| Plan: reads A + tape → ≤12 follow-up threads (cross-ticker causal links, second-order winners/losers, contested catalysts, missing numbers) | `01b_plan.json` | 1 | Sonnet |
| B. Thread follow-ups | `b_<slug>` | ≤12 | `sonar-pro` |
| C. Gap fills: universe movers \|1D\| ≥ 3% (Mega ≥ 1.5%) still "No company-specific catalyst" | `c_<SYM>` | ≤15 | `sonar-pro` |
| Compact (opt-in `--compact`): dedupe channel/thread research into a fact ledger; ticker ledger built in code (cap tier, \|move\|, no-catalyst names collapsed) | `02_ledger/*.md` | 3 | Sonnet |
| Synthesis `synthesis_<model>`: `STEP4_SYSTEM_PROMPT` (`prompts_pipeline.py`) + `SYNTH_ADDENDUM` (length caps, causal chains, timing) | `02_brief_draft.md` | 1 | Opus (default) · `--synth sonnet\|haiku\|perplexity` |
| Coverage + revise: Sonnet lists material ledger stories the draft omits → synth model integrates them | `02_coverage.md` | 2 | Sonnet + synth model |
| Verify (opt-in `--verify`): Sonnet fact-checks numbers/weekdays/timestamps vs research + tape + calendar → exact find/replace fixes | `02_verify.json` → `02_brief.md` | 1 | Sonnet |

~100 calls, ~$4, ~30–35 min. Search filters: `search_domain_filter` = `DOMAIN_DENYLIST` (social, promo, SEO aggregators); `search_after_date_filter` = session − 1d (pre-market probes: session), `search_before_date_filter` = asof + 1d. Filters are day-granular; `--cutoff HH:MM` ET (default now if today, else 09:00) is enforced in prompts. **Backdated runs leak post-cutoff news** (e.g. an evening deal on the brief date).

```bash
docker compose exec backend python -m market_brief.perplexity_brief                     # today, cutoff = now
docker compose exec backend python -m market_brief.perplexity_brief --asof 2026-09-24 --cutoff 08:37  # backdated
docker compose exec backend python -m market_brief.perplexity_brief --dry-run           # phase A prompts only → 00_prompts/ (no run.log)
docker compose exec backend python -m market_brief.perplexity_brief --skip-research     # re-synth from existing 01_research/
docker compose exec backend python -m market_brief.perplexity_brief --resume-followups  # reuse phase A, rerun plan + B/C onward
docker compose exec backend python -m market_brief.perplexity_brief --no-followups --no-revise  # ablations
```

**Status:** `status.json` stages `hydrate` → `research` (`detail`: `Phase A: N/M …`, `Planning follow-up threads`, `Phase B/C: N/M …`) → `compact` → `synthesis_<model>` → `coverage` → `revise` → `verify` (opt-in) → `done`; `failed` + `error` on exception. All phase A searches failed → run fails; partial failures → `detail` on the complete status.

**Scratch artifacts:** `status.json`, `tape.md`, `source/ticker_universe/`, `01_research/*.md` (facts + source URLs) / `*.json` (job + prompt + raw response), `01b_plan.json`, `02_ledger/`, `02_synth_input.md`, `02_brief_draft.md`, `02_coverage.md`, `02_brief.md`, `run_costs.json` (Perplexity + Anthropic rows), `run.log`, `subprocess.log`.

## Losers brief (`px_losers.py`)

Bottom `dr_1` per cap bucket from `stock_metrics` (`losers_universe.py`; `LOSERS_TOP_N` mega 15 / large 10 / mid_small 10, micro excluded, global liquidity + industry filters) → tape → Perplexity ticker batches (4/call, `P.ticker_batch_prompt`) → gap fills for names still without a catalyst (`select_gaps`, ≤15) → ticker ledger → Sonnet with `prompts/losers_synthesis.md` (one-liner, losers table with driver type, thematic/sector weakness, idiosyncratic movers, sector roll-up, watch). Source scope: ticker research only, no macro probes.

~25 calls, ~$0.35, ~4 min.

```bash
docker compose exec backend python -m market_brief.px_losers                          # today
docker compose exec backend python -m market_brief.px_losers --date 2026-10-16 --skip-research --synth opus
```

**Scratch artifacts** (`user_data/market_brief_perplexity_losers/<date>/`): `losers.md` (universe table), `tape.md`, `01_research/`, `02_synth_input.md`, `02_brief.md`, `run_costs.json`, `status.json`, `run.log`, `subprocess.log`.

## Narratives (`narratives.py`)

What moved markets: parses every published daily brief (`app.briefs`, Benzinga where it parses, else Px; last `--window-days` 120) — one-liner, `### ⚡ Top Movers At a Glance` rows, `### 🧭 Narrative Threads` — then **one Sonnet call** lists discrete events ranked by impact; code dedupes and backfills uncovered Mega-cap catalyst moves → `events.json` (~$0.31).

```bash
docker compose exec backend python -m market_brief.narratives            # parse + LLM + publish
docker compose exec backend python -m market_brief.narratives --dry-run  # parse + prompt only
docker compose exec backend python -m market_brief.narratives --rescore  # re-postprocess latest run, no LLM
```

**UI:** `/narratives`, `/m/narratives`. API: `/api/narratives?run=`, `/api/narratives/generate`, `/api/narratives/prices?run=` (request-time, no LLM). Live status: `user_data/market_narratives/status.json`.

## Config (`config.py`)

Paths (`USER_DATA_DIR` scratch root), ticker universe (`TICKER_UNIVERSE_*`, `VOLSPIKE_GAPPER_WINDOW_DAYS`), losers universe (`LOSERS_*`), `MARKET_CAP_CATEGORIES`, liquidity (`LIQUIDITY_MIN_*`) and `EXCLUDED_INDUSTRIES`. Models, batch size, gap thresholds: top of `perplexity_brief.py`.
