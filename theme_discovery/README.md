# Theme Discovery

Finds the themes behind this week's leading mid + large + mega caps, then probes each theme for other stocks. UI: `/theme-discovery`, `/m/theme-discovery`.

```
[1] Candidates  ─►  [2] News  ─►  [3] Themes            ─►  [4] Probe  ─►  confirm 1-stock themes  ─►  themes.json
 /api/WeeklyReview   Perplexity   Opus pass 1 + pass 2      Perplexity
 picks per list      1 per stock  + prior runs' themes      1 per theme
 per cap bucket
```

| Stage | What | Knobs (`config.py`) |
|---|---|---|
| 1 Candidates | `/api/WeeklyReview?include_listed=1` (Fast RS, Strong Stocks, Top Returns 5/20, Vol Spike & Gaps 90d), `mid` + `large` + `mega`, minus Biotechnology. Per list, per cap bucket: Fast RS top 12 by `adjusted_rs_score`, Strong top 12 by `adjusted_ti65`, Top Returns 5d top 8 by `adjusted_dr_5`, 20d top 8 by `adjusted_dr_20`, Vol Spike top 12 by `adjusted_event_return` with `dr_20 > 0 or dr_60 > 0`. Union, no industry limit. Weekly Review still drops current-cycle passes | `PICKS`, `MAX_PER_INDUSTRY` (None = off), `CAP_BUCKETS`, `EXCLUDE_INDUSTRIES` |
| 2 News | `sonar-pro` per candidate: one line, dated drivers, read-through, full write-up, sources | `NEWS_MODEL`, `prompts/news.md` |
| 3 Themes | Pass 1 (`prompts/synthesize.md`): group every reason a stock went up (stock can be in many themes). Pass 2 (`prompts/synthesize_layers.md`): checklist — components/supply chain, inputs/commodities, geography, policy, financing/rates, specific customer, events/flows, business-model shifts; adds new themes + stocks to existing ones. Union of both; a pass-2 theme with the same stock set as an earlier one is dropped. Prior runs' themes (last `PRIOR_RUNS`) are in both prompts: matching themes keep the exact name (`previous`, `first_seen`) and may have 1 stock. Kept if ≥ 2 stocks, or matches a prior theme, or `single` (1 stock, pending probe). Stock-specific / unclear → QA | `SYNTH_MODEL`, `MIN_STOCKS_PER_THEME`, `PRIOR_RUNS` |
| 4 Probe | `sonar-pro` per theme: other US stocks with exposure; kept if in `stock_metrics` and market cap ≥ $2B, max 15 | `PROBE_*`, `prompts/probe.md` |
| Confirm | A `single` theme survives if ≥ 2 probe-added stocks have `dr_20 ≥ 10` or `dr_60 ≥ 20`; else its stock → QA and the theme → `unconfirmed` | `SINGLE_CONFIRM_*` |

~83 candidates, ~40 themes, ~$1.90, ~10 min (2026-10-14 run).

```bash
docker compose exec backend python -m theme_discovery.run                      # today (ET)
docker compose exec backend python -m theme_discovery.run --max-candidates 5   # cheap test
docker compose exec backend python -m theme_discovery.run --from-stage 3       # reuse candidates + news, regroup + reprobe
docker compose exec backend python -m theme_discovery.run --force              # ignore cached news / probe files
```

Artifacts `user_data/theme_discovery/<date>/`: `01_candidates.json`, `02_news/<T>.json` + `02_news.json`, `03_synth_pass{1,2}_input.md` / `_response.txt`, `03_themes.json` (pre-confirm), `04_probe/<slug>.json`, `themes.json` (UI SoT), `run_summary.json` (Perplexity + Anthropic cost), `run_costs.json` (Anthropic), `status.json`, `run.log`.

Theme memory reads every `<run>/themes.json` whose folder name sorts before the current run, so keep run folders date-named.
