# Theme Discovery

Finds the themes behind this week's leading large + mega caps, then probes each theme for other stocks. UI: `/theme-discovery`, `/m/theme-discovery`.

```
[1] Candidates  ─►  [2] News  ─►  [3] Themes  ─►  [4] Probe  ─►  themes.json
 /api/WeeklyReview   Perplexity   Opus, 1 call   Perplexity
 picks per list      1 per stock                 1 per theme
```

| Stage | What | Knobs (`config.py`) |
|---|---|---|
| 1 Candidates | `/api/WeeklyReview?include_listed=1` (Fast RS, Strong Stocks, Top Returns 5/20, Vol Spike & Gaps 90d), `large` + `mega`, minus Biotechnology. Per list: Fast RS top 12 by `adjusted_rs_score`, Strong top 12 by `adjusted_ti65`, Top Returns 5d top 8 by `adjusted_dr_5`, 20d top 8 by `adjusted_dr_20`, Vol Spike top 12 by `adjusted_event_return` with `dr_20 > 0 or dr_60 > 0`. Union, max 6 per industry. Weekly Review still drops current-cycle passes | `PICKS`, `MAX_PER_INDUSTRY`, `CAP_BUCKETS`, `EXCLUDE_INDUSTRIES` |
| 2 News | `sonar-pro` per candidate: one line, dated drivers, read-through, full write-up, sources | `NEWS_MODEL`, `prompts/news.md` |
| 3 Themes | Opus groups shared reasons (≥ 2 stocks); one-company or unclear → QA | `SYNTH_MODEL`, `MIN_STOCKS_PER_THEME`, `prompts/synthesize.md` |
| 4 Probe | `sonar-pro` per theme: other US stocks with exposure; kept if in `stock_metrics` and market cap ≥ $2B, max 15 | `PROBE_*`, `prompts/probe.md` |

~28 candidates, ~9 themes, ~$0.50, ~3 min.

```bash
docker compose exec backend python -m theme_discovery.run                      # today (ET)
docker compose exec backend python -m theme_discovery.run --max-candidates 5   # cheap test
docker compose exec backend python -m theme_discovery.run --from-stage 3       # reuse candidates + news, regroup + reprobe
docker compose exec backend python -m theme_discovery.run --force              # ignore cached news / probe files
```

Artifacts `user_data/theme_discovery/<date>/`: `01_candidates.json`, `02_news/<T>.json` + `02_news.json`, `03_synth_input.md`, `03_synth_response.txt`, `03_themes.json`, `04_probe/<slug>.json`, `themes.json` (UI SoT), `run_summary.json` (Perplexity + Anthropic cost), `run_costs.json` (Anthropic), `status.json`, `run.log`.
