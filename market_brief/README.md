# Market Brief

A separate, deeper-than-headlines daily intelligence pipeline. Probes
Perplexity sector-by-sector and theme-by-theme to surface
**stock-specific** stories — earnings reads, supply-chain wins, product
launches, regulatory shifts — not generic market headlines.

This is **not** part of `daily_screener`. Different goal: that pipeline
picks tickers to trade today; this one builds the morning context.

## Output

```
market_brief/outputs/<YYYY-MM-DD>/
├── 01_probes/
│   ├── semiconductors__overview.md
│   ├── semiconductors__catalyst.md
│   ├── ai_compute__overview.md
│   └── ...
├── 02_brief.md      ← read this each morning
├── 02_brief.json    ← structured form (for future UI wiring)
├── run.log
└── usage.json       ← Perplexity $ cost for the run
```

`02_brief.md` is tiered:

1. **TL;DR** — top 5–7 things across everything
2. **Per-sector / per-theme** — combined overview + catalyst per topic
3. **Stock-specific callouts** — consolidated single-name action list
4. **Watch tomorrow** — named-ticker events in the next 24–48h

## Run

```bash
# Make sure PERPLEXITY_API_KEY is set in your shell.
python -m market_brief.run

# Dry-run: list the topics that would be probed, no API calls.
python -m market_brief.run --dry-run

# Backfill an earlier date label.
python -m market_brief.run --asof 2026-05-15
```

## Cost shape

For the v0 config (5 sectors + ~11 themes ≈ 16 topics × 2 probes + 2
synthesis calls = **~34 Perplexity calls**) at `sonar-pro` for probes
and `sonar` for synthesis, expect roughly **$1–3 per run**. Tune
concurrency / model in `market_brief/config.py`.

## What to edit

- **`config.py`** — `SECTORS`, models, max tokens, concurrency.
- **`user_data/themes.json`** — themes (shared with daily_screener).
- **`prompts.py`** — probe and synthesis prompts. Edit freely; this is
  where the brief's voice lives.
