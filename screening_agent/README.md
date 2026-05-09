# Screening Agent (Prototype)

A modular momentum-stock screener built as a chain of agents:

```
[1] Initial Screener ─► [2] Technical Agent ─► [3] Market News Agent ─► [4] Fundamental Agent ─► Final List
   (DB query)            (Ollama)               (Perplexity + Ollama)    (Perplexity + Ollama)
```

The chain is intentionally loose — every step reads JSON in, writes JSON out, and
can be run standalone for experimentation. State is persisted under
`outputs/<run_id>/` so you can iterate on one agent without re-running the others.

* **Stage 3 — Market News Agent** is a *macro* scanner. It uses Perplexity
  (cheapest `sonar` model, recency=week) to ground a list of the hottest
  themes/narratives the market is paying for right now, then has Ollama
  structure that into clean JSON.
* **Stage 4 — Fundamental Agent** is *ticker-specific*. For each survivor it
  asks Perplexity for that ticker's recent material drivers, then has Ollama
  score how well those drivers fit the hot themes.

## Philosophy

We are **not** valuing companies on PE / DCF. We're hunting for stocks where:

* Price action says "this thing can't be kept down" (technical agent).
* The current market obsession (themes from news in the last 1–2 weeks) directly
  intersects with material, real events at the company (fundamental agent).

## Folder layout

```
screening_agent/
├── README.md
├── requirements.txt
├── config.py              ← All knobs in one place (Ollama model, MCP servers, thresholds)
├── pipeline.py            ← Orchestrates the 3 agents
├── run.py                 ← `python -m screening_agent.run` CLI
├── agents/
│   ├── base.py            ← Common contract + Ollama helpers
│   ├── initial_screener.py
│   ├── technical_agent.py
│   └── fundamental_agent.py
├── tools/
│   ├── db.py              ← Read-only DB session (uses backend/models.py)
│   ├── ollama_client.py   ← HTTP wrapper around `/api/chat` w/ JSON mode
│   ├── mcp_client.py      ← Spawns MCP servers, exposes their tools to agents
│   ├── price_data.py      ← OHLC enrichment + technical features (EMA/MA/ATR/contraction)
│   └── news.py            ← News retrieval via MCP (fallback: stub)
├── prompts/               ← Editable prompt templates (markdown)
├── examples/
└── outputs/               ← Run artifacts (one folder per run_id)
```

## Quick start

```bash
# 1. Make sure Ollama is running locally and the configured model is pulled
ollama serve &
ollama pull qwen2.5:14b   # or qwen2.5:7b for ~3x speedup

# 2. Install deps
pip install -r screening_agent/requirements.txt

# 3. Make sure PERPLEXITY_API_KEY is in your .env (already there in this repo)

# 4. Cheap test: full pipeline but only 5 tickers reach the fundamental stage
python -m screening_agent.run --limit 30 --max-tickers 5 --verbose

# 5. Re-run only stages 3+4 on an existing run
python -m screening_agent.run --run-id <prev_run_id> --from-stage market_news \
    --max-tickers 5 --verbose
```

Per-stage standalone (after a full run has produced the JSONs):

```bash
# Just rerun the fundamental scoring
python -m screening_agent.agents.fundamental_agent \
    --technical   outputs/<run>/02_technical.json \
    --market-news outputs/<run>/03_market_news.json \
    --output      outputs/<run>/04_fundamental.json \
    --max-tickers 5

# Just rerun the theme scan
python -m screening_agent.agents.market_news_agent \
    --input  outputs/<run>/02_technical.json \
    --output outputs/<run>/03_market_news.json
```

## Running outside Docker (host machine, Mac/WSL)

You don't need Docker for prototyping the agents themselves — they only need
Postgres + Ollama. From the workspace root:

```bash
pip install -r screening_agent/requirements.txt
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/stocks_db \
  python -m screening_agent.run --limit 30
```

## (Optional) MCP servers

Stages 3 and 4 use Perplexity directly for grounded news, not MCP. The
`tools/mcp_client.py` + `mcp_servers.json` plumbing is still in the repo if
you want to wire MCP-based tools into a future agent. Auto-discovers tools
from every connected server; no code changes needed when you add or swap one.

## Iterating on a single agent

Each agent file has an `if __name__ == "__main__":` block that takes
`--input` / `--output` JSON paths. Workflow:

1. Run the pipeline once to produce intermediate JSONs.
2. Tweak `prompts/<agent>.md` or the agent's enrichment code.
3. Re-run that agent only.
4. Diff the JSONs.

Prompts live as plain `.md` files — edit, save, re-run, no code change required.
