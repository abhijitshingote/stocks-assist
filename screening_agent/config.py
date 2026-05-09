"""All tunables for the screening agent chain.

Edit this file to swap models, change thresholds, or change the universe.
Each agent reads its own settings dict — keep them isolated so you can
experiment on one stage without disturbing the others.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env BEFORE we read any os.getenv() below so per-agent overrides
# like PERPLEXITY_MODEL_MARKET=sonar-pro actually take effect. Without
# this, .env values aren't visible until perplexity.py loads them later,
# which is after this module has already baked in the defaults.
load_dotenv()

ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = ROOT / "outputs"
PROMPTS_DIR = ROOT / "prompts"
MCP_CONFIG_FILE = ROOT / "mcp_servers.json"

# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------
OLLAMA = {
    "host": os.getenv("OLLAMA_HOST", "http://localhost:11434"),
    # Pick a model strong at reasoning + structured output. Override per-agent below.
    # On Apple Silicon, qwen2.5:7b or llama3.1:8b are 3-5x faster than 14b and
    # plenty good for this task — start there if you're seeing timeouts.
    "default_model": os.getenv("OLLAMA_MODEL", "qwen2.5:14b"),
    # Total request timeout (used only for non-streaming calls; streaming is
    # bounded per-chunk, see `stream_chunk_timeout_s`).
    "request_timeout_s": 600,
    # Time-without-bytes that kills a streaming request. Generous so a slow
    # model doing big JSON output doesn't get cut off, tight enough to detect
    # a truly stuck server.
    "stream_chunk_timeout_s": 180,
    # How long Ollama keeps the model loaded between calls. Big speedup for
    # back-to-back chunks.
    "keep_alive": "30m",
    "options": {
        "temperature": 0.2,
        # 8k is plenty for one chunk of stocks once we keep chunks small.
        # Lower context = much faster on local hardware.
        "num_ctx": 8192,
    },
}

# ---------------------------------------------------------------------------
# Database (read-only — we don't write back to the main DB from agents)
# ---------------------------------------------------------------------------
DB = {
    "url": os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/stocks_db",
    ),
}

# ---------------------------------------------------------------------------
# Stage 1 — Initial Screener
# Goal: cast a wide net of momentum candidates. We want the LIVE WIRES.
# ---------------------------------------------------------------------------
INITIAL_SCREENER = {
    # Candidate buckets, each contributes up to N tickers to the union.
    "buckets": {
        "recent_volspike": {"per_bucket": 40, "lookback_days": 10},
        "recent_gappers":  {"per_bucket": 40, "lookback_days": 10},
        "top_dr5":         {"per_bucket": 30},   # 5-day return leaders
        "top_dr20":        {"per_bucket": 30},   # 20-day return leaders
        "rsi_momentum":    {"per_bucket": 30},   # high RSI percentile
    },
    # Hard universe filters (in addition to the global ones in backend/app.py).
    "min_price": 3.0,
    "min_dollar_volume": 10_000_000,
    "min_avg_vol_10d": 50_000,
    "max_market_cap": None,                 # None = no cap
    "min_market_cap": 100_000_000,          # avoid sub-100M garbage
    "exclude_industries": ["Biotechnology"],
    "exclude_sectors": [],
    # Cap the union before passing to the technical agent.
    "final_cap": 80,
}

# ---------------------------------------------------------------------------
# Stage 2 — Technical Agent
# Goal: keep only stocks whose price/volume action looks like "can't keep it down".
# ---------------------------------------------------------------------------
TECHNICAL_AGENT = {
    "model": os.getenv("OLLAMA_MODEL_TECHNICAL", OLLAMA["default_model"]),
    "prompt_file": "technical.md",
    # Number of recent bars stored in features. We slice this down further when
    # building the prompt (see `bars_in_prompt`).
    "ohlc_window_days": 90,
    # How many recent bars are actually shown to the LLM per stock. The
    # computed feature pack already captures the long-horizon picture.
    "bars_in_prompt": 20,
    # How many tickers per LLM call. Smaller = faster + more reliable JSON.
    # 3 is a good local-LLM default; bump up for hosted models.
    "chunk_size": 3,
    # Pass-through scoring threshold (LLM returns score 0–10).
    "min_score": 6,
    # Hard caps so the LLM never sees too much.
    "max_candidates": 80,
    # Features pre-computed and shown to the LLM in compact tabular form.
    "features": [
        "ema_10", "ema_20", "sma_50", "sma_200",
        "atr20", "vol_vs_10d_avg",
        "dist_from_52w_high_pct", "dist_from_52w_low_pct",
        "contraction_score",   # see tools/price_data.py
        "gap_count_30d", "volspike_count_30d",
    ],
}

# ---------------------------------------------------------------------------
# Perplexity (used by Market News + Fundamental agents)
# `sonar` is the cheapest grounded model. Override per-agent if you want
# different quality/latency trade-offs.
# ---------------------------------------------------------------------------
PERPLEXITY = {
    "default_model": os.getenv("PERPLEXITY_MODEL", "sonar"),
    "timeout_s": 90,
    "max_tokens": 1200,
    # Web-search recency window for grounded queries: hour/day/week/month/year.
    "recency": "week",
}

# ---------------------------------------------------------------------------
# Stage 3 — Market News Agent
# Goal: identify the HOTTEST market themes/narratives right now.
# Independent of any specific stock list — this is the macro/narrative scan
# that the per-ticker stage downstream will match against.
# ---------------------------------------------------------------------------
MARKET_NEWS_AGENT = {
    # Perplexity model used to fetch grounded market commentary.
    "perplexity_model": os.getenv("PERPLEXITY_MODEL_MARKET", "sonar"),
    # Local LLM used to structure Perplexity's free-form answer into the
    # themes JSON the downstream stage expects.
    "ollama_model": os.getenv("OLLAMA_MODEL_MARKET", OLLAMA["default_model"]),
    "perplexity_prompt_file": "market_news_query.md",
    "themes_prompt_file": "themes.md",
    "lookback_days": 28,
    # Recency filter passed to Perplexity (hour/day/week/month). Use "month"
    # so earnings-driven cycle inflections (e.g. Intel print 2-3 weeks back)
    # are still in the search window. Tighter than "month" caused us to miss
    # the multi-quarter re-rating narratives we actually want.
    "recency": "month",
    # Token budget for the Perplexity response. The default 1200 was
    # truncating at theme 5-6; we want 10-15 themes per run, which needs
    # ~600-800 chars per theme block, so budget generously.
    "perplexity_max_tokens": 4000,
    "perplexity_timeout_s": 180,
}

# ---------------------------------------------------------------------------
# Stage 4 — Fundamental Agent (TICKER-SPECIFIC)
# For each survivor: pull recent material events via Perplexity, then ask
# Ollama to score how well those events fit the hot themes.
# ---------------------------------------------------------------------------
FUNDAMENTAL_AGENT = {
    # Perplexity for per-ticker material-events search.
    "perplexity_model": os.getenv("PERPLEXITY_MODEL_TICKER", "sonar"),
    # Local LLM for the theme-fit scoring step.
    "ollama_model": os.getenv("OLLAMA_MODEL_FUNDAMENTAL", OLLAMA["default_model"]),
    "ticker_query_prompt_file": "ticker_news_query.md",
    "match_prompt_file": "fundamental.md",
    "lookback_days": 14,
    "recency": "week",
    # Cap survivors handed to this stage (good for cheap testing). None = no cap.
    "max_survivors": None,
    # How many tickers per match-pass LLM call.
    "chunk_size": 2,
    # Final pass-through threshold (LLM returns conviction score 0–10).
    "min_score": 7,
}

# ---------------------------------------------------------------------------
# MCP
# ---------------------------------------------------------------------------
MCP = {
    "config_file": str(MCP_CONFIG_FILE),
    # If MCP fails to start, agents fall back to stub data so the pipeline
    # still completes (useful while iterating prompts offline).
    "graceful_fallback": True,
}
