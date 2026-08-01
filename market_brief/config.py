"""Configuration for the daily market brief pipeline.

Keep all knobs (sectors, models, concurrency, output paths) here so the
prompts stay small and the orchestrator stays mechanical.
"""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
USER_DATA_DIR = PROJECT_ROOT / "user_data"
# Run artifacts live under user_data/ so the auto_commit.sh backup (which
# git-pushes user_data/) preserves them across app/container resets. The old
# location (market_brief/outputs/) is ephemeral repo state; this one is
# treated as preservable user data.
OUTPUTS_DIR = USER_DATA_DIR / "market_brief"
THEMES_FILE = USER_DATA_DIR / "themes.json"

# ---------------------------------------------------------------------------
# Sectors
# ---------------------------------------------------------------------------
# Tech-heavy seed list to match the existing themes.json bias. Each entry
# becomes its own pair of Perplexity probes (overview + catalyst). Edit
# freely — the pipeline reads this list at runtime.

SECTORS: list[dict[str, str]] = [
    {
        "name": "Semiconductors",
        "desc": (
            "All chip names — design, foundry, equipment, memory, "
            "analog, RF, power. AI-infra capex drives most of this."
        ),
    },
    {
        "name": "AI Infrastructure",
        "desc": (
            "Hyperscaler capex, GPU/ASIC deployments, networking, "
            "optical interconnect, datacenter power, liquid cooling."
        ),
    },
    {
        "name": "Software & SaaS",
        "desc": (
            "Application software, infra software, cybersecurity, "
            "developer tools, AI-application layer, vertical SaaS."
        ),
    },
    {
        "name": "Internet & Platforms",
        "desc": (
            "Mega-cap platforms (MSFT, GOOGL, META, AMZN, AAPL), "
            "ad tech, e-commerce, marketplaces."
        ),
    },
    {
        "name": "Communications & Networking",
        "desc": (
            "Networking gear, telecom equipment, optical networking, "
            "5G/6G, satellite — supplier read-through to AI buildout."
        ),
    },
]

# ---------------------------------------------------------------------------
# Themes
# ---------------------------------------------------------------------------
# Use the user's curated themes.json. Each theme is also probed with its
# own focused query (the ticker list in the theme is included in the
# prompt to give Perplexity concrete names to investigate).

USE_USER_THEMES = True
# Optional cap to avoid runaway fan-out if themes.json gets long.
MAX_THEMES = 20

# Theme discovery (``python -m market_brief.discover_themes``) writes proposals
# here; themes.json is updated only with ``--apply`` after you set approved: true.
THEME_DISCOVERY_DIR = USER_DATA_DIR / "theme_discovery"
THEME_DISCOVERY_MIN_ARTICLES = 2
# Set MARKET_BRIEF_DISCOVER_THEMES=1 to write proposals.json after each brief run.
DISCOVER_THEMES_AFTER_RUN = os.getenv("MARKET_BRIEF_DISCOVER_THEMES", "").lower() in (
    "1",
    "true",
    "yes",
)

# ---------------------------------------------------------------------------
# Catalyst angles
# ---------------------------------------------------------------------------
# These go into the catalyst probe prompt so Perplexity knows what
# specifically to dig for beyond the headline summary.

CATALYST_ANGLES: list[dict[str, str]] = [
    {
        "key": "earnings",
        "label": "Earnings reactions & guide changes (with read-throughs to peers)",
    },
    {
        "key": "supply_chain",
        "label": (
            "Supply-chain / customer / contract wins — design wins, "
            "hyperscaler orders, foundry allocation, supplier reads"
        ),
    },
    {
        "key": "product",
        "label": (
            "Product launches, partnership announcements, design "
            "wins, technology roadmap milestones"
        ),
    },
    {
        "key": "regulatory",
        "label": (
            "Regulatory / legal / policy catalysts — export controls, "
            "antitrust, tariffs, FDA, FTC, DOJ"
        ),
    },
]

# ---------------------------------------------------------------------------
# Benzinga ingest
# ---------------------------------------------------------------------------

# Ticker universe for per-symbol Benzinga pulls (DB screener slices, not themes.json).
TICKER_UNIVERSE_TOP_N = 10
TICKER_UNIVERSE_CAP_BUCKETS: tuple[str, ...] = ("mega", "large", "mid_small")
# First claim wins when a symbol qualifies for multiple screens (see dedupe in screener_universe).
TICKER_UNIVERSE_SLICE_PRIORITY: tuple[str, ...] = (
    "r1d",
    "vol_spike_5d",
    "main_view_ti65",
)
VOLSPIKE_GAPPER_WINDOW_DAYS = 5

# Mirrors backend/app.py MARKET_CAP_CATEGORIES (micro excluded from brief universe).
MARKET_CAP_CATEGORIES: dict[str, dict[str, int | None]] = {
    "micro": {"min": 0, "max": 200_000_000},
    "small": {"min": 200_000_000, "max": 2_000_000_000},
    "mid": {"min": 2_000_000_000, "max": 20_000_000_000},
    "large": {"min": 20_000_000_000, "max": 100_000_000_000},
    "mega": {"min": 100_000_000_000, "max": None},
    # Brief ticker universe: small + mid caps in one bucket (micro excluded).
    "mid_small": {"min": 200_000_000, "max": 20_000_000_000},
}

# Global liquidity + industry filters (mirrors scanner UI / daily_screener).
LIQUIDITY_MIN_AVG_VOL_10D = 50_000
LIQUIDITY_MIN_DOLLAR_VOLUME = 10_000_000
LIQUIDITY_MIN_PRICE = 3.0
EXCLUDED_INDUSTRIES = frozenset({"Biotechnology"})

# Deprecated: ingest uses trading_calendar (5:00 AM ET anchor). Kept for empty-topic copy fallback.
NEWS_WINDOW_HOURS = 24
# Per-ticker pulls start this many hours before the general/channel window (5 AM ET anchor).
TICKER_NEWS_EXTRA_HOURS = 24
PER_TICKER_LIMIT = 25
GENERAL_NEWS_LIMIT = 100
# Extra channel-scoped pulls (Polygon ``channels=``).
# Run ``python -m market_brief.discover_channels`` to reprobe; comment out to save API calls.
# Empirical probe 2026-05-24: all below returned rows; ``wiim`` returned 0 (omit).
GENERAL_CHANNEL_FETCHES: list[tuple[str, int]] = [
    ("news", 50),
    ("markets", 50),
    ("equities", 50),
    # ("politics", 50),
    ("tech", 50),
    # ("global", 50),
    # ("government", 50),
    # ("general", 50),
    ("commodities", 50),
    ("earnings", 50),
    ("movers", 50),
    ("macro economic events", 50),
    # ("cryptocurrency", 50),
    # ("etfs", 50),
    # ("health care", 50),
    # ("large cap", 50),
    # ("sector etfs", 50),
    # ("top stories", 50),
]
# Per-run QA funnel markdown (``qa_funnel.md``). Off by default (storage).
QA_LOG_ENABLED = os.getenv("MARKET_BRIEF_QA_LOG", "").lower() in (
    "1",
    "true",
    "yes",
)
INGEST_CONCURRENCY = 5
ARTICLE_RETENTION_DAYS = 7
# Rolling Polygon refresh window for ``refresh_benzinga_articles`` (market news + brief ingest).
REFRESH_LOOKBACK_DAYS = 3
REFRESH_API_LIMIT = 1000

# Summarize + brief input for stories that matched no theme or sector bucket.
UNASSIGNED_TOPIC_NAME = "Unassigned (no theme/sector)"
UNASSIGNED_TOPIC_DESC = (
    "Benzinga articles in the ingest window that did not match any "
    "theme or sector ticker assignment — still summarized for the brief."
)

# Summarize stage runs these topics first (exact names from themes.json / SECTORS).
# Unassigned always runs last. Remaining topics keep load_topics() order among themselves.
SUMMARIZE_TOPIC_PRIORITY: list[str] = [
    "AI Compute",
    "Memory & Interconnect",
    "Optical & Photonics",
]

# ---------------------------------------------------------------------------
# Summarize backend — Perplexity (chunked topics) or Ollama (per-article snippets)
# ---------------------------------------------------------------------------

SUMMARIZE_BACKEND = os.getenv("MARKET_BRIEF_SUMMARIZE_BACKEND", "perplexity").lower()
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.getenv("MARKET_BRIEF_OLLAMA_MODEL", "gemma4:latest")
OLLAMA_NUM_PREDICT = 8192
OLLAMA_NUM_CTX = 8192
OLLAMA_TEMPERATURE = 0.15
OLLAMA_TIMEOUT_SECONDS = 600

# ---------------------------------------------------------------------------
# Perplexity — topic summaries + synthesis (news from Benzinga, not web probes)
# ---------------------------------------------------------------------------

TOPIC_SUMMARY_MODEL = os.getenv("MARKET_BRIEF_TOPIC_SUMMARY_MODEL", "sonar-pro")
TOPIC_SUMMARY_MAX_TOKENS = 3500
TOPIC_SUMMARY_TEMPERATURE = 0.15
TOPIC_SUMMARY_TIMEOUT_SECONDS = 120
# Split large article bundles so full bodies fit in context.
CHUNK_MAX_CHARS = 55_000

WATCH_PROBE_MODEL = os.getenv("MARKET_BRIEF_WATCH_MODEL", "sonar-pro")
WATCH_PROBE_MAX_TOKENS = 2000
WATCH_PROBE_TEMPERATURE = 0.1
WATCH_PROBE_TIMEOUT_SECONDS = 90

# Legacy web-search probes (disabled; kept for env overrides / experiments).
PROBE_MODEL = os.getenv("MARKET_BRIEF_PROBE_MODEL", "sonar-pro")
USE_WEB_PROBES = os.getenv("MARKET_BRIEF_USE_WEB_PROBES", "").lower() in (
    "1",
    "true",
    "yes",
)
# Synthesis used to default to plain `sonar`, but it ground the rich
# probe outputs (specific numbers, sources, moves) into bland sell-side
# prose — losing the actionable detail. `sonar-pro` preserves quoted
# numbers and source names. We only do 2 synth calls per run, so cost
# impact is small (~$0.10–0.20).
SYNTH_MODEL = os.getenv("MARKET_BRIEF_SYNTH_MODEL", "sonar-pro")

# Per-probe token budget. Higher than daily_screener because we want
# stock-specific detail (named tickers + 1-2 sentences each), not just a
# headline.
PROBE_MAX_TOKENS = 2200
# Bumped from 4000 → 8000 because the brief was hitting the cap and
# truncating mid-sentence, dropping ~half the sectors and the
# "Stock-specific callouts" + "Watch tomorrow" sections entirely.
SYNTH_MAX_TOKENS = 8000

# Probe kinds to run per topic. The catalyst probe was originally
# designed to chase filings/press releases beyond what the overview
# captures, but in practice the overview probe surfaces the same
# catalysts (with better recall) AND the strict catalyst-probe
# constraints (must have ticker + filing + number + named source + date
# in 24-72h) cause Perplexity to short-circuit to "NO MATERIAL FRESH
# CATALYST" without doing real web work. Disabling halves API spend
# with no observed loss in detail. Re-enable here if behavior changes.
ENABLED_PROBE_KINDS = ("overview",)

# Lower temperature → less hallucinated tickers.
PROBE_TEMPERATURE = 0.1
SYNTH_TEMPERATURE = 0.2

PROBE_TIMEOUT_SECONDS = 90
SYNTH_TIMEOUT_SECONDS = 120

# Match daily_screener's sustained-throughput cap on `sonar-pro`.
PROBE_CONCURRENCY = 3
