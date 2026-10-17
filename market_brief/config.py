"""Configuration for the market brief pipelines (``perplexity_brief``, ``px_losers``, ``narratives``).

Search plan, prompts and model choices live in ``px_prompts.py`` / ``perplexity_brief.py``;
this module holds paths and the DB ticker-universe knobs.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
# Run working dirs (status.json, logs, research) — scratch; finished briefs go to app.briefs.
USER_DATA_DIR = PROJECT_ROOT / "user_data"

# ---------------------------------------------------------------------------
# Ticker universe (DB screener slices)
# ---------------------------------------------------------------------------

TICKER_UNIVERSE_TOP_N = 10
TICKER_UNIVERSE_CAP_BUCKETS: tuple[str, ...] = ("mega", "large", "mid_small")
# First claim wins when a symbol qualifies for multiple screens (see dedupe in screener_universe).
TICKER_UNIVERSE_SLICE_PRIORITY: tuple[str, ...] = (
    "r1d",
    "vol_spike_5d",
    "main_view_ti65",
)
VOLSPIKE_GAPPER_WINDOW_DAYS = 5

# R1D losers brief (market_brief.px_losers): bottom N by dr_1 per cap bucket (micro excluded).
LOSERS_TOP_N: dict[str, int] = {
    "mega": 15,
    "large": 10,
    "mid_small": 10,
}
LOSERS_CAP_BUCKETS: tuple[str, ...] = ("mega", "large", "mid_small")

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
