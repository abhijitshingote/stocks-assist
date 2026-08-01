"""Configuration for the R1D losers brief pipeline."""

from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
USER_DATA_DIR = PROJECT_ROOT / "user_data"
OUTPUTS_DIR = USER_DATA_DIR / "market_brief_losers"

# Bottom N by dr_1 per cap bucket (micro excluded).
LOSERS_TOP_N: dict[str, int] = {
    "mega": 15,
    "large": 10,
    "mid_small": 10,
}
LOSERS_CAP_BUCKETS: tuple[str, ...] = ("mega", "large", "mid_small")

# Shared with market_brief (liquidity, cap ranges, ingest knobs).
from market_brief import config as _mb  # noqa: E402

MARKET_CAP_CATEGORIES = _mb.MARKET_CAP_CATEGORIES
LIQUIDITY_MIN_AVG_VOL_10D = _mb.LIQUIDITY_MIN_AVG_VOL_10D
LIQUIDITY_MIN_DOLLAR_VOLUME = _mb.LIQUIDITY_MIN_DOLLAR_VOLUME
LIQUIDITY_MIN_PRICE = _mb.LIQUIDITY_MIN_PRICE
EXCLUDED_INDUSTRIES = _mb.EXCLUDED_INDUSTRIES
PER_TICKER_LIMIT = _mb.PER_TICKER_LIMIT
TICKER_NEWS_EXTRA_HOURS = _mb.TICKER_NEWS_EXTRA_HOURS
INGEST_CONCURRENCY = _mb.INGEST_CONCURRENCY
ARTICLE_RETENTION_DAYS = _mb.ARTICLE_RETENTION_DAYS
