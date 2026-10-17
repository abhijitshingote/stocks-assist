"""Knobs for the theme discovery pipeline."""

import os
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
PROMPTS_DIR = PACKAGE_DIR / "prompts"
# Run working dir (scratch); themes.json + run_summary.json are published to
# app.pipeline_runs (pipeline="theme_discovery").
OUTPUTS_DIR = PROJECT_ROOT / "user_data" / "theme_discovery"

BACKEND_URL = os.getenv("THEME_DISCOVERY_BACKEND_URL", "http://localhost:5000")

# Candidates come from the 4 screeners behind /api/WeeklyReview.
CAP_BUCKETS = ("mid", "large", "mega")
EXCLUDE_INDUSTRIES = ("Biotechnology",)

# (list id, weekly-review source, rank field, top N per cap bucket, extra filter)
# extra filter: "in_5d" / "in_20d" pick one Top Returns list; "still_up" = dr_20 > 0 or dr_60 > 0.
PICKS = (
    ("fastrs", "fastrs", "adjusted_rs_score", 12, None),
    ("strong", "strong", "adjusted_ti65", 12, None),
    ("top5d", "top520", "adjusted_dr_5", 8, "in_5d"),
    ("top20d", "top520", "adjusted_dr_20", 8, "in_20d"),
    ("vsg90", "vsg90", "adjusted_event_return", 12, "still_up"),
)
MAX_PER_INDUSTRY = None  # None = no limit

# News (one Perplexity call per candidate)
NEWS_MODEL = os.getenv("THEME_DISCOVERY_NEWS_MODEL", "sonar-pro")
NEWS_MAX_TOKENS = 1800
NEWS_CONCURRENCY = 6

# Grouping (one Claude call)
SYNTH_MODEL = os.getenv("THEME_DISCOVERY_SYNTH_MODEL", "claude-opus-4-6")
SYNTH_LOGICAL = "claude-opus-4-6"
SYNTH_MAX_TOKENS = 24_000
MIN_STOCKS_PER_THEME = 2
# Prior runs' themes fed to grouping, so a lone stock can join a known theme.
PRIOR_RUNS = 8

# A one-stock theme is kept if the probe finds this many added stocks that are also up.
SINGLE_CONFIRM_MIN = 2
SINGLE_CONFIRM_DR_20 = 10.0  # or
SINGLE_CONFIRM_DR_60 = 20.0

# Probe (one Perplexity call per theme)
PROBE_MODEL = os.getenv("THEME_DISCOVERY_PROBE_MODEL", "sonar-pro")
PROBE_MAX_TOKENS = 2500
PROBE_MAX_ADDED = 15
PROBE_MIN_MARKET_CAP = 2_000_000_000  # mid cap and up
PROBE_CONCURRENCY = 4
