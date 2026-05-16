"""Central configuration for the daily screener pipeline.

All thresholds, model choices, and scoring knobs live here so they can be
tuned during QA without code changes.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
OUTPUTS_DIR = PACKAGE_DIR / "outputs"
PROMPTS_DIR = PACKAGE_DIR / "prompts"
USER_DATA_DIR = PROJECT_ROOT / "user_data"

ABI_WATCHLIST_FILE = USER_DATA_DIR / "abi_watchlist.json"
# Explicit thumbs-down list. Separate from the watchlist on purpose:
# watchlist.stars == 0 means "not yet rated" (neutral), NOT a veto.
ABI_DISLIKES_FILE = USER_DATA_DIR / "abi_dislikes.json"
THEMES_FILE = USER_DATA_DIR / "themes.json"
# Per-date, per-ticker verdict corrections. Populated by the UI; consumed
# by the judge (Stage 5) as in-context calibration examples.
DAILY_SCREENER_FEEDBACK_FILE = USER_DATA_DIR / "daily_screener_feedback.json"

# ---------------------------------------------------------------------------
# Universe selection (Stage 1)
# ---------------------------------------------------------------------------

# How many tickers to pull from each return bucket (matches existing endpoints).
TOP_PERFORMANCE_PER_BUCKET = 30

# Top N from `main_view` using the same ordering as the Main View page (ti65 desc,
# default "All" market-cap tab — no cap filter). Merged with other universe sources.
MAIN_VIEW_TOP_N = 50

# Only include vol-spike/gapper rows whose `last_event_date` is within this many
# calendar days of the run date. The underlying table holds the last 365 days of
# events, but we only want fresh catalysts in the universe.
VOLSPIKE_GAPPER_WINDOW_DAYS = 10

# Apply existing global liquidity filters (mirrors backend/app.py).
LIQUIDITY_MIN_AVG_VOL_10D = 50_000
LIQUIDITY_MIN_DOLLAR_VOLUME = 10_000_000
LIQUIDITY_MIN_PRICE = 3.0

# Exclude tickers that appear in the dedicated dislikes file
# (`user_data/abi_dislikes.json`). This is the ONLY source of vetoes.
# Watchlist `stars: 0` is treated as "unrated" / neutral and is NOT a veto.
EXCLUDE_DISLIKED_TICKERS = True

# Industries to exclude (mirrors backend GLOBAL_EXCLUDE).
EXCLUDED_INDUSTRIES = {"Biotechnology"}

# ---------------------------------------------------------------------------
# Momentum scoring (Stage 2)
# ---------------------------------------------------------------------------

# Composite momentum score is built from the following weighted factors (0-100).
MOMENTUM_FACTOR_WEIGHTS = {
    "multi_screen": 20,        # appearing in 2+ source screens
    "recent_event": 20,        # vol spike or gap within last 5 trading days
    "dr5_atr_norm": 20,        # ATR-normalized 5-day return strength
    "trend_alignment": 15,     # dr_20 and dr_60 both positive and aligned
    "vol_vs_avg": 10,          # vol_vs_10d_avg above threshold
    "rsi_mktcap": 15,          # RSI MktCap percentile
}

# Days back from latest_event_date to count as "recent".
RECENT_EVENT_WINDOW_DAYS = 5

# Keep tickers with momentum score >= this threshold.
MOMENTUM_THRESHOLD = 50

# Hard cap on tickers that proceed to LLM enrichment (cost guardrail).
MAX_LLM_GRADE = 60

# ---------------------------------------------------------------------------
# News enrichment (Stage 4)
# ---------------------------------------------------------------------------

# `sonar-pro` is Perplexity's search-grade tier. The cheaper `sonar` tier
# routinely defaults to the freshest press release (e.g. an earnings 8-K) and
# misses longer-running rumor/adoption/analyst-note catalysts that drive big
# small-mid-cap moves. The cost delta at ~60 tickers/day is modest and the
# recall gain is material. Override via env if you want to A/B.
PERPLEXITY_MODEL = os.getenv("DAILY_SCREENER_PERPLEXITY_MODEL", "sonar-pro")
PERPLEXITY_TIMEOUT_SECONDS = 60
# `sonar-pro` has a tighter rate limit than `sonar`. With a 2-pass design
# (pass 1 + optional verification) concurrency=8 reliably overruns it; cap
# to 3 so we sustain throughput instead of burning retries.
NEWS_CONCURRENCY = 3
NEWS_RETRY_ATTEMPTS = 3

# Verification pass: after pass-1, if the candidate appears to be a big move
# without a satisfying catalyst (or pass-1 surfaced specific named leads
# worth confirming), we run a focused second query.
NEWS_VERIFY_ENABLED = True

# Hard trigger: pass-1 said "no material news" but the move is too big to
# be unexplained.
NEWS_VERIFY_DR5_NO_CATALYST_PCT = 25.0

# Soft trigger: pass-1 said "yes material news" but its drivers look weak
# relative to the size of the move (e.g. a low-conviction earnings print or
# pure technical/momentum framing on a +50%/5d stock). Fires when:
#   - 5d return >= NEWS_VERIFY_DR5_WEAK_CATALYST_PCT, AND
#   - pass-1 drivers/summary contain only soft language (technical breakout,
#     momentum chase, sympathy, no named customer/contract/regulatory event).
NEWS_VERIFY_DR5_WEAK_CATALYST_PCT = 30.0

# Always verify if pass-1 surfaced a likely-rumor lead (Kuo/analyst note,
# unnamed hyperscaler, "potential adoption", patent analysis, etc).
NEWS_VERIFY_ON_RUMOR_LEAD = True

# Cap the per-ticker token budget for the verification pass.
NEWS_VERIFY_MAX_TOKENS = 1200

# ---------------------------------------------------------------------------
# Theme vector (Stage 3)
# ---------------------------------------------------------------------------

# Weight applied to user-taste themes vs hot-market themes vs curated themes.
THEME_WEIGHTS = {
    "user_taste": 3.0,    # themes inferred from watchlist notes - highest signal
    "curated": 2.0,       # user-curated themes.json
    "hot_market": 1.5,    # hot narratives from this week's market scan
}

# How many days a hot-market scan stays valid before re-running.
HOT_MARKET_CACHE_DAYS = 1

# LLM used for Stage-3 user-theme extraction from watchlist notes. Defaults to
# Perplexity sonar (no web search needed for this; it's pure extraction). Swap
# to "claude-haiku" or "claude-sonnet" via env if you want.
USER_THEME_EXTRACTION_MODEL = os.getenv(
    "DAILY_SCREENER_USER_THEME_MODEL", "sonar"
)

# ---------------------------------------------------------------------------
# Judge (Stage 5)
# ---------------------------------------------------------------------------

# Default judge model. Models prefixed "claude-" route to Anthropic; everything
# else routes to Perplexity. The judge's input already contains the news (from
# Stage 4), so we don't need web-search capability — a non-search model is
# fine. We keep `sonar` as the default (Anthropic credits may not always be
# available); override to `claude-haiku` or `claude-sonnet` when you want to
# decouple Stage 5's rate budget from Stage 4's Perplexity budget.
JUDGE_MODEL = os.getenv("DAILY_SCREENER_JUDGE_MODEL", "sonar")

# Optional fallback if the primary model fails. Kept for future use; the
# current pipeline does not automatically fall back.
JUDGE_FALLBACK_MODEL = os.getenv("DAILY_SCREENER_JUDGE_FALLBACK", "sonar-pro")
JUDGE_MAX_TOKENS = 1500
JUDGE_TIMEOUT_SECONDS = 60
# Lowered from 6 to 3 to stay well below Perplexity's per-minute window when
# Stage 4 (also Perplexity) has just completed. With Stage 4 at concurrency=3
# and a small inter-stage cooldown, Stage 5 can run at 3 in parallel without
# tripping rate limits or 401-on-overage behavior.
JUDGE_CONCURRENCY = 3

# How many nearest watchlist examples to include in the judge prompt per ticker.
JUDGE_NEAREST_EXAMPLES = 3

# Feedback (verdict overrides) calibration. The judge sees the trader's last
# N corrections so it can recalibrate going forward. This is the cheap,
# in-context alternative to full fine-tuning - takes effect on the next run.
#
# Set FEEDBACK_LOOKBACK_DAYS=0 to include all feedback regardless of age.
FEEDBACK_LOOKBACK_DAYS = 30
FEEDBACK_MAX_EXAMPLES = 20

# Composite score weights. Must sum to 1.0.
SCORE_WEIGHTS = {
    "momentum": 0.4,
    "news": 0.3,
    "theme": 0.3,
}

# Verdict thresholds on the composite score.
PICK_COMPOSITE_MIN = 75
WATCH_COMPOSITE_MIN = 55

# ---------------------------------------------------------------------------
# Concurrency / retry
# ---------------------------------------------------------------------------

REQUEST_RETRY_BACKOFF_SECONDS = 5
