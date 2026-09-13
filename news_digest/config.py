"""Knobs for the news digest pipeline.

Cost model, measured on the 2026-09-05..09-11 week:

    fetch        3,494 rows -> 3,394 after dedupe          $0
    prefilter    3,394 -> ~2,160                           $0
    screen       Haiku over ~119k tokens -> ~250 ids       ~$0.14
    select       Opus over ~250 titles  -> ~70 ids         ~$0.05
    brief        Opus over ~70 bodies   -> markdown        ~$0.35
                                                    total  ~$0.55

The dominant lever is the window. A 30d window is 18k titles (~640k tokens) and
costs roughly 6x this.
"""

from __future__ import annotations

import os
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
OUTPUTS_DIR = PROJECT_ROOT / "user_data" / "news_digest"

# --- window ---
# One trading week, aligned Saturday 00:00 ET -> the following Saturday 00:00 ET,
# so a run always covers a whole Sat..Fri span rather than a rolling cut.
WEEK_ANCHOR_WEEKDAY = 5  # Saturday, per datetime.weekday()
TIMEZONE = "America/New_York"

# --- fetch ---
API_PAGE_LIMIT = 1000
API_MAX_PAGES = 30
FETCH_TIMEOUT_SECONDS = 120

# --- screen (call 1): cheap pass over every surviving title ---
# Haiku's 200k context fits a week comfortably (~119k tokens after prefilter);
# the 1M-window requirement only existed because of the 30d universe.
SCREEN_MODEL = os.getenv("NEWS_DIGEST_SCREEN_MODEL", "claude-haiku-4-5")
SCREEN_TARGET_N = int(os.getenv("NEWS_DIGEST_SCREEN_N", "250"))
SCREEN_MAX_TOKENS = 16_384
# Hard ceiling; a week should never approach this. Guards against a bad --window.
SCREEN_MAX_INPUT_TOKENS = int(os.getenv("NEWS_DIGEST_SCREEN_MAX_INPUT", "180000"))

# --- select (call 2): Opus makes the final cut from the screened shortlist ---
# Input is only ~250 titles (~10k tokens), so Opus judgement here costs cents.
SELECT_MODEL = os.getenv("NEWS_DIGEST_SELECT_MODEL", "claude-opus-4-6")
SELECT_TARGET_N = int(os.getenv("NEWS_DIGEST_SELECT_N", "70"))
SELECT_MAX_TOKENS = 8_192

# --- brief (call 3): bodies of the final cut -> one markdown recap ---
BRIEF_MODEL = os.getenv("NEWS_DIGEST_BRIEF_MODEL", "claude-opus-4-6")
# Median kept body is ~1,200 chars, p90 ~3,300. A 2,000 cap only bites on long
# feature pieces and roughly halves brief input cost versus the old 6,000.
BRIEF_BODY_CHARS = 2_000
BRIEF_BODY_CHAR_BUDGET = 400_000
BRIEF_MAX_TOKENS = 16_384
