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
OUTPUTS_DIR = PACKAGE_DIR / "outputs"
USER_DATA_DIR = PROJECT_ROOT / "user_data"
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
# Perplexity
# ---------------------------------------------------------------------------
# `sonar-pro` digs deeper across sources; the cheaper `sonar` is fine for
# the synthesis pass since that's pure summarization on text we provide.

PROBE_MODEL = os.getenv("MARKET_BRIEF_PROBE_MODEL", "sonar-pro")
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
