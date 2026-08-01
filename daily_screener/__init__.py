"""Daily Shortlist Screening Agent.

Combines existing momentum screens (Top Returns / Vol Spike / Gappers) with
per-ticker Perplexity news enrichment and Claude-based judging on news
materiality + thematic fit. Every ticker in the daily universe is persisted
with a stage + reason + rationale so you can QA exactly why something was or
wasn't surfaced.
"""

__version__ = "0.1.0"
