"""Market brief pipelines (Perplexity research + Anthropic synthesis, published to ``app.briefs``).

``perplexity_brief``: daily pre-market brief. ``px_losers``: R1D losers brief.
``narratives``: what-moved-markets events across published daily briefs.

This is intentionally NOT part of `daily_screener`: that pipeline picks
*tickers* to trade today; this one builds *context* about what the market
is actually doing.
"""
