"""Daily market brief generator.

Ingests Benzinga news for the full sector/theme ticker universe, summarizes
with Perplexity on the article bodies, and produces a single morning brief.
A small Perplexity web probe covers forward calendar events.

This is intentionally NOT part of `daily_screener`: that pipeline picks
*tickers* to trade today; this one builds *context* about what the market
is actually doing.
"""
