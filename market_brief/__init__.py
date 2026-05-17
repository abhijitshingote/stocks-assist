"""Daily market brief generator.

A separate, deeper-than-headlines pipeline that uses Perplexity to probe
sectors and themes for stock-specific catalysts (earnings reads,
supply-chain wins, product launches, regulatory events) and produces a
single morning brief.

This is intentionally NOT part of `daily_screener`: that pipeline picks
*tickers* to trade today; this one builds *context* about what the market
is actually doing.
"""
