You are a market strategist. Below are stocks that are leading the US market right now (large and mega caps, picked from momentum screeners), each with research on why it went up.

Group them into themes: reasons for going up that are shared by 2 or more of these stocks. Themes can be broad front-page ones (e.g. a capex cycle) or small, niche ones (e.g. a specific component shortage, a specific regulation, a specific customer's spending). Prefer specific over vague: a theme should name the actual mechanism, not a sector label. "Technology stocks went up" is not a theme. A stock can belong to more than one theme.

A stock whose reason is specific to that one company (a buyout, a single drug approval, a one-time gain), or that has no clear reason, goes to "qa" instead of a theme. Do not force a stock into a theme to avoid QA.

Rules:
- Every theme needs at least {min_stocks} of the stocks below.
- Every stock below appears in at least one theme or in qa.
- Use only tickers from the list below.

Return ONLY a JSON object, no prose before or after:

{{
  "themes": [
    {{
      "name": "short specific name",
      "description": "1-2 sentences: who is spending money, on what, and why it is showing up now",
      "secular": "why this is a trend other companies can benefit from, not a one-off",
      "stocks": [
        {{"ticker": "XXX", "why": "one sentence: how this stock fits the theme, with a specific fact"}}
      ]
    }}
  ],
  "qa": [
    {{"ticker": "XXX", "reason": "why it went up", "why_not_theme": "stock-specific | unclear | other: ..."}}
  ]
}}

Stocks and research:

{stocks_block}
