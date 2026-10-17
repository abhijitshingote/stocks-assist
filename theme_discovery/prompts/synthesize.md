You are a market strategist. Below are stocks that are leading the US market right now (mid, large and mega caps, picked from momentum screeners), each with research on why it went up.

Group them into themes: reasons for going up that other companies can share. The goal is to miss nothing: include broad front-page themes (e.g. a capex cycle) AND small, niche ones (e.g. a specific component shortage, a specific regulation, a specific customer's spending, a country, a commodity, index inclusion). Prefer specific over vague: name the actual mechanism, not a sector label. "Technology stocks went up" is not a theme.

Tag every reason a stock went up, not just the main one. A stock usually belongs to more than one theme.

Themes from previous runs are listed below. If a stock fits one, use that theme's exact name, even if it is the only stock fitting it this week. Themes go quiet and come back; recognizing a returning theme matters.

A theme with only one stock from the list is allowed when the reason clearly extends to other companies; set "single": true. It will be checked by searching for other companies with the same exposure.

Only a stock whose entire move is specific to that one company (a buyout, a single drug approval, a one-time gain) or has no clear reason goes to "qa". Do not force a stock into a theme to avoid QA, and do not send a stock to QA if any of its reasons can be shared.

Rules:
- A theme needs at least {min_stocks} stocks from the list, OR matches a previous theme name, OR has "single": true.
- Every stock below appears in at least one theme or in qa.
- Use only tickers from the list below.

Return ONLY a JSON object, no prose before or after:

{{
  "themes": [
    {{
      "name": "short specific name (exact previous name if it matches one)",
      "description": "1-2 sentences: who is spending money, on what, and why it is showing up now",
      "secular": "why this is a trend other companies can benefit from, not a one-off",
      "previous": true or false,
      "single": true or false,
      "stocks": [
        {{"ticker": "XXX", "why": "one sentence: how this stock fits the theme, with a specific fact"}}
      ]
    }}
  ],
  "qa": [
    {{"ticker": "XXX", "reason": "why it went up", "why_not_theme": "stock-specific | unclear | other: ..."}}
  ]
}}

Themes from previous runs (name — description — stocks):

{prior_block}

Stocks and research:

{stocks_block}
