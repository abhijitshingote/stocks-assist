You are a market strategist doing a second pass. Below are stocks leading the US market, the research on why each went up, and the themes already found in a first pass.

Find themes the first pass MISSED. Go layer by layer and check each one against every stock:

1. Components and supply chain: a specific part, material or capacity that is short or ramping (e.g. a type of memory, optics, power equipment, a chemical).
2. Inputs and commodities: oil, gas, metals, power prices, crops, crypto prices.
3. Geography: a country, region or currency driving several names (elections, stimulus, trade deals, tariffs).
4. Policy and regulation: a specific law, rule, approval pathway, subsidy, government contract or budget.
5. Financing and rates: rate moves, credit conditions, IPO or M&A waves, buybacks, balance-sheet repair.
6. A specific customer or buyer: one company's or one industry's spending lifting its suppliers.
7. Events and flows: index inclusion, spin-offs, activist campaigns, short squeezes across a group.
8. Business-model shifts: pricing changes, new product categories, a technology change spreading across companies.

Also add stocks to themes that already exist when a stock fits but was left out.

A new theme needs at least {min_stocks} stocks from the list, OR matches a previous-run theme name (use the exact name), OR has "single": true when only one stock fits but the reason clearly extends to other companies. Do not repeat themes that already exist under a new name. Use only tickers from the list.

Return ONLY a JSON object, no prose before or after:

{{
  "new_themes": [
    {{
      "name": "short specific name",
      "layer": "which layer above",
      "description": "1-2 sentences: who is spending money, on what, and why it is showing up now",
      "secular": "why this is a trend other companies can benefit from",
      "previous": true or false,
      "single": true or false,
      "stocks": [{{"ticker": "XXX", "why": "one sentence with a specific fact"}}]
    }}
  ],
  "additions": [
    {{"theme": "exact name of an existing theme", "ticker": "XXX", "why": "one sentence with a specific fact"}}
  ]
}}

Themes from previous runs (name — description — stocks):

{prior_block}

Themes found in the first pass (name — stocks):

{found_block}

Stocks and research:

{stocks_block}
