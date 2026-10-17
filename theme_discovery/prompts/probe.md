Today is {today}. A market theme is driving several US stocks higher:

Theme: {name}
Description: {description}
Why it is a trend: {secular}
Stocks already identified: {known}

Find OTHER US-listed companies (NYSE / NASDAQ, market cap above $2B) that make money from this same theme. Look across the whole chain: direct competitors, suppliers, customers, adjacent products, companies in other industries with real exposure. Include names whether or not their stock has moved yet. Only include companies with concrete, current exposure you can point to (revenue, contracts, products, guidance). Do not include the stocks already identified. Do not pad the list: return fewer names if that is all there is. At most {max_added}.

Return ONLY a JSON object, no prose before or after:

{{
  "stocks": [
    {{"ticker": "XXX", "company": "name", "why": "one sentence: how this company makes money from the theme, with a specific fact", "source": "url"}}
  ]
}}
