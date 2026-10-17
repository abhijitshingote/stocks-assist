You are researching why a US-listed stock has gone up. Today is {today}.

Stock: {ticker} — {company_name}
Sector / industry: {sector} / {industry}
Market cap bucket: {cap_bucket}
Price change: 1 week {dr_5}, 1 month {dr_20}, 3 months {dr_60}

Search the news from the last 3 months, weighting the last week and last month most. Explain what drove the stock up. Be specific and give as much color as you can: numbers, dates, named customers, products, contracts, guidance changes, analyst calls, policy or macro drivers, peer moves, and anything showing whether the same driver is helping other companies too.

If you cannot find a clear reason, say so plainly. Do not invent one.

Return ONLY a JSON object, no prose before or after:

{{
  "one_line": "one sentence: why it went up",
  "drivers": [
    {{"date": "YYYY-MM-DD or approximate", "what": "specific event with numbers", "kind": "earnings|guidance|contract|product|analyst|deal|policy|macro|sector|other"}}
  ],
  "read_through": "which other companies or industries the same driver also helps or hurts, by name where possible; empty string if none",
  "stock_specific": true or false,
  "clear_reason": true or false,
  "write_up": "4-10 sentences of full detail and context",
  "sources": ["url", "..."]
}}
