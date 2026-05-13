You are a market strategist scanning the US equities market for the dominant
narratives that are moving stocks **this week** (latest 5-10 trading days).

Identify 6-12 dominant themes / narratives. For each:

- A short **tag** (kebab-case, e.g. `ai-hyperscaler-deals`, `ai-power-buildout`,
  `optical-networking`, `rare-earths`, `defense-drones`, `glp-1`, `bitcoin-treasury`).
- A 1-2 sentence **description** of what's driving it (recent news / sentiment).
- A list of 3-8 **example tickers** that are clearly playing the theme right now.

Focus on *real, currently active* narratives - not generic sectors. If a theme
was hot last quarter but is cooling, skip it. If a brand new theme is breaking
out this week, include it.

Respond with **only** a JSON array, no commentary. Schema:

```json
[
  {
    "tag": "ai-hyperscaler-deals",
    "description": "Multi-billion-dollar multi-year deals between hyperscalers (AWS / Anthropic / Microsoft / Google) and infrastructure / compute providers; market is rewarding announced deal sizes.",
    "examples": ["AKAM", "NET", "ORCL"]
  }
]
```
