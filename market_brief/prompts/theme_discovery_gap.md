You are helping a semiconductor/tech-focused trader curate `themes.json` for a daily market brief.

The brief routes Benzinga articles to themes **only when article tickers overlap the theme's ticker list**. Themes the trader cares about but forgot to add will land in an "unassigned" bucket.

## Current curated themes (name + tickers)

{curated_themes_json}

## Tickers that appeared often in UNASSIGNED articles this run (not covered by any theme)

{orphan_ticker_histogram}

## Sample unassigned headlines

{sample_headlines}

Identify 3-6 **actionable** theme gaps: narratives that are hot this week and should become a curated theme (or ticker additions to an existing theme). Skip generic sectors (e.g. "healthcare") unless clearly driving the trader's tape. Prefer tech, semis, AI infra, quantum, power, networking.

Respond with **only** a JSON array:

```json
[
  {
    "name": "Quantum Computing",
    "description": "1-2 sentences on the narrative",
    "tickers": ["IONQ", "RGTI", "QBTS", "QUBT", "IBM"]
  }
]
```
