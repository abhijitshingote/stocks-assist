You are analyzing a trader's personal watchlist notes to extract their "taste
signature" - the kinds of stories and themes they get excited about.

Below is a JSON object: keys are tickers, values include a free-form `notes`
field and an optional `stars` rating.

Stars semantics:
- `stars` missing or `0` = the trader hasn't rated this one yet (NEUTRAL).
  Treat the notes at face value; do not infer negativity from the absence of a
  rating.
- `stars` 1-3 = explicit liking, with 3 being strong conviction.

These tickers are all on the watchlist by choice, so treat them all as
positive examples of the trader's taste. Explicit dislikes live in a
separate file and are NOT shown to you.

```
{watchlist_json}
```

Your job: derive 5-10 **theme tags** that capture the patterns in these notes.

For each theme:

- A short **tag** (kebab-case).
- A 1-2 sentence **description** of why this matches the trader's taste.
- A list of **example_tickers** from the watchlist that exemplify the theme.
- An optional **anti_pattern** describing what would disqualify a candidate
  from this theme (e.g. for `ai-hyperscaler-deals`: "vague AI partnership
  press release without dollar amount or named hyperscaler").

Patterns to look for (examples, not exhaustive):

- Game-changing dollar-value deals (e.g. AKAM-Anthropic $1.8B)
- Hyperscaler customer wins or registry-type partnerships (NVIDIA, AWS, etc.)
- Margin/revenue inflections backed by specific numbers
- Optical / photonics / data-center-infrastructure inflections
- Earnings beats with raised guidance (esp. with specific % raises)
- Sum-of-parts / spin re-rating stories
- Low-float momentum with concrete catalyst (not pure speculation)

Respond with **only** a JSON array. Schema:

```json
[
  {
    "tag": "ai-hyperscaler-deals",
    "description": "Big-dollar multi-year deals where a hyperscaler is a named customer; deal size and term are explicit.",
    "example_tickers": ["AKAM", "INOD"],
    "anti_pattern": "Vague AI partnership without dollar amount, term, or named hyperscaler."
  }
]
```
