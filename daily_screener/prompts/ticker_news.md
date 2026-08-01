You are explaining a **stock price move**, not writing a research report.

### Target

**{company_name} ({ticker})**

### Price action (last few weeks)

{price_action_block}

Your central question:

> **Given this move, what is the market actually reacting to?**

A move of this size in a stock this liquid does not happen without a driver.
Your job is to find that driver — or, if you genuinely can't, to say so
honestly and call out that the move looks like sympathy / technical action.

### Where to look — be exhaustive

A generic Q&A approach will default to whatever press release was filed most
recently (often an earnings 8-K) and miss the real story. Do not do that.
Cast a wide net across **all** of the following before answering:

1. **Earnings / guidance**: beats/misses, raised/lowered guide, segment
   acceleration, margin inflection — with specific numbers.
2. **Named customer adoption (confirmed OR rumored)**: announcements,
   supply-chain analyst notes, or media reports that a marquee buyer
   (Apple, NVIDIA, TSMC, Google, Microsoft, Amazon, Meta, Tesla, OpenAI,
   Anthropic, major hyperscaler, major auto OEM, major defense prime) is
   adopting / qualifying / piloting / mass-producing with this company. This
   often drives the biggest moves in small-mid-cap names and is the catalyst
   most likely to be missed.
3. **Analyst supply-chain reports**: Ming-Chi Kuo (TF International),
   DigiTimes, Wedbush, Loop Capital, Needham, Morgan Stanley supply-chain
   notes, broker upgrades with materially higher targets.
4. **Product / platform inflection**: a new SKU, design win, qualification
   milestone, or mass-production start tied to a specific end market or
   customer window (e.g. "mass production for X in 2026-27").
5. **Acquisitions, divestitures, spins, sum-of-parts re-ratings.**
6. **Regulatory wins/losses**: FDA, FCC, export controls, antitrust, tariff
   exemptions, government contracts.
7. **Sector sympathy**: a clearly attributable move tied to a specific peer's
   news (name the peer and the date).
8. **Insider / 13F / short-interest changes** that are recent and concrete.
9. **Patent filings, conference commentary, executive interviews** that
   reveal a new vertical or customer relationship.

Material does NOT include: pure chart-pattern commentary with no underlying
news, vague "AI partnership" press releases with no named counterparty at
all, and stale events older than 60 days.

### Plausibility check (do this before you answer)

If your draft answer is "the only catalyst is the recent earnings release"
but the stock is up 30%+ in the last few weeks on elevated volume, **stop
and look again**. Earnings alone rarely cause that. There is almost certainly
a second story (named customer, analyst note, product ramp, sector tailwind)
that you have not yet surfaced. Find it or be explicit that you searched
and could not.

### Candidate theme tags

(pick the ones that fit; do NOT invent new ones)

{theme_tags_list}

### Prior thesis hint (optional)

{watchlist_thesis_block}

This is just a hint — verify everything independently. It may be wrong,
stale, or biased.

### Output

Respond with **only** a JSON object, no preamble. Be specific. Whenever
possible, every driver should include: a named counterparty, a date, and a
number (dollars, percentage, units, or timing window).

```json
{{
  "ticker": "{ticker}",
  "has_material_news": true,
  "headline_drivers": [
    "WiseEye AI image-sensing reportedly adopted by Apple for smart glasses, mass production 2026-27 (Ming-Chi Kuo report, 2026-04-15)",
    "Q1 2026 EPS $0.18 beat $0.14 consensus; Q2 revenue guide $200-210M, +6% QoQ midpoint (2026-05-07)"
  ],
  "latest_material_date": "2026-05-07",
  "theme_tags": ["edgeai", "optical-and-photonics"],
  "summary": "2-4 sentences explaining what the market is reacting to. Lead with the biggest re-rating driver, not necessarily the most recent press release."
}}
```

If you genuinely cannot find a material catalyst, return
`has_material_news: false`, set `headline_drivers` to `[]`, and explain in
`summary` what the apparent (likely non-fundamental) driver is — and say
explicitly that you searched the categories above.
