"""Prompt builders for the market brief.

Two probe styles per topic (sector or theme):

1. `build_overview_prompt` — "what moved and why" with stock-specific
   callouts. Goal: surface the day's named-ticker stories that a
   generalist headline scan would miss.

2. `build_catalyst_prompt` — focused on the configured catalyst angles
   (earnings reads, supply-chain/customer wins, product launches,
   regulatory). Goal: dig into the *kind* of catalyst the user trades.

Plus `build_synthesis_prompt` which takes all probe outputs and
produces the final tiered brief.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

from market_brief import config


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _prior_session_label(asof: str) -> str:
    """Best-effort label for the most recent US trading session relative to `asof`.

    `asof` is when the brief is *written*, which is typically the morning
    after the session it covers. We don't have a market-calendar dep, so
    we just walk back to the previous weekday. (Holidays will occasionally
    label a session a day early; the prompt instructs the model to verify
    its own dates, so this is a hint, not a hard constraint.)
    """
    try:
        d = datetime.strptime(asof, "%Y-%m-%d").date()
    except ValueError:
        return "the most recent US trading session"

    # Walk back at least 1 day; if we land on Sat/Sun, keep going to Fri.
    delta = 1
    while True:
        candidate = d - timedelta(days=delta)
        if candidate.weekday() < 5:  # Mon=0..Fri=4
            return candidate.strftime("%A %B %-d, %Y")
        delta += 1


# ---------------------------------------------------------------------------
# Topic = sector OR theme. We unify the shape so the orchestrator can
# treat them identically.
# ---------------------------------------------------------------------------


def topic_header(topic: dict) -> str:
    """Format the topic name + desc + optional tickers for prompt injection."""
    name = topic["name"]
    desc = (topic.get("desc") or "").strip()
    tickers = topic.get("tickers") or []

    parts = [f"Topic: {name}"]
    if desc:
        parts.append(f"Scope: {desc}")
    if tickers:
        parts.append(
            "Representative tickers (use these as anchors, but feel free to "
            f"pull in others in the same space): {', '.join(tickers)}"
        )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Probe 1: Overview — what moved and why, with named tickers
# ---------------------------------------------------------------------------


def build_overview_prompt(
    topic: dict,
    asof: str | None = None,
    tape_block: str | None = None,
) -> str:
    asof = asof or _today_str()
    prior_session = _prior_session_label(asof)
    header = topic_header(topic)
    tape_section = (tape_block or "VERIFIED TAPE: (none provided)").strip()

    return f"""You are a senior equity analyst writing a pre-market briefing for a US-equity trader.
Brief written: {asof}
Trading session being covered: {prior_session} (the most recent regular US session BEFORE {asof}).

{tape_section}

{header}

Use the VERIFIED TAPE block above as ground truth for what the session actually did.
You do NOT need to search the web for prices. You DO need to search the web for the
NEWS / CATALYSTS that explain the moves shown in the tape, and for the broader news
flow around this topic in the last 24-72 hours.

PRICE-DIRECTION INTEGRITY — read carefully:
- The VERIFIED TAPE block above is the source of truth for prices and signs. Use those
  exact numbers (e.g. if it says "NVDA -3.60%", write "NVDA {prior_session} close:
  -3.60%", never "+X" or a different magnitude).
- Cover BOTH advancers AND decliners FROM the verified tape. If the tape was broadly
  red, lead with decliners. If broadly green, lead with advancers. Do NOT bias toward a
  "+" framing because the topic has a positive long-term thesis.
- For tickers NOT in the verified tape, you may describe news without a % move, OR
  describe positioning / analyst notes — but do NOT fabricate a session % move.
- Empty Advancers / Decliners sections = failure mode. The verified tape always gives
  you something to populate them with.

CONTENT RULES:
- Every bullet names at least one ticker. If you can't name a ticker, drop the bullet.
- Every bullet has a session timestamp ("{prior_session} close", "after-hours Thursday
  May 14", "pre-market {asof}").
- Quantify: % move (with sign), $ revenue, # of units, contract size, EPS beat/miss.
- Distinguish FACTS (filings, press releases, official prints) from MARKET CHATTER
  (analyst notes, social, rumor) — tag each.
- Go DEEPER than mainstream headlines. Surface stock-specific stories a generalist scan
  would miss (supplier read-throughs, analyst target changes, design wins, fund positioning).

Structure as markdown:

## Tape on {prior_session}
One-line broad-tape recap built from the index/ETF lines in the VERIFIED TAPE block (e.g.
"SPY -1.2%, QQQ -1.5%, SOXX -3.9%; broad risk-off into weekend"), plus one line on what
likely drove direction (rates, oil, geopolitics, single-name catalyst).

## Decliners on {prior_session}
Named tickers from the VERIFIED TAPE that closed DOWN, biggest losers first. Format:
- **TICKER {prior_session} close: -X.X%** (use the exact number from VERIFIED TAPE) —
  what drove the move, with source.
- Read-through: which related tickers it implicates and how.

## Advancers on {prior_session}
Named tickers from the VERIFIED TAPE that closed UP. Same format.

## News & flow without confirmed price
Stories that matter but where you couldn't pin down a session price reaction.
Format: **TICKER** [date] — what happened — *source* — likely directional bias if any.

## Cross-currents
Themes / positioning shifts under the surface — analyst upgrades/downgrades, 13F changes,
unconfirmed customer wins, technology transitions. Tag tickers + dates.

## Watch next session
Tickers / events to watch in the next 24-48h after {asof} (earnings, conferences, expected
catalysts, Asia-tape read-throughs)."""


# ---------------------------------------------------------------------------
# Probe 2: Catalyst angles — earnings reads, supply chain, product, reg
# ---------------------------------------------------------------------------


def _format_angles(angles: Iterable[dict]) -> str:
    return "\n".join(f"- **{a['key']}**: {a['label']}" for a in angles)


def build_catalyst_prompt(topic: dict, asof: str | None = None) -> str:
    asof = asof or _today_str()
    header = topic_header(topic)
    angles = _format_angles(config.CATALYST_ANGLES)

    # Lead with an explicit search instruction — without it, Perplexity
    # sometimes falls back to chat mode and returns "I don't have live
    # web access" boilerplate. Asking for *named* press releases /
    # *named* analysts forces it to pull from real sources.
    return f"""Search the web RIGHT NOW for news from the last 24-72 hours about: {topic['name']}.
You are an equity-research analyst writing actionable catalysts for a US-market trader.
Today's date: {asof}

{header}

Find SPECIFIC, NAMED catalyst events from the last 24-72 hours that fit these angles:
{angles}

HARD REQUIREMENTS — if a bullet doesn't have these, drop it:
- Name the TICKER.
- Name WHAT happened in concrete terms — a press-release headline, a filing type (8-K, S-1),
  an earnings line item, a contract name, a regulatory action, a product SKU.
- Quote the NUMBER — % move, $ revenue, # of units, contract size, target price, EPS beat/miss.
- Cite the SOURCE by name — "Reuters", "Bloomberg", "company 8-K", "Morgan Stanley note from
  May 14", "TrendForce report", "Kuo on X". Generic "reports said" is not allowed.
- State the DATE the event happened ("Wed pre-market", "May 15 after-close", "Tuesday").

DO NOT WRITE bullets like "remains a beneficiary of AI capex", "continues to gain on
networking exposure", "stays in the sweet spot". Those are sell-side filler. Every bullet
must answer: WHAT happened, WHO said/filed/reported it, WHEN, and the NUMBER.

If you genuinely cannot find a fresh catalyst for this topic in the window, write:
"NO MATERIAL FRESH CATALYST in the last 24-72h." Do NOT pad with thesis-style commentary.

Structure as markdown:

## Confirmed catalysts (filings, press releases, earnings, official announcements)
Format: **TICKER** [angle] — DATE — what happened with the number — *source: …* — one-line read-through to other named tickers.

## Sell-side & positioning (analyst notes, target changes, 13F / activist moves, insider buys)
Format: **TICKER** [angle] — DATE — firm/analyst name → action (PT $X → $Y, upgrade, new position size) — *source: …* — why it matters.

## Rumor / chatter (unconfirmed leaks, M&A speculation, social-driven moves)
Format: **TICKER** [angle] — DATE — what is being claimed and by whom — *source: …* — confidence (low/med).

## Read-throughs
For each confirmed item above, name the 2-3 OTHER tickers it implicates and the directional bias (bull/bear, with one phrase of why)."""


# ---------------------------------------------------------------------------
# Synthesis — fold all probe outputs into a tiered brief
# ---------------------------------------------------------------------------


def build_synthesis_prompt(
    probe_outputs: list[dict],
    asof: str | None = None,
    tape_block: str | None = None,
) -> str:
    """Build the final synthesis prompt.

    `probe_outputs` is a list of {topic_name, kind, content} dicts. We
    concatenate them into the prompt so the synth model has everything
    in one shot. `tape_block` is the global verified-tape block for
    indices + a broad ticker basket; it anchors the macro recap.
    """
    asof = asof or _today_str()

    body_chunks = []
    for p in probe_outputs:
        body_chunks.append(
            f"\n--- BEGIN PROBE: {p['topic_name']} ({p['kind']}) ---\n"
            f"{p['content']}\n"
            f"--- END PROBE: {p['topic_name']} ({p['kind']}) ---\n"
        )
    body = "".join(body_chunks)

    prior_session = _prior_session_label(asof)
    tape_section = (tape_block or "VERIFIED TAPE: (none provided)").strip()
    return f"""You are the editor of a daily pre-market intelligence brief for a US equity trader.
Brief written: {asof}
Trading session covered: {prior_session}.

{tape_section}

Below are research probes on multiple sectors and themes.
Your job is to ASSEMBLE them into a tiered brief — NOT to summarize them.

CORE PRINCIPLE: PRESERVE THE DETAIL. The probes contain specific numbers, source names,
dates, and quoted moves. Those are the WHOLE POINT of the brief. If you replace
"INTC +6-8% intraday on the May 8 preliminary Apple foundry pact (InsiderFinance)" with
"INTC continues to be in the spotlight on Apple foundry chatter" you have destroyed the
brief. Don't do that.

PRICE-DIRECTION INTEGRITY:
- Carry forward the SIGN of every move from the probes verbatim. If a probe said
  "NVDA -2.3% on Friday close", do NOT rewrite that as "NVDA +2.3%" or drop the minus
  sign or describe it as a "constructive bid". Down is down.
- Carry forward the SESSION DATE on every move ("Friday May 15 close: NVDA -2.3%"),
  not just generic "last session".
- Cover BOTH winners AND losers. If the underlying probes for a topic mostly show
  decliners, the brief for that topic must lead with decliners. Do not bias toward
  the bullish framing because the topic has a long-term positive thesis.
- If different probes disagree on direction for the same ticker, FLAG THE DISAGREEMENT
  in one short clause rather than picking one and silently discarding the other.
- If a probe described a story but did NOT verify a price move, the brief should also
  describe the story without inventing a move.

HARD RULES:
1. Carry forward verbatim from the probes: % moves WITH SIGN, $ figures, dates,
   source names ("Reuters", "InsiderFinance", "SIA data", "Morgan Stanley note",
   "TrendForce"), filing types (8-K, 10-Q), specific numbers from earnings prints.
2. BANNED phrases — do NOT write any of these or close paraphrases:
     "remains", "stays", "continues to", "is the cleanest", "in the sweet spot",
     "high-beta theme", "core thesis", "structural read-through", "tape wants proof",
     "constructive", "second-derivative", "leverage play", "best-supported",
     "narrative", "halo", "beneficiary of [generic theme]".
   These are filler. If a bullet's only content is filler like that, DROP THE BULLET.
3. Every bullet must answer at least three of: WHAT happened, WHO reported/filed it,
   WHEN (with session date), and the NUMBER (with sign). If you only have one of
   those, drop the bullet.
4. Do NOT introduce new facts not present in the probes. If a probe said
   "preliminary Apple deal, no orders placed", you say the same — do not soften to
   "Apple foundry chatter".
5. If a probe said "NO MATERIAL FRESH CATALYST", honor that — do not invent one.
6. Keep CONFIRMED / SELL-SIDE / RUMOR / CHATTER tags from the probes intact. Never
   promote a rumor to a fact.

Produce a markdown brief with this EXACT structure:

# Daily Market Brief — {asof}
_Covering: {prior_session}_

## TL;DR (top of mind)
First line: a one-sentence broad-tape recap for {prior_session} ("S&P 500 -0.6%, Nasdaq
-1.1%, SOX -2.4% on {prior_session}; risk-off ahead of weekend"). Use the broad-tape
context that the probes' "Tape on {prior_session}" sections converge on.

Then 5-7 bullets — the most important *concrete events* across ALL probes for
{prior_session}. Mix BOTH gainers and losers — if the tape was broadly red, lead with
red. Each bullet must include ticker, dated session, the actual event, a signed number,
and a source.

Example shape:
- **INTC {prior_session} close: +6.2%** — extension of preliminary Apple foundry pact;
  buyside modeling $3-5B IFS contribution by 2028 (InsiderFinance). Overhang for **TSM**.
- **NVDA {prior_session} close: -2.3%** — profit-taking after multi-week run; SOX -1.8%
  on the day; no single-name catalyst (sell-side desks).

## Tape recap — {prior_session}
A purely directional snapshot. NO commentary, just direction.

### Up
- **TICKER +X.X%** — one phrase, one source.
- ...

### Down
- **TICKER -X.X%** — one phrase, one source.
- ...

If a topic's probes did not include a verified session move for a ticker, do NOT list it
here — list it under "News & flow" below.

## Per-sector / per-theme
For each topic with material findings:

### {{topic name}}
3-5 bullets max. Each bullet is a SPECIFIC event with TICKER, SIGNED NUMBER, SOURCE,
DATE where available. Cover BOTH directions if the underlying probe did. Skip a topic
entirely if there's nothing concrete.

## News & flow (no confirmed price)
Stories that matter but where the probes didn't pin down a session price reaction —
filings, analyst notes, conference commentary, partnership announcements.
Format: **TICKER** [date] — what happened — *source* — likely directional bias if any.

## Stock-specific callouts
The 10-15 most actionable single-name stories. Format:
**TICKER** — DATE — concrete event with signed number — *source* — angle
(earnings / supply_chain / product / regulatory / positioning) —
confidence (confirmed / sell-side / rumor).

## Watch next session
Specific named-ticker events expected in the next 24-48h after {asof}. Each line:
**TICKER** — what to watch — why it matters in one phrase.

Probes follow.
{body}
"""


# ---------------------------------------------------------------------------
# JSON-shaped synthesis (for UI wiring later). Run as a SECOND synth pass
# so the markdown stays high-readability and the JSON stays structured.
# ---------------------------------------------------------------------------


def build_json_synthesis_prompt(brief_markdown: str, asof: str | None = None) -> str:
    asof = asof or _today_str()
    return f"""Convert the following daily market brief into STRICT JSON. Do not add or invent
content; restructure only.

Date: {asof}

Schema (return ONLY this JSON object, no prose, no code fences):
{{
  "asof": "{asof}",
  "tldr": [
    {{"text": "...", "tickers": ["..."]}}
  ],
  "topics": [
    {{
      "name": "...",
      "bullets": [
        {{"text": "...", "tickers": ["..."]}}
      ]
    }}
  ],
  "callouts": [
    {{
      "ticker": "...",
      "summary": "...",
      "angle": "earnings|supply_chain|product|regulatory|other",
      "confidence": "confirmed|sell_side|rumor"
    }}
  ],
  "watch_tomorrow": [
    {{"text": "...", "tickers": ["..."]}}
  ]
}}

Source brief:
---
{brief_markdown}
---

Return JSON only."""
