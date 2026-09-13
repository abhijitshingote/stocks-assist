"""Prompts for the screen, select, and brief stages."""

from __future__ import annotations

_DROP_RULES = """\
## DROP
Auto-generated and engagement-bait content:
- Retrospective return math: "if you invested $1,000 in X", "$100 invested N years ago"
- Peer-ratio boilerplate: "Performance Comparison", "X's Position Compared To Competitors"
- Mechanical lists: "12 Health Care Stocks Moving In Friday's Session", options-flow
  roundups, "N Stocks To Watch", "Top 3 Stocks That May Rocket"
- Single analyst actions with no argument: "Firm Maintains Outperform, Raises PT To $X".
  Keep an analyst item ONLY if it carries a real thesis or an unusually large revision
  on a widely held name.
- Earnings call transcripts
- Routine housekeeping: shelf/resale registrations, Form 4 sales, exchange noncompliance
  letters, reverse splits, trading halts
- Celebrity, net-worth, sports, and lifestyle trivia, even with a ticker attached
- Political theater with no transmission into markets. KEEP policy that has a mechanism:
  tariffs, export controls, sanctions, antitrust, subsidies, rate policy, contracts.
- Pure crypto price commentary with no listed-equity consequence

## KEEP
Anything that changes the trajectory or risk of a company, a sector, or the market:
earnings and guidance changes, demand or pricing inflections, capacity and supply, major
contracts and customer wins/losses, M&A and financing that matters, regulatory and legal
outcomes, macro data and central-bank policy, product cycles and technology milestones,
management changes at consequential companies, operational incidents."""


SCREEN_SYSTEM = f"""You are triaging a raw newswire for a professional investor. You are \
given every headline published in one week and must discard the noise.

This is a FIRST PASS. Be inclusive: your job is to throw out what is obviously worthless,
not to pick the final list. When unsure, keep the item.

{_DROP_RULES}

## DEDUPLICATE
The same development is covered by many headlines. Keep the ONE best headline for each
distinct development and discard restatements and wire re-runs.

## OUTPUT
Return the surviving line numbers as JSONL, one object per line, best first. `n` is the
line number copied exactly. No prose, no markdown fence.

{{"n": 4821}}

Return at most N items (N is given in the user message). Emit nothing for items you drop."""


def screen_user_message(rows: list[dict], *, target_n: int) -> str:
    """Whole filtered universe, one line per article.

    Lines are keyed by a short sequential index rather than the 9-digit Benzinga
    id: long ids get mis-transcribed across a large context, and 4 digits
    instead of 9 also saves meaningful input tokens.
    """
    lines = [
        f"{i}|{a['title']}|{','.join(a['tickers'][:4]) or '-'}|{a['published'][:10]}"
        for i, a in enumerate(rows, start=1)
    ]
    dates = [a["published"][:10] for a in rows if a.get("published")]
    span = f"{min(dates)} to {max(dates)}" if dates else "unknown"
    return (
        f"{len(rows)} headlines covering {span}, numbered 1-{len(rows)}.\n"
        f"Format: n|title|tickers|date\n\n"
        + "\n".join(lines)
        + f"\n\nReturn at most {target_n} line numbers as JSONL, best first, deduplicated "
        f"across developments. Every `n` must be between 1 and {len(rows)}."
    )


SELECT_SYSTEM = f"""You are a buy-side analyst choosing what goes into a weekly catch-up \
brief for a professional investor who has been away.

You are given headlines that already survived a coarse noise filter. Pick the ones that
genuinely mattered this week. You are NOT summarizing — you are selecting and ranking.

{_DROP_RULES}

## SELECTION
Rank by how much a professional needs to know the item, and cover the whole week. Do not
let one mega-cap or one theme consume the list — if ten headlines concern the same AI
capex story, take the single best one and spend the remaining slots elsewhere. A genuinely
important small-cap, sector, or macro development should not be crowded out.

Spread selections across the date range. If fewer than N items clear the bar, return
fewer — do not pad.

## OUTPUT
JSONL, one object per line, best first. `n` is the line number copied exactly. No prose,
no markdown fence.

{{"n": 482, "score": 91, "why": "one terse clause"}}

`score` is 0-100; reserve above 85 for genuinely important news. `why` is under 15 words
and states the substance, not the category."""


def select_user_message(rows: list[dict], *, target_n: int) -> str:
    lines = [
        f"{i}|{a['title']}|{','.join(a['tickers'][:5]) or '-'}|{a['published'][:10]}"
        for i, a in enumerate(rows, start=1)
    ]
    return (
        f"{len(rows)} pre-screened headlines, numbered 1-{len(rows)}.\n"
        f"Format: n|title|tickers|date\n\n"
        + "\n".join(lines)
        + f"\n\nReturn the top {target_n} as JSONL, best first. Every `n` must be between "
        f"1 and {len(rows)}."
    )


BRIEF_SYSTEM = """You are writing a weekly market recap for a professional investor who has \
been away all week and wants one read to catch up.

You receive full article bodies for stories that already passed a significance filter.

## RULES
- Numbers are mandatory where the source has them: percentages, dollar amounts, EPS and
  revenue actual vs estimate, guidance ranges, contract sizes, dates.
- State what happened. Do not speculate about future price action.
- If several articles cover one development, consolidate them into one passage.
- Never invent a fact that is not in the supplied bodies.
- Reference companies by ticker in parentheses on first mention, e.g. Broadcom (AVGO).
- No preamble, no sign-off, no "in conclusion". Start with the content.

## OUTPUT
Markdown only. No JSON, no code fence around the whole response.

Structure:
- Open with `## The Week` — 3-6 sentences on what actually drove the week. This is the
  part that gets read if nothing else does.
- Then `## What Moved` — the individual developments, grouped under `### ` subheadings by
  theme that you choose based on what is actually in the week (do not force a fixed
  taxonomy). Within each theme, write tight paragraphs or bullets. Bold every figure.
- Close with `## Worth Watching` — a short list of things that are set up to matter next
  week, drawn only from what is in the supplied articles.

Be dense and specific. No filler, no restating the obvious, no hedging."""


def brief_user_message(selected: list[dict], *, window_label: str, body_chars: int) -> str:
    blocks = []
    for a in selected:
        body = (a.get("body") or "")[:body_chars] or a.get("teaser") or ""
        blocks.append(
            f"### [{a['benzinga_id']}] {a['title']}\n"
            f"- published: {a['published'][:10]}\n"
            f"- tickers: {', '.join(a['tickers'][:8]) or '(none)'}\n"
            f"- selected because: {a.get('why') or '(n/a)'}\n\n"
            f"{body}\n"
        )
    return (
        f"Week of {window_label}. {len(selected)} stories, most significant first.\n\n"
        + "\n".join(blocks)
        + "\nWrite the recap."
    )
