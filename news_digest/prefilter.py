"""Mechanical drop rules applied before any LLM call.

Sized against the 2026-09-05..09-11 week (3,394 deduped titles): drops ~36% for
zero cost. Recall was checked against the 347 titles Opus selected in the 30d
run — 331 survive, and all 16 losses are categories the selection prompt
already instructs the model to drop (single analyst actions, retrospective
return math, reverse splits).

Channel tags are ADDITIVE, not exclusive. Dropping on the presence of a "noisy"
channel deletes real stories: Benzinga tags Nvidia guidance and antitrust
rulings with nothing but ``news``/``markets``/``tech``. So the only
channel-based rule here requires a corroborating title pattern.
"""

from __future__ import annotations

import re

# Column factories — every item from these bylines is generated from a template.
SLOP_AUTHORS = {"benzinga insights", "benzinga neuro", "tradepulse"}

# A single analyst action, only when the item is tagged as a rating/PT wire AND
# the title reads like a mechanical action line.
ANALYST_CHANNELS = {"price target", "initiation", "reiteration", "upgrades", "downgrades"}
ANALYST_TITLE = re.compile(
    r"\b(raises|lowers|maintains|reiterates|initiates|assumes|keeps|boosts|cuts|trims|"
    r"announces|reinstates|resumes|downgrades|upgrades)\b.{0,60}"
    r"\b(price target|pt|rating|outperform|overweight|underweight|equal-?weight|"
    r"market perform|sector perform|buy|neutral|hold|sell)\b",
    re.I,
)

TITLE_SLOP = re.compile(
    r"if you invested|would be worth (today|\$)|invested \$[\d,]+ in|"
    r"performance comparison|compar(ing|ison of|ed to) .{0,40}competitors|"
    r"versus competitors|peer (analysis|comparison)|industry comparison|"
    r"price over earnings|"
    r"^(market whales|unusual options|options market)|unusual options activity|"
    r"^stock of the day|how to earn \$|earn \$[\d,]+ a month|"
    r"^(top|these) \d+ .{0,40}(stocks|etfs)|^\d+ (stocks|etfs) to watch|"
    r"earnings call transcript|"
    r"announces? (a )?\d+-for-\d+ reverse|reverse stock split, effective|"
    r"ads ratio change|shelf registration|resale registration|"
    r"receives? (nasdaq|nyse) (notification|notice|deficiency)|"
    r"regain compliance|minimum bid price|"
    r"^benzinga (bulls and bears|before the bell|pro)|"
    r"^trading halt|"
    r"insider (sells?|buys?|sold|bought) \$|form 4 filing",
    re.I,
)


def drop_reason(article: dict) -> str | None:
    """Return why this article is dropped, or None to keep it."""
    if article.get("author") in SLOP_AUTHORS:
        return "slop_author"

    title = article.get("title") or ""
    if TITLE_SLOP.search(title):
        return "title_slop"

    channels = set(article.get("channels") or [])
    if channels & ANALYST_CHANNELS and ANALYST_TITLE.search(title):
        return "analyst_wire"

    return None


def apply(articles: list[dict]) -> tuple[list[dict], dict[str, int]]:
    """Return (kept, {reason: count}). ``kept`` preserves input order."""
    kept: list[dict] = []
    dropped: dict[str, int] = {}
    for article in articles:
        reason = drop_reason(article)
        if reason is None:
            kept.append(article)
        else:
            dropped[reason] = dropped.get(reason, 0) + 1
    return kept, dropped
