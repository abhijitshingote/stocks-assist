"""Stage 4: bodies of the final cut -> one markdown recap.

Markdown out, not JSON. The old pipeline asked for a 2-4 sentence insight per
item on top of the narrative, which cost ~49k output tokens ($1.22 on Opus) and
was the single largest line in the run. The recap is what gets read.
"""

from __future__ import annotations

import logging

from news_digest import config, llm, prompts

logger = logging.getLogger(__name__)


def fit_to_budget(selected: list[dict]) -> list[dict]:
    """Trim the tail if the bodies exceed the char budget. Rarely binds at ~70 items."""
    out: list[dict] = []
    used = 0
    for item in selected:
        cost = min(len(item.get("body") or ""), config.BRIEF_BODY_CHARS)
        if used + cost > config.BRIEF_BODY_CHAR_BUDGET:
            logger.warning("body budget hit at %d items", len(out))
            break
        used += cost
        out.append(item)
    logger.info("brief: %d items, %s body chars", len(out), f"{used:,}")
    return out


def write(selected: list[dict], *, window_label: str, tracker: llm.CostTracker) -> tuple[str, dict]:
    """Return (markdown recap, stats)."""
    if not selected:
        return "", {"items": 0}

    items = fit_to_budget(selected)
    text = llm.complete(
        model=config.BRIEF_MODEL,
        system=prompts.BRIEF_SYSTEM,
        user_message=prompts.brief_user_message(
            items, window_label=window_label, body_chars=config.BRIEF_BODY_CHARS
        ),
        step="brief",
        tracker=tracker,
        max_tokens=config.BRIEF_MAX_TOKENS,
    )

    markdown = (text or "").strip()
    # Models occasionally wrap the whole document in a fence despite the prompt.
    if markdown.startswith("```"):
        lines = markdown.split("\n")
        markdown = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:]).strip()

    if not markdown:
        raise RuntimeError("brief call returned empty markdown")

    return markdown, {"model": config.BRIEF_MODEL, "items": len(items), "chars": len(markdown)}
