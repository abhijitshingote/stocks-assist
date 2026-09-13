"""Stages 2-3: Haiku screens the whole week, Opus makes the final cut.

Splitting the two is what makes Opus judgement affordable. Screening 2,160
titles on Opus costs ~$0.60; on Haiku it is ~$0.14. The second pass then sees
only ~250 titles (~10k tokens), so the model that actually decides the brief's
contents costs about five cents.
"""

from __future__ import annotations

import json
import logging
import re

from news_digest import config, llm, prompts

logger = logging.getLogger(__name__)

_JSON_OBJ_RE = re.compile(r"\{[^{}]*\}")


def _parse_jsonl(text: str) -> list[dict]:
    """Tolerant parse — recovers if the model fences the output or adds prose."""
    out: list[dict] = []
    seen: set[int] = set()
    for match in _JSON_OBJ_RE.finditer(text or ""):
        try:
            obj = json.loads(match.group(0))
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        try:
            n = int(obj.get("n"))
        except (TypeError, ValueError):
            continue
        if n in seen:
            continue
        seen.add(n)
        obj["n"] = n
        out.append(obj)
    return out


def _resolve(parsed: list[dict], universe: list[dict]) -> tuple[list[dict], int]:
    """Map line numbers back onto articles, dropping out-of-range hallucinations."""
    resolved: list[dict] = []
    out_of_range = 0
    for obj in parsed:
        n = obj["n"]
        if not 1 <= n <= len(universe):
            out_of_range += 1
            continue
        resolved.append({**universe[n - 1], "_pick": obj})
    return resolved, out_of_range


def screen(articles: list[dict], *, tracker: llm.CostTracker, target_n: int | None = None):
    """Cheap first pass: drop obvious noise, keep a generous shortlist."""
    target_n = target_n or config.SCREEN_TARGET_N
    user_message = prompts.screen_user_message(articles, target_n=target_n)

    input_tokens = llm.count_tokens(
        model=config.SCREEN_MODEL, system=prompts.SCREEN_SYSTEM, user_message=user_message
    )
    if input_tokens > config.SCREEN_MAX_INPUT_TOKENS:
        raise RuntimeError(
            f"screen input is {input_tokens:,} tokens, over the "
            f"{config.SCREEN_MAX_INPUT_TOKENS:,} ceiling — narrow the window"
        )
    logger.info(
        "screen: %s over %d titles (%s tokens), asking for %d",
        config.SCREEN_MODEL,
        len(articles),
        f"{input_tokens:,}",
        target_n,
    )

    text = llm.complete(
        model=config.SCREEN_MODEL,
        system=prompts.SCREEN_SYSTEM,
        user_message=user_message,
        step="screen",
        tracker=tracker,
        max_tokens=config.SCREEN_MAX_TOKENS,
    )

    parsed = _parse_jsonl(text)
    kept, out_of_range = _resolve(parsed, articles)
    logger.info("screen: %d returned, %d valid (%d out of range)", len(parsed), len(kept), out_of_range)

    stats = {
        "model": config.SCREEN_MODEL,
        "input_titles": len(articles),
        "input_tokens": input_tokens,
        "requested": target_n,
        "returned": len(parsed),
        "kept": len(kept),
        "out_of_range": out_of_range,
    }
    return kept, stats


def final_cut(screened: list[dict], *, tracker: llm.CostTracker, target_n: int | None = None):
    """Opus picks and ranks what actually goes in the brief."""
    target_n = target_n or config.SELECT_TARGET_N
    user_message = prompts.select_user_message(screened, target_n=target_n)
    logger.info(
        "select: %s over %d screened titles, asking for %d",
        config.SELECT_MODEL,
        len(screened),
        target_n,
    )

    text = llm.complete(
        model=config.SELECT_MODEL,
        system=prompts.SELECT_SYSTEM,
        user_message=user_message,
        step="select",
        tracker=tracker,
        max_tokens=config.SELECT_MAX_TOKENS,
    )

    parsed = _parse_jsonl(text)
    resolved, out_of_range = _resolve(parsed, screened)

    selected: list[dict] = []
    for order, item in enumerate(resolved, start=1):
        pick = item.pop("_pick")
        try:
            score = int(pick.get("score", 0))
        except (TypeError, ValueError):
            score = 0
        selected.append(
            {
                **item,
                "rank": order,
                "score": max(0, min(100, score)),
                "why": str(pick.get("why") or "")[:300],
            }
        )

    logger.info("select: %d returned, %d valid (%d out of range)", len(parsed), len(selected), out_of_range)
    stats = {
        "model": config.SELECT_MODEL,
        "input_titles": len(screened),
        "requested": target_n,
        "returned": len(parsed),
        "kept": len(selected),
        "out_of_range": out_of_range,
    }
    return selected, stats
