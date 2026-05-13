"""Thin Claude (Anthropic) client used by stages 3 and 5.

Wraps anthropic.Anthropic with a simple JSON-extraction helper. We intentionally
do NOT enable web_search here; the news fetching is done by Perplexity in
stage 4 and passed in as input to the judge.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time

from daily_screener import config

logger = logging.getLogger(__name__)


class ClaudeError(Exception):
    """Raised when Claude fails after retries."""


def _client():
    try:
        from anthropic import Anthropic
    except ImportError as e:
        raise ClaudeError(
            "anthropic package not installed (pip install anthropic)"
        ) from e
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ClaudeError("ANTHROPIC_API_KEY not set in environment")
    return Anthropic(api_key=api_key)


# Map convenience names to actual model identifiers.
_MODEL_ALIASES = {
    "claude-haiku-4-5": "claude-haiku-4-5",
    "claude-haiku": "claude-haiku-4-5",
    "claude-sonnet": "claude-sonnet-4-5-20250929",
    "claude-sonnet-4-5": "claude-sonnet-4-5-20250929",
}


def _resolve_model(name: str) -> str:
    return _MODEL_ALIASES.get(name, name)


def call_claude(
    prompt: str,
    *,
    system: str | None = None,
    model: str | None = None,
    max_tokens: int | None = None,
    timeout: int | None = None,
) -> str:
    """Call Claude and return the raw text content. Retries on transient errors."""
    model = _resolve_model(model or config.JUDGE_MODEL)
    max_tokens = max_tokens or config.JUDGE_MAX_TOKENS
    timeout = timeout or config.JUDGE_TIMEOUT_SECONDS

    client = _client()

    last_err: str | None = None
    for attempt in range(config.NEWS_RETRY_ATTEMPTS + 1):
        try:
            kwargs = {
                "model": model,
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
                "timeout": timeout,
            }
            if system:
                kwargs["system"] = system

            resp = client.messages.create(**kwargs)
            parts = []
            for block in resp.content:
                if getattr(block, "type", None) == "text":
                    parts.append(block.text)
            return "\n".join(parts)
        except Exception as e:  # noqa: BLE001 - anthropic raises various types
            msg = str(e)
            last_err = msg
            transient = "429" in msg or "rate_limit" in msg or "overloaded" in msg or "5" in msg[:3]
            logger.warning(
                "claude error (attempt %s/%s, model=%s): %s",
                attempt + 1,
                config.NEWS_RETRY_ATTEMPTS + 1,
                model,
                msg[:200],
            )
            if not transient:
                break
            time.sleep(config.REQUEST_RETRY_BACKOFF_SECONDS * (attempt + 1))

    raise ClaudeError(f"Claude failed after retries: {last_err}")


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", re.DOTALL)


def extract_json(text: str):
    """Best-effort JSON extraction from a model response.

    Tries (1) full-string parse, (2) fenced ```json ... ``` block,
    (3) greedy first {...} or [...] in the body. Raises ValueError if nothing
    parses.
    """
    if not text:
        raise ValueError("empty response")

    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    m = _JSON_FENCE_RE.search(text)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    for opener, closer in (("{", "}"), ("[", "]")):
        start = text.find(opener)
        end = text.rfind(closer)
        if start != -1 and end != -1 and end > start:
            candidate = text[start : end + 1]
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue

    raise ValueError(f"could not parse JSON from response: {text[:200]!r}")
