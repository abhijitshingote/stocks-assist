"""Unified LLM dispatcher.

Routes a single call to either Claude or Perplexity based on the model name
prefix. Stage 3 (user-theme extraction) and Stage 5 (judge) use this so we can
swap providers per-stage by editing `config.JUDGE_MODEL` only.

- Models starting with ``claude-`` route to Anthropic (see ``utils.claude``).
- Anything else routes to Perplexity (see ``utils.perplexity``). The Perplexity
  ``sonar`` family is grounded-search-capable but follows non-search prompts
  fine for pure judgment / extraction tasks.
"""

from __future__ import annotations

import logging
from typing import Any

from daily_screener.utils.claude import ClaudeError, call_claude
from daily_screener.utils.claude import extract_json as _extract_json
from daily_screener.utils.perplexity import PerplexityError, call_perplexity

logger = logging.getLogger(__name__)


class LLMError(Exception):
    """Provider-agnostic wrapper around ClaudeError / PerplexityError."""


def _is_claude(model: str) -> bool:
    return (model or "").lower().startswith("claude")


def call_llm(
    prompt: str,
    *,
    model: str,
    system: str | None = None,
    max_tokens: int = 1500,
    timeout: int | None = None,
) -> str:
    """Call an LLM by model name; returns raw text content.

    The system prompt is honored for Claude. For Perplexity it's prepended to
    the user message (Perplexity's chat API supports system messages too, but
    our thin client doesn't surface that; concatenation keeps things simple).
    """
    if _is_claude(model):
        try:
            return call_claude(prompt, model=model, system=system, max_tokens=max_tokens, timeout=timeout)
        except ClaudeError as e:
            raise LLMError(f"claude: {e}") from e

    try:
        full_prompt = f"{system}\n\n{prompt}" if system else prompt
        return call_perplexity(full_prompt, model=model, max_tokens=max_tokens, timeout=timeout)
    except PerplexityError as e:
        raise LLMError(f"perplexity: {e}") from e


def call_llm_json(
    prompt: str,
    *,
    model: str,
    system: str | None = None,
    max_tokens: int = 1500,
    timeout: int | None = None,
) -> Any:
    """Call an LLM and best-effort parse a JSON object/array from the response.

    Raises ``LLMError`` on transport failure and ``ValueError`` on parse
    failure. Callers should catch both.
    """
    raw = call_llm(prompt, model=model, system=system, max_tokens=max_tokens, timeout=timeout)
    return _extract_json(raw)


# Re-export so callers can do ``from daily_screener.utils.llm import extract_json``.
extract_json = _extract_json
