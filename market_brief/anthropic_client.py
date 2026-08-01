"""Thin Anthropic SDK wrapper for the market brief pipeline."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import anthropic
from anthropic import RateLimitError

from market_brief.anthropic_pricing import UsageBreakdown
from market_brief.cost_tracker import CostTracker

logger = logging.getLogger(__name__)

SONNET_MODEL = os.getenv("MARKET_BRIEF_SONNET_MODEL", "claude-sonnet-4-5-20250929")
OPUS_MODEL = os.getenv("MARKET_BRIEF_OPUS_MODEL", "claude-opus-4-6")

SONNET_LOGICAL = "claude-sonnet-4-5"
OPUS_LOGICAL = "claude-opus-4-6"

# Anthropic org limit (input tokens per minute) — pace Sonnet calls to stay under this.
INPUT_TPM_LIMIT = int(os.getenv("MARKET_BRIEF_INPUT_TPM_LIMIT", "30000"))
MAX_RETRIES = int(os.getenv("MARKET_BRIEF_API_MAX_RETRIES", "8"))


def _client() -> anthropic.Anthropic:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")
    return anthropic.Anthropic(api_key=api_key)


def pause_for_rate_limit(input_tokens: int) -> None:
    """Sleep so the next request is less likely to hit input TPM limits."""
    if input_tokens <= 0:
        return
    # Spread usage across a 60s window (+ small buffer).
    wait_s = int(60 * input_tokens / INPUT_TPM_LIMIT) + 10
    if wait_s > 10:
        logger.info(
            "Rate-limit pacing: sleeping %ds after %s input tokens",
            wait_s,
            f"{input_tokens:,}",
        )
        time.sleep(wait_s)


def _extract_text(response: Any) -> str:
    parts: list[str] = []
    for block in response.content:
        if getattr(block, "type", None) == "text":
            parts.append(block.text)
    return "".join(parts)


def _retry_wait_seconds(exc: RateLimitError, attempt: int) -> float:
    headers = getattr(getattr(exc, "response", None), "headers", None)
    if headers:
        retry_after = headers.get("retry-after") or headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after) + 2
            except (TypeError, ValueError):
                pass
    return min(120, 45 * (attempt + 1))


def count_message_tokens(
    *,
    model: str,
    system: str,
    user_message: str,
) -> int:
    """Preflight token count via Anthropic ``messages.count_tokens`` (no generation)."""
    client = _client()
    result = client.messages.count_tokens(
        model=model,
        system=system,
        messages=[{"role": "user", "content": user_message}],
    )
    return int(result.input_tokens)


def complete(
    *,
    model: str,
    logical_model: str,
    system: str,
    user_message: str,
    step: str,
    tracker: CostTracker,
    max_tokens: int = 16_384,
    use_stream: bool = False,
    pace_after: bool = True,
) -> str:
    """Call Anthropic messages API with retries and optional streaming."""
    client = _client()
    logger.info("Anthropic call step=%s model=%s stream=%s", step, model, use_stream)

    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            if use_stream:
                text, usage = _complete_streaming(
                    client,
                    model=model,
                    system=system,
                    user_message=user_message,
                    max_tokens=max_tokens,
                )
            else:
                response = client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    system=system,
                    messages=[{"role": "user", "content": user_message}],
                )
                usage = response.usage
                text = _extract_text(response)

            u = UsageBreakdown.from_api(usage)
            tracker.record_usage(step=step, api_model=model, usage=usage)
            logger.info(
                "Anthropic usage step=%s in=%s out=%s cache_write=%s cache_read=%s",
                step,
                f"{u.input_tokens:,}",
                f"{u.output_tokens:,}",
                f"{u.cache_creation_input_tokens:,}",
                f"{u.cache_read_input_tokens:,}",
            )
            if pace_after and logical_model == SONNET_LOGICAL:
                pause_for_rate_limit(u.input_tokens)
            return text
        except RateLimitError as e:
            last_exc = e
            wait = _retry_wait_seconds(e, attempt)
            logger.warning(
                "Rate limited on %s (attempt %d/%d), sleeping %.0fs",
                step,
                attempt + 1,
                MAX_RETRIES,
                wait,
            )
            time.sleep(wait)
        except anthropic.APIStatusError as e:
            if getattr(e, "status_code", None) == 429:
                last_exc = e
                wait = min(120.0, 45.0 * (attempt + 1))
                logger.warning(
                    "HTTP 429 on %s (attempt %d/%d), sleeping %.0fs",
                    step,
                    attempt + 1,
                    MAX_RETRIES,
                    wait,
                )
                time.sleep(wait)
                continue
            raise

    raise last_exc or RuntimeError(f"Anthropic call failed after {MAX_RETRIES} retries: {step}")


def _complete_streaming(
    client: anthropic.Anthropic,
    *,
    model: str,
    system: str,
    user_message: str,
    max_tokens: int,
) -> tuple[str, Any]:
    """Stream a long response (required for large Opus synthesis requests)."""
    parts: list[str] = []
    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user_message}],
    ) as stream:
        for chunk in stream.text_stream:
            parts.append(chunk)
        final = stream.get_final_message()
    return "".join(parts), final.usage


def extract_text_from_response(response: Any) -> str:
    return _extract_text(response)
