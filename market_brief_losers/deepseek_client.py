"""Thin DeepSeek (OpenAI-compatible) wrapper for losers brief synthesis."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from openai import APIStatusError, OpenAI, RateLimitError

from market_brief_losers.cost_tracker import CostTracker
from market_brief_losers.deepseek_pricing import UsageBreakdown

logger = logging.getLogger(__name__)

SYNTH_MODEL = os.getenv("MARKET_BRIEF_LOSERS_DEEPSEEK_MODEL", "deepseek-v4-pro")
SYNTH_LOGICAL = SYNTH_MODEL
BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
MAX_RETRIES = int(os.getenv("MARKET_BRIEF_API_MAX_RETRIES", "8"))


def _client() -> OpenAI:
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is not set")
    return OpenAI(api_key=api_key, base_url=BASE_URL)


def _retry_wait_seconds(exc: RateLimitError | APIStatusError, attempt: int) -> float:
    headers = getattr(getattr(exc, "response", None), "headers", None)
    if headers:
        retry_after = headers.get("retry-after") or headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after) + 2
            except (TypeError, ValueError):
                pass
    return min(120.0, 45.0 * (attempt + 1))


def complete(
    *,
    model: str,
    logical_model: str,
    system: str,
    user_message: str,
    step: str,
    tracker: CostTracker,
    max_tokens: int = 16_384,
    use_stream: bool = True,
) -> str:
    """Call DeepSeek chat completions with retries and optional streaming."""
    client = _client()
    logger.info("DeepSeek call step=%s model=%s stream=%s", step, model, use_stream)

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_message},
    ]

    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            if use_stream:
                text, usage = _complete_streaming(
                    client,
                    model=model,
                    messages=messages,
                    max_tokens=max_tokens,
                )
            else:
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    max_tokens=max_tokens,
                )
                usage = response.usage
                text = response.choices[0].message.content or ""

            tracker.record_usage(step=step, api_model=model, usage=usage)
            u = UsageBreakdown.from_openai(usage)
            logger.info(
                "DeepSeek usage step=%s in=%s out=%s cache_read=%s",
                step,
                f"{u.input_tokens + u.cache_read_input_tokens:,}",
                f"{u.output_tokens:,}",
                f"{u.cache_read_input_tokens:,}",
            )
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
        except APIStatusError as e:
            if getattr(e, "status_code", None) == 429:
                last_exc = e
                wait = _retry_wait_seconds(e, attempt)
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

    raise last_exc or RuntimeError(f"DeepSeek call failed after {MAX_RETRIES} retries: {step}")


def _complete_streaming(
    client: OpenAI,
    *,
    model: str,
    messages: list[dict[str, str]],
    max_tokens: int,
) -> tuple[str, Any]:
    parts: list[str] = []
    usage = None
    with client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        stream=True,
        stream_options={"include_usage": True},
    ) as stream:
        for chunk in stream:
            if chunk.usage is not None:
                usage = chunk.usage
            delta = chunk.choices[0].delta.content if chunk.choices else None
            if delta:
                parts.append(delta)
    if usage is None:
        raise RuntimeError("DeepSeek stream ended without usage metadata")
    return "".join(parts), usage
