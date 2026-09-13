"""Anthropic client + cost tracking. Self-contained: news_digest imports nothing
from market_brief.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anthropic
from anthropic import RateLimitError

logger = logging.getLogger(__name__)

# USD per million tokens: (input, output).
RATES: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5": (1.0, 5.0),
    "claude-sonnet-4-5": (3.0, 15.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-opus-4-5": (5.0, 25.0),
    "claude-opus-4-6": (5.0, 25.0),
}

PRICING_NOTE = (
    "Token counts from the Anthropic Messages API response.usage. USD is estimated "
    "from published per-MTok list prices; the API does not return dollar amounts."
)

MAX_RETRIES = int(os.getenv("NEWS_DIGEST_API_MAX_RETRIES", "6"))


def pricing_key(api_model: str) -> str:
    """Map a (possibly dated) model id onto a rate row."""
    model = (api_model or "").strip()
    for key in sorted(RATES, key=len, reverse=True):
        if key in model:
            return key
    return model


def _rates(api_model: str) -> tuple[float, float]:
    return RATES.get(pricing_key(api_model), (0.0, 0.0))


@dataclass
class CostTracker:
    """Accumulates per-call usage and writes run_costs.json after every call."""

    outdir: Path
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    calls: list[dict] = field(default_factory=list)

    def record(self, *, step: str, api_model: str, usage: Any) -> dict:
        in_tok = int(getattr(usage, "input_tokens", 0) or 0)
        out_tok = int(getattr(usage, "output_tokens", 0) or 0)
        in_rate, out_rate = _rates(api_model)
        in_cost = in_tok * in_rate / 1e6
        out_cost = out_tok * out_rate / 1e6

        rec = {
            "step": step,
            "model": api_model,
            "input_tokens": in_tok,
            "output_tokens": out_tok,
            "input_cost_usd": round(in_cost, 6),
            "output_cost_usd": round(out_cost, 6),
            "total_cost_usd": round(in_cost + out_cost, 6),
        }
        self.calls.append(rec)
        self.flush()
        logger.info(
            "%s: %s in=%s out=%s cost=$%.3f (running $%.3f)",
            step,
            api_model,
            f"{in_tok:,}",
            f"{out_tok:,}",
            rec["total_cost_usd"],
            self.total_cost_usd,
        )
        return rec

    @property
    def total_cost_usd(self) -> float:
        return sum(c["total_cost_usd"] for c in self.calls)

    def summary(self) -> dict:
        return {
            "started_at": self.started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "pricing_note": PRICING_NOTE,
            "call_count": len(self.calls),
            "total_input_tokens": sum(c["input_tokens"] for c in self.calls),
            "total_output_tokens": sum(c["output_tokens"] for c in self.calls),
            "total_cost_usd": round(self.total_cost_usd, 4),
            "calls": self.calls,
        }

    def flush(self) -> None:
        self.outdir.mkdir(parents=True, exist_ok=True)
        (self.outdir / "run_costs.json").write_text(
            json.dumps(self.summary(), indent=2), encoding="utf-8"
        )


def _client() -> anthropic.Anthropic:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")
    return anthropic.Anthropic(api_key=api_key)


def count_tokens(*, model: str, system: str, user_message: str) -> int:
    """Preflight token count. Free — no generation."""
    result = _client().messages.count_tokens(
        model=model,
        system=system,
        messages=[{"role": "user", "content": user_message}],
    )
    return int(result.input_tokens)


def _retry_wait(exc: Exception, attempt: int) -> float:
    headers = getattr(getattr(exc, "response", None), "headers", None)
    if headers:
        retry_after = headers.get("retry-after") or headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after) + 2
            except (TypeError, ValueError):
                pass
    return min(120.0, 30.0 * (attempt + 1))


def complete(
    *,
    model: str,
    system: str,
    user_message: str,
    step: str,
    tracker: CostTracker,
    max_tokens: int,
) -> str:
    """Streamed completion with retry on 429. Streaming avoids the SDK's
    long-request timeout on the multi-minute selection and brief calls.
    """
    client = _client()
    last_exc: Exception | None = None

    for attempt in range(MAX_RETRIES):
        try:
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
            tracker.record(step=step, api_model=model, usage=final.usage)
            return "".join(parts)
        except RateLimitError as e:
            last_exc = e
            wait = _retry_wait(e, attempt)
            logger.warning(
                "rate limited on %s (attempt %d/%d), sleeping %.0fs",
                step,
                attempt + 1,
                MAX_RETRIES,
                wait,
            )
            time.sleep(wait)
        except anthropic.APIStatusError as e:
            if getattr(e, "status_code", None) != 429:
                raise
            last_exc = e
            wait = _retry_wait(e, attempt)
            logger.warning("HTTP 429 on %s, sleeping %.0fs", step, wait)
            time.sleep(wait)

    raise last_exc or RuntimeError(f"{step}: failed after {MAX_RETRIES} retries")
