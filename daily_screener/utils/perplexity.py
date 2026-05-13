"""Thin Perplexity API client.

Mirrors the pattern in backend/app.py::call_perplexity_api but lives standalone
so the screening pipeline doesn't depend on the Flask app being importable.

Also includes a process-wide usage accumulator so callers (and the pipeline
orchestrator) can report token/request totals and an estimated $ cost after a
run.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

import requests

from daily_screener import config

logger = logging.getLogger(__name__)

_API_URL = "https://api.perplexity.ai/chat/completions"


class PerplexityError(Exception):
    """Raised when the Perplexity API fails after retries."""


# ---------------------------------------------------------------------------
# Process-wide usage accumulator (thread-safe). Reset at the start of each
# pipeline run; read by the orchestrator at the end.
# ---------------------------------------------------------------------------

# Posted public pricing as of 2026 (USD per 1M tokens). Approximate — actual
# billing also includes per-search-request fees on `sonar`/`sonar-pro`. We
# surface both token cost AND a rough per-request fee so the totals are
# meaningful for budgeting, even if they aren't billing-accurate to the cent.
#
# Sources: https://docs.perplexity.ai/guides/pricing
_PRICING = {
    "sonar": {
        "input_per_1m": 1.0,
        "output_per_1m": 1.0,
        "request_fee": 0.005,  # $5 per 1k requests (low-tier search)
    },
    "sonar-pro": {
        "input_per_1m": 3.0,
        "output_per_1m": 15.0,
        "request_fee": 0.005,  # $5 per 1k requests (high-tier search)
    },
    # Reasoning tiers — included for completeness; the pipeline doesn't use
    # them today but env overrides could.
    "sonar-reasoning": {
        "input_per_1m": 1.0,
        "output_per_1m": 5.0,
        "request_fee": 0.005,
    },
    "sonar-reasoning-pro": {
        "input_per_1m": 2.0,
        "output_per_1m": 8.0,
        "request_fee": 0.005,
    },
}


class _UsageAccumulator:
    """Accumulates Perplexity API usage across all in-process calls.

    Prefers the **official `usage.cost.total_cost` field** returned by the
    Perplexity API on each response — that's the true billable amount. Falls
    back to the `_PRICING` table only if the API didn't surface a cost
    (older API versions, or models not yet returning the field).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_model: dict[str, dict[str, float]] = {}

    def reset(self) -> None:
        with self._lock:
            self._by_model = {}

    def record(
        self,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        reported_cost_usd: float | None = None,
    ) -> None:
        with self._lock:
            row = self._by_model.setdefault(
                model,
                {
                    "requests": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "reported_cost_usd": 0.0,
                    "missing_cost_requests": 0,
                },
            )
            row["requests"] += 1
            row["prompt_tokens"] += int(prompt_tokens or 0)
            row["completion_tokens"] += int(completion_tokens or 0)
            if reported_cost_usd is not None:
                row["reported_cost_usd"] += float(reported_cost_usd)
            else:
                row["missing_cost_requests"] += 1

    def snapshot(self) -> dict[str, Any]:
        """Return a structured summary with $ cost per model and total."""
        with self._lock:
            rows = {m: dict(v) for m, v in self._by_model.items()}

        total_cost = 0.0
        per_model: dict[str, Any] = {}
        for model, row in rows.items():
            # API-reported cost is authoritative when present.
            reported = float(row.get("reported_cost_usd") or 0.0)
            missing = int(row.get("missing_cost_requests") or 0)

            # For any requests where the API didn't return a cost, fall back
            # to the pricing table so the total isn't silently understated.
            fallback = 0.0
            if missing and (price := _PRICING.get(model)):
                # Allocate the un-priced tokens proportionally and add the
                # per-request fee for the missing-cost requests.
                req_with_cost = row["requests"] - missing
                if row["requests"] > 0:
                    share = missing / row["requests"]
                else:
                    share = 0.0
                in_cost = row["prompt_tokens"] * share / 1_000_000.0 * price["input_per_1m"]
                out_cost = row["completion_tokens"] * share / 1_000_000.0 * price["output_per_1m"]
                req_cost = missing * price["request_fee"]
                fallback = in_cost + out_cost + req_cost

            cost = round(reported + fallback, 4)
            per_model[model] = {
                "requests": int(row["requests"]),
                "prompt_tokens": int(row["prompt_tokens"]),
                "completion_tokens": int(row["completion_tokens"]),
                "reported_cost_usd": round(reported, 4),
                "fallback_cost_usd": round(fallback, 4) if fallback else 0.0,
                "cost_usd": cost,
                "missing_cost_requests": missing,
            }
            total_cost += cost

        return {
            "per_model": per_model,
            "total_requests": sum(int(r["requests"]) for r in rows.values()),
            "total_prompt_tokens": sum(int(r["prompt_tokens"]) for r in rows.values()),
            "total_completion_tokens": sum(int(r["completion_tokens"]) for r in rows.values()),
            "cost_usd_total": round(total_cost, 4) if rows else 0.0,
            "pricing_note": (
                "Uses Perplexity's API-reported `usage.cost.total_cost` per "
                "response where available (authoritative). Falls back to the "
                "_PRICING table in utils/perplexity.py for any response that "
                "did not include a cost field."
            ),
        }


USAGE = _UsageAccumulator()


def call_perplexity(
    prompt: str,
    model: str | None = None,
    max_tokens: int = 1500,
    temperature: float = 0.1,
    timeout: int | None = None,
) -> str:
    """Call Perplexity and return the raw text content.

    Retries on 429 / 5xx up to NEWS_RETRY_ATTEMPTS times with backoff.
    Raises PerplexityError on permanent failure (auth, no credits, etc).

    Token usage from the response is accumulated into the module-level
    `USAGE` so the pipeline can report a per-run cost estimate.
    """
    api_key = os.getenv("PERPLEXITY_API_KEY")
    if not api_key:
        raise PerplexityError("PERPLEXITY_API_KEY not set in environment")

    model = model or config.PERPLEXITY_MODEL
    timeout = timeout or config.PERPLEXITY_TIMEOUT_SECONDS

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature,
        "return_citations": False,
    }

    last_err: str | None = None
    for attempt in range(config.NEWS_RETRY_ATTEMPTS + 1):
        try:
            r = requests.post(_API_URL, headers=headers, json=payload, timeout=timeout)
            if r.status_code == 200:
                data = r.json()
                # Record token usage + API-reported cost for the run total.
                usage = data.get("usage") or {}
                cost_block = usage.get("cost") or {}
                reported_cost = cost_block.get("total_cost")
                USAGE.record(
                    model,
                    int(usage.get("prompt_tokens") or 0),
                    int(usage.get("completion_tokens") or 0),
                    reported_cost_usd=(
                        float(reported_cost) if reported_cost is not None else None
                    ),
                )
                choices = data.get("choices") or []
                if choices:
                    return choices[0].get("message", {}).get("content", "") or ""
                raise PerplexityError("Perplexity returned no choices")
            if r.status_code == 402:
                raise PerplexityError("INSUFFICIENT_CREDITS")
            if r.status_code == 401:
                # Perplexity returns 401 for both bad keys AND for quota
                # exhaustion (error.type == "insufficient_quota"). Disambiguate
                # so callers can tell "fix your key" from "top up credits".
                body = (r.text or "").lower()
                if "insufficient_quota" in body or "quota" in body:
                    raise PerplexityError("INSUFFICIENT_QUOTA")
                raise PerplexityError("AUTH_FAILED")
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                last_err = f"http {r.status_code}: {r.text[:300]}"
                logger.warning(
                    "perplexity transient error (attempt %s/%s): %s",
                    attempt + 1,
                    config.NEWS_RETRY_ATTEMPTS + 1,
                    last_err,
                )
                time.sleep(config.REQUEST_RETRY_BACKOFF_SECONDS * (attempt + 1))
                continue
            raise PerplexityError(f"http {r.status_code}: {r.text[:300]}")
        except requests.RequestException as e:
            last_err = str(e)
            logger.warning(
                "perplexity request exception (attempt %s/%s): %s",
                attempt + 1,
                config.NEWS_RETRY_ATTEMPTS + 1,
                last_err,
            )
            time.sleep(config.REQUEST_RETRY_BACKOFF_SECONDS * (attempt + 1))

    raise PerplexityError(f"Perplexity failed after retries: {last_err}")
