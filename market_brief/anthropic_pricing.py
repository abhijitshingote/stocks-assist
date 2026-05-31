"""Anthropic list prices applied to API ``usage`` fields (see platform.claude.com/docs pricing)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# USD per million tokens — base input, 5m cache write, cache read, output.
# Opus 4.6+ use standard rates for the full 1M context window (no long-context surcharge).
ANTHROPIC_RATES_PER_MTOK: dict[str, tuple[float, float, float, float]] = {
    "claude-opus-4-6": (5.0, 6.25, 0.50, 25.0),
    "claude-opus-4-5": (5.0, 6.25, 0.50, 25.0),
    "claude-opus-4-1": (15.0, 18.75, 1.50, 75.0),
    "claude-sonnet-4-5": (3.0, 3.75, 0.30, 15.0),
    "claude-sonnet-4-6": (3.0, 3.75, 0.30, 15.0),
    "claude-haiku-4-5": (1.0, 1.25, 0.10, 5.0),
}

PRICING_NOTE = (
    "Token counts from Anthropic Messages API response.usage. "
    "USD estimated from published claude.com/api pricing (input, 5m cache write, "
    "cache read, output) — Anthropic does not return dollar amounts on the response."
)


@dataclass(frozen=True)
class UsageBreakdown:
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int

    @classmethod
    def from_api(cls, usage: Any) -> UsageBreakdown:
        return cls(
            input_tokens=int(getattr(usage, "input_tokens", 0) or 0),
            output_tokens=int(getattr(usage, "output_tokens", 0) or 0),
            cache_creation_input_tokens=int(
                getattr(usage, "cache_creation_input_tokens", 0) or 0
            ),
            cache_read_input_tokens=int(
                getattr(usage, "cache_read_input_tokens", 0) or 0
            ),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_creation_input_tokens": self.cache_creation_input_tokens,
            "cache_read_input_tokens": self.cache_read_input_tokens,
        }


@dataclass(frozen=True)
class CostBreakdown:
    input_cost_usd: float
    output_cost_usd: float
    cache_write_cost_usd: float
    cache_read_cost_usd: float

    @property
    def total_cost_usd(self) -> float:
        return (
            self.input_cost_usd
            + self.output_cost_usd
            + self.cache_write_cost_usd
            + self.cache_read_cost_usd
        )


def pricing_key_for_model(api_model: str) -> str:
    """Map dated API model ids (e.g. claude-opus-4-6) to a pricing row."""
    model = (api_model or "").strip()
    for key in sorted(ANTHROPIC_RATES_PER_MTOK, key=lambda k: len(k), reverse=True):
        if key in model:
            return key
    return model


def cost_from_usage(usage: Any, *, api_model: str) -> tuple[UsageBreakdown, CostBreakdown]:
    """Compute USD from API usage using published Anthropic per-MTok rates."""
    breakdown = UsageBreakdown.from_api(usage)
    key = pricing_key_for_model(api_model)
    rates = ANTHROPIC_RATES_PER_MTOK.get(key)
    if not rates:
        return breakdown, CostBreakdown(0.0, 0.0, 0.0, 0.0)

    input_rate, cache_write_rate, cache_read_rate, output_rate = rates
    m = 1_000_000.0
    costs = CostBreakdown(
        input_cost_usd=breakdown.input_tokens * input_rate / m,
        output_cost_usd=breakdown.output_tokens * output_rate / m,
        cache_write_cost_usd=breakdown.cache_creation_input_tokens * cache_write_rate / m,
        cache_read_cost_usd=breakdown.cache_read_input_tokens * cache_read_rate / m,
    )
    return breakdown, costs


def estimate_input_cost(input_tokens: int, *, api_model: str) -> float:
    """USD for input tokens only (``count_tokens`` preflight)."""
    key = pricing_key_for_model(api_model)
    rates = ANTHROPIC_RATES_PER_MTOK.get(key)
    if not rates:
        return 0.0
    return input_tokens * rates[0] / 1_000_000.0
