"""DeepSeek list prices applied to OpenAI-format ``usage`` fields."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

DEEPSEEK_RATES_PER_MTOK: dict[str, tuple[float, float, float]] = {
    "deepseek-v4-flash": (0.14, 0.0028, 0.28),
    "deepseek-v4-pro": (0.435, 0.003625, 0.87),
    "deepseek-chat": (0.14, 0.0028, 0.28),
    "deepseek-reasoner": (0.14, 0.0028, 0.28),
}

PRICING_NOTE = (
    "Token counts from DeepSeek Chat Completions API response.usage. "
    "USD estimated from published api-docs.deepseek.com pricing "
    "(input cache miss, cache hit, output)."
)


@dataclass(frozen=True)
class UsageBreakdown:
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int

    @classmethod
    def from_openai(cls, usage: Any) -> UsageBreakdown:
        cached = 0
        details = getattr(usage, "prompt_tokens_details", None)
        if details is not None:
            cached = int(getattr(details, "cached_tokens", 0) or 0)
        prompt = int(getattr(usage, "prompt_tokens", 0) or 0)
        return cls(
            input_tokens=max(0, prompt - cached),
            output_tokens=int(getattr(usage, "completion_tokens", 0) or 0),
            cache_read_input_tokens=cached,
        )


@dataclass(frozen=True)
class CostBreakdown:
    input_cost_usd: float
    output_cost_usd: float
    cache_read_cost_usd: float

    @property
    def total_cost_usd(self) -> float:
        return self.input_cost_usd + self.output_cost_usd + self.cache_read_cost_usd


def pricing_key_for_model(api_model: str) -> str:
    model = (api_model or "").strip().lower()
    for key in sorted(DEEPSEEK_RATES_PER_MTOK, key=lambda k: len(k), reverse=True):
        if key in model:
            return key
    return model


def cost_from_usage(usage: Any, *, api_model: str) -> tuple[UsageBreakdown, CostBreakdown]:
    breakdown = UsageBreakdown.from_openai(usage)
    key = pricing_key_for_model(api_model)
    rates = DEEPSEEK_RATES_PER_MTOK.get(key)
    if not rates:
        return breakdown, CostBreakdown(0.0, 0.0, 0.0)

    input_rate, cache_hit_rate, output_rate = rates
    m = 1_000_000.0
    costs = CostBreakdown(
        input_cost_usd=breakdown.input_tokens * input_rate / m,
        output_cost_usd=breakdown.output_tokens * output_rate / m,
        cache_read_cost_usd=breakdown.cache_read_input_tokens * cache_hit_rate / m,
    )
    return breakdown, costs


def estimate_input_cost(input_tokens: int, *, api_model: str) -> float:
    key = pricing_key_for_model(api_model)
    rates = DEEPSEEK_RATES_PER_MTOK.get(key)
    if not rates:
        return 0.0
    return input_tokens * rates[0] / 1_000_000.0
