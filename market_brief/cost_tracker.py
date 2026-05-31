"""Track Anthropic API usage and write run_costs.json."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_brief.anthropic_pricing import (
    PRICING_NOTE,
    UsageBreakdown,
    cost_from_usage,
    pricing_key_for_model,
)

MODEL_LABELS: dict[str, str] = {
    "claude-sonnet-4-5": "Sonnet",
    "claude-sonnet-4-6": "Sonnet",
    "claude-opus-4-5": "Opus",
    "claude-opus-4-6": "Opus",
    "claude-opus-4-1": "Opus",
    "claude-haiku-4-5": "Haiku",
}


@dataclass
class CostRecord:
    step: str
    api_model: str
    pricing_model: str
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int
    input_cost_usd: float
    output_cost_usd: float
    cache_write_cost_usd: float
    cache_read_cost_usd: float
    total_cost_usd: float

    # Legacy field for UI rows that still read ``model``.
    @property
    def model(self) -> str:
        return self.pricing_model

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["model"] = self.pricing_model
        return row


@dataclass
class CostTracker:
    """Accumulates API call records and persists run_costs.json."""

    outdir: Path
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    calls: list[CostRecord] = field(default_factory=list)
    current_step: str | None = None

    def record_usage(
        self,
        *,
        step: str,
        api_model: str,
        usage: Any,
        replace_step: bool = True,
    ) -> CostRecord:
        """Record one API call using token counts from ``response.usage``."""
        if replace_step:
            self.calls = [c for c in self.calls if c.step != step]

        breakdown, costs = cost_from_usage(usage, api_model=api_model)
        pricing_model = pricing_key_for_model(api_model)

        rec = CostRecord(
            step=step,
            api_model=api_model,
            pricing_model=pricing_model,
            input_tokens=breakdown.input_tokens,
            output_tokens=breakdown.output_tokens,
            cache_creation_input_tokens=breakdown.cache_creation_input_tokens,
            cache_read_input_tokens=breakdown.cache_read_input_tokens,
            input_cost_usd=round(costs.input_cost_usd, 6),
            output_cost_usd=round(costs.output_cost_usd, 6),
            cache_write_cost_usd=round(costs.cache_write_cost_usd, 6),
            cache_read_cost_usd=round(costs.cache_read_cost_usd, 6),
            total_cost_usd=round(costs.total_cost_usd, 6),
        )
        self.calls.append(rec)
        self.flush()
        return rec

    @property
    def total_cost_usd(self) -> float:
        return sum(c.total_cost_usd for c in self.calls)

    def summary_dict(self) -> dict[str, Any]:
        total_in = sum(c.input_tokens for c in self.calls)
        total_out = sum(c.output_tokens for c in self.calls)
        total_cache_write = sum(c.cache_creation_input_tokens for c in self.calls)
        total_cache_read = sum(c.cache_read_input_tokens for c in self.calls)
        return {
            "started_at": self.started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "call_count": len(self.calls),
            "current_step": self.current_step,
            "pricing_note": PRICING_NOTE,
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "total_cache_creation_input_tokens": total_cache_write,
            "total_cache_read_input_tokens": total_cache_read,
            "total_cost_usd": round(self.total_cost_usd, 4),
            "calls": [c.to_dict() for c in self.calls],
        }

    def flush(self) -> None:
        path = self.outdir / "run_costs.json"
        self.outdir.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.summary_dict(), indent=2), encoding="utf-8")

    def set_current_step(self, step: str | None) -> None:
        self.current_step = step
        self.flush()

    @classmethod
    def load(cls, outdir: Path) -> dict[str, Any] | None:
        path = outdir / "run_costs.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    @classmethod
    def load_or_create(cls, outdir: Path) -> CostTracker:
        """Restore prior call records when resuming a run."""
        tracker = cls(outdir=outdir)
        data = cls.load(outdir)
        if not data:
            return tracker
        tracker.started_at = data.get("started_at") or tracker.started_at
        for row in data.get("calls") or []:
            api_model = row.get("api_model") or row.get("model") or ""
            tracker.calls.append(
                CostRecord(
                    step=row["step"],
                    api_model=api_model,
                    pricing_model=row.get("pricing_model")
                    or pricing_key_for_model(api_model),
                    input_tokens=int(row.get("input_tokens", 0)),
                    output_tokens=int(row.get("output_tokens", 0)),
                    cache_creation_input_tokens=int(
                        row.get("cache_creation_input_tokens", 0)
                    ),
                    cache_read_input_tokens=int(
                        row.get("cache_read_input_tokens", 0)
                    ),
                    input_cost_usd=float(row.get("input_cost_usd", 0)),
                    output_cost_usd=float(row.get("output_cost_usd", 0)),
                    cache_write_cost_usd=float(
                        row.get("cache_write_cost_usd", 0)
                    ),
                    cache_read_cost_usd=float(row.get("cache_read_cost_usd", 0)),
                    total_cost_usd=float(row.get("total_cost_usd", 0)),
                )
            )
        return tracker
