"""Track Anthropic API usage and write run_costs.json."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# USD per million tokens (mid-2025 Anthropic pricing)
PRICING_PER_MTOK: dict[str, tuple[float, float]] = {
    "claude-sonnet-4-5": (3.0, 15.0),
    "claude-opus-4-5": (15.0, 75.0),
}

MODEL_LABELS: dict[str, str] = {
    "claude-sonnet-4-5": "Sonnet",
    "claude-opus-4-5": "Opus",
}


@dataclass
class CostRecord:
    step: str
    model: str
    input_tokens: int
    output_tokens: int
    input_cost_usd: float
    output_cost_usd: float
    total_cost_usd: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CostTracker:
    """Accumulates API call records and persists run_costs.json."""

    outdir: Path
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    calls: list[CostRecord] = field(default_factory=list)
    current_step: str | None = None

    def cost_for_tokens(self, model: str, input_tokens: int, output_tokens: int) -> tuple[float, float]:
        in_rate, out_rate = PRICING_PER_MTOK.get(model, (0.0, 0.0))
        input_cost = input_tokens * in_rate / 1_000_000
        output_cost = output_tokens * out_rate / 1_000_000
        return input_cost, output_cost

    def record(
        self,
        *,
        step: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        replace_step: bool = True,
    ) -> CostRecord:
        if replace_step:
            self.calls = [c for c in self.calls if c.step != step]
        input_cost, output_cost = self.cost_for_tokens(model, input_tokens, output_tokens)
        rec = CostRecord(
            step=step,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            input_cost_usd=round(input_cost, 6),
            output_cost_usd=round(output_cost, 6),
            total_cost_usd=round(input_cost + output_cost, 6),
        )
        self.calls.append(rec)
        self.flush()
        return rec

    @property
    def total_cost_usd(self) -> float:
        return sum(c.total_cost_usd for c in self.calls)

    @property
    def step3_cost_usd(self) -> float:
        return sum(
            c.total_cost_usd for c in self.calls if c.model == "claude-sonnet-4-5"
        )

    @property
    def step4_cost_usd(self) -> float:
        return sum(
            c.total_cost_usd for c in self.calls if c.model == "claude-opus-4-5"
        )

    def summary_dict(self) -> dict[str, Any]:
        total_in = sum(c.input_tokens for c in self.calls)
        total_out = sum(c.output_tokens for c in self.calls)
        return {
            "started_at": self.started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "call_count": len(self.calls),
            "current_step": self.current_step,
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "total_cost_usd": round(self.total_cost_usd, 4),
            "step3_cost_usd": round(self.step3_cost_usd, 4),
            "step4_cost_usd": round(self.step4_cost_usd, 4),
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
        path = outdir / "run_costs.json"
        tracker = cls(outdir=outdir)
        data = cls.load(outdir)
        if not data:
            return tracker
        tracker.started_at = data.get("started_at") or tracker.started_at
        for row in data.get("calls") or []:
            tracker.calls.append(
                CostRecord(
                    step=row["step"],
                    model=row["model"],
                    input_tokens=int(row["input_tokens"]),
                    output_tokens=int(row["output_tokens"]),
                    input_cost_usd=float(row["input_cost_usd"]),
                    output_cost_usd=float(row["output_cost_usd"]),
                    total_cost_usd=float(row["total_cost_usd"]),
                )
            )
        return tracker
