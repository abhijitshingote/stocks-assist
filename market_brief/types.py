"""Shared datatypes for the market brief pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ProbeResult:
    topic_name: str
    topic_kind: str
    kind: str  # "benzinga" | "watch" | "overview" | "catalyst"
    content: str
    error: str | None = None
    elapsed_s: float = 0.0
