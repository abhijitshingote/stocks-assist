"""Topic loading for sectors + user themes."""

from __future__ import annotations

import json
from dataclasses import dataclass

from market_brief import config


@dataclass
class Topic:
    name: str
    desc: str
    tickers: list[str]
    kind: str  # "sector" | "theme" | "macro"

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "desc": self.desc,
            "tickers": self.tickers,
            "kind": self.kind,
        }


def load_topics() -> list[Topic]:
    topics: list[Topic] = []

    for s in config.SECTORS:
        topics.append(
            Topic(
                name=s["name"],
                desc=s.get("desc", ""),
                tickers=[],
                kind="sector",
            )
        )

    if config.USE_USER_THEMES and config.THEMES_FILE.exists():
        with config.THEMES_FILE.open("r", encoding="utf-8") as f:
            raw = json.load(f) or []
        for t in raw[: config.MAX_THEMES]:
            topics.append(
                Topic(
                    name=t.get("name", "").strip(),
                    desc=(t.get("desc") or "").strip(),
                    tickers=t.get("tickers") or [],
                    kind="theme",
                )
            )

    seen: set[str] = set()
    deduped: list[Topic] = []
    for t in topics:
        key = t.name.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(t)
    return deduped
