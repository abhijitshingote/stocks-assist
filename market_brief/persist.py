"""Write run artifacts to disk."""

from __future__ import annotations

import re
from pathlib import Path

from market_brief.types import ProbeResult


def _slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "topic"


def persist_summaries(results: list[ProbeResult], outdir: Path) -> None:
    summaries_dir = outdir / "01_summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    for r in results:
        slug = _slugify(r.topic_name)
        fname = f"{slug}__{r.kind}.md"
        body = (
            f"# {r.topic_name} — {r.kind}\n\n"
            f"_kind: {r.topic_kind} · elapsed: {r.elapsed_s:.1f}s_\n\n"
        )
        if r.error:
            body += f"> **ERROR**: {r.error}\n\n"
        body += r.content or "_(no content)_\n"
        (summaries_dir / fname).write_text(body, encoding="utf-8")
