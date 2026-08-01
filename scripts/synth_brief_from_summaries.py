#!/usr/bin/env python3
"""Synthesize 02_brief from on-disk topic summaries (no Ollama / no re-ingest).

Use while the main Ollama summarize job is still running, or to retry synth only.

Writes:
  user_data/market_brief/<date>/02_brief.interim.md
  user_data/market_brief/<date>/02_brief.interim.json

Usage (in backend container):
  python scripts/synth_brief_from_summaries.py --asof 2026-05-28
  python scripts/synth_brief_from_summaries.py --asof 2026-05-28 --skip-watch
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from market_brief import config
from market_brief.persist import _slugify
from market_brief.pipeline import synthesize, synthesize_json
from market_brief.summarize import run_watch_probe
from market_brief.topics import Topic, load_topics
from market_brief.types import ProbeResult

logger = logging.getLogger(__name__)


def _read_summary_body(path: Path) -> str:
    text = path.read_text(encoding="utf-8").strip()
    lines = text.splitlines()
    if len(lines) >= 2 and lines[0].startswith("# ") and lines[1].strip().startswith("_kind:"):
        return "\n".join(lines[2:]).strip()
    return text


def _usable_summary_body(path: Path) -> str | None:
    if not path.is_file():
        return None
    body = _read_summary_body(path)
    if not body or body == "_(no content)_":
        return None
    if "> **ERROR**" in body[:400]:
        return None
    return body


def _topics_for_synth() -> list[Topic]:
    topics = load_topics()
    unassigned = Topic(
        name=config.UNASSIGNED_TOPIC_NAME,
        desc=config.UNASSIGNED_TOPIC_DESC,
        tickers=[],
        kind="unassigned",
    )
    return [unassigned] + topics


def load_results_from_disk(outdir: Path) -> tuple[list[ProbeResult], list[str]]:
    """Load final summaries; fall back to .partial.md for in-progress topics."""
    summaries = outdir / "01_summaries"
    notes: list[str] = []
    results: list[ProbeResult] = []

    for topic in _topics_for_synth():
        slug = _slugify(topic.name)
        final_path = summaries / f"{slug}__benzinga.md"
        partial_path = summaries / f"{slug}__benzinga.partial.md"

        content = _usable_summary_body(final_path)
        source = "final"
        if content is None and partial_path.is_file():
            content = _usable_summary_body(partial_path)
            source = "partial"
        if content is None:
            notes.append(f"skipped (no summary): {topic.name}")
            continue

        if source == "partial":
            m = re.search(r"in progress (\d+)/(\d+)", partial_path.read_text(encoding="utf-8")[:200])
            prog = f" ({m.group(1)}/{m.group(2)} articles)" if m else ""
            content = f"_Interim partial summary{prog} — Ollama run still in progress._\n\n{content}"

        results.append(
            ProbeResult(
                topic_name=topic.name,
                topic_kind=topic.kind,
                kind="benzinga",
                content=content,
            )
        )
        notes.append(f"included [{source}]: {topic.name}")

    return results, notes


def load_watch_from_disk(outdir: Path) -> ProbeResult | None:
    path = outdir / "01_summaries" / "watch_next_session__watch.md"
    body = _usable_summary_body(path)
    if not body:
        return None
    return ProbeResult(
        topic_name="Watch next session",
        topic_kind="calendar",
        kind="watch",
        content=body,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Perplexity synth from existing 01_summaries (interim brief)"
    )
    parser.add_argument("--asof", default="2026-05-28", help="Brief date folder (YYYY-MM-DD)")
    parser.add_argument(
        "--skip-watch",
        action="store_true",
        help="Do not call Perplexity watch probe; use watch file if present",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    outdir = config.OUTPUTS_DIR / args.asof
    if not (outdir / "01_summaries").is_dir():
        raise SystemExit(f"No summaries dir: {outdir / '01_summaries'}")

    topic_results, notes = load_results_from_disk(outdir)
    print(f"Loaded {len(topic_results)} topic block(s) for synthesis:")
    for line in notes:
        print(f"  · {line}")

    if not topic_results:
        raise SystemExit("Nothing to synthesize — no final or partial summaries found.")

    watch_result: ProbeResult | None = None
    if args.skip_watch:
        watch_result = load_watch_from_disk(outdir)
        if watch_result:
            print("Using existing watch_next_session__watch.md")
    else:
        print("Running Perplexity watch probe (calendar 24–48h)…")
        watch_result = run_watch_probe(load_topics(), args.asof)
        if watch_result.content:
            from market_brief.persist import persist_summaries

            persist_summaries([watch_result], outdir)

    print("Synthesizing 02_brief.interim.md via Perplexity…")
    brief_md = synthesize(topic_results, watch_result, args.asof)
    md_path = outdir / "02_brief.interim.md"
    md_path.write_text(brief_md, encoding="utf-8")
    print(f"Wrote {md_path} ({len(brief_md)} chars)")

    print("Synthesizing 02_brief.interim.json…")
    brief_json = synthesize_json(brief_md, args.asof)
    json_path = outdir / "02_brief.interim.json"
    json_path.write_text(
        json.dumps(brief_json, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Wrote {json_path}")

    meta = {
        "asof": args.asof,
        "topics_included": len(topic_results),
        "notes": notes,
        "watch_included": bool(watch_result and watch_result.content),
    }
    (outdir / "02_brief.interim.meta.json").write_text(
        json.dumps(meta, indent=2),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
