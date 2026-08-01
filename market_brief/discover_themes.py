"""Discover theme gaps from market-brief routing and propose themes.json updates.

The market brief does **not** auto-edit themes.json. It only uses your curated
list. Stories about tickers not in any theme (e.g. IONQ, QBTS) land in
``00_news/_unassigned.json`` and get a generic summarize pass — not a dedicated
Quantum Computing section.

Workflow:

    # After a brief run (or anytime there is a run under user_data/market_brief/)
    docker compose exec backend python -m market_brief.discover_themes

    # Optional: Perplexity scan for narratives you may be missing
    docker compose exec backend python -m market_brief.discover_themes --web

    # Review user_data/theme_discovery/proposals.json — set "approved": true
    docker compose exec backend python -m market_brief.discover_themes --apply

    # Or approve by id from the CLI
    docker compose exec backend python -m market_brief.discover_themes --approve new-quantum_computing --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path

from market_brief import config
from market_brief.theme_discovery import (
    build_proposals,
    find_latest_run_dir,
    load_proposals,
    load_run_articles,
    merge_web_proposals,
    write_proposals,
)
from market_brief.themes_io import apply_proposals, load_themes, save_themes

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "theme_discovery_gap.md"


def _print_proposals(payload: dict) -> None:
    proposals = payload.get("proposals") or []
    stats = payload.get("stats") or {}
    print(f"\nSource run: {payload.get('source_run')} ({payload.get('run_dir')})")
    print(
        f"Corpus: {stats.get('corpus_articles', '?')} articles · "
        f"unassigned: {stats.get('unassigned_articles', '?')}"
    )
    if not proposals:
        print("\nNo proposals — themes.json looks aligned with this run's unassigned bucket.")
        return
    print(f"\n{len(proposals)} proposal(s) (edit proposals.json and set approved: true):\n")
    for p in proposals:
        flag = "✓" if p.get("approved") else " "
        ev = p.get("evidence") or {}
        print(f"  [{flag}] {p.get('id')} ({p.get('type')})")
        if p.get("type") == "new_theme":
            print(f"      NEW  {p.get('name')}: {', '.join(p.get('tickers') or [])}")
        else:
            print(
                f"      ADD  {p.get('theme_name')}: +{', '.join(p.get('tickers') or [])}"
            )
        reason = ev.get("reason") or ""
        if reason:
            print(f"      why: {reason[:120]}{'…' if len(reason) > 120 else ''}")
        n = ev.get("article_count")
        if n:
            print(f"      evidence: {n} articles")
        print()
    notes = payload.get("notes") or []
    if notes:
        print(f"Notes ({len(notes)} themes with tickers but 0 routed articles — check fetch window):\n")
        for note in notes[:8]:
            ev = note.get("evidence") or {}
            print(f"  · {note.get('theme_name')}: {ev.get('reason', '')[:100]}")
        print()


def _orphan_ticker_histogram(unassigned: list[dict], topics) -> str:
    from market_brief.theme_discovery import covered_tickers
    from market_brief.ingest import article_ticker_symbols

    covered = covered_tickers(topics)
    counts: Counter[str] = Counter()
    for article in unassigned:
        for t in article_ticker_symbols(article):
            if t not in covered:
                counts[t] += 1
    lines = [f"{t}: {n}" for t, n in counts.most_common(25)]
    return "\n".join(lines) if lines else "(none)"


def _fetch_web_proposals(unassigned: list[dict]) -> list[dict]:
    from daily_screener.utils.llm import extract_json
    from daily_screener.utils.perplexity import call_perplexity
    from market_brief.topics import load_topics

    topics = load_topics()
    themes = load_themes()
    headlines = [
        (a.get("title") or "").strip()
        for a in unassigned[:12]
        if (a.get("title") or "").strip()
    ]
    prompt = _PROMPT_PATH.read_text(encoding="utf-8")
    prompt = prompt.replace("{curated_themes_json}", json.dumps(themes, indent=2))
    prompt = prompt.replace(
        "{orphan_ticker_histogram}",
        _orphan_ticker_histogram(unassigned, topics),
    )
    prompt = prompt.replace(
        "{sample_headlines}",
        "\n".join(f"- {h}" for h in headlines) or "(none)",
    )
    raw = call_perplexity(prompt, max_tokens=2000)
    data = extract_json(raw)
    return data if isinstance(data, list) else []


def run_discovery(
    *,
    run_dir: Path | None = None,
    min_articles: int | None = None,
    web: bool = False,
    out_path: Path | None = None,
) -> dict:
    rd = run_dir or find_latest_run_dir()
    if rd is None:
        raise FileNotFoundError(
            f"No market brief run found under {config.OUTPUTS_DIR} (need 00_news/)"
        )
    payload = build_proposals(rd, min_articles=min_articles)
    if web:
        _, unassigned, _ = load_run_articles(rd)
        try:
            web_rows = _fetch_web_proposals(unassigned)
            merge_web_proposals(payload, web_rows, load_themes())
            payload["web_scan"] = True
        except Exception as e:  # noqa: BLE001 — Perplexity/network
            logger.warning("web theme scan failed: %s", e)
            payload["web_error"] = str(e)

    path = write_proposals(payload, out_path)
    payload["proposals_path"] = str(path)
    return payload


def run_apply(
    *,
    proposals_path: Path | None = None,
    approve_ids: list[str] | None = None,
) -> list[str]:
    payload = load_proposals(proposals_path)
    proposals = payload.get("proposals") or []
    if approve_ids:
        ids = set(approve_ids)
        for p in proposals:
            if p.get("id") in ids:
                p["approved"] = True

    themes = load_themes()
    updated, logs = apply_proposals(themes, proposals, only_approved=True)
    if not logs:
        return ["No approved proposals — set approved: true or use --approve ID"]
    save_themes(updated)
    write_proposals(payload, proposals_path)  # persist approval flags
    return logs


def discover_after_brief(run_dir: Path) -> Path | None:
    """Called from pipeline when DISCOVER_THEMES_AFTER_RUN is enabled."""
    try:
        payload = build_proposals(run_dir)
        return write_proposals(payload)
    except OSError as e:
        logger.warning("theme discovery failed: %s", e)
        return None


def main() -> int:
    p = argparse.ArgumentParser(
        description="Propose themes.json updates from market-brief unassigned routing"
    )
    p.add_argument("--run-dir", type=Path, help="Brief run dir (default: latest under outputs)")
    p.add_argument(
        "--min-articles",
        type=int,
        default=None,
        help=f"Min unassigned articles per cluster (default {config.THEME_DISCOVERY_MIN_ARTICLES})",
    )
    p.add_argument("--web", action="store_true", help="Add Perplexity hot-narrative suggestions")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Apply proposals with approved: true to themes.json",
    )
    p.add_argument(
        "--approve",
        action="append",
        default=[],
        metavar="ID",
        help="Mark proposal id approved (repeatable); use with --apply",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Write proposals JSON here (default: user_data/theme_discovery/proposals.json)",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        stream=sys.stdout,
    )

    if args.apply:
        logs = run_apply(approve_ids=args.approve or None)
        for line in logs:
            print(line)
        return 0 if any("added" in l or "extended" in l for l in logs) else 1

    payload = run_discovery(
        run_dir=args.run_dir,
        min_articles=args.min_articles,
        web=args.web,
        out_path=args.output,
    )
    _print_proposals(payload)
    print(f"Wrote: {payload.get('proposals_path')}")
    print("Next: set approved: true on items you want, then run with --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
