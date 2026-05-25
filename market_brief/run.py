"""CLI entrypoint for the market brief.

Run inside the backend container (repo mounted at /app):

    docker compose exec backend python -m market_brief.run
    docker compose exec backend python -m market_brief.run --asof 2026-05-15
    docker compose exec backend python -m market_brief.run --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys

from market_brief import config
from market_brief.pipeline import run_brief
from market_brief.topics import load_topics


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        stream=sys.stdout,
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Daily market brief generator")
    p.add_argument("--asof", help="Date label for the run (YYYY-MM-DD). Defaults to today (UTC).")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the topic list and exit without making API calls.",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument(
        "--qa-log",
        action="store_true",
        help="Write qa_funnel.md with ingest/summarize funnel detail (off by default).",
    )
    args = p.parse_args()

    _setup_logging(args.verbose)

    if args.dry_run:
        from market_brief.ingest import collect_ticker_universe

        topics = load_topics()
        universe = collect_ticker_universe(topics)
        mode = "web probes" if config.USE_WEB_PROBES else "Benzinga ingest"
        print(f"Would run {mode} for {len(topics)} topics, {len(universe)} tickers:")
        for t in topics:
            tickers = f" [{', '.join(t.tickers[:6])}{'…' if len(t.tickers) > 6 else ''}]" if t.tickers else ""
            print(f"  · [{t.kind:6s}] {t.name}{tickers}")
        return 0

    qa_log = args.qa_log or config.QA_LOG_ENABLED
    outdir = run_brief(asof=args.asof, qa_log=qa_log)
    print(f"\nBrief written to: {outdir}")
    print(f"  Markdown:   {outdir / '02_brief.md'}")
    print(f"  JSON:       {outdir / '02_brief.json'}")
    print(f"  Raw news:   {outdir / '00_news/'}")
    print(f"  Summaries:  {outdir / '01_summaries/'}")
    print(f"  Usage:      {outdir / 'usage.json'}")
    if qa_log:
        print(f"  QA funnel:  {outdir / 'qa_funnel.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
