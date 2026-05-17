"""CLI entrypoint for the market brief.

Usage:
    python -m market_brief.run                 # today (UTC)
    python -m market_brief.run --asof 2026-05-15
    python -m market_brief.run --dry-run       # list topics, no API calls
"""

from __future__ import annotations

import argparse
import logging
import sys

from market_brief import config
from market_brief.pipeline import load_topics, run_brief


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
    args = p.parse_args()

    _setup_logging(args.verbose)

    if args.dry_run:
        topics = load_topics()
        print(f"Would probe {len(topics)} topics with model={config.PROBE_MODEL}:")
        for t in topics:
            tickers = f" [{', '.join(t.tickers[:6])}{'…' if len(t.tickers) > 6 else ''}]" if t.tickers else ""
            print(f"  · [{t.kind:6s}] {t.name}{tickers}")
        return 0

    outdir = run_brief(asof=args.asof)
    print(f"\nBrief written to: {outdir}")
    print(f"  Markdown: {outdir / '02_brief.md'}")
    print(f"  JSON:     {outdir / '02_brief.json'}")
    print(f"  Probes:   {outdir / '01_probes/'}")
    print(f"  Usage:    {outdir / 'usage.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
