"""CLI entrypoint for the market brief (delegates to run_pipeline).

Run inside the backend container:

    docker compose exec backend python -m market_brief.run
    docker compose exec backend python -m market_brief.run --asof 2026-05-31
    docker compose exec backend python -m market_brief.run --skip-ingest
    docker compose exec backend python -m market_brief.run --skip-llm
    docker compose exec backend python -m market_brief.run --asof 2026-05-31 --resume
    docker compose exec backend python -m market_brief.run --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

from market_brief.topics import load_topics
from market_brief.trading_calendar import ET


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        stream=sys.stdout,
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Daily market brief generator")
    p.add_argument("--asof", "--date", dest="date", help="YYYY-MM-DD (default: today ET)")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print screener universe and exit without API calls.",
    )
    p.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip Benzinga ingest; run Anthropic Steps 3–4 on existing source/",
    )
    p.add_argument(
        "--skip-llm",
        action="store_true",
        help="Ingest only (rewrite source/); skip Anthropic synthesis",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Retry failed summaries + synthesis on existing source/ (implies --skip-ingest)",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    _setup_logging(args.verbose)

    if args.dry_run:
        from market_brief.screener_universe import build_screener_universe

        asof = args.date or datetime.now(ET).strftime("%Y-%m-%d")
        topics = load_topics()
        slices, lineage, fetch_syms = build_screener_universe(asof)
        print(f"Would run Benzinga ingest + Anthropic brief for {asof}")
        print(
            f"  Ticker universe: {lineage['unique_tickers_assigned']} symbols "
            f"({lineage['slice_count']} sections, exclusive dedupe)"
        )
        print(f"  Benzinga per-ticker pulls: {len(fetch_syms)} → source/ticker/<SYM>/")
        print(f"  Overview: user_data/market_brief/{asof}/source/ticker_universe/overview.md")
        for sl in slices:
            if not sl.selection:
                continue
            tickers = ", ".join(r["ticker"] for r in sl.selection)
            print(f"  · {sl.label}: {tickers}")
        print(f"  Themes loaded: {len(topics)}")
        return 0

    if args.skip_ingest and args.skip_llm:
        print("Error: cannot use --skip-ingest and --skip-llm together", file=sys.stderr)
        return 1
    if args.resume and args.skip_llm:
        print("Error: cannot use --resume with --skip-llm", file=sys.stderr)
        return 1

    from market_brief.run_pipeline import main as pipeline_main

    return pipeline_main(
        date=args.date,
        skip_ingest=args.skip_ingest,
        skip_llm=args.skip_llm,
        resume=args.resume,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    raise SystemExit(main())
