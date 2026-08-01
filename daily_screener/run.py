"""CLI: python -m daily_screener.run [options]"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from daily_screener.pipeline import PipelineOptions, run as run_pipeline
from daily_screener.utils import io


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="daily_screener.run",
        description="Daily shortlist screening agent",
    )
    p.add_argument("--date", default=None, help="YYYY-MM-DD; defaults to today (US/Eastern)")
    p.add_argument(
        "--from-stage",
        type=int,
        default=1,
        choices=[1, 2, 3, 4, 5, 6],
        help="Start at this stage (re-uses earlier artifacts). Default 1.",
    )
    p.add_argument(
        "--to-stage",
        type=int,
        default=6,
        choices=[1, 2, 3, 4, 5, 6],
        help="Stop after this stage. Default 6.",
    )
    p.add_argument("--max-tickers", type=int, default=None, help="Cap universe / LLM stages for cheap testing")
    p.add_argument("--force-refresh-themes", action="store_true", help="Ignore theme caches in stage 3")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def setup_logging(verbose: bool, date_str: str):
    log_path = io.run_dir(date_str) / "run.log"
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    level = logging.DEBUG if verbose else logging.INFO

    handlers = [logging.StreamHandler(sys.stderr), logging.FileHandler(log_path)]
    for h in handlers:
        h.setFormatter(logging.Formatter(fmt))

    root = logging.getLogger()
    root.handlers = handlers
    root.setLevel(level)


def main(argv=None):
    args = parse_args(argv)
    date_str = args.date or io.today_str()
    setup_logging(args.verbose, date_str)

    opts = PipelineOptions(
        date=date_str,
        from_stage=args.from_stage,
        to_stage=args.to_stage,
        max_tickers=args.max_tickers,
        force_refresh_themes=args.force_refresh_themes,
    )
    summary = run_pipeline(opts)
    # Persist a small summary file alongside the artifacts.
    io.write_json(io.run_dir(date_str) / "run_summary.json", summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
