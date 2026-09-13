"""Weekly news recap pipeline.

    fetch one Sat..Fri week
      -> dedupe restatements          (free)
      -> mechanical prefilter         (free, ~-36%)
      -> CALL 1  Haiku screen         ~$0.14  all titles -> ~250
      -> CALL 2  Opus final cut       ~$0.05  ~250 -> ~70
      -> CALL 3  Opus brief           ~$0.35  ~70 bodies -> markdown recap
      -> persist                                    total ~$0.55

Run inside the backend container:

    docker compose exec backend python -m news_digest.run_pipeline
    docker compose exec backend python -m news_digest.run_pipeline --week-ending 2026-09-12
    docker compose exec backend python -m news_digest.run_pipeline --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from daily_screener.utils.db import get_session

from news_digest import brief, config, fetch, llm, persist, prefilter, select

logger = logging.getLogger(__name__)


def _label(start: datetime, stop: datetime) -> str:
    """Inclusive human label, e.g. 'Sep 05 - Sep 11, 2026'."""
    tz = ZoneInfo(config.TIMEZONE)
    last_day = (stop - timedelta(days=1)).astimezone(tz)
    return f"{start.astimezone(tz).strftime('%b %d')} - {last_day.strftime('%b %d, %Y')}"


def _run_key(stop: datetime) -> str:
    tz = ZoneInfo(config.TIMEZONE)
    return f"week-{(stop - timedelta(days=1)).astimezone(tz).strftime('%Y-%m-%d')}"


def gather(asof: datetime | None = None, *, on_page=None):
    """Everything up to the first LLM call. Free — also drives --dry-run."""
    start, stop = fetch.week_bounds(asof)
    articles, fetch_stats = fetch.fetch_universe(start, stop, on_page=on_page)
    deduped, collapsed = fetch.dedupe_titles(articles)
    kept, dropped = prefilter.apply(deduped)

    stats = {
        "fetch": fetch_stats,
        "dedupe": {"kept": len(deduped), "collapsed": collapsed},
        "prefilter": {"kept": len(kept), "dropped": dropped},
    }
    logger.info(
        "universe=%d deduped=%d prefiltered=%d %s",
        len(articles),
        len(deduped),
        len(kept),
        dropped,
    )
    return start, stop, deduped, kept, stats


def run(*, asof: datetime | None = None, select_n: int | None = None) -> str:
    session = get_session()
    run_row = None
    try:
        persist.ensure_tables(session.get_bind())
        start, stop = fetch.week_bounds(asof)
        run_key = _run_key(stop)
        window_label = _label(start, stop)

        outdir = config.OUTPUTS_DIR / run_key
        tracker = llm.CostTracker(outdir=outdir)

        run_row = persist.start_run(
            session,
            run_key=run_key,
            window_label=window_label,
            window_start=start,
            window_end=stop,
        )
        logger.info("run_key=%s window=%s", run_key, window_label)

        _, _, deduped, candidates, stats = gather(
            asof,
            on_page=lambda pages, total: persist.set_stage(
                session, run_row, f"fetch: page {pages} ({total} articles)"
            ),
        )
        persist.set_stage(
            session,
            run_row,
            f"screen: {len(candidates)} titles",
            universe_count=len(deduped),
            prefiltered_count=len(candidates),
        )
        if not candidates:
            raise RuntimeError("prefilter left 0 candidates")

        screened, screen_stats = select.screen(candidates, tracker=tracker)
        stats["screen"] = screen_stats
        if not screened:
            raise RuntimeError("screen returned 0 usable ids")

        persist.set_stage(
            session,
            run_row,
            f"select: {len(screened)} titles",
            screened_count=len(screened),
        )
        selected, select_stats = select.final_cut(screened, tracker=tracker, target_n=select_n)
        stats["select"] = select_stats
        if not selected:
            raise RuntimeError("select returned 0 usable ids")

        # Persist sources before the brief call so a failure there does not
        # throw away the selection work.
        persist.set_stage(
            session, run_row, f"brief: {len(selected)} bodies", selected_count=len(selected)
        )
        persist.write_items(session, run_row, selected)

        narrative, brief_stats = brief.write(
            selected, window_label=window_label, tracker=tracker
        )
        stats["brief"] = brief_stats

        persist.set_stage(session, run_row, "persist")
        persist.finish_run(
            session,
            run_row,
            narrative_md=narrative,
            cost_usd=tracker.total_cost_usd,
            stats=stats,
        )
        logger.info(
            "done run_key=%s selected=%d cost=$%.2f",
            run_key,
            len(selected),
            tracker.total_cost_usd,
        )
        return run_key
    except Exception as e:  # noqa: BLE001
        if run_row is not None:
            try:
                persist.fail_run(session, run_row, str(e))
            except Exception:  # noqa: BLE001
                logger.exception("could not record failure")
        raise
    finally:
        session.close()


def _dry_run(asof: datetime | None) -> None:
    """Fetch + prefilter only, then price the screen call. No generation."""
    from news_digest import prompts

    start, stop, deduped, kept, stats = gather(asof)
    print(f"window: {_label(start, stop)}  ({_run_key(stop)})")
    print(f"fetched:    {stats['fetch']['api_rows']}")
    print(f"deduped:    {len(deduped)}  (collapsed {stats['dedupe']['collapsed']})")
    print(f"prefilter:  {len(kept)}  dropped {stats['prefilter']['dropped']}")

    tokens = llm.count_tokens(
        model=config.SCREEN_MODEL,
        system=prompts.SCREEN_SYSTEM,
        user_message=prompts.screen_user_message(kept, target_n=config.SCREEN_TARGET_N),
    )
    in_rate, _ = llm.RATES[llm.pricing_key(config.SCREEN_MODEL)]
    print(f"screen input: {tokens:,} tokens on {config.SCREEN_MODEL} = ${tokens * in_rate / 1e6:.3f}")


def main() -> int:
    p = argparse.ArgumentParser(description="Weekly news recap")
    p.add_argument(
        "--week-ending",
        default=None,
        metavar="YYYY-MM-DD",
        help="run the last complete week ending on or before this date (default: today)",
    )
    p.add_argument(
        "--select-n",
        type=int,
        default=None,
        help=f"stories in the recap (default {config.SELECT_TARGET_N})",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch and prefilter only, print counts and the screen-call price",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        stream=sys.stdout,
    )

    asof = None
    if args.week_ending:
        asof = datetime.strptime(args.week_ending, "%Y-%m-%d").replace(
            tzinfo=ZoneInfo(config.TIMEZONE)
        )

    try:
        if args.dry_run:
            _dry_run(asof)
        else:
            run(asof=asof, select_n=args.select_n)
    except Exception as e:  # noqa: BLE001
        logger.error("pipeline failed: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
