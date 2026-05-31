"""Market brief pipeline: Benzinga ingest + Anthropic brief.

Run inside the backend container:

    docker compose exec backend python -m market_brief.run_pipeline
    docker compose exec backend python -m market_brief.run_pipeline --date 2026-05-31
    docker compose exec backend python -m market_brief.run_pipeline --skip-ingest
    docker compose exec backend python -m market_brief.run_pipeline --skip-llm-summary
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from market_brief import config
from market_brief.anthropic_client import (
    OPUS_LOGICAL,
    OPUS_MODEL,
    SONNET_LOGICAL,
    SONNET_MODEL,
    complete,
)
from market_brief.cost_tracker import CostTracker
from market_brief.prompts_pipeline import (
    STEP3_SYSTEM_PROMPT,
    STEP4_SYSTEM_PROMPT,
    step3_user_message,
    step4_user_message,
)
from market_brief.source_loader import (
    channel_article_sets,
    channel_output_filename,
    concat_articles,
    parse_ticker_batches_from_overview,
    split_oversized_batch,
    ticker_articles_for_symbols,
)
from market_brief import status as status_mod

logger = logging.getLogger(__name__)

PLACEHOLDER = "# Fact extraction failed\n\n_No material facts extracted for this group._\n"
PLACEHOLDER_MARKER = "Fact extraction failed"


def _is_failed_summary(path: Path) -> bool:
    if not path.exists():
        return True
    text = path.read_text(encoding="utf-8")
    return PLACEHOLDER_MARKER in text[:300]


def _summaries_need_work(summaries_dir: Path, source_dir: Path) -> bool:
    """True if any Step 3 output is missing or a failed placeholder."""
    if not summaries_dir.is_dir():
        return True
    channel_sets = channel_article_sets(source_dir, {})
    for slug, articles in channel_sets.items():
        if not articles:
            continue
        out_path = summaries_dir / channel_output_filename(slug)
        if _is_failed_summary(out_path):
            return True
    overview_path = source_dir / "ticker_universe" / "overview.md"
    for batch_label, symbols in parse_ticker_batches_from_overview(overview_path):
        articles = ticker_articles_for_symbols(source_dir, symbols, {})
        if not articles:
            continue
        for sub_label, _ in split_oversized_batch(batch_label, articles):
            out_path = summaries_dir / f"tickers_{sub_label}.md"
            if _is_failed_summary(out_path):
                return True
    return False
_BRIEF_TZ = ZoneInfo("America/New_York")


def _default_asof() -> str:
    return datetime.now(_BRIEF_TZ).strftime("%Y-%m-%d")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        stream=sys.stdout,
    )


def _write_summary(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _clear_source(outdir: Path) -> None:
    """Remove source/ entirely so the next ingest is a full rewrite."""
    source_dir = outdir / "source"
    if source_dir.exists():
        shutil.rmtree(source_dir)
        logger.info("Removed existing %s", source_dir)


def run_fetch(date_str: str, outdir: Path) -> None:
    """Step 1: pull Benzinga news and write source/ snapshots."""
    from market_brief import ingest
    from market_brief.funnel_log import IngestFunnelData
    from market_brief.topics import load_topics

    outdir.mkdir(parents=True, exist_ok=True)
    _clear_source(outdir)
    topics = load_topics()
    funnel = IngestFunnelData()
    _articles, ingest_stats, source_slices = ingest.ingest_all(
        date_str, topics, funnel=funnel
    )
    ingest.persist_source_snapshots(source_slices, outdir, topics, asof=date_str)
    (outdir / "ingest_stats.json").write_text(
        json.dumps(ingest_stats.__dict__, indent=2),
        encoding="utf-8",
    )
    logger.info(
        "Fetch complete: %d unique articles → %s",
        ingest_stats.unique_articles,
        outdir / "source",
    )


def run_step3(
    *,
    date_str: str,
    source_dir: Path,
    summaries_dir: Path,
    tracker: CostTracker,
    resume: bool = False,
) -> list[Path]:
    """Fact extraction: one Sonnet call per channel + ticker batches."""
    written: list[Path] = []
    channel_sets = channel_article_sets(source_dir, {})

    for slug, articles in channel_sets.items():
        out_name = channel_output_filename(slug)
        out_path = summaries_dir / out_name
        step_id = f"step3_channel_{slug}"
        tracker.set_current_step(step_id)

        if not articles:
            logger.info("Skipping empty channel: %s", slug)
            continue

        if resume and not _is_failed_summary(out_path):
            logger.info("Resume: keeping %s", out_path.name)
            written.append(out_path)
            continue

        articles_text = concat_articles(articles)
        user_msg = step3_user_message(
            source_label=f"channel: {slug}",
            date_str=date_str,
            articles_text=articles_text,
        )
        try:
            md = complete(
                model=SONNET_MODEL,
                logical_model=SONNET_LOGICAL,
                system=STEP3_SYSTEM_PROMPT,
                user_message=user_msg,
                step=step_id,
                tracker=tracker,
            )
            _write_summary(out_path, md)
            written.append(out_path)
            logger.info("Wrote %s (%d articles)", out_path.name, len(articles))
        except Exception as e:
            logger.exception("Step 3 failed for channel %s: %s", slug, e)
            _write_summary(out_path, PLACEHOLDER)
            written.append(out_path)

    overview_path = source_dir / "ticker_universe" / "overview.md"
    batches = parse_ticker_batches_from_overview(overview_path)
    if not batches:
        logger.warning("No ticker batches parsed from overview; skipping ticker extraction")
    else:
        for batch_label, symbols in batches:
            articles = ticker_articles_for_symbols(source_dir, symbols, {})
            if not articles:
                logger.info("Skipping empty ticker batch: %s", batch_label)
                continue

            sub_batches = split_oversized_batch(batch_label, articles)
            for sub_label, sub_articles in sub_batches:
                out_name = f"tickers_{sub_label}.md"
                out_path = summaries_dir / out_name
                step_id = f"step3_tickers_{sub_label}"
                tracker.set_current_step(step_id)

                if resume and not _is_failed_summary(out_path):
                    logger.info("Resume: keeping %s", out_path.name)
                    written.append(out_path)
                    continue

                articles_text = concat_articles(sub_articles)
                user_msg = step3_user_message(
                    source_label=f"ticker batch: {sub_label} ({', '.join(symbols[:8])}{'…' if len(symbols) > 8 else ''})",
                    date_str=date_str,
                    articles_text=articles_text,
                )
                try:
                    md = complete(
                        model=SONNET_MODEL,
                        logical_model=SONNET_LOGICAL,
                        system=STEP3_SYSTEM_PROMPT,
                        user_message=user_msg,
                        step=step_id,
                        tracker=tracker,
                    )
                    _write_summary(out_path, md)
                    written.append(out_path)
                    logger.info(
                        "Wrote %s (%d articles, %d symbols)",
                        out_path.name,
                        len(sub_articles),
                        len(symbols),
                    )
                except Exception as e:
                    logger.exception("Step 3 failed for ticker batch %s: %s", sub_label, e)
                    _write_summary(out_path, PLACEHOLDER)
                    written.append(out_path)

    return written


def _load_summaries_text(summaries_dir: Path) -> tuple[str, str]:
    channel_parts: list[str] = []
    ticker_parts: list[str] = []

    for path in sorted(summaries_dir.glob("*.md")):
        content = path.read_text(encoding="utf-8")
        if path.name.startswith("channel_"):
            name = path.stem.replace("channel_", "", 1)
            channel_parts.append(f"## Channel: {name}\n\n{content}")
        elif path.name.startswith("tickers_"):
            name = path.stem.replace("tickers_", "", 1)
            ticker_parts.append(f"## Ticker Group: {name}\n\n{content}")

    return "\n\n".join(channel_parts), "\n\n".join(ticker_parts)


def run_step4(
    *,
    date_str: str,
    source_dir: Path,
    summaries_dir: Path,
    outdir: Path,
    tracker: CostTracker,
) -> Path:
    """Synthesis: one Opus call → 02_brief.md."""
    overview_path = source_dir / "ticker_universe" / "overview.md"
    ticker_universe = (
        overview_path.read_text(encoding="utf-8")
        if overview_path.exists()
        else ""
    )
    channel_text, ticker_text = _load_summaries_text(summaries_dir)

    step_id = "step4_synthesis"
    tracker.set_current_step(step_id)

    user_msg = step4_user_message(
        date_str=date_str,
        ticker_universe=ticker_universe,
        channel_summaries=channel_text,
        ticker_summaries=ticker_text,
    )
    brief_md = complete(
        model=OPUS_MODEL,
        logical_model=OPUS_LOGICAL,
        system=STEP4_SYSTEM_PROMPT,
        user_message=user_msg,
        step=step_id,
        tracker=tracker,
        max_tokens=32_768,
        use_stream=True,
        pace_after=False,
    )
    brief_path = outdir / "02_brief.md"
    brief_path.write_text(brief_md, encoding="utf-8")
    logger.info("Wrote %s", brief_path)
    return brief_path


def run_pipeline(
    date_str: str | None = None,
    *,
    skip_ingest: bool = False,
    skip_llm_summary: bool = False,
    resume: bool = False,
) -> Path:
    """Ingest + Anthropic brief for the given date. Returns output directory."""
    if skip_ingest and skip_llm_summary:
        raise ValueError("Cannot use --skip-ingest and --skip-llm-summary together")
    if resume:
        skip_ingest = True

    asof = date_str or _default_asof()
    outdir = config.OUTPUTS_DIR / asof
    source_dir = outdir / "source"
    outdir.mkdir(parents=True, exist_ok=True)
    brief_path = outdir / "02_brief.md"

    if resume and not source_dir.is_dir():
        raise FileNotFoundError(
            f"Cannot resume: no source/ at {source_dir} — run a full pipeline first"
        )

    if not skip_llm_summary:
        status_mod.write_status(outdir, "running", stage="queued" if not resume else "resuming")
        if not resume:
            for stale_name in ("02_brief.md", "02_brief.json"):
                stale = outdir / stale_name
                if stale.exists():
                    stale.unlink()
                    logger.info("Removed stale %s before run", stale_name)

    if not skip_ingest:
        status_mod.write_status(outdir, "running", stage="ingest")
        logger.info("Ingest: Benzinga fetch for %s", asof)
        run_fetch(asof, outdir)

    if skip_llm_summary:
        status_mod.write_status(outdir, "complete", stage="ingest_done")
        logger.info("Skipping LLM steps (--skip-llm-summary)")
        print(f"\nIngest complete: {outdir / 'source'}")
        return outdir

    if skip_ingest:
        if not source_dir.is_dir():
            raise FileNotFoundError(
                f"No source data at {source_dir} — run without --skip-ingest first"
            )

    summaries_dir = outdir / "01_summaries"
    if not resume:
        if summaries_dir.exists():
            shutil.rmtree(summaries_dir)
        summaries_dir.mkdir(parents=True, exist_ok=True)
    else:
        summaries_dir.mkdir(parents=True, exist_ok=True)

    tracker = CostTracker.load_or_create(outdir) if resume else CostTracker(outdir=outdir)

    need_step3 = (not resume) or _summaries_need_work(summaries_dir, source_dir)
    if need_step3:
        status_mod.write_status(outdir, "running", stage="step3_fact_extraction")
        logger.info("Step 3: fact extraction for %s (resume=%s)", asof, resume)
        run_step3(
            date_str=asof,
            source_dir=source_dir,
            summaries_dir=summaries_dir,
            tracker=tracker,
            resume=resume,
        )
    elif resume:
        logger.info("Resume: all Step 3 summaries present — skipping to synthesis")

    if brief_path.exists() and resume:
        logger.info("Resume: %s already exists — skipping Step 4", brief_path.name)
    else:
        status_mod.write_status(outdir, "running", stage="step4_synthesis")
        logger.info("Step 4: synthesis for %s", asof)
        run_step4(
            date_str=asof,
            source_dir=source_dir,
            summaries_dir=summaries_dir,
            outdir=outdir,
            tracker=tracker,
        )

    tracker.set_current_step(None)
    tracker.flush()
    status_mod.write_status(
        outdir,
        "complete",
        stage="done",
        extra={
            "total_cost_usd": round(tracker.total_cost_usd, 4),
            "call_count": len(tracker.calls),
        },
    )

    _print_cost_summary(tracker)
    return outdir


def _print_cost_summary(tracker: CostTracker) -> None:
    print("\n=== Run cost summary ===")
    print(f"  API calls:     {len(tracker.calls)}")
    print(f"  Step 3 total:  ${tracker.step3_cost_usd:.4f}")
    print(f"  Step 4 total:  ${tracker.step4_cost_usd:.4f}")
    print(f"  Grand total:   ${tracker.total_cost_usd:.4f}")
    for rec in tracker.calls:
        print(
            f"    {rec.step}: in={rec.input_tokens:,} out={rec.output_tokens:,} "
            f"${rec.total_cost_usd:.4f}"
        )


def main(
    *,
    date: str | None = None,
    skip_ingest: bool = False,
    skip_llm_summary: bool = False,
    resume: bool = False,
    verbose: bool = False,
) -> int:
    """Program entry; also callable from market_brief.run with explicit flags."""
    if date is None and __name__ == "__main__":
        p = argparse.ArgumentParser(
            description="Market brief: Benzinga ingest + Anthropic fact extraction + synthesis",
        )
        p.add_argument("--date", "--asof", dest="date", help="YYYY-MM-DD (default: today UTC)")
        p.add_argument(
            "--skip-ingest",
            action="store_true",
            help="Skip Benzinga ingest; run Anthropic Steps 3–4 on existing source/",
        )
        p.add_argument(
            "--skip-llm-summary",
            action="store_true",
            help="Ingest only (rewrite source/); skip Anthropic Steps 3–4",
        )
        p.add_argument(
            "--resume",
            action="store_true",
            help="Resume failed/missing Step 3 summaries + Step 4 (implies --skip-ingest)",
        )
        p.add_argument("-v", "--verbose", action="store_true")
        args = p.parse_args()
        date = args.date
        skip_ingest = args.skip_ingest
        skip_llm_summary = args.skip_llm_summary
        resume = args.resume
        verbose = args.verbose

    _setup_logging(verbose)

    if skip_ingest and skip_llm_summary:
        logger.error("Cannot use --skip-ingest and --skip-llm-summary together")
        return 1
    if resume and skip_llm_summary:
        logger.error("Cannot use --resume with --skip-llm-summary")
        return 1

    try:
        outdir = run_pipeline(
            date,
            skip_ingest=skip_ingest,
            skip_llm_summary=skip_llm_summary,
            resume=resume,
        )
    except (FileNotFoundError, ValueError) as e:
        logger.error("%s", e)
        return 1
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        asof = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        status_mod.write_status(
            config.OUTPUTS_DIR / asof, "failed", stage="error", error=str(e)
        )
        return 1

    if skip_llm_summary:
        return 0

    print(f"\nBrief written to: {outdir / '02_brief.md'}")
    print(f"  Summaries:  {outdir / '01_summaries/'}")
    print(f"  Costs:      {outdir / 'run_costs.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
