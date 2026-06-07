"""R1D losers brief: screener bottom names + ticker-only Benzinga + DeepSeek synthesis.

Run inside the backend container:

    docker compose exec backend python -m market_brief_losers.run_pipeline
    docker compose exec backend python -m market_brief_losers.run_pipeline --date 2026-06-06
    docker compose exec backend python -m market_brief_losers.run_pipeline --skip-ingest
    docker compose exec backend python -m market_brief_losers.run_pipeline --skip-llm
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from market_brief_losers import config, status as status_mod
from market_brief_losers.cost_tracker import CostTracker
from market_brief_losers.deepseek_client import SYNTH_LOGICAL, SYNTH_MODEL, complete
from market_brief_losers.prompts import synthesis_system_prompt, synthesis_user_message
from market_brief_losers.source_loader import (
    audit_source,
    build_ticker_summaries_text,
    prompt_counts,
)

logger = logging.getLogger(__name__)

_BRIEF_TZ = ZoneInfo("America/New_York")


def _default_asof() -> str:
    return datetime.now(_BRIEF_TZ).strftime("%Y-%m-%d")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        stream=sys.stdout,
    )


def _clear_source(outdir: Path) -> None:
    source_dir = outdir / "source"
    if source_dir.exists():
        shutil.rmtree(source_dir)
        logger.info("Removed existing %s", source_dir)


def run_fetch(date_str: str, outdir: Path) -> None:
    """Step 1: ticker-only Benzinga ingest → DB upsert → metadata."""
    from market_brief_losers import ingest

    outdir.mkdir(parents=True, exist_ok=True)
    _clear_source(outdir)
    metadata, stats, _corpus = ingest.prepare_run(date_str, outdir)
    metadata_path = outdir / "metadata.json"
    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _fill_prompt_counts(outdir, metadata_path)
    c = metadata["counts"]
    p = (c.get("prompt") or {})
    warnings = metadata.get("fetch_warnings") or []
    logger.info(
        "Ingest complete: api_total=%d deduped=%d upserted=%d prompt_articles=%s → %s",
        c["api_total_rows"],
        c["after_dedupe_unique"],
        c["upserted"],
        p.get("articles_total", "—"),
        outdir,
    )
    if warnings:
        logger.warning(
            "fetch_warnings (%d) — see metadata.json: %s",
            len(warnings),
            "; ".join(warnings[:3]) + (" …" if len(warnings) > 3 else ""),
        )


def _fill_prompt_counts(outdir: Path, metadata_path: Path) -> None:
    try:
        built = build_ticker_summaries_text(outdir)
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata.setdefault("counts", {})["prompt"] = prompt_counts(built)
        metadata["article_ids"] = built.article_ids
        metadata_path.write_text(
            json.dumps(metadata, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("prompt counts skipped: %s", e)


def run_synthesis(
    *,
    date_str: str,
    outdir: Path,
    tracker: CostTracker,
) -> Path:
    """One DeepSeek call → 02_brief.md."""
    overview_path = outdir / "source" / "losers_universe" / "overview.md"
    losers_table = (
        overview_path.read_text(encoding="utf-8") if overview_path.exists() else ""
    )
    built = build_ticker_summaries_text(outdir)
    ticker_text = built.ticker_text
    audit = audit_source(outdir, ticker_text=ticker_text)
    logger.info(
        "Source audit (ticker-only): %d unique articles → %d prompt blocks "
        "symbols=%d coverage_ok=%s",
        audit["unique_benzinga_ids"],
        audit["prompt_blocks"],
        audit["ticker_symbols"],
        audit["coverage_ok"],
    )
    logger.info("Ingest window: %s", audit.get("ticker_window"))
    logger.info(
        "Article text: %s chars (~%s tokens at chars/3.5)",
        f"{audit['article_chars']:,}",
        f"{audit['estimated_tokens_chars_per_3_5']:,}",
    )
    if not audit["coverage_ok"]:
        logger.warning(
            "Coverage mismatch: %d unique in source vs %d blocks in prompt",
            audit["unique_benzinga_ids"],
            audit["prompt_blocks"],
        )

    step_id = "synthesis"
    tracker.set_current_step(step_id)

    user_msg = synthesis_user_message(
        date_str=date_str,
        losers_table=losers_table,
        ticker_articles=ticker_text,
    )
    system_prompt = synthesis_system_prompt()

    brief_md = complete(
        model=SYNTH_MODEL,
        logical_model=SYNTH_LOGICAL,
        system=system_prompt,
        user_message=user_msg,
        step=step_id,
        tracker=tracker,
        max_tokens=32_768,
        use_stream=True,
    )
    brief_path = outdir / "02_brief.md"
    brief_path.write_text(brief_md, encoding="utf-8")
    logger.info("Wrote %s", brief_path)

    metadata_path = outdir / "metadata.json"
    metadata: dict = {}
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            metadata = {}
    metadata.setdefault("counts", {})["prompt"] = prompt_counts(built)
    metadata["article_ids"] = built.article_ids
    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return brief_path


def run_pipeline(
    date_str: str | None = None,
    *,
    skip_ingest: bool = False,
    skip_llm: bool = False,
    resume: bool = False,
) -> Path:
    if skip_ingest and skip_llm:
        raise ValueError("Cannot use --skip-ingest and --skip-llm together")
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

    if not skip_llm:
        status_mod.write_status(
            outdir, "running", stage="queued" if not resume else "resuming"
        )
        if not resume:
            for stale_name in ("02_brief.md", "02_brief.json"):
                stale = outdir / stale_name
                if stale.exists():
                    stale.unlink()
                    logger.info("Removed stale %s before run", stale_name)

    if not skip_ingest:
        status_mod.write_status(outdir, "running", stage="ingest")
        logger.info("Ingest: ticker-only Benzinga for %s", asof)
        run_fetch(asof, outdir)

    if skip_llm:
        status_mod.write_status(outdir, "complete", stage="ingest_done")
        logger.info("Skipping LLM (--skip-llm)")
        print(f"\nIngest complete: {outdir / 'metadata.json'}")
        return outdir

    if skip_ingest:
        if not source_dir.is_dir():
            raise FileNotFoundError(
                f"No source data at {source_dir} — run without --skip-ingest first"
            )
        if not (outdir / "metadata.json").exists():
            raise FileNotFoundError(
                f"No metadata.json at {outdir} — run without --skip-ingest first"
            )

    tracker = CostTracker.load_or_create(outdir) if resume else CostTracker(outdir=outdir)

    if brief_path.exists() and resume:
        logger.info("Resume: %s already exists — skipping synthesis", brief_path.name)
    else:
        status_mod.write_status(outdir, "running", stage="synthesis")
        logger.info("Synthesis (DeepSeek) for %s", asof)
        run_synthesis(date_str=asof, outdir=outdir, tracker=tracker)

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
    print(f"  Total cost:    ${tracker.total_cost_usd:.4f}")
    for rec in tracker.calls:
        print(
            f"    {rec.step} ({rec.api_model}): "
            f"in={rec.input_tokens:,} out={rec.output_tokens:,} "
            f"cache_r={rec.cache_read_input_tokens:,} "
            f"${rec.total_cost_usd:.4f}"
        )


def main(
    *,
    date: str | None = None,
    skip_ingest: bool = False,
    skip_llm: bool = False,
    resume: bool = False,
    verbose: bool = False,
) -> int:
    if date is None and __name__ == "__main__":
        p = argparse.ArgumentParser(
            description="R1D losers brief: ticker-only ingest + DeepSeek synthesis",
        )
        p.add_argument("--date", "--asof", dest="date", help="YYYY-MM-DD (default: today ET)")
        p.add_argument(
            "--skip-ingest",
            action="store_true",
            help="Skip ingest; run synthesis on existing source/",
        )
        p.add_argument(
            "--skip-llm",
            action="store_true",
            help="Ingest only; skip DeepSeek synthesis",
        )
        p.add_argument(
            "--resume",
            action="store_true",
            help="Resume synthesis if missing (implies --skip-ingest)",
        )
        p.add_argument("-v", "--verbose", action="store_true")
        args = p.parse_args()
        date = args.date
        skip_ingest = args.skip_ingest
        skip_llm = args.skip_llm
        resume = args.resume
        verbose = args.verbose

    _setup_logging(verbose)

    if skip_ingest and skip_llm:
        logger.error("Cannot use --skip-ingest and --skip-llm together")
        return 1
    if resume and skip_llm:
        logger.error("Cannot use --resume with --skip-llm")
        return 1

    try:
        outdir = run_pipeline(
            date,
            skip_ingest=skip_ingest,
            skip_llm=skip_llm,
            resume=resume,
        )
    except (FileNotFoundError, ValueError) as e:
        logger.error("%s", e)
        return 1
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        asof = date or _default_asof()
        status_mod.write_status(
            config.OUTPUTS_DIR / asof, "failed", stage="error", error=str(e)
        )
        return 1

    if skip_llm:
        return 0

    print(f"\nBrief written to: {outdir / '02_brief.md'}")
    print(f"  Costs:      {outdir / 'run_costs.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
