"""Market brief pipeline: Benzinga ingest + Anthropic brief.

Run inside the backend container:

    docker compose exec backend python -m market_brief.run_pipeline
    docker compose exec backend python -m market_brief.run_pipeline --date 2026-05-31
    docker compose exec backend python -m market_brief.run_pipeline --skip-ingest
    docker compose exec backend python -m market_brief.run_pipeline --skip-llm
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

from market_brief import config
from market_brief.anthropic_client import (
    OPUS_LOGICAL,
    OPUS_MODEL,
    complete,
    count_message_tokens,
)
from market_brief.anthropic_pricing import estimate_input_cost
from market_brief.cost_tracker import CostTracker
from market_brief.prompts_pipeline import (
    STEP4_SYSTEM_PROMPT,
    step4_user_message,
)
from market_brief.source_loader import (
    audit_step4_source,
    build_step4_summaries_text,
    prompt_counts,
)
from market_brief import status as status_mod

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
    """Remove source/ entirely so the next ingest is a full rewrite."""
    source_dir = outdir / "source"
    if source_dir.exists():
        shutil.rmtree(source_dir)
        logger.info("Removed existing %s", source_dir)


def run_fetch(date_str: str, outdir: Path) -> None:
    """Step 1: API bucket ingest → DB upsert → metadata (ingest_pools + corpus ids)."""
    from market_brief import ingest
    from market_brief.funnel_log import IngestFunnelData
    from market_brief.topics import load_topics

    outdir.mkdir(parents=True, exist_ok=True)
    _clear_source(outdir)
    topics = load_topics()
    funnel = IngestFunnelData()
    metadata, stats, _corpus = ingest.prepare_run(
        date_str, outdir, topics, funnel=funnel
    )
    metadata_path = outdir / "metadata.json"
    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _fill_prompt_counts(outdir, metadata_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    c = metadata["counts"]
    p = c.get("prompt") or {}
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
    """Load ingest pools from DB and record prompt section counts (no Opus)."""
    try:
        built = build_step4_summaries_text(outdir)
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata.setdefault("counts", {})["prompt"] = prompt_counts(built)
        metadata["article_ids"] = built.article_ids
        metadata_path.write_text(
            json.dumps(metadata, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("prompt counts skipped: %s", e)


def run_step4(
    *,
    date_str: str,
    source_dir: Path,
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
    built = build_step4_summaries_text(outdir)
    channel_text, ticker_text = built.channel_text, built.ticker_text
    audit = audit_step4_source(outdir, channel_text=channel_text, ticker_text=ticker_text)
    logger.info(
        "Step 4 source audit: %d unique articles → %d prompt blocks "
        "(channel=%d ticker-only=%d) coverage_ok=%s",
        audit["unique_benzinga_ids"],
        audit["step4_blocks_total"],
        audit["step4_blocks_channel"],
        audit["ticker_only_articles"],
        audit["coverage_ok"],
    )
    logger.info(
        "Ingest windows: general=%s | ticker=%s",
        audit.get("general_window"),
        audit.get("ticker_window"),
    )
    logger.info(
        "Step 4 article text: %s chars (~%s tokens at chars/3.5; API tokenizer may differ)",
        f"{audit['article_chars']:,}",
        f"{audit['estimated_tokens_chars_per_3_5']:,}",
    )
    if not audit["coverage_ok"]:
        logger.warning(
            "Step 4 coverage mismatch: %d unique in source vs %d blocks in prompt",
            audit["unique_benzinga_ids"],
            audit["step4_blocks_total"],
        )

    step_id = "step4_synthesis"
    tracker.set_current_step(step_id)

    user_msg = step4_user_message(
        date_str=date_str,
        ticker_universe=ticker_universe,
        channel_summaries=channel_text,
        ticker_summaries=ticker_text,
    )
    try:
        preflight_in = count_message_tokens(
            model=OPUS_MODEL,
            system=STEP4_SYSTEM_PROMPT,
            user_message=user_msg,
        )
        logger.info(
            "Anthropic count_tokens preflight: %s input tokens (~$%.4f input @ list price)",
            f"{preflight_in:,}",
            estimate_input_cost(preflight_in, api_model=OPUS_MODEL),
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("count_tokens preflight failed (non-fatal): %s", e)

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
    p = metadata["counts"]["prompt"]
    logger.info(
        "Wrote %s (%d articles in prompt, %d sections)",
        metadata_path,
        p["articles_total"],
        len(p.get("by_section") or {}),
    )
    return brief_path


def run_pipeline(
    date_str: str | None = None,
    *,
    skip_ingest: bool = False,
    skip_llm: bool = False,
    resume: bool = False,
) -> Path:
    """Ingest + Anthropic brief for the given date. Returns output directory."""
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

    if skip_llm:
        status_mod.write_status(outdir, "complete", stage="ingest_done")
        logger.info("Skipping LLM steps (--skip-llm)")
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
        logger.info("Resume: %s already exists — skipping Step 4", brief_path.name)
    else:
        status_mod.write_status(outdir, "running", stage="step4_synthesis")
        logger.info("Step 4: synthesis for %s", asof)
        run_step4(
            date_str=asof,
            source_dir=source_dir,
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
    """Program entry; also callable from market_brief.run with explicit flags."""
    if date is None and __name__ == "__main__":
        p = argparse.ArgumentParser(
            description="Market brief: Benzinga ingest + Anthropic synthesis",
        )
        p.add_argument("--date", "--asof", dest="date", help="YYYY-MM-DD (default: today ET)")
        p.add_argument(
            "--skip-ingest",
            action="store_true",
            help="Skip Benzinga ingest; run Anthropic Step 4 on existing source/",
        )
        p.add_argument(
            "--skip-llm",
            action="store_true",
            help="Ingest only (rewrite source/); skip Anthropic Step 4",
        )
        p.add_argument(
            "--resume",
            action="store_true",
            help="Resume Step 4 synthesis if missing (implies --skip-ingest)",
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
