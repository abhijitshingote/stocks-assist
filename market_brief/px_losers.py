"""R1D losers brief: bottom ``dr_1`` names by cap bucket → Perplexity per-ticker research → Sonnet.

Same research machinery as ``perplexity_brief`` (ticker batches + gap fills for names with no
catalyst found), written with ``prompts/losers_synthesis.md``. Published to ``app.briefs``
(kind ``losers``, source ``perplexity``).

    docker compose exec backend python -m market_brief.px_losers
    docker compose exec backend python -m market_brief.px_losers --date 2026-10-16 --skip-research

Working dir: ``user_data/market_brief_perplexity_losers/<date>/``.
"""

from __future__ import annotations

import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from market_brief import config
from market_brief import px_prompts as P
from market_brief import status as status_mod
from market_brief.losers_universe import build_losers_universe, render_losers_markdown
from market_brief.perplexity_brief import (
    ANTHROPIC_SYNTH_MODELS,
    BATCH_SIZE,
    RESEARCH_MODEL,
    LockedTracker,
    PerplexityCall,
    _cap_label,
    _setup_logging,
    build_ticker_ledger,
    claude,
    date_windows,
    load_research,
    load_tape,
    publish,
    run_research,
    select_gaps,
    ticker_sections,
)
from market_brief.trading_calendar import ET

logger = logging.getLogger(__name__)

OUTPUTS_DIR = config.USER_DATA_DIR / "market_brief_perplexity_losers"
SYSTEM_PROMPT = (config.PACKAGE_DIR / "prompts" / "losers_synthesis.md").read_text(encoding="utf-8").strip()


def _user_message(*, asof: str, cutoff: str, losers_table: str, tape_block: str, research: str) -> str:
    return (
        f"<date>{asof} (brief cutoff {cutoff} ET)</date>\n\n"
        f"<losers_table>\n{losers_table}\n\n{tape_block}\n</losers_table>\n\n"
        f"<ticker_research>\n{research}\n</ticker_research>\n\n"
        "Write the R1D losers brief. Source scope is the ticker research only — "
        "do not use macro news."
    )


def _batches(tickers: list[str], size: int) -> list[tuple[str, list[str]]]:
    return [(f"tickers_{i // size + 1:02d}", tickers[i : i + size]) for i in range(0, len(tickers), size)]


def _run(args: argparse.Namespace, asof: str, cutoff: str, outdir: Path) -> int:
    status_mod.write_status(outdir, "running", stage="hydrate")
    slices, lineage, _ = build_losers_universe(asof)
    tickers = [t for sl in slices for t in (r["ticker"] for r in sl.selection)]
    if not tickers:
        raise RuntimeError("Losers universe is empty (stock_metrics not updated?)")
    losers_md = render_losers_markdown(slices, lineage)
    (outdir / "losers.md").write_text(losers_md, encoding="utf-8")
    session_date, moves, tape_block = load_tape(asof, tickers)
    (outdir / "tape.md").write_text(tape_block, encoding="utf-8")

    def table(syms: list[str]) -> str:
        return P.tape_table(syms, lineage, moves, _cap_label)

    research_dir = outdir / "01_research"
    windows = date_windows(session_date, asof)
    failed: list[str] = []
    if args.skip_research:
        if not any(research_dir.glob("*.json")):
            raise FileNotFoundError(f"No research at {research_dir} — run without --skip-research")
        tracker = LockedTracker.load_or_create(outdir)
        tracker.calls = [c for c in tracker.calls if not c.step.startswith("synthesis_")]
        pplx = PerplexityCall(tracker)
    else:
        if research_dir.exists():
            shutil.rmtree(research_dir)
        pplx = PerplexityCall(LockedTracker(outdir=outdir))
        pplx.tracker.set_current_step("research")
        jobs: list[dict[str, Any]] = [
            {"name": name, "kind": "ticker", "group": "tickers", "window": "session",
             "prompt": P.ticker_batch_prompt(table(syms), session_date, asof, cutoff)}
            for name, syms in _batches(tickers, args.batch_size)
        ]
        failed += run_research(jobs, research_dir, pplx, windows, args.model, "Tickers")
        if len(failed) == len(jobs):
            raise RuntimeError(f"All {len(jobs)} ticker searches failed (see run.log)")

        sections = ticker_sections(load_research(research_dir))
        gaps = select_gaps(lineage, moves, sections)
        logger.info("Gap fills: %d: %s", len(gaps), ", ".join(gaps))
        by_ticker = lineage.get("by_ticker") or {}
        gap_jobs = [
            {"name": f"c_{sym}", "kind": "gap", "group": "tickers", "window": "session",
             "prompt": P.gap_prompt(table([sym]), sym, by_ticker.get(sym, {}).get("company_name") or sym,
                                    session_date, asof, cutoff, sections.get(sym, ""))}
            for sym in gaps
        ]
        failed += run_research(gap_jobs, research_dir, pplx, windows, args.model, "Gap fills")

    ledger = build_ticker_ledger(lineage, moves, ticker_sections(load_research(research_dir)))
    user_msg = _user_message(asof=asof, cutoff=cutoff, losers_table=losers_md, tape_block=tape_block,
                             research=ledger)
    (outdir / "02_synth_input.md").write_text(user_msg, encoding="utf-8")
    step = f"synthesis_{args.synth}"
    status_mod.write_status(outdir, "running", stage=step)
    pplx.tracker.set_current_step(step)
    brief = claude(ANTHROPIC_SYNTH_MODELS[args.synth], SYSTEM_PROMPT, user_msg, step, pplx.tracker)
    (outdir / "02_brief.md").write_text(brief, encoding="utf-8")

    tracker = pplx.tracker
    tracker.set_current_step(None)
    tracker.flush()
    publish(asof, "losers", outdir, metadata={
        "cutoff": cutoff, "session_date": session_date, "synth": args.synth,
        "tickers": tickers, "failed_searches": failed,
    })
    status_mod.write_status(
        outdir, "complete", stage="done",
        extra={
            "total_cost_usd": round(tracker.total_cost_usd, 4),
            "call_count": len(tracker.calls),
            **({"detail": f"{len(failed)} searches failed: {', '.join(failed)}"} if failed else {}),
        },
    )
    logger.info("Losers brief %s: $%.4f across %d calls", asof, tracker.total_cost_usd, len(tracker.calls))
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="R1D losers brief (Perplexity research + Sonnet)")
    p.add_argument("--asof", "--date", dest="asof", help="YYYY-MM-DD (default: today ET)")
    p.add_argument("--cutoff", help="HH:MM ET news cutoff (default: now if today, else 09:00)")
    p.add_argument("--synth", choices=tuple(ANTHROPIC_SYNTH_MODELS), default="sonnet")
    p.add_argument("--model", default=RESEARCH_MODEL, help="Perplexity research model")
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    p.add_argument("--skip-research", action="store_true", help="Reuse existing 01_research/")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    now = datetime.now(ET)
    asof = args.asof or now.strftime("%Y-%m-%d")
    cutoff = args.cutoff or (now.strftime("%H:%M") if asof == now.strftime("%Y-%m-%d") else "09:00")
    outdir = OUTPUTS_DIR / asof
    _setup_logging(outdir, args.verbose, to_file=True)
    try:
        return _run(args, asof, cutoff, outdir)
    except Exception as e:  # noqa: BLE001
        logger.exception("Losers brief failed: %s", e)
        status_mod.write_status(outdir, "failed", stage="error", error=str(e)[:500])
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
