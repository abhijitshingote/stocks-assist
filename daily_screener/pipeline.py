"""Pipeline orchestrator.

Runs stages 1 through 6 in order. Each stage is idempotent and reads JSON
input + writes JSON output, so re-running --from-stage N skips earlier work.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from daily_screener import config
from daily_screener.stages import (
    s1_universe,
    s2_momentum,
    s3_themes,
    s4_news,
    s5_judge,
    s6_audit,
)
from daily_screener.utils import io
from daily_screener.utils.perplexity import USAGE as _PPLX_USAGE

logger = logging.getLogger(__name__)

# Cooldown between Stage 4 (news) and Stage 5 (judge) when both stages use the
# same provider (Perplexity). Lets the rate-limit window drain so the judge
# doesn't start with a fresh batch of 429/401s. Skipped when the judge is on
# Claude (different provider, no shared budget).
_PERPLEXITY_COOLDOWN_SECONDS = 30


@dataclass
class PipelineOptions:
    date: str | None = None
    from_stage: int = 1
    to_stage: int = 6
    max_tickers: int | None = None
    force_refresh_themes: bool = False


def run(opts: PipelineOptions) -> dict:
    date_str = opts.date or io.today_str()
    summary = {"date": date_str, "stages": {}}

    # Reset the Perplexity usage accumulator so the per-run cost report only
    # reflects what this invocation actually called.
    _PPLX_USAGE.reset()

    stages: list[tuple[int, str, callable]] = [
        (1, "universe", lambda: s1_universe.run(date_str, max_tickers=opts.max_tickers)),
        (2, "momentum", lambda: s2_momentum.run(date_str)),
        (3, "themes", lambda: s3_themes.run(date_str, force_refresh=opts.force_refresh_themes)),
        (4, "news", lambda: s4_news.run(date_str, max_tickers=opts.max_tickers)),
        (5, "judge", lambda: s5_judge.run(date_str, max_tickers=opts.max_tickers)),
        (6, "audit", lambda: s6_audit.run(date_str)),
    ]

    for stage_num, name, fn in stages:
        if stage_num < opts.from_stage:
            logger.info("[pipeline] skip stage %d (%s) - before from_stage", stage_num, name)
            continue
        if stage_num > opts.to_stage:
            logger.info("[pipeline] skip stage %d (%s) - after to_stage", stage_num, name)
            continue

        # Cooldown before Stage 5 if both news and judge are on Perplexity
        # AND we just ran the news stage in this same invocation.
        if (
            stage_num == 5
            and "news" in summary["stages"]
            and not config.JUDGE_MODEL.lower().startswith("claude")
        ):
            logger.info(
                "[pipeline] sleeping %ds between Perplexity stages to drain rate-limit window",
                _PERPLEXITY_COOLDOWN_SECONDS,
            )
            time.sleep(_PERPLEXITY_COOLDOWN_SECONDS)

        t0 = time.time()
        logger.info("[pipeline] running stage %d (%s)", stage_num, name)
        try:
            result = fn()
        except Exception as e:
            logger.exception("[pipeline] stage %d (%s) FAILED: %s", stage_num, name, e)
            summary["stages"][name] = {
                "stage": stage_num,
                "status": "failed",
                "error": str(e),
                "elapsed_s": round(time.time() - t0, 1),
            }
            return summary

        summary["stages"][name] = {
            "stage": stage_num,
            "status": "ok",
            "elapsed_s": round(time.time() - t0, 1),
            "stats": _summarize_result(result),
        }

    # Roll up Perplexity token/cost usage at the end of the run.
    usage = _PPLX_USAGE.snapshot()
    summary["perplexity_usage"] = usage
    logger.info(
        "[pipeline] perplexity usage: %d requests, %d prompt tokens, %d completion tokens, total $%.4f",
        usage["total_requests"],
        usage["total_prompt_tokens"],
        usage["total_completion_tokens"],
        usage["cost_usd_total"],
    )

    return summary


def _summarize_result(result):
    if not isinstance(result, dict):
        return None
    keep = (
        "total_universe",
        "source_counts",
        "survivor_count",
        "drop_count",
        "merged_count",
        "result_count",
        "failure_count",
        "judged_count",
        "verdict_counts",
        "skipped_no_news_count",
    )
    return {k: result[k] for k in keep if k in result}
