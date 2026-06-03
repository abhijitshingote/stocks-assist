"""Market brief pipeline orchestrator.

Stages:

1. Ingest Benzinga news (full ticker universe + general/channel feeds, 24h window).
2. Persist raw snapshots to ``00_news/`` and upsert into ``benzinga_articles``.
3. Summarize per topic (chunked full-body articles → Perplexity, no web search).
4. Run a Perplexity **watch** probe for forward calendar events.
5. Synthesize into ``02_brief.md`` / ``02_brief.json``.
6. Write usage/cost snapshot.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from daily_screener.utils.perplexity import (
    USAGE,
    PerplexityError,
    call_perplexity,
)

from market_brief import config, ingest, ingest_report, prompts, summarize, status as status_mod, tape as tape_mod
from market_brief.funnel_log import (
    FunnelReport,
    IngestFunnelData,
    write_funnel_md,
)
from market_brief.persist import persist_summaries
from market_brief.topics import Topic, load_topics

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Legacy web probes (optional)
# ---------------------------------------------------------------------------


from market_brief.types import ProbeResult


def _run_web_probe(topic: Topic, kind: str, asof: str) -> ProbeResult:
    if kind == "overview":
        try:
            tape_block = tape_mod.get_topic_tape_block(topic.as_dict(), asof)
        except Exception as e:  # noqa: BLE001
            logger.warning("tape lookup failed for %s: %s", topic.name, e)
            tape_block = None
        prompt = prompts.build_overview_prompt(
            topic.as_dict(), asof=asof, tape_block=tape_block
        )
    elif kind == "catalyst":
        prompt = prompts.build_catalyst_prompt(topic.as_dict(), asof=asof)
    else:
        raise ValueError(f"unknown probe kind: {kind}")

    t0 = time.time()
    try:
        content = call_perplexity(
            prompt,
            model=config.PROBE_MODEL,
            max_tokens=config.PROBE_MAX_TOKENS,
            temperature=config.PROBE_TEMPERATURE,
            timeout=config.PROBE_TIMEOUT_SECONDS,
            log_label=(
                f"WEB_PROBE | [{topic.kind}] {topic.name} | {kind} "
                f"| task: legacy web-search probe (USE_WEB_PROBES)"
            ),
        )
        return ProbeResult(
            topic_name=topic.name,
            topic_kind=topic.kind,
            kind=kind,
            content=content,
            elapsed_s=time.time() - t0,
        )
    except PerplexityError as e:
        logger.error("probe failed: %s/%s: %s", topic.name, kind, e)
        return ProbeResult(
            topic_name=topic.name,
            topic_kind=topic.kind,
            kind=kind,
            content="",
            error=str(e),
            elapsed_s=time.time() - t0,
        )


def run_web_probes(topics: list[Topic], asof: str) -> list[ProbeResult]:
    jobs: list[tuple[Topic, str]] = []
    for t in topics:
        for kind in config.ENABLED_PROBE_KINDS:
            jobs.append((t, kind))

    results: list[ProbeResult] = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.PROBE_CONCURRENCY
    ) as pool:
        futures = {
            pool.submit(_run_web_probe, topic, kind, asof): (topic.name, kind)
            for topic, kind in jobs
        }
        for fut in concurrent.futures.as_completed(futures):
            name, kind = futures[fut]
            try:
                results.append(fut.result())
            except Exception as e:  # noqa: BLE001
                results.append(
                    ProbeResult(
                        topic_name=name,
                        topic_kind="unknown",
                        kind=kind,
                        content="",
                        error=f"crashed: {e}",
                    )
                )
    return results


# ---------------------------------------------------------------------------
# Synthesis
# ---------------------------------------------------------------------------


def synthesize(
    topic_results: list[ProbeResult],
    watch_result: ProbeResult | None,
    asof: str,
) -> str:
    payload = [
        {
            "topic_name": r.topic_name,
            "kind": r.kind,
            "content": r.content,
        }
        for r in topic_results
        if r.content and r.kind != "watch"
    ]
    if watch_result and watch_result.content:
        payload.append(
            {
                "topic_name": watch_result.topic_name,
                "kind": watch_result.kind,
                "content": watch_result.content,
            }
        )

    if not payload:
        return f"# Daily Market Brief — {asof}\n\n_(all summaries failed)_\n"

    try:
        all_tickers: list[str] = []
        for sect_tickers in tape_mod.SECTOR_TICKERS.values():
            all_tickers.extend(sect_tickers)
        if config.USE_USER_THEMES and config.THEMES_FILE.exists():
            with config.THEMES_FILE.open("r", encoding="utf-8") as f:
                for t in json.load(f) or []:
                    all_tickers.extend(t.get("tickers") or [])
        session_date, t_quotes, i_quotes = tape_mod.get_tape(all_tickers, asof)
        tape_block = tape_mod.format_tape_block(session_date, t_quotes, i_quotes)
    except Exception as e:  # noqa: BLE001
        logger.warning("global tape lookup failed: %s", e)
        tape_block = None

    prompt = prompts.build_synthesis_prompt(payload, asof=asof, tape_block=tape_block)
    topics_in = ", ".join(p["topic_name"] for p in payload[:8])
    if len(payload) > 8:
        topics_in += f", … (+{len(payload) - 8} more)"
    label = (
        f"SYNTHESIZE | 02_brief.md | {len(payload)} summary block(s) in "
        f"| topics: {topics_in} "
        f"| task: assemble tiered morning brief from summaries (no new facts)"
    )
    return call_perplexity(
        prompt,
        model=config.SYNTH_MODEL,
        max_tokens=config.SYNTH_MAX_TOKENS,
        temperature=config.SYNTH_TEMPERATURE,
        timeout=config.SYNTH_TIMEOUT_SECONDS,
        log_label=label,
    )


def synthesize_json(brief_md: str, asof: str) -> dict:
    prompt = prompts.build_json_synthesis_prompt(brief_md, asof=asof)
    raw = call_perplexity(
        prompt,
        model=config.SYNTH_MODEL,
        max_tokens=config.SYNTH_MAX_TOKENS,
        temperature=0.0,
        timeout=config.SYNTH_TIMEOUT_SECONDS,
        log_label=(
            f"SYNTHESIZE | 02_brief.json | brief_chars={len(brief_md)} "
            f"| task: restructure 02_brief.md into JSON (no new content)"
        ),
    )
    cleaned = _strip_code_fence(raw).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("json synth returned non-JSON; persisting raw text")
        return {"_raw": raw, "_parse_error": True}


def _strip_code_fence(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines)
    return text


# ---------------------------------------------------------------------------
# Top-level
# ---------------------------------------------------------------------------


def _load_articles_from_snapshots(outdir: Path) -> list[dict]:
    from market_brief.theme_discovery import load_run_articles

    corpus, _, _ = load_run_articles(outdir)
    if not corpus:
        raise FileNotFoundError(
            f"No articles under {outdir / '00_news'} — run ingest first or drop --skip-ingest"
        )
    return corpus


def run_brief(
    asof: str | None = None,
    *,
    qa_log: bool | None = None,
    resume_summarize: bool = False,
    skip_ingest: bool = False,
) -> Path:
    """Run the full pipeline. Returns the output directory path."""
    USAGE.reset()

    asof = asof or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    outdir = config.OUTPUTS_DIR / asof
    outdir.mkdir(parents=True, exist_ok=True)

    write_qa = config.QA_LOG_ENABLED if qa_log is None else qa_log
    funnel_report: FunnelReport | None = (
        FunnelReport(asof=asof, outdir=outdir) if write_qa else None
    )

    log_path = outdir / "run.log"
    file_handler = logging.FileHandler(log_path, mode="w")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s")
    )
    root = logging.getLogger()
    root.addHandler(file_handler)
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)

    status_mod.write_status(outdir, "running", stage="starting")

    try:
        topics = load_topics()
        logger.info(
            "loaded %d topics (%d sectors + %d themes)",
            len(topics),
            sum(1 for t in topics if t.kind == "sector"),
            sum(1 for t in topics if t.kind == "theme"),
        )

        if config.USE_WEB_PROBES:
            logger.info("using legacy web-search probes")
            t0 = time.time()
            topic_results = run_web_probes(topics, asof)
            watch_result = None
            logger.info("web probes done in %.1fs", time.time() - t0)
        else:
            if skip_ingest:
                status_mod.write_status(outdir, "running", stage="summarize")
                logger.info("")
                logger.info(
                    "=== SKIP INGEST — loading corpus from %s ===",
                    outdir / "00_news",
                )
                articles = _load_articles_from_snapshots(outdir)
                logger.info("loaded %d unique articles from snapshots", len(articles))
            else:
                status_mod.write_status(outdir, "running", stage="ingest")
                logger.info("")
                logger.info(
                    "=== STAGE: INGEST — Polygon/Benzinga → benzinga_articles (Postgres) ==="
                )
                t0 = time.time()
                ingest_funnel = IngestFunnelData()
                metadata, refresh, articles = ingest.prepare_run(
                    asof, outdir, topics, funnel=ingest_funnel
                )
                (outdir / "metadata.json").write_text(
                    json.dumps(metadata, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                ingest_report.log_ingest_report(
                    ingest_funnel,
                    None,
                    window=ingest.news_window_for_run(asof),
                )
                ingest.persist_news_snapshots(articles, topics, outdir, asof=asof)
                ingest_report.log_routing_report(articles, topics)
                if funnel_report:
                    funnel_report.ingest = ingest_funnel
                logger.info(
                    "INGEST complete in %.1fs — %d upserted, %d corpus stories",
                    time.time() - t0,
                    refresh.unique_upserted,
                    len(metadata.get("corpus_article_ids") or []),
                )

            status_mod.write_status(outdir, "running", stage="summarize")
            summarize_backend = config.SUMMARIZE_BACKEND
            if summarize_backend == "ollama":
                summarize_label = (
                    f"Ollama per-article snippets ({config.OLLAMA_MODEL}, no web)"
                )
            else:
                summarize_label = (
                    "Perplexity reads Benzinga bodies per topic (no web)"
                )
            logger.info("")
            logger.info("=== STAGE: SUMMARIZE — %s ===", summarize_label)
            if summarize_backend == "ollama":
                from market_brief import ollama as ollama_mod

                logger.info("Ollama endpoint: %s", config.OLLAMA_BASE_URL)
                ollama_mod.check_model_available()
            t0 = time.time()
            topic_results = summarize.run_topic_summaries(
                articles,
                topics,
                asof,
                tape_mod=tape_mod,
                outdir=outdir,
                funnel=funnel_report,
                resume=resume_summarize,
            )
            logger.info(
                "topic summaries done: %d blocks, %.1fs",
                len(topic_results),
                time.time() - t0,
            )

            status_mod.write_status(outdir, "running", stage="watch")
            logger.info("")
            logger.info(
                "=== STAGE: WATCH — Perplexity web search for calendar next 24-48h ==="
            )
            watch_result = summarize.run_watch_probe(topics, asof)
            if funnel_report and watch_result:
                funnel_report.watch = {
                    "elapsed_s": watch_result.elapsed_s,
                    "error": watch_result.error,
                    "content_chars": len(watch_result.content or ""),
                }

        persist_summaries(topic_results + ([watch_result] if watch_result else []), outdir)

        status_mod.write_status(outdir, "running", stage="synthesize")
        logger.info("")
        logger.info(
            "=== STAGE: SYNTHESIZE — Perplexity assembles 02_brief.md from topic summaries ==="
        )
        brief_md = synthesize(topic_results, watch_result, asof)
        (outdir / "02_brief.md").write_text(brief_md, encoding="utf-8")

        logger.info(
            "=== STAGE: SYNTHESIZE JSON — Perplexity structures 02_brief.md → 02_brief.json ==="
        )
        try:
            brief_json = synthesize_json(brief_md, asof)
        except PerplexityError as e:
            logger.error("SYNTHESIZE FAILED | 02_brief.json | %s", e)
            brief_json = {"_error": str(e)}
        if funnel_report:
            funnel_report.synth = {
                "brief_md_chars": len(brief_md),
                "json_parse_error": bool(
                    isinstance(brief_json, dict) and brief_json.get("_parse_error")
                ),
            }
        (outdir / "02_brief.json").write_text(
            json.dumps(brief_json, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        usage = USAGE.snapshot()
        (outdir / "usage.json").write_text(
            json.dumps(usage, indent=2),
            encoding="utf-8",
        )
        logger.info(
            "DONE: %s · $%.4f · %d tokens",
            outdir,
            usage["cost_usd_total"],
            usage["total_prompt_tokens"] + usage["total_completion_tokens"],
        )
        status_mod.write_status(outdir, "complete", stage="done")
        if funnel_report:
            funnel_report.usage = usage
            write_funnel_md(funnel_report, outdir / "qa_funnel.md")
            logger.info("QA funnel log: %s", outdir / "qa_funnel.md")
        if config.DISCOVER_THEMES_AFTER_RUN:
            from market_brief.discover_themes import discover_after_brief

            prop_path = discover_after_brief(outdir)
            if prop_path:
                logger.info(
                    "Theme discovery proposals: %s (review, approve, then discover_themes --apply)",
                    prop_path,
                )
        return outdir
    except Exception as e:
        logger.exception("market brief run failed: %s", e)
        if funnel_report:
            funnel_report.errors.append(str(e))
            try:
                write_funnel_md(funnel_report, outdir / "qa_funnel.md")
            except OSError:
                pass
        status_mod.write_status(outdir, "error", stage="failed", error=str(e))
        raise
    finally:
        root.removeHandler(file_handler)
        file_handler.close()
