"""Summarize Benzinga article bundles into topic-level brief blocks."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from daily_screener.utils.perplexity import PerplexityError, call_perplexity

from market_brief import config, prompts
from market_brief.ingest import assign_articles_to_topic, assign_unassigned_articles
from market_brief.topics import Topic
from market_brief.funnel_log import FunnelReport, SummarizeFunnelRow
from market_brief.types import ProbeResult

logger = logging.getLogger(__name__)


@dataclass
class ArticleChunk:
    index: int
    total: int
    text: str


def _article_block(article: dict, index: int) -> str:
    tickers = ", ".join(article.get("tickers") or []) or "(none)"
    channels = ", ".join(article.get("channels") or []) or "(none)"
    body = (article.get("body_text") or article.get("teaser") or "").strip()
    return (
        f"### Article {index}\n"
        f"- benzinga_id: {article.get('benzinga_id')}\n"
        f"- published: {article.get('published') or article.get('published_date')}\n"
        f"- tickers: {tickers}\n"
        f"- channels: {channels}\n"
        f"- title: {article.get('title')}\n"
        f"- url: {article.get('url')}\n"
        f"- author: {article.get('author')}\n\n"
        f"{body}\n"
    )


def _chunk_articles(articles: list[dict]) -> list[ArticleChunk]:
    if not articles:
        return []
    max_chars = config.CHUNK_MAX_CHARS
    chunks: list[ArticleChunk] = []
    current: list[str] = []
    current_len = 0

    for i, article in enumerate(articles, start=1):
        block = _article_block(article, i)
        if current and current_len + len(block) > max_chars:
            chunks.append(
                ArticleChunk(
                    index=len(chunks) + 1,
                    total=0,
                    text="".join(current),
                )
            )
            current = []
            current_len = 0
        current.append(block)
        current_len += len(block)

    if current:
        chunks.append(
            ArticleChunk(index=len(chunks) + 1, total=0, text="".join(current))
        )

    total = len(chunks)
    return [
        ArticleChunk(index=c.index, total=total, text=c.text) for c in chunks
    ]


def _summarize_chunk(
    topic: Topic,
    chunk: ArticleChunk,
    asof: str,
    tape_block: str | None,
    *,
    total_articles: int,
) -> str:
    prompt = prompts.build_benzinga_chunk_prompt(
        topic.as_dict(),
        chunk.text,
        asof=asof,
        tape_block=tape_block,
        chunk_index=chunk.index,
        chunk_total=chunk.total,
    )
    n_in_chunk = chunk.text.count("### Article")
    label = (
        f"SUMMARIZE | [{topic.kind}] {topic.name} | chunk {chunk.index}/{chunk.total} "
        f"| {n_in_chunk} of {total_articles} article(s) in this request "
        f"| task: summarize Benzinga article bodies → markdown (no web search)"
    )
    return call_perplexity(
        prompt,
        model=config.TOPIC_SUMMARY_MODEL,
        max_tokens=config.TOPIC_SUMMARY_MAX_TOKENS,
        temperature=config.TOPIC_SUMMARY_TEMPERATURE,
        timeout=config.TOPIC_SUMMARY_TIMEOUT_SECONDS,
        log_label=label,
    )


def _merge_chunk_summaries(
    topic: Topic,
    parts: list[str],
    asof: str,
    tape_block: str | None,
) -> str:
    prompt = prompts.build_benzinga_merge_prompt(
        topic.as_dict(),
        parts,
        asof=asof,
        tape_block=tape_block,
    )
    label = (
        f"SUMMARIZE | [{topic.kind}] {topic.name} | merge {len(parts)} partial chunk(s) "
        f"| task: combine partial summaries into one topic section (no web)"
    )
    return call_perplexity(
        prompt,
        model=config.TOPIC_SUMMARY_MODEL,
        max_tokens=config.TOPIC_SUMMARY_MAX_TOKENS,
        temperature=config.TOPIC_SUMMARY_TEMPERATURE,
        timeout=config.TOPIC_SUMMARY_TIMEOUT_SECONDS,
        log_label=label,
    )


def summarize_topic_articles(
    topic: Topic,
    articles: list[dict],
    asof: str,
    tape_block: str | None = None,
) -> str:
    if not articles:
        from market_brief.trading_calendar import prior_session_for_brief

        session = prior_session_for_brief(asof or None).isoformat()
        return (
            f"## {topic.name}\n\n"
            f"_No Benzinga articles in the trading-day window "
            f"(from 5:00 AM ET on session {session}) matched this topic._\n"
        )

    chunks = _chunk_articles(articles)
    n = len(articles)
    if len(chunks) == 1:
        return _summarize_chunk(topic, chunks[0], asof, tape_block, total_articles=n)

    partials: list[str] = []
    for chunk in chunks:
        partials.append(
            _summarize_chunk(topic, chunk, asof, tape_block, total_articles=n)
        )
    return _merge_chunk_summaries(topic, partials, asof, tape_block)


def run_topic_summaries(
    articles: list[dict],
    topics: list[Topic],
    asof: str,
    *,
    tape_mod,
    outdir: Path | None = None,
    funnel: FunnelReport | None = None,
) -> list[ProbeResult]:
    """Turn ingested Benzinga articles into per-topic summary blocks."""
    results: list[ProbeResult] = []

    unassigned = assign_unassigned_articles(articles, topics)
    prepend: list[Topic] = []
    if unassigned:
        prepend.append(
            Topic(
                name=config.UNASSIGNED_TOPIC_NAME,
                desc=config.UNASSIGNED_TOPIC_DESC,
                tickers=[],
                kind="unassigned",
            )
        )
    topics_to_run = prepend + list(topics)

    for topic in topics_to_run:
        if topic.kind == "unassigned":
            matched = unassigned
        else:
            matched = assign_articles_to_topic(articles, topic)

        try:
            tape_block = (
                None
                if topic.kind in ("macro", "unassigned")
                else tape_mod.get_topic_tape_block(topic.as_dict(), asof)
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("tape lookup failed for %s: %s", topic.name, e)
            tape_block = None

        n_chunks = len(_chunk_articles(matched)) if matched else 1
        planned_calls = n_chunks if matched else 0
        if matched and n_chunks > 1:
            planned_calls = n_chunks + 1  # chunks + merge
        logger.info(
            "SUMMARIZE topic | [%s] %s | %d articles | %d Perplexity call(s) planned",
            topic.kind,
            topic.name,
            len(matched),
            planned_calls,
        )
        t0 = time.time()
        try:
            content = summarize_topic_articles(
                topic, matched, asof, tape_block=tape_block
            )
            elapsed = time.time() - t0
            result = ProbeResult(
                topic_name=topic.name,
                topic_kind=topic.kind,
                kind="benzinga",
                content=content,
                elapsed_s=elapsed,
            )
            results.append(result)
            if funnel is not None:
                funnel.summarize_rows.append(
                    SummarizeFunnelRow(
                        topic_name=topic.name,
                        kind=topic.kind,
                        articles_in=len(matched),
                        chunks=n_chunks,
                        elapsed_s=elapsed,
                        content_chars=len(content or ""),
                    )
                )
            if outdir is not None:
                from market_brief.persist import persist_summaries

                persist_summaries(results, outdir)
                from market_brief import status as status_mod

                status_mod.write_status(
                    outdir,
                    "running",
                    stage=f"summarize:{topic.name}",
                )
            logger.info(
                "SUMMARIZE done: [%s] %s — %d articles in → %d chars out (%.1fs)",
                topic.kind,
                topic.name,
                len(matched),
                len(content or ""),
                elapsed,
            )
        except PerplexityError as e:
            logger.error(
                "SUMMARIZE FAILED | [%s] %s | %d articles | lost for final brief unless retried: %s",
                topic.kind,
                topic.name,
                len(matched),
                e,
            )
            elapsed = time.time() - t0
            results.append(
                ProbeResult(
                    topic_name=topic.name,
                    topic_kind=topic.kind,
                    kind="benzinga",
                    content="",
                    error=str(e),
                    elapsed_s=elapsed,
                )
            )
            if funnel is not None:
                funnel.summarize_rows.append(
                    SummarizeFunnelRow(
                        topic_name=topic.name,
                        kind=topic.kind,
                        articles_in=len(matched),
                        chunks=n_chunks,
                        elapsed_s=elapsed,
                        error=str(e),
                    )
                )
    return results


def run_watch_probe(topics: list[Topic], asof: str) -> ProbeResult:
    """Perplexity web probe for calendar events (not in Benzinga history)."""
    from market_brief.ingest import collect_ticker_universe

    tickers = collect_ticker_universe(topics)
    t0 = time.time()
    prompt = prompts.build_watch_tomorrow_prompt(tickers, asof=asof)
    label = (
        f"WATCH | calendar next 24-48h | {len(tickers)} tickers in universe "
        f"| task: web search for scheduled events (not Benzinga bodies)"
    )
    try:
        content = call_perplexity(
            prompt,
            model=config.WATCH_PROBE_MODEL,
            max_tokens=config.WATCH_PROBE_MAX_TOKENS,
            temperature=config.WATCH_PROBE_TEMPERATURE,
            timeout=config.WATCH_PROBE_TIMEOUT_SECONDS,
            log_label=label,
        )
        return ProbeResult(
            topic_name="Watch next session",
            topic_kind="calendar",
            kind="watch",
            content=content,
            elapsed_s=time.time() - t0,
        )
    except PerplexityError as e:
        logger.error("WATCH FAILED | calendar probe | %s", e)
        return ProbeResult(
            topic_name="Watch next session",
            topic_kind="calendar",
            kind="watch",
            content="",
            error=str(e),
            elapsed_s=time.time() - t0,
        )
