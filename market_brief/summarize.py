"""Summarize Benzinga article bundles into topic-level brief blocks."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from daily_screener.utils.perplexity import PerplexityError, call_perplexity

from market_brief import config, ollama, prompts
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


def _assemble_topic_from_snippets(
    topic: Topic,
    snippets: list[tuple[dict, str]],
    *,
    tape_block: str | None,
    errors: list[str],
    include_source_body: bool = False,
    tape_at_end: bool = False,
) -> str:
    from market_brief.snippet_price import split_fundamental_and_price
    from market_brief.trading_calendar import prior_session_for_brief

    session = prior_session_for_brief(None).isoformat()
    parts: list[str] = [f"## {topic.name}\n"]
    if tape_block and not tape_at_end:
        parts.append("### Verified tape\n")
        parts.append(tape_block.strip())
        parts.append("")
    if errors:
        parts.append(f"_Ollama errors on {len(errors)} article(s): {'; '.join(errors[:3])}_\n")
    for article, snippet in snippets:
        title = article.get("title") or "(untitled)"
        bid = article.get("benzinga_id")
        pub = article.get("published") or article.get("published_date") or ""
        url = article.get("url") or ""
        parts.append(f"### {title}\n")
        parts.append(f"_benzinga_id: {bid} · published: {pub}_")
        if url:
            parts.append(f" · {url}")
        parts.append("\n\n")
        parts.append("#### Snippet (Ollama)\n\n")
        fund, price_block = split_fundamental_and_price(snippet)
        if fund is None:
            from market_brief.persist import _split_snippet_file

            fund, price_block, _ = _split_snippet_file(snippet)
        parts.append((fund or snippet).strip())
        if price_block:
            parts.append("\n\n#### Price reference\n\n")
            parts.append(price_block.replace("## Price reference", "", 1).strip())
        if include_source_body:
            from market_brief.persist import article_source_body

            parts.append("\n\n#### Source article\n\n")
            parts.append(article_source_body(article))
        parts.append("\n\n---\n\n")
    if not snippets and not errors:
        parts.append(
            f"_No Benzinga articles in the trading-day window "
            f"(from 5:00 AM ET on session {session}) matched this topic._\n"
        )
    if tape_block and tape_at_end:
        parts.append("\n### Verified tape (session)\n\n")
        parts.append(tape_block.strip())
        parts.append("\n")
    return "".join(parts).rstrip() + "\n"


def summarize_topic_articles_ollama(
    topic: Topic,
    articles: list[dict],
    asof: str,
    tape_block: str | None = None,
    *,
    outdir: Path | None = None,
    resume: bool = False,
) -> str:
    if not articles:
        return _assemble_topic_from_snippets(topic, [], tape_block=tape_block, errors=[])

    from market_brief.persist import (
        is_topic_ollama_complete,
        load_article_snippet,
        mark_topic_ollama_complete,
        persist_article_snippet,
        persist_topic_partial,
    )

    if outdir and resume and is_topic_ollama_complete(outdir, topic.name):
        from market_brief.persist import _slugify

        final = outdir / "01_summaries" / f"{_slugify(topic.name)}__benzinga.md"
        logger.info(
            "SUMMARIZE skip | [%s] %s | ollama complete (resume) | %s",
            topic.kind,
            topic.name,
            final.name,
        )
        if final.is_file():
            text = final.read_text(encoding="utf-8")
            marker = "\n\n"
            if text.startswith("# ") and marker in text:
                return text.split(marker, 2)[-1]
            return text
        return _assemble_topic_from_snippets(topic, [], tape_block=tape_block, errors=[])

    ollama.check_model_available()
    snippets: list[tuple[dict, str]] = []
    errors: list[str] = []
    n = len(articles)
    for i, article in enumerate(articles, start=1):
        bid = article.get("benzinga_id")
        label = (
            f"OLLAMA SNIPPET | [{topic.kind}] {topic.name} | "
            f"article {i}/{n} | benzinga_id={bid}"
        )
        text: str | None = None
        if outdir and resume and bid is not None:
            text = load_article_snippet(outdir, int(bid))
            if text:
                from market_brief.snippet_price import append_price_reference

                logger.info("%s | reused cached snippet", label)
                text = append_price_reference(text, article, asof)
        try:
            if text is None:
                from market_brief.snippet_price import (
                    append_price_reference,
                    price_reference_footer,
                )

                text = ollama.summarize_article(article, log_label=label)
                price_ref = price_reference_footer(article, asof)
                if outdir:
                    persist_article_snippet(
                        outdir,
                        article,
                        text,
                        price_reference=price_ref,
                    )
                text = append_price_reference(text, article, asof)
            snippets.append((article, text))
        except ollama.OllamaError as e:
            logger.error("%s | %s", label, e)
            errors.append(f"{bid}: {e}")
        if outdir:
            partial_body = _assemble_topic_from_snippets(
                topic,
                snippets,
                tape_block=tape_block,
                errors=errors,
                include_source_body=True,
                tape_at_end=True,
            )
            persist_topic_partial(
                outdir,
                topic.name,
                topic.kind,
                partial_body,
                articles_done=i,
                articles_total=n,
            )
    content = _assemble_topic_from_snippets(
        topic,
        snippets,
        tape_block=tape_block,
        errors=errors,
        tape_at_end=True,
    )
    if outdir:
        mark_topic_ollama_complete(outdir, topic.name)
    return content


def summarize_topic_articles(
    topic: Topic,
    articles: list[dict],
    asof: str,
    tape_block: str | None = None,
    *,
    outdir: Path | None = None,
    resume: bool = False,
) -> str:
    if config.SUMMARIZE_BACKEND == "ollama":
        return summarize_topic_articles_ollama(
            topic,
            articles,
            asof,
            tape_block=tape_block,
            outdir=outdir,
            resume=resume,
        )
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
    resume: bool = False,
) -> list[ProbeResult]:
    """Turn ingested Benzinga articles into per-topic summary blocks."""
    from market_brief.ingest import assign_articles_to_topic, assign_unassigned_articles

    results: list[ProbeResult] = []

    unassigned = assign_unassigned_articles(articles, topics)
    from market_brief.topics import order_topics_for_summarize

    topics_to_run = order_topics_for_summarize(list(topics))
    if unassigned:
        topics_to_run.append(
            Topic(
                name=config.UNASSIGNED_TOPIC_NAME,
                desc=config.UNASSIGNED_TOPIC_DESC,
                tickers=[],
                kind="unassigned",
            )
        )
    logger.info(
        "summarize topic order: %s",
        " → ".join(t.name for t in topics_to_run),
    )

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
        if config.SUMMARIZE_BACKEND == "ollama":
            planned_calls = len(matched)
            backend = f"Ollama/{config.OLLAMA_MODEL}"
        else:
            backend = f"Perplexity/{config.TOPIC_SUMMARY_MODEL}"
        logger.info(
            "SUMMARIZE topic | [%s] %s | %d articles | %d call(s) | %s",
            topic.kind,
            topic.name,
            len(matched),
            planned_calls,
            backend,
        )
        t0 = time.time()
        try:
            content = summarize_topic_articles(
                topic,
                matched,
                asof,
                tape_block=tape_block,
                outdir=outdir,
                resume=resume,
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
        except (PerplexityError, ollama.OllamaError) as e:
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
    if config.SUMMARIZE_BACKEND == "ollama":
        ollama.unload_model()

    return results


def run_watch_probe(topics: list[Topic], asof: str) -> ProbeResult:
    """Perplexity web probe for calendar events (not in Benzinga history)."""
    from market_brief.ingest import collect_ticker_universe

    tickers = collect_ticker_universe(topics, asof=asof)
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
