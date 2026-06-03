"""Human-readable ingest + routing logs for ``run.log``."""

from __future__ import annotations

import logging
from market_brief.funnel_log import IngestFunnelData
from market_brief.trading_calendar import NewsWindow
from market_brief.ingest import (
    IngestStats,
    assign_articles_to_topic,
    assign_unassigned_articles,
    assigned_content_topic_ids,
    content_topics,
)
from market_brief.topics import Topic

logger = logging.getLogger(__name__)


def _hdr(title: str) -> None:
    logger.info("")
    logger.info("=== %s ===", title)


def log_ingest_report(
    funnel: IngestFunnelData,
    stats: IngestStats | None,
    *,
    window: NewsWindow,
) -> None:
    """Write ingest breakdown to the run log."""
    _hdr("INGEST: what was pulled from Benzinga")
    if stats is None:
        logger.info("Published window (synthesis): %s", funnel.window_label or window.label)
        logger.info(
            "  start (UTC): %s | end (UTC): %s",
            funnel.since_iso or window.start_utc.isoformat(),
            funnel.end_iso or window.end_utc.isoformat(),
        )
        logger.info("DB purge: %d rows older than retention deleted", funnel.purge_count)
        logger.info("Ticker universe (screener): %d symbols", funnel.universe_size)
        logger.info("Corpus loaded from DB for synthesis windows: %d", funnel.db_loaded)
        return

    dup = stats.duplicate_rows_removed
    logger.info("Published window: %s", funnel.window_label or window.label)
    logger.info(
        "  start (UTC): %s | end (UTC): %s",
        funnel.since_iso or window.start_utc.isoformat(),
        funnel.end_iso or window.end_utc.isoformat(),
    )
    logger.info(
        "  anchor session (5:00 AM ET): %s | run clock (ET): %s",
        funnel.anchor_session or window.anchor_session.isoformat(),
        window.run_at_et.strftime("%Y-%m-%d %H:%M %Z"),
    )
    logger.info("DB purge: %d rows older than retention deleted", funnel.purge_count)
    logger.info("Ticker fetch universe: %d symbols (one API call each)", funnel.universe_size)

    _hdr("INGEST: general + channel API pulls")
    g = funnel.general
    if g:
        if g.error:
            logger.info("  general (no channel): ERROR — %s", g.error)
        else:
            logger.info(
                "  general (no channel): %d articles from API → %d after time filter"
                " → %d new story ids",
                g.api_rows,
                g.after_filter,
                g.new_unique_ids,
            )
    for ch in funnel.channels:
        if ch.error:
            logger.info("  %-22s: ERROR — %s", ch.label, ch.error)
        else:
            logger.info(
                "  %-22s: %d from API → %d kept → %d new story ids",
                ch.label,
                ch.api_rows,
                ch.after_filter,
                ch.new_unique_ids,
            )
    logger.info(
        "  ROWS from general+channels (before ticker pulls): %d",
        stats.general_channel_rows,
    )

    _hdr("INGEST: per-ticker API pulls")
    ticker_rows = stats.ticker_rows
    hits = sum(1 for n in funnel.ticker_article_counts.values() if n > 0)
    zeros = len(funnel.ticker_article_counts) - hits
    failed = len(funnel.ticker_failed)
    logger.info(
        "  API calls: %d tickers × up to %d articles each",
        stats.ticker_api_calls,
        stats.per_ticker_limit,
    )
    logger.info("  Article rows returned from ticker pulls: %d", ticker_rows)
    logger.info("  Tickers with ≥1 article: %d", hits)
    logger.info("  Tickers with 0 articles: %d", zeros)
    if failed:
        logger.info("  Tickers failed (API error): %d — %s", failed, ", ".join(funnel.ticker_failed[:20]))
    top = sorted(funnel.ticker_article_counts.items(), key=lambda x: -x[1])[:12]
    if top:
        logger.info("  Top tickers by rows returned: %s", ", ".join(f"{t}:{n}" for t, n in top))

    _hdr("INGEST: merge (same story from many pulls counts once)")
    logger.info("  Rows appended from ALL pulls: %d", stats.raw_articles)
    logger.info("    = general/channel rows (%d) + ticker rows (%d)", stats.general_channel_rows, stats.ticker_rows)
    logger.info("  Duplicate rows (same benzinga_id, different pull): %d removed", dup)
    logger.info("  Rows with no benzinga_id: %d dropped", stats.dropped_no_benzinga_id)
    logger.info("  UNIQUE stories kept for pipeline: %d", stats.unique_articles)
    logger.info("  Upserted to Postgres: %d", stats.db_upserted)
    logger.info("  Loaded back from DB for this window: %d", funnel.db_loaded)


def log_routing_report(articles: list[dict], topics: list[Topic]) -> None:
    """Log how many corpus stories land in each topic file / unassigned."""
    _hdr("ROUTING: corpus → 00_news/*.json (by article tickers, not fetch path)")
    logger.info("Corpus size: %d unique stories", len(articles))
    assigned_ids = assigned_content_topic_ids(articles, topics)
    unassigned = assign_unassigned_articles(articles, topics)

    for topic in sorted(content_topics(topics), key=lambda t: (t.kind, t.name)):
        n = len(assign_articles_to_topic(articles, topic))
        logger.info("  [%6s] %-40s %3d articles", topic.kind, topic.name, n)

    logger.info("  [unassigned] %-40s %3d articles (no theme/sector match)", "—", len(unassigned))
    logger.info(
        "  Distinct stories matched ≥1 theme/sector: %d",
        len(assigned_ids),
    )
    logger.info(
        "  Stories only in unassigned bucket: %d",
        len(unassigned),
    )
    logger.info(
        "  Note: one story can appear in multiple theme/sector files if tags overlap;"
        " unassigned is disjoint from theme/sector."
    )
