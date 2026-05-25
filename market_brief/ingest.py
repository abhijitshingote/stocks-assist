"""Benzinga news ingest for the daily market brief."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from daily_screener.utils.db import get_session
from market_brief import config, tape as tape_mod
from market_brief.funnel_log import FetchSlice, IngestFunnelData
from market_brief.ingest_window import filter_published_window, get_news_window
from market_brief.topics import Topic, load_topics
from market_brief.trading_calendar import NewsWindow

import benzinga_news as bz  # noqa: E402 — backend on sys.path via daily_screener.utils.db

logger = logging.getLogger(__name__)


@dataclass
class IngestStats:
    ticker_api_calls: int = 0
    general_api_calls: int = 0
    general_channel_rows: int = 0
    ticker_rows: int = 0
    raw_articles: int = 0
    duplicate_rows_removed: int = 0
    dropped_no_benzinga_id: int = 0
    unique_articles: int = 0
    db_upserted: int = 0
    purged: int = 0
    per_ticker_limit: int = 0


def news_window_start(asof: str | None = None) -> datetime:
    """UTC instant where the ingest window opens (5:00 AM ET on anchor session)."""
    return get_news_window(asof).start_utc


def news_window_for_run(asof: str | None = None) -> NewsWindow:
    """Full published-time window for Benzinga pulls."""
    return get_news_window(asof)


def collect_ticker_universe(topics: list[Topic]) -> list[str]:
    tickers: set[str] = set()
    for topic in topics:
        tickers.update(t.upper() for t in (topic.tickers or []))
        tickers.update(tape_mod.SECTOR_TICKERS.get(topic.name, []))
    return sorted(tickers)


def _slugify(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "topic"


def normalize_ticker_symbol(raw: str) -> str | None:
    """Normalize Benzinga/Polygon ticker strings for set overlap (e.g. ``X:NVDA`` → ``NVDA``)."""
    s = (raw or "").strip().upper()
    if not s:
        return None
    if ":" in s:
        s = s.split(":")[-1]
    s = re.sub(r"[^A-Z0-9.-]", "", s)
    return s or None


def article_ticker_symbols(article: dict) -> set[str]:
    symbols: set[str] = set()
    for raw in article.get("tickers") or []:
        norm = normalize_ticker_symbol(str(raw))
        if norm:
            symbols.add(norm)
    return symbols


def topic_ticker_set(topic: Topic) -> set[str]:
    tickers = list(topic.tickers or [])
    if not tickers:
        tickers = tape_mod.SECTOR_TICKERS.get(topic.name, [])
    out: set[str] = set()
    for t in tickers:
        norm = normalize_ticker_symbol(str(t))
        if norm:
            out.add(norm)
    return out


def content_topics(topics: list[Topic]) -> list[Topic]:
    """Sector + theme rows only (excludes synthetic summarize buckets)."""
    return [t for t in topics if t.kind in ("sector", "theme")]


def assign_articles_to_topic(articles: list[dict], topic: Topic) -> list[dict]:
    """Articles whose ``tickers`` overlap the topic universe."""
    universe = topic_ticker_set(topic)
    if not universe:
        return []
    matched: list[dict] = []
    for article in articles:
        if article_ticker_symbols(article) & universe:
            matched.append(article)
    matched.sort(key=lambda a: a.get("published") or "", reverse=True)
    return matched


def assigned_content_topic_ids(articles: list[dict], topics: list[Topic]) -> set[int]:
    """``benzinga_id`` values that matched at least one theme or sector topic."""
    ids: set[int] = set()
    for topic in content_topics(topics):
        for article in assign_articles_to_topic(articles, topic):
            bid = article.get("benzinga_id")
            if bid is not None:
                ids.add(int(bid))
    return ids


def assign_unassigned_articles(
    articles: list[dict], topics: list[Topic]
) -> list[dict]:
    """Articles that matched no theme and no sector — always summarized for the brief."""
    assigned = assigned_content_topic_ids(articles, topics)
    unassigned: list[dict] = []
    for article in articles:
        bid = article.get("benzinga_id")
        if bid is None:
            continue
        if int(bid) not in assigned:
            unassigned.append(article)
    unassigned.sort(key=lambda a: a.get("published") or "", reverse=True)
    return unassigned


def assign_macro_articles(articles: list[dict], topics: list[Topic]) -> list[dict]:
    """Alias for unassigned bucket (kept for callers expecting the old name)."""
    return assign_unassigned_articles(articles, topics)


def _dedupe_by_id(articles: list[dict]) -> tuple[list[dict], int]:
    """Return (unique articles, count dropped for missing benzinga_id)."""
    by_id: dict[int, dict] = {}
    dropped = 0
    for article in articles:
        bid = article.get("benzinga_id")
        if bid is None:
            dropped += 1
            continue
        by_id[int(bid)] = article
    unique = sorted(
        by_id.values(),
        key=lambda a: a.get("published") or "",
        reverse=True,
    )
    return unique, dropped


def _count_new_ids(rows: list[dict], seen: set[int]) -> int:
    n = 0
    for article in rows:
        bid = article.get("benzinga_id")
        if bid is None:
            continue
        i = int(bid)
        if i not in seen:
            seen.add(i)
            n += 1
    return n


def _channel_tags_on_articles(articles: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for article in articles:
        for ch in article.get("channels") or []:
            key = ch if isinstance(ch, str) else str(ch)
            key = key.strip().lower()
            if key:
                counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: (-x[1], x[0])))


def _fetch_ticker(ticker: str, window: NewsWindow) -> tuple[str, list[dict]]:
    raw = bz.fetch_benzinga_from_api(
        ticker,
        limit=config.PER_TICKER_LIMIT,
        published_gte=window.start_utc,
    )
    return ticker, filter_published_window(raw, window)


def _fetch_general(
    window: NewsWindow,
    funnel: IngestFunnelData | None = None,
) -> list[dict]:
    combined: list[dict] = []
    seen_ids: set[int] = set()

    try:
        raw = bz.fetch_benzinga_general(
            limit=config.GENERAL_NEWS_LIMIT,
            published_gte=window.start_utc,
        )
        filtered = filter_published_window(raw, window)
        combined.extend(filtered)
        if funnel is not None:
            funnel.general = FetchSlice(
                label="general (no channel)",
                api_rows=len(raw),
                after_filter=len(filtered),
                new_unique_ids=_count_new_ids(filtered, seen_ids),
            )
    except Exception as e:  # noqa: BLE001
        if funnel is not None:
            funnel.general = FetchSlice(
                label="general (no channel)",
                error=str(e),
            )
        raise

    for channel, limit in config.GENERAL_CHANNEL_FETCHES:
        slice_row = FetchSlice(label=f"channel:{channel}")
        try:
            raw = bz.fetch_benzinga_general(
                limit=limit,
                published_gte=window.start_utc,
                channels=channel,
            )
            filtered = filter_published_window(raw, window)
            combined.extend(filtered)
            slice_row.api_rows = len(raw)
            slice_row.after_filter = len(filtered)
            slice_row.new_unique_ids = _count_new_ids(filtered, seen_ids)
        except Exception as e:  # noqa: BLE001
            slice_row.error = str(e)
            logger.error("channel fetch failed for %s: %s", channel, e)
        if funnel is not None:
            funnel.channels.append(slice_row)
    return combined


def ingest_all(
    asof: str,
    topics: list[Topic] | None = None,
    *,
    funnel: IngestFunnelData | None = None,
) -> tuple[list[dict], IngestStats]:
    """Pull Benzinga news for the full ticker universe + general feeds."""
    topics = topics or load_topics()
    window = news_window_for_run(asof)
    stats = IngestStats()
    if funnel is not None:
        funnel.since_iso = window.start_utc.isoformat()
        funnel.end_iso = window.end_utc.isoformat()
        funnel.window_label = window.label
        funnel.anchor_session = window.anchor_session.isoformat()

    session = get_session()
    try:
        from models import BenzingaArticle

        BenzingaArticle.__table__.create(session.get_bind(), checkfirst=True)
        stats.purged = bz.purge_articles_older_than(
            session, days=config.ARTICLE_RETENTION_DAYS
        )
        if funnel is not None:
            funnel.purge_count = stats.purged
        logger.info("purged %d benzinga rows older than %d days", stats.purged, config.ARTICLE_RETENTION_DAYS)

        universe = collect_ticker_universe(topics)
        stats.per_ticker_limit = config.PER_TICKER_LIMIT
        if funnel is not None:
            funnel.universe_size = len(universe)
            funnel.universe_sample = universe[:25]

        all_raw: list[dict] = []

        general = _fetch_general(window, funnel=funnel)
        stats.general_api_calls = 1 + len(config.GENERAL_CHANNEL_FETCHES)
        stats.general_channel_rows = len(general)
        all_raw.extend(general)

        ticker_counts: dict[str, int] = {}
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=config.INGEST_CONCURRENCY
        ) as pool:
            futures = {
                pool.submit(_fetch_ticker, ticker, window): ticker
                for ticker in universe
            }
            for fut in concurrent.futures.as_completed(futures):
                ticker = futures[fut]
                try:
                    _, rows = fut.result()
                    stats.ticker_api_calls += 1
                    ticker_counts[ticker] = len(rows)
                    all_raw.extend(rows)
                except Exception as e:  # noqa: BLE001
                    logger.error("benzinga fetch failed for %s: %s", ticker, e)
                    if funnel is not None:
                        funnel.ticker_failed.append(ticker)

        stats.ticker_rows = sum(ticker_counts.values())
        if funnel is not None:
            funnel.ticker_ok = stats.ticker_api_calls
            funnel.ticker_article_counts = ticker_counts

        stats.raw_articles = len(all_raw)
        unique, dropped_no_id = _dedupe_by_id(all_raw)
        stats.unique_articles = len(unique)
        stats.dropped_no_benzinga_id = dropped_no_id
        stats.duplicate_rows_removed = stats.raw_articles - stats.unique_articles - dropped_no_id
        if funnel is not None:
            funnel.raw_rows = stats.raw_articles
            funnel.dropped_no_benzinga_id = dropped_no_id
            funnel.unique_articles = stats.unique_articles

        stats.db_upserted = bz.upsert_articles(session, unique)
        if funnel is not None:
            funnel.db_upserted = stats.db_upserted

        db_rows = bz.load_articles_since(session, window.start_utc, limit=10000)
        db_rows = [
            r
            for r in db_rows
            if r.published is not None and r.published <= window.end_utc
        ]
        articles = [bz.article_to_json(r) for r in db_rows]
        if funnel is not None:
            funnel.db_loaded = len(articles)
            funnel.channel_tags_on_corpus = _channel_tags_on_articles(articles)
            funnel.tickers_fetched_zero_articles = sorted(
                t for t in universe if ticker_counts.get(t, 0) == 0
            )
            for topic in topics:
                funnel.topic_assignment[topic.name] = len(
                    assign_articles_to_topic(articles, topic)
                )
            n_unassigned = len(assign_unassigned_articles(articles, topics))
            funnel.unassigned_count = n_unassigned
            funnel.macro_count = n_unassigned
            funnel.assigned_content_ids = len(
                assigned_content_topic_ids(articles, topics)
            )
        return articles, stats
    finally:
        session.close()


def persist_news_snapshots(
    articles: list[dict],
    topics: list[Topic],
    outdir: Path,
    *,
    asof: str | None = None,
) -> None:
    """Write per-topic + macro JSON snapshots under ``00_news/``."""
    news_dir = outdir / "00_news"
    news_dir.mkdir(parents=True, exist_ok=True)

    win = news_window_for_run(asof)
    manifest = {
        "window": win.label,
        "anchor_session": win.anchor_session.isoformat(),
        "published_gte_utc": win.start_utc.isoformat(),
        "published_lte_utc": win.end_utc.isoformat(),
        "article_count": len(articles),
        "topics": {},
    }

    for topic in topics:
        matched = assign_articles_to_topic(articles, topic)
        slug = _slugify(topic.name)
        path = news_dir / f"{slug}.json"
        path.write_text(json.dumps(matched, indent=2, ensure_ascii=False), encoding="utf-8")
        manifest["topics"][topic.name] = len(matched)

    unassigned = assign_unassigned_articles(articles, topics)
    payload = json.dumps(unassigned, indent=2, ensure_ascii=False)
    (news_dir / "_unassigned.json").write_text(payload, encoding="utf-8")
    (news_dir / "_macro.json").write_text(payload, encoding="utf-8")
    manifest["unassigned"] = len(unassigned)
    manifest["macro"] = len(unassigned)

    (news_dir / "_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
