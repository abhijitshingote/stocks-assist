"""Benzinga news ingest for the daily market brief."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from daily_screener.utils.db import get_session
from market_brief import config, tape as tape_mod
from market_brief.funnel_log import FetchSlice, IngestFunnelData
from market_brief.ingest_window import (
    filter_published_window,
    get_news_window,
    get_ticker_news_window,
)
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


@dataclass
class IngestSourceSlices:
    """Per-API-pull article lists (after published-window filter, before corpus dedupe)."""

    general: list[dict]
    channels: dict[str, list[dict]]
    ticker_universe_slices: list  # market_brief.screener_universe.UniverseSlice
    ticker_articles: dict[str, list[dict]]
    ticker_lineage: dict


def news_window_start(asof: str | None = None) -> datetime:
    """UTC instant where the ingest window opens (5:00 AM ET on anchor session)."""
    return get_news_window(asof).start_utc


def news_window_for_run(asof: str | None = None) -> NewsWindow:
    """Full published-time window for Benzinga pulls."""
    return get_news_window(asof)


def collect_ticker_universe(
    topics: list[Topic] | None = None, *, asof: str | None = None
) -> list[str]:
    """Per-ticker Benzinga symbols from DB screener slices (not themes.json)."""
    from market_brief.screener_universe import collect_ticker_symbols

    asof = asof or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return collect_ticker_symbols(asof)


def persist_ticker_lineage(
    lineage: dict,
    exclusive_slices: list,
    outdir: Path,
) -> None:
    """Write ``overview.md`` + ``lineage.json`` under ``source/ticker_universe/``."""
    from market_brief.screener_universe import render_overview_markdown

    root = outdir / "source" / "ticker_universe"
    root.mkdir(parents=True, exist_ok=True)
    (root / "lineage.json").write_text(
        json.dumps(lineage, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (root / "overview.md").write_text(
        render_overview_markdown(exclusive_slices, lineage),
        encoding="utf-8",
    )
    logger.info(
        "ticker universe overview: %d symbols → %s",
        lineage.get("unique_tickers_assigned", 0),
        root / "overview.md",
    )


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
) -> tuple[list[dict], dict[str, list[dict]]]:
    """Return (general-only rows, channel name → rows)."""
    general_rows: list[dict] = []
    channel_rows: dict[str, list[dict]] = {}
    seen_ids: set[int] = set()

    try:
        raw = bz.fetch_benzinga_general(
            limit=config.GENERAL_NEWS_LIMIT,
            published_gte=window.start_utc,
        )
        filtered = filter_published_window(raw, window)
        general_rows = filtered
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
            channel_rows[channel] = filtered
            slice_row.api_rows = len(raw)
            slice_row.after_filter = len(filtered)
            slice_row.new_unique_ids = _count_new_ids(filtered, seen_ids)
        except Exception as e:  # noqa: BLE001
            slice_row.error = str(e)
            channel_rows[channel] = []
            logger.error("channel fetch failed for %s: %s", channel, e)
        if funnel is not None:
            funnel.channels.append(slice_row)
    return general_rows, channel_rows


def ingest_all(
    asof: str,
    topics: list[Topic] | None = None,
    *,
    funnel: IngestFunnelData | None = None,
) -> tuple[list[dict], IngestStats, IngestSourceSlices]:
    """Pull Benzinga news for the full ticker universe + general feeds."""
    topics = topics or load_topics()
    general_window = news_window_for_run(asof)
    ticker_window = get_ticker_news_window(asof)
    stats = IngestStats()
    if funnel is not None:
        funnel.since_iso = general_window.start_utc.isoformat()
        funnel.end_iso = general_window.end_utc.isoformat()
        funnel.window_label = (
            f"general: {general_window.label} | ticker: {ticker_window.label}"
        )
        funnel.anchor_session = general_window.anchor_session.isoformat()

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

        from market_brief.screener_universe import build_screener_universe

        universe_slices, ticker_lineage, universe = build_screener_universe(asof)
        stats.per_ticker_limit = config.PER_TICKER_LIMIT
        if funnel is not None:
            funnel.universe_size = len(universe)
            funnel.universe_sample = universe[:25]

        all_raw: list[dict] = []

        general_rows, channel_rows = _fetch_general(general_window, funnel=funnel)
        stats.general_api_calls = 1 + len(config.GENERAL_CHANNEL_FETCHES)
        stats.general_channel_rows = len(general_rows) + sum(
            len(rows) for rows in channel_rows.values()
        )
        all_raw.extend(general_rows)
        for rows in channel_rows.values():
            all_raw.extend(rows)

        ticker_articles: dict[str, list[dict]] = {}
        ticker_counts: dict[str, int] = {}
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=config.INGEST_CONCURRENCY
        ) as pool:
            futures = {
                pool.submit(_fetch_ticker, ticker, ticker_window): ticker
                for ticker in universe
            }
            for fut in concurrent.futures.as_completed(futures):
                ticker = futures[fut]
                try:
                    _, rows = fut.result()
                    stats.ticker_api_calls += 1
                    ticker_counts[ticker] = len(rows)
                    ticker_articles[ticker] = rows
                    all_raw.extend(rows)
                except Exception as e:  # noqa: BLE001
                    logger.error("benzinga fetch failed for %s: %s", ticker, e)
                    ticker_articles[ticker] = []
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

        corpus_start = min(general_window.start_utc, ticker_window.start_utc)
        db_rows = bz.load_articles_since(session, corpus_start, limit=10000)
        db_rows = [
            r
            for r in db_rows
            if r.published is not None and r.published <= general_window.end_utc
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
        slices = IngestSourceSlices(
            general=general_rows,
            channels=channel_rows,
            ticker_universe_slices=universe_slices,
            ticker_articles=ticker_articles,
            ticker_lineage=ticker_lineage,
        )
        return articles, stats, slices
    finally:
        session.close()


def _write_source_slice(
    base: Path,
    articles: list[dict],
    *,
    source_kind: str,
    source_key: str,
    window: NewsWindow,
    limit: int | None = None,
    selection: list[dict] | None = None,
    extra: dict | None = None,
) -> None:
    base.mkdir(parents=True, exist_ok=True)
    payload: dict = {
        "source_kind": source_kind,
        "source_key": source_key,
        "window": window.label,
        "anchor_session": window.anchor_session.isoformat(),
        "published_gte_utc": window.start_utc.isoformat(),
        "published_lte_utc": window.end_utc.isoformat(),
        "api_limit": limit,
        "article_count": len(articles),
        "articles": articles,
    }
    if selection is not None:
        payload["selection"] = selection
        payload["selection_count"] = len(selection)
    if extra:
        payload.update(extra)
    (base / "articles.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _prune_stale_source_dirs(root: Path, active_tickers: set[str]) -> None:
    """Drop per-ticker dirs and legacy slice folders not in this run."""
    ticker_root = root / "ticker"
    if ticker_root.is_dir():
        for path in ticker_root.iterdir():
            if path.is_dir() and path.name.upper() not in active_tickers:
                shutil.rmtree(path, ignore_errors=True)

    tu_root = root / "ticker_universe"
    if tu_root.is_dir():
        for name in ("r1d", "vol_spike_5d", "main_view_ti65"):
            legacy = tu_root / name
            if legacy.is_dir():
                shutil.rmtree(legacy, ignore_errors=True)

    for stale in ("ticker_universe_map.md", "ticker_universe_map.json", "lineage.md"):
        p = root / stale
        if p.is_file():
            p.unlink()


def persist_source_snapshots(
    slices: IngestSourceSlices,
    outdir: Path,
    topics: list[Topic] | None = None,
    *,
    asof: str | None = None,
) -> None:
    """Write ``source/general``, ``source/channel/…``, ``source/ticker/<SYM>/``."""
    general_window = news_window_for_run(asof)
    ticker_window = get_ticker_news_window(asof)
    root = outdir / "source"
    root.mkdir(parents=True, exist_ok=True)

    active_tickers = {sym.upper() for sym in slices.ticker_articles}
    _prune_stale_source_dirs(root, active_tickers)

    persist_ticker_lineage(
        slices.ticker_lineage, slices.ticker_universe_slices, outdir
    )

    _write_source_slice(
        root / "general",
        slices.general,
        source_kind="general",
        source_key="general",
        window=general_window,
        limit=config.GENERAL_NEWS_LIMIT,
    )

    channel_limits = dict(config.GENERAL_CHANNEL_FETCHES)
    for channel, articles in slices.channels.items():
        slug = _slugify(channel)
        _write_source_slice(
            root / "channel" / slug,
            articles,
            source_kind="channel",
            source_key=channel,
            window=general_window,
            limit=channel_limits.get(channel),
        )

    by_ticker = slices.ticker_lineage.get("by_ticker") or {}
    for sym in sorted(slices.ticker_articles):
        meta = by_ticker.get(sym) or {}
        _write_source_slice(
            root / "ticker" / sym,
            slices.ticker_articles[sym],
            source_kind="ticker",
            source_key=sym,
            window=ticker_window,
            limit=config.PER_TICKER_LIMIT,
            extra={
                "section": meta.get("section"),
                "label": meta.get("label"),
                "rank": meta.get("rank"),
            },
        )

    slice_manifest = [
        {
            "slice_id": sl.slice_id,
            "cap_bucket": sl.cap_bucket,
            "label": sl.label,
            "selection_count": len(sl.selection),
            "tickers": [r["ticker"] for r in sl.selection],
        }
        for sl in slices.ticker_universe_slices
    ]

    manifest = {
        "general_window": general_window.label,
        "ticker_window": ticker_window.label,
        "anchor_session": general_window.anchor_session.isoformat(),
        "general_published_gte_utc": general_window.start_utc.isoformat(),
        "ticker_published_gte_utc": ticker_window.start_utc.isoformat(),
        "published_lte_utc": general_window.end_utc.isoformat(),
        "general_count": len(slices.general),
        "channels": {name: len(rows) for name, rows in slices.channels.items()},
        "ticker_count": len(slices.ticker_articles),
        "ticker_rows_total": sum(len(rows) for rows in slices.ticker_articles.values()),
        "ticker_universe_slices": slice_manifest,
        "overview": "ticker_universe/overview.md",
    }
    (root / "_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(
        "source snapshots: general=%d channels=%d tickers=%d → %s",
        len(slices.general),
        len(slices.channels),
        len(slices.ticker_articles),
        root,
    )


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
