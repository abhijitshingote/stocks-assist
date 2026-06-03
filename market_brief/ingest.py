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
from market_brief.funnel_log import IngestFunnelData
from market_brief.ingest_window import get_news_window, get_ticker_news_window
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


def article_ids_from_rows(articles: list[dict]) -> list[str]:
    """Stable list of benzinga_id strings (preserves first-seen order)."""
    out: list[str] = []
    seen: set[int] = set()
    for article in articles:
        bid = article.get("benzinga_id")
        if bid is None:
            continue
        i = int(bid)
        if i in seen:
            continue
        seen.add(i)
        out.append(str(i))
    return out


def _channel_tags_on_articles(articles: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for article in articles:
        for ch in article.get("channels") or []:
            key = ch if isinstance(ch, str) else str(ch)
            key = key.strip().lower()
            if key:
                counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: (-x[1], x[0])))


def prepare_run(
    asof: str,
    outdir: Path,
    topics: list[Topic] | None = None,
    *,
    funnel: IngestFunnelData | None = None,
) -> tuple[dict[str, Any], bz.RefreshStats, list[dict]]:
    """Refresh DB, write ticker universe under ``source/``, return metadata + corpus articles."""
    from market_brief import source_loader
    from market_brief.screener_universe import build_screener_universe

    topics = topics or load_topics()
    general_window = news_window_for_run(asof)
    ticker_window = get_ticker_news_window(asof)
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
        refresh = bz.refresh_benzinga_articles(
            session,
            days=config.REFRESH_LOOKBACK_DAYS,
            limit=config.REFRESH_API_LIMIT,
            purge_days=config.ARTICLE_RETENTION_DAYS,
            end=general_window.end_utc,
        )
        if funnel is not None:
            funnel.purge_count = refresh.purged

        universe_slices, ticker_lineage, universe = build_screener_universe(asof)
        universe_set = {s.upper() for s in universe}
        if funnel is not None:
            funnel.universe_size = len(universe)
            funnel.universe_sample = universe[:25]

        slices = IngestSourceSlices(
            general=[],
            channels={},
            ticker_universe_slices=universe_slices,
            ticker_articles={},
            ticker_lineage=ticker_lineage,
        )
        persist_source_snapshots(slices, outdir, topics, asof=asof)

        articles, corpus_ids = source_loader.load_synthesis_corpus(
            session, asof, universe_set
        )
        if funnel is not None:
            funnel.db_upserted = refresh.unique_upserted
            funnel.unique_articles = len(articles)
            funnel.db_loaded = len(articles)
            funnel.channel_tags_on_corpus = _channel_tags_on_articles(articles)
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

        metadata = build_ingest_metadata(
            asof,
            slices,
            corpus_article_ids=corpus_ids,
            refresh=refresh,
            funnel=funnel,
        )
        return metadata, refresh, articles
    finally:
        session.close()


def build_ingest_metadata(
    asof: str,
    slices: IngestSourceSlices,
    *,
    corpus_article_ids: list[str],
    refresh: bz.RefreshStats | None = None,
    funnel: IngestFunnelData | None = None,
) -> dict:
    """Run-level metadata: synthesis windows, corpus ids, refresh stats."""
    general_window = news_window_for_run(asof)
    ticker_window = get_ticker_news_window(asof)

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
    universe_symbols = sorted(
        (slices.ticker_lineage.get("by_ticker") or {}).keys()
    )

    metadata: dict = {
        "asof": asof,
        "windows": {
            "general_window": general_window.label,
            "ticker_window": ticker_window.label,
            "anchor_session": general_window.anchor_session.isoformat(),
            "general_published_gte_utc": general_window.start_utc.isoformat(),
            "ticker_published_gte_utc": ticker_window.start_utc.isoformat(),
            "published_lte_utc": general_window.end_utc.isoformat(),
        },
        "corpus_article_ids": corpus_article_ids,
        "universe_symbols": universe_symbols,
        "ticker_universe_slices": slice_manifest,
        "ticker_universe_overview": "source/ticker_universe/overview.md",
        "refresh": {
            "lookback_days": config.REFRESH_LOOKBACK_DAYS,
        },
    }
    if refresh is not None:
        metadata["refresh"] = {
            "lookback_days": refresh.lookback_days,
            "start_utc": refresh.start_utc,
            "end_utc": refresh.end_utc,
            "purged": refresh.purged,
            "api_rows": refresh.api_rows,
            "unique_upserted": refresh.unique_upserted,
        }
    if funnel is not None:
        metadata["funnel"] = {
            "since_iso": funnel.since_iso,
            "end_iso": funnel.end_iso,
            "window_label": funnel.window_label,
            "anchor_session": funnel.anchor_session,
            "universe_size": funnel.universe_size,
            "purge_count": funnel.purge_count,
            "general": funnel.general.__dict__ if funnel.general else None,
            "channels": [c.__dict__ for c in funnel.channels],
            "ticker_ok": funnel.ticker_ok,
            "ticker_failed": funnel.ticker_failed,
            "ticker_article_counts": funnel.ticker_article_counts,
            "raw_rows": funnel.raw_rows,
            "dropped_no_benzinga_id": funnel.dropped_no_benzinga_id,
            "unique_articles": funnel.unique_articles,
            "db_upserted": funnel.db_upserted,
            "db_loaded": funnel.db_loaded,
            "topic_assignment": funnel.topic_assignment,
            "unassigned_count": funnel.unassigned_count,
            "assigned_content_ids": funnel.assigned_content_ids,
            "tickers_fetched_zero_articles": funnel.tickers_fetched_zero_articles,
            "channel_tags_on_corpus": funnel.channel_tags_on_corpus,
        }
    return metadata


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
    """Write screener ticker universe docs under ``source/ticker_universe/`` only."""
    root = outdir / "source"
    root.mkdir(parents=True, exist_ok=True)

    active_tickers = {
        sym.upper() for sym in (slices.ticker_lineage.get("by_ticker") or {})
    }
    _prune_stale_source_dirs(root, active_tickers)

    persist_ticker_lineage(
        slices.ticker_lineage, slices.ticker_universe_slices, outdir
    )
    logger.info(
        "source: ticker_universe (%d symbols) → %s",
        len(active_tickers),
        root / "ticker_universe",
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
