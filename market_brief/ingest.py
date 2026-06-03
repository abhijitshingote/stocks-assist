"""Benzinga news ingest for the daily market brief."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from daily_screener.utils.db import get_session
from market_brief import config, tape as tape_mod
from market_brief.funnel_log import FetchSlice, IngestFunnelData
from market_brief.ingest_window import (
    filter_published_window,
    get_news_window,
    get_ticker_news_window,
    published_range,
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
class FetchBucketStats:
    """One API pull: rows returned vs rows inside the ET ingest window."""

    api_rows: int = 0
    in_window: int = 0
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "api_rows": self.api_rows,
            "in_window": self.in_window,
            "error": self.error,
        }


@dataclass
class IngestFetchReport:
    general: FetchBucketStats
    channels: dict[str, FetchBucketStats]
    ticker: dict[str, FetchBucketStats]
    warnings: list[str]

    def ticker_summary(self) -> FetchBucketStats:
        total = FetchBucketStats()
        for st in self.ticker.values():
            total.api_rows += st.api_rows
            total.in_window += st.in_window
            if st.error and not total.error:
                total.error = f"{st.error} (first ticker error)"
        return total


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


def _fetch_ticker(
    ticker: str, window: NewsWindow
) -> tuple[str, list[dict], FetchBucketStats]:
    stats = FetchBucketStats()
    try:
        raw = bz.fetch_benzinga_from_api(
            ticker,
            limit=config.PER_TICKER_LIMIT,
            published_gte=window.start_utc,
            published_lte=window.end_utc,
        )
        stats.api_rows = len(raw)
        filtered = filter_published_window(raw, window)
        stats.in_window = len(filtered)
        return ticker, filtered, stats
    except Exception as e:  # noqa: BLE001
        stats.error = str(e)
        logger.error("benzinga fetch failed for %s: %s", ticker, e)
        return ticker, [], stats


def _warn_if_empty_bucket(label: str, stats: FetchBucketStats, warnings: list[str]) -> None:
    if stats.error:
        msg = f"{label}: API error — {stats.error}"
        warnings.append(msg)
        logger.error(msg)
        return
    if stats.api_rows > 0 and stats.in_window == 0:
        msg = (
            f"{label}: API returned {stats.api_rows} rows but 0 in ingest window "
            f"(check published.lte / asof window)"
        )
        warnings.append(msg)
        logger.warning(msg)
    elif stats.api_rows == 0 and stats.in_window == 0:
        msg = f"{label}: API returned 0 rows"
        warnings.append(msg)
        logger.warning(msg)


def _fetch_general(
    window: NewsWindow,
    funnel: IngestFunnelData | None = None,
    *,
    warnings: list[str] | None = None,
) -> tuple[list[dict], dict[str, list[dict]], FetchBucketStats, dict[str, FetchBucketStats]]:
    """Return (general rows, channel rows, general stats, channel stats)."""
    general_rows: list[dict] = []
    channel_rows: dict[str, list[dict]] = {}
    general_stats = FetchBucketStats()
    channel_stats: dict[str, FetchBucketStats] = {}
    seen_ids: set[int] = set()
    warn = warnings if warnings is not None else []

    try:
        raw = bz.fetch_benzinga_general(
            limit=config.GENERAL_NEWS_LIMIT,
            published_gte=window.start_utc,
            published_lte=window.end_utc,
        )
        filtered = filter_published_window(raw, window)
        general_stats.api_rows = len(raw)
        general_stats.in_window = len(filtered)
        general_rows = filtered
        if funnel is not None:
            funnel.general = FetchSlice(
                label="general (no channel)",
                api_rows=len(raw),
                after_filter=len(filtered),
                new_unique_ids=_count_new_ids(filtered, seen_ids),
            )
    except Exception as e:  # noqa: BLE001
        general_stats.error = str(e)
        if funnel is not None:
            funnel.general = FetchSlice(
                label="general (no channel)",
                error=str(e),
            )
        logger.error("general benzinga fetch failed: %s", e)
        raise

    _warn_if_empty_bucket("general", general_stats, warn)

    for channel, limit in config.GENERAL_CHANNEL_FETCHES:
        slug = _slugify(channel)
        st = FetchBucketStats()
        channel_stats[slug] = st
        slice_row = FetchSlice(label=f"channel:{channel}")
        try:
            raw = bz.fetch_benzinga_general(
                limit=limit,
                published_gte=window.start_utc,
                published_lte=window.end_utc,
                channels=channel,
            )
            filtered = filter_published_window(raw, window)
            channel_rows[channel] = filtered
            st.api_rows = len(raw)
            st.in_window = len(filtered)
            slice_row.api_rows = len(raw)
            slice_row.after_filter = len(filtered)
            slice_row.new_unique_ids = _count_new_ids(filtered, seen_ids)
        except Exception as e:  # noqa: BLE001
            st.error = str(e)
            slice_row.error = str(e)
            channel_rows[channel] = []
            logger.error("channel fetch failed for %s: %s", channel, e)
        if funnel is not None:
            funnel.channels.append(slice_row)
        _warn_if_empty_bucket(f"channel:{channel}", st, warn)
    return general_rows, channel_rows, general_stats, channel_stats


def ingest_pool_ids(slices: IngestSourceSlices) -> dict[str, Any]:
    """Per-bucket ``benzinga_id`` lists for Step 4 dedupe (no on-disk article JSON)."""
    return {
        "general": article_ids_from_rows(slices.general),
        "channels": {
            _slugify(channel): article_ids_from_rows(rows)
            for channel, rows in slices.channels.items()
        },
        "ticker": {
            sym: article_ids_from_rows(rows)
            for sym, rows in sorted(slices.ticker_articles.items())
        },
    }


def ingest_all(
    asof: str,
    topics: list[Topic] | None = None,
    *,
    funnel: IngestFunnelData | None = None,
) -> tuple[list[dict], IngestStats, IngestSourceSlices, IngestFetchReport]:
    """API pulls (general / channel / per-ticker) → dedupe → upsert DB."""
    topics = topics or load_topics()
    general_window = news_window_for_run(asof)
    ticker_window = get_ticker_news_window(asof)
    stats = IngestStats()
    fetch_warnings: list[str] = []
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
        logger.info(
            "purged %d benzinga rows older than %d days",
            stats.purged,
            config.ARTICLE_RETENTION_DAYS,
        )

        from market_brief.screener_universe import build_screener_universe

        universe_slices, ticker_lineage, universe = build_screener_universe(asof)
        stats.per_ticker_limit = config.PER_TICKER_LIMIT
        if funnel is not None:
            funnel.universe_size = len(universe)
            funnel.universe_sample = universe[:25]

        all_raw: list[dict] = []

        general_rows, channel_rows, general_stats, channel_stats = _fetch_general(
            general_window, funnel=funnel, warnings=fetch_warnings
        )
        stats.general_api_calls = 1 + len(config.GENERAL_CHANNEL_FETCHES)
        stats.general_channel_rows = len(general_rows) + sum(
            len(rows) for rows in channel_rows.values()
        )
        all_raw.extend(general_rows)
        for rows in channel_rows.values():
            all_raw.extend(rows)

        ticker_articles: dict[str, list[dict]] = {}
        ticker_counts: dict[str, int] = {}
        ticker_stats: dict[str, FetchBucketStats] = {}
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
                    _, rows, t_stats = fut.result()
                    stats.ticker_api_calls += 1
                    ticker_stats[ticker] = t_stats
                    ticker_counts[ticker] = len(rows)
                    ticker_articles[ticker] = rows
                    all_raw.extend(rows)
                    if t_stats.error:
                        if funnel is not None:
                            funnel.ticker_failed.append(ticker)
                except Exception as e:  # noqa: BLE001
                    logger.error("benzinga fetch failed for %s: %s", ticker, e)
                    ticker_articles[ticker] = []
                    ticker_stats[ticker] = FetchBucketStats(error=str(e))
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
        stats.duplicate_rows_removed = (
            stats.raw_articles - stats.unique_articles - dropped_no_id
        )
        if funnel is not None:
            funnel.raw_rows = stats.raw_articles
            funnel.dropped_no_benzinga_id = dropped_no_id
            funnel.unique_articles = stats.unique_articles

        stats.db_upserted = bz.upsert_articles(session, unique)
        if funnel is not None:
            funnel.db_upserted = stats.db_upserted
            funnel.db_loaded = len(unique)
            funnel.channel_tags_on_corpus = _channel_tags_on_articles(unique)
            funnel.tickers_fetched_zero_articles = sorted(
                t for t in universe if ticker_counts.get(t, 0) == 0
            )
            for topic in topics:
                funnel.topic_assignment[topic.name] = len(
                    assign_articles_to_topic(unique, topic)
                )
            n_unassigned = len(assign_unassigned_articles(unique, topics))
            funnel.unassigned_count = n_unassigned
            funnel.macro_count = n_unassigned
            funnel.assigned_content_ids = len(
                assigned_content_topic_ids(unique, topics)
            )

        slices = IngestSourceSlices(
            general=general_rows,
            channels=channel_rows,
            ticker_universe_slices=universe_slices,
            ticker_articles=ticker_articles,
            ticker_lineage=ticker_lineage,
        )
        t_summary = FetchBucketStats()
        for sym, st in ticker_stats.items():
            t_summary.api_rows += st.api_rows
            t_summary.in_window += st.in_window
            if st.error:
                fetch_warnings.append(f"ticker:{sym}: API error — {st.error}")
        n_ticker_api_but_empty = sum(
            1
            for st in ticker_stats.values()
            if st.api_rows > 0 and st.in_window == 0 and not st.error
        )
        if n_ticker_api_but_empty:
            fetch_warnings.append(
                f"ticker: {n_ticker_api_but_empty} symbols had API rows but 0 in window"
            )
        fetch_report = IngestFetchReport(
            general=general_stats,
            channels=channel_stats,
            ticker=ticker_stats,
            warnings=fetch_warnings,
        )
        if fetch_warnings:
            logger.warning(
                "Ingest fetch warnings (%d): %s",
                len(fetch_warnings),
                "; ".join(fetch_warnings[:5])
                + (" …" if len(fetch_warnings) > 5 else ""),
            )
        return unique, stats, slices, fetch_report
    finally:
        session.close()


def prepare_run(
    asof: str,
    outdir: Path,
    topics: list[Topic] | None = None,
    *,
    funnel: IngestFunnelData | None = None,
) -> tuple[dict[str, Any], IngestStats, list[dict]]:
    """API ingest → DB upsert → ticker universe under ``source/`` + ``metadata.json`` fields."""
    topics = topics or load_topics()
    articles, stats, slices, fetch_report = ingest_all(asof, topics, funnel=funnel)
    persist_source_snapshots(slices, outdir, topics, asof=asof)
    metadata = build_ingest_metadata(
        asof,
        slices,
        ingest_stats=stats,
        deduped_articles=articles,
        fetch_report=fetch_report,
    )
    return metadata, stats, articles


def build_ingest_metadata(
    asof: str,
    slices: IngestSourceSlices,
    *,
    ingest_stats: IngestStats,
    deduped_articles: list[dict] | None = None,
    fetch_report: IngestFetchReport | None = None,
) -> dict:
    """Run metadata: windows, per-fetch counts, ingest pool ids for Step 4."""
    general_window = news_window_for_run(asof)
    ticker_window = get_ticker_news_window(asof)
    ticker_by_sym = {
        sym: len(rows) for sym, rows in sorted(slices.ticker_articles.items())
    }
    api_by_fetch: dict[str, Any]
    if fetch_report is not None:
        api_by_fetch = {
            "general": fetch_report.general.as_dict(),
            "channels": {
                slug: st.as_dict() for slug, st in sorted(fetch_report.channels.items())
            },
            "ticker": {
                sym: st.as_dict() for sym, st in sorted(fetch_report.ticker.items())
            },
            "ticker_totals": fetch_report.ticker_summary().as_dict(),
        }
    else:
        api_by_fetch = {
            "general": {"api_rows": None, "in_window": len(slices.general), "error": None},
            "channels": {
                _slugify(ch): {"api_rows": None, "in_window": len(rows), "error": None}
                for ch, rows in slices.channels.items()
            },
            "ticker": {
                sym: {"api_rows": None, "in_window": n, "error": None}
                for sym, n in ticker_by_sym.items()
            },
        }

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

    return {
        "asof": asof,
        "windows": {
            "general_window": general_window.label,
            "ticker_window": ticker_window.label,
            "anchor_session": general_window.anchor_session.isoformat(),
            "general_published_gte_utc": general_window.start_utc.isoformat(),
            "ticker_published_gte_utc": ticker_window.start_utc.isoformat(),
            "published_lte_utc": general_window.end_utc.isoformat(),
        },
        "counts": {
            "api_by_fetch": api_by_fetch,
            "api_total_rows": ingest_stats.raw_articles,
            "after_dedupe_unique": ingest_stats.unique_articles,
            "upserted": ingest_stats.db_upserted,
            "deduped_published": published_range(deduped_articles or []),
            "prompt": None,
        },
        "fetch_warnings": fetch_report.warnings if fetch_report else [],
        "ingest_pools": ingest_pool_ids(slices),
        "universe_symbols": universe_symbols,
        "ticker_universe_slices": slice_manifest,
        "ticker_universe_overview": "source/ticker_universe/overview.md",
    }


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

    active_tickers = {sym.upper() for sym in slices.ticker_articles}
    if not active_tickers:
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
