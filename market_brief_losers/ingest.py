"""Benzinga ticker-only ingest for the R1D losers brief."""

from __future__ import annotations

import concurrent.futures
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from daily_screener.utils.db import get_session
from market_brief.ingest_window import (
    filter_published_window,
    get_ticker_news_window,
    published_range,
)
from market_brief_losers import config
from market_brief_losers.screener_universe import (
    LosersSlice,
    build_losers_universe,
    render_losers_markdown,
)

import benzinga_news as bz  # noqa: E402

logger = logging.getLogger(__name__)


@dataclass
class IngestStats:
    ticker_api_calls: int = 0
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
class IngestSourceSlices:
    losers_slices: list[LosersSlice]
    ticker_articles: dict[str, list[dict]]
    ticker_lineage: dict


def _dedupe_by_id(articles: list[dict]) -> tuple[list[dict], int]:
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


def _fetch_ticker(ticker: str, window) -> tuple[str, list[dict], FetchBucketStats]:
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


def persist_losers_universe(
    slices: list[LosersSlice],
    lineage: dict,
    outdir: Path,
) -> None:
    root = outdir / "source" / "losers_universe"
    root.mkdir(parents=True, exist_ok=True)
    (root / "lineage.json").write_text(
        json.dumps(lineage, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (root / "overview.md").write_text(
        render_losers_markdown(slices, lineage),
        encoding="utf-8",
    )
    logger.info(
        "losers universe: %d symbols → %s",
        lineage.get("unique_tickers", 0),
        root / "overview.md",
    )


def ingest_all(
    asof: str,
) -> tuple[list[dict], IngestStats, IngestSourceSlices, list[str], dict[str, FetchBucketStats]]:
    """Ticker-only Benzinga pulls → dedupe → DB upsert."""
    ticker_window = get_ticker_news_window(asof)
    stats = IngestStats()
    fetch_warnings: list[str] = []

    session = get_session()
    try:
        from models import BenzingaArticle

        BenzingaArticle.__table__.create(session.get_bind(), checkfirst=True)
        stats.purged = bz.purge_articles_older_than(
            session, days=config.ARTICLE_RETENTION_DAYS
        )
        logger.info(
            "purged %d benzinga rows older than %d days",
            stats.purged,
            config.ARTICLE_RETENTION_DAYS,
        )

        losers_slices, ticker_lineage, universe = build_losers_universe(asof)
        stats.per_ticker_limit = config.PER_TICKER_LIMIT

        all_raw: list[dict] = []
        ticker_articles: dict[str, list[dict]] = {}
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
                    ticker_articles[ticker] = rows
                    all_raw.extend(rows)
                    if t_stats.error:
                        fetch_warnings.append(
                            f"ticker:{ticker}: API error — {t_stats.error}"
                        )
                except Exception as e:  # noqa: BLE001
                    logger.error("benzinga fetch failed for %s: %s", ticker, e)
                    ticker_articles[ticker] = []
                    ticker_stats[ticker] = FetchBucketStats(error=str(e))
                    fetch_warnings.append(f"ticker:{ticker}: {e}")

        stats.ticker_rows = sum(len(rows) for rows in ticker_articles.values())

        stats.raw_articles = len(all_raw)
        unique, dropped_no_id = _dedupe_by_id(all_raw)
        stats.unique_articles = len(unique)
        stats.dropped_no_benzinga_id = dropped_no_id
        stats.duplicate_rows_removed = (
            stats.raw_articles - stats.unique_articles - dropped_no_id
        )

        stats.db_upserted = bz.upsert_articles(session, unique)

        for sym, st in ticker_stats.items():
            if st.api_rows > 0 and st.in_window == 0 and not st.error:
                fetch_warnings.append(
                    f"ticker:{sym}: API returned {st.api_rows} rows but 0 in window"
                )

        slices = IngestSourceSlices(
            losers_slices=losers_slices,
            ticker_articles=ticker_articles,
            ticker_lineage=ticker_lineage,
        )
        if fetch_warnings:
            logger.warning(
                "Ingest fetch warnings (%d): %s",
                len(fetch_warnings),
                "; ".join(fetch_warnings[:5])
                + (" …" if len(fetch_warnings) > 5 else ""),
            )
        return unique, stats, slices, fetch_warnings, ticker_stats
    finally:
        session.close()


def ingest_pool_ids(slices: IngestSourceSlices) -> dict[str, Any]:
    return {
        "ticker": {
            sym: article_ids_from_rows(rows)
            for sym, rows in sorted(slices.ticker_articles.items())
        },
    }


def build_ingest_metadata(
    asof: str,
    slices: IngestSourceSlices,
    *,
    ingest_stats: IngestStats,
    ticker_stats: dict[str, FetchBucketStats] | None = None,
    deduped_articles: list[dict] | None = None,
    fetch_warnings: list[str] | None = None,
) -> dict:
    ticker_window = get_ticker_news_window(asof)
    ticker_by_sym = {
        sym: len(rows) for sym, rows in sorted(slices.ticker_articles.items())
    }
    ticker_stats = ticker_stats or {}
    t_summary = FetchBucketStats()
    for st in ticker_stats.values():
        t_summary.api_rows += st.api_rows
        t_summary.in_window += st.in_window
    slice_manifest = [
        {
            "cap_bucket": sl.cap_bucket,
            "label": sl.label,
            "selection_count": len(sl.selection),
            "tickers": [r["ticker"] for r in sl.selection],
        }
        for sl in slices.losers_slices
    ]
    universe_symbols = sorted((slices.ticker_lineage.get("by_ticker") or {}).keys())

    return {
        "asof": asof,
        "pipeline": "market_brief_losers",
        "source_scope": "ticker_only",
        "windows": {
            "ticker_window": ticker_window.label,
            "anchor_session": ticker_window.anchor_session.isoformat(),
            "ticker_published_gte_utc": ticker_window.start_utc.isoformat(),
            "published_lte_utc": ticker_window.end_utc.isoformat(),
        },
        "counts": {
            "api_by_fetch": {
                "ticker": {
                    sym: st.as_dict() for sym, st in sorted(ticker_stats.items())
                },
                "ticker_totals": t_summary.as_dict(),
            },
            "api_total_rows": ingest_stats.raw_articles,
            "after_dedupe_unique": ingest_stats.unique_articles,
            "upserted": ingest_stats.db_upserted,
            "deduped_published": published_range(deduped_articles or []),
            "prompt": None,
        },
        "fetch_warnings": fetch_warnings or [],
        "ingest_pools": ingest_pool_ids(slices),
        "universe_symbols": universe_symbols,
        "losers_universe_slices": slice_manifest,
        "losers_universe_overview": "source/losers_universe/overview.md",
    }


def prepare_run(asof: str, outdir: Path) -> tuple[dict[str, Any], IngestStats, list[dict]]:
    """Ticker ingest → DB upsert → losers universe under ``source/`` + ``metadata.json``."""
    articles, stats, slices, fetch_warnings, ticker_stats = ingest_all(asof)
    outdir.mkdir(parents=True, exist_ok=True)
    persist_losers_universe(slices.losers_slices, slices.ticker_lineage, outdir)
    metadata = build_ingest_metadata(
        asof,
        slices,
        ingest_stats=stats,
        ticker_stats=ticker_stats,
        deduped_articles=articles,
        fetch_warnings=fetch_warnings,
    )
    return metadata, stats, articles
