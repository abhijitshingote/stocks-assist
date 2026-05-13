"""Stage 1: Universe Gathering.

Replicates the universe of the existing screens (Top Performance + VolSpike +
Gappers) by querying the DB directly (no HTTP). Tags every ticker with the set
of source screens it appeared in, so later stages can use multi-screen
membership as a momentum signal.

Output: outputs/<date>/00_universe.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from sqlalchemy import asc, desc, func, or_

from daily_screener import config
from daily_screener.utils import io
from daily_screener.utils.db import get_session

logger = logging.getLogger(__name__)


def _apply_liquidity(query, metrics_model):
    return (
        query.filter(metrics_model.avg_vol_10d >= config.LIQUIDITY_MIN_AVG_VOL_10D)
        .filter(metrics_model.dollar_volume >= config.LIQUIDITY_MIN_DOLLAR_VOLUME)
        .filter(metrics_model.current_price >= config.LIQUIDITY_MIN_PRICE)
    )


def _row_to_dict(row, source: str) -> dict[str, Any]:
    out = {
        "ticker": row.ticker,
        "company_name": getattr(row, "company_name", None),
        "sector": getattr(row, "sector", None),
        "industry": getattr(row, "industry", None),
        "country": getattr(row, "country", None),
        "market_cap": getattr(row, "market_cap", None),
        "current_price": _round(getattr(row, "current_price", None)),
        "dollar_volume": _round(getattr(row, "dollar_volume", None)),
        "avg_vol_10d": _round(getattr(row, "avg_vol_10d", None)),
        "vol_vs_10d_avg": _round(getattr(row, "vol_vs_10d_avg", None)),
        "dr_1": _round(getattr(row, "dr_1", None)),
        "dr_5": _round(getattr(row, "dr_5", None)),
        "dr_20": _round(getattr(row, "dr_20", None)),
        "dr_60": _round(getattr(row, "dr_60", None)),
        "dr_120": _round(getattr(row, "dr_120", None)),
        "atr20": _round(getattr(row, "atr20", None)),
        "rsi": getattr(row, "rsi", None),
        "rsi_mktcap": getattr(row, "rsi_mktcap", None),
        "ipo_date": _date_str(getattr(row, "ipo_date", None)),
        "last_event_date": _date_str(getattr(row, "last_event_date", None)),
        "last_event_type": getattr(row, "last_event_type", None),
        "last_event_magnitude": _float(getattr(row, "last_event_magnitude", None)),
        "last_event_return": _float(getattr(row, "last_event_return", None)),
        "sources": {source},
    }
    return out


def _round(v):
    if v is None:
        return None
    try:
        return round(float(v), 4)
    except (TypeError, ValueError):
        return None


def _float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _date_str(v):
    if v is None:
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    return str(v)


def _query_top_performance(session, source: str, return_col: str, limit: int):
    from models import StockMetrics, StockVolspikeGapper

    q = (
        session.query(
            StockMetrics.ticker,
            StockMetrics.company_name,
            StockMetrics.country,
            StockMetrics.sector,
            StockMetrics.industry,
            StockMetrics.ipo_date,
            StockMetrics.market_cap,
            StockMetrics.current_price,
            StockMetrics.dollar_volume,
            StockMetrics.avg_vol_10d,
            StockMetrics.vol_vs_10d_avg,
            StockMetrics.dr_1,
            StockMetrics.dr_5,
            StockMetrics.dr_20,
            StockMetrics.dr_60,
            StockMetrics.dr_120,
            StockMetrics.atr20,
            StockMetrics.rsi,
            StockMetrics.rsi_mktcap,
            StockVolspikeGapper.last_event_date,
            StockVolspikeGapper.last_event_type,
            StockVolspikeGapper.last_event_magnitude,
            StockVolspikeGapper.last_event_return,
        )
        .outerjoin(StockVolspikeGapper, StockVolspikeGapper.ticker == StockMetrics.ticker)
        .filter(getattr(StockMetrics, return_col).isnot(None))
        .filter(StockMetrics.market_cap.isnot(None))
    )
    q = _apply_liquidity(q, StockMetrics)
    q = q.order_by(desc(getattr(StockMetrics, return_col))).limit(limit)
    return [_row_to_dict(r, source) for r in q.all()]


def _query_volspike_gapper(session, source: str, date_str: str):
    from datetime import datetime, timedelta

    from models import StockMetrics, StockVolspikeGapper

    run_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    cutoff = run_date - timedelta(days=config.VOLSPIKE_GAPPER_WINDOW_DAYS)

    q = (
        session.query(
            StockMetrics.ticker,
            StockMetrics.company_name,
            StockMetrics.country,
            StockMetrics.sector,
            StockMetrics.industry,
            StockMetrics.ipo_date,
            StockMetrics.market_cap,
            StockMetrics.current_price,
            StockMetrics.dollar_volume,
            StockMetrics.avg_vol_10d,
            StockMetrics.vol_vs_10d_avg,
            StockMetrics.dr_1,
            StockMetrics.dr_5,
            StockMetrics.dr_20,
            StockMetrics.dr_60,
            StockMetrics.dr_120,
            StockMetrics.atr20,
            StockMetrics.rsi,
            StockMetrics.rsi_mktcap,
            StockVolspikeGapper.last_event_date,
            StockVolspikeGapper.last_event_type,
            StockVolspikeGapper.last_event_magnitude,
            StockVolspikeGapper.last_event_return,
        )
        .join(StockVolspikeGapper, StockVolspikeGapper.ticker == StockMetrics.ticker)
        .filter(
            or_(
                func.coalesce(StockVolspikeGapper.spike_day_count, 0) > 0,
                func.coalesce(StockVolspikeGapper.gapper_day_count, 0) > 0,
            )
        )
        .filter(StockVolspikeGapper.last_event_date.isnot(None))
        .filter(StockVolspikeGapper.last_event_date >= cutoff)
        .filter(StockMetrics.market_cap.isnot(None))
    )
    q = _apply_liquidity(q, StockMetrics)
    q = q.order_by(desc(StockVolspikeGapper.last_event_date))
    return [_row_to_dict(r, source) for r in q.all()]


def _load_dislikes() -> dict[str, str]:
    """Return {TICKER: notes} for explicit thumbs-down entries.

    The dislikes file is the ONLY source of vetoes. Watchlist `stars: 0` is
    treated as "not yet rated" (neutral) - the user can have a ticker on their
    watchlist with no rating and that should not eliminate them from screens.
    """
    if not config.EXCLUDE_DISLIKED_TICKERS:
        return {}
    if not config.ABI_DISLIKES_FILE.exists():
        return {}
    try:
        with config.ABI_DISLIKES_FILE.open("r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.warning("dislikes file is invalid JSON: %s", e)
        return {}

    out: dict[str, str] = {}
    for ticker, entry in (data or {}).items():
        if isinstance(entry, dict):
            out[ticker.upper()] = entry.get("notes", "") or ""
        else:
            out[ticker.upper()] = ""
    return out


def run(date_str: str | None = None, *, max_tickers: int | None = None) -> dict[str, Any]:
    """Build the universe and write 00_universe.json. Returns the in-memory dict."""
    date_str = date_str or io.today_str()
    logger.info("[s1] gathering universe for %s", date_str)

    session = get_session()
    try:
        sources_data: list[list[dict[str, Any]]] = []
        sources_data.append(_query_top_performance(session, "top_1d", "dr_1", config.TOP_PERFORMANCE_PER_BUCKET))
        sources_data.append(_query_top_performance(session, "top_5d", "dr_5", config.TOP_PERFORMANCE_PER_BUCKET))
        sources_data.append(_query_top_performance(session, "top_20d", "dr_20", config.TOP_PERFORMANCE_PER_BUCKET))
        sources_data.append(_query_volspike_gapper(session, "volspike_gapper", date_str))
    finally:
        session.close()

    merged: dict[str, dict[str, Any]] = {}
    source_counts: dict[str, int] = {}
    for rows in sources_data:
        for r in rows:
            src = next(iter(r["sources"]))
            source_counts[src] = source_counts.get(src, 0) + 1
            ticker = r["ticker"]
            if ticker in merged:
                merged[ticker]["sources"].add(src)
            else:
                merged[ticker] = r

    unique_after_dedup = len(merged)
    raw_total = sum(source_counts.values())

    dislikes = _load_dislikes()
    drops: list[dict[str, Any]] = []

    for ticker in list(merged.keys()):
        row = merged[ticker]
        if ticker in dislikes:
            drop = {"ticker": ticker, "drop_reason": "user_dislike"}
            if dislikes[ticker]:
                drop["dislike_notes"] = dislikes[ticker]
            drops.append(drop)
            merged.pop(ticker)
            continue
        if row.get("industry") in config.EXCLUDED_INDUSTRIES:
            drops.append({"ticker": ticker, "drop_reason": f"excluded_industry:{row['industry']}"})
            merged.pop(ticker)
            continue

    universe = []
    for row in merged.values():
        row["sources"] = sorted(row["sources"])
        universe.append(row)

    universe.sort(key=lambda r: (-(r.get("dr_5") or 0)))

    pre_truncation_count = len(universe)
    if max_tickers is not None:
        universe = universe[:max_tickers]
    truncated_count = pre_truncation_count - len(universe)

    artifact = {
        "date": date_str,
        "source_counts": source_counts,
        "raw_total": raw_total,
        "unique_after_dedup": unique_after_dedup,
        "survivors_after_filters": pre_truncation_count,
        "max_tickers_cap": max_tickers,
        "truncated_count": truncated_count,
        "total_universe": len(universe),
        "disliked_count": len([d for d in drops if d["drop_reason"] == "user_dislike"]),
        "industry_dropped_count": len([d for d in drops if d["drop_reason"].startswith("excluded_industry")]),
        "drops": drops,
        "tickers": universe,
    }

    io.write_json(io.artifact_path(date_str, "00_universe.json"), artifact)
    logger.info(
        "[s1] universe=%d (raw=%d, unique=%d, dropped %d disliked, %d industry-excluded, %d truncated by max_tickers=%s)",
        len(universe),
        raw_total,
        unique_after_dedup,
        artifact["disliked_count"],
        artifact["industry_dropped_count"],
        truncated_count,
        max_tickers,
    )
    return artifact


def main():  # standalone CLI: python -m daily_screener.stages.s1_universe
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    parser.add_argument("--max-tickers", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run(args.date, max_tickers=args.max_tickers)


if __name__ == "__main__":
    main()
