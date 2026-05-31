"""Build market-brief ticker universe from DB screener tables (same logic as the UI)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import desc, func, or_

from daily_screener.utils.db import get_session
from market_brief import config

logger = logging.getLogger(__name__)

_SLICE_LABELS = {
    "r1d": "Top 10 · 1D return",
    "vol_spike_5d": "Vol spike / gapper · last 5D",
    "main_view_ti65": "Top 10 · Main View TI65",
}

_CAP_LABELS = {
    "mega": "Mega cap",
    "large": "Large cap",
    "mid_small": "Mid & small cap",
}


@dataclass
class UniverseSlice:
    """One cap-bucket screen (e.g. top 10 R1D mega-cap)."""

    slice_id: str
    cap_bucket: str
    label: str
    selection: list[dict[str, Any]] = field(default_factory=list)


def _round(v: Any, places: int = 2) -> float | None:
    if v is None:
        return None
    try:
        return round(float(v), places)
    except (TypeError, ValueError):
        return None


def _fmt_pct(v: float | None, *, bold_threshold: float = 10.0) -> str:
    if v is None:
        return "—"
    text = f"{v:+.1f}%"
    if abs(v) >= bold_threshold:
        return f"**{text}**"
    return text


def _fmt_num(v: float | None, *, bold_threshold: float | None = None) -> str:
    if v is None:
        return "—"
    text = f"{v:.1f}"
    if bold_threshold is not None and v >= bold_threshold:
        return f"**{text}**"
    return text


def _fmt_mcap(v: int | float | None) -> str:
    if v is None:
        return "—"
    v = float(v)
    if v >= 1_000_000_000_000:
        return f"${v / 1_000_000_000_000:.1f}T"
    if v >= 1_000_000_000:
        return f"${v / 1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"${v / 1_000_000:.0f}M"
    return f"${v:,.0f}"


def _fmt_price(v: float | None) -> str:
    if v is None:
        return "—"
    return f"${v:,.2f}" if v < 1000 else f"${v:,.0f}"


def _fmt_vol_mult(v: float | None) -> str:
    if v is None:
        return "—"
    text = f"{v:.1f}×"
    if v >= 2.0:
        return f"**{text}**"
    return text


def _selection_row(
    *,
    ticker: str,
    slice_id: str,
    cap_bucket: str,
    rank: int,
    row: Any,
    sort_key: str,
    sort_value: Any,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "rank": rank,
        "slice_id": slice_id,
        "cap_bucket": cap_bucket,
        "sort_key": sort_key,
        "sort_value": sort_value,
        "company_name": getattr(row, "company_name", None),
        "sector": getattr(row, "sector", None),
        "industry": getattr(row, "industry", None),
        "market_cap": getattr(row, "market_cap", None),
        "current_price": _round(getattr(row, "current_price", None)),
        "dr_1": _round(getattr(row, "dr_1", None)),
        "dr_5": _round(getattr(row, "dr_5", None)),
        "ti65": _round(getattr(row, "ti65", None)),
        "vol_vs_10d_avg": _round(getattr(row, "vol_vs_10d_avg", None)),
        "last_event_date": (
            row.last_event_date.strftime("%Y-%m-%d")
            if getattr(row, "last_event_date", None) is not None
            and hasattr(row.last_event_date, "strftime")
            else getattr(row, "last_event_date", None)
        ),
        "last_event_type": getattr(row, "last_event_type", None),
        "last_event_magnitude": _round(getattr(row, "last_event_magnitude", None), 4),
    }


def _apply_cap_filter(query, model, cap_bucket: str):
    cat = config.MARKET_CAP_CATEGORIES.get(cap_bucket)
    if not cat:
        return query
    query = query.filter(model.market_cap >= cat["min"])
    if cat["max"] is not None:
        query = query.filter(model.market_cap < cat["max"])
    return query


def _apply_liquidity_metrics(query, model):
    query = query.filter(model.avg_vol_10d >= config.LIQUIDITY_MIN_AVG_VOL_10D)
    query = query.filter(model.dollar_volume >= config.LIQUIDITY_MIN_DOLLAR_VOLUME)
    query = query.filter(model.current_price >= config.LIQUIDITY_MIN_PRICE)
    return query


def _apply_industry_exclude_metrics(query, model):
    if config.EXCLUDED_INDUSTRIES:
        query = query.filter(~model.industry.in_(list(config.EXCLUDED_INDUSTRIES)))
    return query


def _query_top_r1d(session, cap_bucket: str, limit: int) -> list[dict[str, Any]]:
    from models import StockMetrics

    q = session.query(StockMetrics).filter(
        StockMetrics.dr_1.isnot(None),
        StockMetrics.market_cap.isnot(None),
    )
    q = _apply_cap_filter(q, StockMetrics, cap_bucket)
    q = _apply_liquidity_metrics(q, StockMetrics)
    q = _apply_industry_exclude_metrics(q, StockMetrics)
    q = q.order_by(desc(StockMetrics.dr_1)).limit(limit)
    rows = q.all()
    out: list[dict[str, Any]] = []
    for i, row in enumerate(rows, start=1):
        out.append(
            _selection_row(
                ticker=row.ticker,
                slice_id="r1d",
                cap_bucket=cap_bucket,
                rank=i,
                row=row,
                sort_key="dr_1",
                sort_value=_round(row.dr_1),
            )
        )
    return out


def _query_vol_spike_5d(session, cap_bucket: str, asof: str, limit: int) -> list[dict[str, Any]]:
    from models import StockMetrics, StockVolspikeGapper

    run_date = datetime.strptime(asof, "%Y-%m-%d").date()
    cutoff = run_date - timedelta(days=config.VOLSPIKE_GAPPER_WINDOW_DAYS)

    q = (
        session.query(
            StockMetrics.ticker,
            StockMetrics.company_name,
            StockMetrics.sector,
            StockMetrics.industry,
            StockMetrics.market_cap,
            StockMetrics.current_price,
            StockMetrics.avg_vol_10d,
            StockMetrics.dollar_volume,
            StockMetrics.dr_1,
            StockMetrics.dr_5,
            StockMetrics.ti65,
            StockMetrics.vol_vs_10d_avg,
            StockVolspikeGapper.last_event_date,
            StockVolspikeGapper.last_event_type,
            StockVolspikeGapper.last_event_magnitude,
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
    q = _apply_cap_filter(q, StockMetrics, cap_bucket)
    q = _apply_liquidity_metrics(q, StockMetrics)
    q = _apply_industry_exclude_metrics(q, StockMetrics)
    q = q.order_by(desc(StockVolspikeGapper.last_event_date)).limit(limit)
    rows = q.all()
    out: list[dict[str, Any]] = []
    for i, row in enumerate(rows, start=1):
        out.append(
            _selection_row(
                ticker=row.ticker,
                slice_id="vol_spike_5d",
                cap_bucket=cap_bucket,
                rank=i,
                row=row,
                sort_key="last_event_date",
                sort_value=(
                    row.last_event_date.strftime("%Y-%m-%d")
                    if hasattr(row.last_event_date, "strftime")
                    else str(row.last_event_date)
                ),
            )
        )
    return out


def _query_main_view_ti65(session, cap_bucket: str, limit: int) -> list[dict[str, Any]]:
    from models import MainView

    q = session.query(MainView).filter(
        MainView.ti65.isnot(None),
        MainView.market_cap.isnot(None),
    )
    q = _apply_cap_filter(q, MainView, cap_bucket)
    q = _apply_liquidity_metrics(q, MainView)
    q = _apply_industry_exclude_metrics(q, MainView)
    q = q.order_by(MainView.ti65.desc().nullslast()).limit(limit)
    rows = q.all()
    out: list[dict[str, Any]] = []
    for i, row in enumerate(rows, start=1):
        out.append(
            _selection_row(
                ticker=row.ticker,
                slice_id="main_view_ti65",
                cap_bucket=cap_bucket,
                rank=i,
                row=row,
                sort_key="ti65",
                sort_value=_round(row.ti65),
            )
        )
    return out


def _build_raw_slices(session, asof: str) -> list[UniverseSlice]:
    slices: list[UniverseSlice] = []
    for cap in config.TICKER_UNIVERSE_CAP_BUCKETS:
        slices.append(
            UniverseSlice(
                slice_id="r1d",
                cap_bucket=cap,
                label=f"{_SLICE_LABELS['r1d']} · {_CAP_LABELS.get(cap, cap)}",
                selection=_query_top_r1d(session, cap, config.TICKER_UNIVERSE_TOP_N),
            )
        )
        slices.append(
            UniverseSlice(
                slice_id="vol_spike_5d",
                cap_bucket=cap,
                label=(
                    f"{_SLICE_LABELS['vol_spike_5d']} · "
                    f"{_CAP_LABELS.get(cap, cap)}"
                ),
                selection=_query_vol_spike_5d(
                    session, cap, asof, config.TICKER_UNIVERSE_TOP_N
                ),
            )
        )
        slices.append(
            UniverseSlice(
                slice_id="main_view_ti65",
                cap_bucket=cap,
                label=(
                    f"{_SLICE_LABELS['main_view_ti65']} · "
                    f"{_CAP_LABELS.get(cap, cap)}"
                ),
                selection=_query_main_view_ti65(
                    session, cap, config.TICKER_UNIVERSE_TOP_N
                ),
            )
        )
    return slices


def _slice_sort_key(sl: UniverseSlice) -> tuple[int, int]:
    type_rank = {
        sid: i
        for i, sid in enumerate(config.TICKER_UNIVERSE_SLICE_PRIORITY)
    }
    cap_rank = {cap: i for i, cap in enumerate(config.TICKER_UNIVERSE_CAP_BUCKETS)}
    return (type_rank.get(sl.slice_id, 99), cap_rank.get(sl.cap_bucket, 99))


def dedupe_slices_exclusive(
    raw_slices: list[UniverseSlice],
) -> tuple[list[UniverseSlice], dict[str, dict[str, Any]]]:
    """Each ticker appears in at most one section (first claim by slice priority)."""
    claimed: set[str] = set()
    deduped: list[UniverseSlice] = []
    assignments: dict[str, dict[str, Any]] = {}
    skipped: list[dict[str, Any]] = []

    for sl in sorted(raw_slices, key=_slice_sort_key):
        kept: list[dict[str, Any]] = []
        for row in sl.selection:
            sym = row["ticker"].upper()
            if sym in claimed:
                skipped.append(
                    {
                        "ticker": sym,
                        "would_be": f"{sl.slice_id}/{sl.cap_bucket}#{row['rank']}",
                        "assigned_to": assignments[sym]["section"],
                    }
                )
                continue
            claimed.add(sym)
            assignments[sym] = {
                "section": f"{sl.slice_id}/{sl.cap_bucket}",
                "slice_id": sl.slice_id,
                "cap_bucket": sl.cap_bucket,
                "rank": row["rank"],
                "label": sl.label,
                **{k: row[k] for k in row if k not in ("ticker", "rank", "slice_id", "cap_bucket")},
            }
            kept.append(row)
        deduped.append(
            UniverseSlice(
                slice_id=sl.slice_id,
                cap_bucket=sl.cap_bucket,
                label=sl.label,
                selection=kept,
            )
        )

    return deduped, {"assignments": assignments, "skipped_duplicates": skipped}


def _build_lineage(
    asof: str,
    exclusive_slices: list[UniverseSlice],
    assignment_data: dict[str, Any],
    *,
    raw_unique_count: int,
) -> dict[str, Any]:
    assignments = assignment_data["assignments"]
    return {
        "asof": asof,
        "source": "stock_metrics / stock_volspike_gapper / main_view (DB)",
        "cap_buckets": list(config.TICKER_UNIVERSE_CAP_BUCKETS),
        "excludes_micro": True,
        "top_n_per_slice": config.TICKER_UNIVERSE_TOP_N,
        "vol_spike_window_days": config.VOLSPIKE_GAPPER_WINDOW_DAYS,
        "dedupe": "exclusive — each ticker in one section (priority: r1d → vol_spike_5d → main_view_ti65)",
        "slice_count": len(exclusive_slices),
        "unique_tickers_assigned": len(assignments),
        "unique_tickers_raw_union": raw_unique_count,
        "skipped_duplicate_slots": len(assignment_data.get("skipped_duplicates") or []),
        "by_ticker": {
            sym: {
                "section": data["section"],
                "label": data["label"],
                "rank": data["rank"],
                "dr_1": data.get("dr_1"),
                "dr_5": data.get("dr_5"),
                "ti65": data.get("ti65"),
                "vol_vs_10d_avg": data.get("vol_vs_10d_avg"),
                "last_event_date": data.get("last_event_date"),
                "last_event_type": data.get("last_event_type"),
                "market_cap": data.get("market_cap"),
                "company_name": data.get("company_name"),
            }
            for sym, data in sorted(assignments.items())
        },
        "skipped_duplicates": assignment_data.get("skipped_duplicates") or [],
        "slices": [
            {
                "slice_id": sl.slice_id,
                "cap_bucket": sl.cap_bucket,
                "label": sl.label,
                "ticker_count": len(sl.selection),
                "tickers": [r["ticker"] for r in sl.selection],
            }
            for sl in exclusive_slices
        ],
    }


def render_overview_markdown(
    exclusive_slices: list[UniverseSlice],
    lineage: dict[str, Any],
) -> str:
    """Compact human-readable view — one ticker per section, stats highlighted."""
    n = lineage.get("unique_tickers_assigned", 0)
    raw_n = lineage.get("unique_tickers_raw_union", n)
    skipped = lineage.get("skipped_duplicate_slots", 0)

    lines = [
        "# Ticker universe",
        "",
        f"**{n}** symbols across **{lineage.get('slice_count', 0)}** sections "
        f"(no ticker repeated). "
        f"{raw_n} qualified before dedupe; **{skipped}** lower-priority slots dropped.",
        "",
        "Bold = standout (**1D** ≥10%, **TI65** ≥85, **vol** ≥2.0× avg). "
        "Benzinga per-ticker pulls use this list only (`source/ticker/<SYM>/`).",
        "",
    ]

    for sl in sorted(exclusive_slices, key=_slice_sort_key):
        if not sl.selection:
            continue
        lines.append(f"## {sl.label}")
        lines.append("")
        lines.append(
            "| # | Ticker | 1D | 5D | TI65 | Vol× | Event | Mkt cap | Price |"
        )
        lines.append(
            "|--:|--------|---:|---:|-----:|-----:|-------|--------:|------:|"
        )
        for display_rank, row in enumerate(sl.selection, start=1):
            ev = "—"
            if row.get("last_event_date"):
                typ = (row.get("last_event_type") or "?")[:4]
                ev = f"{typ} {row['last_event_date']}"
            lines.append(
                "| {rank} | **{ticker}** | {d1} | {d5} | {ti} | {vol} | {ev} | {mcap} | {px} |".format(
                    rank=display_rank,
                    ticker=row["ticker"],
                    d1=_fmt_pct(row.get("dr_1")),
                    d5=_fmt_pct(row.get("dr_5"), bold_threshold=15.0),
                    ti=_fmt_num(row.get("ti65"), bold_threshold=85.0),
                    vol=_fmt_vol_mult(row.get("vol_vs_10d_avg")),
                    ev=ev,
                    mcap=_fmt_mcap(row.get("market_cap")),
                    px=_fmt_price(row.get("current_price")),
                )
            )
        lines.append("")

    lines.append("## Master list (A–Z)")
    lines.append("")
    lines.append("| Ticker | Section | 1D | 5D | TI65 | Vol× |")
    lines.append("|--------|---------|---:|---:|-----:|-----:|")
    for sym, data in sorted((lineage.get("by_ticker") or {}).items()):
        lines.append(
            "| **{t}** | {sec} | {d1} | {d5} | {ti} | {vol} |".format(
                t=sym,
                sec=data.get("section", ""),
                d1=_fmt_pct(data.get("dr_1")),
                d5=_fmt_pct(data.get("dr_5"), bold_threshold=15.0),
                ti=_fmt_num(data.get("ti65"), bold_threshold=85.0),
                vol=_fmt_vol_mult(data.get("vol_vs_10d_avg")),
            )
        )
    lines.append("")
    return "\n".join(lines)


def build_screener_universe(
    asof: str,
) -> tuple[list[UniverseSlice], dict[str, Any], list[str]]:
    """Return (exclusive slices, lineage, all symbols for Benzinga fetch)."""
    session = get_session()
    try:
        raw_slices = _build_raw_slices(session, asof)
    finally:
        session.close()

    raw_symbols: set[str] = set()
    for sl in raw_slices:
        for row in sl.selection:
            raw_symbols.add(row["ticker"].upper())

    exclusive_slices, assignment_data = dedupe_slices_exclusive(raw_slices)
    lineage = _build_lineage(
        asof,
        exclusive_slices,
        assignment_data,
        raw_unique_count=len(raw_symbols),
    )
    # Benzinga per-ticker pulls: exclusive assigned symbols only (see overview.md).
    fetch_symbols = sorted(assignment_data["assignments"].keys())
    return exclusive_slices, lineage, fetch_symbols


def collect_ticker_symbols(asof: str) -> list[str]:
    _, _, fetch_symbols = build_screener_universe(asof)
    return fetch_symbols
