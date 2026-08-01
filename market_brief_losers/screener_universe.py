"""Build R1D losers universe from DB (bottom dr_1 by cap bucket)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import asc

from daily_screener.utils.db import get_session
from market_brief_losers import config

logger = logging.getLogger(__name__)

_CAP_LABELS = {
    "mega": "Mega cap",
    "large": "Large cap",
    "mid_small": "Mid & small cap",
}


@dataclass
class LosersSlice:
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
    cap_bucket: str,
    rank: int,
    row: Any,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "rank": rank,
        "slice_id": "r1d_losers",
        "cap_bucket": cap_bucket,
        "sort_key": "dr_1",
        "sort_value": _round(getattr(row, "dr_1", None)),
        "company_name": getattr(row, "company_name", None),
        "sector": getattr(row, "sector", None),
        "industry": getattr(row, "industry", None),
        "market_cap": getattr(row, "market_cap", None),
        "current_price": _round(getattr(row, "current_price", None)),
        "dr_1": _round(getattr(row, "dr_1", None)),
        "dr_5": _round(getattr(row, "dr_5", None)),
        "ti65": _round(getattr(row, "ti65", None)),
        "vol_vs_10d_avg": _round(getattr(row, "vol_vs_10d_avg", None)),
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


def _query_bottom_r1d(session, cap_bucket: str, limit: int) -> list[dict[str, Any]]:
    from models import StockMetrics

    q = session.query(StockMetrics).filter(
        StockMetrics.dr_1.isnot(None),
        StockMetrics.market_cap.isnot(None),
    )
    q = _apply_cap_filter(q, StockMetrics, cap_bucket)
    q = _apply_liquidity_metrics(q, StockMetrics)
    q = _apply_industry_exclude_metrics(q, StockMetrics)
    q = q.order_by(asc(StockMetrics.dr_1)).limit(limit)
    rows = q.all()
    out: list[dict[str, Any]] = []
    for i, row in enumerate(rows, start=1):
        out.append(
            _selection_row(
                ticker=row.ticker,
                cap_bucket=cap_bucket,
                rank=i,
                row=row,
            )
        )
    return out


def _build_lineage(
    asof: str,
    slices: list[LosersSlice],
) -> dict[str, Any]:
    assignments: dict[str, dict[str, Any]] = {}
    for sl in slices:
        for row in sl.selection:
            sym = row["ticker"].upper()
            assignments[sym] = {
                "section": f"r1d_losers/{sl.cap_bucket}",
                "cap_bucket": sl.cap_bucket,
                "label": sl.label,
                "rank": row["rank"],
                **{k: row[k] for k in row if k not in ("ticker", "rank", "cap_bucket")},
            }
    return {
        "asof": asof,
        "source": "stock_metrics (DB) — bottom dr_1",
        "cap_buckets": list(config.LOSERS_CAP_BUCKETS),
        "excludes_micro": True,
        "top_n_by_bucket": dict(config.LOSERS_TOP_N),
        "unique_tickers": len(assignments),
        "by_ticker": {
            sym: {
                "section": data["section"],
                "label": data["label"],
                "rank": data["rank"],
                "dr_1": data.get("dr_1"),
                "dr_5": data.get("dr_5"),
                "sector": data.get("sector"),
                "industry": data.get("industry"),
                "market_cap": data.get("market_cap"),
                "company_name": data.get("company_name"),
            }
            for sym, data in sorted(assignments.items())
        },
        "slices": [
            {
                "cap_bucket": sl.cap_bucket,
                "label": sl.label,
                "ticker_count": len(sl.selection),
                "tickers": [r["ticker"] for r in sl.selection],
            }
            for sl in slices
        ],
    }


def render_losers_markdown(slices: list[LosersSlice], lineage: dict[str, Any]) -> str:
    """Markdown table of R1D bottom names — passed to DeepSeek synthesis."""
    n = lineage.get("unique_tickers", 0)
    lines = [
        "# R1D Losers",
        "",
        f"**{n}** symbols — bottom `dr_1` by cap bucket (micro excluded). "
        f"mega top {config.LOSERS_TOP_N['mega']}, "
        f"large top {config.LOSERS_TOP_N['large']}, "
        f"mid+small top {config.LOSERS_TOP_N['mid_small']}.",
        "",
        "Bold = |1D| ≥10%. Benzinga pulls are ticker-specific only.",
        "",
    ]

    cap_order = {cap: i for i, cap in enumerate(config.LOSERS_CAP_BUCKETS)}
    for sl in sorted(slices, key=lambda s: cap_order.get(s.cap_bucket, 99)):
        if not sl.selection:
            continue
        lines.append(f"## {sl.label}")
        lines.append("")
        lines.append(
            "| # | Ticker | 1D | 5D | Sector | Industry | Vol× | Mkt cap | Price |"
        )
        lines.append(
            "|--:|--------|---:|---:|--------|----------|-----:|--------:|------:|"
        )
        for display_rank, row in enumerate(sl.selection, start=1):
            lines.append(
                "| {rank} | **{ticker}** | {d1} | {d5} | {sec} | {ind} | {vol} | {mcap} | {px} |".format(
                    rank=display_rank,
                    ticker=row["ticker"],
                    d1=_fmt_pct(row.get("dr_1")),
                    d5=_fmt_pct(row.get("dr_5"), bold_threshold=15.0),
                    sec=(row.get("sector") or "—")[:24],
                    ind=(row.get("industry") or "—")[:24],
                    vol=_fmt_vol_mult(row.get("vol_vs_10d_avg")),
                    mcap=_fmt_mcap(row.get("market_cap")),
                    px=_fmt_price(row.get("current_price")),
                )
            )
        lines.append("")

    lines.append("## Master list (A–Z)")
    lines.append("")
    lines.append("| Ticker | Cap | 1D | 5D | Sector |")
    lines.append("|--------|-----|---:|---:|--------|")
    for sym, data in sorted((lineage.get("by_ticker") or {}).items()):
        cap = data.get("label", "").split("·")[-1].strip() if data.get("label") else ""
        lines.append(
            "| **{t}** | {cap} | {d1} | {d5} | {sec} |".format(
                t=sym,
                cap=cap or data.get("section", "").split("/")[-1],
                d1=_fmt_pct(data.get("dr_1")),
                d5=_fmt_pct(data.get("dr_5"), bold_threshold=15.0),
                sec=(data.get("sector") or "—")[:28],
            )
        )
    lines.append("")
    return "\n".join(lines)


def build_losers_universe(
    asof: str,
) -> tuple[list[LosersSlice], dict[str, Any], list[str]]:
    """Return (slices, lineage, symbols for Benzinga fetch)."""
    session = get_session()
    try:
        slices: list[LosersSlice] = []
        for cap in config.LOSERS_CAP_BUCKETS:
            limit = config.LOSERS_TOP_N[cap]
            label = f"Bottom {limit} · 1D return · {_CAP_LABELS.get(cap, cap)}"
            slices.append(
                LosersSlice(
                    cap_bucket=cap,
                    label=label,
                    selection=_query_bottom_r1d(session, cap, limit),
                )
            )
    finally:
        session.close()

    lineage = _build_lineage(asof, slices)
    fetch_symbols = sorted((lineage.get("by_ticker") or {}).keys())
    logger.info(
        "losers universe: %d symbols (%s)",
        len(fetch_symbols),
        ", ".join(f"{cap}={config.LOSERS_TOP_N[cap]}" for cap in config.LOSERS_CAP_BUCKETS),
    )
    return slices, lineage, fetch_symbols
