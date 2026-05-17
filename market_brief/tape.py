"""Verified-tape lookup.

Perplexity's `sonar-pro` cannot reliably retrieve ticker-level closing
prices via web search, so we pull them from the local OHLC table and
inject them into the probe prompts as ground truth. The probe is then
told to USE these verified moves and not fabricate others.

Two outputs:

* `get_tape_block(tickers, asof)` — formatted markdown block listing the
  verified prior-session moves for the given tickers, plus broad-market
  indices for context. This is what gets dropped into prompts.
* `get_topic_tape_block(topic, asof)` — convenience wrapper that picks
  the right ticker set for a Topic (theme tickers, or a default
  large-cap basket for sectors that don't carry their own ticker list).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from sqlalchemy import text

from daily_screener.utils.db import get_session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default ticker baskets per sector. Used when a topic doesn't carry its
# own ticker list (sectors don't, themes do). These are the names a
# trader scanning the sector expects to see prices for. Edit freely.
# ---------------------------------------------------------------------------

SECTOR_TICKERS: dict[str, list[str]] = {
    "Semiconductors": [
        "NVDA", "AMD", "INTC", "AVGO", "TSM", "ASML", "AMAT", "LRCX",
        "KLAC", "MU", "MRVL", "QCOM", "TXN", "ARM", "ADI",
    ],
    "AI Infrastructure": [
        "NVDA", "AMD", "AVGO", "MRVL", "ANET", "SMCI", "VRT", "ETN",
        "DELL", "HPE", "ORCL", "MSFT", "AMZN", "GOOGL", "META",
    ],
    "Software & SaaS": [
        "MSFT", "ORCL", "CRM", "NOW", "SNOW", "DDOG", "MDB", "ESTC",
        "PANW", "CRWD", "ZS", "TEAM", "ADBE", "INTU",
    ],
    "Internet & Platforms": [
        "GOOGL", "AMZN", "META", "AAPL", "MSFT", "NFLX", "TTD", "PINS",
        "SNAP", "RBLX", "SPOT", "UBER",
    ],
    "Communications & Networking": [
        "CSCO", "ANET", "JNPR", "CIEN", "LITE", "COHR", "AAOI", "AVGO",
        "MRVL", "ERIC", "NOK",
    ],
}

# Indices we always want for broad-tape context.
DEFAULT_INDICES: list[str] = ["SPY", "QQQ", "SOXX", "SMH", "IGV", "XLK", "DIA"]


@dataclass
class Quote:
    ticker: str
    date: str  # YYYY-MM-DD of the close used
    close: float
    prior_close: float
    pct_change: float  # (close - prior_close) / prior_close * 100

    def fmt(self) -> str:
        sign = "+" if self.pct_change >= 0 else ""
        return f"{self.ticker} {sign}{self.pct_change:.2f}% (close ${self.close:.2f})"


def _fetch_ohlc(session, ticker: str, asof_date: str) -> Quote | None:
    """Return the OHLC quote for `ticker` on/before `asof_date` plus the
    prior trading day's close, computed from local data."""
    rows = session.execute(
        text(
            "SELECT date::text, close FROM ohlc "
            "WHERE ticker = :t AND date <= :d "
            "ORDER BY date DESC LIMIT 2"
        ),
        {"t": ticker, "d": asof_date},
    ).fetchall()
    if len(rows) < 2:
        return None
    cur_date, cur_close = rows[0]
    _, prior_close = rows[1]
    if not prior_close:
        return None
    pct = (cur_close - prior_close) / prior_close * 100.0
    return Quote(ticker=ticker, date=cur_date, close=float(cur_close),
                 prior_close=float(prior_close), pct_change=pct)


def _fetch_index(session, symbol: str, asof_date: str) -> Quote | None:
    rows = session.execute(
        text(
            "SELECT date::text, close_price FROM index_prices "
            "WHERE symbol = :s AND date <= :d "
            "ORDER BY date DESC LIMIT 2"
        ),
        {"s": symbol, "d": asof_date},
    ).fetchall()
    if len(rows) < 2:
        return None
    cur_date, cur_close = rows[0]
    _, prior_close = rows[1]
    if not prior_close:
        return None
    pct = (cur_close - prior_close) / prior_close * 100.0
    return Quote(ticker=symbol, date=cur_date, close=float(cur_close),
                 prior_close=float(prior_close), pct_change=pct)


def _resolve_session_date(asof: str) -> str:
    """`asof` is the brief's run date. Return the date of the most recent
    OHLC entry on or before `asof` (handles weekends / holidays)."""
    session = get_session()
    try:
        row = session.execute(
            text("SELECT max(date)::text FROM ohlc WHERE date <= :d"),
            {"d": asof},
        ).fetchone()
        return row[0] if row and row[0] else asof
    finally:
        session.close()


def get_tape(
    tickers: Iterable[str],
    asof: str,
    include_indices: bool = True,
) -> tuple[str, list[Quote], list[Quote]]:
    """Pull verified moves for the given tickers + indices.

    Returns (session_date_used, ticker_quotes, index_quotes).
    """
    session_date = _resolve_session_date(asof)
    sess = get_session()
    try:
        tickers = list(dict.fromkeys(tickers))  # preserve order, dedupe
        ticker_quotes = [
            q for t in tickers if (q := _fetch_ohlc(sess, t, session_date))
        ]
        index_quotes = (
            [q for s in DEFAULT_INDICES if (q := _fetch_index(sess, s, session_date))]
            if include_indices
            else []
        )
        return session_date, ticker_quotes, index_quotes
    finally:
        sess.close()


def format_tape_block(
    session_date: str,
    ticker_quotes: list[Quote],
    index_quotes: list[Quote],
) -> str:
    """Format a markdown-ish block ready to drop into a prompt."""
    if not ticker_quotes and not index_quotes:
        return (
            "VERIFIED TAPE: (no local price data available for this session — "
            "do not fabricate ticker-level moves)"
        )

    try:
        pretty_date = datetime.strptime(session_date, "%Y-%m-%d").strftime(
            "%A %B %-d, %Y"
        )
    except ValueError:
        pretty_date = session_date

    lines = [
        f"VERIFIED TAPE for {pretty_date} (close-to-close % change vs prior session,",
        "from local OHLC data — these are AUTHORITATIVE; use these signs and",
        "magnitudes verbatim, do not contradict them):",
        "",
    ]
    if index_quotes:
        lines.append("Indices / ETFs:")
        for q in index_quotes:
            lines.append(f"  - {q.fmt()}")
        lines.append("")
    if ticker_quotes:
        lines.append("Tickers in this topic:")
        for q in ticker_quotes:
            lines.append(f"  - {q.fmt()}")
        lines.append("")
    lines.append(
        "Rules: when describing this session's tape, the SIGN and approximate "
        "magnitude of every ticker above is fixed by the data above. Use these "
        "numbers exactly. For tickers not listed above, you may describe news but "
        "do NOT invent a session % move."
    )
    return "\n".join(lines)


def get_topic_tape_block(topic: dict, asof: str) -> str:
    """Convenience: pick ticker set for a Topic and build the block."""
    tickers: list[str] = list(topic.get("tickers") or [])
    if not tickers:
        # Sectors fall back to the curated basket above.
        tickers = SECTOR_TICKERS.get(topic.get("name", ""), [])
    session_date, t_quotes, i_quotes = get_tape(tickers, asof)
    return format_tape_block(session_date, t_quotes, i_quotes)
