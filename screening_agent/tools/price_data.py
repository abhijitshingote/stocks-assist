"""Price-action enrichment for the technical agent.

We don't expect the LLM to do math on raw OHLC. Instead we precompute a
compact feature set per ticker — EMAs, MAs, ATR, distance-to-52w-high, a
'contraction score' for tightening price ranges, etc — and hand that to the
LLM along with a small recent-bars table so it can spot flags / bases / gap
behaviour visually-in-text.

Everything here is pandas-based and read-only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import desc

from . import db


@dataclass
class TickerFeatures:
    ticker: str
    last_close: float
    last_volume: int
    ema_10: float | None = None
    ema_20: float | None = None
    sma_50: float | None = None
    sma_200: float | None = None
    atr20_pct: float | None = None
    vol_vs_10d_avg: float | None = None
    dist_from_52w_high_pct: float | None = None
    dist_from_52w_low_pct: float | None = None
    contraction_score: float | None = None
    gap_count_30d: int = 0
    volspike_count_30d: int = 0
    recent_bars: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}


def _load_ohlc(session, ticker: str, lookback_days: int) -> pd.DataFrame:
    """Return a DataFrame indexed by date for the last `lookback_days+260` bars.

    We pull a long tail so 200-day SMA and 52w high/low are accurate.
    """
    rows = (
        session.query(db.OHLC)
        .filter(db.OHLC.ticker == ticker)
        .order_by(desc(db.OHLC.date))
        .limit(max(lookback_days, 260) + 30)
        .all()
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([
        {
            "date": r.date,
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.volume,
        }
        for r in rows
    ]).sort_values("date").reset_index(drop=True)
    return df


def _contraction_score(df: pd.DataFrame, lookback: int = 30) -> float | None:
    """Crude tightness metric.

    Compares the average daily true-range of the last `lookback//3` bars to the
    earliest `lookback//3` bars in a `lookback` window. <1.0 means contracting.
    """
    if len(df) < lookback:
        return None
    window = df.tail(lookback).copy()
    high_low = window["high"] - window["low"]
    third = lookback // 3
    early = high_low.iloc[:third].mean()
    late = high_low.iloc[-third:].mean()
    if early <= 0:
        return None
    return round(float(late / early), 3)


def compute_features(
    ticker: str,
    *,
    window_days: int = 90,
    session=None,
) -> TickerFeatures | None:
    """Compute the technical feature pack for a single ticker."""
    if session is None:
        with db.session_scope() as s:
            return compute_features(ticker, window_days=window_days, session=s)

    df = _load_ohlc(session, ticker, window_days)
    if df.empty:
        return None

    df["ema_10"] = df["close"].ewm(span=10, adjust=False).mean()
    df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["sma_50"] = df["close"].rolling(50, min_periods=10).mean()
    df["sma_200"] = df["close"].rolling(200, min_periods=50).mean()
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    df["atr20"] = tr.rolling(20, min_periods=5).mean()

    last = df.iloc[-1]
    last_close = float(last["close"])
    last_vol = int(last["volume"]) if pd.notna(last["volume"]) else 0

    high_52w = float(df["high"].tail(252).max())
    low_52w = float(df["low"].tail(252).min())

    avg_vol_10d = float(df["volume"].tail(10).mean())
    vol_vs_10d = (last_vol / avg_vol_10d) if avg_vol_10d else None

    # Gaps: prior close to today's open >= 3% (up) in last 30 sessions
    gaps_30d = 0
    spikes_30d = 0
    recent = df.tail(30).reset_index(drop=True)
    for i in range(1, len(recent)):
        prev = recent.iloc[i - 1]
        cur = recent.iloc[i]
        if prev["close"] > 0:
            gap_pct = (cur["open"] - prev["close"]) / prev["close"]
            if gap_pct >= 0.03:
                gaps_30d += 1
        avg_v = recent["volume"].iloc[max(0, i - 10):i].mean()
        if avg_v and cur["volume"] >= 2.0 * avg_v:
            spikes_30d += 1

    feat = TickerFeatures(
        ticker=ticker,
        last_close=last_close,
        last_volume=last_vol,
        ema_10=round(float(last["ema_10"]), 4) if pd.notna(last["ema_10"]) else None,
        ema_20=round(float(last["ema_20"]), 4) if pd.notna(last["ema_20"]) else None,
        sma_50=round(float(last["sma_50"]), 4) if pd.notna(last["sma_50"]) else None,
        sma_200=round(float(last["sma_200"]), 4) if pd.notna(last["sma_200"]) else None,
        atr20_pct=(round(float(last["atr20"] / last_close * 100), 2)
                    if pd.notna(last["atr20"]) and last_close else None),
        vol_vs_10d_avg=round(vol_vs_10d, 2) if vol_vs_10d else None,
        dist_from_52w_high_pct=round((last_close / high_52w - 1) * 100, 2) if high_52w else None,
        dist_from_52w_low_pct=round((last_close / low_52w - 1) * 100, 2) if low_52w else None,
        contraction_score=_contraction_score(df),
        gap_count_30d=gaps_30d,
        volspike_count_30d=spikes_30d,
    )

    # Hand the LLM the last `window_days` bars in compressed form.
    bars = df.tail(min(window_days, len(df)))
    feat.recent_bars = [
        {
            "d": b.date.isoformat() if isinstance(b.date, date) else str(b.date),
            "o": round(float(b.open), 2),
            "h": round(float(b.high), 2),
            "l": round(float(b.low), 2),
            "c": round(float(b.close), 2),
            "v": int(b.volume) if pd.notna(b.volume) else 0,
        }
        for b in bars.itertuples(index=False)
    ]
    return feat


def compute_features_bulk(tickers: list[str], *, window_days: int = 90) -> dict[str, TickerFeatures]:
    out: dict[str, TickerFeatures] = {}
    with db.session_scope() as s:
        for t in tickers:
            f = compute_features(t, window_days=window_days, session=s)
            if f is not None:
                out[t] = f
    return out
