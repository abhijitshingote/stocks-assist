"""Stage 2: Momentum Scoring.

Reads 00_universe.json. For each ticker, computes a 0-100 composite momentum
score from a small set of transparent factors (no LLM). Survivors of the
threshold proceed; everyone else gets a drop_reason recorded.

Output: outputs/<date>/01_momentum.json
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from daily_screener import config
from daily_screener.utils import io

logger = logging.getLogger(__name__)


def _safe(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def _score_multi_screen(sources: list[str]) -> tuple[float, str]:
    """20 pts for 3+ screens, 12 for 2, 0 for 1."""
    n = len(sources)
    if n >= 3:
        return config.MOMENTUM_FACTOR_WEIGHTS["multi_screen"], f"{n} screens"
    if n == 2:
        return 0.6 * config.MOMENTUM_FACTOR_WEIGHTS["multi_screen"], "2 screens"
    return 0.0, "1 screen"


def _score_recent_event(last_event_date_str, run_date_str) -> tuple[float, str]:
    """20 pts if vol-spike/gap event within RECENT_EVENT_WINDOW_DAYS calendar days."""
    last_date = _parse_date(last_event_date_str)
    if not last_date:
        return 0.0, "no event"
    run_date = _parse_date(run_date_str) or datetime.today().date()
    age = (run_date - last_date).days
    if age <= config.RECENT_EVENT_WINDOW_DAYS:
        return config.MOMENTUM_FACTOR_WEIGHTS["recent_event"], f"event {age}d ago"
    if age <= 2 * config.RECENT_EVENT_WINDOW_DAYS:
        return 0.5 * config.MOMENTUM_FACTOR_WEIGHTS["recent_event"], f"event {age}d ago"
    return 0.0, f"event stale ({age}d)"


def _score_dr5_atr(dr_5, atr20) -> tuple[float, str]:
    """ATR-normalized 5-day return: dr_5 / (atr20 * sqrt(5)) capped, scaled."""
    dr5 = _safe(dr_5)
    atr = _safe(atr20)
    if atr <= 0:
        return 0.0, "no atr"
    norm = dr5 / (atr * (5 ** 0.5))
    # Normalize: norm >= 3 = full credit, 0 = none, linear in between.
    scaled = max(0.0, min(1.0, norm / 3.0))
    pts = scaled * config.MOMENTUM_FACTOR_WEIGHTS["dr5_atr_norm"]
    return pts, f"dr5={dr5:.1f}% atr={atr:.1f}% norm={norm:.2f}"


def _score_trend_alignment(dr_20, dr_60) -> tuple[float, str]:
    """Full credit if both >0; partial if one positive but small."""
    d20 = _safe(dr_20)
    d60 = _safe(dr_60)
    if d20 > 0 and d60 > 0:
        pts = config.MOMENTUM_FACTOR_WEIGHTS["trend_alignment"]
        return pts, f"both up (20d={d20:.1f}, 60d={d60:.1f})"
    if d20 > 0 or d60 > 0:
        return 0.4 * config.MOMENTUM_FACTOR_WEIGHTS["trend_alignment"], (
            f"partial (20d={d20:.1f}, 60d={d60:.1f})"
        )
    return 0.0, f"both down (20d={d20:.1f}, 60d={d60:.1f})"


def _score_vol_vs_avg(vol_vs_10d_avg) -> tuple[float, str]:
    """Bonus for elevated relative volume."""
    v = _safe(vol_vs_10d_avg)
    # v is in same units as in DB (a multiplier, e.g. 2.5 = 250%, OR sometimes %).
    # Inspecting the DB: vol_vs_10d_avg is typically a ratio (e.g. 1.5 = 150%).
    if v >= 2.0:
        return config.MOMENTUM_FACTOR_WEIGHTS["vol_vs_avg"], f"vol {v:.2f}x"
    if v >= 1.3:
        return 0.6 * config.MOMENTUM_FACTOR_WEIGHTS["vol_vs_avg"], f"vol {v:.2f}x"
    return 0.0, f"vol {v:.2f}x"


def _score_rsi_mktcap(rsi_mktcap) -> tuple[float, str]:
    if rsi_mktcap is None:
        return 0.0, "no rsi"
    r = int(rsi_mktcap)
    if r >= 90:
        return config.MOMENTUM_FACTOR_WEIGHTS["rsi_mktcap"], f"rsi_mktcap={r}"
    if r >= 80:
        return 0.7 * config.MOMENTUM_FACTOR_WEIGHTS["rsi_mktcap"], f"rsi_mktcap={r}"
    if r >= 70:
        return 0.4 * config.MOMENTUM_FACTOR_WEIGHTS["rsi_mktcap"], f"rsi_mktcap={r}"
    return 0.0, f"rsi_mktcap={r}"


def score_ticker(row: dict[str, Any], run_date_str: str) -> dict[str, Any]:
    factors: dict[str, dict[str, Any]] = {}
    total = 0.0

    pts, why = _score_multi_screen(row.get("sources", []))
    factors["multi_screen"] = {"points": round(pts, 2), "reason": why}
    total += pts

    pts, why = _score_recent_event(row.get("last_event_date"), run_date_str)
    factors["recent_event"] = {"points": round(pts, 2), "reason": why}
    total += pts

    pts, why = _score_dr5_atr(row.get("dr_5"), row.get("atr20"))
    factors["dr5_atr_norm"] = {"points": round(pts, 2), "reason": why}
    total += pts

    pts, why = _score_trend_alignment(row.get("dr_20"), row.get("dr_60"))
    factors["trend_alignment"] = {"points": round(pts, 2), "reason": why}
    total += pts

    pts, why = _score_vol_vs_avg(row.get("vol_vs_10d_avg"))
    factors["vol_vs_avg"] = {"points": round(pts, 2), "reason": why}
    total += pts

    pts, why = _score_rsi_mktcap(row.get("rsi_mktcap"))
    factors["rsi_mktcap"] = {"points": round(pts, 2), "reason": why}
    total += pts

    return {
        "ticker": row["ticker"],
        "momentum_score": round(min(100.0, total), 2),
        "factors": factors,
    }


def run(date_str: str | None = None) -> dict[str, Any]:
    date_str = date_str or io.today_str()
    universe_path = io.artifact_path(date_str, "00_universe.json")
    if not universe_path.exists():
        raise FileNotFoundError(
            f"00_universe.json not found at {universe_path}. Run stage 1 first."
        )
    universe = io.read_json(universe_path)

    scored: list[dict[str, Any]] = []
    for row in universe["tickers"]:
        s = score_ticker(row, date_str)
        scored.append(s)

    by_ticker = {s["ticker"]: s for s in scored}

    survivors = []
    drops = []
    for row in universe["tickers"]:
        s = by_ticker[row["ticker"]]
        if s["momentum_score"] >= config.MOMENTUM_THRESHOLD:
            survivors.append(
                {
                    "ticker": row["ticker"],
                    "company_name": row.get("company_name"),
                    "sector": row.get("sector"),
                    "industry": row.get("industry"),
                    "market_cap": row.get("market_cap"),
                    "current_price": row.get("current_price"),
                    "sources": row.get("sources", []),
                    "dr_1": row.get("dr_1"),
                    "dr_5": row.get("dr_5"),
                    "dr_20": row.get("dr_20"),
                    "dr_60": row.get("dr_60"),
                    "atr20": row.get("atr20"),
                    "rsi_mktcap": row.get("rsi_mktcap"),
                    "vol_vs_10d_avg": row.get("vol_vs_10d_avg"),
                    "last_event_date": row.get("last_event_date"),
                    "last_event_type": row.get("last_event_type"),
                    "last_event_magnitude": row.get("last_event_magnitude"),
                    "momentum_score": s["momentum_score"],
                    "factors": s["factors"],
                }
            )
        else:
            drops.append(
                {
                    "ticker": row["ticker"],
                    "company_name": row.get("company_name"),
                    "sources": row.get("sources", []),
                    "momentum_score": s["momentum_score"],
                    "factors": s["factors"],
                    "drop_reason": (
                        f"momentum_score {s['momentum_score']:.1f} < threshold "
                        f"{config.MOMENTUM_THRESHOLD}"
                    ),
                }
            )

    survivors.sort(key=lambda r: -r["momentum_score"])

    # Hard cap on survivors (cost guardrail for LLM stages).
    capped_drops: list[dict[str, Any]] = []
    if len(survivors) > config.MAX_LLM_GRADE:
        overflow = survivors[config.MAX_LLM_GRADE :]
        for o in overflow:
            capped_drops.append(
                {
                    "ticker": o["ticker"],
                    "company_name": o.get("company_name"),
                    "sources": o.get("sources", []),
                    "momentum_score": o["momentum_score"],
                    "factors": o["factors"],
                    "drop_reason": (
                        f"capped: only top {config.MAX_LLM_GRADE} go to LLM stages"
                    ),
                }
            )
        survivors = survivors[: config.MAX_LLM_GRADE]

    artifact = {
        "date": date_str,
        "momentum_threshold": config.MOMENTUM_THRESHOLD,
        "max_llm_grade": config.MAX_LLM_GRADE,
        "factor_weights": config.MOMENTUM_FACTOR_WEIGHTS,
        "survivor_count": len(survivors),
        "drop_count": len(drops) + len(capped_drops),
        "survivors": survivors,
        "drops": drops + capped_drops,
    }

    io.write_json(io.artifact_path(date_str, "01_momentum.json"), artifact)
    logger.info(
        "[s2] survivors=%d (dropped %d below threshold, %d above cap)",
        len(survivors),
        len(drops),
        len(capped_drops),
    )
    return artifact


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run(args.date)


if __name__ == "__main__":
    main()
