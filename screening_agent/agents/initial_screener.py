"""Stage 1 — Initial Screener.

Pulls a wide net of momentum candidates from the database. No LLM here; this
is straight SQL/SQLAlchemy. We union several "buckets" of stocks and dedupe.

Buckets (all configurable in `config.INITIAL_SCREENER`):

* recent_volspike   — recent volume-spike events (within N days)
* recent_gappers    — recent gap-up events
* top_dr5 / top_dr20 — leaders by 5d / 20d return
* rsi_momentum      — top RSI percentile within market cap

Output schema:
{
  "candidates": [
    {"ticker": "CRDO", "company_name": "...", "sector": "...", "industry": "...",
     "market_cap": 12345, "price": 99.0, "buckets": ["recent_volspike", "top_dr20"],
     "metrics": {...full main_view row...}},
    ...
  ],
  "stats": {...}
}
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy import desc

from .. import config as cfg
from ..tools import db
from .base import Agent

logger = logging.getLogger(__name__)


class InitialScreener(Agent):
    name = "initial_screener"

    def __init__(self) -> None:
        self.cfg = cfg.INITIAL_SCREENER

    # ---------------------- bucket queries ----------------------

    def _base_query(self, session):
        q = session.query(db.MainView)
        if self.cfg["min_price"] is not None:
            q = q.filter(db.MainView.current_price >= self.cfg["min_price"])
        if self.cfg["min_dollar_volume"] is not None:
            q = q.filter(db.MainView.dollar_volume >= self.cfg["min_dollar_volume"])
        if self.cfg["min_avg_vol_10d"] is not None:
            q = q.filter(db.MainView.avg_vol_10d >= self.cfg["min_avg_vol_10d"])
        if self.cfg["min_market_cap"] is not None:
            q = q.filter(db.MainView.market_cap >= self.cfg["min_market_cap"])
        if self.cfg["max_market_cap"] is not None:
            q = q.filter(db.MainView.market_cap <= self.cfg["max_market_cap"])
        if self.cfg["exclude_industries"]:
            q = q.filter(~db.MainView.industry.in_(self.cfg["exclude_industries"]))
        if self.cfg["exclude_sectors"]:
            q = q.filter(~db.MainView.sector.in_(self.cfg["exclude_sectors"]))
        return q

    def _bucket_recent_volspike(self, session) -> list[str]:
        bcfg = self.cfg["buckets"]["recent_volspike"]
        cutoff = date.today() - timedelta(days=bcfg["lookback_days"])
        q = (self._base_query(session)
             .filter(db.MainView.last_event_type == "volume_spike")
             .filter(db.MainView.last_event_date >= cutoff)
             .order_by(desc(db.MainView.last_event_magnitude))
             .limit(bcfg["per_bucket"]))
        return [r.ticker for r in q.all()]

    def _bucket_recent_gappers(self, session) -> list[str]:
        bcfg = self.cfg["buckets"]["recent_gappers"]
        cutoff = date.today() - timedelta(days=bcfg["lookback_days"])
        q = (self._base_query(session)
             .filter(db.MainView.last_event_type == "gapper")
             .filter(db.MainView.last_event_date >= cutoff)
             .order_by(desc(db.MainView.last_event_magnitude))
             .limit(bcfg["per_bucket"]))
        return [r.ticker for r in q.all()]

    def _bucket_top_dr(self, session, days: int, cap: int) -> list[str]:
        col = {5: db.MainView.dr_5, 20: db.MainView.dr_20}[days]
        q = (self._base_query(session)
             .filter(col.isnot(None))
             .order_by(desc(col))
             .limit(cap))
        return [r.ticker for r in q.all()]

    def _bucket_rsi_momentum(self, session, cap: int) -> list[str]:
        q = (self._base_query(session)
             .filter(db.MainView.rsi.isnot(None))
             .filter(db.MainView.rsi >= 90)
             .order_by(desc(db.MainView.rsi), desc(db.MainView.dr_20))
             .limit(cap))
        return [r.ticker for r in q.all()]

    # ---------------------- main entry ----------------------

    def run(self, input_data: dict[str, Any] | None = None) -> dict[str, Any]:
        bucket_map: dict[str, list[str]] = {}
        with db.session_scope() as s:
            bucket_map["recent_volspike"] = self._bucket_recent_volspike(s)
            bucket_map["recent_gappers"]  = self._bucket_recent_gappers(s)
            bucket_map["top_dr5"]         = self._bucket_top_dr(
                s, 5, self.cfg["buckets"]["top_dr5"]["per_bucket"]
            )
            bucket_map["top_dr20"]        = self._bucket_top_dr(
                s, 20, self.cfg["buckets"]["top_dr20"]["per_bucket"]
            )
            bucket_map["rsi_momentum"]    = self._bucket_rsi_momentum(
                s, self.cfg["buckets"]["rsi_momentum"]["per_bucket"]
            )

            # Union + dedupe, remembering bucket membership.
            membership: dict[str, list[str]] = {}
            for bname, tickers in bucket_map.items():
                for t in tickers:
                    membership.setdefault(t, []).append(bname)

            tickers = list(membership.keys())
            if not tickers:
                return self.envelope({"candidates": [], "stats": {"buckets": {b: 0 for b in bucket_map}}})

            rows = (
                s.query(db.MainView)
                .filter(db.MainView.ticker.in_(tickers))
                .all()
            )

            def _score(r) -> float:
                """Lightweight pre-rank: prefer multi-bucket + recent event + 20d strength."""
                buckets = membership.get(r.ticker, [])
                base = len(buckets) * 10
                if r.last_event_date:
                    days_ago = (date.today() - r.last_event_date).days
                    base += max(0, 15 - days_ago)
                base += float(r.dr_20 or 0) * 0.3
                base += float(r.dr_5 or 0) * 0.6
                return base

            rows.sort(key=_score, reverse=True)
            rows = rows[: self.cfg["final_cap"]]

            candidates: list[dict[str, Any]] = []
            for r in rows:
                candidates.append({
                    "ticker": r.ticker,
                    "company_name": r.company_name,
                    "sector": r.sector,
                    "industry": r.industry,
                    "market_cap": r.market_cap,
                    "price": r.current_price,
                    "buckets": membership.get(r.ticker, []),
                    "metrics": {
                        "dr_1": r.dr_1, "dr_5": r.dr_5, "dr_20": r.dr_20,
                        "dr_60": r.dr_60, "dr_120": r.dr_120,
                        "rsi": r.rsi, "rsi_mktcap": r.rsi_mktcap,
                        "atr20": r.atr20,
                        "vol_vs_10d_avg": r.vol_vs_10d_avg,
                        "avg_vol_10d": r.avg_vol_10d,
                        "dollar_volume": r.dollar_volume,
                        "spike_day_count": r.spike_day_count,
                        "gapper_day_count": r.gapper_day_count,
                        "last_event_date": r.last_event_date,
                        "last_event_type": r.last_event_type,
                        "last_event_magnitude": r.last_event_magnitude,
                        "tags": r.tags,
                        "range_52_week": r.range_52_week,
                    },
                })

        stats = {
            "buckets": {b: len(t) for b, t in bucket_map.items()},
            "union_size": len(membership),
            "after_cap": len(candidates),
        }
        logger.info("InitialScreener stats: %s", stats)
        return self.envelope({"candidates": candidates, "stats": stats})


if __name__ == "__main__":
    # No input needed — but we still accept it for chain consistency.
    import argparse
    import logging as _logging
    from .base import save_json

    ap = argparse.ArgumentParser()
    ap.add_argument("--output", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    _logging.basicConfig(
        level=_logging.DEBUG if args.verbose else _logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )

    out = InitialScreener().run({})
    save_json(args.output, out)
    print(f"Wrote {args.output} ({len(out['candidates'])} candidates)")
