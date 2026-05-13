"""Stage 6: Audit Roll-up.

Combines every ticker in the original universe with whatever the pipeline
learned about it: momentum score, news summary, judge verdict + rationale, OR
the stage and reason at which it was dropped. Single source of truth read by
the UI.

Output: outputs/<date>/05_audit.json
"""

from __future__ import annotations

import logging
from typing import Any

from daily_screener.utils import io

logger = logging.getLogger(__name__)


def _index_by_ticker(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {(i.get("ticker") or "").upper(): i for i in items if i.get("ticker")}


def _row_skeleton(univ_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": univ_row["ticker"],
        "company_name": univ_row.get("company_name"),
        "sector": univ_row.get("sector"),
        "industry": univ_row.get("industry"),
        "market_cap": univ_row.get("market_cap"),
        "current_price": univ_row.get("current_price"),
        "sources": univ_row.get("sources", []),
        "dr_1": univ_row.get("dr_1"),
        "dr_5": univ_row.get("dr_5"),
        "dr_20": univ_row.get("dr_20"),
        "dr_60": univ_row.get("dr_60"),
        "atr20": univ_row.get("atr20"),
        "rsi_mktcap": univ_row.get("rsi_mktcap"),
        "vol_vs_10d_avg": univ_row.get("vol_vs_10d_avg"),
        "last_event_date": univ_row.get("last_event_date"),
        "last_event_type": univ_row.get("last_event_type"),
        "last_event_magnitude": univ_row.get("last_event_magnitude"),
        # filled in below
        "momentum_score": None,
        "momentum_factors": None,
        "news_summary": None,
        "headline_drivers": [],
        "news_theme_tags": [],
        "latest_material_date": None,
        "has_material_news": None,
        "news_materiality": None,
        "news_materiality_reason": None,
        "theme_fit": None,
        "theme_fit_reason": None,
        "matched_themes": [],
        "composite_score": None,
        "verdict": None,
        "verdict_claude": None,
        "verdict_rationale": None,
        "dropped_at_stage": None,
        "drop_reason": None,
    }


def run(date_str: str | None = None) -> dict[str, Any]:
    date_str = date_str or io.today_str()
    logger.info("[s6] rolling up audit for %s", date_str)

    universe = io.read_json(io.artifact_path(date_str, "00_universe.json"))
    momentum = io.read_json_if_exists(
        io.artifact_path(date_str, "01_momentum.json"),
        default={"survivors": [], "drops": []},
    )
    news = io.read_json_if_exists(
        io.artifact_path(date_str, "03_news.json"),
        default={"results": [], "failures": []},
    )
    judged = io.read_json_if_exists(
        io.artifact_path(date_str, "04_judged.json"),
        default={"results": [], "failures": [], "skipped_no_news": []},
    )

    mom_survivors = _index_by_ticker(momentum.get("survivors", []))
    mom_drops = _index_by_ticker(momentum.get("drops", []))
    news_results = _index_by_ticker(news.get("results", []))
    news_failures = _index_by_ticker(news.get("failures", []))
    judged_results = _index_by_ticker(judged.get("results", []))
    judged_failures = _index_by_ticker(judged.get("failures", []))
    skipped_no_news = _index_by_ticker(judged.get("skipped_no_news", []))

    rows: list[dict[str, Any]] = []
    for univ_row in universe.get("tickers", []):
        ticker = univ_row["ticker"].upper()
        row = _row_skeleton(univ_row)

        # Stage 2: momentum
        if ticker in mom_survivors:
            m = mom_survivors[ticker]
            row["momentum_score"] = m.get("momentum_score")
            row["momentum_factors"] = m.get("factors")
        elif ticker in mom_drops:
            m = mom_drops[ticker]
            row["momentum_score"] = m.get("momentum_score")
            row["momentum_factors"] = m.get("factors")
            row["dropped_at_stage"] = 2
            row["drop_reason"] = m.get("drop_reason")

        # Stage 4: news
        if ticker in news_results:
            n = news_results[ticker]
            row["news_summary"] = n.get("summary")
            row["headline_drivers"] = n.get("headline_drivers", [])
            row["news_theme_tags"] = n.get("theme_tags", [])
            row["latest_material_date"] = n.get("latest_material_date")
            row["has_material_news"] = n.get("has_material_news")
        elif ticker in news_failures and row["dropped_at_stage"] is None:
            row["dropped_at_stage"] = 4
            row["drop_reason"] = news_failures[ticker].get("drop_reason")
        elif ticker in skipped_no_news and row["dropped_at_stage"] is None:
            row["dropped_at_stage"] = 4
            row["drop_reason"] = "no_news_fetched"

        # Stage 5: judge
        if ticker in judged_results:
            j = judged_results[ticker]
            row["news_materiality"] = j.get("news_materiality")
            row["news_materiality_reason"] = j.get("news_materiality_reason")
            row["theme_fit"] = j.get("theme_fit")
            row["theme_fit_reason"] = j.get("theme_fit_reason")
            row["matched_themes"] = j.get("matched_themes", [])
            row["composite_score"] = j.get("composite_score")
            row["verdict"] = j.get("verdict")
            row["verdict_claude"] = j.get("verdict_claude")
            row["verdict_rationale"] = j.get("verdict_rationale")
        elif ticker in judged_failures and row["dropped_at_stage"] is None:
            row["dropped_at_stage"] = 5
            row["drop_reason"] = judged_failures[ticker].get("drop_reason")

        # Default verdict for rows that never made it past Stage 2 / 4: SKIP
        if row["verdict"] is None:
            row["verdict"] = "SKIP"

        rows.append(row)

    # Pick up vetoes / industry drops that never made it into the universe.
    for d in universe.get("drops", []):
        rows.append(
            {
                "ticker": d["ticker"],
                "company_name": None,
                "sources": [],
                "verdict": "SKIP",
                "dropped_at_stage": 1,
                "drop_reason": d.get("drop_reason"),
            }
        )

    # Sort: PICKs first (by composite desc), then WATCH (by composite desc),
    # then SKIPs (by composite desc within rated, then by momentum desc).
    def _sort_key(r):
        rank_map = {"PICK": 0, "WATCH": 1, "SKIP": 2}
        rank = rank_map.get(r.get("verdict") or "SKIP", 3)
        comp = -(r.get("composite_score") or 0)
        mom = -(r.get("momentum_score") or 0)
        return (rank, comp, mom)

    rows.sort(key=_sort_key)

    pick_count = sum(1 for r in rows if r["verdict"] == "PICK")
    watch_count = sum(1 for r in rows if r["verdict"] == "WATCH")
    skip_count = sum(1 for r in rows if r["verdict"] == "SKIP")

    artifact = {
        "date": date_str,
        "total_universe": universe.get("total_universe"),
        "verdict_counts": {
            "PICK": pick_count,
            "WATCH": watch_count,
            "SKIP": skip_count,
        },
        "source_counts": universe.get("source_counts"),
        "rows": rows,
    }
    io.write_json(io.artifact_path(date_str, "05_audit.json"), artifact)
    logger.info(
        "[s6] audit complete: PICK=%d, WATCH=%d, SKIP=%d (total %d rows)",
        pick_count,
        watch_count,
        skip_count,
        len(rows),
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
