"""Stage 1: pick candidates from the 4 weekly screeners (via /api/WeeklyReview)."""

from __future__ import annotations

import logging
from collections import defaultdict

import requests

from theme_discovery import config

logger = logging.getLogger(__name__)

KEEP_FIELDS = (
    "ticker", "company_name", "sector", "industry", "cap_bucket", "market_cap",
    "dr_1", "dr_5", "dr_20", "dr_60", "sources", "listed",
    "adjusted_rs_score", "adjusted_ti65", "adjusted_dr_5", "adjusted_dr_20",
    "adjusted_event_return", "in_5d", "in_20d",
)


def _fetch_weekly_review() -> dict:
    url = f"{config.BACKEND_URL}/api/WeeklyReview?include_listed=1"
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.json()


def _passes(row: dict, extra: str | None) -> bool:
    if extra == "in_5d":
        return bool(row.get("in_5d"))
    if extra == "in_20d":
        return bool(row.get("in_20d"))
    if extra == "still_up":
        return (row.get("dr_20") or 0) > 0 or (row.get("dr_60") or 0) > 0
    return True


def build_candidates() -> dict:
    payload = _fetch_weekly_review()
    rows = [
        r for r in payload.get("stocks") or []
        if r.get("cap_bucket") in config.CAP_BUCKETS
        and r.get("industry") not in config.EXCLUDE_INDUSTRIES
    ]
    by_ticker = {r["ticker"]: r for r in rows}

    lists = {}
    picked_by: dict[str, list[str]] = defaultdict(list)
    for list_id, source, field, top_n, extra in config.PICKS:
        pool = [r for r in rows if source in (r.get("sources") or []) and _passes(r, extra)]
        pool.sort(key=lambda r: (r.get(field) is None, -(r.get(field) or 0)))
        top = []
        for cap in config.CAP_BUCKETS:
            top += [r for r in pool if r.get("cap_bucket") == cap][:top_n]
        lists[list_id] = {
            "source": source,
            "rank_field": field,
            "top_n": top_n,
            "per_cap": True,
            "filter": extra,
            "pool": len(pool),
            "picked": [r["ticker"] for r in top],
        }
        for r in top:
            picked_by[r["ticker"]].append(list_id)

    ordered = sorted(
        picked_by,
        key=lambda t: (-len(picked_by[t]), -(by_ticker[t].get("dr_20") or 0)),
    )
    per_industry: dict[str, int] = defaultdict(int)
    candidates, dropped = [], []
    for t in ordered:
        row = by_ticker[t]
        ind = row.get("industry") or "Unknown"
        entry = {k: row.get(k) for k in KEEP_FIELDS}
        entry["picked_by"] = picked_by[t]
        if config.MAX_PER_INDUSTRY and per_industry[ind] >= config.MAX_PER_INDUSTRY:
            entry["drop_reason"] = f"industry limit {config.MAX_PER_INDUSTRY}"
            dropped.append(entry)
            continue
        per_industry[ind] += 1
        candidates.append(entry)

    funnel = payload.get("funnel") or {}
    logger.info(
        "candidates: %d kept, %d dropped by industry limit, %d passes hidden by weekly review",
        len(candidates), len(dropped), funnel.get("hidden_pass", 0),
    )
    return {
        "weekly_cycle": payload.get("cycle"),
        "universe": {
            "cap_buckets": list(config.CAP_BUCKETS),
            "exclude_industries": list(config.EXCLUDE_INDUSTRIES),
            "rows": len(rows),
            "hidden_pass": funnel.get("hidden_pass", 0),
        },
        "lists": lists,
        "max_per_industry": config.MAX_PER_INDUSTRY,
        "candidates": candidates,
        "dropped": dropped,
    }
