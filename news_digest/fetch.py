"""Stage 1: pull the whole Benzinga universe for a window, dedupe near-identical titles.

No channel/ticker/tag filtering and no ticker grouping — news is not always
ticker-scoped, so grouping by symbol injects noise. The only mechanical
reduction here is collapsing restatements of the same headline
(``(UPDATED)``, ``CORRECTION:``, wire re-runs).
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from news_digest import config

logger = logging.getLogger(__name__)

_BASE_URL = os.getenv("POLYGON_API_BASE_URL", "https://api.polygon.io").rstrip("/")

_TAG_RE = re.compile(r"<[^>]+>")
_RESTATE_RE = re.compile(
    r"\((updated|corrected|revised)\)|^(update|updated|correction|corrected|exclusive)\s*:",
    re.I,
)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]")


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def week_bounds(asof: datetime | None = None) -> tuple[datetime, datetime]:
    """The most recent complete Sat 00:00 ET -> Sat 00:00 ET week ending on or
    before ``asof``.

    Run on Sunday Sep 13 this returns Sat Sep 5 through Fri Sep 11 inclusive.
    A run during the current week therefore reports last week, not a partial one.
    """
    tz = ZoneInfo(config.TIMEZONE)
    asof = (asof or datetime.now(timezone.utc)).astimezone(tz)

    # Days back to the most recent Saturday boundary at or before asof.
    days_since_anchor = (asof.weekday() - config.WEEK_ANCHOR_WEEKDAY) % 7
    latest_anchor = datetime.combine(asof.date() - timedelta(days=days_since_anchor), time.min, tz)

    # That anchor opens the in-progress week, so step back one more to get a
    # week that has actually finished.
    end = latest_anchor
    return end - timedelta(days=7), end


def _plain(html: str | None) -> str:
    """Cheap HTML strip. Bodies are only used as LLM input, not rendered."""
    if not html:
        return ""
    import html as html_mod

    text = _TAG_RE.sub(" ", html)
    text = html_mod.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_title(title: str) -> str:
    """Collapse restatement markers so wire re-runs hash to one key."""
    t = _RESTATE_RE.sub(" ", title.lower())
    t = _NON_ALNUM_RE.sub(" ", t)
    return " ".join(t.split())


def fetch_universe(
    start: datetime,
    stop: datetime,
    *,
    on_page=None,
) -> tuple[list[dict], dict]:
    """Paginated ``GET /benzinga/v2/news`` across the window. No filters."""
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("POLYGON_API_KEY not configured")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    url = f"{_BASE_URL}/benzinga/v2/news"
    params: dict | None = {
        "limit": config.API_PAGE_LIMIT,
        "sort": "published.desc",
        "published.gte": _iso(start),
        "published.lte": _iso(stop),
    }

    articles: list[dict] = []
    pages = 0
    truncated = False

    while url:
        resp = requests.get(
            url, params=params, headers=headers, timeout=config.FETCH_TIMEOUT_SECONDS
        )
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("results") or []
        pages += 1

        for raw in rows:
            bid = raw.get("benzinga_id")
            if bid is None:
                continue
            body = _plain(raw.get("body"))
            articles.append(
                {
                    "benzinga_id": int(bid),
                    "title": _plain(raw.get("title")),
                    "teaser": _plain(raw.get("teaser")),
                    "body": body,
                    "author": (raw.get("author") or "").strip().lower(),
                    "url": raw.get("url") or "",
                    "published": raw.get("published") or "",
                    "channels": [str(c).strip().lower() for c in (raw.get("channels") or [])],
                    "tags": [str(t).strip().lower() for t in (raw.get("tags") or [])],
                    "tickers": [str(t).upper() for t in (raw.get("tickers") or [])],
                }
            )

        if on_page:
            on_page(pages, len(articles))
        logger.info("fetch page %d: %d rows (total %d)", pages, len(rows), len(articles))

        url, params = payload.get("next_url"), None
        if pages >= config.API_MAX_PAGES:
            truncated = bool(url)
            if truncated:
                logger.warning("hit API_MAX_PAGES=%d, window truncated", config.API_MAX_PAGES)
            break

    # Same benzinga_id can appear on two pages; keep one.
    by_id: dict[int, dict] = {}
    for a in articles:
        by_id[a["benzinga_id"]] = a
    unique = list(by_id.values())

    stats = {
        "pages": pages,
        "api_rows": len(articles),
        "unique_ids": len(unique),
        "truncated": truncated,
        "window_start": start.isoformat(),
        "window_end": stop.isoformat(),
    }
    return unique, stats


def dedupe_titles(articles: list[dict]) -> tuple[list[dict], int]:
    """Collapse restatements of the same headline.

    Winner is the variant with the longest body (most complete version); the
    losers' ids ride along in ``dup_ids`` so nothing is lost on drill-down.
    """
    best: dict[str, dict] = {}
    for a in sorted(articles, key=lambda x: len(x["body"]), reverse=True):
        key = normalize_title(a["title"])
        if not key:
            key = f"__id_{a['benzinga_id']}"
        winner = best.get(key)
        if winner is None:
            a["dup_ids"] = []
            best[key] = a
        else:
            winner["dup_ids"].append(a["benzinga_id"])

    out = sorted(best.values(), key=lambda x: x["published"], reverse=True)
    collapsed = len(articles) - len(out)
    return out, collapsed
