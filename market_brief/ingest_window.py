"""Published-time filtering for Benzinga ingest."""

from __future__ import annotations

from datetime import datetime, timezone

from market_brief.trading_calendar import NewsWindow, compute_news_window


def get_news_window(asof: str | None = None) -> NewsWindow:
    return compute_news_window(asof)


def filter_published_window(
    raw_articles: list[dict],
    window: NewsWindow,
) -> list[dict]:
    """Keep articles with ``published`` in [window.start, window.end] (UTC)."""
    start = window.start_utc
    end = window.end_utc
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    out: list[dict] = []
    for raw in raw_articles:
        pub = _parse_datetime(raw.get("published"))
        if pub is None:
            continue
        if start <= pub <= end:
            out.append(raw)
    return out


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None
