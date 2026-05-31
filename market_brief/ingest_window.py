"""Published-time filtering for Benzinga ingest."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from market_brief import config
from market_brief.trading_calendar import NewsWindow, compute_news_window


def get_news_window(asof: str | None = None) -> NewsWindow:
    """General + channel pulls: 5:00 AM ET anchor session → run time."""
    return compute_news_window(asof)


def get_ticker_news_window(asof: str | None = None) -> NewsWindow:
    """Per-ticker pulls: same end as general, start ``TICKER_NEWS_EXTRA_HOURS`` earlier."""
    base = compute_news_window(asof)
    extra = timedelta(hours=config.TICKER_NEWS_EXTRA_HOURS)
    start_utc = base.start_utc - extra
    start_et = start_utc.astimezone(base.start_et.tzinfo)
    label = (
        f"{start_et.strftime('%Y-%m-%d %H:%M %Z')} → "
        f"{base.end_et.strftime('%Y-%m-%d %H:%M %Z')} "
        f"(session {base.anchor_session.isoformat()}, "
        f"ticker −{config.TICKER_NEWS_EXTRA_HOURS}h vs general)"
    )
    return NewsWindow(
        start_utc=start_utc,
        end_utc=base.end_utc,
        anchor_session=base.anchor_session,
        run_at_et=base.run_at_et,
        label=label,
    )


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
