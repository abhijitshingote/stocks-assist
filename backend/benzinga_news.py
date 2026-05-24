"""Fetch, normalize, and persist Benzinga news from Polygon/Massive API."""

from __future__ import annotations

import html
import os
import re
from datetime import datetime, timezone
from html.parser import HTMLParser

import requests
from sqlalchemy.dialects.postgresql import insert

from models import BenzingaArticle

DEFAULT_BASE_URL = "https://api.polygon.io"
DEFAULT_LIMIT = 40


class _HTMLToText(HTMLParser):
    BLOCK_TAGS = frozenset(
        {"p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6", "li", "tr"}
    )

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in self.BLOCK_TAGS:
            self._parts.append("\n\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in self.BLOCK_TAGS:
            self._parts.append("\n\n")

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


def html_to_plain(text: str | None) -> str:
    if not text:
        return ""
    parser = _HTMLToText()
    parser.feed(text)
    plain = html.unescape(parser.get_text())
    plain = re.sub(r"[ \t]+\n", "\n", plain)
    plain = re.sub(r"\n{3,}", "\n\n", plain)
    plain = re.sub(r"[ \t]+", " ", plain)
    return plain.strip()


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


def fetch_benzinga_from_api(ticker: str, *, limit: int = DEFAULT_LIMIT) -> list[dict]:
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("POLYGON_API_KEY not configured")

    base_url = os.getenv("POLYGON_API_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    url = f"{base_url}/benzinga/v2/news"
    params = {"tickers": ticker.upper(), "limit": limit}
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    response = requests.get(url, params=params, headers=headers, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "OK":
        raise RuntimeError(payload.get("error") or "Benzinga API returned non-OK status")
    return payload.get("results") or []


def article_row_from_api(ticker: str, raw: dict) -> dict:
    body_html = raw.get("body")
    teaser_html = raw.get("teaser")
    images = raw.get("images") or []
    return {
        "benzinga_id": int(raw["benzinga_id"]),
        "ticker": ticker.upper(),
        "title": html.unescape(raw.get("title") or ""),
        "teaser": html_to_plain(teaser_html),
        "body_html": body_html,
        "body_text": html_to_plain(body_html),
        "url": raw.get("url"),
        "author": raw.get("author"),
        "published": _parse_datetime(raw.get("published")),
        "last_updated": _parse_datetime(raw.get("last_updated")),
        "channels": raw.get("channels"),
        "tags": raw.get("tags"),
        "tickers": raw.get("tickers"),
        "images": images,
        "fetched_at": datetime.now(timezone.utc),
    }


def upsert_articles(session, ticker: str, raw_articles: list[dict]) -> int:
    if not raw_articles:
        return 0
    rows = [
        article_row_from_api(ticker, raw)
        for raw in raw_articles
        if raw.get("benzinga_id") is not None
    ]
    if not rows:
        return 0

    stmt = insert(BenzingaArticle).values(rows)
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in BenzingaArticle.__table__.columns
        if c.name not in ("id", "benzinga_id")
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=["benzinga_id"],
        set_=update_cols,
    )
    session.execute(stmt)
    session.commit()
    return len(rows)


def load_articles_from_db(session, ticker: str, *, limit: int = DEFAULT_LIMIT) -> list[BenzingaArticle]:
    return (
        session.query(BenzingaArticle)
        .filter(BenzingaArticle.ticker == ticker.upper())
        .order_by(BenzingaArticle.published.desc().nullslast())
        .limit(limit)
        .all()
    )


def article_to_json(row: BenzingaArticle) -> dict:
    published = row.published.isoformat() if row.published else None
    images = row.images or []
    snippet = row.teaser or (row.body_text[:200] if row.body_text else "")
    return {
        "benzinga_id": row.benzinga_id,
        "title": row.title,
        "url": row.url,
        "published_date": published,
        "site": "Benzinga",
        "text": snippet,
        "image": images[0] if images else None,
        "symbol": row.ticker,
        "source": "Benzinga",
        "author": row.author,
        "teaser": row.teaser,
        "body_text": row.body_text,
        "channels": row.channels or [],
        "tags": row.tags or [],
        "tickers": row.tickers or [],
    }


def refresh_benzinga_news(session, ticker: str, *, limit: int = DEFAULT_LIMIT) -> list[dict]:
    """Fetch from API, upsert into DB, return normalized articles for ticker."""
    raw = fetch_benzinga_from_api(ticker, limit=limit)
    upsert_articles(session, ticker, raw)
    rows = load_articles_from_db(session, ticker, limit=limit)
    return [article_to_json(r) for r in rows]


def get_cached_benzinga_news(session, ticker: str, *, limit: int = DEFAULT_LIMIT) -> list[dict]:
    rows = load_articles_from_db(session, ticker, limit=limit)
    return [article_to_json(r) for r in rows]
