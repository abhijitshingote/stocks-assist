"""News retrieval helper.

This is intentionally thin: the fundamental agent does the heavy thinking.
Here we just provide two helpers backed by MCP:

* `discover_hot_themes()` — fetch a couple of momentum/news landing pages,
  return raw text snippets the LLM can summarise into themes.
* `fetch_ticker_news(ticker)` — fetch a stock-specific news search results
  page and return raw text snippets.

If MCP is unavailable, we return empty data and let the agent decide what to do
(graceful fallback is enabled by default in `config.py`).
"""

from __future__ import annotations

import logging
from typing import Any

from . import mcp_client

logger = logging.getLogger(__name__)

# Curated list of momentum/news landing pages. The fetch MCP server returns
# their cleaned text; the LLM extracts themes from the merged blob.
THEME_SOURCES = [
    "https://www.stocktitan.net/news/whats-trending",
    "https://finance.yahoo.com/topic/stock-market-news/",
    "https://www.investing.com/news/stock-market-news",
    "https://seekingalpha.com/market-news",
]

TICKER_NEWS_TEMPLATES = [
    # finviz news for a ticker is fairly clean and lists headlines + sources.
    "https://finviz.com/quote.ashx?t={ticker}&p=d",
    "https://www.stocktitan.net/news/{ticker}",
]


def _safe_fetch(client, url: str) -> str:
    try:
        # Find any tool whose qualified name ends with `.fetch` (mcp-server-fetch
        # exposes a single tool called `fetch`).
        target = next(
            (t["name"] for t in client.list_tools() if t["raw_name"] == "fetch"),
            None,
        )
        if target is None:
            logger.warning("No fetch tool available in connected MCP servers")
            return ""
        return client.call_tool(target, {"url": url, "max_length": 8000})
    except Exception as e:  # noqa: BLE001 - we always degrade gracefully
        logger.warning("MCP fetch failed for %s: %s", url, e)
        return ""


def discover_hot_themes_raw() -> list[dict[str, str]]:
    """Return raw text snippets from theme-source pages."""
    try:
        client = mcp_client.get_client()
    except Exception as e:  # noqa: BLE001
        logger.warning("MCP client unavailable: %s", e)
        return []

    snippets: list[dict[str, str]] = []
    for url in THEME_SOURCES:
        text = _safe_fetch(client, url)
        if text:
            snippets.append({"url": url, "text": text[:6000]})
    return snippets


def fetch_ticker_news(ticker: str, max_pages: int = 2) -> list[dict[str, str]]:
    try:
        client = mcp_client.get_client()
    except Exception as e:  # noqa: BLE001
        logger.warning("MCP client unavailable: %s", e)
        return []

    out: list[dict[str, str]] = []
    for tmpl in TICKER_NEWS_TEMPLATES[:max_pages]:
        url = tmpl.format(ticker=ticker.upper())
        text = _safe_fetch(client, url)
        if text:
            out.append({"url": url, "text": text[:5000]})
    return out
