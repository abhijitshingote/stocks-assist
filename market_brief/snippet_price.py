"""Price context appended after Ollama snippets — not part of the LLM prompt."""

from __future__ import annotations

import logging
import re

from market_brief import tape as tape_mod

logger = logging.getLogger(__name__)

PRICE_REFERENCE_HEADER = "## Price reference\n\n"


def article_body_for_summarize(article: dict) -> str:
    """Article text for Ollama — strips Benzinga price-scan blocks so tape does not steer the model."""
    body = (article.get("body_text") or article.get("teaser") or "").strip()
    if not body:
        return ""
    body = re.sub(
        r"(?is)\n*#{1,6}\s*(?:price action|stock price activity)\s*\n.*?(?=\n#{1,6}\s|\Z)",
        "\n\n",
        body,
    )
    body = re.sub(
        r"(?is)(?:price action|stock price activity)\s*:?\s*.+?(?:\n\n|photo via|\Z)",
        "",
        body,
    )
    return re.sub(r"\n{3,}", "\n\n", body).strip()


def split_fundamental_and_price(text: str) -> tuple[str, str | None]:
    """Split Ollama fundamental output from post-appended price reference."""
    if PRICE_REFERENCE_HEADER in text:
        fund, price = text.split(PRICE_REFERENCE_HEADER, 1)
        return fund.strip(), price.strip() or None
    return text.strip(), None


def _extract_benzinga_price_scan(body: str) -> str | None:
    """Pull explicit Benzinga price-scan lines from the article body (regex only)."""
    if not body:
        return None

    section = re.search(
        r"(?is)(?:price action|stock price activity)\s*:?\s*(.+?)(?:\n\n|photo via|\Z)",
        body,
    )
    if section:
        line = " ".join(section.group(1).split())
        if line and len(line) <= 400:
            return line

    for line in body.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if re.search(r"\d+\.?\d*\s*%", stripped) and re.search(
            r"\$\d", stripped
        ):
            if re.search(
                r"(?i)benzinga|shares were|stock price|at the time of publication",
                stripped,
            ):
                return stripped
    return None


def price_reference_footer(article: dict, asof: str) -> str:
    """Verified tape + article price scan — appended after summarization, not sent to Ollama."""
    lines: list[str] = []
    body = article.get("body_text") or article.get("teaser") or ""

    scan = _extract_benzinga_price_scan(body)
    if scan:
        lines.append(f"- Article price scan: {scan}")

    tickers = article.get("tickers") or []
    primary = (tickers[0] if tickers else None) or article.get("symbol")
    if primary:
        try:
            session_date, t_quotes, _ = tape_mod.get_tape(
                [str(primary).upper()], asof, include_indices=False
            )
            if t_quotes:
                lines.append(f"- Verified tape ({session_date}): {t_quotes[0].fmt()}")
        except Exception as e:  # noqa: BLE001
            logger.debug("tape lookup failed for %s: %s", primary, e)

    if not lines:
        return ""
    return PRICE_REFERENCE_HEADER + "\n".join(lines) + "\n"


def append_price_reference(snippet: str, article: dict, asof: str) -> str:
    """Return fundamental snippet plus optional price block (for persistence / assembly)."""
    base = snippet.strip()
    footer = price_reference_footer(article, asof)
    if not footer:
        return base
    return f"{base}\n\n{footer}"
