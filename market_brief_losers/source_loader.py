"""Load ticker-only Benzinga articles for losers brief synthesis."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from daily_screener.utils.db import get_session

import benzinga_news as bz  # noqa: E402

CHARS_PER_TOKEN = 3.5


def article_id(article: dict[str, Any]) -> str:
    raw = article.get("id") or article.get("benzinga_id")
    if raw is None:
        raise ValueError("article missing id/benzinga_id")
    return str(raw)


@dataclass
class TickerPool:
    ticker: dict[str, list[dict]] = field(default_factory=dict)


@dataclass
class SourceBuild:
    ticker_text: str
    article_ids: list[str]
    section_counts: dict[str, int]
    ticker_symbols: int


def load_metadata(outdir: Path) -> dict[str, Any]:
    path = outdir / "metadata.json"
    if not path.exists():
        raise FileNotFoundError(f"missing metadata.json at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_articles_by_ids(ids: list[str]) -> dict[str, dict[str, Any]]:
    if not ids:
        return {}
    int_ids = [int(i) for i in ids]
    session = get_session()
    try:
        rows = bz.load_articles_by_ids(session, int_ids)
        return {str(r.benzinga_id): bz.article_to_json(r) for r in rows}
    finally:
        session.close()


def _collect_pool_ids(pool_ids: dict[str, Any]) -> list[str]:
    ids: set[str] = set()
    for batch in (pool_ids.get("ticker") or {}).values():
        for raw in batch:
            ids.add(str(raw))
    return sorted(ids, key=int, reverse=True)


def load_ticker_pool(outdir: Path) -> tuple[dict[str, dict[str, Any]], TickerPool]:
    meta = load_metadata(outdir)
    pool_ids = meta.get("ingest_pools")
    if not pool_ids:
        raise FileNotFoundError(
            f"{outdir / 'metadata.json'} missing ingest_pools — run ingest first"
        )
    all_ids = _collect_pool_ids(pool_ids)
    by_id = load_articles_by_ids(all_ids)
    pool = TickerPool(
        ticker={
            sym: [by_id[i] for i in ids if i in by_id]
            for sym, ids in sorted((pool_ids.get("ticker") or {}).items())
        },
    )
    return by_id, pool


def format_brief_article_block(
    article: dict[str, Any],
    *,
    ticker: str,
) -> str:
    title = (article.get("title") or "").strip()
    body = (
        article.get("body")
        or article.get("body_html")
        or article.get("body_text")
        or ""
    ).strip()
    created = article.get("created") or article.get("published") or ""
    return f"[TICKER: {ticker}] {title}\nPublished: {created}\n{body}\n---\n"


def _cap_bucket_sections(outdir: Path) -> list[tuple[str, list[str]]]:
    """Return (section label, tickers) ordered mega → large → mid_small."""
    meta = load_metadata(outdir)
    sections: list[tuple[str, list[str]]] = []
    for sl in meta.get("losers_universe_slices") or []:
        cap = sl.get("cap_bucket", "")
        label = sl.get("label") or cap
        tickers = [str(t).upper() for t in (sl.get("tickers") or [])]
        if tickers:
            sections.append((label, tickers))
    if sections:
        return sections

    overview = outdir / "source" / "losers_universe" / "overview.md"
    if not overview.exists():
        return []
    text = overview.read_text(encoding="utf-8")
    out: list[tuple[str, list[str]]] = []
    parts = re.split(r"^## ", text, flags=re.MULTILINE)
    for part in parts[1:]:
        lines = part.split("\n", 1)
        header = lines[0].strip()
        if header.startswith("Master list"):
            break
        body = lines[1] if len(lines) > 1 else ""
        tickers = re.findall(
            r"^\|\s*\d+\s*\|\s*\*?\*?([A-Z][A-Z0-9.]{0,9})\*?\*?\s*\|",
            body,
            re.MULTILINE,
        )
        if tickers:
            out.append((header, tickers))
    return out


def build_ticker_summaries_text(outdir: Path) -> SourceBuild:
    """Build synthesis input from ticker ingest pool only."""
    _by_id, pool = load_ticker_pool(outdir)
    seen: set[int] = set()
    article_ids: list[str] = []
    section_counts: dict[str, int] = {}
    ticker_sections: list[str] = []

    def _blocks_for_symbols(symbols: list[str]) -> list[str]:
        blocks: list[str] = []
        for sym in symbols:
            for article in pool.ticker.get(sym, []):
                bid = article.get("benzinga_id")
                if bid is None:
                    continue
                i = int(bid)
                if i in seen:
                    continue
                seen.add(i)
                article_ids.append(article_id(article))
                blocks.append(format_brief_article_block(article, ticker=sym))
        return blocks

    for label, symbols in _cap_bucket_sections(outdir):
        blocks = _blocks_for_symbols(symbols)
        if blocks:
            section_counts[f"Ticker: {label}"] = len(blocks)
            ticker_sections.append(f"## Ticker: {label}\n\n{''.join(blocks)}")

    meta = load_metadata(outdir)
    batched: set[str] = set()
    for _label, syms in _cap_bucket_sections(outdir):
        batched.update(syms)
    extra = [s for s in (meta.get("universe_symbols") or []) if s not in batched]
    if extra:
        blocks = _blocks_for_symbols(extra)
        if blocks:
            section_counts["Ticker: other"] = len(blocks)
            ticker_sections.append(f"## Ticker: other\n\n{''.join(blocks)}")

    return SourceBuild(
        ticker_text="\n\n".join(ticker_sections),
        article_ids=article_ids,
        section_counts=section_counts,
        ticker_symbols=len(meta.get("universe_symbols") or []),
    )


def prompt_counts(built: SourceBuild) -> dict[str, Any]:
    return {
        "articles_total": len(built.article_ids),
        "by_section": dict(sorted(built.section_counts.items())),
    }


def estimate_tokens(text: str) -> int:
    return int(len(text) / CHARS_PER_TOKEN)


def audit_source(outdir: Path, *, ticker_text: str | None = None) -> dict[str, Any]:
    by_id, pool = load_ticker_pool(outdir)
    tk_text = ticker_text or build_ticker_summaries_text(outdir).ticker_text
    blocks = tk_text.count("---\n") if tk_text else 0
    ticker_row_count = sum(len(batch) for batch in pool.ticker.values())
    windows = load_metadata(outdir).get("windows") or {}
    return {
        "source_scope": "ticker_only",
        "ticker_rows": ticker_row_count,
        "unique_benzinga_ids": len(by_id),
        "prompt_blocks": blocks,
        "coverage_ok": len(by_id) == blocks,
        "ticker_symbols": len(load_metadata(outdir).get("universe_symbols") or []),
        "article_chars": len(tk_text),
        "estimated_tokens_chars_per_3_5": estimate_tokens(tk_text),
        "ticker_window": windows.get("ticker_window"),
    }
