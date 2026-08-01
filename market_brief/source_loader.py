"""Load Benzinga articles for market brief Step 4 from metadata ingest pools + Postgres."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from daily_screener.utils.db import get_session
import benzinga_news as bz  # noqa: E402

CHARS_PER_TOKEN = 3.5
MAX_BATCH_TOKENS = 80_000

_TICKER_ROW_RE = re.compile(
    r"^\|\s*\d+\s*\|\s*\*?\*?([A-Z][A-Z0-9.]{0,9})\*?\*?\s*\|",
    re.MULTILINE,
)
_MASTER_TICKER_RE = re.compile(
    r"^\|\s*\*?\*?([A-Z][A-Z0-9.]{0,9})\*?\*?\s*\|\s*([^|]+)\s*\|",
    re.MULTILINE,
)


def article_id(article: dict[str, Any]) -> str:
    raw = article.get("id") or article.get("benzinga_id")
    if raw is None:
        raise ValueError("article missing id/benzinga_id")
    return str(raw)


def _slugify(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "topic"


@dataclass
class IngestPools:
    """API-bucket articles loaded from DB via ``metadata.json`` ``ingest_pools`` ids."""

    general: list[dict] = field(default_factory=list)
    channels: dict[str, list[dict]] = field(default_factory=dict)
    ticker: dict[str, list[dict]] = field(default_factory=dict)


@dataclass
class Step4SourceBuild:
    """Step 4 prompt text plus per-section assignment counts."""

    channel_text: str
    ticker_text: str
    article_ids: list[str]
    section_counts: dict[str, int]
    pool_counts: dict[str, int]
    corpus_unique: int


def prompt_counts(built: Step4SourceBuild) -> dict[str, Any]:
    """Step 4: articles loaded from DB into the Opus user message (post dedupe)."""
    return {
        "articles_total": len(built.article_ids),
        "by_section": dict(sorted(built.section_counts.items())),
    }


def _pool_counts(pools: IngestPools) -> dict[str, int]:
    counts: dict[str, int] = {"general_pool": len(pools.general)}
    for slug, batch in sorted(pools.channels.items()):
        counts[f"channel_pool:{slug}"] = len(batch)
    counts["ticker_pool"] = sum(len(batch) for batch in pools.ticker.values())
    return counts


def _merge_pools(pools: IngestPools) -> tuple[list[dict], list[str]]:
    by_id: dict[int, dict] = {}

    def _add(batch: list[dict]) -> None:
        for article in batch:
            bid = article.get("benzinga_id")
            if bid is not None:
                by_id[int(bid)] = article

    _add(pools.general)
    for batch in pools.channels.values():
        _add(batch)
    for batch in pools.ticker.values():
        _add(batch)

    articles = list(by_id.values())
    articles.sort(key=lambda a: a.get("published") or "", reverse=True)
    corpus_ids = [str(bid) for bid in sorted(by_id.keys(), reverse=True)]
    return articles, corpus_ids


def _collect_pool_ids(pool_ids: dict[str, Any]) -> list[str]:
    ids: set[str] = set()
    for raw in pool_ids.get("general") or []:
        ids.add(str(raw))
    for batch in (pool_ids.get("channels") or {}).values():
        for raw in batch:
            ids.add(str(raw))
    for batch in (pool_ids.get("ticker") or {}).values():
        for raw in batch:
            ids.add(str(raw))
    return sorted(ids, key=int, reverse=True)


def _articles_for_ids(ids: list[str], by_id: dict[str, dict[str, Any]]) -> list[dict]:
    return [by_id[i] for i in ids if i in by_id]


def load_ingest_pools(outdir: Path) -> tuple[dict[str, dict[str, Any]], IngestPools]:
    """Hydrate API-bucket articles from ``ingest_pools`` ids in ``metadata.json``."""
    meta = load_metadata(outdir)
    pool_ids = meta.get("ingest_pools")
    if not pool_ids:
        raise FileNotFoundError(
            f"{outdir / 'metadata.json'} missing ingest_pools — run ingest first"
        )
    all_ids = _collect_pool_ids(pool_ids)
    by_id = load_articles_by_ids(all_ids)
    pools = IngestPools(
        general=_articles_for_ids(pool_ids.get("general") or [], by_id),
        channels={
            slug: _articles_for_ids(ids, by_id)
            for slug, ids in sorted((pool_ids.get("channels") or {}).items())
        },
        ticker={
            sym: _articles_for_ids(ids, by_id)
            for sym, ids in sorted((pool_ids.get("ticker") or {}).items())
        },
    )
    return by_id, pools


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


def _load_ingest_pools(outdir: Path) -> tuple[dict[str, dict[str, Any]], IngestPools]:
    return load_ingest_pools(outdir)


def dedupe_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for a in articles:
        try:
            aid = article_id(a)
        except ValueError:
            continue
        if aid in seen:
            continue
        seen.add(aid)
        out.append(a)
    return out


def format_article_block(article: dict[str, Any]) -> str:
    title = (article.get("title") or "").strip()
    teaser = (article.get("teaser") or "").strip()
    body = (
        article.get("body")
        or article.get("body_html")
        or article.get("body_text")
        or ""
    ).strip()
    created = article.get("created") or article.get("published") or ""
    tickers = ", ".join(article.get("tickers") or [])
    return (
        f"Title: {title}\n"
        f"Published: {created}\n"
        f"Tickers: {tickers}\n"
        f"Teaser: {teaser}\n"
        f"Body:\n{body}\n"
    )


def concat_articles(articles: list[dict[str, Any]]) -> str:
    if not articles:
        return ""
    return "\n---\n".join(format_article_block(a) for a in articles)


def _benzinga_id_int(article: dict[str, Any]) -> int | None:
    bid = article.get("benzinga_id")
    if bid is None:
        return None
    return int(bid)


def _ticker_label(article: dict[str, Any], *, folder_symbol: str = "") -> str:
    tickers = article.get("tickers") or []
    if tickers:
        return ", ".join(str(t) for t in tickers)
    if folder_symbol:
        return folder_symbol
    return "—"


def format_brief_article_block(
    article: dict[str, Any],
    *,
    channel: str,
    ticker: str = "",
) -> str:
    title = (article.get("title") or "").strip()
    body = (
        article.get("body")
        or article.get("body_html")
        or article.get("body_text")
        or ""
    ).strip()
    ticker_part = ticker or _ticker_label(article)
    return f"[TICKER: {ticker_part}] [{channel}] {title}\n{body}\n---\n"


def all_ticker_symbols(outdir: Path) -> list[str]:
    meta = load_metadata(outdir)
    syms = set(meta.get("universe_symbols") or [])
    overview_path = outdir / "source" / "ticker_universe" / "overview.md"
    for _, batch in parse_ticker_batches_from_overview(overview_path):
        syms.update(batch)
    return sorted(syms)


def audit_step4_source(
    outdir: Path,
    *,
    channel_text: str | None = None,
    ticker_text: str | None = None,
) -> dict[str, Any]:
    by_id, pools = _load_ingest_pools(outdir)
    if channel_text is None or ticker_text is None:
        built = build_step4_summaries_text(outdir)
        channel_text, ticker_text = built.channel_text, built.ticker_text
    ch_text = channel_text or ""
    tk_text = ticker_text or ""
    blocks_ch = ch_text.count("---\n") if ch_text else 0
    blocks_tk = tk_text.count("---\n") if tk_text else 0

    general_ch_ids: set[int] = set()
    for article in pools.general:
        bid = _benzinga_id_int(article)
        if bid is not None:
            general_ch_ids.add(bid)
    for batch in pools.channels.values():
        for article in batch:
            bid = _benzinga_id_int(article)
            if bid is not None:
                general_ch_ids.add(bid)

    ticker_only = 0
    for batch in pools.ticker.values():
        for article in batch:
            bid = _benzinga_id_int(article)
            if bid is not None and bid not in general_ch_ids:
                ticker_only += 1

    channel_row_count = sum(len(batch) for batch in pools.channels.values())
    ticker_row_count = sum(len(batch) for batch in pools.ticker.values())
    windows = load_metadata(outdir).get("windows") or {}
    return {
        "general_rows": len(pools.general),
        "channel_rows": channel_row_count,
        "ticker_rows": ticker_row_count,
        "raw_rows_total": len(pools.general) + channel_row_count + ticker_row_count,
        "unique_benzinga_ids": len(by_id),
        "step4_blocks_channel": blocks_ch,
        "step4_blocks_ticker": blocks_tk,
        "step4_blocks_total": blocks_ch + blocks_tk,
        "ticker_only_articles": ticker_only,
        "ticker_symbols": len(all_ticker_symbols(outdir)),
        "coverage_ok": len(by_id) == blocks_ch + blocks_tk,
        "article_chars": len(ch_text) + len(tk_text),
        "estimated_tokens_chars_per_3_5": estimate_tokens(ch_text + tk_text),
        "general_window": windows.get("general_window"),
        "ticker_window": windows.get("ticker_window"),
    }


def build_step4_summaries_text(outdir: Path) -> Step4SourceBuild:
    """Build Step 4 input from ingest API buckets (ids in metadata, bodies from DB)."""
    _by_id, pools = _load_ingest_pools(outdir)
    _articles, corpus_ids = _merge_pools(pools)
    seen: set[int] = set()
    article_ids: list[str] = []
    channel_sections: list[str] = []
    section_counts: dict[str, int] = {}

    def _take(article: dict[str, Any]) -> bool:
        bid = _benzinga_id_int(article)
        if bid is None or bid in seen:
            return False
        seen.add(bid)
        article_ids.append(article_id(article))
        return True

    general_blocks: list[str] = []
    for article in pools.general:
        if not _take(article):
            continue
        general_blocks.append(
            format_brief_article_block(
                article, channel="general", ticker=_ticker_label(article)
            )
        )
    if general_blocks:
        section_counts["Channel: general"] = len(general_blocks)
        channel_sections.append(f"## Channel: general\n\n{''.join(general_blocks)}")

    for slug in sorted(pools.channels.keys()):
        blocks: list[str] = []
        for article in pools.channels[slug]:
            if not _take(article):
                continue
            blocks.append(
                format_brief_article_block(
                    article, channel=slug, ticker=_ticker_label(article)
                )
            )
        if blocks:
            section_counts[f"Channel: {slug}"] = len(blocks)
            channel_sections.append(f"## Channel: {slug}\n\n{''.join(blocks)}")

    overview_path = outdir / "source" / "ticker_universe" / "overview.md"
    ticker_sections: list[str] = []
    batched: set[str] = set()
    for batch_label, symbols in parse_ticker_batches_from_overview(overview_path):
        batched.update(symbols)
        blocks = _ticker_blocks_for_symbols(
            symbols, pools.ticker, seen, article_ids
        )
        if blocks:
            section_counts[f"Ticker Group: {batch_label}"] = len(blocks)
            ticker_sections.append(f"## Ticker Group: {batch_label}\n\n{''.join(blocks)}")

    extra_syms = [s for s in all_ticker_symbols(outdir) if s not in batched]
    if extra_syms:
        blocks = _ticker_blocks_for_symbols(
            extra_syms, pools.ticker, seen, article_ids
        )
        if blocks:
            section_counts["Ticker Group: other_tickers"] = len(blocks)
            ticker_sections.append(f"## Ticker Group: other_tickers\n\n{''.join(blocks)}")

    return Step4SourceBuild(
        channel_text="\n\n".join(channel_sections),
        ticker_text="\n\n".join(ticker_sections),
        article_ids=article_ids,
        section_counts=section_counts,
        pool_counts=_pool_counts(pools),
        corpus_unique=len(corpus_ids),
    )


def _ticker_blocks_for_symbols(
    symbols: list[str],
    ticker_by_sym: dict[str, list[dict[str, Any]]],
    seen: set[int],
    article_ids: list[str],
) -> list[str]:
    blocks: list[str] = []
    for sym in symbols:
        for article in ticker_by_sym.get(sym, []):
            bid = _benzinga_id_int(article)
            if bid is None or bid in seen:
                continue
            seen.add(bid)
            article_ids.append(article_id(article))
            ch = article.get("channels") or []
            channel_label = ", ".join(str(c) for c in ch) if ch else sym
            blocks.append(
                format_brief_article_block(
                    article, channel=channel_label, ticker=sym
                )
            )
    return blocks


def estimate_tokens(text: str) -> int:
    return int(len(text) / CHARS_PER_TOKEN)


def load_all_source_articles(outdir: Path) -> dict[str, dict[str, Any]]:
    by_id, _ = _load_ingest_pools(outdir)
    return by_id


def channel_article_sets(
    outdir: Path, all_by_id: dict[str, dict[str, Any]] | None = None
) -> dict[str, list[dict[str, Any]]]:
    _by_id, pools = _load_ingest_pools(outdir)
    result: dict[str, list[dict[str, Any]]] = {}
    if pools.general:
        result["general"] = list(pools.general)
    for slug, arts in pools.channels.items():
        if arts:
            result[slug] = list(arts)
    return result


def channel_output_filename(slug: str) -> str:
    return f"channel_{slug}.md"


def parse_ticker_batches_from_overview(overview_path: Path) -> list[tuple[str, list[str]]]:
    if not overview_path.exists():
        return []

    text = overview_path.read_text(encoding="utf-8")
    master_start = text.find("## Master list")
    master_section = text[master_start:] if master_start >= 0 else ""

    ticker_to_screen: dict[str, str] = {}
    for m in _MASTER_TICKER_RE.finditer(master_section):
        sym = m.group(1).strip()
        section = m.group(2).strip()
        if sym and sym != "Ticker":
            ticker_to_screen[sym] = section

    if not ticker_to_screen:
        parts = re.split(r"^## ", text, flags=re.MULTILINE)
        for part in parts[1:]:
            if part.startswith("Master list"):
                break
            lines = part.split("\n", 1)
            header = lines[0].strip()
            body = lines[1] if len(lines) > 1 else ""
            tickers = _TICKER_ROW_RE.findall(body)
            screen_key = "mixed"
            if "r1d" in header.lower() or "1d return" in header.lower():
                screen_key = "r1d"
            elif "vol spike" in header.lower():
                screen_key = "vol_spike_5d"
            elif "ti65" in header.lower():
                screen_key = "main_view_ti65"
            for t in tickers:
                ticker_to_screen.setdefault(t, screen_key)

    batches_map: dict[str, list[str]] = {
        "r1d": [],
        "vol_spike_5d": [],
        "main_view_ti65_mega_large": [],
        "main_view_ti65_mid_small": [],
        "other": [],
    }
    for sym, screen in sorted(ticker_to_screen.items()):
        base = screen.split("/")[0].strip()
        cap = screen.split("/")[1].strip() if "/" in screen else ""
        if base == "r1d":
            batches_map["r1d"].append(sym)
        elif base == "vol_spike_5d":
            batches_map["vol_spike_5d"].append(sym)
        elif base == "main_view_ti65":
            if cap == "mid_small":
                batches_map["main_view_ti65_mid_small"].append(sym)
            else:
                batches_map["main_view_ti65_mega_large"].append(sym)
        else:
            batches_map["other"].append(sym)

    labels = {
        "r1d": "r1d_screener",
        "vol_spike_5d": "vol_spike_5d",
        "main_view_ti65_mega_large": "ti65_mega_large",
        "main_view_ti65_mid_small": "ti65_mid_small",
        "other": "other_tickers",
    }
    out: list[tuple[str, list[str]]] = []
    for key, label in labels.items():
        syms = sorted(set(batches_map[key]))
        if syms:
            out.append((label, syms))
    return out


def ticker_articles_for_symbols(
    outdir: Path,
    symbols: list[str],
    all_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    _by_id, pools = _load_ingest_pools(outdir)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for sym in symbols:
        for article in pools.ticker.get(sym, []):
            try:
                aid = article_id(article)
            except ValueError:
                continue
            if aid in seen:
                continue
            seen.add(aid)
            out.append(article)
    return out


def split_oversized_batch(
    label: str, articles: list[dict[str, Any]]
) -> list[tuple[str, list[dict[str, Any]]]]:
    text = concat_articles(articles)
    if estimate_tokens(text) <= MAX_BATCH_TOKENS:
        return [(label, articles)]

    mid = len(articles) // 2
    if mid < 1:
        return [(label, articles)]
    return [
        *split_oversized_batch(f"{label}_a", articles[:mid]),
        *split_oversized_batch(f"{label}_b", articles[mid:]),
    ]
