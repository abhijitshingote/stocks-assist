"""Load Benzinga articles for market brief steps from metadata.json + Postgres."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from daily_screener.utils.db import get_session
from market_brief import config
from market_brief.ingest import article_ticker_symbols, news_window_for_run
from market_brief.ingest_window import get_news_window, get_ticker_news_window
from market_brief.trading_calendar import NewsWindow

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


def _channel_slugs() -> list[str]:
    slugs = ["general"]
    for ch, _limit in config.GENERAL_CHANNEL_FETCHES:
        slug = _slugify(ch)
        if slug not in slugs:
            slugs.append(slug)
    return slugs


def _parse_published(article: dict[str, Any]) -> datetime | None:
    raw = article.get("published") or article.get("published_date")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _published_in_window(article: dict[str, Any], window: NewsWindow) -> bool:
    pub = _parse_published(article)
    if pub is None:
        return False
    start = window.start_utc
    end = window.end_utc
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return start <= pub <= end


def _article_channel_slugs(article: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for ch in article.get("channels") or []:
        slug = _slugify(str(ch))
        if slug:
            out.add(slug)
    return out


@dataclass
class SynthesisPools:
    """Three explicit DB pulls for Step 4 (general window, per-channel, ticker window)."""

    general: list[dict] = field(default_factory=list)
    channels: dict[str, list[dict]] = field(default_factory=dict)
    ticker: list[dict] = field(default_factory=list)


def load_synthesis_pools(
    session,
    asof: str,
    universe: set[str],
) -> SynthesisPools:
    """DB pulls: general (no channel tag) + each configured channel + ticker universe."""
    general_window = news_window_for_run(asof)
    ticker_window = get_ticker_news_window(asof)
    start_g = general_window.start_utc
    end_g = general_window.end_utc

    general = bz._rows_to_json(
        bz.load_articles_general_window(session, start_g, end_g)
    )

    channels: dict[str, list[dict]] = {}
    for channel_name, _limit in config.GENERAL_CHANNEL_FETCHES:
        slug = _slugify(channel_name)
        rows = bz.load_articles_channel_window(
            session, start_g, end_g, channel_name
        )
        channels[slug] = bz._rows_to_json(rows)

    ticker_rows = bz.load_articles_published_between(
        session,
        ticker_window.start_utc,
        ticker_window.end_utc,
    )
    ticker = [
        bz.article_to_json(r)
        for r in ticker_rows
        if article_ticker_symbols(bz.article_to_json(r)) & universe
    ]

    return SynthesisPools(general=general, channels=channels, ticker=ticker)


def _merge_pools(pools: SynthesisPools) -> tuple[list[dict], list[str]]:
    by_id: dict[int, dict] = {}

    def _add(batch: list[dict]) -> None:
        for article in batch:
            bid = article.get("benzinga_id")
            if bid is not None:
                by_id[int(bid)] = article

    _add(pools.general)
    for batch in pools.channels.values():
        _add(batch)
    _add(pools.ticker)

    articles = list(by_id.values())
    articles.sort(key=lambda a: a.get("published") or "", reverse=True)
    corpus_ids = [str(bid) for bid in sorted(by_id.keys(), reverse=True)]
    return articles, corpus_ids


def load_synthesis_corpus(
    session,
    asof: str,
    universe: set[str],
) -> tuple[list[dict], list[str]]:
    """Merged corpus ids from general + channel + ticker DB pulls."""
    pools = load_synthesis_pools(session, asof, universe)
    return _merge_pools(pools)


def synthesis_pools_for_run(asof: str, universe: set[str]) -> SynthesisPools:
    session = get_session()
    try:
        return load_synthesis_pools(session, asof, universe)
    finally:
        session.close()


def synthesis_corpus_for_run(asof: str, universe: set[str]) -> tuple[list[dict], list[str]]:
    session = get_session()
    try:
        return load_synthesis_corpus(session, asof, universe)
    finally:
        session.close()


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


def _load_window_pools(
    outdir: Path,
) -> tuple[dict[str, dict[str, Any]], SynthesisPools, set[str]]:
    meta = load_metadata(outdir)
    asof = meta["asof"]
    universe = {s.upper() for s in (meta.get("universe_symbols") or [])}
    pools = synthesis_pools_for_run(asof, universe)
    articles, _ = _merge_pools(pools)
    by_id: dict[str, dict[str, Any]] = {}
    for article in articles:
        try:
            by_id[article_id(article)] = article
        except ValueError:
            continue
    return by_id, pools, universe


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
    by_id, pools, _universe = _load_window_pools(outdir)
    if channel_text is None or ticker_text is None:
        channel_text, ticker_text, _ = build_step4_summaries_text(outdir)
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

    ticker_only = sum(
        1
        for article in pools.ticker
        if (bid := _benzinga_id_int(article)) is not None and bid not in general_ch_ids
    )

    channel_row_count = sum(len(batch) for batch in pools.channels.values())
    windows = load_metadata(outdir).get("windows") or {}
    return {
        "general_window_rows": len(pools.general),
        "channel_window_rows": channel_row_count,
        "ticker_window_rows": len(pools.ticker),
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


def build_step4_summaries_text(outdir: Path) -> tuple[str, str, list[str]]:
    """Build Step 4 input from three DB pulls: general, per-channel, ticker."""
    _by_id, pools, universe = _load_window_pools(outdir)
    seen: set[int] = set()
    article_ids: list[str] = []
    channel_sections: list[str] = []

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
            channel_sections.append(f"## Channel: {slug}\n\n{''.join(blocks)}")

    overview_path = outdir / "source" / "ticker_universe" / "overview.md"
    ticker_sections: list[str] = []
    batched: set[str] = set()
    for batch_label, symbols in parse_ticker_batches_from_overview(overview_path):
        batched.update(symbols)
        blocks = _ticker_blocks_for_symbols(symbols, pools.ticker, seen, article_ids)
        if blocks:
            ticker_sections.append(f"## Ticker Group: {batch_label}\n\n{''.join(blocks)}")

    extra_syms = [s for s in all_ticker_symbols(outdir) if s not in batched]
    if extra_syms:
        blocks = _ticker_blocks_for_symbols(extra_syms, pools.ticker, seen, article_ids)
        if blocks:
            ticker_sections.append(f"## Ticker Group: other_tickers\n\n{''.join(blocks)}")

    return "\n\n".join(channel_sections), "\n\n".join(ticker_sections), article_ids


def _ticker_blocks_for_symbols(
    symbols: list[str],
    ticker_pool: list[dict[str, Any]],
    seen: set[int],
    article_ids: list[str],
) -> list[str]:
    sym_set = {s.upper() for s in symbols}
    blocks: list[str] = []
    for article in ticker_pool:
        if not article_ticker_symbols(article) & sym_set:
            continue
        bid = _benzinga_id_int(article)
        if bid is None or bid in seen:
            continue
        seen.add(bid)
        article_ids.append(article_id(article))
        overlap = sorted(article_ticker_symbols(article) & sym_set)
        sym = overlap[0] if overlap else symbols[0]
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
    by_id, _, _ = _load_window_pools(outdir)
    return by_id


def channel_article_sets(
    outdir: Path, all_by_id: dict[str, dict[str, Any]] | None = None
) -> dict[str, list[dict[str, Any]]]:
    _by_id, pools, _ = _load_window_pools(outdir)
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
    _by_id, pools, _ = _load_window_pools(outdir)
    sym_set = {s.upper() for s in symbols}
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for article in pools.ticker:
        if not article_ticker_symbols(article) & sym_set:
            continue
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
