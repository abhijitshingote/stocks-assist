"""Load and deduplicate Benzinga articles from source/ snapshots."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

# Rough chars-per-token for batch sizing (conservative)
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


def load_articles_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return list(data.get("articles") or [])
    return []


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
    blocks = [format_article_block(a) for a in articles]
    return "\n---\n".join(blocks)


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
    """Single article block for Step 4 synthesis input."""
    title = (article.get("title") or "").strip()
    body = (
        article.get("body")
        or article.get("body_html")
        or article.get("body_text")
        or ""
    ).strip()
    ticker_part = ticker or _ticker_label(article)
    return f"[TICKER: {ticker_part}] [{channel}] {title}\n{body}\n---\n"


def all_ticker_symbols(source_dir: Path) -> list[str]:
    """Union of screener overview symbols and on-disk ``source/ticker/<SYM>/`` dirs."""
    syms: set[str] = set()
    overview_path = source_dir / "ticker_universe" / "overview.md"
    for _, batch in parse_ticker_batches_from_overview(overview_path):
        syms.update(batch)
    ticker_root = source_dir / "ticker"
    if ticker_root.is_dir():
        for d in ticker_root.iterdir():
            if d.is_dir():
                syms.add(d.name)
    return sorted(syms)


def audit_step4_source(
    source_dir: Path,
    *,
    channel_text: str | None = None,
    ticker_text: str | None = None,
) -> dict[str, Any]:
    """Coverage stats for logging — confirms ingest → Step 4 alignment."""
    general_n = len(load_articles_file(source_dir / "general" / "articles.json"))
    channel_n = 0
    channel_dir = source_dir / "channel"
    if channel_dir.is_dir():
        for slug_dir in channel_dir.iterdir():
            if slug_dir.is_dir():
                channel_n += len(load_articles_file(slug_dir / "articles.json"))
    ticker_n = 0
    ticker_syms = all_ticker_symbols(source_dir)
    for sym in ticker_syms:
        ticker_n += len(load_articles_file(source_dir / "ticker" / sym / "articles.json"))

    unique = load_all_source_articles(source_dir)
    if channel_text is None or ticker_text is None:
        channel_text, ticker_text = build_step4_summaries_text(source_dir)
    ch_text = channel_text or ""
    tk_text = ticker_text or ""
    blocks_ch = ch_text.count("---\n") if ch_text else 0
    blocks_tk = tk_text.count("---\n") if tk_text else 0

    ids_general_ch: set[int] = set()
    for path in [source_dir / "general" / "articles.json"] + list(
        (source_dir / "channel").glob("*/articles.json")
    ):
        for a in load_articles_file(path):
            bid = _benzinga_id_int(a)
            if bid is not None:
                ids_general_ch.add(bid)

    ticker_only = 0
    for sym in ticker_syms:
        for a in load_articles_file(source_dir / "ticker" / sym / "articles.json"):
            bid = _benzinga_id_int(a)
            if bid is not None and bid not in ids_general_ch:
                ticker_only += 1

    manifest: dict[str, Any] = {}
    manifest_path = source_dir / "_manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            manifest = {}

    article_chars = len(ch_text) + len(tk_text)
    return {
        "general_rows": general_n,
        "channel_rows": channel_n,
        "ticker_rows": ticker_n,
        "raw_rows_total": general_n + channel_n + ticker_n,
        "unique_benzinga_ids": len(unique),
        "step4_blocks_channel": blocks_ch,
        "step4_blocks_ticker": blocks_tk,
        "step4_blocks_total": blocks_ch + blocks_tk,
        "ticker_only_articles": ticker_only,
        "ticker_symbols": len(ticker_syms),
        "coverage_ok": len(unique) == blocks_ch + blocks_tk,
        "article_chars": article_chars,
        "estimated_tokens_chars_per_3_5": estimate_tokens(ch_text + tk_text),
        "general_window": manifest.get("general_window"),
        "ticker_window": manifest.get("ticker_window"),
    }


def build_step4_summaries_text(source_dir: Path) -> tuple[str, str]:
    """Build channel_summaries and ticker_summaries from raw source/ articles.

    Articles are deduped globally by benzinga_id (ingest order: general → channels → tickers).
    Each article appears once: channel/general first, then ticker-only extras.
    """
    seen: set[int] = set()
    channel_sections: list[str] = []

    general_path = source_dir / "general" / "articles.json"
    general_blocks: list[str] = []
    for article in dedupe_articles(load_articles_file(general_path)):
        bid = _benzinga_id_int(article)
        if bid is None or bid in seen:
            continue
        seen.add(bid)
        general_blocks.append(
            format_brief_article_block(
                article,
                channel="general",
                ticker=_ticker_label(article),
            )
        )
    if general_blocks:
        channel_sections.append(f"## Channel: general\n\n{''.join(general_blocks)}")

    channel_dir = source_dir / "channel"
    if channel_dir.is_dir():
        for slug_dir in sorted(channel_dir.iterdir()):
            if not slug_dir.is_dir():
                continue
            slug = slug_dir.name
            blocks: list[str] = []
            for article in dedupe_articles(load_articles_file(slug_dir / "articles.json")):
                bid = _benzinga_id_int(article)
                if bid is None or bid in seen:
                    continue
                seen.add(bid)
                blocks.append(
                    format_brief_article_block(
                        article,
                        channel=slug,
                        ticker=_ticker_label(article),
                    )
                )
            if blocks:
                channel_sections.append(f"## Channel: {slug}\n\n{''.join(blocks)}")

    ticker_sections: list[str] = []
    overview_path = source_dir / "ticker_universe" / "overview.md"
    batched: set[str] = set()
    for batch_label, symbols in parse_ticker_batches_from_overview(overview_path):
        batched.update(symbols)
        blocks = _ticker_blocks_for_symbols(source_dir, symbols, seen)
        if blocks:
            ticker_sections.append(f"## Ticker Group: {batch_label}\n\n{''.join(blocks)}")

    extra_syms = [s for s in all_ticker_symbols(source_dir) if s not in batched]
    if extra_syms:
        blocks = _ticker_blocks_for_symbols(source_dir, extra_syms, seen)
        if blocks:
            ticker_sections.append(f"## Ticker Group: other_tickers\n\n{''.join(blocks)}")

    return "\n\n".join(channel_sections), "\n\n".join(ticker_sections)


def _ticker_blocks_for_symbols(
    source_dir: Path,
    symbols: list[str],
    seen: set[int],
) -> list[str]:
    blocks: list[str] = []
    for sym in symbols:
        path = source_dir / "ticker" / sym / "articles.json"
        for article in dedupe_articles(load_articles_file(path)):
            bid = _benzinga_id_int(article)
            if bid is None or bid in seen:
                continue
            seen.add(bid)
            ch = article.get("channels") or []
            channel_label = ", ".join(str(c) for c in ch) if ch else sym
            blocks.append(
                format_brief_article_block(
                    article,
                    channel=channel_label,
                    ticker=sym,
                )
            )
    return blocks


def estimate_tokens(text: str) -> int:
    return int(len(text) / CHARS_PER_TOKEN)


def load_all_source_articles(source_dir: Path) -> dict[str, dict[str, Any]]:
    """Return deduped article dict keyed by id from all source pulls."""
    by_id: dict[str, dict[str, Any]] = {}

    def _merge(batch: list[dict[str, Any]]) -> None:
        for a in batch:
            try:
                by_id[article_id(a)] = a
            except ValueError:
                continue

    general_path = source_dir / "general" / "articles.json"
    _merge(load_articles_file(general_path))

    channel_dir = source_dir / "channel"
    if channel_dir.is_dir():
        for slug_dir in sorted(channel_dir.iterdir()):
            if slug_dir.is_dir():
                _merge(load_articles_file(slug_dir / "articles.json"))

    ticker_dir = source_dir / "ticker"
    if ticker_dir.is_dir():
        for sym_dir in sorted(ticker_dir.iterdir()):
            if sym_dir.is_dir():
                _merge(load_articles_file(sym_dir / "articles.json"))

    return by_id


def channel_article_sets(
    source_dir: Path, all_by_id: dict[str, dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    """Articles per channel slug (deduped within channel file)."""
    result: dict[str, list[dict[str, Any]]] = {}

    general_path = source_dir / "general" / "articles.json"
    general = dedupe_articles(load_articles_file(general_path))
    if general:
        result["general"] = general

    channel_dir = source_dir / "channel"
    if channel_dir.is_dir():
        for slug_dir in sorted(channel_dir.iterdir()):
            if not slug_dir.is_dir():
                continue
            slug = slug_dir.name
            arts = dedupe_articles(load_articles_file(slug_dir / "articles.json"))
            if arts:
                result[slug] = arts

    return result


def channel_output_filename(slug: str) -> str:
    return f"channel_{slug}.md"


def parse_ticker_batches_from_overview(overview_path: Path) -> list[tuple[str, list[str]]]:
    """Group tickers into ~4 batches using screener screen from overview.md."""
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
        # Fallback: parse section tables before master list
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
    source_dir: Path,
    symbols: list[str],
    all_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Collect ticker-pull articles for symbols (deduped by id)."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for sym in symbols:
        path = source_dir / "ticker" / sym / "articles.json"
        for a in dedupe_articles(load_articles_file(path)):
            try:
                aid = article_id(a)
            except ValueError:
                continue
            if aid in seen:
                continue
            seen.add(aid)
            out.append(a)
    return out


def split_oversized_batch(
    label: str, articles: list[dict[str, Any]]
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Split article list if concatenated text exceeds token budget."""
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
