"""Write run artifacts to disk."""

from __future__ import annotations

import re
from pathlib import Path

from market_brief.types import ProbeResult

SNIPPETS_SUBDIR = "_snippets"
SNIPPET_MARKER = "## Snippet (Ollama)\n\n"
PRICE_MARKER = "## Price reference\n\n"
SOURCE_MARKER = "## Source article\n\n"


def _slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "topic"


def persist_summaries(results: list[ProbeResult], outdir: Path) -> None:
    summaries_dir = outdir / "01_summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    for r in results:
        slug = _slugify(r.topic_name)
        fname = f"{slug}__{r.kind}.md"
        backend = (
            "ollama"
            if (outdir / "01_summaries" / f"{slug}__benzinga.ollama.done").is_file()
            else "perplexity"
        )
        body = (
            f"# {r.topic_name} — {r.kind}\n\n"
            f"_kind: {r.topic_kind} · backend: {backend} · elapsed: {r.elapsed_s:.1f}s_\n\n"
        )
        if r.error:
            body += f"> **ERROR**: {r.error}\n\n"
        body += r.content or "_(no content)_\n"
        (summaries_dir / fname).write_text(body, encoding="utf-8")


def snippet_path(outdir: Path, benzinga_id: int) -> Path:
    return outdir / "01_summaries" / SNIPPETS_SUBDIR / f"{benzinga_id}.md"


def article_source_body(article: dict, *, max_chars: int = 12_000) -> str:
    """Original Benzinga body for QA diff against the model snippet."""
    body = (article.get("body_text") or article.get("teaser") or "").strip()
    if not body:
        return "_(no body in article record)_"
    if len(body) > max_chars:
        return body[:max_chars] + "\n\n[truncated for QA file]"
    return body


def _split_snippet_file(text: str) -> tuple[str | None, str | None, str | None]:
    """Return (fundamental snippet, price block, source body) from a QA snippet file."""
    if SNIPPET_MARKER not in text:
        return None, None, None
    after = text.split(SNIPPET_MARKER, 1)[1]
    price_block: str | None = None
    if PRICE_MARKER in after:
        snippet_part, price_block = after.split(PRICE_MARKER, 1)
        price_block = price_block.strip() or None
    else:
        snippet_part = after
    if SOURCE_MARKER in snippet_part:
        snippet_part, source = snippet_part.split(SOURCE_MARKER, 1)
        return snippet_part.strip() or None, price_block, source.strip() or None
    return snippet_part.strip() or None, price_block, None


def load_article_snippet(outdir: Path, benzinga_id: int) -> str | None:
    """Fundamental Ollama snippet only (excludes price reference and source)."""
    path = snippet_path(outdir, benzinga_id)
    if not path.is_file():
        return None
    text = path.read_text(encoding="utf-8").strip()
    snippet, _, _ = _split_snippet_file(text)
    if snippet:
        return snippet
    if text.startswith("<!--") and "\n\n" in text:
        text = text.split("\n\n", 1)[1].strip()
    return text or None


def persist_article_snippet(
    outdir: Path,
    article: dict,
    snippet: str,
    *,
    price_reference: str = "",
) -> None:
    bid = article.get("benzinga_id")
    if bid is None:
        return
    path = snippet_path(outdir, int(bid))
    path.parent.mkdir(parents=True, exist_ok=True)
    title = article.get("title") or "(untitled)"
    url = article.get("url") or ""
    pub = article.get("published") or article.get("published_date") or ""
    header = (
        f"<!-- benzinga_id={bid} title={title!r} -->\n\n"
        f"_published: {pub}_"
    )
    if url:
        header += f" · {url}"
    header += "\n\n"
    body = header + SNIPPET_MARKER + snippet.strip() + "\n\n"
    if price_reference.strip():
        pr = price_reference.strip()
        if not pr.startswith("## Price reference"):
            pr = PRICE_MARKER + pr
        body += pr + "\n\n"
    body += SOURCE_MARKER + article_source_body(article) + "\n"
    path.write_text(body, encoding="utf-8")


def persist_topic_partial(
    outdir: Path,
    topic_name: str,
    topic_kind: str,
    content: str,
    *,
    articles_done: int,
    articles_total: int,
) -> None:
    """Write in-progress topic markdown so quality can be reviewed mid-run."""
    summaries_dir = outdir / "01_summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    slug = _slugify(topic_name)
    fname = f"{slug}__benzinga.partial.md"
    body = (
        f"# {topic_name} — benzinga (in progress {articles_done}/{articles_total})\n\n"
        f"_kind: {topic_kind} · backend: ollama · partial — snippet + source per article for QA_\n\n"
    )
    body += content or "_(no snippets yet)_\n"
    (summaries_dir / fname).write_text(body, encoding="utf-8")


def topic_summarize_complete_marker(outdir: Path, topic_name: str) -> Path:
    return outdir / "01_summaries" / f"{_slugify(topic_name)}__benzinga.ollama.done"


def is_topic_ollama_complete(outdir: Path, topic_name: str) -> bool:
    return topic_summarize_complete_marker(outdir, topic_name).is_file()


def mark_topic_ollama_complete(outdir: Path, topic_name: str) -> None:
    topic_summarize_complete_marker(outdir, topic_name).write_text("", encoding="utf-8")
