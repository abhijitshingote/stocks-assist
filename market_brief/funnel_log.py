"""Optional per-run QA funnel report (``qa_funnel.md``).

Enable with ``--qa-log`` on ``market_brief.run`` or ``MARKET_BRIEF_QA_LOG=1``.
Default off to avoid growing user_data storage during normal runs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class FetchSlice:
    label: str
    api_rows: int = 0
    after_filter: int = 0
    new_unique_ids: int = 0
    error: str | None = None


@dataclass
class IngestFunnelData:
    since_iso: str = ""
    end_iso: str = ""
    window_label: str = ""
    anchor_session: str = ""
    window_hours: int = 0  # legacy; unused (trading-day window)
    universe_size: int = 0
    universe_sample: list[str] = field(default_factory=list)
    purge_count: int = 0
    general: FetchSlice | None = None
    channels: list[FetchSlice] = field(default_factory=list)
    ticker_ok: int = 0
    ticker_failed: list[str] = field(default_factory=list)
    ticker_article_counts: dict[str, int] = field(default_factory=dict)
    raw_rows: int = 0
    dropped_no_benzinga_id: int = 0
    unique_articles: int = 0
    db_upserted: int = 0
    db_loaded: int = 0
    topic_assignment: dict[str, int] = field(default_factory=dict)
    macro_count: int = 0  # legacy alias; same as unassigned_count
    unassigned_count: int = 0
    assigned_content_ids: int = 0
    tickers_fetched_zero_articles: list[str] = field(default_factory=list)
    channel_tags_on_corpus: dict[str, int] = field(default_factory=dict)


@dataclass
class SummarizeFunnelRow:
    topic_name: str
    kind: str
    articles_in: int
    chunks: int = 0
    elapsed_s: float = 0.0
    error: str | None = None
    content_chars: int = 0


@dataclass
class FunnelReport:
    """Accumulates funnel metrics and writes ``qa_funnel.md``."""

    asof: str
    outdir: Path
    started: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ingest: IngestFunnelData | None = None
    summarize_rows: list[SummarizeFunnelRow] = field(default_factory=list)
    watch: dict[str, Any] = field(default_factory=dict)
    synth: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)


def _md_table(headers: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return "_(none)_\n"
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines) + "\n"


def write_funnel_md(report: FunnelReport, path: Path) -> None:
    finished = datetime.now(timezone.utc)
    parts: list[str] = [
        f"# Market Brief QA Funnel — {report.asof}",
        "",
        f"- **Started (UTC):** {report.started.isoformat()}",
        f"- **Finished (UTC):** {finished.isoformat()}",
        f"- **Output dir:** `{report.outdir}`",
        "",
    ]

    if report.errors:
        parts.append("## Errors\n")
        for e in report.errors:
            parts.append(f"- {e}")
        parts.append("")

    ing = report.ingest
    if ing:
        parts.extend(
            [
                "## 1. Ingest",
                "",
                f"- **Window:** {ing.window_label or ing.since_iso}",
                f"- **Anchor session (5:00 AM ET):** `{ing.anchor_session}`",
                f"- **End (UTC):** `{ing.end_iso}`",
                f"- **DB purge (rows deleted):** {ing.purge_count}",
                f"- **Ticker universe:** {ing.universe_size} symbols",
                "",
            ]
        )
        if ing.universe_sample:
            parts.append(
                f"Sample tickers: `{', '.join(ing.universe_sample[:20])}`"
                + (" …" if ing.universe_size > 20 else "")
                + "\n"
            )

        parts.append("### API pulls\n")
        fetch_rows: list[list[Any]] = []
        if ing.general:
            g = ing.general
            fetch_rows.append(
                [
                    g.label,
                    g.api_rows,
                    g.after_filter,
                    g.new_unique_ids,
                    g.error or "",
                ]
            )
        for ch in ing.channels:
            fetch_rows.append(
                [
                    ch.label,
                    ch.api_rows,
                    ch.after_filter,
                    ch.new_unique_ids,
                    ch.error or "",
                ]
            )
        parts.append(
            _md_table(
                ["source", "api_rows", "after_filter", "new_ids", "error"],
                fetch_rows,
            )
        )
        parts.append(
            f"- **Ticker API calls:** {ing.ticker_ok} ok"
            + (f", {len(ing.ticker_failed)} failed" if ing.ticker_failed else "")
            + "\n"
        )
        if ing.ticker_failed:
            parts.append(f"- Failed tickers: `{', '.join(ing.ticker_failed[:30])}`\n")

        parts.extend(
            [
                "### Merge & dedupe\n",
                f"- **Raw rows appended:** {ing.raw_rows}",
                f"- **Dropped (no benzinga_id):** {ing.dropped_no_benzinga_id}",
                f"- **Unique articles (by benzinga_id):** {ing.unique_articles}",
                f"- **DB upserted:** {ing.db_upserted}",
                f"- **DB loaded (published >= since):** {ing.db_loaded}",
                "",
            ]
        )

        zero = ing.tickers_fetched_zero_articles
        parts.append(f"### Tickers with zero articles in final DB window ({len(zero)})\n")
        if zero:
            parts.append(
                f"`{', '.join(zero[:40])}`"
                + (f" … +{len(zero) - 40} more" if len(zero) > 40 else "")
                + "\n\n"
            )
        else:
            parts.append("_(none)_\n\n")

        parts.append("### Topic assignment (`00_news/*.json`)\n")
        assign_rows = sorted(
            ing.topic_assignment.items(), key=lambda x: (-x[1], x[0])
        )
        parts.append(_md_table(["topic", "articles"], [[k, v] for k, v in assign_rows]))
        parts.append(
            f"- **Unassigned (`_unassigned.json`):** {ing.unassigned_count} "
            f"(no theme/sector match; always summarized when > 0)\n"
        )
        parts.append(
            f"- **Distinct articles assigned to a theme/sector:** "
            f"{ing.assigned_content_ids}\n"
        )

        if ing.channel_tags_on_corpus:
            parts.append("### Channel tags on final corpus (metadata)\n")
            tag_rows = sorted(
                ing.channel_tags_on_corpus.items(), key=lambda x: (-x[1], x[0])
            )[:25]
            parts.append(_md_table(["channel", "count"], [[k, v] for k, v in tag_rows]))

        low = [(k, v) for k, v in assign_rows if v == 0]
        if low:
            parts.append(
                f"\n**Topics with 0 assigned articles ({len(low)}):** "
                + ", ".join(k for k, _ in low)
                + "\n"
            )
            parts.append(
                "_Assignment uses normalized article `tickers` ∩ topic set. "
                "Stories with no theme/sector match go to `_unassigned.json` and are "
                "always summarized when count > 0._\n"
            )

    if report.summarize_rows:
        parts.append("## 2. Summarize (Perplexity / Benzinga bodies)\n")
        parts.append(
            _md_table(
                ["topic", "kind", "articles", "chunks", "chars", "sec", "error"],
                [
                    [
                        r.topic_name,
                        r.kind,
                        r.articles_in,
                        r.chunks,
                        r.content_chars,
                        f"{r.elapsed_s:.1f}",
                        r.error or "",
                    ]
                    for r in report.summarize_rows
                ],
            )
        )

    if report.watch:
        parts.extend(
            [
                "## 3. Watch probe\n",
                f"- **Elapsed:** {report.watch.get('elapsed_s', '—')}s",
                f"- **Error:** {report.watch.get('error') or '—'}",
                f"- **Content chars:** {report.watch.get('content_chars', 0)}",
                "",
            ]
        )

    if report.synth:
        parts.extend(
            [
                "## 4. Synthesize\n",
                f"- **Brief MD chars:** {report.synth.get('brief_md_chars', 0)}",
                f"- **JSON parse error:** {report.synth.get('json_parse_error', False)}",
                "",
            ]
        )

    if report.usage:
        parts.extend(
            [
                "## 5. Usage (Perplexity)\n",
                f"- **Cost USD:** {report.usage.get('cost_usd_total', 0):.4f}",
                f"- **Prompt tokens:** {report.usage.get('total_prompt_tokens', 0)}",
                f"- **Completion tokens:** {report.usage.get('total_completion_tokens', 0)}",
                "",
            ]
        )

    parts.append(
        "---\n_Auto-generated QA log. Disable with default run (no `--qa-log`)._\n"
    )
    path.write_text("".join(parts), encoding="utf-8")
