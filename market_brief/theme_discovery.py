"""Analyze market-brief routing gaps and build theme proposals."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_brief import config
from market_brief.ingest import (
    article_ticker_symbols,
    assign_articles_to_topic,
    topic_ticker_set,
)
from market_brief.themes_io import (
    all_curated_tickers,
    find_theme,
    load_themes,
    normalize_ticker,
    slugify_theme_name,
    theme_ticker_set,
)
from market_brief.topics import Topic, load_topics

# Phrases → suggested theme name (when tag cluster is weak).
NARRATIVE_PHRASES: list[tuple[str, str]] = [
    ("quantum computing", "Quantum Computing"),
    ("quantum computer", "Quantum Computing"),
    ("quantum hardware", "Quantum Computing"),
    ("quantum error", "Quantum Computing"),
    ("nuclear energy", "Nuclear Power"),
    ("small modular reactor", "Nuclear Power"),
    ("humanoid robot", "Robotics"),
    ("robotaxi", "Autonomous Vehicles"),
    ("weight loss drug", "GLP-1"),
    ("glp-1", "GLP-1"),
]

# Well-known pure-play tickers → default theme when they dominate a cluster.
TICKER_THEME_HINTS: dict[str, str] = {
    "IONQ": "Quantum Computing",
    "RGTI": "Quantum Computing",
    "QBTS": "Quantum Computing",
    "QUBT": "Quantum Computing",
    "SMR": "Nuclear Power",
    "OKLO": "Nuclear Power",
    "LEU": "Nuclear Power",
}


def find_latest_run_dir(outputs_dir: Path | None = None) -> Path | None:
    root = outputs_dir or config.OUTPUTS_DIR
    if not root.exists():
        return None
    candidates = sorted(
        [p for p in root.iterdir() if p.is_dir() and (p / "00_news").is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_run_articles(run_dir: Path) -> tuple[list[dict], list[dict], dict[str, Any] | None]:
    news_dir = run_dir / "00_news"
    unassigned_path = news_dir / "_unassigned.json"
    manifest_path = news_dir / "_manifest.json"
    unassigned: list[dict] = []
    if unassigned_path.exists():
        unassigned = json.loads(unassigned_path.read_text(encoding="utf-8"))
    manifest = None
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    corpus: list[dict] = list(unassigned)
    seen_ids: set[int] = set()
    for a in unassigned:
        bid = a.get("benzinga_id")
        if bid is not None:
            seen_ids.add(int(bid))
    for path in sorted(news_dir.glob("*.json")):
        if path.name.startswith("_"):
            continue
        rows = json.loads(path.read_text(encoding="utf-8"))
        for a in rows:
            bid = a.get("benzinga_id")
            if bid is not None and int(bid) not in seen_ids:
                corpus.append(a)
                seen_ids.add(int(bid))
    return corpus, unassigned, manifest


def _article_text(article: dict) -> str:
    parts = [
        article.get("title") or "",
        article.get("teaser") or "",
        article.get("text") or "",
        article.get("body_text") or "",
    ]
    return " ".join(parts).lower()


def narrative_signals(article: dict) -> set[str]:
    signals: set[str] = set()
    for raw in article.get("tags") or []:
        tag = str(raw).strip().lower()
        if tag and tag not in ("benznews",):
            signals.add(tag)
    text = _article_text(article)
    for phrase, _ in NARRATIVE_PHRASES:
        if phrase in text:
            signals.add(phrase)
    return signals


def suggested_theme_name(signal: str) -> str:
    low = signal.lower().strip()
    for phrase, name in NARRATIVE_PHRASES:
        if phrase in low or low in phrase:
            return name
    return low.replace("-", " ").title()


def covered_tickers(topics: list[Topic]) -> set[str]:
    out: set[str] = set()
    for topic in topics:
        out.update(topic_ticker_set(topic))
    return out


def analyze_unassigned(
    unassigned: list[dict],
    *,
    topics: list[Topic],
    themes: list[dict[str, Any]],
    min_articles: int = 2,
    min_tickers: int = 2,
) -> list[dict[str, Any]]:
    """Build proposals from stories that matched no theme/sector bucket."""
    covered = covered_tickers(topics)
    curated = all_curated_tickers(themes)

    clusters: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "articles": [],
            "tickers": Counter(),
            "signals": Counter(),
            "headlines": [],
        }
    )

    for article in unassigned:
        tickers = article_ticker_symbols(article)
        orphan = {t for t in tickers if t not in covered}
        if not orphan:
            continue
        signals = narrative_signals(article)
        cluster_key = "orphan_tickers"
        if signals:
            cluster_key = suggested_theme_name(max(signals, key=len))
        elif orphan:
            hinted = [TICKER_THEME_HINTS[t] for t in orphan if t in TICKER_THEME_HINTS]
            if hinted:
                cluster_key = Counter(hinted).most_common(1)[0][0]

        bucket = clusters[cluster_key]
        bucket["articles"].append(article)
        for t in orphan:
            bucket["tickers"][t] += 1
        for s in signals:
            bucket["signals"][s] += 1
        title = (article.get("title") or "").strip()
        if title and len(bucket["headlines"]) < 5:
            bucket["headlines"].append(title)

    proposals: list[dict[str, Any]] = []
    for theme_name, bucket in sorted(
        clusters.items(),
        key=lambda x: -len(x[1]["articles"]),
    ):
        if theme_name == "orphan_tickers":
            continue
        n_articles = len(bucket["articles"])
        if n_articles < min_articles:
            continue
        ranked_tickers = [t for t, _ in bucket["tickers"].most_common(12)]
        if len(ranked_tickers) < min_tickers:
            continue
        missing = [t for t in ranked_tickers if t not in curated]
        if not missing:
            continue

        existing = find_theme(themes, theme_name)
        pid = slugify_theme_name(theme_name)
        top_signals = [s for s, _ in bucket["signals"].most_common(4)]

        if existing:
            to_add = [t for t in missing if t not in theme_ticker_set(existing)]
            if not to_add:
                continue
            proposals.append(
                {
                    "id": f"add-{pid}",
                    "type": "add_tickers",
                    "approved": False,
                    "theme_name": existing.get("name") or theme_name,
                    "tickers": to_add,
                    "desc": "",
                    "evidence": {
                        "source": "unassigned_cluster",
                        "article_count": n_articles,
                        "signals": top_signals,
                        "sample_headlines": bucket["headlines"],
                        "reason": (
                            f"{n_articles} unassigned articles mention {theme_name}; "
                            f"tickers not in themes.json: {', '.join(to_add)}"
                        ),
                    },
                }
            )
        else:
            proposals.append(
                {
                    "id": f"new-{pid}",
                    "type": "new_theme",
                    "approved": False,
                    "name": theme_name,
                    "desc": _default_desc(theme_name, top_signals),
                    "tickers": missing,
                    "evidence": {
                        "source": "unassigned_cluster",
                        "article_count": n_articles,
                        "signals": top_signals,
                        "sample_headlines": bucket["headlines"],
                        "reason": (
                            f"{n_articles} unassigned articles cluster on "
                            f"{', '.join(top_signals) or theme_name}; "
                            f"no matching theme in themes.json"
                        ),
                    },
                }
            )

    return proposals


def analyze_routing_gaps(
    corpus: list[dict],
    topics: list[Topic],
    *,
    min_articles: int = 3,
) -> list[dict[str, Any]]:
    """Themes with zero routed articles but corpus mentions their tickers."""
    themes = load_themes()
    proposals: list[dict[str, Any]] = []
    content = [t for t in topics if t.kind in ("sector", "theme")]

    for theme in themes:
        name = (theme.get("name") or "").strip()
        if not name:
            continue
        topic = next((t for t in content if t.name.lower() == name.lower()), None)
        if not topic:
            continue
        routed = assign_articles_to_topic(corpus, topic)
        if routed:
            continue
        tickers = theme_ticker_set(theme)
        if not tickers:
            continue
        hits = [a for a in corpus if article_ticker_symbols(a) & tickers]
        if len(hits) < min_articles:
            continue
        headlines = [(a.get("title") or "").strip() for a in hits[:5] if a.get("title")]
        proposals.append(
            {
                "id": f"gap-{slugify_theme_name(name)}",
                "type": "add_tickers",
                "approved": False,
                "theme_name": name,
                "tickers": [],
                "desc": "",
                "evidence": {
                    "source": "zero_route",
                    "article_count": len(hits),
                    "sample_headlines": headlines,
                    "reason": (
                        f"Theme '{name}' has tickers in themes.json but 0 articles "
                        f"in 00_news/{slugify_theme_name(name)}.json — "
                        "stories may use tags without your tickers, or fetch window missed them."
                    ),
                },
            }
        )
    return proposals


def _default_desc(theme_name: str, signals: list[str]) -> str:
    if theme_name == "Quantum Computing":
        return (
            "Quantum hardware, error correction, government funding & foundry buildout"
        )
    if signals:
        return f"Auto-discovered: {', '.join(signals[:3])}"
    return ""


def build_proposals(
    run_dir: Path,
    *,
    min_articles: int | None = None,
    include_routing_gaps: bool = True,
) -> dict[str, Any]:
    min_n = min_articles if min_articles is not None else config.THEME_DISCOVERY_MIN_ARTICLES
    corpus, unassigned, manifest = load_run_articles(run_dir)
    topics = load_topics()
    themes = load_themes()

    from_unassigned = analyze_unassigned(
        unassigned,
        topics=topics,
        themes=themes,
        min_articles=min_n,
    )
    notes = (
        analyze_routing_gaps(corpus, topics, min_articles=min_n + 1)
        if include_routing_gaps
        else []
    )

    by_id: dict[str, dict[str, Any]] = {}
    for p in from_unassigned:
        by_id[p["id"]] = p

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_run": run_dir.name,
        "run_dir": str(run_dir),
        "manifest": manifest,
        "stats": {
            "corpus_articles": len(corpus),
            "unassigned_articles": len(unassigned),
            "curated_themes": len(themes),
            "topic_count": len(topics),
        },
        "proposals": list(by_id.values()),
        "notes": notes,
    }


def write_proposals(payload: dict[str, Any], out_path: Path | None = None) -> Path:
    discovery_dir = config.THEME_DISCOVERY_DIR
    discovery_dir.mkdir(parents=True, exist_ok=True)
    latest = out_path or (discovery_dir / "proposals.json")
    latest.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    dated = discovery_dir / f"proposals_{payload.get('source_run', 'run')}.json"
    if dated != latest:
        dated.write_text(latest.read_text(encoding="utf-8"), encoding="utf-8")
    return latest


def load_proposals(path: Path | None = None) -> dict[str, Any]:
    p = path or (config.THEME_DISCOVERY_DIR / "proposals.json")
    if not p.exists():
        return {"proposals": []}
    return json.loads(p.read_text(encoding="utf-8"))


def merge_web_proposals(
    payload: dict[str, Any],
    web_rows: list[dict[str, Any]],
    themes: list[dict[str, Any]],
) -> None:
    """Append Perplexity-suggested themes not already covered."""
    curated_names = {(t.get("name") or "").lower() for t in themes}
    existing_ids = {p["id"] for p in payload.get("proposals") or []}

    for row in web_rows:
        if not isinstance(row, dict):
            continue
        name = (row.get("name") or row.get("label") or "").strip()
        if not name or name.lower() in curated_names:
            continue
        tickers = [
            t
            for t in (normalize_ticker(x) for x in (row.get("tickers") or row.get("examples") or []))
            if t
        ]
        pid = f"web-{slugify_theme_name(name)}"
        if pid in existing_ids:
            continue
        payload.setdefault("proposals", []).append(
            {
                "id": pid,
                "type": "new_theme",
                "approved": False,
                "name": name,
                "desc": (row.get("description") or row.get("desc") or "").strip(),
                "tickers": tickers,
                "evidence": {
                    "source": "perplexity_hot_market",
                    "reason": row.get("description") or "Hot market narrative (web scan)",
                },
            }
        )
        existing_ids.add(pid)
