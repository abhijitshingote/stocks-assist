"""Stages 2-4: news per candidate, grouping into themes, probe per theme."""

from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sqlalchemy import create_engine, text

from daily_screener.utils.claude import extract_json
from daily_screener.utils.io import read_json_if_exists, write_json
from daily_screener.utils.perplexity import call_perplexity
from market_brief.anthropic_client import complete
from market_brief.cost_tracker import CostTracker
from theme_discovery import config

logger = logging.getLogger(__name__)


def _prompt(name: str) -> str:
    return (config.PROMPTS_DIR / f"{name}.md").read_text(encoding="utf-8")


def _pct(v) -> str:
    return "n/a" if v is None else f"{v:+.1f}%"


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:60] or "theme"


# ---------------------------------------------------------------------------
# Stage 2: news
# ---------------------------------------------------------------------------

def _news_one(c: dict, today: str, outdir: Path, force: bool) -> dict:
    path = outdir / "02_news" / f"{c['ticker']}.json"
    if not force and (cached := read_json_if_exists(path)):
        return cached
    prompt = _prompt("news").format(
        today=today,
        ticker=c["ticker"],
        company_name=c.get("company_name") or "",
        sector=c.get("sector") or "",
        industry=c.get("industry") or "",
        cap_bucket=c.get("cap_bucket") or "",
        dr_5=_pct(c.get("dr_5")),
        dr_20=_pct(c.get("dr_20")),
        dr_60=_pct(c.get("dr_60")),
    )
    out = {"ticker": c["ticker"]}
    try:
        raw = call_perplexity(
            prompt,
            model=config.NEWS_MODEL,
            max_tokens=config.NEWS_MAX_TOKENS,
            timeout=150,
            log_label=f"news {c['ticker']}",
        )
        out["raw"] = raw
        try:
            out.update(extract_json(raw))
        except ValueError as e:
            out["parse_error"] = str(e)
            out["write_up"] = raw
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)
    write_json(path, out)
    return out


def run_news(candidates: list[dict], today: str, outdir: Path, force: bool = False) -> dict:
    with ThreadPoolExecutor(max_workers=config.NEWS_CONCURRENCY) as ex:
        results = list(ex.map(lambda c: _news_one(c, today, outdir, force), candidates))
    news = {r["ticker"]: r for r in results}
    write_json(outdir / "02_news.json", news)
    return news


# ---------------------------------------------------------------------------
# Stage 3: group into themes (one Opus call)
# ---------------------------------------------------------------------------

def _stock_block(c: dict, n: dict) -> str:
    lines = [
        f"### {c['ticker']} — {c.get('company_name') or ''}",
        f"{c.get('industry')} | {c.get('cap_bucket')} | 1w {_pct(c.get('dr_5'))}, "
        f"1m {_pct(c.get('dr_20'))}, 3m {_pct(c.get('dr_60'))} | screeners: {', '.join(c.get('picked_by') or [])}",
    ]
    if n.get("error"):
        lines.append(f"(news lookup failed: {n['error']})")
        return "\n".join(lines)
    if n.get("one_line"):
        lines.append(f"Why: {n['one_line']}")
    for d in n.get("drivers") or []:
        lines.append(f"- {d.get('date', '')} [{d.get('kind', '')}] {d.get('what', '')}")
    if n.get("read_through"):
        lines.append(f"Read-through: {n['read_through']}")
    if n.get("write_up"):
        lines.append(f"Detail: {n['write_up']}")
    return "\n".join(lines)


def run_synthesis(candidates: list[dict], news: dict, outdir: Path, tracker: CostTracker) -> dict:
    block = "\n\n".join(_stock_block(c, news.get(c["ticker"], {})) for c in candidates)
    prompt = _prompt("synthesize").format(min_stocks=config.MIN_STOCKS_PER_THEME, stocks_block=block)
    (outdir / "03_synth_input.md").write_text(prompt, encoding="utf-8")
    raw = complete(
        model=config.SYNTH_MODEL,
        logical_model=config.SYNTH_LOGICAL,
        system="You group stocks into market themes. Output strict JSON only.",
        user_message=prompt,
        step="synthesize",
        tracker=tracker,
        max_tokens=config.SYNTH_MAX_TOKENS,
        use_stream=True,
        pace_after=False,
    )
    (outdir / "03_synth_response.txt").write_text(raw, encoding="utf-8")
    data = extract_json(raw)

    known = {c["ticker"] for c in candidates}
    themes = []
    for t in data.get("themes") or []:
        stocks = [s for s in t.get("stocks") or [] if s.get("ticker") in known]
        if len(stocks) < config.MIN_STOCKS_PER_THEME:
            logger.warning("dropping theme %r: %d valid stocks", t.get("name"), len(stocks))
            continue
        t["stocks"] = stocks
        t["slug"] = _slug(t.get("name") or "")
        themes.append(t)
    in_theme = {s["ticker"] for t in themes for s in t["stocks"]}
    qa = [q for q in data.get("qa") or [] if q.get("ticker") in known and q["ticker"] not in in_theme]
    qa_tickers = {q["ticker"] for q in qa}
    for t in sorted(known - in_theme - qa_tickers):
        qa.append({"ticker": t, "reason": (news.get(t) or {}).get("one_line", ""), "why_not_theme": "not placed by the model"})
    result = {"themes": themes, "qa": qa}
    write_json(outdir / "03_themes.json", result)
    return result


# ---------------------------------------------------------------------------
# Stage 4: probe each theme for other stocks
# ---------------------------------------------------------------------------

def _lookup_tickers(tickers: list[str]) -> dict[str, dict]:
    if not tickers:
        return {}
    engine = create_engine(os.environ["DATABASE_URL"])
    sql = text(
        "SELECT ticker, company_name, sector, industry, market_cap, dr_5, dr_20, dr_60 "
        "FROM stock_metrics WHERE ticker = ANY(:t)"
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"t": tickers}).mappings().all()
    return {r["ticker"]: dict(r) for r in rows}


def _cap_bucket(mcap) -> str | None:
    if mcap is None:
        return None
    if mcap >= 100e9:
        return "mega"
    if mcap >= 20e9:
        return "large"
    if mcap >= 2e9:
        return "mid"
    if mcap >= 2e8:
        return "small"
    return "micro"


def _probe_one(theme: dict, today: str, outdir: Path, force: bool) -> dict:
    path = outdir / "04_probe" / f"{theme['slug']}.json"
    if not force and (cached := read_json_if_exists(path)):
        return cached
    known = [s["ticker"] for s in theme["stocks"]]
    prompt = _prompt("probe").format(
        today=today,
        name=theme.get("name", ""),
        description=theme.get("description", ""),
        secular=theme.get("secular", ""),
        known=", ".join(known),
        max_added=config.PROBE_MAX_ADDED,
    )
    out = {"slug": theme["slug"], "name": theme.get("name")}
    try:
        raw = call_perplexity(
            prompt,
            model=config.PROBE_MODEL,
            max_tokens=config.PROBE_MAX_TOKENS,
            timeout=180,
            log_label=f"probe {theme['slug']}",
        )
        out["raw"] = raw
        out["proposed"] = extract_json(raw).get("stocks") or []
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)
        out["proposed"] = []

    proposed = [
        p for p in out["proposed"]
        if (p.get("ticker") or "").upper() and p["ticker"].upper() not in known
    ]
    meta = _lookup_tickers([p["ticker"].upper() for p in proposed])
    added, rejected = [], []
    for p in proposed:
        t = p["ticker"].upper()
        m = meta.get(t)
        if not m:
            rejected.append({**p, "ticker": t, "reject_reason": "not in stock_metrics"})
        elif (m.get("market_cap") or 0) < config.PROBE_MIN_MARKET_CAP:
            rejected.append({**p, "ticker": t, "reject_reason": "below mid cap"})
        elif len(added) >= config.PROBE_MAX_ADDED:
            rejected.append({**p, "ticker": t, "reject_reason": f"over limit {config.PROBE_MAX_ADDED}"})
        else:
            added.append({
                "ticker": t,
                "why": p.get("why", ""),
                "source": p.get("source", ""),
                "company_name": m.get("company_name"),
                "industry": m.get("industry"),
                "cap_bucket": _cap_bucket(m.get("market_cap")),
                "dr_5": m.get("dr_5"),
                "dr_20": m.get("dr_20"),
                "dr_60": m.get("dr_60"),
            })
    out["added"] = added
    out["rejected"] = rejected
    write_json(path, out)
    return out


def run_probe(themes: list[dict], today: str, outdir: Path, force: bool = False) -> dict:
    with ThreadPoolExecutor(max_workers=config.PROBE_CONCURRENCY) as ex:
        results = list(ex.map(lambda t: _probe_one(t, today, outdir, force), themes))
    return {r["slug"]: r for r in results}
