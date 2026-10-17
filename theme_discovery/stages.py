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


def load_prior_themes(current_run: str) -> dict[str, dict]:
    """Themes from the last PRIOR_RUNS runs before current_run, keyed by lowercased name."""
    if not config.OUTPUTS_DIR.is_dir():
        return {}
    runs = sorted(
        d.name for d in config.OUTPUTS_DIR.iterdir()
        if d.is_dir() and d.name < current_run and (d / "themes.json").is_file()
    )[-config.PRIOR_RUNS:]
    prior: dict[str, dict] = {}
    for run in runs:
        data = read_json_if_exists(config.OUTPUTS_DIR / run / "themes.json") or {}
        for t in data.get("themes") or []:
            key = (t.get("name") or "").strip().lower()
            if not key:
                continue
            p = prior.setdefault(key, {
                "name": t["name"],
                "first_seen": t.get("first_seen") or run,
                "tickers": [],
            })
            p["description"] = t.get("description")
            p["last_seen"] = run
            p["first_seen"] = min(p["first_seen"], t.get("first_seen") or run)
            for s in t.get("stocks") or []:
                if s["ticker"] not in p["tickers"]:
                    p["tickers"].append(s["ticker"])
    return prior


def _prior_block(prior: dict[str, dict]) -> str:
    if not prior:
        return "(none yet)"
    return "\n".join(
        f"- {p['name']} — {p.get('description') or ''} — {', '.join(p['tickers'])} "
        f"(first seen {p['first_seen']}, last seen {p['last_seen']})"
        for p in prior.values()
    )


def _opus(prompt: str, step: str, outdir: Path, tracker: CostTracker) -> dict:
    (outdir / f"03_{step}_input.md").write_text(prompt, encoding="utf-8")
    raw = complete(
        model=config.SYNTH_MODEL,
        logical_model=config.SYNTH_LOGICAL,
        system="You group stocks into market themes. Output strict JSON only.",
        user_message=prompt,
        step=step,
        tracker=tracker,
        max_tokens=config.SYNTH_MAX_TOKENS,
        use_stream=True,
        pace_after=False,
    )
    (outdir / f"03_{step}_response.txt").write_text(raw, encoding="utf-8")
    return extract_json(raw)


def dedupe_pass2(themes: dict[str, dict]) -> dict[str, dict]:
    """Drop a pass-2 theme whose stock set equals an earlier theme's (a rename, not a new theme)."""
    seen: set[frozenset] = set()
    out = {}
    for key, t in sorted(themes.items(), key=lambda kv: kv[1].get("found_in") != "pass1"):
        tickers = frozenset(s["ticker"] for s in t["stocks"])
        if t.get("found_in") == "pass2" and tickers in seen:
            logger.info("dropping pass-2 duplicate %r", t["name"])
            continue
        seen.add(tickers)
        out[key] = t
    return out


def _add_stocks(theme: dict, stocks: list[dict], known: set[str]) -> None:
    have = {s["ticker"] for s in theme["stocks"]}
    for s in stocks:
        t = s.get("ticker")
        if t in known and t not in have:
            theme["stocks"].append({"ticker": t, "why": s.get("why", "")})
            have.add(t)


def run_synthesis(
    candidates: list[dict], news: dict, outdir: Path, tracker: CostTracker, prior: dict[str, dict], run: str,
) -> dict:
    known = {c["ticker"] for c in candidates}
    stocks_block = "\n\n".join(_stock_block(c, news.get(c["ticker"], {})) for c in candidates)
    prior_block = _prior_block(prior)
    themes: dict[str, dict] = {}

    def merge(t: dict, source: str) -> None:
        name = (t.get("name") or "").strip()
        if not name:
            return
        key = name.lower()
        if key not in themes:
            themes[key] = {
                "name": name,
                "description": t.get("description"),
                "secular": t.get("secular"),
                "layer": t.get("layer"),
                "found_in": source,
                "single": bool(t.get("single")),
                "stocks": [],
            }
        _add_stocks(themes[key], t.get("stocks") or [], known)

    p1 = _opus(
        _prompt("synthesize").format(
            min_stocks=config.MIN_STOCKS_PER_THEME, prior_block=prior_block, stocks_block=stocks_block,
        ),
        "synth_pass1", outdir, tracker,
    )
    for t in p1.get("themes") or []:
        merge(t, "pass1")

    found_block = "\n".join(
        f"- {t['name']} — {', '.join(s['ticker'] for s in t['stocks'])}" for t in themes.values()
    ) or "(none)"
    p2 = _opus(
        _prompt("synthesize_layers").format(
            min_stocks=config.MIN_STOCKS_PER_THEME, prior_block=prior_block,
            found_block=found_block, stocks_block=stocks_block,
        ),
        "synth_pass2", outdir, tracker,
    )
    for t in p2.get("new_themes") or []:
        merge(t, "pass2")
    for a in p2.get("additions") or []:
        key = (a.get("theme") or "").strip().lower()
        if key in themes:
            _add_stocks(themes[key], [a], known)

    kept = []
    for key, t in dedupe_pass2(themes).items():
        n = len(t["stocks"])
        p = prior.get(key)
        t["previous"] = p is not None
        t["first_seen"] = p["first_seen"] if p else run
        if n >= config.MIN_STOCKS_PER_THEME:
            t["kind"] = "multi"
        elif n == 1 and p:
            t["kind"] = "previous_single"
        elif n == 1 and t["single"]:
            t["kind"] = "single"
        else:
            logger.warning("dropping theme %r: %d valid stocks", t["name"], n)
            continue
        t["slug"] = _slug(t["name"])
        kept.append(t)

    in_theme = {s["ticker"] for t in kept for s in t["stocks"]}
    qa = [q for q in p1.get("qa") or [] if q.get("ticker") in known and q["ticker"] not in in_theme]
    qa_tickers = {q["ticker"] for q in qa}
    for t in sorted(known - in_theme - qa_tickers):
        qa.append({"ticker": t, "reason": (news.get(t) or {}).get("one_line", ""), "why_not_theme": "not placed by the model"})
    result = {"themes": kept, "qa": qa}
    write_json(outdir / "03_themes.json", result)
    return result


def confirm_singles(synth: dict, probes: dict, news: dict) -> dict:
    """Keep a one-stock theme only if its probe found enough added stocks that are also up."""
    def is_up(a: dict) -> bool:
        return (a.get("dr_20") or 0) >= config.SINGLE_CONFIRM_DR_20 or (a.get("dr_60") or 0) >= config.SINGLE_CONFIRM_DR_60

    kept, failed = [], []
    for t in synth["themes"]:
        if t["kind"] == "single":
            up = [a["ticker"] for a in probes.get(t["slug"], {}).get("added", []) if is_up(a)]
            t["confirm_up"] = up
            if len(up) < config.SINGLE_CONFIRM_MIN:
                failed.append(t)
                continue
        kept.append(t)

    in_theme = {s["ticker"] for t in kept for s in t["stocks"]}
    qa = [q for q in synth["qa"] if q["ticker"] not in in_theme]
    qa_tickers = {q["ticker"] for q in qa}
    for t in failed:
        tk = t["stocks"][0]["ticker"]
        if tk in in_theme or tk in qa_tickers:
            continue
        qa.append({
            "ticker": tk,
            "reason": (news.get(tk) or {}).get("one_line", ""),
            "why_not_theme": f"one-stock theme \"{t['name']}\" not confirmed: probe found "
                             f"{len(t['confirm_up'])} related stocks up (need {config.SINGLE_CONFIRM_MIN})",
        })
        qa_tickers.add(tk)
    return {"themes": kept, "qa": qa, "unconfirmed": [
        {"name": t["name"], "ticker": t["stocks"][0]["ticker"], "confirm_up": t["confirm_up"]} for t in failed
    ]}


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
