"""What moved markets: rank the biggest market-impact events across daily briefs.

Parse (code) every brief's one-liner, ``### ⚡ Top Movers At a Glance`` rows and
``### 🧭 Narrative Threads`` → one Sonnet call lists discrete events ranked by impact
(optional date, headline, why, moves) → code dedupes + backfills uncovered Mega-cap
catalyst moves → ``events.json``.

    docker compose exec backend python -m market_brief.narratives               # ~$0.31
    docker compose exec backend python -m market_brief.narratives --dry-run     # parse + prompt only
    docker compose exec backend python -m market_brief.narratives --rescore     # re-postprocess latest run, no LLM
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from market_brief.config import OUTPUTS_DIR, USER_DATA_DIR

logger = logging.getLogger(__name__)

PX_OUTPUTS_DIR = USER_DATA_DIR / "market_brief_perplexity"
NARRATIVES_DIR = USER_DATA_DIR / "market_narratives"

WINDOW_DAYS = 120
LEAD_CHARS = 220
DEDUPE_DAYS = 3
COVER_DAYS = 2
REPEAT_DAYS = 7
BACKFILL_MIN_MOVE = 6.0
# A 6%+ company-specific move in these names moves the index → ranked as impact 3, else 2.
INDEX_HEAVY = {
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "AVGO", "TSM", "AMD",
    "ORCL", "LLY", "BRK.B", "JPM", "WMT", "V", "MA", "NFLX",
}
GENERIC_CATALYST_RE = re.compile(
    r"sector|rally|rotation|rebound|no specific|no identified|no in-window|no new catalyst|sympathy|"
    r"bounce|profit-taking|dip-buying|risk-on|read-through|reiterat|maintains|\bPT\b|initiat|broad|sentiment|"
    r"upgrade|downgrade|momentum|\d+D |gave back|demand|volume|sell-?off|risk-off|company-specific|"
    r"new ATH|signal|gapped|unwind|names .* picks|new CFO",
    re.I,
)
EARNINGS_RE = re.compile(r"\bQ[1-4]\b|\bEPS\b|earnings|\bguid", re.I)

DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
THREAD_RE = re.compile(r"^\*\*(?P<title>[^*]+)\*\*\s+`(?P<cat>[^`]+)`\s+→\s+(?P<status>\w+)")
ONE_LINER_RE = re.compile(r"^\*\*One-liner\*\*:\s*(.+)$", re.M)
MOVER_ROW_RE = re.compile(
    r"^\|\s*\*{0,2}(?P<sym>[A-Z][A-Z.\-]{0,6})\*{0,2}\s*\|\s*(?P<move>[^|]*)\|"
    r"\s*(?P<cap>[A-Za-z]+)\s*\|\s*(?P<cat>[^|]*)\|"
)
MOVE_CELL_RE = re.compile(r"(?P<pct>[+\-−]?\d+(?:\.\d+)?)%\s*(?P<tag>PM|AH)?")
MOVE_PCT_RE = re.compile(r"[+\-−]?(\d+(?:\.\d+)?)%")
STOPWORDS = {
    "with", "from", "after", "into", "over", "that", "this", "than", "said", "says", "report",
    "reports", "shares", "stock", "stocks", "deal", "plans", "announces", "est", "billion",
    "despite", "first", "since", "record", "revenue", "beat", "beats", "misses", "raises", "guide",
    "largest", "ever", "highest", "surges", "falls", "drops", "jumps",
}

SYSTEM_PROMPT = """You are a markets editor building a trader's memory of what actually moved markets.

Input: daily pre-market briefs, oldest to newest: one-liner, 'mover' rows (ticker, move, cap, catalyst) and narrative threads per day.

List the news events in this period that had the biggest market impact: moved indices, sectors or mega-caps
the most and/or kept mattering for weeks. Each item is one discrete event or development; do not build
storylines or link items. 30-45 items, biggest first. Always include any Mega or Large cap
single-stock move of 6%+ driven by a company-specific catalyst (launch, deal, earnings, ruling); skip moves
explained only as sector rally, rotation or no catalyst. If one event moved several stocks on several days,
list it once on its first date; a later distinct milestone (e.g. adoption, approval) is its own item.

Per item:
- impact: 1-5 (5 = moved the whole market or reset the backdrop)
- date: YYYY-MM-DD when it happened or first hit the tape (brief date if unsure); null if not datable
- headline: <=10 words, concrete (who did what), e.g. 'Meta launches Muse AI agent'
- why: <=20 words on why it mattered for markets, with the key number
- moves: up to 4 strings like 'META +6.2%' taken from the briefs; [] if none stated
- theme: 1-3 word tag (AI, Rates, Oil, Memory, Crypto, Software, Space, Biotech, M&A, ...)

Only use facts present in the input. Return only JSON:
{"events": [{"impact": 5, "date": "...", "headline": "...", "why": "...", "moves": [], "theme": "..."}]}
Never put double quotes inside string values; use single quotes."""

def _parse_iso(d: str) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()


def _strip_md(text: str) -> str:
    text = re.sub(r"\*\*|__|`", "", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_brief(path: Path) -> dict[str, Any]:
    """Extract one-liner, Top Movers rows and Narrative Threads (title + lead paragraph)."""
    text = path.read_text(encoding="utf-8")
    m = ONE_LINER_RE.search(text)
    one_liner = _strip_md(m.group(1)) if m else ""

    movers: list[dict[str, Any]] = []
    threads: list[dict[str, Any]] = []
    cur: dict[str, Any] | None = None
    body: list[str] = []
    section = ""

    def close() -> None:
        if cur is None:
            return
        paras = [p for p in "\n".join(body).split("\n\n") if p.strip()]
        lead = _strip_md(next((p for p in paras if not p.lstrip().startswith("-")), ""))
        if len(lead) > LEAD_CHARS:
            lead = lead[: LEAD_CHARS - 1].rsplit(" ", 1)[0] + "…"
        cur["lead"] = lead
        threads.append(cur)

    for line in text.splitlines():
        if line.startswith("###"):
            close()
            cur = None
            section = line
            continue
        if "Top Movers" in section:
            mm = MOVER_ROW_RE.match(line)
            if mm:
                pm = MOVE_CELL_RE.search(mm.group("move"))
                movers.append({
                    "sym": mm.group("sym"),
                    "move": float(pm.group("pct").replace("−", "-")) if pm else None,
                    "tag": (pm.group("tag") or "") if pm else "",
                    "cap": mm.group("cap"),
                    "catalyst": _strip_md(mm.group("cat")),
                })
            continue
        hm = THREAD_RE.match(line)
        if hm:
            close()
            cur = {"rank": len(threads) + 1, "title": hm.group("title").strip()}
            body = []
            continue
        if cur is not None:
            if line.startswith("---") or line.startswith("**📡"):
                close()
                cur = None
                continue
            body.append(line)
    close()
    return {"one_liner": one_liner, "movers": movers, "threads": threads}


def collect_briefs(window_days: int) -> list[dict[str, Any]]:
    """One brief per date: Benzinga if it parses, else Px. Oldest → newest."""
    by_date: dict[str, dict[str, Any]] = {}
    for source, root in (("px", PX_OUTPUTS_DIR), ("benzinga", OUTPUTS_DIR)):
        if not root.is_dir():
            continue
        for d in root.iterdir():
            f = d / "02_brief.md"
            if not (DATE_DIR_RE.match(d.name) and f.is_file()):
                continue
            parsed = parse_brief(f)
            if not (parsed["threads"] or parsed["movers"]):
                continue
            by_date[d.name] = {"date": d.name, "source": source, **parsed}
    if not by_date:
        return []
    latest = max(_parse_iso(k) for k in by_date)
    cutoff = latest - timedelta(days=window_days)
    return [by_date[k] for k in sorted(by_date) if _parse_iso(k) >= cutoff]


def build_user_message(briefs: list[dict[str, Any]]) -> str:
    out: list[str] = []
    for b in briefs:
        out.append(f"## {b['date']} ({_parse_iso(b['date']).strftime('%a')})")
        if b["one_liner"]:
            out.append(f"one-liner: {b['one_liner']}")
        for mv in b["movers"]:
            move = f"{mv['move']:+.1f}%{' ' + mv['tag'] if mv['tag'] else ''}" if mv["move"] is not None else "n/a"
            out.append(f"mover {mv['sym']} {move} ({mv['cap']}): {mv['catalyst']}")
        for t in b["threads"]:
            out.append(f"thread r{t['rank']} {t['title']} | {t['lead']}")
        out.append("")
    return "\n".join(out)


_STR_LINE_RE = re.compile(r'^(\s*"[a-z_]+":\s*")(.*)("\s*,?\s*)$')


def _repair_inner_quotes(text: str) -> str:
    """Escape stray double quotes inside one-line string values (``"why": "a "b" c",``)."""
    out = []
    for line in text.splitlines():
        m = _STR_LINE_RE.match(line)
        if m:
            inner = re.sub(r'(?<!\\)"', r'\\"', m.group(2))
            line = m.group(1) + inner + m.group(3)
        out.append(line)
    return "\n".join(out)


def _extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", text, re.S)
    if fence:
        text = fence.group(1)
    start, end = text.find("{"), text.rfind("}")
    text = text[start : end + 1]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return json.loads(_repair_inner_quotes(text))


def _syms(e: dict[str, Any]) -> set[str]:
    return {m.split()[0] for m in e["moves"] if m.split()}


def _words(text: str) -> set[str]:
    return {w for w in re.findall(r"[a-z0-9]{4,}", text.lower()) if w not in STOPWORDS}


def _covers(e: dict[str, Any], sym: str, d: str, catalyst: str) -> bool:
    """Same ticker and either a shared catalyst keyword (≤ REPEAT_DAYS) or both are earnings (≤ COVER_DAYS)."""
    if sym not in _syms(e):
        return False
    text = f"{e['headline']} {e['why']}"
    if _near(e["date"], d, REPEAT_DAYS) and _words(catalyst) & _words(text):
        return True
    return _near(e["date"], d, COVER_DAYS) and bool(EARNINGS_RE.search(catalyst) and EARNINGS_RE.search(text))


def _near(a: str | None, b: str | None, days: int) -> bool:
    return bool(a and b) and abs((_parse_iso(a) - _parse_iso(b)).days) <= days


def _max_move(e: dict[str, Any]) -> float:
    return max((abs(float(x)) for m in e["moves"] for x in MOVE_PCT_RE.findall(m)), default=0.0)


def postprocess(briefs: list[dict[str, Any]], raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize LLM events, drop repeats of the same lead ticker, backfill missed Mega-cap catalysts."""
    first, last = briefs[0]["date"], briefs[-1]["date"]
    events: list[dict[str, Any]] = []
    for r in raw.get("events") or []:
        if not isinstance(r, dict) or not (r.get("headline") or "").strip():
            continue
        d = r.get("date")
        if not (isinstance(d, str) and DATE_DIR_RE.match(d) and first <= d <= last):
            d = None
        moves = [m.strip() for m in r.get("moves") or [] if isinstance(m, str) and m.strip()][:4]
        e = {
            "impact": max(1, min(5, int(r.get("impact") or 2))),
            "date": d,
            "headline": r["headline"].strip(),
            "why": (r.get("why") or "").strip(),
            "moves": moves,
            "theme": (r.get("theme") or "").strip(),
            "source": "llm",
        }
        lead = moves[0].split()[0] if moves else None
        if lead and any(
            o["moves"] and o["moves"][0].split()[0] == lead and _near(o["date"], d, DEDUPE_DAYS)
            for o in events
        ):
            continue
        events.append(e)

    n_llm = len(events)
    for b in briefs:
        for mv in b["movers"]:
            if mv["cap"] != "Mega" or mv["move"] is None or abs(mv["move"]) < BACKFILL_MIN_MOVE:
                continue
            head, _, rest = mv["catalyst"].partition(";")
            head = head.strip()
            if not head or GENERIC_CATALYST_RE.search(head):
                continue
            if any(_covers(e, mv["sym"], b["date"], head) for e in events):
                continue
            events.append({
                "impact": 3 if mv["sym"] in INDEX_HEAVY else 2,
                "date": b["date"],
                "headline": f"{mv['sym']}: {head}",
                "why": rest.strip(" ;"),
                "moves": [f"{mv['sym']} {mv['move']:+.1f}%{' ' + mv['tag'] if mv['tag'] else ''}"],
                "theme": "",
                "source": "mover",
            })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "asof": last,
        "window": {"first": first, "last": last, "n_briefs": len(briefs)},
        "n_llm": n_llm,
        "n_backfill": len(events) - n_llm,
        "events": events,
    }


def _latest_run_dir() -> Path | None:
    if not NARRATIVES_DIR.is_dir():
        return None
    runs = sorted(
        d for d in NARRATIVES_DIR.iterdir()
        if DATE_DIR_RE.match(d.name) and (d / "02_llm_response.txt").is_file() and (d / "01_briefs.json").is_file()
    )
    return runs[-1] if runs else None


def _write_status(outdir: Path, status: str, stage: str, **extra: Any) -> None:
    """Single global run state at ``NARRATIVES_DIR/status.json`` (``outdir`` = run being built)."""
    payload = {
        "status": status,
        "stage": stage,
        "run": outdir.name,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        **extra,
    }
    NARRATIVES_DIR.mkdir(parents=True, exist_ok=True)
    (NARRATIVES_DIR / "status.json").write_text(json.dumps(payload), encoding="utf-8")


def _finish(outdir: Path, briefs: list[dict[str, Any]], raw: dict[str, Any], cost: float | None) -> None:
    """Write the extracted events newest first; no secondary ranking call."""
    result = postprocess(briefs, raw)
    for e in result["events"]:
        # Three visual levels: major market events, notable events, then the rest.
        e["grade"] = 3 if e["impact"] >= 5 else 2 if e["impact"] >= 4 or (
            e["source"] == "mover" and e["impact"] >= 3
        ) else 1
    result["events"].sort(
        key=lambda e: (e["date"] is not None, e["date"] or "", e["grade"]), reverse=True
    )
    result["cost_usd"] = cost
    (outdir / "events.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
    _write_status(outdir, "done", "done", cost_usd=cost)
    logger.info(
        "%d events (%d llm + %d backfill), cost $%s → %s",
        len(result["events"]), result["n_llm"], result["n_backfill"], cost, outdir,
    )

def run(*, window_days: int, dry_run: bool, rescore: bool) -> Path:
    if rescore:
        prior = _latest_run_dir()
        if prior is None:
            raise SystemExit("--rescore: no prior run with 01_briefs.json + 02_llm_response.txt")
        briefs = json.loads((prior / "01_briefs.json").read_text(encoding="utf-8"))["briefs"]
        raw = _extract_json((prior / "02_llm_response.txt").read_text(encoding="utf-8"))
        (prior / "02_llm_response.json").write_text(json.dumps(raw, indent=1), encoding="utf-8")
        costs_path = prior / "run_costs.json"
        costs = json.loads(costs_path.read_text(encoding="utf-8")) if costs_path.is_file() else {}
        _finish(prior, briefs, raw, costs.get("total_cost_usd"))
        return prior

    briefs = collect_briefs(window_days)
    if not briefs:
        raise SystemExit("No briefs with Top Movers or Narrative Threads found")
    user_message = build_user_message(briefs)

    outdir = NARRATIVES_DIR / briefs[-1]["date"]
    outdir.mkdir(parents=True, exist_ok=True)
    _write_status(outdir, "running", "parse")
    (outdir / "01_briefs.json").write_text(json.dumps({"briefs": briefs}, indent=1), encoding="utf-8")
    (outdir / "02_llm_input.md").write_text(user_message, encoding="utf-8")
    logger.info(
        "Parsed %d briefs (%s → %s), %d movers, %d threads, ~%d input tokens",
        len(briefs), briefs[0]["date"], briefs[-1]["date"],
        sum(len(b["movers"]) for b in briefs), sum(len(b["threads"]) for b in briefs),
        (len(SYSTEM_PROMPT) + len(user_message)) // 4,
    )
    if dry_run:
        _write_status(outdir, "done", "dry_run")
        return outdir

    from market_brief.anthropic_client import SONNET_LOGICAL, SONNET_MODEL, complete
    from market_brief.cost_tracker import CostTracker

    try:
        _write_status(outdir, "running", "events")
        tracker = CostTracker(outdir=outdir)
        text = complete(
            model=SONNET_MODEL,
            logical_model=SONNET_LOGICAL,
            system=SYSTEM_PROMPT,
            user_message=user_message,
            step="events",
            tracker=tracker,
            max_tokens=12_000,
            use_stream=True,
            pace_after=False,
        )
        (outdir / "02_llm_response.txt").write_text(text, encoding="utf-8")
        raw = _extract_json(text)
        (outdir / "02_llm_response.json").write_text(json.dumps(raw, indent=1), encoding="utf-8")
        _finish(outdir, briefs, raw, round(tracker.total_cost_usd, 4))
    except Exception as e:
        _write_status(outdir, "failed", "events", error=str(e))
        raise
    return outdir


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--window-days", type=int, default=WINDOW_DAYS)
    ap.add_argument("--dry-run", action="store_true", help="parse + write prompt, no LLM")
    ap.add_argument("--rescore", action="store_true", help="re-postprocess latest events response, no LLM")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(window_days=args.window_days, dry_run=args.dry_run, rescore=args.rescore)


if __name__ == "__main__":
    main()
