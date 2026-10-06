"""Experimental market brief with Perplexity web search as the news source (no Benzinga).

Hydration comes from Postgres (screener universe + OHLC tape). News comes from Perplexity
``sonar-pro`` web search in three phases (plan + prompts in ``px_prompts.py``):

  A. ~26 narrow broad probes + ticker batches (4/call)
  B. thread follow-ups planned by an LLM from A (second-order effects, contested catalysts)
  C. single-ticker gap fills for movers still without a catalyst

Research is compacted into a deduplicated fact ledger, then synthesized with STEP4_SYSTEM_PROMPT +
``SYNTH_ADDENDUM`` (length caps, timing rules), audited for ledger omissions and revised so the output is comparable to
``user_data/market_brief/<date>/02_brief.md``. ``compare_nuance.md`` grades recall vs that brief.

Run inside the backend container:

    docker compose exec backend python -m market_brief.perplexity_brief
    docker compose exec backend python -m market_brief.perplexity_brief --dry-run
    docker compose exec backend python -m market_brief.perplexity_brief --skip-research --synth haiku
    docker compose exec backend python -m market_brief.perplexity_brief --asof 2026-09-17 --compare-only

Artifacts: ``user_data/market_brief_perplexity/<date>/``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from market_brief import config
from market_brief import px_prompts as P
from market_brief import status as status_mod
from market_brief.cost_tracker import CostRecord, CostTracker
from market_brief.prompts_pipeline import STEP4_SYSTEM_PROMPT, step4_user_message
from market_brief.trading_calendar import ET, prior_session_for_brief

logger = logging.getLogger(__name__)

OUTPUTS_DIR = config.USER_DATA_DIR / "market_brief_perplexity"
BASELINE_DIR = config.OUTPUTS_DIR

API_URL = "https://api.perplexity.ai/chat/completions"
RESEARCH_MODEL = os.getenv("MARKET_BRIEF_PPLX_RESEARCH_MODEL", "sonar-pro")
SYNTH_MODEL = os.getenv("MARKET_BRIEF_PPLX_SYNTH_MODEL", "sonar-pro")
RESEARCH_MAX_TOKENS = 6000
SYNTH_MAX_TOKENS = 8000
RESEARCH_TIMEOUT_SECONDS = 240
SYNTH_TIMEOUT_SECONDS = 300
CONCURRENCY = 5
BATCH_SIZE = 4
MAX_THREADS = 12
MAX_GAPS = 15
GAP_MIN_MOVE = 3.0
GAP_MIN_MOVE_MEGA = 1.5
ANTHROPIC_SYNTH_MODELS: dict[str, str] = {
    "sonnet": os.getenv("MARKET_BRIEF_PPLX_SONNET_MODEL", "claude-sonnet-4-6"),
    "haiku": os.getenv("MARKET_BRIEF_PPLX_HAIKU_MODEL", "claude-haiku-4-5"),
    "opus": os.getenv("MARKET_BRIEF_OPUS_MODEL", "claude-opus-4-6"),
}
# Planner, compaction, eval.
AUX_MODEL = os.getenv("MARKET_BRIEF_PPLX_AUX_MODEL", ANTHROPIC_SYNTH_MODELS["sonnet"])
RETRIES = 3
NO_CATALYST = "No company-specific catalyst found"
GENERIC_REASON_RE = re.compile(
    r"profit[- ]taking|reversal|no new (?:\w+ )?catalyst|broad (?:ai[- ])?(?:sell-?off|rally)|"
    r"sector[- ]wide|no (?:specific|company-specific) (?:reason|catalyst)", re.I)

SYNTH_CHOICES = ("sonnet", "haiku", "opus", "perplexity")


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------


class LockedTracker(CostTracker):
    """CostTracker safe to share across research / compaction threads."""

    _lock = threading.RLock()

    def record_usage(self, **kwargs: Any) -> CostRecord:
        with self._lock:
            return super().record_usage(**kwargs)

    def add(self, rec: CostRecord) -> None:
        with self._lock:
            self.calls = [c for c in self.calls if c.step != rec.step]
            self.calls.append(rec)
            self.flush()

    def flush(self) -> None:
        with self._lock:
            super().flush()


class PerplexityCall:
    """Perplexity chat completions; each successful call is appended to ``tracker``."""

    def __init__(self, tracker: LockedTracker) -> None:
        self.tracker = tracker
        self.lock = threading.Lock()
        self.quota_exhausted = False

    def __call__(
        self,
        *,
        label: str,
        model: str,
        messages: list[dict[str, str]],
        max_tokens: int,
        timeout: int,
        date_window: tuple[str, str] | None = None,
        disable_search: bool = False,
    ) -> dict[str, Any]:
        api_key = os.getenv("PERPLEXITY_API_KEY")
        if not api_key:
            raise RuntimeError("PERPLEXITY_API_KEY not set")
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0.1,
        }
        if disable_search:
            payload["disable_search"] = True
        else:
            payload["web_search_options"] = {"search_context_size": "high"}
            payload["search_domain_filter"] = P.DOMAIN_DENYLIST
            if date_window:
                payload["search_after_date_filter"] = date_window[0]
                payload["search_before_date_filter"] = date_window[1]

        if self.quota_exhausted:
            raise RuntimeError("Perplexity quota exhausted (earlier http 401)")
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        last_err = ""
        for attempt in range(1, RETRIES + 1):
            t0 = time.time()
            try:
                r = requests.post(API_URL, headers=headers, json=payload, timeout=timeout)
            except requests.RequestException as e:
                last_err = str(e)
                logger.warning("PPLX RETRY %s %d/%d: %s", label, attempt, RETRIES, last_err)
                time.sleep(5 * attempt)
                continue
            if r.status_code == 200:
                data = r.json()
                usage = data.get("usage") or {}
                cost = float((usage.get("cost") or {}).get("total_cost") or 0.0)
                in_tok = int(usage.get("prompt_tokens") or 0)
                out_tok = int(usage.get("completion_tokens") or 0)
                self.tracker.add(
                    CostRecord(
                        step=label,
                        api_model=model,
                        pricing_model=model,
                        input_tokens=in_tok,
                        output_tokens=out_tok,
                        cache_creation_input_tokens=0,
                        cache_read_input_tokens=0,
                        input_cost_usd=0.0,
                        output_cost_usd=0.0,
                        cache_write_cost_usd=0.0,
                        cache_read_cost_usd=0.0,
                        total_cost_usd=round(cost, 6),
                    )
                )
                content = ((data.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
                logger.info(
                    "PPLX OK %s | %.1fs | out=%d | $%.4f", label, time.time() - t0, out_tok, cost
                )
                return {
                    "content": content,
                    "search_results": data.get("search_results") or [],
                    "citations": data.get("citations") or [],
                    "usage": {"input_tokens": in_tok, "output_tokens": out_tok, "cost_usd": cost},
                }
            last_err = f"http {r.status_code}: {r.text[:300]}"
            if r.status_code == 401:
                self.quota_exhausted = True
                break
            if r.status_code == 429 or r.status_code >= 500:
                logger.warning("PPLX RETRY %s %d/%d: %s", label, attempt, RETRIES, last_err)
                time.sleep(10 * attempt)
                continue
            break
        raise RuntimeError(f"Perplexity failed for {label}: {last_err}")


def claude(model: str, system: str, user: str, step: str, tracker: LockedTracker,
           max_tokens: int = 16_000) -> str:
    import httpx

    from market_brief.anthropic_client import complete

    for attempt in range(3):
        try:
            return complete(
                model=model,
                logical_model=model,
                system=system,
                user_message=user,
                step=step,
                tracker=tracker,
                max_tokens=max_tokens,
                use_stream=True,
                pace_after=False,
            )
        except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ReadTimeout) as e:
            if attempt == 2:
                raise
            logger.warning("Anthropic %s stream error (%s), retry %d/2", step, e, attempt + 1)
            time.sleep(10 * (attempt + 1))
    raise AssertionError("unreachable")


# ---------------------------------------------------------------------------
# Hydration: ticker universe + verified tape (DB, no Benzinga)
# ---------------------------------------------------------------------------


def _cap_label(mcap: Any) -> str:
    if mcap is None:
        return "—"
    v = float(mcap)
    if v >= 100e9:
        return "Mega"
    if v >= 20e9:
        return "Large"
    if v >= 2e9:
        return "Mid"
    return "Small"


def load_universe(asof: str, outdir: Path, source: str) -> tuple[dict[str, Any], str]:
    """Return (lineage, overview_md). ``source``: auto | baseline | db."""
    baseline_tu = BASELINE_DIR / asof / "source" / "ticker_universe"
    use_baseline = source == "baseline" or (
        source == "auto" and (baseline_tu / "lineage.json").is_file()
    )
    dest = outdir / "source" / "ticker_universe"
    dest.mkdir(parents=True, exist_ok=True)

    if use_baseline:
        lineage = json.loads((baseline_tu / "lineage.json").read_text(encoding="utf-8"))
        overview = (baseline_tu / "overview.md").read_text(encoding="utf-8")
        logger.info("Universe: baseline %s (%d tickers)", baseline_tu, len(lineage["by_ticker"]))
    else:
        from market_brief.screener_universe import build_screener_universe, render_overview_markdown

        slices, lineage, _ = build_screener_universe(asof)
        overview = render_overview_markdown(slices, lineage)
        logger.info("Universe: DB screener (%d tickers)", len(lineage["by_ticker"]))

    (dest / "lineage.json").write_text(json.dumps(lineage, indent=2), encoding="utf-8")
    (dest / "overview.md").write_text(overview, encoding="utf-8")
    return lineage, overview


def load_tape(asof: str, tickers: list[str]) -> tuple[str, dict[str, float], str]:
    """Return (session_date, {ticker: pct}, formatted tape block) from local OHLC."""
    from market_brief.tape import format_tape_block, get_tape

    session_date, t_quotes, i_quotes = get_tape(tickers, asof)
    moves = {q.ticker: q.pct_change for q in t_quotes}
    block = format_tape_block(session_date, t_quotes, i_quotes)
    logger.info("Tape: session %s, %d/%d tickers priced, %d indices",
                session_date, len(t_quotes), len(tickers), len(i_quotes))
    return session_date, moves, block


def build_batches(lineage: dict[str, Any], batch_size: int) -> list[tuple[str, list[str]]]:
    """(batch_name, tickers): universe flattened in slice priority order, chunked evenly."""
    prio = {s: i for i, s in enumerate(config.TICKER_UNIVERSE_SLICE_PRIORITY)}
    caps = {c: i for i, c in enumerate(config.TICKER_UNIVERSE_CAP_BUCKETS)}
    slices = sorted(
        lineage.get("slices") or [],
        key=lambda s: (prio.get(s["slice_id"], 99), caps.get(s["cap_bucket"], 99)),
    )
    syms = [t for sl in slices for t in (sl.get("tickers") or [])]
    if not syms:
        return []
    n_batches = -(-len(syms) // batch_size)
    size = -(-len(syms) // n_batches)
    return [
        (f"tickers_{i // size + 1:02d}", syms[i : i + size])
        for i in range(0, len(syms), size)
    ]


# ---------------------------------------------------------------------------
# Research
# ---------------------------------------------------------------------------


def _fmt_date(d: datetime) -> str:
    return f"{d.month}/{d.day}/{d.year}"


def date_windows(session_date: str, asof: str) -> dict[str, tuple[str, str]]:
    """Perplexity filters are day-granular; the prompt's cutoff time does the intraday cut."""
    sess = datetime.strptime(session_date, "%Y-%m-%d")
    end = _fmt_date(datetime.strptime(asof, "%Y-%m-%d") + timedelta(days=1))
    return {
        "session": (_fmt_date(sess - timedelta(days=1)), end),
        "premarket": (_fmt_date(sess), end),
    }


def _sources_md(result: dict[str, Any]) -> str:
    rows = result.get("search_results") or []
    if rows:
        lines = [
            f"{i}. [{r.get('title') or r.get('url')}]({r.get('url')})"
            + (f" — {r['date']}" if r.get("date") else "")
            for i, r in enumerate(rows, start=1)
        ]
    else:
        lines = [f"{i}. {u}" for i, u in enumerate(result.get("citations") or [], start=1)]
    return "\n".join(lines) if lines else "(none returned)"


def run_research(
    jobs: list[dict[str, Any]],
    research_dir: Path,
    pplx: PerplexityCall,
    windows: dict[str, tuple[str, str]] | None,
    model: str,
    phase: str,
) -> list[str]:
    """jobs: {name, kind, group, window, prompt}. Writes <name>.md (content + sources) + <name>.json."""
    research_dir.mkdir(parents=True, exist_ok=True)
    outdir = research_dir.parent
    total = len(jobs)
    done: list[str] = []
    failed: list[str] = []

    def _progress() -> None:
        status_mod.write_status(
            outdir, "running", stage="research",
            extra={"detail": f"{phase}: {len(done)}/{total} Perplexity searches done"
                   + (f" · {len(failed)} failed" if failed else "")},
        )

    _progress()

    def _one(job: dict[str, Any]) -> None:
        name = job["name"]
        try:
            res = pplx(
                label=name,
                model=model,
                messages=[{"role": "user", "content": job["prompt"]}],
                max_tokens=RESEARCH_MAX_TOKENS,
                timeout=RESEARCH_TIMEOUT_SECONDS,
                date_window=windows[job["window"]] if windows else None,
            )
        except Exception as e:  # noqa: BLE001
            logger.error("Research failed %s: %s", name, e)
            res = {"content": f"_Research call failed: {e}_", "search_results": [], "citations": []}
            with pplx.lock:
                failed.append(name)
        (research_dir / f"{name}.json").write_text(
            json.dumps({**job, **res}, indent=2), encoding="utf-8",
        )
        (research_dir / f"{name}.md").write_text(
            f"{res['content'].strip()}\n\n---\n\n### Sources\n\n{_sources_md(res)}\n",
            encoding="utf-8",
        )
        with pplx.lock:
            done.append(name)
            _progress()

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        list(ex.map(_one, jobs))

    if failed:
        logger.warning("%s: %d/%d searches failed: %s", phase, len(failed), total, ", ".join(failed))
    if pplx.quota_exhausted:
        logger.error("Perplexity returned 401 (quota/auth) — remaining searches skipped")
    return failed


def load_research(research_dir: Path) -> list[dict[str, Any]]:
    rows = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(research_dir.glob("*.json"))]
    return [r for r in rows if not r["content"].startswith("_Research call failed")]


def _block(r: dict[str, Any]) -> str:
    return f"<summary source=\"{r['name']}\">\n{r['content'].strip()}\n</summary>"


_SECTION_RE = re.compile(r"^##\s+.*?\(([A-Z][A-Z0-9.\-]*)\)", re.M)


def ticker_sections(research: list[dict[str, Any]]) -> dict[str, str]:
    """{TICKER: section markdown}. Gap-fill (kind=gap) results replace batch sections that found
    nothing."""
    out: dict[str, str] = {}
    for kind in ("ticker", "gap"):
        for r in research:
            if r.get("kind") != kind:
                continue
            text = r["content"]
            heads = list(_SECTION_RE.finditer(text))
            for i, m in enumerate(heads):
                end = heads[i + 1].start() if i + 1 < len(heads) else len(text)
                sec = text[m.start():end].strip()
                sym = m.group(1)
                if kind == "gap" and NO_CATALYST in sec and sym in out:
                    continue
                out[sym] = sec
    return out


def select_gaps(
    lineage: dict[str, Any], moves: dict[str, float], sections: dict[str, str]
) -> list[str]:
    by_ticker = lineage.get("by_ticker") or {}
    picks: list[tuple[bool, float, str]] = []
    for sym, d in by_ticker.items():
        mv = moves.get(sym, d.get("dr_1"))
        if mv is None:
            continue
        mega = _cap_label(d.get("market_cap")) == "Mega"
        thresh = GAP_MIN_MOVE_MEGA if mega else GAP_MIN_MOVE
        sec = sections.get(sym, "")
        generic = mega and GENERIC_REASON_RE.search(sec)
        if abs(mv) >= thresh and (not sec or NO_CATALYST in sec or generic):
            picks.append((mega, abs(mv), sym))
    return [s for *_, s in sorted(picks, reverse=True)[:MAX_GAPS]]


def _parse_json_block(text: str) -> dict[str, Any]:
    m = re.search(r"```json\s*(.*?)```", text, re.S) or re.search(r"(\{.*\})", text, re.S)
    if not m:
        return {}
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}


def plan_threads(
    *, asof: str, cutoff: str, tape_block: str, research: list[dict[str, Any]],
    outdir: Path, tracker: LockedTracker,
) -> list[dict[str, Any]]:
    text = claude(
        AUX_MODEL, P.PLANNER_SYSTEM,
        P.planner_user(asof=asof, cutoff=cutoff, tape=tape_block,
                       research="\n\n".join(_block(r) for r in research),
                       max_threads=MAX_THREADS),
        step="plan_threads", tracker=tracker, max_tokens=10_000,
    )
    threads = (_parse_json_block(text).get("threads") or [])[:MAX_THREADS]
    seen: set[str] = set()
    for i, t in enumerate(threads, start=1):
        slug = re.sub(r"[^a-z0-9_]+", "_", str(t.get("slug") or f"thread_{i}").lower())[:30]
        t["slug"] = slug if slug not in seen else f"{slug}_{i}"
        seen.add(t["slug"])
    (outdir / "01b_plan.json").write_text(json.dumps({"threads": threads, "raw": text}, indent=2),
                                          encoding="utf-8")
    logger.info("Planner: %d threads: %s", len(threads), ", ".join(t["slug"] for t in threads))
    return threads


# ---------------------------------------------------------------------------
# Ledger (compaction) + synthesis
# ---------------------------------------------------------------------------


def build_ticker_ledger(
    lineage: dict[str, Any], moves: dict[str, float], sections: dict[str, str]
) -> str:
    """Universe tickers ordered by cap tier then |move|; no-catalyst names collapsed to one line."""
    by_ticker = lineage.get("by_ticker") or {}
    tier = {"Mega": 0, "Large": 1, "Mid": 2, "Small": 3, "—": 4}
    rows = []
    for sym, d in by_ticker.items():
        cap = _cap_label(d.get("market_cap"))
        mv = moves.get(sym, d.get("dr_1"))
        rows.append((tier[cap], -abs(mv or 0.0), sym, cap, mv))
    rows.sort()
    blocks: list[str] = []
    quiet: list[str] = []
    for _, _, sym, cap, mv in rows:
        mv_s = f"{mv:+.2f}%" if mv is not None else "n/a"
        sec = sections.get(sym)
        if not sec or NO_CATALYST in sec and sec.count("\n- ") <= 1:
            quiet.append(f"{sym} {mv_s} ({cap})")
            continue
        head, _, body = sec.partition("\n")
        blocks.append(f"{head}\n- Tape 1D: {mv_s} · Cap: {cap}\n{body.strip()}")
    if quiet:
        blocks.append("## Universe tickers with no catalyst found\n- " + ", ".join(quiet))
    return "\n\n".join(blocks)


def build_channel_ledger(
    *, asof: str, cutoff: str, research: list[dict[str, Any]], outdir: Path,
    tracker: LockedTracker, compact: bool,
) -> str:
    groups: dict[str, list[dict[str, Any]]] = {g: [] for g in P.COMPACT_GROUPS}
    for r in research:
        if r.get("kind") == "channel":
            g = r.get("group") or "corporate"
            groups["macro" if g in ("macro", "calendar") else g].append(r)
        elif r.get("kind") == "thread":
            groups["themes"].append(r)
    if not compact:
        return "\n\n".join(_block(r) for g in groups.values() for r in g)

    ledger_dir = outdir / "02_ledger"
    ledger_dir.mkdir(parents=True, exist_ok=True)

    def _one(g: str) -> str:
        notes = "\n\n".join(_block(r) for r in groups[g])
        if not notes:
            return ""
        text = claude(
            AUX_MODEL, P.COMPACT_SYSTEM,
            P.compact_user(asof=asof, cutoff=cutoff, group_label=P.COMPACT_GROUPS[g], notes=notes),
            step=f"compact_{g}", tracker=tracker, max_tokens=24_000,
        )
        (ledger_dir / f"{g}.md").write_text(text, encoding="utf-8")
        logger.info("Ledger %s: %s → %s chars", g, f"{len(notes):,}", f"{len(text):,}")
        return f"<ledger group=\"{g}\">\n{text.strip()}\n</ledger>"

    with ThreadPoolExecutor(max_workers=len(groups)) as ex:
        parts = list(ex.map(_one, list(groups)))
    return "\n\n".join(p for p in parts if p)


def revise_brief(*, outdir: Path, draft: str, system: str, synth: str, ledger: str,
                 tracker: LockedTracker) -> str:
    """Coverage audit (ledger vs draft) → revision pass that integrates material omissions."""
    (outdir / "02_brief_draft.md").write_text(draft, encoding="utf-8")
    status_mod.write_status(outdir, "running", stage="coverage")
    omissions = claude(AUX_MODEL, P.COVERAGE_SYSTEM, P.coverage_user(ledger=ledger, brief=draft),
                       step="coverage", tracker=tracker, max_tokens=4000).strip()
    (outdir / "02_coverage.md").write_text(omissions + "\n", encoding="utf-8")
    n = sum(1 for ln in omissions.splitlines() if " | " in ln)
    logger.info("Coverage: %d omissions", n)
    if not n:
        return draft
    status_mod.write_status(outdir, "running", stage="revise")
    return claude(ANTHROPIC_SYNTH_MODELS[synth], system + P.REVISE_ADDENDUM,
                  P.revise_user(draft=draft, omissions=omissions),
                  step=f"revise_{synth}", tracker=tracker)


def verify_brief(*, asof: str, cutoff: str, outdir: Path, brief: str, notes: str, tape_block: str,
                 tracker: LockedTracker) -> str:
    """Fact-check pass → exact find/replace fixes applied in code (02_verify.json)."""
    status_mod.write_status(outdir, "running", stage="verify")
    raw = claude(AUX_MODEL, P.VERIFY_SYSTEM,
                 P.verify_user(brief=brief, notes=notes, tape=tape_block,
                               calendar=P.calendar_block(asof), asof=asof, cutoff=cutoff),
                 step="verify", tracker=tracker, max_tokens=6000)
    m = re.search(r"\{.*\}", raw, re.S)
    try:
        fixes = json.loads(m.group(0)).get("fixes", []) if m else []
    except json.JSONDecodeError:
        logger.warning("Verify: unparseable output")
        fixes = []
    applied = []
    for f in fixes:
        find, repl = f.get("find") or "", f.get("replace")
        if find and repl is not None and brief.count(find) == 1:
            brief = brief.replace(find, repl)
            applied.append(f)
    (outdir / "02_verify.json").write_text(
        json.dumps({"proposed": fixes, "applied": len(applied)}, indent=2), encoding="utf-8")
    logger.info("Verify: %d fixes proposed, %d applied", len(fixes), len(applied))
    return brief


def run_synthesis(
    *, asof: str, cutoff: str, outdir: Path, overview: str, tape_block: str, synth: str,
    channel_ledger: str, ticker_ledger: str, pplx: PerplexityCall, revise: bool = True,
    verify: bool = True,
) -> Path:
    user_msg = step4_user_message(
        date_str=f"{asof} (brief cutoff {cutoff} ET)",
        ticker_universe=f"{P.calendar_block(asof)}\n\n{tape_block}\n\n{overview}",
        channel_summaries=channel_ledger,
        ticker_summaries=ticker_ledger,
    )
    (outdir / "02_synth_input.md").write_text(user_msg, encoding="utf-8")
    logger.info("Synthesis input: %s chars (%s)", f"{len(user_msg):,}", synth)

    system = STEP4_SYSTEM_PROMPT + P.SYNTH_ADDENDUM
    step = f"synthesis_{synth}"
    pplx.tracker.set_current_step(step)
    status_mod.write_status(outdir, "running", stage=step)
    if synth in ANTHROPIC_SYNTH_MODELS:
        brief = claude(ANTHROPIC_SYNTH_MODELS[synth], system, user_msg, step, pplx.tracker)
    else:
        res = pplx(
            label=step,
            model=SYNTH_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user_msg}],
            max_tokens=SYNTH_MAX_TOKENS,
            timeout=SYNTH_TIMEOUT_SECONDS,
            disable_search=True,
        )
        brief = res["content"]

    if revise and synth in ANTHROPIC_SYNTH_MODELS:
        brief = revise_brief(outdir=outdir, draft=brief, system=system, synth=synth,
                             ledger=f"{channel_ledger}\n\n{ticker_ledger}", tracker=pplx.tracker)
    if verify:
        brief = verify_brief(asof=asof, cutoff=cutoff, outdir=outdir, brief=brief,
                             notes=f"{channel_ledger}\n\n{ticker_ledger}", tape_block=tape_block,
                             tracker=pplx.tracker)

    path = outdir / "02_brief.md"
    path.write_text(brief, encoding="utf-8")
    logger.info("Wrote %s (%s chars)", path, f"{len(brief):,}")
    return path


# ---------------------------------------------------------------------------
# Compare vs Benzinga baseline
# ---------------------------------------------------------------------------

_ROW_RE = re.compile(r"^\|\s*(?:\*\*)?([A-Z][A-Z0-9.\-]*)(?:\*\*)?\s*\|\s*([^|]*)\|")


def _top_movers(md: str) -> dict[str, str]:
    """{ticker: move} from the Top Movers table."""
    out: dict[str, str] = {}
    in_section = False
    for line in md.splitlines():
        if line.startswith("### "):
            in_section = "Top Movers" in line
            continue
        if in_section and (m := _ROW_RE.match(line)):
            out[m.group(1)] = m.group(2).strip()
    return out


def _section_titles(md: str, heading: str) -> list[str]:
    out: list[str] = []
    in_section = False
    for line in md.splitlines():
        if line.startswith("### "):
            in_section = heading in line
            continue
        if in_section and (m := re.match(r"^\*\*(.+?)\*\*\s*`", line)):
            out.append(m.group(1))
    return out


def compare(asof: str, outdir: Path) -> Path | None:
    base_path = BASELINE_DIR / asof / "02_brief.md"
    test_path = outdir / "02_brief.md"
    if not base_path.is_file() or not test_path.is_file():
        logger.warning("Compare skipped: need %s and %s", base_path, test_path)
        return None
    base_md = base_path.read_text(encoding="utf-8")
    test_md = test_path.read_text(encoding="utf-8")
    base, test = _top_movers(base_md), _top_movers(test_md)
    both = sorted(set(base) & set(test))

    def _sign(s: str) -> str:
        s = s.strip()
        return "-" if s.startswith(("-", "−")) else "+" if s.startswith("+") else "?"

    sign_mismatch = [t for t in both if _sign(base[t]) != _sign(test[t])]
    recall = len(both) / len(base) if base else 0.0
    lines = [
        f"# Compare — {asof}",
        "",
        f"- Baseline (Benzinga): `{base_path}`",
        f"- Test (Perplexity): `{test_path}`",
        "",
        "## Top Movers",
        "",
        f"- Baseline rows: {len(base)} · Test rows: {len(test)} · Overlap: {len(both)} "
        f"(recall {recall:.0%})",
        f"- Sign mismatches: {', '.join(sign_mismatch) or 'none'}",
        f"- Only in baseline: {', '.join(sorted(set(base) - set(test))) or 'none'}",
        f"- Only in test: {', '.join(sorted(set(test) - set(base))) or 'none'}",
        "",
        "| Ticker | Baseline | Test |",
        "|---|---|---|",
    ]
    for t in sorted(set(base) | set(test)):
        lines.append(f"| {t} | {base.get(t, '—')} | {test.get(t, '—')} |")
    lines += [
        "",
        "## Narrative Threads",
        "",
        "Baseline:",
        *[f"- {t}" for t in _section_titles(base_md, "Narrative Threads")],
        "",
        "Test:",
        *[f"- {t}" for t in _section_titles(test_md, "Narrative Threads")],
        "",
        f"Size: baseline {len(base_md):,} chars · test {len(test_md):,} chars",
        "",
    ]
    path = outdir / "compare.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Top Movers overlap %d/%d (recall %.0f%%) → %s", len(both), len(base), recall * 100, path)
    return path


def eval_nuance(asof: str, outdir: Path, tracker: LockedTracker) -> Path | None:
    """LLM-graded recall of Benzinga brief items in the Px brief → compare_nuance.md.

    Items are extracted once per date (``OUTPUTS_DIR/<date>/eval_items.md``, rebuilt when the
    Benzinga brief changes) so scores across Px versions share a denominator.
    """
    base_path = BASELINE_DIR / asof / "02_brief.md"
    test_path = outdir / "02_brief.md"
    if not base_path.is_file() or not test_path.is_file():
        logger.warning("Nuance eval skipped: need %s and %s", base_path, test_path)
        return None
    items_path = OUTPUTS_DIR / asof / "eval_items.md"
    if not items_path.is_file() or items_path.stat().st_mtime < base_path.stat().st_mtime:
        items = claude(AUX_MODEL, P.EVAL_ITEMS_SYSTEM, base_path.read_text(encoding="utf-8"),
                       step="eval_items", tracker=tracker, max_tokens=6000)
        items_path.parent.mkdir(parents=True, exist_ok=True)
        items_path.write_text(items.strip() + "\n", encoding="utf-8")
    text = claude(
        AUX_MODEL, P.EVAL_SYSTEM,
        P.eval_user(items=items_path.read_text(encoding="utf-8"),
                    test=test_path.read_text(encoding="utf-8"), calendar=P.calendar_block(asof)),
        step="eval_nuance", tracker=tracker, max_tokens=10_000,
    )
    item_rows = re.findall(r"^(I\d+)\s*\|\s*(major|minor)?", items_path.read_text(encoding="utf-8"),
                           re.M | re.I)
    ids = [i for i, _ in item_rows]
    tiers = {i: (t or "minor").lower() for i, t in item_rows}
    rows = re.findall(
        r"^\|\s*(I\d+)\s*\|\s*\**(?:major|minor)\**\s*\|[^|]*\|\s*\**"
        r"(found|partial|missing|misattributed|baseline_error)\**\s*\|", text, re.M | re.I)
    graded = {i: (tiers.get(i, "minor"), s.lower()) for i, s in rows}
    statuses = ("found", "partial", "missing", "misattributed", "baseline_error", "ungraded")

    def _score(sel: list[str]) -> tuple[dict[str, int], float, float]:
        c = {s: 0 for s in statuses}
        for i in sel:
            c[graded.get(i, ("", "ungraded"))[1]] += 1
        n = len(sel) or 1
        recall = 100 * (c["found"] + c["baseline_error"] + 0.5 * c["partial"]) / n
        return c, recall, 100 * (n - c["missing"] - c["ungraded"]) / n

    lines = ["## Score"]
    for label, sel in (("All", ids), ("Major", [i for i in ids if tiers[i] == "major"])):
        c, recall, story = _score(sel)
        lines.append(f"- {label} ({len(sel)}): " + " · ".join(f"{k} {v}" for k, v in c.items() if v)
                     + f" → recall **{recall:.1f}%** · story coverage **{story:.1f}%**")
        logger.info("Nuance %s: recall %.1f%% · coverage %.1f%% %s", label, recall, story, c)
    lines.append("- recall = (found + baseline_error + 0.5 × partial) / N; numbers from snapshot "
                 "timing are not graded")
    path = outdir / "compare_nuance.md"
    path.write_text(f"# Nuance eval — {asof}\n\n" + "\n".join(lines) + f"\n\n{text.strip()}\n",
                    encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _setup_logging(outdir: Path, verbose: bool, *, to_file: bool) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if to_file:
        # Backend treats a fresh run.log as "running"; dry runs must not write it.
        handlers.append(logging.FileHandler(outdir / "run.log", mode="a", encoding="utf-8"))
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        handlers=handlers,
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Perplexity-sourced market brief (no Benzinga)")
    p.add_argument("--asof", "--date", dest="asof", help="YYYY-MM-DD (default: today ET)")
    p.add_argument("--cutoff", help="HH:MM ET news cutoff on the brief date "
                                    "(default: now if today, else 09:00)")
    p.add_argument("--universe", choices=("db", "baseline", "auto"), default="db",
                   help="db = own DB screener run (default, decoupled); "
                        "baseline = copy Benzinga run's lineage.json; auto = baseline if present")
    p.add_argument("--synth", choices=SYNTH_CHOICES, default="opus",
                   help="Synthesis model (STEP4_SYSTEM_PROMPT + Px addendum)")
    p.add_argument("--model", default=RESEARCH_MODEL, help="Perplexity research model")
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    p.add_argument("--no-followups", action="store_true", help="Skip phases B (threads) and C (gaps)")
    p.add_argument("--compact", action="store_true",
                   help="Compact research into a fact ledger before synthesis (default: raw)")
    p.add_argument("--no-compact", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--no-revise", action="store_true", help="Skip coverage audit + revision pass")
    p.add_argument("--verify", action="store_true",
                   help="Run fact-check pass after revise (02_verify.json; default off)")
    p.add_argument("--no-verify", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--eval", action="store_true",
                   help="After the run, compare vs Benzinga (compare.md) + LLM grading (compare_nuance.md)")
    p.add_argument("--no-date-filter", action="store_true",
                   help="Drop search_after/before_date_filter (live runs only)")
    p.add_argument("--dry-run", action="store_true",
                   help="Write phase A prompts to 00_prompts/; no API calls")
    p.add_argument("--skip-research", action="store_true", help="Reuse existing 01_research/")
    p.add_argument("--resume-followups", action="store_true",
                   help="Reuse phase A research; rerun phases B/C onward")
    p.add_argument("--outdir", help="Run directory (default OUTPUTS_DIR/<date>); for A/B slots")
    p.add_argument("--compare-only", action="store_true", help="Only rebuild compare.md")
    p.add_argument("--eval-only", action="store_true", help="Only rebuild compare_nuance.md")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    now = datetime.now(ET)
    asof = args.asof or now.strftime("%Y-%m-%d")
    cutoff = args.cutoff or (now.strftime("%H:%M") if asof == now.strftime("%Y-%m-%d") else "09:00")
    outdir = Path(args.outdir) if args.outdir else OUTPUTS_DIR / asof
    _setup_logging(outdir, args.verbose,
                   to_file=not (args.dry_run or args.compare_only or args.eval_only))

    if args.compare_only:
        path = compare(asof, outdir)
        print(path.read_text(encoding="utf-8") if path else "Nothing to compare")
        return 0
    if args.eval_only:
        tracker = LockedTracker.load_or_create(outdir)
        path = eval_nuance(asof, outdir, tracker)
        print(path.read_text(encoding="utf-8") if path else "Nothing to compare")
        return 0

    try:
        return _run(args, asof, cutoff, outdir)
    except Exception as e:  # noqa: BLE001
        logger.exception("Pipeline failed: %s", e)
        if not args.dry_run:
            status_mod.write_status(outdir, "failed", stage="error", error=str(e)[:500])
        return 1


def _run(args: argparse.Namespace, asof: str, cutoff: str, outdir: Path) -> int:
    if not args.dry_run:
        status_mod.write_status(outdir, "running", stage="hydrate")

    lineage, overview = load_universe(asof, outdir, args.universe)
    by_ticker = lineage.get("by_ticker") or {}
    tickers = sorted(by_ticker)
    session_date, moves, tape_block = load_tape(asof, tickers)
    expected_session = prior_session_for_brief(asof).isoformat()
    if session_date != expected_session:
        logger.warning("OHLC latest session %s != expected %s (run daily_price_update?)",
                       session_date, expected_session)
    (outdir / "tape.md").write_text(tape_block, encoding="utf-8")

    def table(syms: list[str]) -> str:
        return P.tape_table(syms, lineage, moves, _cap_label)

    jobs_a: list[dict[str, Any]] = [
        {"name": f"a_{pr['slug']}", "kind": "channel", "group": pr["group"],
         "window": pr["window"], "prompt": P.channel_prompt(pr, session_date, asof, cutoff)}
        for pr in P.BROAD_PROBES
    ]
    jobs_a += [
        {"name": name, "kind": "ticker", "group": "tickers", "window": "session",
         "prompt": P.ticker_batch_prompt(table(syms), session_date, asof, cutoff)}
        for name, syms in build_batches(lineage, args.batch_size)
    ]
    windows = None if args.no_date_filter else date_windows(session_date, asof)
    logger.info("Phase A jobs: %d · cutoff %s %s ET · date filters %s",
                len(jobs_a), asof, cutoff, windows)

    if args.dry_run:
        pdir = outdir / "00_prompts"
        if pdir.exists():
            shutil.rmtree(pdir)
        pdir.mkdir(parents=True, exist_ok=True)
        for job in jobs_a:
            (pdir / f"{job['name']}.md").write_text(job["prompt"], encoding="utf-8")
        print(f"Wrote {len(jobs_a)} phase A prompts → {pdir} (B/C depend on A results)")
        return 0

    research_dir = outdir / "01_research"
    failed: list[str] = []
    if args.skip_research or args.resume_followups:
        if not any(research_dir.glob("*.json")):
            raise FileNotFoundError(f"No research at {research_dir} — run without --skip-research")
        tracker = LockedTracker.load_or_create(outdir)
        tracker.calls = [c for c in tracker.calls
                         if not c.step.startswith(("synthesis_", "compact_", "eval_", "coverage", "revise_"))]
        if args.resume_followups:
            for f in list(research_dir.glob("b_*")) + list(research_dir.glob("c_*")):
                f.unlink()
            tracker.calls = [c for c in tracker.calls if c.step != "plan_threads"]
        pplx = PerplexityCall(tracker)
        pplx.tracker.set_current_step("research")
    else:
        # Previous research/brief are replaced only once phase A has produced something.
        tmp_dir = outdir / "01_research.tmp"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        pplx = PerplexityCall(LockedTracker(outdir=outdir / "_run_costs.tmp"))
        pplx.tracker.set_current_step("research")
        failed += run_research(jobs_a, tmp_dir, pplx, windows, args.model, "Phase A")
        if len(failed) == len(jobs_a):
            shutil.rmtree(tmp_dir, ignore_errors=True)
            shutil.rmtree(outdir / "_run_costs.tmp", ignore_errors=True)
            raise RuntimeError(f"All {len(jobs_a)} phase A searches failed (see run.log)")
        if research_dir.exists():
            shutil.rmtree(research_dir)
        tmp_dir.rename(research_dir)
        (outdir / "02_brief.md").unlink(missing_ok=True)
        pplx.tracker.outdir = outdir
        pplx.tracker.flush()
        shutil.rmtree(outdir / "_run_costs.tmp", ignore_errors=True)

    if not args.skip_research and not args.no_followups:
        research = load_research(research_dir)
        status_mod.write_status(outdir, "running", stage="research",
                                extra={"detail": "Planning follow-up threads"})
        threads = plan_threads(asof=asof, cutoff=cutoff, tape_block=tape_block,
                               research=research, outdir=outdir, tracker=pplx.tracker)
        gaps = select_gaps(lineage, moves, ticker_sections(research))
        logger.info("Gap fills: %d: %s", len(gaps), ", ".join(gaps))
        sections = ticker_sections(research)
        jobs_bc = [
            {"name": f"b_{t['slug']}", "kind": "thread", "group": "themes",
             "window": "session", "prompt": P.thread_prompt(t, session_date, asof, cutoff)}
            for t in threads
        ] + [
            {"name": f"c_{sym}", "kind": "gap", "group": "tickers", "window": "session",
             "prompt": P.gap_prompt(table([sym]), sym,
                                    by_ticker.get(sym, {}).get("company_name") or sym,
                                    session_date, asof, cutoff, sections.get(sym, ""))}
            for sym in gaps
        ]
        failed += run_research(jobs_bc, research_dir, pplx, windows, args.model, "Phase B/C")

    research = load_research(research_dir)
    status_mod.write_status(outdir, "running", stage="compact")
    ticker_ledger = build_ticker_ledger(lineage, moves, ticker_sections(research))
    channel_ledger = build_channel_ledger(asof=asof, cutoff=cutoff, research=research,
                                          outdir=outdir, tracker=pplx.tracker,
                                          compact=args.compact and not args.no_compact)

    run_synthesis(asof=asof, cutoff=cutoff, outdir=outdir, overview=overview,
                  tape_block=tape_block, synth=args.synth, channel_ledger=channel_ledger,
                  ticker_ledger=ticker_ledger, pplx=pplx, revise=not args.no_revise,
                  verify=args.verify)

    tracker = pplx.tracker
    compare_path = compare(asof, outdir) if args.eval else None
    if args.eval:
        status_mod.write_status(outdir, "running", stage="eval")
        try:
            eval_nuance(asof, outdir, tracker)
        except Exception as e:  # noqa: BLE001
            logger.warning("Nuance eval failed: %s", e)
    tracker.set_current_step(None)
    tracker.flush()
    status_mod.write_status(
        outdir, "complete", stage="done",
        extra={
            "total_cost_usd": round(tracker.total_cost_usd, 4),
            "call_count": len(tracker.calls),
            **({"detail": f"{len(failed)} searches failed: {', '.join(failed)}"} if failed else {}),
        },
    )

    print(f"\nBrief:   {outdir / '02_brief.md'}")
    print(f"Cost:    ${tracker.total_cost_usd:.4f} across {len(tracker.calls)} calls → run_costs.json")
    if compare_path:
        print(f"Compare: {compare_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
