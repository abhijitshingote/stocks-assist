"""Experimental market brief with Perplexity web search as the news source (no Benzinga).

Hydration comes from Postgres (screener universe + OHLC tape). News comes from
Perplexity `sonar-pro` web search: 3 broad market calls + ticker batches (12/call).
Synthesis reuses STEP4_SYSTEM_PROMPT (default Sonnet) so the output is directly
comparable to ``user_data/market_brief/<date>/02_brief.md``.

Run inside the backend container:

    docker compose exec backend python -m market_brief.perplexity_brief
    docker compose exec backend python -m market_brief.perplexity_brief --dry-run
    docker compose exec backend python -m market_brief.perplexity_brief --skip-research --synth haiku
    docker compose exec backend python -m market_brief.perplexity_brief --asof 2026-09-17 --compare-only

Artifacts: ``user_data/market_brief_perplexity/<date>/`` (not listed by the UI).
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
CONCURRENCY = 3
BATCH_SIZE = 12
ANTHROPIC_SYNTH_MODELS: dict[str, str] = {
    "sonnet": os.getenv("MARKET_BRIEF_PPLX_SONNET_MODEL", "claude-sonnet-4-6"),
    "haiku": os.getenv("MARKET_BRIEF_PPLX_HAIKU_MODEL", "claude-haiku-4-5"),
    "opus": os.getenv("MARKET_BRIEF_OPUS_MODEL", "claude-opus-4-6"),
}
RETRIES = 3

CATEGORY_VOCAB = (
    "AI Compute · Memory & Interconnect · Optical & Photonics · Chip Equipment · "
    "Fab & Foundry · Wireless & Mobile · Analog & Mixed-Signal · Power & Wide-Bandgap · "
    "Test & Advanced Packaging · Specialty Materials & IP Licensing · Quantum Computing · "
    "EdgeAI · Semiconductors · AI Infrastructure · Software & SaaS · Internet & Platforms · "
    "Communications & Networking"
)

# Broad market calls (replace Benzinga GENERAL_CHANNEL_FETCHES). One web-search call each.
BROAD_PROBES: list[dict[str, str]] = [
    {
        "slug": "macro_cross_asset",
        "label": "Macro & cross-asset",
        "focus": (
            "- US index closes: S&P 500, Nasdaq Composite, Dow, Russell 2000, PHLX SOX (% change).\n"
            "- Rates/FX: 2-yr and 10-yr Treasury yields, DXY; Treasury auctions.\n"
            "- Economic data released: actual vs consensus vs prior.\n"
            "- Fed/FOMC decisions and named speaker quotes; ECB/BoE/BoJ/PBoC actions.\n"
            "- Commodities: WTI, Brent, natural gas, gold, silver, copper (price, % change); OPEC.\n"
            "- Crypto: Bitcoin, Ether (price, % change); SEC/CFTC/Congress crypto actions; ETF flows.\n"
            "- Policy/geopolitics: tariffs, export controls, sanctions, wars moving oil or risk assets."
        ),
    },
    {
        "slug": "corporate_news",
        "label": "Corporate news flow",
        "focus": (
            "- Biggest US movers (market cap >= $2B) in the session, after-hours, and pre-market, with the "
            "catalyst for each (CNBC / MarketWatch / Barron's / Reuters 'stocks making the biggest moves').\n"
            "- Earnings after the close or pre-market: EPS and revenue actual vs consensus, guidance old → new.\n"
            "- Notable analyst actions: firm, rating old → new, PT old → new.\n"
            "- M&A (terms, value), contracts with $ size, FDA decisions, SEC filings (8-K, 13D, S-1, "
            "offerings), buybacks, executive changes, index adds/deletes, activist stakes.\n"
            "- Sector threads: AI capex / hyperscaler spend, semis and memory pricing, AI networking and "
            "optics, data-center power, software/SaaS; supply-chain reports (Nikkei, DigiTimes, TrendForce)."
        ),
    },
    {
        "slug": "calendar",
        "label": "Calendar",
        "focus": (
            "Scheduled catalysts from the brief date through the next 5 NYSE sessions: earnings "
            "(date, BMO/AMC, EPS and revenue consensus) for US stocks >= $10B plus notable mid-caps, "
            "investor/analyst days, FDA PDUFA dates, major economic releases with consensus, Fed "
            "speakers, options expiry, index rebalances, lockup expiries."
        ),
    },
]

SYNTH_CHOICES = ("sonnet", "haiku", "opus", "perplexity")


# ---------------------------------------------------------------------------
# Perplexity client (returns content + search_results + cost)
# ---------------------------------------------------------------------------


class PerplexityCall:
    """Perplexity chat completions; each successful call is appended to ``tracker``."""

    def __init__(self, tracker: CostTracker) -> None:
        self.tracker = tracker
        self.lock = threading.Lock()

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
            if date_window:
                payload["search_after_date_filter"] = date_window[0]
                payload["search_before_date_filter"] = date_window[1]

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
                with self.lock:
                    self.tracker.calls = [c for c in self.tracker.calls if c.step != label]
                    self.tracker.calls.append(
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
                    self.tracker.flush()
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
            if r.status_code == 429 or r.status_code >= 500:
                logger.warning("PPLX RETRY %s %d/%d: %s", label, attempt, RETRIES, last_err)
                time.sleep(10 * attempt)
                continue
            break
        raise RuntimeError(f"Perplexity failed for {label}: {last_err}")


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
# Prompts
# ---------------------------------------------------------------------------


def _pretty(d: str) -> str:
    return datetime.strptime(d, "%Y-%m-%d").strftime("%A %B %-d, %Y")


def _window_text(session_date: str, asof: str) -> str:
    return (
        f"{_pretty(session_date)} regular session (9:30-16:00 ET), after-hours that evening, "
        f"and overnight/pre-market news through {_pretty(asof)} ~6:00 AM ET"
    )


_FACT_RULES = f"""RULES
- Facts only. Every bullet has a specific number, name, or date. No bullet without a data point.
- End every bullet with the named source in parentheses: (Reuters), (company 8-K), (CNBC interview),
  (Morgan Stanley note). "Analysts said" / "reports suggest" without a name is not allowed.
- Earnings: actual vs consensus for EPS and revenue; guidance old → new.
- Analyst actions: firm, rating old → new, PT old → new.
- Mark unconfirmed items (people familiar, leaks, social media, M&A speculation) with [rumor].
- Only facts published in the window below. Older background only if it is the direct cause of this
  move, and label it with its date.
- Do not interpret price action. Do not write "strong", "beat expectations", "investors cheered",
  "remains", "continues to", "well-positioned", "tailwinds". Write the number.
- Do not drop facts to save space."""


def ticker_batch_prompt(
    tickers: list[str],
    lineage: dict[str, Any],
    moves: dict[str, float],
    session_date: str,
    asof: str,
) -> str:
    by_ticker = lineage.get("by_ticker") or {}
    rows = [
        "| Ticker | Company | Cap | Screen | 1D close | 5D | Vol× 10d | Vol/gap event |",
        "|---|---|---|---|---:|---:|---:|---|",
    ]
    for sym in tickers:
        d = by_ticker.get(sym, {})
        d1 = moves.get(sym, d.get("dr_1"))
        ev = f"{d.get('last_event_type')} {d.get('last_event_date')}" if d.get("last_event_date") else "—"
        rows.append(
            "| {t} | {c} | {cap} | {sec} | {d1} | {d5} | {vol} | {ev} |".format(
                t=sym,
                c=d.get("company_name") or "",
                cap=_cap_label(d.get("market_cap")),
                sec=d.get("label") or d.get("section") or "",
                d1=f"{d1:+.2f}%" if d1 is not None else "n/a",
                d5=f"{d['dr_5']:+.1f}%" if d.get("dr_5") is not None else "n/a",
                vol=f"{d['vol_vs_10d_avg']:.1f}×" if d.get("vol_vs_10d_avg") is not None else "n/a",
                ev=ev,
            )
        )
    table = "\n".join(rows)
    return f"""Search the web now for news on each stock below. You are a financial fact extractor for a
pre-market brief. Brief date: {_pretty(asof)}.

Window: {_window_text(session_date, asof)}.

VERIFIED TAPE (from exchange OHLC data; authoritative, do not restate different prices):
{table}

TASK
For EACH ticker, find why it moved on {_pretty(session_date)} and any other material news in the window.
Search press releases (BusinessWire, PR Newswire, GlobeNewswire), SEC filings, Reuters, Bloomberg, CNBC,
WSJ, Barron's, MarketWatch, Investor's Business Daily, company IR pages, analyst-action roundups.
If a big mover (|1D| >= 5% or 5D >= 15%) has no company news, check for sector sympathy and name the
lead stock it followed.

OUTPUT (markdown; one section per ticker, in the order given; no intro or conclusion):

## Company Name (TICKER) `[Category]`
- fact (source)
- fact (source)

Category: pick from {CATEGORY_VOCAB}. If none fit, invent a concise category.
If you find nothing, write exactly one bullet: "- No company-specific catalyst found in window" and,
if applicable, a second bullet naming the sympathy driver.

Include when available: catalyst for the move, earnings figures, guidance, analyst actions, contracts
with $ size, filings, management quotes with numbers, next catalyst date with consensus estimates.

{_FACT_RULES}"""


def channel_prompt(probe: dict[str, str], session_date: str, asof: str) -> str:
    return f"""Search the web now. You are a financial fact extractor for a US-equity pre-market brief.
Brief date: {_pretty(asof)}. Scope: {probe['label']}.

Window: {_window_text(session_date, asof)}.

COVER EVERY ITEM BELOW (run a separate search for each bullet; do not skip one because another was rich)
{probe['focus']}

Prefer primary sources and wire services (Reuters, Bloomberg, AP, CNBC, WSJ, MarketWatch, Barron's,
company press releases, SEC EDGAR, BLS/BEA/Fed releases).

OUTPUT (markdown; no intro or conclusion). Group by ticker or topic:

## TICKER or Topic `[Category]`
- fact (source)

Category: pick from {CATEGORY_VOCAB}, or Macro / Commodities / Geopolitics / Crypto / Calendar, or
invent a concise one. Bold tickers.

{_FACT_RULES}"""


# ---------------------------------------------------------------------------
# Research + synthesis
# ---------------------------------------------------------------------------


def _date_window(session_date: str, asof: str) -> tuple[str, str]:
    start = datetime.strptime(session_date, "%Y-%m-%d") - timedelta(days=1)
    end = datetime.strptime(asof, "%Y-%m-%d") + timedelta(days=1)
    return f"{start.month}/{start.day}/{start.year}", f"{end.month}/{end.day}/{end.year}"


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
    jobs: list[tuple[str, str, str]],
    research_dir: Path,
    pplx: PerplexityCall,
    date_window: tuple[str, str] | None,
    model: str,
) -> list[str]:
    """jobs: (name, kind, prompt). Writes <name>.md (content + sources) and <name>.json."""
    research_dir.mkdir(parents=True, exist_ok=True)
    outdir = research_dir.parent
    total = len(jobs)
    done: list[str] = []
    failed: list[str] = []

    def _progress() -> None:
        status_mod.write_status(
            outdir, "running", stage="research",
            extra={"detail": f"{len(done)}/{total} Perplexity searches done"
                   + (f" · {len(failed)} failed" if failed else "")},
        )

    _progress()

    def _one(job: tuple[str, str, str]) -> None:
        name, kind, prompt = job
        try:
            res = pplx(
                label=name,
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=RESEARCH_MAX_TOKENS,
                timeout=RESEARCH_TIMEOUT_SECONDS,
                date_window=date_window,
            )
        except Exception as e:  # noqa: BLE001
            logger.error("Research failed %s: %s", name, e)
            res = {"content": f"_Research call failed: {e}_", "search_results": [], "citations": []}
            with pplx.lock:
                failed.append(name)
        (research_dir / f"{name}.json").write_text(
            json.dumps({"name": name, "kind": kind, "prompt": prompt, **res}, indent=2),
            encoding="utf-8",
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

    if len(failed) == total:
        raise RuntimeError(f"All {total} Perplexity searches failed (see run.log)")
    if failed:
        logger.warning("Research: %d/%d searches failed: %s", len(failed), total, ", ".join(failed))
    return failed


def load_research_text(research_dir: Path) -> tuple[str, str]:
    """(channel_text, ticker_text) from 01_research/*.json content (sources excluded)."""
    channel_parts: list[str] = []
    ticker_parts: list[str] = []
    for path in sorted(research_dir.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        block = f"<summary source=\"{data['name']}\">\n{data['content'].strip()}\n</summary>"
        (channel_parts if data["kind"] == "channel" else ticker_parts).append(block)
    return "\n\n".join(channel_parts), "\n\n".join(ticker_parts)


def run_synthesis(
    *,
    asof: str,
    outdir: Path,
    overview: str,
    tape_block: str,
    synth: str,
    pplx: PerplexityCall,
) -> Path:
    channel_text, ticker_text = load_research_text(outdir / "01_research")
    user_msg = step4_user_message(
        date_str=asof,
        ticker_universe=f"{tape_block}\n\n{overview}",
        channel_summaries=channel_text,
        ticker_summaries=ticker_text,
    )
    (outdir / "02_synth_input.md").write_text(user_msg, encoding="utf-8")
    logger.info("Synthesis input: %s chars (%s)", f"{len(user_msg):,}", synth)

    step = f"synthesis_{synth}"
    pplx.tracker.set_current_step(step)
    status_mod.write_status(outdir, "running", stage=step)
    if synth in ANTHROPIC_SYNTH_MODELS:
        from market_brief.anthropic_client import complete

        model = ANTHROPIC_SYNTH_MODELS[synth]
        brief = complete(
            model=model,
            logical_model=model,
            system=STEP4_SYSTEM_PROMPT,
            user_message=user_msg,
            step=step,
            tracker=pplx.tracker,
            max_tokens=16_000,
            use_stream=True,
            pace_after=False,
        )
    else:
        res = pplx(
            label=step,
            model=SYNTH_MODEL,
            messages=[
                {"role": "system", "content": STEP4_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=SYNTH_MAX_TOKENS,
            timeout=SYNTH_TIMEOUT_SECONDS,
            disable_search=True,
        )
        brief = res["content"]

    path = outdir / "02_brief.md"
    path.write_text(brief, encoding="utf-8")
    logger.info("Wrote %s", path)
    return path


# ---------------------------------------------------------------------------
# Compare vs Benzinga baseline
# ---------------------------------------------------------------------------

_ROW_RE = re.compile(r"^\|\s*\*\*([A-Z][A-Z0-9.\-]*)\*\*\s*\|\s*([^|]*)\|")


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
    p.add_argument("--universe", choices=("db", "baseline", "auto"), default="db",
                   help="db = own DB screener run (default, decoupled); "
                        "baseline = copy Benzinga run's lineage.json; auto = baseline if present")
    p.add_argument("--synth", choices=SYNTH_CHOICES, default="sonnet",
                   help="Step 4 synthesis model (same STEP4_SYSTEM_PROMPT as production)")
    p.add_argument("--model", default=RESEARCH_MODEL, help="Perplexity research model")
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    p.add_argument("--no-date-filter", action="store_true",
                   help="Drop search_after/before_date_filter (live runs only)")
    p.add_argument("--dry-run", action="store_true", help="Write prompts to 00_prompts/; no API calls")
    p.add_argument("--skip-research", action="store_true", help="Reuse existing 01_research/")
    p.add_argument("--compare-only", action="store_true", help="Only rebuild compare.md")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    asof = args.asof or datetime.now(ET).strftime("%Y-%m-%d")
    outdir = OUTPUTS_DIR / asof
    _setup_logging(outdir, args.verbose, to_file=not (args.dry_run or args.compare_only))

    if args.compare_only:
        path = compare(asof, outdir)
        print(path.read_text(encoding="utf-8") if path else "Nothing to compare")
        return 0

    try:
        return _run(args, asof, outdir)
    except Exception as e:  # noqa: BLE001
        logger.exception("Pipeline failed: %s", e)
        if not args.dry_run:
            status_mod.write_status(outdir, "failed", stage="error", error=str(e)[:500])
        return 1


def _run(args: argparse.Namespace, asof: str, outdir: Path) -> int:
    if not args.dry_run:
        status_mod.write_status(outdir, "running", stage="hydrate")
        stale = outdir / "02_brief.md"
        if stale.exists() and not args.skip_research:
            stale.unlink()

    lineage, overview = load_universe(asof, outdir, args.universe)
    tickers = sorted((lineage.get("by_ticker") or {}).keys())
    session_date, moves, tape_block = load_tape(asof, tickers)
    expected_session = prior_session_for_brief(asof).isoformat()
    if session_date != expected_session:
        logger.warning("OHLC latest session %s != expected %s (run daily_price_update?)",
                       session_date, expected_session)
    (outdir / "tape.md").write_text(tape_block, encoding="utf-8")

    jobs: list[tuple[str, str, str]] = [
        (f"broad_{pr['slug']}", "channel", channel_prompt(pr, session_date, asof))
        for pr in BROAD_PROBES
    ]
    jobs += [
        (name, "ticker", ticker_batch_prompt(syms, lineage, moves, session_date, asof))
        for name, syms in build_batches(lineage, args.batch_size)
    ]
    date_window = None if args.no_date_filter else _date_window(session_date, asof)
    logger.info("Research jobs: %d (%d broad, %d ticker batches) · date filter %s",
                len(jobs), len(BROAD_PROBES), len(jobs) - len(BROAD_PROBES), date_window)

    if args.dry_run:
        pdir = outdir / "00_prompts"
        if pdir.exists():
            shutil.rmtree(pdir)
        pdir.mkdir(parents=True, exist_ok=True)
        for name, _, prompt in jobs:
            (pdir / f"{name}.md").write_text(prompt, encoding="utf-8")
        print(f"Wrote {len(jobs)} prompts → {pdir}")
        return 0

    research_dir = outdir / "01_research"
    failed: list[str] = []
    if args.skip_research:
        if not any(research_dir.glob("*.json")):
            raise FileNotFoundError(f"No research at {research_dir} — run without --skip-research")
        tracker = CostTracker.load_or_create(outdir)
        tracker.calls = [c for c in tracker.calls if not c.step.startswith("synthesis_")]
        pplx = PerplexityCall(tracker)
    else:
        if research_dir.exists():
            shutil.rmtree(research_dir)
        pplx = PerplexityCall(CostTracker(outdir=outdir))
        pplx.tracker.set_current_step("research")
        failed = run_research(jobs, research_dir, pplx, date_window, args.model)

    run_synthesis(asof=asof, outdir=outdir, overview=overview, tape_block=tape_block,
                  synth=args.synth, pplx=pplx)

    tracker = pplx.tracker
    tracker.set_current_step(None)
    tracker.flush()
    compare_path = compare(asof, outdir)
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
