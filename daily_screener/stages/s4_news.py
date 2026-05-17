"""Stage 4: News Enrichment.

For every Stage-2 survivor, ask Perplexity *why this stock is moving*. The
prompt is framed around the actual price action (5d / 20d / 60d return,
volume vs avg, last event type) rather than a generic "any material news?"
question, because the generic framing reliably loses to whatever 8-K was
filed most recently and misses the longer-running rumor / adoption /
analyst-note catalysts that drive big small-mid-cap moves.

Pipeline per ticker:

  1. Pass 1: "what's driving the move?" — broad search, enumerate plausible
     drivers with confidence levels.
  2. Plausibility / lead check on the pass-1 output:
       - If pass-1 returned no material news but the stock is up >= X% in 5d
         on elevated volume, that is implausible -> run a verification pass.
       - If pass-1 surfaced rumor-style leads (Kuo / analyst note, unnamed
         partner, "potential adoption", patent analysis) -> run a focused
         verification pass to confirm/refute.
  3. Verification pass (when triggered): targeted query that nails down the
     specific leads from pass-1.
  4. Merge: combine pass-1 and verification into the final result, keeping
     the richer answer.

If the trader has a watchlist note for the ticker, we attach it as a hint to
pass 1 (helps Perplexity focus) but the pipeline is designed to succeed
WITHOUT one.

Concurrent (ThreadPoolExecutor, NEWS_CONCURRENCY in flight). Results are
written per-ticker to outputs/<date>/03_news/<ticker>.json AND rolled up into
outputs/<date>/03_news.json. Failures are recorded (with reason) and passed
through to the audit stage as `dropped_at_stage: 4`.
"""

from __future__ import annotations

import concurrent.futures
import logging
import re
from typing import Any

from daily_screener import config
from daily_screener.utils import io
from daily_screener.utils.claude import extract_json
from daily_screener.utils.comments import watchlist_notes_only
from daily_screener.utils.perplexity import PerplexityError, call_perplexity

logger = logging.getLogger(__name__)


# Cap the optional watchlist note we paste into the prompt so a single long
# entry doesn't blow the token budget.
_WATCHLIST_NOTE_CHAR_CAP = 1500

# Phrases in the pass-1 summary or drivers that strongly suggest there's a
# rumor / unconfirmed catalyst worth a verification round-trip. Matched
# case-insensitively against the concatenated summary+drivers blob.
_RUMOR_LEAD_PATTERNS = [
    r"\brumor",
    r"\brumou?red\b",
    r"\bunconfirmed\b",
    r"\bpotential(?:ly)? (?:adopted|adoption|partnership|customer|deal|win)\b",
    r"\bsupply[- ]chain (?:check|report|note|commentary)\b",
    r"\bchannel check",
    r"\bKuo\b",  # Ming-Chi Kuo style supply-chain analyst notes
    r"\bDigiTimes\b",
    r"\banalyst note\b",
    r"\bpatent (?:analysis|filing|application)\b",
    r"\bunnamed (?:hyperscaler|customer|partner|client)\b",
    r"\brepresentative (?:client|customer)\b",
    r"\bmajor (?:hyperscaler|customer|partner) (?:reportedly|rumored)\b",
    r"\bdesign[- ]win(?:s)?\b",
]
_RUMOR_LEAD_RE = re.compile("|".join(_RUMOR_LEAD_PATTERNS), re.IGNORECASE)

# Phrases that signal the pass-1 driver is *weak* relative to a big move
# (technical / sympathy / vague-recovery framing). When pass-1 leans on this
# language and the move is big, we don't trust the answer — force a verify
# pass that explicitly hunts for named customers / analyst reports.
_WEAK_DRIVER_PATTERNS = [
    r"\btechnical breakout\b",
    r"\bmomentum (?:chase|trade|fund)",
    r"\bRSI(?:\W+(?:at|of|near)\W+(?:100|extreme|overbought))?",
    r"\boverbought\b",
    r"\bshort[- ]covering\b",
    r"\bsqueeze\b",
    r"\bsystematic[- ]?(?:buy|flow)",
    r"\bretail chase\b",
    r"\bsympathy (?:move|trade|rally)\b",
    r"\bfeedback loop\b",
    r"\bnew (?:all[- ]time )?high(?:s)?\b",
    r"\b(?:no|without) (?:a )?(?:discrete|specific|named) (?:catalyst|customer|contract)\b",
    r"\bnot (?:a )?(?:single|fresh|transformational|new) (?:catalyst|deal|customer)\b",
    r"\bnot enough on its own\b",
    r"\bgradual recovery\b",
    r"\bin[- ]line (?:with )?(?:consensus|estimates|expectations)\b",
    r"\bslightly beat\b",
    r"\bmodest(?:ly)? beat\b",
]
_WEAK_DRIVER_RE = re.compile("|".join(_WEAK_DRIVER_PATTERNS), re.IGNORECASE)

# Phrases that, if present, indicate pass-1 DID surface a strong named driver
# — in which case we should NOT trigger a weak-driver verify even if some
# soft-language is also present.
_STRONG_DRIVER_PATTERNS = [
    r"\bsigned\b",
    r"\bawarded\b",
    r"\bcontract (?:with|signed)\b",
    r"\bacquisition (?:of|by)\b",
    r"\bdefinitive agreement\b",
    r"\bnamed (?:customer|partner|hyperscaler)\b",
    # Specific marquee buyers — if any of these are mentioned by name we
    # already have a story worth keeping.
    r"\b(?:Apple|NVIDIA|TSMC|Google|Alphabet|Microsoft|Amazon|AWS|Meta|Tesla|OpenAI|Anthropic|Oracle)\b",
    r"\bFDA (?:approval|clearance|breakthrough)\b",
    r"\b(?:raised|raising) (?:full[- ]year )?guidance\b",
    r"\bguidance raise\b",
    r"\bbeat[- ]and[- ]raise\b",
]
_STRONG_DRIVER_RE = re.compile("|".join(_STRONG_DRIVER_PATTERNS), re.IGNORECASE)


# ---------------------------------------------------------------------------
# Watchlist hint (optional, never required)
# ---------------------------------------------------------------------------

def _load_watchlist_notes() -> dict[str, str]:
    """Return ``{TICKER: note}`` for tickers that BOTH appear on the
    watchlist AND have a non-empty comment in ``user_data/abi_comments.json``.

    Comments outside the watchlist are intentionally excluded - free-floating
    notes (e.g. on a stock the trader hasn't formally added to the watchlist)
    must NOT influence Stage 4. Used as an OPTIONAL hint only; Stage 4 is
    designed to work without any notes.
    """
    notes = watchlist_notes_only()
    return {tk: note[:_WATCHLIST_NOTE_CHAR_CAP] for tk, note in notes.items()}


# ---------------------------------------------------------------------------
# Price-action context block
# ---------------------------------------------------------------------------

def _format_price_action(survivor: dict[str, Any]) -> str:
    """Compact, model-friendly summary of the stock's recent move. This is the
    *primary* framing for the news fetch: we are explaining a move, not
    answering an abstract question."""
    def _pct(x):
        if x is None:
            return "n/a"
        return f"{float(x):+.1f}%"

    parts = [
        f"- 1d return: {_pct(survivor.get('dr_1'))}",
        f"- 5d return: {_pct(survivor.get('dr_5'))}",
        f"- 20d return: {_pct(survivor.get('dr_20'))}",
        f"- 60d return: {_pct(survivor.get('dr_60'))}",
        f"- 20-day ATR: {survivor.get('atr20', 'n/a')}%",
        f"- volume vs 10d avg: {survivor.get('vol_vs_10d_avg', 'n/a')}x",
        f"- RSI mktcap percentile: {survivor.get('rsi_mktcap', 'n/a')}",
    ]
    last_evt = survivor.get("last_event_date")
    if last_evt:
        parts.append(
            f"- last event: {last_evt} ({survivor.get('last_event_type')}, "
            f"magnitude={survivor.get('last_event_magnitude')})"
        )
    sources = survivor.get("sources") or []
    if sources:
        parts.append(f"- appears on screens: {', '.join(sources)}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Pass 1: "what's driving the move?"
# ---------------------------------------------------------------------------

def _build_pass1_prompt(
    survivor: dict[str, Any],
    theme_tags: list[str],
    watchlist_note: str | None,
) -> str:
    template = io.load_prompt("ticker_news")
    if theme_tags:
        tag_list = "\n".join(f"- `{t}`" for t in theme_tags)
    else:
        tag_list = "(no theme vector available - infer from the news itself)"

    if watchlist_note:
        hint_block = (
            "(Optional hint — the trader has this ticker on their watchlist "
            "with the following note. Use it to bias your search but do NOT "
            "treat it as authoritative; verify independently.)\n\n"
            f'"""\n{watchlist_note}\n"""'
        )
    else:
        hint_block = "(no prior thesis on file — research independently)"

    return (
        template.replace("{ticker}", survivor["ticker"])
        .replace("{company_name}", survivor.get("company_name") or survivor["ticker"])
        .replace("{price_action_block}", _format_price_action(survivor))
        .replace("{theme_tags_list}", tag_list)
        .replace("{watchlist_thesis_block}", hint_block)
    )


def _call_and_parse(prompt: str, max_tokens: int) -> tuple[dict[str, Any] | None, str | None, str | None]:
    """Returns (parsed_dict_or_None, raw_text, error_message)."""
    try:
        raw = call_perplexity(prompt, max_tokens=max_tokens)
    except PerplexityError as e:
        return None, None, str(e)
    try:
        parsed = extract_json(raw)
    except ValueError as e:
        return None, raw, f"parse_error: {e}"
    if not isinstance(parsed, dict):
        return None, raw, "non_dict_response"
    return parsed, raw, None


# ---------------------------------------------------------------------------
# Pass 2: targeted verification
# ---------------------------------------------------------------------------

def _pass1_text_blob(parsed: dict[str, Any]) -> str:
    blob_parts = [parsed.get("summary") or ""]
    for d in parsed.get("headline_drivers") or []:
        if isinstance(d, str):
            blob_parts.append(d)
    return " ".join(blob_parts)


def _looks_like_rumor_lead(parsed: dict[str, Any]) -> bool:
    """Did pass-1 surface a lead that warrants a focused second query?"""
    return bool(_RUMOR_LEAD_RE.search(_pass1_text_blob(parsed)))


def _looks_like_weak_driver(parsed: dict[str, Any]) -> bool:
    """Pass-1 claimed material news but the framing is technical/sympathy/
    vague-recovery — i.e. the model rationalized the move rather than finding
    a real catalyst. We treat that as a signal to verify."""
    blob = _pass1_text_blob(parsed)
    if not blob.strip():
        return False
    if _STRONG_DRIVER_RE.search(blob):
        return False
    return bool(_WEAK_DRIVER_RE.search(blob))


def _needs_verification(parsed: dict[str, Any], survivor: dict[str, Any]) -> tuple[bool, str]:
    """Decide whether to spend a second Perplexity call. Returns (should_verify, reason)."""
    if not config.NEWS_VERIFY_ENABLED:
        return False, "verify_disabled"

    dr5 = survivor.get("dr_5") or 0.0
    dr20 = survivor.get("dr_20") or 0.0
    has_news = bool(parsed.get("has_material_news"))

    # Case 1: pass-1 said "nothing material" but the move is too big to ignore.
    if not has_news and dr5 >= config.NEWS_VERIFY_DR5_NO_CATALYST_PCT:
        return True, f"big_move_no_catalyst (dr5={dr5:.1f}%)"

    # Case 2: pass-1 surfaced a rumor-style lead worth confirming.
    if config.NEWS_VERIFY_ON_RUMOR_LEAD and _looks_like_rumor_lead(parsed):
        return True, "rumor_lead_detected"

    # Case 3: pass-1 claimed material news but leaned on weak/technical framing
    # for a big move. Don't trust it - the real story is probably hiding under
    # the "momentum chase" rationalization.
    if (
        has_news
        and (dr5 >= config.NEWS_VERIFY_DR5_WEAK_CATALYST_PCT or dr20 >= 60.0)
        and _looks_like_weak_driver(parsed)
    ):
        return True, f"weak_driver_big_move (dr5={dr5:.1f}%, dr20={dr20:.1f}%)"

    return False, "skip_verify"


def _build_verify_prompt(
    survivor: dict[str, Any],
    pass1: dict[str, Any],
    verify_reason: str,
) -> str:
    """A short, focused follow-up. We give it pass-1's findings and ask for
    confirmation, refutation, or anything pass-1 missed — with concrete
    sources / dates / named entities."""
    pass1_summary = (pass1.get("summary") or "").strip()
    drivers = pass1.get("headline_drivers") or []
    drivers_block = "\n".join(f"- {d}" for d in drivers) if drivers else "(none reported)"

    framing = ""
    if verify_reason.startswith("big_move_no_catalyst"):
        framing = (
            "The first-pass research returned no material catalyst, but the "
            "stock has moved sharply (see price action below). That is "
            "implausible — there is almost certainly a driver. Look harder, "
            "specifically at: rumored or unconfirmed named-customer adoption "
            "(especially Apple, NVIDIA, TSMC, Google, Microsoft, Amazon, Meta, "
            "Tesla); supply-chain analyst notes (Ming-Chi Kuo, DigiTimes, "
            "TF International, Wedbush, Loop, Needham); sector sympathy moves "
            "tied to a specific peer's news; short-interest squeezes; insider "
            "or 13F changes; recent patent filings or design wins; and any "
            "industry-conference commentary in the last 30 days."
        )
    elif verify_reason.startswith("weak_driver_big_move"):
        framing = (
            "The first-pass research rationalized the price move as a "
            "technical / momentum / sympathy / 'in-line earnings' story. For "
            "a move this large, that explanation is almost certainly "
            "incomplete — momentum funds and short covering are downstream "
            "effects of a catalyst, not causes. There is very likely a "
            "specific, nameable fundamental driver underneath that pass-1 "
            "missed.\n\n"
            "**Specifically search for the following, by name:**\n"
            "- Has any **marquee buyer (Apple, NVIDIA, TSMC, Google, "
            "Microsoft, Amazon, Meta, Tesla, OpenAI, Anthropic, major auto "
            "OEM, major defense prime)** been reported — even via rumor, "
            "supply-chain check, or patent analysis — as adopting / "
            "qualifying / mass-producing with this company in the last 60 "
            "days? Name the source and date.\n"
            "- Any **Ming-Chi Kuo (TF International), DigiTimes, Wedbush, "
            "Loop Capital, Needham, Morgan Stanley** supply-chain note on "
            "this company in the last 90 days?\n"
            "- Any **design win, qualification, mass-production milestone, "
            "or end-market window** (e.g. 'production for 2026-27 smart "
            "glasses', 'in-vehicle ramp for OEM X') that the market may have "
            "discovered before the press release?\n"
            "- Any **product/platform** (e.g. a flagship SKU or IP block) "
            "that has been newly tied to a major customer's roadmap?\n"
            "- Any **patent filing or recent conference disclosure** that "
            "implies a relationship with a marquee buyer?\n\n"
            "If your honest answer after a deep search is 'I still cannot "
            "find one', say so plainly. But you must look — do not rephrase "
            "the same technical/momentum framing back at me."
        )
    elif verify_reason == "rumor_lead_detected":
        framing = (
            "The first-pass research surfaced a rumor or analyst-note lead. "
            "Confirm or refute it: name the source (publication, analyst, "
            "filing), the date, the specific product / customer / dollar "
            "amount, and the current state (still active / refuted / no "
            "update). If the lead is real, surface any supporting evidence "
            "(patent filings, supplier disclosures, product roadmaps, "
            "executive commentary). If it has been denied, say so plainly."
        )
    else:
        framing = (
            "Look for any additional material catalyst the first pass may "
            "have missed."
        )

    return f"""You are doing a focused verification pass on **{survivor.get('company_name') or survivor['ticker']} ({survivor['ticker']})**.

### Why we're asking again

{framing}

### Price action

{_format_price_action(survivor)}

### What pass 1 returned

summary: {pass1_summary or '(empty)'}
drivers:
{drivers_block}

### Your job

Return a JSON object with the SAME schema as pass 1, but reflect what you find
in the verification round. If the verification adds, refines, or refutes
specific items, that should show up in `headline_drivers` and `summary`.
Always include named counterparties / dates / product names when you have
them. If you find nothing new, return your honest read — including a candid
"likely sympathy / technical move" call if that's what it is.

Schema (return ONLY a JSON object, no preamble):

```json
{{
  "ticker": "{survivor['ticker']}",
  "has_material_news": true,
  "headline_drivers": [
    "WiseEye AI image-sensing adopted by Apple for Vision Pro / smart glasses, mass production 2026-27 (Ming-Chi Kuo report, 2026-04-15)",
    "Q1 2026 EPS $0.18 beat $0.14 consensus, Q2 revenue guide +6% QoQ midpoint (2026-05-07)"
  ],
  "latest_material_date": "2026-05-07",
  "theme_tags": ["edgeai", "optical-and-photonics"],
  "summary": "Be specific. Name the partner / analyst / dollar figure / timing wherever possible."
}}
```
"""


# ---------------------------------------------------------------------------
# Merge pass-1 + verification into a single final result
# ---------------------------------------------------------------------------

def _merge_results(pass1: dict[str, Any], pass2: dict[str, Any] | None) -> dict[str, Any]:
    """Prefer pass-2 fields when they are non-empty / richer; otherwise keep
    pass-1. Drivers are de-duplicated and concatenated (pass-2 first)."""
    if not pass2:
        return pass1

    merged = dict(pass1)
    # Boolean: pass-2 can flip false -> true (verification found something
    # pass-1 missed) but should not silently flip true -> false unless pass-2
    # is explicit about refuting it.
    if pass2.get("has_material_news") is True:
        merged["has_material_news"] = True
    elif pass1.get("has_material_news") is True:
        merged["has_material_news"] = True
    else:
        merged["has_material_news"] = bool(pass2.get("has_material_news"))

    # Drivers: pass-2 first (it has the verified/refined items), then pass-1
    # items that pass-2 didn't already cover (rough de-dupe by lowercased prefix).
    seen: set[str] = set()
    out_drivers: list[str] = []
    for src in (pass2.get("headline_drivers") or [], pass1.get("headline_drivers") or []):
        for d in src:
            if not isinstance(d, str):
                continue
            key = d.strip().lower()[:80]
            if key and key not in seen:
                seen.add(key)
                out_drivers.append(d.strip())
    merged["headline_drivers"] = out_drivers

    # Theme tags: union, preserving pass-2 order first.
    tag_seen: set[str] = set()
    out_tags: list[str] = []
    for src in (pass2.get("theme_tags") or [], pass1.get("theme_tags") or []):
        for t in src:
            if not isinstance(t, str):
                continue
            if t not in tag_seen:
                tag_seen.add(t)
                out_tags.append(t)
    merged["theme_tags"] = out_tags

    # Use pass-2 summary if it's substantively longer or pass-1 is empty.
    p1_sum = (pass1.get("summary") or "").strip()
    p2_sum = (pass2.get("summary") or "").strip()
    if p2_sum and (not p1_sum or len(p2_sum) > len(p1_sum) * 0.6):
        merged["summary"] = p2_sum

    # Latest material date: keep the more recent one if both are valid.
    p1_dt = (pass1.get("latest_material_date") or "").strip()
    p2_dt = (pass2.get("latest_material_date") or "").strip()
    if p2_dt and (not p1_dt or p2_dt > p1_dt):
        merged["latest_material_date"] = p2_dt

    return merged


# ---------------------------------------------------------------------------
# Per-ticker driver
# ---------------------------------------------------------------------------

def _fetch_one(
    survivor: dict[str, Any],
    theme_tags: list[str],
    watchlist_note: str | None,
) -> dict[str, Any]:
    ticker = survivor["ticker"]

    # --- Pass 1 -----------------------------------------------------------
    p1_prompt = _build_pass1_prompt(survivor, theme_tags, watchlist_note)
    p1_parsed, p1_raw, p1_err = _call_and_parse(p1_prompt, max_tokens=1500)
    if p1_parsed is None:
        logger.warning("[s4] pass1 failed for %s: %s", ticker, p1_err)
        return {
            "ticker": ticker,
            "ok": False,
            "error": p1_err or "pass1_failed",
            "raw": p1_raw,
        }

    # --- Pass 2 (verification, conditional) -------------------------------
    should_verify, verify_reason = _needs_verification(p1_parsed, survivor)
    p2_parsed: dict[str, Any] | None = None
    p2_raw: str | None = None
    p2_err: str | None = None
    if should_verify:
        logger.info("[s4] verifying %s (%s)", ticker, verify_reason)
        p2_prompt = _build_verify_prompt(survivor, p1_parsed, verify_reason)
        p2_parsed, p2_raw, p2_err = _call_and_parse(
            p2_prompt, max_tokens=config.NEWS_VERIFY_MAX_TOKENS
        )
        if p2_parsed is None:
            # Verification failed; log but keep pass-1 as the result.
            logger.warning("[s4] verify failed for %s: %s", ticker, p2_err)

    merged = _merge_results(p1_parsed, p2_parsed)
    merged.setdefault("ticker", ticker)
    merged["ok"] = True
    merged["raw"] = p1_raw
    # Audit metadata so we can QA the verification logic without re-running.
    merged["_verify"] = {
        "triggered": should_verify,
        "reason": verify_reason,
        "pass2_ok": p2_parsed is not None,
        "pass2_error": p2_err,
        "pass2_raw": p2_raw,
    }
    return merged


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run(date_str: str | None = None, *, max_tickers: int | None = None) -> dict[str, Any]:
    date_str = date_str or io.today_str()
    logger.info("[s4] news enrichment for %s", date_str)

    momentum = io.read_json(io.artifact_path(date_str, "01_momentum.json"))
    theme_vec = io.read_json_if_exists(
        io.artifact_path(date_str, "02_theme_vector.json"),
        default={"themes": []},
    )
    theme_tags = [t["tag"] for t in theme_vec.get("themes", [])]

    survivors = momentum.get("survivors", [])
    if max_tickers is not None:
        survivors = survivors[:max_tickers]

    if not survivors:
        artifact = {"date": date_str, "results": [], "failures": []}
        io.write_json(io.artifact_path(date_str, "03_news.json"), artifact)
        return artifact

    watchlist_notes = _load_watchlist_notes()
    seeded_count = sum(1 for s in survivors if watchlist_notes.get(s["ticker"].upper()))
    if seeded_count:
        logger.info(
            "[s4] %d/%d tickers have an optional watchlist hint attached",
            seeded_count, len(survivors),
        )

    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    verify_count = 0

    workers = max(1, config.NEWS_CONCURRENCY)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _fetch_one,
                s,
                theme_tags,
                watchlist_notes.get(s["ticker"].upper()),
            ): s
            for s in survivors
        }
        for fut in concurrent.futures.as_completed(futures):
            survivor = futures[fut]
            ticker = survivor["ticker"]
            try:
                result = fut.result()
            except Exception as e:  # noqa: BLE001
                logger.exception("[s4] unexpected failure for %s", ticker)
                result = {"ticker": ticker, "ok": False, "error": f"exception: {e}"}

            # Persist per-ticker file (includes raw + verify metadata).
            per_ticker_path = io.news_subdir(date_str) / f"{ticker}.json"
            io.write_json(per_ticker_path, result)

            if result.get("ok"):
                if (result.get("_verify") or {}).get("triggered"):
                    verify_count += 1
                results.append(
                    {
                        "ticker": ticker,
                        "company_name": survivor.get("company_name"),
                        "has_material_news": result.get("has_material_news", False),
                        "headline_drivers": result.get("headline_drivers", []),
                        "latest_material_date": result.get("latest_material_date"),
                        "theme_tags": result.get("theme_tags", []),
                        "summary": result.get("summary", ""),
                        "watchlist_thesis": watchlist_notes.get(ticker.upper()),
                        "verify_triggered": bool((result.get("_verify") or {}).get("triggered")),
                        "verify_reason": (result.get("_verify") or {}).get("reason"),
                    }
                )
            else:
                failures.append(
                    {
                        "ticker": ticker,
                        "company_name": survivor.get("company_name"),
                        "drop_reason": f"news_fetch_failed: {result.get('error')}",
                    }
                )

    artifact = {
        "date": date_str,
        "model": config.PERPLEXITY_MODEL,
        "requested_count": len(survivors),
        "result_count": len(results),
        "failure_count": len(failures),
        "verify_count": verify_count,
        "results": results,
        "failures": failures,
    }
    io.write_json(io.artifact_path(date_str, "03_news.json"), artifact)
    logger.info(
        "[s4] success=%d, failed=%d, verified=%d",
        len(results), len(failures), verify_count,
    )
    return artifact


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    parser.add_argument("--max-tickers", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run(args.date, max_tickers=args.max_tickers)


if __name__ == "__main__":
    main()
