"""Stage 5: Claude Judge.

For each ticker that has news from Stage 4, ask Claude to grade
`news_materiality` and `theme_fit` and emit a verdict (PICK/WATCH/SKIP) with
rationale. The judge is given:

  - The momentum factor breakdown (from Stage 2).
  - The news bullets (from Stage 4).
  - The day's theme vector (from Stage 3).
  - The 2-3 most thematically-similar watchlist entries (taste calibration).

Composite score = momentum * 0.4 + news_materiality_pct * 0.3 + theme_fit_pct * 0.3.

Output: outputs/<date>/04_judged.json
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
from datetime import datetime, timedelta
from typing import Any

from daily_screener import config
from daily_screener.utils import io
from daily_screener.utils.comments import load_watchlist_with_notes
from daily_screener.utils.llm import LLMError, call_llm_json

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Nearest-watchlist-examples calibration
# ---------------------------------------------------------------------------

def _load_watchlist() -> dict:
    """Return watchlisted tickers joined with their (optional) comments.

    Notes are sourced from `user_data/abi_comments.json` and included only
    for tickers that are on the watchlist (so free-floating comments outside
    the watchlist never reach the judge prompt).
    """
    return load_watchlist_with_notes()


# ---------------------------------------------------------------------------
# User feedback (verdict-override) calibration
# ---------------------------------------------------------------------------

def _load_feedback() -> dict:
    """Return {date: {ticker: record}} or {} if missing/invalid."""
    if not config.DAILY_SCREENER_FEEDBACK_FILE.exists():
        return {}
    try:
        with config.DAILY_SCREENER_FEEDBACK_FILE.open("r") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:  # noqa: BLE001
        logger.warning("[s5] feedback file unreadable: %s", e)
        return {}


def _format_user_corrections(
    feedback: dict,
    run_date: str,
    lookback_days: int,
    max_examples: int,
) -> str:
    """Format the trader's recent verdict overrides as a calibration block.

    Each entry tells the judge "on date D you said X for ticker T, the trader
    corrected it to Y because Z" - the model should generalize from these to
    avoid making the same mistake again.
    """
    if not feedback:
        return "(no prior corrections - the trader has not flagged any of your past verdicts yet)"

    # Filter by date window (if lookback_days > 0). Sort newest first.
    if lookback_days and lookback_days > 0:
        try:
            run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
            cutoff = run_dt - timedelta(days=lookback_days)
        except ValueError:
            cutoff = None
    else:
        cutoff = None

    flat: list[tuple[str, str, dict]] = []
    for date_key in sorted(feedback.keys(), reverse=True):
        per_date = feedback[date_key]
        if not isinstance(per_date, dict):
            continue
        if cutoff is not None:
            try:
                d = datetime.strptime(date_key, "%Y-%m-%d").date()
            except ValueError:
                continue
            if d < cutoff:
                continue
        for ticker, rec in per_date.items():
            if not isinstance(rec, dict):
                continue
            if not rec.get("expected_verdict"):
                continue
            flat.append((date_key, ticker.upper(), rec))

    if not flat:
        return f"(no corrections in the last {lookback_days} day(s))"

    flat = flat[:max_examples]

    blocks = []
    for date_key, ticker, rec in flat:
        agent = (rec.get("agent_verdict") or "").upper() or "?"
        wanted = (rec.get("expected_verdict") or "").upper() or "?"
        reason = (rec.get("reason") or "").strip()
        composite = rec.get("agent_composite_score")
        news_mat = rec.get("agent_news_materiality")
        theme_fit = rec.get("agent_theme_fit")
        matched = rec.get("agent_matched_themes") or []
        rationale = (rec.get("agent_rationale") or "").strip()

        head = f"- {date_key} **{ticker}**: agent={agent} → trader said **{wanted}**"
        bits = []
        if composite is not None:
            bits.append(f"composite={composite}")
        if news_mat is not None:
            bits.append(f"news={news_mat}/3")
        if theme_fit is not None:
            bits.append(f"theme={theme_fit}/3")
        if matched:
            bits.append("matched=" + ",".join(matched[:4]))
        if bits:
            head += " (" + ", ".join(bits) + ")"
        blocks.append(head)
        if rationale:
            blocks.append(f"    agent_rationale: {rationale[:280]}")
        if reason:
            blocks.append(f"    trader_reason:   {reason[:400]}")
    return "\n".join(blocks)


def _nearest_examples(
    candidate_tags: list[str],
    watchlist: dict,
    user_themes: list[dict[str, Any]],
    k: int,
) -> list[dict[str, Any]]:
    """Pick K watchlist entries whose inferred themes overlap most with the
    candidate's tags. Prefers high-star entries on ties."""
    if not candidate_tags or not watchlist:
        # Fall back to top-stars examples so the judge always has *some* anchor.
        return _top_stars_examples(watchlist, k)

    # Build a map: ticker -> set of inferred tags. We approximate inference by
    # checking which user-taste themes list this ticker as an example.
    ticker_tags: dict[str, set[str]] = {}
    for t in user_themes:
        for ex in t.get("example_tickers") or []:
            ticker_tags.setdefault(ex.upper(), set()).add(t["tag"])

    candidate_set = set(candidate_tags)

    scored = []
    for tk, entry in watchlist.items():
        if not isinstance(entry, dict):
            continue
        stars = int(entry.get("stars") or 0)
        overlap = len(candidate_set & ticker_tags.get(tk.upper(), set()))
        # `stars: 0` (or missing) now means "unrated", not "vetoed" - all
        # watchlist entries are positive examples; we just rank by overlap
        # first and stars second.
        score = overlap * 10 + stars
        if overlap == 0 and stars == 0:
            # Zero overlap AND no explicit rating - skip; we have stronger
            # signals to fill the k slots. (Explicit dislikes never reach
            # this code path; they are filtered in Stage 1.)
            continue
        scored.append((score, tk, entry))

    scored.sort(key=lambda x: -x[0])
    out: list[dict[str, Any]] = []
    for _, tk, entry in scored[:k]:
        out.append(
            {
                "ticker": tk,
                "stars": int(entry.get("stars") or 0),
                "notes": entry.get("notes") or "",
            }
        )
    if not out:
        return _top_stars_examples(watchlist, k)
    return out


def _top_stars_examples(watchlist: dict, k: int) -> list[dict[str, Any]]:
    rows = []
    for tk, entry in watchlist.items():
        if not isinstance(entry, dict):
            continue
        stars = int(entry.get("stars") or 0)
        rows.append((stars, tk, entry))
    rows.sort(key=lambda x: -x[0])
    return [
        {"ticker": tk, "stars": int(e.get("stars") or 0), "notes": e.get("notes") or ""}
        for _, tk, e in rows[:k]
    ]


# ---------------------------------------------------------------------------
# Prompt assembly
# ---------------------------------------------------------------------------

def _format_theme_vector(themes: list[dict]) -> str:
    if not themes:
        return "(none - rely on the news itself)"
    lines = []
    for t in themes[:15]:
        descs = "; ".join((t.get("descriptions") or [t.get("description") or ""]))
        srcs = ",".join(t.get("sources") or [t.get("source") or ""])
        lines.append(
            f"- `{t['tag']}` (weight={t['weight']:.1f}, sources={srcs}): {descs.strip()}"
        )
        if t.get("anti_pattern"):
            lines.append(f"    anti-pattern: {t['anti_pattern']}")
    return "\n".join(lines)


def _format_examples(examples: list[dict]) -> str:
    if not examples:
        return "(no watchlist entries available)"
    blocks = []
    for ex in examples:
        notes = (ex.get("notes") or "").strip()
        if not notes:
            notes = "(no notes)"
        blocks.append(
            f"- **{ex['ticker']}** (stars={ex['stars']}):\n  {notes[:1200]}"
        )
    return "\n".join(blocks)


def _format_candidate(
    survivor: dict[str, Any],
    news: dict[str, Any] | None,
) -> str:
    factors = survivor.get("factors", {})
    factor_lines = []
    for name, info in factors.items():
        factor_lines.append(f"  - {name}: +{info['points']} ({info['reason']})")
    parts = [
        f"ticker: {survivor['ticker']}",
        f"company: {survivor.get('company_name')}",
        f"sector: {survivor.get('sector')} / industry: {survivor.get('industry')}",
        f"market_cap: {survivor.get('market_cap')}",
        f"price: {survivor.get('current_price')}",
        f"returns: 1d={survivor.get('dr_1')}% 5d={survivor.get('dr_5')}% 20d={survivor.get('dr_20')}% 60d={survivor.get('dr_60')}%",
        f"atr20: {survivor.get('atr20')}% rsi_mktcap: {survivor.get('rsi_mktcap')}",
        f"vol_vs_10d_avg: {survivor.get('vol_vs_10d_avg')}",
        f"sources: {survivor.get('sources')}",
        f"last_event: {survivor.get('last_event_date')} ({survivor.get('last_event_type')}, mag={survivor.get('last_event_magnitude')})",
        f"momentum_score: {survivor.get('momentum_score')}",
        "momentum factor breakdown:",
        *factor_lines,
        "",
        "NEWS (from Perplexity):",
    ]
    if news and news.get("has_material_news"):
        drivers = news.get("headline_drivers") or []
        parts.append(f"  has_material_news: True")
        parts.append(f"  latest_material_date: {news.get('latest_material_date')}")
        parts.append(f"  theme_tags (model-suggested): {news.get('theme_tags') or []}")
        parts.append("  headline_drivers:")
        for d in drivers:
            parts.append(f"    - {d}")
        parts.append(f"  summary: {news.get('summary')}")
    elif news:
        parts.append(f"  has_material_news: False")
        parts.append(f"  summary: {news.get('summary')}")
    else:
        parts.append("  (news fetch failed - judge based on momentum only)")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Scoring + verdict
# ---------------------------------------------------------------------------

def _composite(momentum: float, news: int, theme: int) -> float:
    w = config.SCORE_WEIGHTS
    return round(
        momentum * w["momentum"]
        + (news / 3.0 * 100.0) * w["news"]
        + (theme / 3.0 * 100.0) * w["theme"],
        2,
    )


def _final_verdict(composite: float, claude_verdict: str | None) -> str:
    """Composite thresholds dominate, but Claude verdict can downgrade a
    composite PICK to WATCH/SKIP if Claude flagged a problem (e.g. weak news)."""
    if composite >= config.PICK_COMPOSITE_MIN:
        base = "PICK"
    elif composite >= config.WATCH_COMPOSITE_MIN:
        base = "WATCH"
    else:
        base = "SKIP"

    cv = (claude_verdict or "").upper()
    order = {"SKIP": 0, "WATCH": 1, "PICK": 2}
    if cv in order:
        # Take the more conservative of the two (we trust Claude's veto).
        if order[cv] < order[base]:
            return cv
    return base


# ---------------------------------------------------------------------------
# Per-ticker judging
# ---------------------------------------------------------------------------

def _format_own_thesis(ticker: str, watchlist: dict) -> str:
    """If the candidate ticker itself is on the watchlist, return the user's
    own thesis note + star rating as the strongest possible calibration anchor.

    This is separate from `_nearest_examples` (which uses *other* tickers as
    analogies) and intentionally stronger: when the trader has already written
    a thesis for *this exact ticker*, that thesis IS the bar.
    """
    entry = watchlist.get(ticker.upper()) if watchlist else None
    if not isinstance(entry, dict):
        return ""
    note = (entry.get("notes") or "").strip()
    stars = int(entry.get("stars") or 0)
    if not note:
        if stars > 0:
            return f"The trader has this ticker on their watchlist with stars={stars} but no note."
        return ""
    return (
        f"The trader ALREADY has this ticker on their watchlist (stars={stars}) "
        f"with the following thesis note. Honor it: if the news from Stage 4 "
        f"confirms or extends this thesis, that is a very strong PICK signal "
        f"even if the headline doesn't read like a fresh press release.\n\n"
        f'"""\n{note[:1500]}\n"""'
    )


def _judge_one(
    survivor: dict[str, Any],
    news: dict[str, Any] | None,
    theme_block: str,
    examples: list[dict[str, Any]],
    own_thesis_block: str,
    user_corrections_block: str,
    prompt_template: str,
) -> dict[str, Any]:
    ticker = survivor["ticker"]
    candidate_block = _format_candidate(survivor, news)
    nearest_block = _format_examples(examples)
    own_thesis = own_thesis_block or "(this ticker is not on the trader's watchlist)"

    prompt = (
        prompt_template
        .replace("{ticker}", ticker)
        .replace("{theme_vector_block}", theme_block)
        .replace("{nearest_examples_block}", nearest_block)
        .replace("{own_thesis_block}", own_thesis)
        .replace("{user_corrections_block}", user_corrections_block)
        .replace("{candidate_block}", candidate_block)
    )

    try:
        parsed = call_llm_json(prompt, model=config.JUDGE_MODEL, max_tokens=config.JUDGE_MAX_TOKENS)
    except (LLMError, ValueError) as e:
        logger.warning("[s5] judge failed for %s: %s", ticker, e)
        return {
            "ticker": ticker,
            "ok": False,
            "error": str(e),
        }

    if not isinstance(parsed, dict):
        return {"ticker": ticker, "ok": False, "error": "non_dict_response"}

    news_mat = int(parsed.get("news_materiality") or 0)
    theme_fit = int(parsed.get("theme_fit") or 0)
    composite = _composite(survivor.get("momentum_score", 0.0), news_mat, theme_fit)
    final = _final_verdict(composite, parsed.get("verdict"))

    return {
        "ticker": ticker,
        "ok": True,
        "news_materiality": news_mat,
        "news_materiality_reason": parsed.get("news_materiality_reason") or "",
        "theme_fit": theme_fit,
        "matched_themes": parsed.get("matched_themes") or [],
        "theme_fit_reason": parsed.get("theme_fit_reason") or "",
        "verdict_claude": parsed.get("verdict") or "",
        "verdict": final,
        "verdict_rationale": parsed.get("verdict_rationale") or "",
        "composite_score": composite,
    }


def run(date_str: str | None = None, *, max_tickers: int | None = None) -> dict[str, Any]:
    date_str = date_str or io.today_str()
    logger.info("[s5] judging for %s", date_str)

    momentum = io.read_json(io.artifact_path(date_str, "01_momentum.json"))
    theme_vec = io.read_json_if_exists(
        io.artifact_path(date_str, "02_theme_vector.json"),
        default={"themes": []},
    )
    news = io.read_json_if_exists(
        io.artifact_path(date_str, "03_news.json"),
        default={"results": [], "failures": []},
    )
    user_themes = io.read_json_if_exists(
        io.artifact_path(date_str, "02b_user_themes.json"),
        default={"themes": []},
    ).get("themes", [])

    news_by_ticker = {n["ticker"]: n for n in news.get("results", [])}
    failed_tickers = {n["ticker"]: n for n in news.get("failures", [])}

    survivors = momentum.get("survivors", [])
    if max_tickers is not None:
        survivors = survivors[:max_tickers]

    # Tickers we'll actually send to Claude (must have news).
    judgable = [s for s in survivors if s["ticker"] in news_by_ticker]
    skipped_no_news = [s for s in survivors if s["ticker"] not in news_by_ticker]

    if not judgable:
        artifact = {"date": date_str, "results": [], "skipped_no_news": skipped_no_news}
        io.write_json(io.artifact_path(date_str, "04_judged.json"), artifact)
        return artifact

    theme_block = _format_theme_vector(theme_vec.get("themes", []))
    prompt_template = io.load_prompt("judge")
    watchlist = _load_watchlist()
    feedback = _load_feedback()
    user_corrections_block = _format_user_corrections(
        feedback,
        run_date=date_str,
        lookback_days=getattr(config, "FEEDBACK_LOOKBACK_DAYS", 30),
        max_examples=getattr(config, "FEEDBACK_MAX_EXAMPLES", 20),
    )
    if feedback:
        logger.info(
            "[s5] feeding %d-day user-correction history into judge prompt",
            getattr(config, "FEEDBACK_LOOKBACK_DAYS", 30),
        )

    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    workers = max(1, config.JUDGE_CONCURRENCY)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {}
        for s in judgable:
            news_entry = news_by_ticker.get(s["ticker"])
            tags = (news_entry or {}).get("theme_tags") or []
            examples = _nearest_examples(tags, watchlist, user_themes, config.JUDGE_NEAREST_EXAMPLES)
            own_thesis_block = _format_own_thesis(s["ticker"], watchlist)
            future_map[
                pool.submit(
                    _judge_one,
                    s,
                    news_entry,
                    theme_block,
                    examples,
                    own_thesis_block,
                    user_corrections_block,
                    prompt_template,
                )
            ] = s

        for fut in concurrent.futures.as_completed(future_map):
            s = future_map[fut]
            try:
                res = fut.result()
            except Exception as e:  # noqa: BLE001
                logger.exception("[s5] failure for %s", s["ticker"])
                res = {"ticker": s["ticker"], "ok": False, "error": f"exception: {e}"}

            if not res.get("ok"):
                failures.append(
                    {
                        "ticker": s["ticker"],
                        "drop_reason": f"judge_failed: {res.get('error')}",
                    }
                )
                continue

            # Enrich with the momentum + news context for the audit stage.
            news_entry = news_by_ticker.get(s["ticker"]) or {}
            results.append(
                {
                    **res,
                    "company_name": s.get("company_name"),
                    "sector": s.get("sector"),
                    "industry": s.get("industry"),
                    "market_cap": s.get("market_cap"),
                    "current_price": s.get("current_price"),
                    "momentum_score": s.get("momentum_score"),
                    "momentum_factors": s.get("factors"),
                    "sources": s.get("sources", []),
                    "dr_1": s.get("dr_1"),
                    "dr_5": s.get("dr_5"),
                    "dr_20": s.get("dr_20"),
                    "dr_60": s.get("dr_60"),
                    "rsi_mktcap": s.get("rsi_mktcap"),
                    "last_event_date": s.get("last_event_date"),
                    "last_event_type": s.get("last_event_type"),
                    "headline_drivers": news_entry.get("headline_drivers", []),
                    "news_summary": news_entry.get("summary", ""),
                    "news_theme_tags": news_entry.get("theme_tags", []),
                    "latest_material_date": news_entry.get("latest_material_date"),
                }
            )

    results.sort(key=lambda r: -r.get("composite_score", 0.0))

    artifact = {
        "date": date_str,
        "model": config.JUDGE_MODEL,
        "score_weights": config.SCORE_WEIGHTS,
        "pick_composite_min": config.PICK_COMPOSITE_MIN,
        "watch_composite_min": config.WATCH_COMPOSITE_MIN,
        "judged_count": len(results),
        "failed_count": len(failures),
        "skipped_no_news_count": len(skipped_no_news),
        "verdict_counts": {
            v: sum(1 for r in results if r["verdict"] == v) for v in ("PICK", "WATCH", "SKIP")
        },
        "results": results,
        "failures": failures,
        "skipped_no_news": [
            {"ticker": s["ticker"], "company_name": s.get("company_name")}
            for s in skipped_no_news
        ],
    }
    io.write_json(io.artifact_path(date_str, "04_judged.json"), artifact)
    logger.info(
        "[s5] judged=%d (PICK=%d, WATCH=%d, SKIP=%d), failed=%d",
        len(results),
        artifact["verdict_counts"]["PICK"],
        artifact["verdict_counts"]["WATCH"],
        artifact["verdict_counts"]["SKIP"],
        len(failures),
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
