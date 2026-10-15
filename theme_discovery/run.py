"""Theme discovery pipeline.

    docker compose exec backend python -m theme_discovery.run
    docker compose exec backend python -m theme_discovery.run --from-stage 3   # reuse candidates + news
    docker compose exec backend python -m theme_discovery.run --max-candidates 5  # cheap test
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

from daily_screener.utils.io import read_json, today_str, write_json
from daily_screener.utils.perplexity import USAGE
from market_brief.cost_tracker import CostTracker
from theme_discovery import config
from theme_discovery.candidates import build_candidates
from theme_discovery.stages import run_news, run_probe, run_synthesis

logger = logging.getLogger("theme_discovery")


def _status(outdir, **kw) -> None:
    write_json(outdir / "status.json", {"updated_at": datetime.now(timezone.utc).isoformat(), **kw})


def _assemble(cands: dict, news: dict, synth: dict, probes: dict) -> dict:
    by_ticker = {c["ticker"]: c for c in cands["candidates"]}

    def screener_stock(s: dict) -> dict:
        c = by_ticker.get(s["ticker"], {})
        n = news.get(s["ticker"], {})
        return {
            "ticker": s["ticker"],
            "found_by": "screener",
            "why": s.get("why", ""),
            "went_up_because": n.get("one_line", ""),
            "picked_by": c.get("picked_by", []),
            **{k: c.get(k) for k in ("company_name", "industry", "cap_bucket", "dr_5", "dr_20", "dr_60", "listed")},
        }

    themes = []
    for t in synth["themes"]:
        p = probes.get(t["slug"], {})
        themes.append({
            "slug": t["slug"],
            "name": t.get("name"),
            "description": t.get("description"),
            "secular": t.get("secular"),
            "stocks": [screener_stock(s) for s in t["stocks"]],
            "probe_added": [{**a, "found_by": "probe"} for a in p.get("added", [])],
            "probe_rejected": p.get("rejected", []),
            "probe_error": p.get("error"),
        })
    themes.sort(key=lambda t: -len(t["stocks"]))

    qa = []
    for q in synth["qa"]:
        c = by_ticker.get(q["ticker"], {})
        qa.append({
            **q,
            **{k: c.get(k) for k in ("company_name", "industry", "cap_bucket", "dr_5", "dr_20", "dr_60", "picked_by")},
        })
    return {
        "weekly_cycle": cands.get("weekly_cycle"),
        "themes": themes,
        "qa": qa,
        "funnel": {
            "universe": cands["universe"],
            "lists": cands["lists"],
            "max_per_industry": cands["max_per_industry"],
            "candidates": [
                {k: c.get(k) for k in ("ticker", "company_name", "industry", "cap_bucket", "dr_5", "dr_20", "dr_60", "picked_by", "listed")}
                for c in cands["candidates"]
            ],
            "dropped": cands["dropped"],
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="Run folder name (default: today ET)")
    ap.add_argument("--from-stage", type=int, default=1, help="1 candidates, 2 news, 3 themes, 4 probe")
    ap.add_argument("--max-candidates", type=int, default=None)
    ap.add_argument("--force", action="store_true", help="Ignore cached per-ticker / per-theme files")
    args = ap.parse_args()

    date = args.date or today_str()
    outdir = config.OUTPUTS_DIR / date
    outdir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(outdir / "run.log")],
    )
    USAGE.reset()
    tracker = CostTracker.load_or_create(outdir)
    t0 = time.time()

    try:
        _status(outdir, status="running", stage="candidates")
        if args.from_stage <= 1:
            cands = build_candidates()
            if args.max_candidates:
                cands["candidates"] = cands["candidates"][: args.max_candidates]
            write_json(outdir / "01_candidates.json", cands)
        else:
            cands = read_json(outdir / "01_candidates.json")

        _status(outdir, status="running", stage="news")
        if args.from_stage <= 2:
            news = run_news(cands["candidates"], date, outdir, force=args.force)
        else:
            news = read_json(outdir / "02_news.json")

        _status(outdir, status="running", stage="themes")
        if args.from_stage <= 3:
            synth = run_synthesis(cands["candidates"], news, outdir, tracker)
        else:
            synth = read_json(outdir / "03_themes.json")

        _status(outdir, status="running", stage="probe")
        probes = run_probe(synth["themes"], date, outdir, force=args.force or args.from_stage <= 3)

        result = _assemble(cands, news, synth, probes)
        result["date"] = date
        result["generated_at"] = datetime.now(timezone.utc).isoformat()
        write_json(outdir / "themes.json", result)

        px = USAGE.snapshot()
        summary = {
            "elapsed_seconds": round(time.time() - t0, 1),
            "candidates": len(cands["candidates"]),
            "themes": len(result["themes"]),
            "qa": len(result["qa"]),
            "probe_added": sum(len(t["probe_added"]) for t in result["themes"]),
            "perplexity": px,
            "anthropic_cost_usd": round(tracker.total_cost_usd, 4),
            "total_cost_usd": round((px.get("cost_usd_total") or 0) + tracker.total_cost_usd, 4),
        }
        write_json(outdir / "run_summary.json", summary)
        _status(outdir, status="done", **{k: summary[k] for k in ("candidates", "themes", "qa", "total_cost_usd")})
        logger.info("done: %s", summary)
        return 0
    except Exception as e:  # noqa: BLE001
        logger.exception("theme discovery failed")
        _status(outdir, status="failed", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
