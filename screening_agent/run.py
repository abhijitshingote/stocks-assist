"""CLI entry point for the screening agent chain.

Examples:

    # Full pipeline, capping initial universe at 30 stocks.
    python -m screening_agent.run --limit 30 --verbose

    # Cheap test: only 5 tickers reach the fundamental stage.
    python -m screening_agent.run --limit 30 --max-tickers 5 --verbose

    # Re-run only the fundamental stage on an existing run id.
    python -m screening_agent.run --run-id 20260508_164855_5cf1a1 \
        --from-stage fundamental --max-tickers 5 --verbose

    # Stop after market_news (don't burn Perplexity ticker calls yet).
    python -m screening_agent.run --skip fundamental
"""

from __future__ import annotations

import argparse
import logging
import sys

from . import config as cfg
from .agents.base import load_json
from .pipeline import Pipeline
from .tools import ollama_client

STAGES = ["initial", "technical", "market_news", "fundamental"]


def _ensure_ollama_ready(model_name: str) -> None:
    try:
        models = ollama_client.list_models()
    except Exception as e:  # noqa: BLE001
        print(f"[fatal] Cannot reach Ollama at {cfg.OLLAMA['host']}: {e}", file=sys.stderr)
        sys.exit(2)

    matching = [m for m in models
                if m == model_name or m.split(":")[0] == model_name.split(":")[0]]
    if not matching:
        print(f"[warn] Model '{model_name}' not found. Available: {models}", file=sys.stderr)
        print(f"       Pull it with: ollama pull {model_name}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the screening agent chain.")
    ap.add_argument("--run-id", help="Reuse an existing run-id (creates if missing).")
    ap.add_argument("--from-stage", choices=STAGES, default="initial",
                    help="Start at this stage. Earlier stages must already exist on disk.")
    ap.add_argument("--skip", choices=["technical", "market_news", "fundamental"],
                    help="Stop pipeline after this stage's predecessor.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Override INITIAL_SCREENER.final_cap for a quick test.")
    ap.add_argument("--max-tickers", type=int, default=None,
                    help="Cap survivors handed to the FUNDAMENTAL stage (cheap testing).")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )

    if args.limit is not None:
        cfg.INITIAL_SCREENER["final_cap"] = args.limit
    if args.max_tickers is not None:
        cfg.FUNDAMENTAL_AGENT["max_survivors"] = args.max_tickers

    _ensure_ollama_ready(cfg.OLLAMA["default_model"])

    pipeline = Pipeline(run_id=args.run_id)
    print(f"=== Run ID: {pipeline.run_id} ===")
    print(f"=== Outputs: {pipeline.run_dir} ===")

    # Helper: load an existing stage output if the user is resuming.
    def _existing(name: str) -> dict | None:
        path = pipeline.run_dir / name
        return load_json(path) if path.exists() else None

    initial_out = None
    technical_out = None
    market_news_out = None

    # Stage 1
    if STAGES.index(args.from_stage) <= STAGES.index("initial"):
        initial_out = pipeline.run_initial()
    else:
        initial_out = _existing("01_initial.json")
        if initial_out is None:
            print("[fatal] 01_initial.json missing; cannot resume", file=sys.stderr)
            sys.exit(2)

    if args.skip == "technical":
        return

    # Stage 2
    if STAGES.index(args.from_stage) <= STAGES.index("technical"):
        technical_out = pipeline.run_technical(initial_out)
    else:
        technical_out = _existing("02_technical.json")
        if technical_out is None:
            print("[fatal] 02_technical.json missing; cannot resume", file=sys.stderr)
            sys.exit(2)

    if args.skip == "market_news":
        return

    # Stage 3
    if STAGES.index(args.from_stage) <= STAGES.index("market_news"):
        market_news_out = pipeline.run_market_news()
    else:
        market_news_out = _existing("03_market_news.json")
        if market_news_out is None:
            print("[fatal] 03_market_news.json missing; cannot resume", file=sys.stderr)
            sys.exit(2)

    if args.skip == "fundamental":
        return

    # Stage 4
    final_out = pipeline.run_fundamental(technical_out, market_news_out)

    print("\n=== HOT THEMES ===")
    for t in (market_news_out.get("themes") or [])[:10]:
        catalyst = t.get("catalyst") or t.get("description") or ""
        thesis = t.get("thesis") or ""
        body = " | ".join(p for p in (catalyst, thesis) if p)
        print(f"  - {t.get('name')}: {body}")

    print("\n=== FINAL LIST ===")
    for r in (final_out.get("final") or [])[:20]:
        themes = ", ".join(r.get("matched_themes") or []) or "—"
        print(f"  {r['ticker']:<6}  combined={r.get('combined_score')}  "
              f"tech={r.get('technical_score')}  story={r.get('fundamental_score')}  "
              f"themes=[{themes}]")
    print(f"\n=== Outputs in {pipeline.run_dir} ===")


if __name__ == "__main__":
    main()
