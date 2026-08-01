#!/usr/bin/env python3
"""Standalone Ollama test: one micro-summary per Benzinga article.

No Docker, no market_brief imports. Reads a local JSON fixture and writes
results under market_brief/fixtures/ollama_test_output/.

Usage:
  python3 scripts/test_ollama_article_summarize.py
  python3 scripts/test_ollama_article_summarize.py --models qwen3:30b
  python3 scripts/test_ollama_article_summarize.py --limit 2
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from market_brief import config, ollama
from market_brief.prompts import build_article_snippet_prompt
from market_brief.snippet_price import append_price_reference, price_reference_footer

FIXTURE = ROOT / "market_brief" / "fixtures" / "arm_stock_thursday_2026-05-28.json"
OUT_DIR = ROOT / "market_brief" / "fixtures" / "ollama_test_output"
DEFAULT_MODELS = ["gemma4:latest"]


def check_ollama(models: list[str]) -> None:
    ollama.check_model_available(models[0])


def summarize_article(model: str, article: dict, *, asof: str = "2026-05-28") -> dict:
    prompt = build_article_snippet_prompt(article)
    t0 = time.time()
    try:
        saved = config.OLLAMA_MODEL
        config.OLLAMA_MODEL = model
        fundamental = ollama.summarize_article(article)
        text = append_price_reference(fundamental, article, asof)
        config.OLLAMA_MODEL = saved
    except ollama.OllamaError as e:
        return {
            "model": model,
            "benzinga_id": article.get("benzinga_id"),
            "error": str(e),
            "elapsed_s": round(time.time() - t0, 2),
        }
    return {
        "model": model,
        "benzinga_id": article.get("benzinga_id"),
        "title": article.get("title"),
        "prompt_chars": len(prompt),
        "price_reference": price_reference_footer(article, asof),
        "elapsed_s": round(time.time() - t0, 2),
        "response_chars": len(text),
        "response": text,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Per-article Ollama summarize test")
    parser.add_argument("--fixture", type=Path, default=FIXTURE)
    parser.add_argument("--models", nargs="+", default=DEFAULT_MODELS)
    parser.add_argument("--limit", type=int, default=0, help="Max articles (0 = all)")
    parser.add_argument("--out", type=Path, default=OUT_DIR)
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove prior *.md / *.error.txt in --out before run",
    )
    args = parser.parse_args()

    if not args.fixture.is_file():
        raise SystemExit(f"Fixture missing: {args.fixture}")

    for model in args.models:
        try:
            config.OLLAMA_MODEL = model
            check_ollama([model])
        except ollama.OllamaError as e:
            raise SystemExit(str(e)) from e

    articles = json.loads(args.fixture.read_text(encoding="utf-8"))
    if args.limit > 0:
        articles = articles[: args.limit]

    args.out.mkdir(parents=True, exist_ok=True)
    if args.clean:
        for p in args.out.glob("*"):
            if p.suffix in (".md", ".txt", ".json") and p.name != "README.md":
                p.unlink()
    all_results: list[dict] = []

    print(f"Fixture: {args.fixture} ({len(articles)} articles)")
    print(f"Models: {', '.join(args.models)} (all articles per model before switching)")
    print(f"Ollama: {config.OLLAMA_BASE_URL}\n")

    for model in args.models:
        print(f"========== {model} ==========")
        for i, article in enumerate(articles, 1):
            print(f"--- Article {i}/{len(articles)} id={article.get('benzinga_id')} ---")
            print(f"    {(article.get('title') or '')[:80]}")
            row = summarize_article(model, article)
            all_results.append(row)
            if row.get("error"):
                print(f"  ERROR {row['error']}")
            else:
                print(f"  {row['elapsed_s']}s | out={row['response_chars']} chars")
                for line in (row.get("response") or "").splitlines()[:4]:
                    print(f"      {line[:100]}")
            stem = f"{article.get('benzinga_id')}_{model.replace(':', '_')}"
            if row.get("error"):
                (args.out / f"{stem}.error.txt").write_text(row["error"], encoding="utf-8")
            else:
                (args.out / f"{stem}.md").write_text(row["response"], encoding="utf-8")
            print()
        ollama.unload_model(model)
        print(f"Unloaded {model}\n")

    summary_path = args.out / "results.json"
    summary_path.write_text(json.dumps(all_results, indent=2), encoding="utf-8")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
