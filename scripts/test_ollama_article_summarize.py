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
import http.client
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "market_brief" / "fixtures" / "arm_stock_thursday_2026-05-28.json"
OUT_DIR = ROOT / "market_brief" / "fixtures" / "ollama_test_output"
OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
OLLAMA_TAGS_URL = "http://127.0.0.1:11434/api/tags"
DEFAULT_MODELS = ["gemma4:latest"]


def check_ollama() -> list[str]:
    """Return installed model names or raise if Ollama is down."""
    try:
        with urllib.request.urlopen(OLLAMA_TAGS_URL, timeout=5) as resp:
            data = json.loads(resp.read())
    except urllib.error.URLError as e:
        raise SystemExit(
            f"Ollama not reachable at {OLLAMA_TAGS_URL}: {e}\n"
            "Start Ollama, then re-run."
        ) from e
    return [m["name"] for m in data.get("models", [])]


def build_prompt(article: dict) -> str:
    tickers = article.get("tickers") or []
    tickers_label = ", ".join(tickers) if tickers else "(none in metadata)"
    body = (article.get("body_text") or article.get("teaser") or "").strip()
    if len(body) > 12_000:
        body = body[:12_000] + "\n\n[truncated for test]"

    return f"""You are a buy-side analyst extracting actionable material from ONE Benzinga article.
DO NOT search the web. Use ONLY the article text. Do not invent facts.

Write markdown for a US equity trader: dense bullets with concrete numbers, dates, and names.
Focus on what changes a position or risk view — not recap fluff or photo credits.

Cover only themes the article actually supports (omit empty themes):
- Sell-side actions: firms, price targets, ratings, dates
- Investment thesis: structural drivers, product/market shift, TAM or economics vs alternatives
- Demand / fundamentals: reported quarters, backlog, orders, growth %, near-term guidance
- Long-range targets: management revenue or margin goals for future fiscal years (by segment if given) — use its own subhead; do not fold into quarterly results
- Risks and constraints: supply, execution, valuation stretch, technical warnings — only if in text
- Forward outlook: next earnings/catalyst dates, consensus estimates, valuation metrics
- Price action: session move, levels, extension vs moving averages if the article gives them

If the article names multiple tickers with distinct facts (e.g. options tables), give each such ticker its own bullets.
If one company is clearly the subject, center snippets on that ticker.

Use short thematic subheads only where they help scan (plain **bold** labels are fine).
Bold key figures. No URLs, no [confirmed]/[sell-side] tags in this test output.

Title: {article.get("title") or ""}
Published: {article.get("published") or ""}
Tickers tagged: {tickers_label}

Article:
{body}
"""


def ollama_unload(model: str) -> None:
    """Free VRAM/RAM before loading the next model."""
    payload = {"model": model, "keep_alive": 0}
    req = urllib.request.Request(
        OLLAMA_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=30)
    except urllib.error.URLError:
        pass


def ollama_generate(
    model: str,
    prompt: str,
    *,
    num_predict: int = 8192,
    num_ctx: int = 8192,
    temperature: float = 0.15,
    timeout: int = 600,
) -> dict:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": num_predict,
            "num_ctx": num_ctx,
        },
    }
    req = urllib.request.Request(
        OLLAMA_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def summarize_article(model: str, article: dict) -> dict:
    prompt = build_prompt(article)
    t0 = time.time()
    try:
        data = ollama_generate(model, prompt)
    except (
        urllib.error.URLError,
        urllib.error.HTTPError,
        http.client.RemoteDisconnected,
        TimeoutError,
        json.JSONDecodeError,
    ) as e:
        return {
            "model": model,
            "benzinga_id": article.get("benzinga_id"),
            "error": str(e),
            "elapsed_s": round(time.time() - t0, 2),
        }

    response = (data.get("response") or "").strip()
    thinking = (data.get("thinking") or "").strip()
    return {
        "model": model,
        "benzinga_id": article.get("benzinga_id"),
        "title": article.get("title"),
        "prompt_chars": len(prompt),
        "elapsed_s": round(time.time() - t0, 2),
        "done_reason": data.get("done_reason"),
        "thinking_chars": len(thinking),
        "response_chars": len(response),
        "response": response,
        "thinking_preview": thinking[:500] if thinking else "",
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

    installed = check_ollama()
    missing = [m for m in args.models if m not in installed]
    if missing:
        raise SystemExit(
            f"Models not in Ollama: {missing}\nInstalled: {', '.join(installed)}"
        )

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
    print(f"Ollama: {OLLAMA_URL}\n")

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
                print(
                    f"  {row['elapsed_s']}s | out={row['response_chars']} chars | "
                    f"think={row['thinking_chars']} | {row['done_reason']}"
                )
                for line in (row.get("response") or "").splitlines()[:4]:
                    print(f"      {line[:100]}")
            stem = f"{article.get('benzinga_id')}_{model.replace(':', '_')}"
            if row.get("error"):
                (args.out / f"{stem}.error.txt").write_text(row["error"], encoding="utf-8")
            else:
                (args.out / f"{stem}.md").write_text(row["response"], encoding="utf-8")
            print()
        ollama_unload(model)
        print(f"Unloaded {model}\n")

    summary_path = args.out / "results.json"
    summary_path.write_text(json.dumps(all_results, indent=2), encoding="utf-8")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
