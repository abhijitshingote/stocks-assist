"""Stage 4 — Fundamental Agent (TICKER-SPECIFIC).

For each survivor of the technical stage:

  1. Ask Perplexity (cheap `sonar`, recency=week): "what are the recent
     material drivers for {TICKER}?". Get text + citations.

  2. Hand the per-ticker events bundle + the hot themes (from the Market
     News stage) to Ollama with `format=json`. Score 0–10 conviction on
     theme-fit backed by real material events.

Input shape (from pipeline orchestrator):

    {
      "survivors": [...],        # from 02_technical.json
      "themes": [...],           # from 03_market_news.json
    }

Output shape:

    {
      "final": [...],            # filtered by min_score, sorted by combined_score
      "all_scored": [...],       # everything we scored
      "ticker_news": {ticker: {"text": ..., "citations": [...]}},
      "stats": {...}
    }
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

from .. import config as cfg
from ..tools import ollama_client, perplexity
from .base import Agent, load_json, load_prompt, save_json

logger = logging.getLogger(__name__)


class FundamentalAgent(Agent):
    name = "fundamental_agent"

    def __init__(self) -> None:
        self.cfg = cfg.FUNDAMENTAL_AGENT
        self.ticker_query_prompt = load_prompt(self.cfg["ticker_query_prompt_file"])
        self.match_prompt = load_prompt(self.cfg["match_prompt_file"])

    # ------------------------------------------------------------------
    # Per-ticker grounded events fetch
    # ------------------------------------------------------------------
    def _fetch_ticker_news(self, ticker: str, company_name: str) -> dict[str, Any]:
        prompt = (self.ticker_query_prompt
                  .replace("{TICKER}", ticker)
                  .replace("{COMPANY_NAME}", company_name or "")
                  .replace("{LOOKBACK_DAYS}", str(self.cfg["lookback_days"])))
        try:
            res = perplexity.search(
                prompt,
                model=self.cfg["perplexity_model"],
                recency=self.cfg["recency"],
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("Perplexity failed for %s: %s", ticker, e)
            return {"text": "", "citations": [], "error": str(e)}
        return {"text": res.text, "citations": res.citations}

    # ------------------------------------------------------------------
    # Theme-fit scoring (Ollama, JSON-mode, batched by chunk_size)
    # ------------------------------------------------------------------
    def _format_stock_block(self, survivor: dict[str, Any], events: dict[str, Any]) -> str:
        m = survivor.get("metrics", {}) or {}
        out = [
            f"## {survivor['ticker']} — {survivor.get('company_name','')}",
            f"sector={survivor.get('sector')} industry={survivor.get('industry')}",
            f"technical_score={survivor.get('score')} setup={survivor.get('setup')}",
            f"technical_rationale={survivor.get('rationale')}",
            f"buckets={survivor.get('buckets')} tags={m.get('tags')}",
            "",
            "### Recent material events (Perplexity, last 1–2 weeks)",
        ]
        text = (events or {}).get("text", "").strip()
        if not text:
            out.append("(no events returned — score conservatively)")
        else:
            out.append(text)
            cites = events.get("citations") or []
            if cites:
                out.append("")
                out.append("citations: " + ", ".join(cites[:8]))
        return "\n".join(out)

    def _match_chunk(
        self,
        themes: list[dict[str, Any]],
        chunk: list[dict[str, Any]],
        ticker_news: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        def _theme_line(t: dict[str, Any]) -> str:
            name = t.get("name", "")
            catalyst = t.get("catalyst") or t.get("description") or ""
            thesis = t.get("thesis") or ""
            cues = t.get("keywords") or []
            body = " | ".join(p for p in (catalyst, thesis) if p)
            return f"- {name}: {body}  (cues: {cues})"

        themes_block = "\n".join(
            _theme_line(t) for t in (themes or [])
        ) or "(no themes available — flag this in `risks`)"

        blocks = [
            self._format_stock_block(s, ticker_news.get(s["ticker"], {}))
            for s in chunk
        ]
        user_msg = (
            "## Hot themes right now\n"
            f"{themes_block}\n\n"
            "## Stocks to evaluate\n"
            "For each stock, decide whether it is a strong narrative-fit and "
            "back it with the material events shown. Return JSON as described "
            "in the system prompt.\n\n"
            + "\n\n---\n\n".join(blocks)
        )
        result = ollama_client.chat_json(
            messages=[
                {"role": "system", "content": self.match_prompt},
                {"role": "user", "content": user_msg},
            ],
            model=self.cfg["ollama_model"],
        )
        if isinstance(result, dict) and "results" in result:
            return result["results"]
        if isinstance(result, list):
            return result
        logger.warning("Unexpected match JSON shape: %s", type(result))
        return []

    # ------------------------------------------------------------------
    def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        survivors = (input_data or {}).get("survivors", []) or []
        themes = (input_data or {}).get("themes", []) or []

        cap = self.cfg.get("max_survivors")
        if cap is not None:
            survivors = survivors[:cap]

        if not survivors:
            return self.envelope({"final": [], "all_scored": [],
                                  "ticker_news": {}, "stats": {"input_survivors": 0}})

        # Step 1 — Perplexity per ticker
        logger.info("Fetching Perplexity events for %d tickers...", len(survivors))
        ticker_news: dict[str, dict[str, Any]] = {}
        for s in survivors:
            ticker_news[s["ticker"]] = self._fetch_ticker_news(
                s["ticker"], s.get("company_name") or ""
            )
            tn = ticker_news[s["ticker"]]
            logger.info("  %s: %d chars, %d citations",
                        s["ticker"], len(tn.get("text", "")), len(tn.get("citations", [])))

        # Step 2 — Theme-fit scoring via Ollama
        all_results: list[dict[str, Any]] = []
        chunk_size = self.cfg.get("chunk_size", 2)
        for i in range(0, len(survivors), chunk_size):
            chunk = survivors[i:i + chunk_size]
            logger.info("Scoring theme-fit chunk %d-%d (%d tickers)...",
                        i, i + len(chunk) - 1, len(chunk))
            try:
                chunk_results = self._match_chunk(themes, chunk, ticker_news)
            except Exception as e:  # noqa: BLE001
                logger.error("Match chunk failed (%s) — skipping", e)
                continue

            by_ticker = {(r.get("ticker") or "").upper(): r
                         for r in chunk_results if isinstance(r, dict)}
            for s in chunk:
                r = by_ticker.get(s["ticker"].upper())
                if not r:
                    continue
                fundamental_score = float(r.get("score", 0))
                technical_score = float(s.get("score") or 0)
                all_results.append({
                    "ticker": s["ticker"],
                    "company_name": s.get("company_name"),
                    "sector": s.get("sector"),
                    "industry": s.get("industry"),
                    "technical_score": technical_score,
                    "technical_setup": s.get("setup"),
                    "technical_rationale": s.get("rationale"),
                    "fundamental_score": fundamental_score,
                    "matched_themes": r.get("matched_themes"),
                    "material_events": r.get("material_events"),
                    "story": r.get("story"),
                    "risks": r.get("risks"),
                    "combined_score": round((technical_score + fundamental_score) / 2.0, 2),
                    "citations": ticker_news.get(s["ticker"], {}).get("citations", []),
                })

        final = [r for r in all_results
                 if r["fundamental_score"] >= self.cfg["min_score"]]
        final.sort(key=lambda r: r["combined_score"], reverse=True)
        all_results.sort(key=lambda r: r["combined_score"], reverse=True)

        stats = {
            "input_survivors": len(survivors),
            "themes_used": len(themes),
            "scored": len(all_results),
            "final": len(final),
            "min_score": self.cfg["min_score"],
            "perplexity_model": self.cfg["perplexity_model"],
            "ollama_model": self.cfg["ollama_model"],
        }
        logger.info("FundamentalAgent stats: %s", stats)
        return self.envelope({
            "final": final,
            "all_scored": all_results,
            "ticker_news": ticker_news,
            "stats": stats,
        })


# ---------------------------------------------------------------------------
# Standalone CLI — accepts the technical + market-news outputs separately so
# you can iterate on this agent without re-running everything.
# ---------------------------------------------------------------------------
def _main() -> None:
    ap = argparse.ArgumentParser(description="Run FundamentalAgent standalone.")
    ap.add_argument("--technical", required=True, help="Path to 02_technical.json")
    ap.add_argument("--market-news", required=True, help="Path to 03_market_news.json")
    ap.add_argument("--output", required=True, help="Where to write 04_fundamental.json")
    ap.add_argument("--max-tickers", type=int, default=None,
                    help="Override FUNDAMENTAL_AGENT.max_survivors for this run.")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )

    if args.max_tickers is not None:
        cfg.FUNDAMENTAL_AGENT["max_survivors"] = args.max_tickers

    technical = load_json(args.technical)
    market_news = load_json(args.market_news)

    merged = {
        "survivors": technical.get("survivors", []),
        "themes": market_news.get("themes", []),
    }
    out = FundamentalAgent().run(merged)
    save_json(args.output, out)
    print(f"Wrote {args.output} ({len(out.get('final') or [])} final picks)")


if __name__ == "__main__":
    _main()
