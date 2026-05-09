"""Stage 2 — Technical Agent.

Takes the candidate list from stage 1, enriches each with a compact technical
feature pack + recent OHLC bars, and asks the LLM (Ollama) to keep only the
ones whose price action looks like a momentum continuation:

  • Above-the-line trends (EMA10 > EMA20 > SMA50, often > SMA200).
  • Constructive bases / flags / contractions after vertical advances.
  • Gap-and-go behaviour, vol confirmation on up-moves.
  • Refusal to give back gains ("can't keep it down").

LLM returns a score 0–10 plus rationale for each ticker; we filter on the
threshold from `config.TECHNICAL_AGENT.min_score`.

Notably we batch in chunks because feeding 80 stocks of OHLC at once blows
through the context window. Default chunk size = 8.
"""

from __future__ import annotations

import logging
from typing import Any

from .. import config as cfg
from ..tools import ollama_client, price_data
from .base import Agent, cli_for, load_prompt

logger = logging.getLogger(__name__)


class TechnicalAgent(Agent):
    name = "technical_agent"

    def __init__(self) -> None:
        self.cfg = cfg.TECHNICAL_AGENT
        self.system_prompt = load_prompt(self.cfg["prompt_file"])

    def _format_candidate_block(self, cand: dict[str, Any], feat) -> str:
        """Compact human-readable block per ticker for the LLM."""
        lines = [
            f"## {cand['ticker']} — {cand.get('company_name','')}",
            f"sector={cand.get('sector')} industry={cand.get('industry')} "
            f"mcap={cand.get('market_cap')} price={cand.get('price')}",
            f"buckets={cand.get('buckets')}  tags={cand.get('metrics',{}).get('tags')}",
            "",
            "### Headline metrics",
        ]
        m = cand.get("metrics", {}) or {}
        lines.append(
            f"dr_1={m.get('dr_1')}  dr_5={m.get('dr_5')}  dr_20={m.get('dr_20')}  "
            f"dr_60={m.get('dr_60')}  dr_120={m.get('dr_120')}"
        )
        lines.append(
            f"rsi={m.get('rsi')}  rsi_mktcap={m.get('rsi_mktcap')}  "
            f"atr20={m.get('atr20')}  vol_vs_10d_avg={m.get('vol_vs_10d_avg')}"
        )
        lines.append(
            f"spike_days={m.get('spike_day_count')}  gap_days={m.get('gapper_day_count')}  "
            f"last_event={m.get('last_event_type')}@{m.get('last_event_date')} "
            f"mag={m.get('last_event_magnitude')}  52w_range={m.get('range_52_week')}"
        )

        lines += [
            "",
            "### Computed technicals",
            f"ema10={feat.ema_10}  ema20={feat.ema_20}  sma50={feat.sma_50}  sma200={feat.sma_200}",
            f"atr20%={feat.atr20_pct}  vol_vs_10d={feat.vol_vs_10d_avg}  "
            f"dist_from_52w_high%={feat.dist_from_52w_high_pct}  "
            f"dist_from_52w_low%={feat.dist_from_52w_low_pct}",
            f"contraction_score={feat.contraction_score}  "
            f"gap_count_30d={feat.gap_count_30d}  volspike_count_30d={feat.volspike_count_30d}",
        ]

        # Recent bars — clipped via config so we keep the prompt short.
        bars_n = self.cfg.get("bars_in_prompt", 20)
        last_bars = feat.recent_bars[-bars_n:]
        lines.append("")
        lines.append("### Recent daily bars (oldest -> newest, o/h/l/c/v):")
        for b in last_bars:
            lines.append(f"{b['d']}  o={b['o']} h={b['h']} l={b['l']} c={b['c']} v={b['v']}")
        return "\n".join(lines)

    def _score_chunk(self, blocks: list[str]) -> list[dict[str, Any]]:
        user_msg = (
            "Score the following stocks. Return JSON exactly as specified in the "
            "system prompt. Stocks are separated by '---'.\n\n"
            + "\n\n---\n\n".join(blocks)
        )
        result = ollama_client.chat_json(
            messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": user_msg},
            ],
            model=self.cfg["model"],
        )
        # Tolerate both `{"results":[...]}` and a bare list.
        if isinstance(result, dict) and "results" in result:
            return result["results"]
        if isinstance(result, list):
            return result
        logger.warning("Unexpected technical-agent JSON shape: %s", type(result))
        return []

    def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        candidates = (input_data or {}).get("candidates", [])[: self.cfg["max_candidates"]]
        if not candidates:
            return self.envelope({"survivors": [], "all_scored": [], "stats": {}})

        # 1) Pre-compute features for every candidate.
        tickers = [c["ticker"] for c in candidates]
        logger.info("Computing technical features for %d tickers...", len(tickers))
        features = price_data.compute_features_bulk(
            tickers, window_days=self.cfg["ohlc_window_days"]
        )

        scored: list[dict[str, Any]] = []
        survivors: list[dict[str, Any]] = []
        skipped = 0
        chunk_size = self.cfg.get("chunk_size", 3)

        for i in range(0, len(candidates), chunk_size):
            chunk = candidates[i:i + chunk_size]
            blocks = []
            chunk_idx_to_ticker: list[str] = []
            for cand in chunk:
                feat = features.get(cand["ticker"])
                if feat is None:
                    skipped += 1
                    continue
                blocks.append(self._format_candidate_block(cand, feat))
                chunk_idx_to_ticker.append(cand["ticker"])

            if not blocks:
                continue

            logger.info("Scoring technical chunk %d-%d (%d tickers)...",
                        i, i + len(chunk) - 1, len(blocks))
            try:
                results = self._score_chunk(blocks)
            except Exception as e:  # noqa: BLE001
                logger.error("Technical chunk failed (%s) — skipping chunk", e)
                continue

            results_by_ticker = {
                (r.get("ticker") or "").upper(): r for r in results if isinstance(r, dict)
            }

            for cand in chunk:
                t = cand["ticker"].upper()
                r = results_by_ticker.get(t)
                if not r:
                    continue
                score = float(r.get("score", 0))
                enriched = {
                    "ticker": cand["ticker"],
                    "company_name": cand.get("company_name"),
                    "sector": cand.get("sector"),
                    "industry": cand.get("industry"),
                    "buckets": cand.get("buckets"),
                    "score": score,
                    "setup": r.get("setup"),
                    "rationale": r.get("rationale"),
                    "warnings": r.get("warnings"),
                    "metrics": cand.get("metrics"),
                }
                scored.append(enriched)
                if score >= self.cfg["min_score"]:
                    survivors.append(enriched)

        scored.sort(key=lambda x: x.get("score", 0), reverse=True)
        survivors.sort(key=lambda x: x.get("score", 0), reverse=True)

        stats = {
            "input_candidates": len(candidates),
            "skipped_no_features": skipped,
            "scored": len(scored),
            "survivors": len(survivors),
            "min_score": self.cfg["min_score"],
            "model": self.cfg["model"],
        }
        logger.info("TechnicalAgent stats: %s", stats)
        return self.envelope({
            "survivors": survivors,
            "all_scored": scored,
            "stats": stats,
        })


if __name__ == "__main__":
    cli_for(TechnicalAgent())
