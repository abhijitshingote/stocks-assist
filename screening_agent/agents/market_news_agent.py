"""Stage 3 — Market News Agent.

Macro / narrative scanner. Independent of any specific stock list.

Two steps:

  Step 1 — Grounded fetch
      Ask Perplexity (cheap `sonar` model, recency=week) for the hottest
      market themes right now. Free-form text + citations.

  Step 2 — Structure
      Hand that text to Ollama with `format=json` to produce the canonical
      themes JSON the next stage expects.

Output:
  {
    "themes": [
      {"name": ..., "catalyst": ..., "thesis": ..., "keywords": [...],
       "example_tickers": [...], "sources": [...]}
    ],
    "raw_perplexity": {"text": "...", "citations": [...]},
    "stats": {...}
  }

Run standalone:
    python -m screening_agent.agents.market_news_agent \
        --input <unused-ok> --output outputs/<run>/03_market_news.json
"""

from __future__ import annotations

import logging
from typing import Any

from .. import config as cfg
from ..tools import ollama_client, perplexity
from .base import Agent, cli_for, load_prompt

logger = logging.getLogger(__name__)


class MarketNewsAgent(Agent):
    name = "market_news_agent"

    def __init__(self) -> None:
        self.cfg = cfg.MARKET_NEWS_AGENT
        self.perplexity_prompt = load_prompt(self.cfg["perplexity_prompt_file"])
        self.themes_prompt = load_prompt(self.cfg["themes_prompt_file"])

    def _fetch_perplexity(self) -> perplexity.PerplexityResult:
        # Inject lookback into the prompt template via simple substitution.
        prompt = self.perplexity_prompt.replace(
            "{LOOKBACK_DAYS}", str(self.cfg["lookback_days"])
        )
        logger.info(
            "Querying Perplexity for hot market themes (recency=%s, model=%s, max_tokens=%s)...",
            self.cfg["recency"],
            self.cfg["perplexity_model"],
            self.cfg.get("perplexity_max_tokens", 1200),
        )
        return perplexity.search(
            prompt,
            model=self.cfg["perplexity_model"],
            recency=self.cfg["recency"],
            max_tokens=self.cfg.get("perplexity_max_tokens", 1200),
            timeout_s=self.cfg.get("perplexity_timeout_s", 90),
        )

    def _structure_with_ollama(self, perplexity_text: str, citations: list[str]) -> list[dict[str, Any]]:
        cite_block = "\n".join(f"- {c}" for c in citations) or "(no citations)"
        user_msg = (
            "## Perplexity-grounded market summary\n\n"
            f"{perplexity_text}\n\n"
            "## Citations\n"
            f"{cite_block}\n\n"
            "## Task\n"
            "Distill the above into the JSON format described in the system prompt."
        )
        result = ollama_client.chat_json(
            messages=[
                {"role": "system", "content": self.themes_prompt},
                {"role": "user", "content": user_msg},
            ],
            model=self.cfg["ollama_model"],
            # Bump context: ~4000 tok of Perplexity input + system prompt +
            # 12-15 structured themes in JSON output is too tight for the
            # global 8192 default.
            options={"num_ctx": 16384},
        )
        if isinstance(result, dict) and "themes" in result:
            return result["themes"]
        if isinstance(result, list):
            return result
        logger.warning("Unexpected themes JSON shape: %s", type(result))
        return []

    def run(self, input_data: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            pres = self._fetch_perplexity()
        except Exception as e:  # noqa: BLE001
            logger.error("Perplexity call failed: %s", e)
            return self.envelope({
                "themes": [],
                "raw_perplexity": {"text": "", "citations": []},
                "stats": {"error": str(e)},
            })

        logger.info("Got %d chars + %d citations from Perplexity. Structuring with Ollama...",
                    len(pres.text), len(pres.citations))

        themes: list[dict[str, Any]] = []
        try:
            themes = self._structure_with_ollama(pres.text, pres.citations)
        except Exception as e:  # noqa: BLE001
            logger.error("Theme structuring failed: %s", e)

        stats = {
            "perplexity_chars": len(pres.text),
            "citations": len(pres.citations),
            "themes_found": len(themes),
            "perplexity_model": self.cfg["perplexity_model"],
            "ollama_model": self.cfg["ollama_model"],
        }
        logger.info("MarketNewsAgent stats: %s", stats)
        return self.envelope({
            "themes": themes,
            "raw_perplexity": {"text": pres.text, "citations": pres.citations},
            "stats": stats,
        })


if __name__ == "__main__":
    # Doesn't actually need an input file, but cli_for keeps the convention.
    # Pass any JSON path (e.g. /dev/null) — content is ignored.
    cli_for(MarketNewsAgent())
