"""End-to-end orchestrator.

Stages:
  1. Initial Screener        -> 01_initial.json
  2. Technical Agent          -> 02_technical.json
  3. Market News Agent        -> 03_market_news.json
  4. Fundamental Agent        -> 04_fundamental.json
  +  Summary roll-up          -> 00_summary.json

Each stage is fully isolated: every output is JSON, persisted to disk, and
re-runnable on demand. `Pipeline` exposes per-stage methods so `run.py` (or
your own scripts) can stitch them together however they want.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from . import config as cfg
from .agents.base import save_json
from .agents.fundamental_agent import FundamentalAgent
from .agents.initial_screener import InitialScreener
from .agents.market_news_agent import MarketNewsAgent
from .agents.technical_agent import TechnicalAgent

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, run_id: str | None = None) -> None:
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        self.run_dir: Path = cfg.OUTPUTS_DIR / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.initial = InitialScreener()
        self.technical = TechnicalAgent()
        self.market_news = MarketNewsAgent()
        self.fundamental = FundamentalAgent()

    def _save(self, name: str, data: dict[str, Any]) -> Path:
        path = self.run_dir / name
        save_json(path, data)
        logger.info("Saved %s", path)
        return path

    def run_initial(self) -> dict[str, Any]:
        logger.info("=== Stage 1: Initial Screener ===")
        out = self.initial.run({})
        self._save("01_initial.json", out)
        return out

    def run_technical(self, initial_out: dict[str, Any]) -> dict[str, Any]:
        logger.info("=== Stage 2: Technical Agent ===")
        out = self.technical.run(initial_out)
        self._save("02_technical.json", out)
        return out

    def run_market_news(self) -> dict[str, Any]:
        logger.info("=== Stage 3: Market News Agent ===")
        out = self.market_news.run({})
        self._save("03_market_news.json", out)
        return out

    def run_fundamental(
        self, technical_out: dict[str, Any], market_news_out: dict[str, Any],
    ) -> dict[str, Any]:
        logger.info("=== Stage 4: Fundamental Agent ===")
        merged = {
            "survivors": technical_out.get("survivors", []),
            "themes": market_news_out.get("themes", []),
        }
        out = self.fundamental.run(merged)
        self._save("04_fundamental.json", out)
        return out

    def run_all(self) -> dict[str, Any]:
        initial_out = self.run_initial()
        technical_out = self.run_technical(initial_out)
        market_news_out = self.run_market_news()
        fundamental_out = self.run_fundamental(technical_out, market_news_out)

        summary = {
            "run_id": self.run_id,
            "stats": {
                "initial":     initial_out.get("stats"),
                "technical":   technical_out.get("stats"),
                "market_news": market_news_out.get("stats"),
                "fundamental": fundamental_out.get("stats"),
            },
            "themes": market_news_out.get("themes"),
            "final": fundamental_out.get("final"),
        }
        self._save("00_summary.json", summary)
        return summary
