"""Base utilities for agents in the chain.

Conventions every agent honours:

* `name` — short slug used to title artifacts.
* `run(input_data: dict) -> dict` — pure function, no surprise mutations.
* Standalone CLI: each agent module exposes a `main()` that reads/writes JSON
  paths so you can run a single stage with the previous stage's output.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from .. import config as cfg

logger = logging.getLogger(__name__)


class Agent:
    name: str = "agent"

    def run(self, input_data: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover
        raise NotImplementedError

    def envelope(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Wrap output with provenance metadata for downstream debugging."""
        return {
            "agent": self.name,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            **payload,
        }


def load_prompt(filename: str) -> str:
    path = cfg.PROMPTS_DIR / filename
    return path.read_text(encoding="utf-8")


def load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def save_json(path: str | Path, data: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2, default=str))


def cli_for(agent: Agent) -> None:
    """Boilerplate `if __name__ == '__main__':` for any agent."""
    ap = argparse.ArgumentParser(description=f"Run {agent.name} standalone.")
    ap.add_argument("--input", required=True, help="Input JSON path (output from previous stage).")
    ap.add_argument("--output", required=True, help="Where to write this stage's JSON output.")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )

    in_data = load_json(args.input)
    out = agent.run(in_data)
    save_json(args.output, out)
    logger.info("Wrote %s", args.output)
