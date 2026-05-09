"""Tiny end-to-end Ollama check.

Run this BEFORE the full pipeline if you suspect the model/host is slow:

    python -m screening_agent.examples.ollama_smoke

Streams the response so you'll see tokens arrive within seconds — if you
don't, the model is truly stuck (or wrong host).
"""

from __future__ import annotations

import logging
import time

from screening_agent import config as cfg
from screening_agent.tools import ollama_client


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
    print(f"host  = {cfg.OLLAMA['host']}")
    print(f"model = {cfg.OLLAMA['default_model']}")
    print(f"models on host: {ollama_client.list_models()}")
    print("---")

    started = time.time()
    chars = 0
    for piece in ollama_client.chat_stream(
        messages=[
            {"role": "system", "content": "You return JSON only."},
            {"role": "user", "content":
                'Score AAPL 0-10 for momentum. Return {"score": <num>, "why": "<5 words>"}.'},
        ],
        fmt="json",
    ):
        print(piece, end="", flush=True)
        chars += len(piece)
    print()
    print(f"--- {chars} chars in {time.time() - started:.1f}s ---")


if __name__ == "__main__":
    main()
