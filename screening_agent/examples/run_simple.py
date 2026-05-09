"""Embedding example: run the chain from another Python program.

    python -m screening_agent.examples.run_simple
"""

from __future__ import annotations

import json
import logging

from screening_agent.pipeline import Pipeline


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
    pipe = Pipeline()
    summary = pipe.run_all()
    print(json.dumps(summary["stats"], indent=2, default=str))
    print(f"\n{len(summary.get('final') or [])} final picks. See {pipe.run_dir}/")


if __name__ == "__main__":
    main()
