"""Prompts for the R1D losers brief pipeline."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
_SYNTHESIS_PATH = _PROMPTS_DIR / "synthesis.md"


@lru_cache(maxsize=1)
def synthesis_system_prompt() -> str:
    return _SYNTHESIS_PATH.read_text(encoding="utf-8").strip()


def synthesis_user_message(
    *,
    date_str: str,
    losers_table: str,
    ticker_articles: str,
) -> str:
    return (
        f"<date>{date_str}</date>\n\n"
        f"<losers_table>\n{losers_table}\n</losers_table>\n\n"
        f"<ticker_articles>\n{ticker_articles}\n</ticker_articles>\n\n"
        "Write the R1D losers brief. Source scope is ticker articles only — "
        "do not use channel or macro news."
    )
