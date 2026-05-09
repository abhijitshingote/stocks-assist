"""Thin Perplexity API wrapper.

We use Perplexity's chat-completions endpoint (OpenAI-compatible) so we can
ask natural-language questions and get back grounded answers WITH citations
and a freshness filter.

Two helpers:

* `search(prompt, ...)`         — returns the assistant text + citations.
* `search_recent(prompt, ...)`  — same but with `search_recency_filter='week'`,
                                  which is exactly what we want for hot-themes
                                  and recent-material-events queries.

Auth: reads `PERPLEXITY_API_KEY` from the environment (already present in
this repo's `.env`).

Default model: `sonar` — Perplexity's cheapest grounded model.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

PERPLEXITY_URL = "https://api.perplexity.ai/chat/completions"


class PerplexityError(RuntimeError):
    pass


@dataclass
class PerplexityResult:
    text: str
    citations: list[str]
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"text": self.text, "citations": self.citations}


def _api_key() -> str:
    key = os.getenv("PERPLEXITY_API_KEY")
    if not key:
        raise PerplexityError(
            "PERPLEXITY_API_KEY not set. Add it to .env or export it."
        )
    return key


def search(
    prompt: str,
    *,
    model: str = "sonar",
    system: str | None = None,
    recency: str | None = None,        # one of: hour, day, week, month, year
    temperature: float = 0.2,
    max_tokens: int = 1200,
    timeout_s: float = 90,
) -> PerplexityResult:
    """Call Perplexity. `recency` filters the underlying web search."""
    messages: list[dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "return_citations": True,
    }
    if recency:
        payload["search_recency_filter"] = recency

    try:
        r = httpx.post(
            PERPLEXITY_URL,
            headers={
                "Authorization": f"Bearer {_api_key()}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout_s,
        )
        r.raise_for_status()
    except httpx.HTTPError as e:
        raise PerplexityError(f"Perplexity request failed: {e}") from e

    data = r.json()
    try:
        text = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as e:
        raise PerplexityError(f"Unexpected Perplexity response shape: {data}") from e

    # Citations live at the top level on Perplexity responses.
    citations = data.get("citations") or []
    if isinstance(citations, list):
        citations = [str(c) for c in citations]
    else:
        citations = []

    return PerplexityResult(text=text, citations=citations, raw=data)


def search_recent(prompt: str, **kwargs: Any) -> PerplexityResult:
    """Convenience: same as `search()` with recency='week'."""
    kwargs.setdefault("recency", "week")
    return search(prompt, **kwargs)
