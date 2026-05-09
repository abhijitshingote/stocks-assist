"""Tiny Ollama wrapper.

Three entry points:

* `chat(messages, ...)`        - non-streaming, returns the full text.
* `chat_stream(messages, ...)` - generator yielding token chunks; logs progress.
* `chat_json(messages, ...)`   - streams under the hood (so the connection
                                 stays alive and we get visible progress) and
                                 then parses the result through Ollama's
                                 `format=json` mode.

The streaming path matters because for long generations, non-streaming makes
the read-timeout effectively a *total*-time budget — Ollama doesn't send any
bytes until the model is done. With streaming, the timeout becomes per-chunk
inactivity, which is what you actually want.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Iterator

import httpx

from .. import config as cfg

logger = logging.getLogger(__name__)


class OllamaError(RuntimeError):
    pass


def _build_body(
    messages: list[dict[str, str]],
    *,
    model: str | None,
    options: dict[str, Any] | None,
    fmt: str | None,
    stream: bool,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": model or cfg.OLLAMA["default_model"],
        "messages": messages,
        "stream": stream,
        "options": {**cfg.OLLAMA["options"], **(options or {})},
        # Keep the model warm between chunks so we don't reload weights.
        "keep_alive": cfg.OLLAMA.get("keep_alive", "30m"),
    }
    if fmt:
        body["format"] = fmt
    return body


def _post_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{cfg.OLLAMA['host'].rstrip('/')}{path}"
    try:
        r = httpx.post(url, json=payload, timeout=cfg.OLLAMA["request_timeout_s"])
        r.raise_for_status()
    except httpx.HTTPError as e:
        raise OllamaError(f"Ollama request failed ({url}): {e}") from e
    return r.json()


def chat_stream(
    messages: list[dict[str, str]],
    *,
    model: str | None = None,
    options: dict[str, Any] | None = None,
    fmt: str | None = None,
    log_every_s: float = 5.0,
) -> Iterator[str]:
    """Yield token chunks. The HTTP read-timeout applies *per chunk* now."""
    url = f"{cfg.OLLAMA['host'].rstrip('/')}/api/chat"
    body = _build_body(messages, model=model, options=options, fmt=fmt, stream=True)

    # Per-chunk read timeout. Connect/write/pool kept short.
    timeout = httpx.Timeout(
        cfg.OLLAMA.get("stream_chunk_timeout_s", 120),
        connect=10,
        write=30,
        pool=10,
    )

    started = time.time()
    last_log = started
    bytes_seen = 0

    try:
        with httpx.stream("POST", url, json=body, timeout=timeout) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    logger.debug("non-json stream line: %s", line[:200])
                    continue

                msg = obj.get("message") or {}
                piece = msg.get("content", "")
                if piece:
                    bytes_seen += len(piece)
                    yield piece

                now = time.time()
                if now - last_log >= log_every_s:
                    logger.info(
                        "  ollama streaming… %ds elapsed, %d chars so far",
                        int(now - started),
                        bytes_seen,
                    )
                    last_log = now

                if obj.get("done"):
                    break
    except httpx.HTTPError as e:
        raise OllamaError(f"Ollama stream failed ({url}): {e}") from e


def chat(
    messages: list[dict[str, str]],
    *,
    model: str | None = None,
    options: dict[str, Any] | None = None,
    fmt: str | None = None,
    stream: bool = True,
) -> str:
    """Single-shot chat. Streams under the hood by default for visibility."""
    if stream:
        parts: list[str] = []
        for piece in chat_stream(messages, model=model, options=options, fmt=fmt):
            parts.append(piece)
        full = "".join(parts)
        if not full:
            raise OllamaError("Empty streamed response from Ollama")
        return full

    body = _build_body(messages, model=model, options=options, fmt=fmt, stream=False)
    data = _post_json("/api/chat", body)
    msg = data.get("message", {}).get("content", "")
    if not msg:
        raise OllamaError(f"Empty response from Ollama: {data}")
    return msg


def chat_json(
    messages: list[dict[str, str]],
    *,
    model: str | None = None,
    options: dict[str, Any] | None = None,
) -> Any:
    """Chat with `format=json`. Returns the parsed JSON value.

    The model is told via the system prompt to emit pure JSON; we additionally
    pass `format=json` to force it. If parsing fails, raises OllamaError.
    """
    raw = chat(messages, model=model, options=options, fmt="json", stream=True)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        # Models occasionally wrap JSON in ```json fences. Strip + retry.
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
            cleaned = cleaned.strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            logger.error("Failed to parse JSON from Ollama: %r", raw[:1500])
            raise OllamaError(f"Bad JSON from model: {e}") from e


def list_models() -> list[str]:
    """Sanity-check helper — used by `run.py` to fail fast if model missing."""
    try:
        r = httpx.get(f"{cfg.OLLAMA['host'].rstrip('/')}/api/tags", timeout=10)
        r.raise_for_status()
        return [m["name"] for m in r.json().get("models", [])]
    except httpx.HTTPError as e:
        raise OllamaError(f"Cannot reach Ollama at {cfg.OLLAMA['host']}: {e}") from e
