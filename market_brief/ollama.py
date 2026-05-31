"""Local Ollama client for per-article market-brief summarization."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

from market_brief import config
from market_brief.prompts import build_article_snippet_prompt

logger = logging.getLogger(__name__)


class OllamaError(RuntimeError):
    pass


def _api_url(path: str) -> str:
    base = config.OLLAMA_BASE_URL.rstrip("/")
    return f"{base}{path}"


def check_model_available(model: str | None = None) -> None:
    model = model or config.OLLAMA_MODEL
    try:
        with urllib.request.urlopen(_api_url("/api/tags"), timeout=5) as resp:
            data = json.loads(resp.read())
    except urllib.error.URLError as e:
        hint = ""
        if config.OLLAMA_BASE_URL.startswith("http://127.0.0.1") or config.OLLAMA_BASE_URL.startswith(
            "http://localhost"
        ):
            hint = (
                " If this run is inside Docker, set "
                "OLLAMA_BASE_URL=http://host.docker.internal:11434 "
                "(or run the brief on the host with Ollama on 127.0.0.1)."
            )
        raise OllamaError(
            f"Ollama not reachable at {config.OLLAMA_BASE_URL}: {e}.{hint}"
        ) from e
    installed = [m["name"] for m in data.get("models", [])]
    if model not in installed:
        raise OllamaError(
            f"Model {model!r} not in Ollama. Installed: {', '.join(installed) or '(none)'}"
        )


def unload_model(model: str | None = None) -> None:
    model = model or config.OLLAMA_MODEL
    payload = {"model": model, "keep_alive": 0}
    req = urllib.request.Request(
        _api_url("/api/generate"),
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=30)
    except urllib.error.URLError:
        pass


def generate(
    prompt: str,
    *,
    model: str | None = None,
    log_label: str | None = None,
) -> str:
    model = model or config.OLLAMA_MODEL
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": config.OLLAMA_TEMPERATURE,
            "num_predict": config.OLLAMA_NUM_PREDICT,
            "num_ctx": config.OLLAMA_NUM_CTX,
        },
    }
    if log_label:
        logger.info("OLLAMA START | %s | model=%s", log_label, model)
    req = urllib.request.Request(
        _api_url("/api/generate"),
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            req, timeout=config.OLLAMA_TIMEOUT_SECONDS
        ) as resp:
            data = json.loads(resp.read())
    except urllib.error.URLError as e:
        if log_label:
            logger.error("OLLAMA FAILED | %s | %s", log_label, e)
        raise OllamaError(str(e)) from e

    response = (data.get("response") or "").strip()
    if not response and data.get("thinking"):
        response = (data.get("thinking") or "").strip()
    if not response:
        raise OllamaError(
            f"empty response (done_reason={data.get('done_reason')!r})"
        )
    if log_label:
        logger.info(
            "OLLAMA DONE | %s | out_chars=%d | %s",
            log_label,
            len(response),
            data.get("done_reason"),
        )
    return response


def summarize_article(article: dict, *, log_label: str | None = None) -> str:
    prompt = build_article_snippet_prompt(article)
    return generate(prompt, log_label=log_label)
