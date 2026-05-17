"""Market brief pipeline orchestrator.

Stages:

1. Load topics (sectors from config + themes from user_data/themes.json).
2. Fan out Perplexity probes (overview + catalyst) in parallel, with
   bounded concurrency to respect `sonar-pro` rate limits.
3. Persist every raw probe response to `outputs/<date>/01_probes/`.
4. Synthesize all probes into a single markdown brief (`02_brief.md`).
5. Convert that brief to JSON for future UI wiring (`02_brief.json`).
6. Write usage/cost snapshot from the shared Perplexity USAGE accumulator.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from daily_screener.utils.perplexity import (
    USAGE,
    PerplexityError,
    call_perplexity,
)

from market_brief import config, prompts, tape as tape_mod

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Topic loading
# ---------------------------------------------------------------------------


@dataclass
class Topic:
    name: str
    desc: str
    tickers: list[str]
    kind: str  # "sector" or "theme"

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "desc": self.desc,
            "tickers": self.tickers,
            "kind": self.kind,
        }


def load_topics() -> list[Topic]:
    topics: list[Topic] = []

    for s in config.SECTORS:
        topics.append(
            Topic(
                name=s["name"],
                desc=s.get("desc", ""),
                tickers=[],
                kind="sector",
            )
        )

    if config.USE_USER_THEMES and config.THEMES_FILE.exists():
        with config.THEMES_FILE.open("r", encoding="utf-8") as f:
            raw = json.load(f) or []
        for t in raw[: config.MAX_THEMES]:
            topics.append(
                Topic(
                    name=t.get("name", "").strip(),
                    desc=(t.get("desc") or "").strip(),
                    tickers=t.get("tickers") or [],
                    kind="theme",
                )
            )

    # De-dupe by lowercase name (sector + theme could collide).
    seen: set[str] = set()
    deduped: list[Topic] = []
    for t in topics:
        key = t.name.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(t)
    return deduped


# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------


@dataclass
class ProbeResult:
    topic_name: str
    topic_kind: str  # "sector" | "theme"
    kind: str  # "overview" | "catalyst"
    content: str
    error: str | None = None
    elapsed_s: float = 0.0


def _slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "topic"


def _run_probe(topic: Topic, kind: str, asof: str) -> ProbeResult:
    if kind == "overview":
        try:
            tape_block = tape_mod.get_topic_tape_block(topic.as_dict(), asof)
        except Exception as e:  # noqa: BLE001 — tape is best-effort
            logger.warning("tape lookup failed for %s: %s", topic.name, e)
            tape_block = None
        prompt = prompts.build_overview_prompt(
            topic.as_dict(), asof=asof, tape_block=tape_block
        )
    elif kind == "catalyst":
        prompt = prompts.build_catalyst_prompt(topic.as_dict(), asof=asof)
    else:
        raise ValueError(f"unknown probe kind: {kind}")

    t0 = time.time()
    try:
        content = call_perplexity(
            prompt,
            model=config.PROBE_MODEL,
            max_tokens=config.PROBE_MAX_TOKENS,
            temperature=config.PROBE_TEMPERATURE,
            timeout=config.PROBE_TIMEOUT_SECONDS,
        )
        return ProbeResult(
            topic_name=topic.name,
            topic_kind=topic.kind,
            kind=kind,
            content=content,
            elapsed_s=time.time() - t0,
        )
    except PerplexityError as e:
        logger.error("probe failed: %s/%s: %s", topic.name, kind, e)
        return ProbeResult(
            topic_name=topic.name,
            topic_kind=topic.kind,
            kind=kind,
            content="",
            error=str(e),
            elapsed_s=time.time() - t0,
        )


def run_probes(topics: list[Topic], asof: str) -> list[ProbeResult]:
    """Fan out probes per topic (kinds controlled by config)."""
    jobs: list[tuple[Topic, str]] = []
    for t in topics:
        for kind in config.ENABLED_PROBE_KINDS:
            jobs.append((t, kind))

    results: list[ProbeResult] = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.PROBE_CONCURRENCY
    ) as pool:
        futures = {
            pool.submit(_run_probe, topic, kind, asof): (topic.name, kind)
            for topic, kind in jobs
        }
        for fut in concurrent.futures.as_completed(futures):
            name, kind = futures[fut]
            try:
                res = fut.result()
                logger.info(
                    "probe done: %s/%s (%.1fs%s)",
                    name,
                    kind,
                    res.elapsed_s,
                    f", err={res.error}" if res.error else "",
                )
                results.append(res)
            except Exception as e:  # noqa: BLE001 — surface as failed probe
                logger.exception("probe crashed: %s/%s", name, kind)
                results.append(
                    ProbeResult(
                        topic_name=name,
                        topic_kind="unknown",
                        kind=kind,
                        content="",
                        error=f"crashed: {e}",
                    )
                )
    return results


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def persist_probes(results: list[ProbeResult], outdir: Path) -> None:
    probes_dir = outdir / "01_probes"
    probes_dir.mkdir(parents=True, exist_ok=True)
    for r in results:
        slug = _slugify(r.topic_name)
        fname = f"{slug}__{r.kind}.md"
        body = (
            f"# {r.topic_name} — {r.kind}\n\n"
            f"_kind: {r.topic_kind} · elapsed: {r.elapsed_s:.1f}s_\n\n"
        )
        if r.error:
            body += f"> **ERROR**: {r.error}\n\n"
        body += r.content or "_(no content)_\n"
        (probes_dir / fname).write_text(body, encoding="utf-8")


# ---------------------------------------------------------------------------
# Synthesis
# ---------------------------------------------------------------------------


def synthesize(results: list[ProbeResult], asof: str) -> str:
    payload = [
        {
            "topic_name": r.topic_name,
            "kind": r.kind,
            "content": r.content,
        }
        for r in results
        if r.content
    ]
    if not payload:
        return f"# Daily Market Brief — {asof}\n\n_(all probes failed; see 01_probes/)_\n"

    # Build a global tape block: indices + the union of every topic's tickers.
    try:
        all_tickers: list[str] = []
        for sect_tickers in tape_mod.SECTOR_TICKERS.values():
            all_tickers.extend(sect_tickers)
        if config.USE_USER_THEMES and config.THEMES_FILE.exists():
            import json as _json
            with config.THEMES_FILE.open("r", encoding="utf-8") as f:
                for t in _json.load(f) or []:
                    all_tickers.extend(t.get("tickers") or [])
        session_date, t_quotes, i_quotes = tape_mod.get_tape(all_tickers, asof)
        tape_block = tape_mod.format_tape_block(session_date, t_quotes, i_quotes)
    except Exception as e:  # noqa: BLE001
        logger.warning("global tape lookup failed: %s", e)
        tape_block = None

    prompt = prompts.build_synthesis_prompt(payload, asof=asof, tape_block=tape_block)
    return call_perplexity(
        prompt,
        model=config.SYNTH_MODEL,
        max_tokens=config.SYNTH_MAX_TOKENS,
        temperature=config.SYNTH_TEMPERATURE,
        timeout=config.SYNTH_TIMEOUT_SECONDS,
    )


def synthesize_json(brief_md: str, asof: str) -> dict:
    prompt = prompts.build_json_synthesis_prompt(brief_md, asof=asof)
    raw = call_perplexity(
        prompt,
        model=config.SYNTH_MODEL,
        max_tokens=config.SYNTH_MAX_TOKENS,
        temperature=0.0,
        timeout=config.SYNTH_TIMEOUT_SECONDS,
    )
    cleaned = _strip_code_fence(raw).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("json synth returned non-JSON; persisting raw text")
        return {"_raw": raw, "_parse_error": True}


def _strip_code_fence(text: str) -> str:
    """Remove ```json ... ``` wrappers if the model added them anyway."""
    text = text.strip()
    if text.startswith("```"):
        # Drop first line (``` or ```json) and trailing fence.
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines)
    return text


# ---------------------------------------------------------------------------
# Top-level
# ---------------------------------------------------------------------------


def run_brief(asof: str | None = None) -> Path:
    """Run the full pipeline. Returns the output directory path."""
    USAGE.reset()

    asof = asof or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    outdir = config.OUTPUTS_DIR / asof
    outdir.mkdir(parents=True, exist_ok=True)

    # File-based logging for this run (in addition to stdout).
    log_path = outdir / "run.log"
    file_handler = logging.FileHandler(log_path, mode="w")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s")
    )
    root = logging.getLogger()
    root.addHandler(file_handler)
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)

    try:
        topics = load_topics()
        logger.info(
            "loaded %d topics (%d sectors + %d themes)",
            len(topics),
            sum(1 for t in topics if t.kind == "sector"),
            sum(1 for t in topics if t.kind == "theme"),
        )
        for t in topics:
            logger.info("  · %s [%s]", t.name, t.kind)

        # Stage 1: probes
        t0 = time.time()
        results = run_probes(topics, asof)
        logger.info(
            "probes complete: %d total, %d failed, %.1fs wall",
            len(results),
            sum(1 for r in results if r.error),
            time.time() - t0,
        )
        persist_probes(results, outdir)

        # Stage 2: synthesis (markdown)
        logger.info("synthesizing markdown brief…")
        brief_md = synthesize(results, asof)
        (outdir / "02_brief.md").write_text(brief_md, encoding="utf-8")

        # Stage 3: synthesis (json)
        logger.info("synthesizing JSON brief…")
        try:
            brief_json = synthesize_json(brief_md, asof)
        except PerplexityError as e:
            logger.error("json synth failed: %s", e)
            brief_json = {"_error": str(e)}
        (outdir / "02_brief.json").write_text(
            json.dumps(brief_json, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        # Usage snapshot
        usage = USAGE.snapshot()
        (outdir / "usage.json").write_text(
            json.dumps(usage, indent=2),
            encoding="utf-8",
        )
        logger.info(
            "DONE: %s · %d probes · $%.4f · %d tokens",
            outdir,
            len(results),
            usage["cost_usd_total"],
            usage["total_prompt_tokens"] + usage["total_completion_tokens"],
        )
        return outdir
    finally:
        root.removeHandler(file_handler)
        file_handler.close()
