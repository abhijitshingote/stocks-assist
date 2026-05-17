"""Stage 3: Theme Vector Build.

Combines three inputs into a daily "theme vector" used by stages 4 and 5:

  1. User-curated themes from `user_data/themes.json`.
  2. User-taste themes extracted from `user_data/abi_watchlist.json` by Claude.
     Cached to 02b_user_themes.json - only re-extracted when the watchlist hash
     changes.
  3. Hot market themes for this week from a single Perplexity scan, cached to
     02a_market_themes.json (HOT_MARKET_CACHE_DAYS old before re-run).

Output: outputs/<date>/02_theme_vector.json
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Any

from daily_screener import config
from daily_screener.utils import io
from daily_screener.utils.comments import load_watchlist_with_notes
from daily_screener.utils.llm import LLMError, call_llm_json, extract_json
from daily_screener.utils.perplexity import PerplexityError, call_perplexity

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# (1) Curated themes
# ---------------------------------------------------------------------------

def _load_curated_themes() -> list[dict[str, Any]]:
    if not config.THEMES_FILE.exists():
        return []
    try:
        with config.THEMES_FILE.open("r") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        logger.warning("themes.json invalid; skipping curated themes")
        return []

    out = []
    for entry in data or []:
        name = entry.get("name", "")
        if not name:
            continue
        out.append(
            {
                "tag": _slugify(name),
                "label": name,
                "description": entry.get("desc") or "",
                "example_tickers": entry.get("tickers") or [],
                "source": "curated",
                "weight": config.THEME_WEIGHTS["curated"],
            }
        )
    return out


def _slugify(name: str) -> str:
    return (
        name.lower()
        .replace("&", "and")
        .replace("/", "-")
        .replace(",", "")
        .replace("'", "")
        .replace(".", "")
        .strip()
        .replace(" ", "-")
    )


# ---------------------------------------------------------------------------
# (2) User-taste themes (extracted via Claude, cached by watchlist hash)
# ---------------------------------------------------------------------------

def _watchlist_hash(wl: dict) -> str:
    raw = json.dumps(wl, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:12]


def _load_watchlist() -> dict[str, Any]:
    """Return watchlisted tickers joined with their (optional) comments.

    Notes are sourced from `user_data/abi_comments.json` and included only
    for tickers that are on the watchlist (so free-floating comments outside
    the watchlist never reach the user-theme extractor).
    """
    return load_watchlist_with_notes()


def _extract_user_themes(date_str: str, force: bool = False) -> dict[str, Any]:
    """Returns dict with `watchlist_hash` and `themes` list."""
    wl = _load_watchlist()
    if not wl:
        return {"watchlist_hash": None, "themes": []}

    cached_path = io.artifact_path(date_str, "02b_user_themes.json")
    current_hash = _watchlist_hash(wl)

    # Try today's cache first; if absent, look at most-recent prior day's cache.
    cached = io.read_json_if_exists(cached_path)
    if not cached:
        cached = _find_most_recent_user_themes_cache()

    if not force and cached and cached.get("watchlist_hash") == current_hash:
        logger.info("[s3] user themes cache hit (hash=%s)", current_hash)
        # Re-persist into today's run dir for completeness.
        io.write_json(cached_path, cached)
        return cached

    model = config.USER_THEME_EXTRACTION_MODEL
    logger.info("[s3] extracting user themes via %s (hash=%s)", model, current_hash)
    prompt_template = io.load_prompt("user_theme_extraction")
    prompt = prompt_template.replace("{watchlist_json}", json.dumps(wl, indent=2))
    try:
        themes_raw = call_llm_json(prompt, model=model, max_tokens=2500)
    except (LLMError, ValueError) as e:
        logger.error("[s3] user theme extraction failed: %s", e)
        # Fall back to empty - we still have curated themes.
        return {"watchlist_hash": current_hash, "themes": [], "error": str(e)}

    themes = []
    for t in themes_raw if isinstance(themes_raw, list) else []:
        if not isinstance(t, dict) or not t.get("tag"):
            continue
        themes.append(
            {
                "tag": t["tag"],
                "label": t.get("tag", "").replace("-", " ").title(),
                "description": t.get("description") or "",
                "example_tickers": t.get("example_tickers") or [],
                "anti_pattern": t.get("anti_pattern") or "",
                "source": "user_taste",
                "weight": config.THEME_WEIGHTS["user_taste"],
            }
        )

    payload = {"watchlist_hash": current_hash, "themes": themes}
    io.write_json(cached_path, payload)
    return payload


def _find_most_recent_user_themes_cache() -> dict | None:
    """Walk prior date folders newest-first looking for 02b_user_themes.json."""
    if not config.OUTPUTS_DIR.exists():
        return None
    candidates = sorted(
        [p for p in config.OUTPUTS_DIR.iterdir() if p.is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )
    for d in candidates:
        f = d / "02b_user_themes.json"
        if f.exists():
            return io.read_json_if_exists(f)
    return None


# ---------------------------------------------------------------------------
# (3) Hot market themes (Perplexity, cached HOT_MARKET_CACHE_DAYS)
# ---------------------------------------------------------------------------

def _fetch_hot_market_themes(date_str: str, force: bool = False) -> dict[str, Any]:
    cached_path = io.artifact_path(date_str, "02a_market_themes.json")
    if not force and cached_path.exists():
        logger.info("[s3] using today's cached market themes")
        return io.read_json(cached_path)

    if not force and config.HOT_MARKET_CACHE_DAYS > 0:
        # Look back N days for a still-fresh cache.
        run_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        for back in range(1, config.HOT_MARKET_CACHE_DAYS + 1):
            prior = (run_dt - timedelta(days=back)).strftime("%Y-%m-%d")
            prior_path = io.artifact_path(prior, "02a_market_themes.json")
            if prior_path.exists():
                logger.info("[s3] reusing market themes from %s", prior)
                data = io.read_json(prior_path)
                io.write_json(cached_path, data)
                return data

    logger.info("[s3] fetching hot market themes via Perplexity")
    prompt = io.load_prompt("market_themes")
    try:
        raw = call_perplexity(prompt, max_tokens=2500)
        themes_raw = extract_json(raw)
    except (PerplexityError, ValueError) as e:
        logger.error("[s3] hot market theme fetch failed: %s", e)
        payload = {"themes": [], "error": str(e), "fetched_at": datetime.utcnow().isoformat()}
        io.write_json(cached_path, payload)
        return payload

    themes = []
    for t in themes_raw if isinstance(themes_raw, list) else []:
        if not isinstance(t, dict) or not t.get("tag"):
            continue
        themes.append(
            {
                "tag": t["tag"],
                "label": t.get("tag", "").replace("-", " ").title(),
                "description": t.get("description") or "",
                "example_tickers": t.get("examples") or t.get("example_tickers") or [],
                "source": "hot_market",
                "weight": config.THEME_WEIGHTS["hot_market"],
            }
        )

    payload = {"themes": themes, "fetched_at": datetime.utcnow().isoformat()}
    io.write_json(cached_path, payload)
    return payload


# ---------------------------------------------------------------------------
# Merger
# ---------------------------------------------------------------------------

def _merge_themes(buckets: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Merge by tag; if same tag appears in multiple buckets, sum weights and
    union example tickers / sources."""
    by_tag: dict[str, dict[str, Any]] = {}
    for bucket in buckets:
        for t in bucket:
            tag = t["tag"]
            if tag in by_tag:
                existing = by_tag[tag]
                existing["weight"] = round(existing["weight"] + t["weight"], 2)
                existing["sources"] = sorted(set(existing["sources"]) | {t["source"]})
                existing["example_tickers"] = sorted(
                    set(existing["example_tickers"]) | set(t.get("example_tickers") or [])
                )
                if t.get("description") and t["description"] not in (existing.get("descriptions") or []):
                    existing.setdefault("descriptions", []).append(t["description"])
                if t.get("anti_pattern") and not existing.get("anti_pattern"):
                    existing["anti_pattern"] = t["anti_pattern"]
            else:
                by_tag[tag] = {
                    "tag": tag,
                    "label": t.get("label") or tag.replace("-", " ").title(),
                    "weight": t["weight"],
                    "sources": [t["source"]],
                    "descriptions": [t["description"]] if t.get("description") else [],
                    "example_tickers": list(t.get("example_tickers") or []),
                    "anti_pattern": t.get("anti_pattern") or "",
                }

    merged = list(by_tag.values())
    merged.sort(key=lambda x: -x["weight"])
    return merged


def run(date_str: str | None = None, *, force_refresh: bool = False) -> dict[str, Any]:
    date_str = date_str or io.today_str()
    logger.info("[s3] building theme vector for %s", date_str)

    curated = _load_curated_themes()
    user = _extract_user_themes(date_str, force=force_refresh)
    market = _fetch_hot_market_themes(date_str, force=force_refresh)

    merged = _merge_themes([curated, user.get("themes", []), market.get("themes", [])])

    artifact = {
        "date": date_str,
        "curated_count": len(curated),
        "user_taste_count": len(user.get("themes", [])),
        "hot_market_count": len(market.get("themes", [])),
        "merged_count": len(merged),
        "watchlist_hash": user.get("watchlist_hash"),
        "themes": merged,
        "errors": {
            "user_taste": user.get("error"),
            "hot_market": market.get("error"),
        },
    }

    io.write_json(io.artifact_path(date_str, "02_theme_vector.json"), artifact)
    logger.info(
        "[s3] merged=%d (curated=%d, user=%d, market=%d)",
        len(merged),
        len(curated),
        len(user.get("themes", [])),
        len(market.get("themes", [])),
    )
    return artifact


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run(args.date, force_refresh=args.force_refresh)


if __name__ == "__main__":
    main()
