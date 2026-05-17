"""Per-ticker comments loader.

Comments live in `user_data/abi_comments.json` and are decoupled from
watchlist / dislike membership: a comment can exist for any ticker. The
screener pipeline (s3, s4, s5) is intentionally restricted to comments whose
ticker is ALSO on the watchlist - so adding a stray note to a random ticker
won't influence the screener output.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from daily_screener import config

logger = logging.getLogger(__name__)


def load_comments() -> dict[str, dict[str, Any]]:
    """Return the raw comments file: {TICKER: {notes, created_at, updated_at}}."""
    if not config.ABI_COMMENTS_FILE.exists():
        return {}
    try:
        with config.ABI_COMMENTS_FILE.open("r") as f:
            data = json.load(f) or {}
    except (OSError, ValueError) as e:
        logger.warning("[comments] could not read %s: %s", config.ABI_COMMENTS_FILE, e)
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k).upper(): v for k, v in data.items() if isinstance(v, dict)}


def load_watchlist_raw() -> dict[str, Any]:
    """Return the raw watchlist file (membership + stars + added_at)."""
    if not config.ABI_WATCHLIST_FILE.exists():
        return {}
    try:
        with config.ABI_WATCHLIST_FILE.open("r") as f:
            return json.load(f) or {}
    except (OSError, ValueError) as e:
        logger.warning("[comments] could not read %s: %s", config.ABI_WATCHLIST_FILE, e)
        return {}


def load_watchlist_with_notes() -> dict[str, dict[str, Any]]:
    """Return watchlist entries with their (optional) comment merged in.

    Result shape: ``{TICKER: {notes, stars, added_at}}``. ``notes`` is filled
    from ``abi_comments.json`` when a comment exists for that ticker; tickers
    without a comment get ``notes = ""``. Tickers NOT on the watchlist are
    excluded - which is exactly the filter the screener wants (notes outside
    the watchlist must not influence the pipeline).
    """
    wl = load_watchlist_raw()
    comments = load_comments()
    out: dict[str, dict[str, Any]] = {}
    for tk, entry in wl.items():
        if not isinstance(entry, dict):
            continue
        merged = {k: v for k, v in entry.items() if k != "notes"}
        merged["notes"] = (comments.get(tk.upper()) or {}).get("notes", "")
        out[tk] = merged
    return out


def watchlist_notes_only() -> dict[str, str]:
    """Convenience: ``{TICKER: note}`` only for watchlisted tickers that have
    a non-empty comment. Tickers without a comment are omitted entirely."""
    wl = load_watchlist_raw()
    comments = load_comments()
    out: dict[str, str] = {}
    for tk in wl.keys():
        note = (comments.get(tk.upper()) or {}).get("notes") or ""
        note = note.strip()
        if note:
            out[tk.upper()] = note
    return out
