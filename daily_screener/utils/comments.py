"""Per-ticker Abi ticker notes loader.

Notes live in ``user_data/abi_ticker_notes.json``, decoupled from watchlist /
dislike membership. The screener pipeline (s3, s4, s5) only uses notes whose
ticker is on the watchlist.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from daily_screener import config

logger = logging.getLogger(__name__)


def load_abi_ticker_notes() -> dict[str, dict[str, Any]]:
    """Return the raw Abi ticker notes file: {TICKER: {notes, created_at, updated_at}}."""
    if not config.ABI_TICKER_NOTES_FILE.exists():
        return {}
    try:
        with config.ABI_TICKER_NOTES_FILE.open("r") as f:
            data = json.load(f) or {}
    except (OSError, ValueError) as e:
        logger.warning("[abi-ticker-notes] could not read %s: %s", config.ABI_TICKER_NOTES_FILE, e)
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k).upper(): v for k, v in data.items() if isinstance(v, dict)}


def load_watchlist_raw() -> dict[str, Any]:
    """Return the raw watchlist file (membership + stars + added_at).

    Drops any embedded ``notes`` keys; Abi ticker notes are only in
    abi_ticker_notes.json.
    """
    if not config.ABI_WATCHLIST_FILE.exists():
        return {}
    try:
        with config.ABI_WATCHLIST_FILE.open("r") as f:
            data = json.load(f) or {}
    except (OSError, ValueError) as e:
        logger.warning("[abi-ticker-notes] could not read %s: %s", config.ABI_WATCHLIST_FILE, e)
        return {}
    if not isinstance(data, dict):
        return {}
    for entry in data.values():
        if isinstance(entry, dict) and "notes" in entry:
            del entry["notes"]
    return data


def load_watchlist_with_notes() -> dict[str, dict[str, Any]]:
    """Return watchlist entries with their (optional) Abi ticker note merged in.

    Result shape: ``{TICKER: {notes, stars, added_at}}``. ``notes`` is filled
    from Abi ticker notes when present; tickers without a note get
    ``notes = ""``. Tickers NOT on the watchlist are excluded.
    """
    wl = load_watchlist_raw()
    comments = load_abi_ticker_notes()
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
    a non-empty Abi ticker note. Tickers without a note are omitted entirely."""
    wl = load_watchlist_raw()
    comments = load_abi_ticker_notes()
    out: dict[str, str] = {}
    for tk in wl.keys():
        note = (comments.get(tk.upper()) or {}).get("notes") or ""
        note = note.strip()
        if note:
            out[tk.upper()] = note
    return out
