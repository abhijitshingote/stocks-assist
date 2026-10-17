"""Per-ticker Abi ticker notes loader.

Notes live in ``app.ticker_notes``, decoupled from watchlist / dislike
membership. The screener pipeline (s3, s4, s5) only uses notes whose
ticker is on the watchlist.
"""

from __future__ import annotations

from typing import Any

from app_store import user as user_store


def load_abi_ticker_notes() -> dict[str, dict[str, Any]]:
    """{TICKER: {notes, created_at, updated_at}}."""
    data = user_store.load_dict(user_store.TICKER_NOTES)
    return {str(k).upper(): v for k, v in data.items()}


def load_watchlist_raw() -> dict[str, Any]:
    """Watchlist membership + added_at. Legacy ``stars`` values are ignored."""
    return user_store.load_dict(user_store.WATCHLIST)


def load_watchlist_with_notes() -> dict[str, dict[str, Any]]:
    """Return watchlist entries with their (optional) Abi ticker note merged in.

    Result shape: ``{TICKER: {notes, added_at}}``. ``notes`` is filled
    from Abi ticker notes when present; tickers without a note get
    ``notes = ""``. Tickers NOT on the watchlist are excluded.
    """
    wl = load_watchlist_raw()
    comments = load_abi_ticker_notes()
    out: dict[str, dict[str, Any]] = {}
    for tk, entry in wl.items():
        merged = dict(entry)
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
