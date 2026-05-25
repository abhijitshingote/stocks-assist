"""Load/save ``user_data/themes.json`` and apply approved discovery proposals."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from market_brief import config


def load_themes() -> list[dict[str, Any]]:
    if not config.THEMES_FILE.exists():
        return []
    with config.THEMES_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def save_themes(themes: list[dict[str, Any]]) -> None:
    config.THEMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with config.THEMES_FILE.open("w", encoding="utf-8") as f:
        json.dump(themes, f, indent=2)
        f.write("\n")


def normalize_ticker(raw: str) -> str | None:
    s = (raw or "").strip().upper()
    if not s or s in ("GENERAL", "BTC", "CRYPTO"):
        return None
    if ":" in s:
        s = s.split(":")[-1]
    s = re.sub(r"[^A-Z0-9.-]", "", s)
    if not s or len(s) > 6:
        return None
    return s


def theme_ticker_set(theme: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for raw in theme.get("tickers") or []:
        norm = normalize_ticker(str(raw))
        if norm:
            out.add(norm)
    return out


def all_curated_tickers(themes: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for theme in themes:
        out.update(theme_ticker_set(theme))
    return out


def find_theme(themes: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    key = name.strip().lower()
    for theme in themes:
        if (theme.get("name") or "").strip().lower() == key:
            return theme
    return None


def slugify_theme_name(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "theme"


def apply_proposals(
    themes: list[dict[str, Any]],
    proposals: list[dict[str, Any]],
    *,
    only_approved: bool = True,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Return (updated themes, log lines)."""
    logs: list[str] = []
    out = [dict(t) for t in themes]

    for prop in proposals:
        if only_approved and not prop.get("approved"):
            continue
        ptype = prop.get("type")
        pid = prop.get("id", "?")

        if ptype == "new_theme":
            name = (prop.get("name") or "").strip()
            if not name:
                logs.append(f"skip {pid}: missing name")
                continue
            if find_theme(out, name):
                logs.append(f"skip {pid}: theme already exists ({name})")
                continue
            tickers = sorted(
                {
                    t
                    for t in (normalize_ticker(x) for x in (prop.get("tickers") or []))
                    if t
                }
            )
            out.append(
                {
                    "name": name,
                    "desc": (prop.get("desc") or "").strip(),
                    "tickers": tickers,
                }
            )
            logs.append(f"added theme {name} ({len(tickers)} tickers)")

        elif ptype == "add_tickers":
            name = (prop.get("theme_name") or "").strip()
            theme = find_theme(out, name)
            if not theme:
                logs.append(f"skip {pid}: unknown theme {name}")
                continue
            existing = theme_ticker_set(theme)
            added: list[str] = []
            for raw in prop.get("tickers") or []:
                norm = normalize_ticker(str(raw))
                if norm and norm not in existing:
                    existing.add(norm)
                    added.append(norm)
            if added:
                theme["tickers"] = sorted(existing)
                logs.append(f"extended {name}: +{', '.join(added)}")
            else:
                logs.append(f"skip {pid}: no new tickers for {name}")

        else:
            logs.append(f"skip {pid}: unknown type {ptype}")

    return out, logs
