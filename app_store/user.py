"""User-entered stores (watchlist, notes, passes, ...).

Each store loads/saves the same shape the JSON files had, so callers keep their
read-modify-write logic: dict stores are ``{key: {field: value}}``, list stores
are ``[{field: value}]``. Saves replace the whole table in one transaction,
serialized per table. Fields absent from a dict entry load back absent (not
None); timestamps load back as US/Eastern ISO 8601 strings.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pytz
from sqlalchemy import text

from app_store.db import SCHEMA, engine

logger = logging.getLogger(__name__)
ET = pytz.timezone("US/Eastern")


@dataclass(frozen=True)
class _Store:
    table: str
    key: str | None  # dict stores: key column; list stores: None
    cols: tuple[tuple[str, str], ...]  # (json field, kind); kind: plain | ts | date | json
    renames: tuple[tuple[str, str], ...] = ()  # (json field, column) where they differ

    def column(self, field: str) -> str:
        return dict(self.renames).get(field, field)


def _ts(*names: str) -> tuple[tuple[str, str], ...]:
    return tuple((n, "ts") for n in names)


WATCHLIST = _Store("watchlist", "ticker", (("stars", "plain"),) + _ts("added_at"))
DISLIKES = _Store("dislikes", "ticker", (("kind", "plain"),) + _ts("added_at", "expires_at"))
PASSES = _Store("passes", "ticker", (
    ("disposition", "plain"), ("scope", "plain"), ("cycle", "date"),
    ("sources", "plain"), ("at", "ts"),
))
TRADES = _Store("trades", "ticker", (("side", "plain"), ("cycle", "date")) + _ts("added_at", "updated_at"))
TICKER_NOTES = _Store("ticker_notes", "ticker", (("notes", "plain"),) + _ts("created_at", "updated_at"))
CHART_NOTES = _Store("chart_notes", "ticker", (("notes", "plain"),) + _ts("created_at", "updated_at"))
CHART_LEVELS = _Store("chart_levels", "ticker", (("levels", "plain"),) + _ts("created_at", "updated_at"))
ALERT_FIRES = _Store("alert_fires", "key", (("date", "date"), ("price", "plain"), ("fired_at", "ts")))
GENERAL_NOTES = _Store("general_notes", None, (
    ("id", "plain"), ("note_date", "date"), ("title", "plain"), ("content", "plain"),
    ("tags", "plain"),
) + _ts("created_at", "updated_at"))
THEMES = _Store("themes", None, (("name", "plain"), ("desc", "plain"), ("tickers", "plain")),
                renames=(("desc", "description"),))
ETF_GROUPS = _Store("etf_groups", None, (("category", "plain"), ("etfs", "json")))


def _to_db(kind: str, value: Any) -> Any:
    if value is None:
        return None
    if kind == "json":
        return json.dumps(value)
    return value


def _from_db(kind: str, value: Any) -> Any:
    if isinstance(value, datetime):
        return (value if value.tzinfo else ET.localize(value)).astimezone(ET).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _placeholder(store: _Store, field: str, kind: str) -> str:
    p = f":{store.column(field)}"
    return f"CAST({p} AS jsonb)" if kind == "json" else p


def _select(store: _Store) -> list[dict[str, Any]]:
    cols = ([store.key] if store.key else []) + [store.column(f) for f, _ in store.cols]
    sql = f"SELECT {', '.join(cols)} FROM {SCHEMA}.{store.table} ORDER BY position"
    with engine().connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text(sql))]


def _replace(store: _Store, rows: list[dict[str, Any]]) -> None:
    cols = ["position"] + ([store.key] if store.key else []) + [store.column(f) for f, _ in store.cols]
    vals = [":position"] + ([f":{store.key}"] if store.key else []) + [
        _placeholder(store, f, k) for f, k in store.cols
    ]
    insert = text(f"INSERT INTO {SCHEMA}.{store.table} ({', '.join(cols)}) VALUES ({', '.join(vals)})")
    with engine().begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:t))"), {"t": f"app.{store.table}"})
        conn.execute(text(f"DELETE FROM {SCHEMA}.{store.table}"))
        if rows:
            conn.execute(insert, rows)


def _check_fields(store: _Store, entry: dict[str, Any], ident: Any) -> None:
    known = {f for f, _ in store.cols}
    extra = set(entry) - known - ({store.key} if store.key else set())
    if extra:
        logger.warning("app.%s %r: dropping unknown fields %s", store.table, ident, sorted(extra))


def load_dict(store: _Store) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in _select(store):
        entry = {}
        for field, kind in store.cols:
            v = row[store.column(field)]
            if v is not None:
                entry[field] = _from_db(kind, v)
        out[row[store.key]] = entry
    return out


def save_dict(store: _Store, data: dict[str, dict[str, Any]]) -> None:
    rows = []
    for i, (k, entry) in enumerate((data or {}).items()):
        entry = entry if isinstance(entry, dict) else {}
        _check_fields(store, entry, k)
        row = {"position": i, store.key: k}
        for field, kind in store.cols:
            row[store.column(field)] = _to_db(kind, entry.get(field))
        rows.append(row)
    _replace(store, rows)


def load_list(store: _Store) -> list[dict[str, Any]]:
    return [
        {field: _from_db(kind, row[store.column(field)]) for field, kind in store.cols}
        for row in _select(store)
    ]


def save_list(store: _Store, items: list[dict[str, Any]]) -> None:
    rows = []
    for i, entry in enumerate(items or []):
        entry = entry if isinstance(entry, dict) else {}
        _check_fields(store, entry, i)
        row = {"position": i}
        for field, kind in store.cols:
            row[store.column(field)] = _to_db(kind, entry.get(field))
        rows.append(row)
    _replace(store, rows)


# ---- screener_feedback: {date: {ticker: record}} ----------------------------

def load_feedback() -> dict[str, dict[str, Any]]:
    sql = f"SELECT date, ticker, record FROM {SCHEMA}.screener_feedback ORDER BY date, ticker"
    out: dict[str, dict[str, Any]] = {}
    with engine().connect() as conn:
        for d, tk, rec in conn.execute(text(sql)):
            out.setdefault(d.isoformat(), {})[tk] = rec
    return out


def save_feedback(data: dict[str, dict[str, Any]]) -> None:
    rows = [
        {"date": d, "ticker": tk, "record": json.dumps(rec)}
        for d, per in (data or {}).items() if isinstance(per, dict)
        for tk, rec in per.items() if isinstance(rec, dict)
    ]
    with engine().begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('app.screener_feedback'))"))
        conn.execute(text(f"DELETE FROM {SCHEMA}.screener_feedback"))
        if rows:
            conn.execute(text(
                f"INSERT INTO {SCHEMA}.screener_feedback (date, ticker, record) "
                "VALUES (:date, :ticker, CAST(:record AS jsonb))"
            ), rows)
