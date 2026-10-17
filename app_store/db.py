"""Engine + DDL for the ``app`` schema (user and workflow data).

``public`` is rebuilt nightly by ``init-zd`` (schema swap); ``app`` is never
touched by it. No foreign keys from ``app`` into ``public``: the swap drops
``public`` with CASCADE.
"""

from __future__ import annotations

import os
import threading

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SCHEMA = "app"

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

CREATE TABLE IF NOT EXISTS {SCHEMA}.watchlist (
    ticker      TEXT PRIMARY KEY,
    position    INTEGER NOT NULL,
    stars       INTEGER,
    added_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.dislikes (
    ticker      TEXT PRIMARY KEY,
    position    INTEGER NOT NULL,
    kind        TEXT,
    added_at    TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.passes (
    ticker      TEXT PRIMARY KEY,
    position    INTEGER NOT NULL,
    disposition TEXT,
    scope       TEXT,
    cycle       DATE,
    sources     TEXT[],
    at          TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.trades (
    ticker      TEXT PRIMARY KEY,
    position    INTEGER NOT NULL,
    side        TEXT,
    cycle       DATE,
    added_at    TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.ticker_notes (
    ticker      TEXT PRIMARY KEY,
    position    INTEGER NOT NULL,
    notes       TEXT,
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.chart_notes (
    ticker      TEXT PRIMARY KEY,
    position    INTEGER NOT NULL,
    notes       TEXT[],
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.chart_levels (
    ticker      TEXT PRIMARY KEY,
    position    INTEGER NOT NULL,
    levels      DOUBLE PRECISION[],
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.general_notes (
    id          INTEGER PRIMARY KEY,
    position    INTEGER NOT NULL,
    note_date   DATE,
    title       TEXT,
    content     TEXT,
    tags        TEXT,
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.themes (
    position    INTEGER PRIMARY KEY,
    name        TEXT,
    description TEXT,
    tickers     TEXT[]
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.etf_groups (
    position    INTEGER PRIMARY KEY,
    category    TEXT,
    etfs        JSONB
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.screener_feedback (
    date        DATE NOT NULL,
    ticker      TEXT NOT NULL,
    record      JSONB NOT NULL,
    PRIMARY KEY (date, ticker)
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.alert_fires (
    key         TEXT PRIMARY KEY,
    position    INTEGER NOT NULL,
    date        DATE,
    price       DOUBLE PRECISION,
    fired_at    TIMESTAMPTZ
);

-- One row per (session date, kind, source). kind: daily | losers.
-- source: benzinga (historical, Massive) | perplexity.
CREATE TABLE IF NOT EXISTS {SCHEMA}.briefs (
    date        DATE NOT NULL,
    kind        TEXT NOT NULL,
    source      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'complete',
    brief_md    TEXT,
    brief_json  JSONB,
    run_costs   JSONB,
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, kind, source)
);

-- UI-facing outputs of other pipelines: files = {{filename: parsed JSON}}.
CREATE TABLE IF NOT EXISTS {SCHEMA}.pipeline_runs (
    pipeline    TEXT NOT NULL,
    run_key     TEXT NOT NULL,
    files       JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, run_key)
);
"""

_engine: Engine | None = None
_lock = threading.Lock()


def engine() -> Engine:
    """Process-wide engine; creates the ``app`` schema on first use."""
    global _engine
    if _engine is None:
        with _lock:
            if _engine is None:
                url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/stocks_db")
                eng = create_engine(url, pool_pre_ping=True)
                with eng.begin() as conn:
                    # Concurrent CREATE ... IF NOT EXISTS can still collide across processes.
                    conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('app_store.ddl'))"))
                    conn.execute(text(DDL))
                _engine = eng
    return _engine
