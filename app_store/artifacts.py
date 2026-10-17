"""Pipeline outputs the UI shows: market briefs + per-run files of other pipelines.

Run working dirs (status.json, logs, intermediates) stay on disk as scratch;
a pipeline publishes here when it completes.
"""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from app_store.db import SCHEMA, engine

BRIEF_KINDS = ("daily", "losers")
BRIEF_SOURCES = ("perplexity", "benzinga")  # display / default order


def upsert_brief(
    date: str,
    kind: str,
    source: str,
    *,
    brief_md: str | None,
    brief_json: Any = None,
    run_costs: Any = None,
    metadata: Any = None,
    status: str = "complete",
) -> None:
    if kind not in BRIEF_KINDS or source not in BRIEF_SOURCES:
        raise ValueError(f"bad brief kind/source: {kind}/{source}")
    sql = f"""
        INSERT INTO {SCHEMA}.briefs (date, kind, source, status, brief_md, brief_json, run_costs, metadata)
        VALUES (:date, :kind, :source, :status, :brief_md,
                CAST(:brief_json AS jsonb), CAST(:run_costs AS jsonb), CAST(:metadata AS jsonb))
        ON CONFLICT (date, kind, source) DO UPDATE SET
            status = EXCLUDED.status, brief_md = EXCLUDED.brief_md,
            brief_json = EXCLUDED.brief_json, run_costs = EXCLUDED.run_costs,
            metadata = EXCLUDED.metadata, updated_at = now()
    """
    dump = lambda v: None if v is None else json.dumps(v)  # noqa: E731
    with engine().begin() as conn:
        conn.execute(text(sql), {
            "date": date, "kind": kind, "source": source, "status": status,
            "brief_md": brief_md, "brief_json": dump(brief_json),
            "run_costs": dump(run_costs), "metadata": dump(metadata),
        })


def _brief_row(r: Any) -> dict[str, Any]:
    d = dict(r._mapping)
    d["date"] = d["date"].isoformat()
    for k in ("created_at", "updated_at"):
        if d.get(k) is not None:
            d[k] = d[k].isoformat()
    return d


def list_briefs(kind: str | None = None) -> list[dict[str, Any]]:
    """Index rows (no bodies), newest date first."""
    where = "WHERE kind = :kind" if kind else ""
    sql = f"""
        SELECT date, kind, source, status, created_at, updated_at
        FROM {SCHEMA}.briefs {where} ORDER BY date DESC, kind, source
    """
    with engine().connect() as conn:
        return [_brief_row(r) for r in conn.execute(text(sql), {"kind": kind})]


def brief_bodies(kind: str) -> list[dict[str, Any]]:
    """[{date, source, brief_md}] for every brief of ``kind`` with markdown, oldest first."""
    sql = f"""
        SELECT date, source, brief_md FROM {SCHEMA}.briefs
        WHERE kind = :kind AND brief_md IS NOT NULL ORDER BY date, source
    """
    with engine().connect() as conn:
        return [
            {"date": d.isoformat(), "source": s, "brief_md": md}
            for d, s, md in conn.execute(text(sql), {"kind": kind})
        ]


def get_briefs(date: str, kind: str) -> dict[str, dict[str, Any]]:
    """All sources for (date, kind): {source: row}, in BRIEF_SOURCES order."""
    sql = f"SELECT * FROM {SCHEMA}.briefs WHERE date = :date AND kind = :kind"
    with engine().connect() as conn:
        rows = {r.source: _brief_row(r) for r in conn.execute(text(sql), {"date": date, "kind": kind})}
    return {s: rows[s] for s in BRIEF_SOURCES if s in rows}


def put_run(pipeline: str, run_key: str, files: dict[str, Any]) -> None:
    """Upsert a run; ``files`` are merged into any already published for it."""
    sql = f"""
        INSERT INTO {SCHEMA}.pipeline_runs (pipeline, run_key, files)
        VALUES (:pipeline, :run_key, CAST(:files AS jsonb))
        ON CONFLICT (pipeline, run_key) DO UPDATE SET
            files = {SCHEMA}.pipeline_runs.files || EXCLUDED.files, updated_at = now()
    """
    with engine().begin() as conn:
        conn.execute(text(sql), {"pipeline": pipeline, "run_key": run_key, "files": json.dumps(files)})


def get_run(pipeline: str, run_key: str) -> dict[str, Any] | None:
    sql = f"SELECT files FROM {SCHEMA}.pipeline_runs WHERE pipeline = :p AND run_key = :k"
    with engine().connect() as conn:
        row = conn.execute(text(sql), {"p": pipeline, "k": run_key}).first()
    return row[0] if row else None


def list_runs(pipeline: str) -> list[dict[str, Any]]:
    """[{run_key, file_names, updated_at}], newest first (keys sort by date)."""
    sql = f"""
        SELECT run_key, ARRAY(SELECT jsonb_object_keys(files)) AS file_names, updated_at
        FROM {SCHEMA}.pipeline_runs WHERE pipeline = :p ORDER BY run_key DESC
    """
    with engine().connect() as conn:
        return [
            {"run_key": k, "file_names": list(names), "updated_at": ts.isoformat()}
            for k, names, ts in conn.execute(text(sql), {"p": pipeline})
        ]
