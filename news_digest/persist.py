"""DB reads/writes for weekly news recap runs."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from models import NewsDigestItem, NewsDigestRun

logger = logging.getLogger(__name__)


def ensure_tables(engine) -> None:
    NewsDigestRun.__table__.create(engine, checkfirst=True)
    NewsDigestItem.__table__.create(engine, checkfirst=True)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def start_run(
    session,
    *,
    run_key: str,
    window_label: str,
    window_start: datetime,
    window_end: datetime,
) -> NewsDigestRun:
    """Create or reset a run row. Re-running a key replaces its items."""
    run = session.query(NewsDigestRun).filter(NewsDigestRun.run_key == run_key).one_or_none()
    if run is None:
        run = NewsDigestRun(run_key=run_key)
        session.add(run)
    else:
        session.query(NewsDigestItem).filter(NewsDigestItem.run_id == run.id).delete(
            synchronize_session=False
        )

    run.window_label = window_label
    run.window_start = window_start
    run.window_end = window_end
    run.status = "running"
    run.stage = "fetch"
    run.error = None
    run.narrative_md = None
    run.cost_usd = None
    run.stats = None
    run.universe_count = None
    run.prefiltered_count = None
    run.screened_count = None
    run.selected_count = None
    session.commit()
    return run


def set_stage(session, run: NewsDigestRun, stage: str, **fields) -> None:
    run.stage = stage
    for k, v in fields.items():
        setattr(run, k, v)
    session.commit()


def fail_run(session, run: NewsDigestRun, error: str) -> None:
    run.status = "failed"
    run.error = error[:4000]
    session.commit()


def finish_run(
    session,
    run: NewsDigestRun,
    *,
    narrative_md: str,
    cost_usd: float,
    stats: dict,
) -> None:
    run.status = "complete"
    run.stage = "done"
    run.narrative_md = narrative_md
    run.cost_usd = cost_usd
    run.stats = stats
    session.commit()


def write_items(session, run: NewsDigestRun, selected: list[dict]) -> int:
    """Persist the sources behind the recap, in selection order."""
    session.query(NewsDigestItem).filter(NewsDigestItem.run_id == run.id).delete(
        synchronize_session=False
    )

    rows = [
        NewsDigestItem(
            run_id=run.id,
            benzinga_id=item["benzinga_id"],
            rank=item["rank"],
            score=item["score"],
            why=item.get("why"),
            title=item["title"],
            tickers=item.get("tickers") or [],
            dup_ids=item.get("dup_ids") or [],
            url=item.get("url"),
            published=_parse_dt(item.get("published")),
        )
        for item in selected
    ]
    session.bulk_save_objects(rows)
    session.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Read side
# ---------------------------------------------------------------------------


def item_to_json(row: NewsDigestItem) -> dict:
    return {
        "id": row.id,
        "benzinga_id": row.benzinga_id,
        "rank": row.rank,
        "score": row.score,
        "why": row.why,
        "title": row.title,
        "tickers": row.tickers or [],
        "dup_count": len(row.dup_ids or []),
        "url": row.url,
        "published": row.published.isoformat() if row.published else None,
    }


def run_to_json(row: NewsDigestRun, *, include_narrative: bool = False) -> dict:
    out = {
        "id": row.id,
        "run_key": row.run_key,
        "window_label": row.window_label,
        "window_start": row.window_start.isoformat() if row.window_start else None,
        "window_end": row.window_end.isoformat() if row.window_end else None,
        "status": row.status,
        "stage": row.stage,
        "error": row.error,
        "universe_count": row.universe_count,
        "prefiltered_count": row.prefiltered_count,
        "screened_count": row.screened_count,
        "selected_count": row.selected_count,
        "cost_usd": round(row.cost_usd, 4) if row.cost_usd else None,
        "stats": row.stats,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }
    if include_narrative:
        out["narrative_md"] = row.narrative_md
    return out


def list_runs(session, *, limit: int = 50) -> list[NewsDigestRun]:
    return (
        session.query(NewsDigestRun)
        .order_by(NewsDigestRun.window_end.desc().nullslast(), NewsDigestRun.id.desc())
        .limit(limit)
        .all()
    )


def get_run(session, run_key: str) -> NewsDigestRun | None:
    return session.query(NewsDigestRun).filter(NewsDigestRun.run_key == run_key).one_or_none()


def latest_run(session) -> NewsDigestRun | None:
    return (
        session.query(NewsDigestRun)
        .filter(NewsDigestRun.status == "complete")
        .order_by(NewsDigestRun.window_end.desc().nullslast())
        .first()
    )


def items_for_run(session, run_id: int, *, limit: int = 200) -> list[NewsDigestItem]:
    return (
        session.query(NewsDigestItem)
        .filter(NewsDigestItem.run_id == run_id)
        .order_by(NewsDigestItem.rank.asc())
        .limit(limit)
        .all()
    )
