"""Run status file for UI polling (``status.json`` in each dated output dir)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def write_status(
    outdir: Path,
    status: str,
    *,
    stage: str | None = None,
    error: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "status": status,
        "stage": stage,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        payload["error"] = error
    if extra:
        payload.update(extra)
    (outdir / "status.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def read_status(outdir: Path) -> dict[str, Any] | None:
    path = outdir / "status.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
