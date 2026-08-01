"""IO helpers: artifact paths, JSON read/write, prompt loading."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pytz

from daily_screener import config

logger = logging.getLogger(__name__)


def today_str() -> str:
    """Today's date in Eastern time as YYYY-MM-DD (matches market schedule)."""
    eastern = pytz.timezone("US/Eastern")
    return datetime.now(eastern).strftime("%Y-%m-%d")


def run_dir(date_str: str) -> Path:
    """Path to outputs/<date_str>/, created if needed."""
    d = config.OUTPUTS_DIR / date_str
    d.mkdir(parents=True, exist_ok=True)
    return d


def run_dir_path(date_str: str) -> Path:
    """Path to outputs/<date_str>/ WITHOUT creating it. Useful for cache lookups
    that should not leave empty folders behind."""
    return config.OUTPUTS_DIR / date_str


def news_subdir(date_str: str) -> Path:
    d = run_dir(date_str) / "03_news"
    d.mkdir(parents=True, exist_ok=True)
    return d


def artifact_path(date_str: str, filename: str) -> Path:
    """Path WITHOUT creating the date folder. Use `write_json` (which creates
    parents on write) for writes, and `read_json_if_exists` for reads."""
    return run_dir_path(date_str) / filename


def write_json(path: Path, data: Any) -> None:
    """Write JSON with consistent formatting. Creates parent dirs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(data, f, indent=2, default=_json_default)
    logger.info("wrote %s", path)


def read_json(path: Path) -> Any:
    with path.open("r") as f:
        return json.load(f)


def read_json_if_exists(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return read_json(path)


def load_prompt(name: str) -> str:
    """Read a prompt template by basename (without .md)."""
    p = config.PROMPTS_DIR / f"{name}.md"
    with p.open("r") as f:
        return f.read()


def _json_default(obj):
    """Handle datetime, date, and other non-JSON-serializable types."""
    from datetime import date, datetime as _dt

    if isinstance(obj, (_dt, date)):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)
