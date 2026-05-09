"""Read-only DB access for the screening agent.

We reuse the SQLAlchemy models from `backend/models.py` rather than
redefining them. This module only exposes a session factory and a few
convenience queries — agents are expected to compose them.
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# Make `from models import ...` work regardless of CWD.
_BACKEND_DIR = Path(__file__).resolve().parents[2] / "backend"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from models import (  # noqa: E402  (import after sys.path mutation)
    Ticker, OHLC, MainView, StockMetrics, StockVolspikeGapper,
    HistoricalRSI, RsScreener, CompanyProfile,
)

from .. import config as cfg

_engine = None
_SessionFactory: sessionmaker | None = None


def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(cfg.DB["url"], pool_pre_ping=True)
    return _engine


def get_session_factory() -> sessionmaker:
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=_get_engine())
    return _SessionFactory


@contextmanager
def session_scope() -> Session:  # type: ignore[type-arg]
    """`with session_scope() as s:` — auto-closes."""
    s = get_session_factory()()
    try:
        yield s
    finally:
        s.close()


# Re-export the models everyone wants
__all__ = [
    "session_scope", "get_session_factory",
    "Ticker", "OHLC", "MainView", "StockMetrics",
    "StockVolspikeGapper", "HistoricalRSI", "RsScreener", "CompanyProfile",
]
