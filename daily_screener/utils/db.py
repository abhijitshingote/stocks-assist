"""Database session helpers, modelled after db_scripts conventions."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = PROJECT_ROOT / "backend"

# Make `from models import ...` work the same way db_scripts do.
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(PROJECT_ROOT / ".env")

_engine = None
_Session = None


def get_session():
    """Return a fresh SQLAlchemy session. Caller is responsible for .close()."""
    global _engine, _Session
    if _Session is None:
        database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:postgres@db:5432/stocks_db",
        )
        _engine = create_engine(database_url, pool_pre_ping=True)
        _Session = sessionmaker(bind=_engine)
    return _Session()
