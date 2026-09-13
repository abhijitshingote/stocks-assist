"""Curated market-news digest (v2). Independent of market_brief.

Puts ``backend/`` on ``sys.path`` so ``from models import ...`` resolves the
same way it does for ``db_scripts`` and ``daily_screener``.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_BACKEND_DIR = _PROJECT_ROOT / "backend"

for _p in (str(_PROJECT_ROOT), str(_BACKEND_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
