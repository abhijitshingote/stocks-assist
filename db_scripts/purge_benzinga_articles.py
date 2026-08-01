"""
Delete Benzinga articles older than N days (by published timestamp).

Usage:
  python db_scripts/purge_benzinga_articles.py
  python db_scripts/purge_benzinga_articles.py --days 7
"""

from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../backend"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import benzinga_news as bz  # noqa: E402
from daily_screener.utils.db import get_session  # noqa: E402

load_dotenv()


def main() -> None:
    p = argparse.ArgumentParser(description="Purge old Benzinga articles")
    p.add_argument("--days", type=int, default=7, help="Retention window (default: 7)")
    args = p.parse_args()

    session = get_session()
    try:
        deleted = bz.purge_articles_older_than(session, days=args.days)
        print(f"Deleted {deleted} rows with published older than {args.days} days.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
