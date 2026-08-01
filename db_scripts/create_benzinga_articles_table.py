"""
Create benzinga_articles table if it does not exist.

Usage:
  python db_scripts/create_benzinga_articles_table.py
"""

import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../backend"))
from models import Base, BenzingaArticle  # noqa: F401

load_dotenv()


def main() -> None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not set")
    engine = create_engine(database_url)
    BenzingaArticle.__table__.create(engine, checkfirst=True)
    print("benzinga_articles table ready.")


if __name__ == "__main__":
    main()
