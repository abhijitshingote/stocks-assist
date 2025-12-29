"""
Seed stock_preferences table from the JSON backup file.

This script restores user preferences (favorites/dislikes) that were exported to user_data/stock_preferences.json.
Run this after a database reset to restore your preferences.

Usage:
    python seed_stock_preferences.py
"""

import os
import sys
import json
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from db_scripts.logger import get_logger, write_summary, flush_logger

# Add backend to path for models
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import StockPreference, Ticker

load_dotenv()

SCRIPT_NAME = 'seed_stock_preferences'
logger = get_logger(SCRIPT_NAME)

# Path to the stock preferences backup file
STOCK_PREFERENCES_FILE = os.path.join(os.path.dirname(__file__), '../../user_data/stock_preferences.json')


def seed_stock_preferences():
    """
    Restore stock preferences from the JSON backup file.
    Only inserts preferences for tickers that exist in the database.
    """
    start_time = time.time()

    # Check if backup file exists
    if not os.path.exists(STOCK_PREFERENCES_FILE):
        logger.info(f"No stock preferences backup file found at {STOCK_PREFERENCES_FILE}")
        logger.info("Skipping stock preferences restoration.")
        write_summary(SCRIPT_NAME, 'SKIPPED', 'No backup file found', duration_seconds=time.time() - start_time)
        flush_logger(SCRIPT_NAME)
        return

    # Load preferences from file
    try:
        with open(STOCK_PREFERENCES_FILE, 'r') as f:
            preferences_data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing stock preferences file: {e}")
        write_summary(SCRIPT_NAME, 'ERROR', f'JSON parse error: {e}', duration_seconds=time.time() - start_time)
        flush_logger(SCRIPT_NAME)
        return

    if not preferences_data:
        logger.info("Stock preferences file is empty, nothing to restore.")
        write_summary(SCRIPT_NAME, 'SKIPPED', 'Backup file is empty', duration_seconds=time.time() - start_time)
        flush_logger(SCRIPT_NAME)
        return

    logger.info(f"Found {len(preferences_data)} stock preferences in backup file")

    # Connect to database
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get list of valid tickers in database
        valid_tickers = set(t[0] for t in session.query(Ticker.ticker).all())
        logger.info(f"Found {len(valid_tickers)} valid tickers in database")

        inserted = 0
        skipped = 0

        for ticker, pref_info in preferences_data.items():
            ticker = ticker.upper()

            # Skip if ticker doesn't exist in database
            if ticker not in valid_tickers:
                logger.warning(f"Skipping {ticker} - not found in tickers table")
                skipped += 1
                continue

            # Check if preference already exists
            existing = session.query(StockPreference).filter(StockPreference.ticker == ticker).first()

            if existing:
                # Update existing preference
                existing.preference = pref_info.get('preference')
                logger.info(f"Updated preference for {ticker}: {pref_info.get('preference')}")
            else:
                # Insert new preference
                new_pref = StockPreference(
                    ticker=ticker,
                    preference=pref_info.get('preference')
                )
                session.add(new_pref)
                logger.info(f"Inserted preference for {ticker}: {pref_info.get('preference')}")

            inserted += 1

        session.commit()

        elapsed = time.time() - start_time
        logger.info(f"Stock preferences restoration complete!")
        logger.info(f"  - Restored: {inserted}")
        logger.info(f"  - Skipped (ticker not found): {skipped}")
        logger.info(f"  - Duration: {elapsed:.2f}s")

        write_summary(SCRIPT_NAME, 'SUCCESS', f'Restored {inserted} preferences, skipped {skipped}', duration_seconds=elapsed)

    except Exception as e:
        session.rollback()
        logger.error(f"Error restoring stock preferences: {e}")
        write_summary(SCRIPT_NAME, 'ERROR', str(e), duration_seconds=time.time() - start_time)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    seed_stock_preferences()
