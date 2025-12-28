"""
Seed stock_notes table from the JSON backup file.

This script restores user notes that were exported to user_data/stock_notes.json.
Run this after a database reset to restore your notes.

Usage:
    python seed_stock_notes.py
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
from models import StockNotes, Ticker

load_dotenv()

SCRIPT_NAME = 'seed_stock_notes'
logger = get_logger(SCRIPT_NAME)

# Path to the stock notes backup file
STOCK_NOTES_FILE = os.path.join(os.path.dirname(__file__), '../../user_data/stock_notes.json')


def seed_stock_notes():
    """
    Restore stock notes from the JSON backup file.
    Only inserts notes for tickers that exist in the database.
    """
    start_time = time.time()
    
    # Check if backup file exists
    if not os.path.exists(STOCK_NOTES_FILE):
        logger.info(f"No stock notes backup file found at {STOCK_NOTES_FILE}")
        logger.info("Skipping stock notes restoration.")
        write_summary(SCRIPT_NAME, 'SKIPPED', 'No backup file found', duration_seconds=time.time() - start_time)
        flush_logger(SCRIPT_NAME)
        return
    
    # Load notes from file
    try:
        with open(STOCK_NOTES_FILE, 'r') as f:
            notes_data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing stock notes file: {e}")
        write_summary(SCRIPT_NAME, 'ERROR', f'JSON parse error: {e}', duration_seconds=time.time() - start_time)
        flush_logger(SCRIPT_NAME)
        return
    
    if not notes_data:
        logger.info("Stock notes file is empty, nothing to restore.")
        write_summary(SCRIPT_NAME, 'SKIPPED', 'Backup file is empty', duration_seconds=time.time() - start_time)
        flush_logger(SCRIPT_NAME)
        return
    
    logger.info(f"Found {len(notes_data)} stock notes in backup file")
    
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
        
        for ticker, note_info in notes_data.items():
            ticker = ticker.upper()
            
            # Skip if ticker doesn't exist in database
            if ticker not in valid_tickers:
                logger.warning(f"Skipping {ticker} - not found in tickers table")
                skipped += 1
                continue
            
            # Check if note already exists
            existing = session.query(StockNotes).filter(StockNotes.ticker == ticker).first()
            
            if existing:
                # Update existing note
                existing.notes = note_info.get('notes')
                logger.info(f"Updated notes for {ticker}")
            else:
                # Insert new note
                new_note = StockNotes(
                    ticker=ticker,
                    notes=note_info.get('notes')
                )
                session.add(new_note)
                logger.info(f"Inserted notes for {ticker}")
            
            inserted += 1
        
        session.commit()
        
        elapsed = time.time() - start_time
        logger.info(f"Stock notes restoration complete!")
        logger.info(f"  - Restored: {inserted}")
        logger.info(f"  - Skipped (ticker not found): {skipped}")
        logger.info(f"  - Duration: {elapsed:.2f}s")
        
        write_summary(SCRIPT_NAME, 'SUCCESS', f'Restored {inserted} notes, skipped {skipped}', duration_seconds=elapsed)
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error restoring stock notes: {e}")
        write_summary(SCRIPT_NAME, 'ERROR', str(e), duration_seconds=time.time() - start_time)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    seed_stock_notes()

