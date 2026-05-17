"""
Seed abi_general_notes table from the JSON backup file.

This script restores user abi general notes that were exported to
user_data/abi_general_notes.json.
Run this after a database reset to restore your notes.

Usage:
    python seed_abi_general_notes.py
"""

import os
import sys
import json
import time
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from db_scripts.logger import get_logger, write_summary, flush_logger

# Add backend to path for models
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import AbiGeneralNotes

load_dotenv()

SCRIPT_NAME = 'seed_abi_general_notes'
logger = get_logger(SCRIPT_NAME)

ABI_GENERAL_NOTES_FILE = os.path.join(
    os.path.dirname(__file__), '../../user_data/abi_general_notes.json'
)


def seed_abi_general_notes():
    """
    Restore abi general notes from the JSON backup file.
    """
    start_time = time.time()

    if not os.path.exists(ABI_GENERAL_NOTES_FILE):
        logger.info(f"No abi general notes backup at {ABI_GENERAL_NOTES_FILE}")
        logger.info("Skipping abi general notes restoration.")
        write_summary(SCRIPT_NAME, 'SKIPPED', 'No backup file found', duration_seconds=time.time() - start_time)
        flush_logger(SCRIPT_NAME)
        return

    try:
        with open(ABI_GENERAL_NOTES_FILE, 'r') as f:
            notes_data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing abi general notes file: {e}")
        write_summary(SCRIPT_NAME, 'ERROR', f'JSON parse error: {e}', duration_seconds=time.time() - start_time)
        flush_logger(SCRIPT_NAME)
        return

    if not notes_data:
        logger.info("Abi general notes file is empty, nothing to restore.")
        write_summary(SCRIPT_NAME, 'SKIPPED', 'Backup file is empty', duration_seconds=time.time() - start_time)
        flush_logger(SCRIPT_NAME)
        return

    logger.info(f"Found {len(notes_data)} abi general notes in abi_general_notes.json")

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        inserted = 0

        for note_info in notes_data:
            note_date_str = note_info.get('note_date')
            if note_date_str:
                note_date = datetime.strptime(note_date_str, '%Y-%m-%d').date()
            else:
                continue

            existing = session.query(AbiGeneralNotes).filter(
                AbiGeneralNotes.note_date == note_date,
                AbiGeneralNotes.title == note_info.get('title')
            ).first()

            if existing:
                existing.content = note_info.get('content')
                existing.tags = note_info.get('tags')
                logger.info(f"Updated note for {note_date_str}: {note_info.get('title', 'Untitled')}")
            else:
                new_note = AbiGeneralNotes(
                    note_date=note_date,
                    title=note_info.get('title'),
                    content=note_info.get('content'),
                    tags=note_info.get('tags')
                )
                session.add(new_note)
                logger.info(f"Inserted note for {note_date_str}: {note_info.get('title', 'Untitled')}")

            inserted += 1

        session.commit()

        elapsed = time.time() - start_time
        logger.info("Abi general notes restoration complete!")
        logger.info(f"  - Restored: {inserted}")
        logger.info(f"  - Duration: {elapsed:.2f}s")

        write_summary(SCRIPT_NAME, 'SUCCESS', f'Restored {inserted} notes', duration_seconds=elapsed)

    except Exception as e:
        session.rollback()
        logger.error(f"Error restoring abi general notes: {e}")
        write_summary(SCRIPT_NAME, 'ERROR', str(e), duration_seconds=time.time() - start_time)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    seed_abi_general_notes()
