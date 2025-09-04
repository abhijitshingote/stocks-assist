from datetime import datetime
import os
import json
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add backend directory to Python path to import models
sys.path.append('/app/backend')
from models import Stock, Comment, Flags, ConciseNote
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def get_symbols_from_database(session):
    """Get all ticker symbols from the database"""
    all_stocks = session.query(Stock.ticker).all()
    return {stock[0] for stock in all_stocks}

def restore_comments(session, symbols_set, backup_dir='user_data'):
    """Restore comments from JSON backup"""
    backup_path = Path(backup_dir)
    comments_file = backup_path / 'comments_backup.json'
    restored_comments = 0

    if comments_file.exists():
        try:
            data = json.loads(comments_file.read_text())
            for item in data:
                if item['ticker'] not in symbols_set:
                    continue  # skip if ticker not present in seeded data

                # Prepare datetime parsing helper
                def parse_ts(ts):
                    return datetime.fromisoformat(ts) if ts else None

                comment_obj = Comment(
                    ticker=item['ticker'],
                    comment_text=item['comment_text'],
                    comment_type=item.get('comment_type', 'user'),
                    status=item.get('status', 'active'),
                    ai_source=item.get('ai_source'),
                    reviewed_by=item.get('reviewed_by'),
                    reviewed_at=parse_ts(item.get('reviewed_at')),
                    created_at=parse_ts(item.get('created_at')),
                    updated_at=parse_ts(item.get('updated_at'))
                )

                # Use merge for upsert-like behaviour to avoid duplicates
                session.add(comment_obj)
                restored_comments += 1

            session.commit()
            logger.info(f"Restored {restored_comments} comments from backup")
        except Exception as e:
            session.rollback()
            logger.error(f"Error restoring comments backup: {e}")
            raise
    else:
        logger.warning(f"Comments backup file not found: {comments_file}")

    return restored_comments

def restore_flags(session, symbols_set, backup_dir='user_data'):
    """Restore flags from JSON backup"""
    backup_path = Path(backup_dir)
    flags_file = backup_path / 'flags_backup.json'
    restored_flags = 0

    if flags_file.exists():
        try:
            data = json.loads(flags_file.read_text())
            for item in data:
                if item['ticker'] in symbols_set:
                    session.merge(Flags(
                        ticker=item['ticker'],
                        is_reviewed=item.get('is_reviewed', False),
                        is_shortlisted=item.get('is_shortlisted', False),
                        is_blacklisted=item.get('is_blacklisted', False)
                    ))
                    restored_flags += 1

            session.commit()
            logger.info(f"Restored {restored_flags} flag rows from backup")
        except Exception as e:
            session.rollback()
            logger.error(f"Error restoring flags backup: {e}")
            raise
    else:
        logger.warning(f"Flags backup file not found: {flags_file}")

    return restored_flags

def restore_concise_notes(session, symbols_set, backup_dir='user_data'):
    """Restore concise notes from JSON backup"""
    backup_path = Path(backup_dir)
    concise_notes_file = backup_path / 'concise_notes_backup.json'
    restored_notes = 0

    if concise_notes_file.exists():
        try:
            data = json.loads(concise_notes_file.read_text())
            for item in data:
                if item['ticker'] in symbols_set:
                    session.merge(ConciseNote(
                        ticker=item['ticker'],
                        note=item.get('note', '')
                    ))
                    restored_notes += 1

            session.commit()
            logger.info(f"Restored {restored_notes} concise notes from backup")
        except Exception as e:
            session.rollback()
            logger.error(f"Error restoring concise notes backup: {e}")
            raise
    else:
        logger.warning(f"Concise notes backup file not found: {concise_notes_file}")

    return restored_notes

def main():
    """Main function to restore comments, flags, and concise notes from backup"""
    import argparse

    parser = argparse.ArgumentParser(description='Restore comments, flags, and concise notes from backup files')
    parser.add_argument('--backup-dir', default='user_data', help='Directory containing backup files')
    args = parser.parse_args()

    print("Starting restoration of comments, flags, and concise notes from backup...")

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get symbols from database
        symbols_set = get_symbols_from_database(session)
        if not symbols_set:
            logger.warning("No symbols found in database. Please run stock seeding first.")
            return

        logger.info(f"Found {len(symbols_set)} symbols in database")

        # Restore data from backups
        restored_comments = restore_comments(session, symbols_set, args.backup_dir)
        restored_flags = restore_flags(session, symbols_set, args.backup_dir)
        restored_notes = restore_concise_notes(session, symbols_set, args.backup_dir)

        # Final statistics
        total_comments = session.query(Comment).count()
        total_flags = session.query(Flags).count()
        total_notes = session.query(ConciseNote).count()

        logger.info("✅ Restoration completed successfully!")
        logger.info(f"📊 Final counts: {total_comments} comments, {total_flags} flags, {total_notes} concise notes")
        logger.info(f"📊 Restored in this run: {restored_comments} comments, {restored_flags} flags, {restored_notes} concise notes")

    except Exception as e:
        logger.error(f"Error in restoration process: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == '__main__':
    main()
