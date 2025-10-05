from datetime import datetime
import os
import json
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add backend directory to Python path to import models
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import Stock, Earnings
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def get_symbols_from_database(session):
    """Get all ticker symbols from the database"""
    all_stocks = session.query(Stock.ticker).all()
    return {stock[0] for stock in all_stocks}

def restore_earnings(session, symbols_set, backup_dir='user_data'):
    """Restore earnings data from JSON backup"""
    backup_path = Path(backup_dir)
    earnings_file = backup_path / 'earnings_backup.json'
    restored_earnings = 0

    if earnings_file.exists():
        try:
            data = json.loads(earnings_file.read_text())
            for item in data:
                if item['ticker'] not in symbols_set:
                    continue  # skip if ticker not present in seeded data

                # Parse datetime and date fields
                def parse_ts(ts):
                    return datetime.fromisoformat(ts) if ts else None

                def parse_date(date_str):
                    return datetime.fromisoformat(date_str).date() if date_str else None

                earnings_obj = Earnings(
                    ticker=item['ticker'],
                    earnings_date=parse_date(item.get('earnings_date')),
                    announcement_type=item.get('announcement_type', 'earnings'),
                    is_confirmed=item.get('is_confirmed', False),
                    quarter=item.get('quarter'),
                    fiscal_year=item.get('fiscal_year'),
                    announcement_time=item.get('announcement_time'),
                    estimated_eps=item.get('estimated_eps'),
                    actual_eps=item.get('actual_eps'),
                    revenue_estimate=item.get('revenue_estimate'),
                    actual_revenue=item.get('actual_revenue'),
                    source=item.get('source'),
                    notes=item.get('notes'),
                    created_at=parse_ts(item.get('created_at')),
                    updated_at=parse_ts(item.get('updated_at'))
                )

                # Use merge for upsert-like behavior to avoid duplicates
                session.merge(earnings_obj)
                restored_earnings += 1

            session.commit()
            logger.info(f"Restored {restored_earnings} earnings records from backup")
        except Exception as e:
            session.rollback()
            logger.error(f"Error restoring earnings backup: {e}")
            raise
    else:
        logger.warning(f"Earnings backup file not found: {earnings_file}")

    return restored_earnings

def main():
    """Main function to restore earnings data from backup"""
    import argparse

    parser = argparse.ArgumentParser(description='Restore earnings data from backup files')
    parser.add_argument('--backup-dir', default='user_data', help='Directory containing backup files')
    args = parser.parse_args()

    print("Starting restoration of earnings data from backup...")

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

        # Restore earnings data from backup
        restored_earnings = restore_earnings(session, symbols_set, args.backup_dir)

        # Final statistics
        total_earnings = session.query(Earnings).count()

        logger.info("✅ Earnings restoration completed successfully!")
        logger.info(f"📊 Final count: {total_earnings} earnings records")
        logger.info(f"📊 Restored in this run: {restored_earnings} earnings records")

    except Exception as e:
        logger.error(f"Error in earnings restoration process: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == '__main__':
    main()
