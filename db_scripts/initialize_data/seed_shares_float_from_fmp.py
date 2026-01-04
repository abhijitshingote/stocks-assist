"""
Seed shares_float table from FMP API.

Uses:
- /stable/shares-float?symbol=X - for float shares, outstanding shares, free float %

Reference: https://site.financialmodelingprep.com/developer/docs/stable/shares-float
"""

from datetime import datetime
import os
import sys
import time
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import argparse

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, SharesFloat, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'seed_shares_float_from_fmp'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


def fetch_shares_float(api_key, symbol):
    """Fetch shares float data from FMP API"""
    url = f'{BASE_URL}/shares-float'
    params = {'symbol': symbol, 'apikey': api_key}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.ok:
            data = response.json()
            if data and len(data) > 0:
                return data[0]
        return None
    except Exception as e:
        logger.debug(f"Error fetching shares float for {symbol}: {e}")
        return None


def parse_float(value):
    try:
        return float(value) if value is not None else None
    except:
        return None


def parse_int(value):
    try:
        return int(value) if value is not None else None
    except:
        return None


def parse_date(value):
    """Parse date string to date object"""
    if not value:
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except:
        return None


def upsert_shares_float(session, ticker, float_data):
    """Upsert shares float data"""
    if not float_data:
        return False
    
    data = {
        'ticker': ticker,
        'float_shares': parse_int(float_data.get('floatShares')),
        'outstanding_shares': parse_int(float_data.get('outstandingShares')),
        'free_float': parse_float(float_data.get('freeFloat')),
        'date': parse_date(float_data.get('date')),
        'updated_at': datetime.utcnow()
    }
    
    try:
        sql = insert(SharesFloat).values(**data)
        sql = sql.on_conflict_do_update(
            index_elements=['ticker'],
            set_={k: sql.excluded[k] for k in data.keys() if k != 'ticker'}
        )
        session.execute(sql)
        return True
    except Exception as e:
        logger.error(f"Error inserting shares float for {ticker}: {e}")
        return False


def update_sync_metadata(session, key):
    stmt = insert(SyncMetadata).values(key=key, last_synced_at=datetime.utcnow())
    stmt = stmt.on_conflict_do_update(index_elements=['key'], set_={'last_synced_at': datetime.utcnow()})
    session.execute(stmt)
    session.commit()


def get_tickers(session, limit=None):
    """Get list of tickers to process, ordered by market cap descending"""
    query = session.query(Ticker.ticker).filter(
        Ticker.is_actively_trading == True
    ).order_by(Ticker.market_cap.desc().nullslast())
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def process_shares_float(session, api_key, tickers, delay=0.15, commit_every=50):
    """Fetch and process shares float data"""
    
    float_count = 0
    failed = 0
    
    for i, ticker in enumerate(tickers):
        float_data = fetch_shares_float(api_key, ticker)
        if float_data and upsert_shares_float(session, ticker, float_data):
            float_count += 1
        else:
            failed += 1
        
        if (i + 1) % commit_every == 0:
            session.commit()
            logger.info(f"  Progress: {i+1}/{len(tickers)} (float records: {float_count})")
        
        time.sleep(delay)
    
    session.commit()
    return float_count, failed


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed shares_float from FMP')
    parser.add_argument('--limit', type=int, default=None, help='Max tickers to process')
    parser.add_argument('--delay', type=float, default=0.15, help='Delay between API calls (default: 0.15s)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Shares Float Seeding from FMP API")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    api_key = get_api_key()
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        tickers = get_tickers(session, limit=args.limit)
        
        if not tickers:
            logger.info("No tickers to process.")
            return
        
        logger.info(f"Tickers to process: {len(tickers)}")
        logger.info(f"API delay: {args.delay}s")
        
        estimated = len(tickers) * args.delay / 60
        logger.info(f"Estimated time: ~{estimated:.1f} minutes")
        logger.info("-" * 60)
        
        float_count, failed = process_shares_float(session, api_key, tickers, delay=args.delay)
        
        update_sync_metadata(session, 'shares_float_last_sync')
        
        logger.info("=" * 60)
        logger.info("Results:")
        logger.info(f"  Shares float records: {float_count}")
        logger.info(f"  Failed fetches: {failed}")
        
        total_float = session.query(SharesFloat).count()
        logger.info(f"  Total shares float records: {total_float}")
        
        elapsed = time.time() - start_time
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ Shares float seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{float_count} float records saved, {failed} failed', total_float, duration_seconds=elapsed)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted. Saving progress...")
        session.commit()
        update_sync_metadata(session, 'shares_float_last_sync')
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'INTERRUPTED', 'Progress saved', duration_seconds=elapsed)
    except Exception as e:
        logger.error(f"Error: {e}")
        session.rollback()
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=elapsed)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()

