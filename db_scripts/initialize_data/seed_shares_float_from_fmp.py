"""
Seed shares_float table from FMP API.

Uses:
- /stable/shares-float?symbol=X - for float shares, outstanding shares, free float %

OPTIMIZED:
- Concurrent API calls with rate limiting (290/min)
- Bulk inserts instead of row-by-row

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
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, SharesFloat, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration, ProgressTracker, get_test_ticker_limit
from db_scripts.fmp_utils import RateLimiter, bulk_upsert_simple

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
    except:
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
    if not value:
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except:
        return None


def process_float_data(ticker, float_data):
    """Convert API response to database record"""
    if not float_data:
        return None
    
    return {
        'ticker': ticker,
        'float_shares': parse_int(float_data.get('floatShares')),
        'outstanding_shares': parse_int(float_data.get('outstandingShares')),
        'free_float': parse_float(float_data.get('freeFloat')),
        'date': parse_date(float_data.get('date')),
        'updated_at': datetime.utcnow()
    }


def update_sync_metadata(session, key):
    stmt = insert(SyncMetadata).values(key=key, last_synced_at=datetime.utcnow())
    stmt = stmt.on_conflict_do_update(index_elements=['key'], set_={'last_synced_at': datetime.utcnow()})
    session.execute(stmt)
    session.commit()


def get_tickers(session, limit=None):
    test_limit = get_test_ticker_limit()
    if test_limit:
        logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
        limit = test_limit
    
    query = session.query(Ticker.ticker).filter(
        Ticker.is_actively_trading == True
    ).order_by(Ticker.market_cap.desc().nullslast())
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed shares_float from FMP')
    parser.add_argument('--limit', type=int, default=None, help='Max tickers to process')
    parser.add_argument('--workers', type=int, default=10, help='Concurrent API workers (default: 10)')
    parser.add_argument('--batch-size', type=int, default=100, help='DB write batch size (default: 100)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Shares Float Seeding from FMP API (Optimized)")
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
        logger.info(f"Workers: {args.workers}")
        logger.info(f"Rate limit: 290 calls/min")
        
        estimated_minutes = len(tickers) / 290
        logger.info(f"Estimated time: ~{estimated_minutes:.1f} minutes")
        logger.info("-" * 60)
        
        rate_limiter = RateLimiter(calls_per_minute=290)
        progress = ProgressTracker(len(tickers), logger, update_interval=50, prefix="Float:")
        
        all_records = []
        success_count = 0
        failed_count = 0
        processed = 0
        total_saved = 0
        db_batch_num = 0
        
        logger.info("📡 Phase 1: Fetching shares float data from API...")
        
        def worker(ticker):
            rate_limiter.acquire()
            float_data = fetch_shares_float(api_key, ticker)
            if float_data:
                record = process_float_data(ticker, float_data)
                return (ticker, record, True)
            return (ticker, None, False)
        
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(worker, t): t for t in tickers}
            
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    _, record, success = future.result()
                    if success and record:
                        all_records.append(record)
                        success_count += 1
                    elif not success:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.debug(f"Error processing {ticker}: {e}")
                
                processed += 1
                
                # Batch write to DB
                if len(all_records) >= args.batch_size:
                    db_batch_num += 1
                    records_to_write = len(all_records)
                    bulk_upsert_simple(
                        engine, 'shares_float', all_records,
                        primary_key='ticker',
                        update_columns=['float_shares', 'outstanding_shares', 
                                       'free_float', 'date', 'updated_at']
                    )
                    total_saved += records_to_write
                    progress.log_db_write(records_to_write, db_batch_num)
                    all_records = []
                
                progress.update(processed, f"| Fetched: {success_count}, Saved: {total_saved}")

        # Write remaining
        if all_records:
            db_batch_num += 1
            records_to_write = len(all_records)
            logger.info("💾 Phase 2: Writing final batch to database...")
            bulk_upsert_simple(
                engine, 'shares_float', all_records,
                primary_key='ticker',
                update_columns=['float_shares', 'outstanding_shares', 
                               'free_float', 'date', 'updated_at']
            )
            total_saved += records_to_write
            progress.log_db_write(records_to_write, db_batch_num)

        progress.finish(f"- Fetched: {success_count}, Failed: {failed_count}, Total saved: {total_saved}")
        
        update_sync_metadata(session, 'shares_float_last_sync')
        
        total_float = session.query(SharesFloat).count()
        elapsed = time.time() - start_time
        
        logger.info("=" * 60)
        logger.info(f"  Shares float records: {success_count}")
        logger.info(f"  Failed fetches: {failed_count}")
        logger.info(f"  Total shares float records: {total_float}")
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        if elapsed > 0:
            logger.info(f"  Effective rate: {processed / (elapsed / 60):.1f} calls/min")
        logger.info("=" * 60)
        logger.info("✅ Shares float seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{success_count} float records saved, {failed_count} failed', total_float, duration_seconds=elapsed)

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
