"""
Seed earnings table from FMP API.
Source: /stable/earnings?symbol=X

OPTIMIZED:
- Concurrent API calls with rate limiting (290/min)
- Bulk inserts instead of row-by-row
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
from models import Ticker, Earnings, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration, ProgressTracker, get_test_ticker_limit
from db_scripts.fmp_utils import RateLimiter, bulk_upsert

# Script name for logging
SCRIPT_NAME = 'seed_earnings_from_fmp'
logger = get_logger(SCRIPT_NAME)
load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found")
    return api_key


def fetch_earnings(api_key, symbol):
    url = f'{BASE_URL}/earnings'
    params = {'symbol': symbol, 'apikey': api_key}
    try:
        r = requests.get(url, params=params, timeout=30)
        return r.json() if r.ok else []
    except:
        return []


def parse_float(v):
    try: return float(v) if v is not None else None
    except: return None

def parse_int(v):
    try: return int(float(v)) if v is not None else None
    except: return None

def parse_date(s):
    try: return datetime.strptime(s, '%Y-%m-%d').date() if s else None
    except: return None


def process_earnings_data(ticker, earnings_list):
    """Convert API response to database records.
    
    Note: FMP can return multiple earnings for the same date (e.g., Q4 and Annual 
    announced on same day). Since our constraint is (ticker, date), we keep only 
    the first record per date (API returns latest first).
    """
    # Use dict to deduplicate by date (keep first occurrence - API sorted latest first)
    records_by_date = {}
    for e in earnings_list:
        date = parse_date(e.get('date'))
        if not date:
            continue
        
        # Only add if we haven't seen this date yet (keeps first/latest)
        if date not in records_by_date:
            records_by_date[date] = {
                'ticker': ticker,
                'date': date,
                'eps_actual': parse_float(e.get('epsActual')),
                'eps_estimated': parse_float(e.get('epsEstimated')),
                'revenue_actual': parse_int(e.get('revenueActual')),
                'revenue_estimated': parse_int(e.get('revenueEstimated'))
            }
    return list(records_by_date.values())


def get_tickers(session, limit=None):
    test_limit = get_test_ticker_limit()
    if test_limit:
        logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
        limit = test_limit
    
    q = session.query(Ticker.ticker).filter(Ticker.is_actively_trading == True)
    q = q.order_by(Ticker.market_cap.desc().nullslast())
    if limit: q = q.limit(limit)
    return [t[0] for t in q.all()]


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description='Seed earnings from FMP')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--workers', type=int, default=10, help='Concurrent API workers (default: 10)')
    parser.add_argument('--batch-size', type=int, default=100, help='DB write batch size (default: 100)')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Earnings Seeding (Optimized)")
    logger.info("=" * 60)

    engine = create_engine(os.getenv('DATABASE_URL'))
    Session = sessionmaker(bind=engine)
    session = Session()
    api_key = get_api_key()

    try:
        tickers = get_tickers(session, args.limit)
        logger.info(f"Tickers: {len(tickers)}")
        logger.info(f"Workers: {args.workers}")
        logger.info(f"Rate limit: 290 calls/min")

        estimated_minutes = len(tickers) / 290
        logger.info(f"Estimated time: ~{estimated_minutes:.1f} minutes")
        logger.info("-" * 60)

        rate_limiter = RateLimiter(calls_per_minute=290)
        progress = ProgressTracker(len(tickers), logger, update_interval=50, prefix="Earnings:")

        all_records = []
        success_count = 0
        failed_count = 0
        processed = 0
        total_records = 0
        db_batch_num = 0
        
        logger.info("📡 Phase 1: Fetching earnings data from API...")
        
        def worker(ticker):
            rate_limiter.acquire()
            data = fetch_earnings(api_key, ticker)
            if data:
                records = process_earnings_data(ticker, data)
                return (ticker, records, True)
            return (ticker, [], False)
        
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(worker, t): t for t in tickers}
            
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    _, records, success = future.result()
                    if success and records:
                        all_records.extend(records)
                        success_count += 1
                    elif not success:
                        failed_count += 1
                    else:
                        success_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.debug(f"Error processing {ticker}: {e}")
                
                processed += 1
                
                # Batch write to DB
                if len(all_records) >= args.batch_size * 10:
                    db_batch_num += 1
                    records_to_write = len(all_records)
                    bulk_upsert(
                        engine, 'earnings', all_records,
                        conflict_constraint='uq_earnings',
                        conflict_columns=['ticker', 'date'],
                        update_columns=['eps_actual', 'eps_estimated', 
                                       'revenue_actual', 'revenue_estimated']
                    )
                    total_records += records_to_write
                    progress.log_db_write(records_to_write, db_batch_num)
                    all_records = []
                
                progress.update(processed, f"| Fetched: {success_count}, Records: {total_records + len(all_records)}")

        # Write remaining
        if all_records:
            db_batch_num += 1
            records_to_write = len(all_records)
            logger.info("💾 Phase 2: Writing final batch to database...")
            bulk_upsert(
                engine, 'earnings', all_records,
                conflict_constraint='uq_earnings',
                conflict_columns=['ticker', 'date'],
                update_columns=['eps_actual', 'eps_estimated', 
                               'revenue_actual', 'revenue_estimated']
            )
            total_records += records_to_write
            progress.log_db_write(records_to_write, db_batch_num)

        progress.finish(f"- Fetched: {success_count}, Failed: {failed_count}, Total records: {total_records}")

        stmt = insert(SyncMetadata).values(key='earnings_sync', last_synced_at=datetime.utcnow())
        stmt = stmt.on_conflict_do_update(index_elements=['key'], set_={'last_synced_at': datetime.utcnow()})
        session.execute(stmt)
        session.commit()

        total = session.query(Earnings).count()
        elapsed = time.time() - start_time
        
        logger.info("=" * 60)
        logger.info(f"  Tickers processed: {success_count}")
        logger.info(f"  Failed: {failed_count}")
        logger.info(f"  Total in DB: {total}")
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        if elapsed > 0:
            logger.info(f"  Effective rate: {processed / (elapsed / 60):.1f} calls/min")
        logger.info("=" * 60)
        logger.info("✅ Earnings seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{success_count} tickers processed', total, duration_seconds=elapsed)

    except KeyboardInterrupt:
        session.commit()
        logger.info("Interrupted, progress saved.")
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'INTERRUPTED', 'Progress saved', duration_seconds=elapsed)
    except Exception as e:
        logger.error(f"Error: {e}")
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=elapsed)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()
