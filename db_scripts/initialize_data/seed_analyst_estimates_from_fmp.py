"""
Seed analyst_estimates table from FMP API.
Source: /stable/analyst-estimates?symbol=X&period=annual

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
import argparse

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, AnalystEstimates, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration, ProgressTracker, estimate_processing_time, get_test_ticker_limit
from db_scripts.fmp_utils import RateLimiter, bulk_upsert, process_tickers_concurrent

# Script name for logging
SCRIPT_NAME = 'seed_analyst_estimates_from_fmp'
logger = get_logger(SCRIPT_NAME)
load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found")
    return api_key


def fetch_analyst_estimates(api_key, symbol, limit=10):
    """Fetch analyst estimates for a symbol (no rate limiting - handled by caller)"""
    url = f'{BASE_URL}/analyst-estimates'
    params = {'symbol': symbol, 'period': 'annual', 'limit': limit, 'apikey': api_key}
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


def process_estimates_data(ticker, estimates):
    """Convert API response to database records.
    
    Note: Deduplicate by date since constraint is (ticker, date).
    API returns latest first, so we keep the first occurrence.
    """
    # Use dict to deduplicate by date (keep first occurrence - API sorted latest first)
    records_by_date = {}
    for e in estimates:
        date = parse_date(e.get('date'))
        if not date:
            continue
        
        # Only add if we haven't seen this date yet (keeps first/latest)
        if date not in records_by_date:
            records_by_date[date] = {
                'ticker': ticker,
                'date': date,
                'revenue_avg': parse_int(e.get('revenueAvg')),
                'revenue_low': parse_int(e.get('revenueLow')),
                'revenue_high': parse_int(e.get('revenueHigh')),
                'ebitda_avg': parse_int(e.get('ebitdaAvg')),
                'ebit_avg': parse_int(e.get('ebitAvg')),
                'net_income_avg': parse_int(e.get('netIncomeAvg')),
                'eps_avg': parse_float(e.get('epsAvg')),
                'eps_low': parse_float(e.get('epsLow')),
                'eps_high': parse_float(e.get('epsHigh')),
                'num_analysts_revenue': parse_int(e.get('numAnalystsRevenue')),
                'num_analysts_eps': parse_int(e.get('numAnalystsEps'))
            }
    return list(records_by_date.values())


def get_tickers(session, limit=None):
    # Check for test mode limit first
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
    parser = argparse.ArgumentParser(description='Seed analyst_estimates from FMP')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--workers', type=int, default=10, help='Concurrent API workers (default: 10)')
    parser.add_argument('--batch-size', type=int, default=100, help='DB write batch size (default: 100)')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Analyst Estimates Seeding (Optimized)")
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

        # Estimate time based on rate limit
        estimated_minutes = len(tickers) / 290
        logger.info(f"Estimated time: ~{estimated_minutes:.1f} minutes")
        logger.info("-" * 60)

        # Initialize rate limiter
        rate_limiter = RateLimiter(calls_per_minute=290)
        
        # Progress tracker
        progress = ProgressTracker(len(tickers), logger, update_interval=50, prefix="Estimates:")

        # Process concurrently
        all_records = []
        success_count = 0
        failed_count = 0
        processed = 0
        total_records = 0
        db_batch_num = 0
        
        logger.info("📡 Phase 1: Fetching analyst estimates from API...")
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def worker(ticker):
            rate_limiter.acquire()
            data = fetch_analyst_estimates(api_key, ticker)
            if data:
                records = process_estimates_data(ticker, data)
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
                        success_count += 1  # No data but successful call
                except Exception as e:
                    failed_count += 1
                    logger.debug(f"Error processing {ticker}: {e}")
                
                processed += 1
                
                # Batch write to DB periodically
                if len(all_records) >= args.batch_size * 10:
                    db_batch_num += 1
                    records_to_write = len(all_records)
                    bulk_upsert(
                        engine, 'analyst_estimates', all_records,
                        conflict_constraint='uq_analyst_est',
                        conflict_columns=['ticker', 'date'],
                        update_columns=['revenue_avg', 'revenue_low', 'revenue_high', 
                                       'ebitda_avg', 'ebit_avg', 'net_income_avg',
                                       'eps_avg', 'eps_low', 'eps_high',
                                       'num_analysts_revenue', 'num_analysts_eps']
                    )
                    total_records += records_to_write
                    progress.log_db_write(records_to_write, db_batch_num)
                    all_records = []
                
                progress.update(processed, f"| Fetched: {success_count}, Records: {total_records + len(all_records)}")

        # Write remaining records
        if all_records:
            db_batch_num += 1
            records_to_write = len(all_records)
            logger.info("💾 Phase 2: Writing final batch to database...")
            bulk_upsert(
                engine, 'analyst_estimates', all_records,
                conflict_constraint='uq_analyst_est',
                conflict_columns=['ticker', 'date'],
                update_columns=['revenue_avg', 'revenue_low', 'revenue_high', 
                               'ebitda_avg', 'ebit_avg', 'net_income_avg',
                               'eps_avg', 'eps_low', 'eps_high',
                               'num_analysts_revenue', 'num_analysts_eps']
            )
            total_records += records_to_write
            progress.log_db_write(records_to_write, db_batch_num)

        progress.finish(f"- Fetched: {success_count}, Failed: {failed_count}, Total records: {total_records}")

        # Update sync metadata
        stmt = insert(SyncMetadata).values(key='analyst_estimates_sync', last_synced_at=datetime.utcnow())
        stmt = stmt.on_conflict_do_update(index_elements=['key'], set_={'last_synced_at': datetime.utcnow()})
        session.execute(stmt)
        session.commit()

        total = session.query(AnalystEstimates).count()
        elapsed = time.time() - start_time
        
        logger.info("=" * 60)
        logger.info(f"  Tickers processed: {success_count}")
        logger.info(f"  Failed: {failed_count}")
        logger.info(f"  Total in DB: {total}")
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        if elapsed > 0:
            logger.info(f"  Effective rate: {processed / (elapsed / 60):.1f} calls/min")
        logger.info("=" * 60)
        logger.info("✅ Analyst estimates seeding completed!")
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
