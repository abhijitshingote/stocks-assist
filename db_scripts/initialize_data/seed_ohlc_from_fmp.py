"""
Seed OHLC (price history) table from FMP Historical Price API.

Uses the /stable/historical-price-eod/full endpoint to fetch daily OHLC data.

OPTIMIZATIONS:
- Concurrent API calls with proper rate limiting (300/min)
- True bulk upserts using execute_values (not row-by-row)
- Pre-fetch last dates for all tickers in single query
- Batch writes: accumulate data from multiple tickers before DB write

Reference: https://site.financialmodelingprep.com/developer/docs
"""

from datetime import datetime, timedelta
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import argparse

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, OHLC, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration, ProgressTracker, estimate_processing_time, get_test_ticker_limit

# Script name for logging
SCRIPT_NAME = 'seed_ohlc_from_fmp'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


class RateLimiter:
    """
    Token bucket rate limiter for API calls.
    Allows bursting while maintaining average rate of calls_per_minute.
    """
    def __init__(self, calls_per_minute=300):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute  # Minimum seconds between calls
        self.lock = threading.Lock()
        self.call_times = deque(maxlen=calls_per_minute)
    
    def acquire(self):
        """Wait until we can make an API call without exceeding rate limit."""
        with self.lock:
            now = time.time()
            
            # Remove timestamps older than 60 seconds
            while self.call_times and now - self.call_times[0] > 60:
                self.call_times.popleft()
            
            # If we've made max calls in the last minute, wait
            if len(self.call_times) >= self.calls_per_minute:
                # Wait until the oldest call is more than 60 seconds ago
                sleep_time = 60 - (now - self.call_times[0]) + 0.05  # Add small buffer
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    # Clean up again after sleeping
                    while self.call_times and now - self.call_times[0] > 60:
                        self.call_times.popleft()
            
            self.call_times.append(now)


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


def fetch_historical_prices(api_key, symbol, from_date=None, to_date=None, rate_limiter=None):
    """Fetch historical OHLC data for a symbol"""
    if rate_limiter:
        rate_limiter.acquire()
    
    url = f'{BASE_URL}/historical-price-eod/full'
    params = {
        'symbol': symbol,
        'apikey': api_key
    }
    
    if from_date:
        params['from'] = from_date.strftime('%Y-%m-%d')
    if to_date:
        params['to'] = to_date.strftime('%Y-%m-%d')
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.ok:
            data = response.json()
            return data if isinstance(data, list) else []
        return []
    except Exception as e:
        logger.error(f"Error fetching prices for {symbol}: {e}")
        return []


def parse_float(value):
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def parse_int(value):
    try:
        return int(float(value)) if value is not None else None
    except (ValueError, TypeError):
        return None


def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        return None


def bulk_upsert_ohlc(engine, ohlc_records):
    """
    True bulk upsert using raw SQL with ON CONFLICT.
    Much faster than row-by-row inserts.
    """
    if not ohlc_records:
        return 0
    
    # Build values for bulk insert
    # Use raw connection for best performance
    with engine.connect() as conn:
        try:
            # Chunk into batches of 5000 to avoid query size limits
            chunk_size = 5000
            total_inserted = 0
            
            for i in range(0, len(ohlc_records), chunk_size):
                chunk = ohlc_records[i:i + chunk_size]
                
                # Build the VALUES clause
                values_list = []
                params = {}
                for idx, rec in enumerate(chunk):
                    values_list.append(
                        f"(:ticker_{idx}, :date_{idx}, :open_{idx}, :high_{idx}, :low_{idx}, :close_{idx}, :volume_{idx})"
                    )
                    params[f'ticker_{idx}'] = rec['ticker']
                    params[f'date_{idx}'] = rec['date']
                    params[f'open_{idx}'] = rec['open']
                    params[f'high_{idx}'] = rec['high']
                    params[f'low_{idx}'] = rec['low']
                    params[f'close_{idx}'] = rec['close']
                    params[f'volume_{idx}'] = rec['volume']
                
                sql = f"""
                    INSERT INTO ohlc (ticker, date, open, high, low, close, volume)
                    VALUES {', '.join(values_list)}
                    ON CONFLICT ON CONSTRAINT uq_ohlc 
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """
                
                conn.execute(text(sql), params)
                total_inserted += len(chunk)
            
            conn.commit()
            return total_inserted
            
        except Exception as e:
            logger.error(f"Error in bulk upsert: {e}")
            conn.rollback()
            return 0


def update_sync_metadata(session, key, timestamp=None):
    """Update sync metadata tracking"""
    if timestamp is None:
        timestamp = datetime.utcnow()
    
    stmt = insert(SyncMetadata).values(key=key, last_synced_at=timestamp)
    stmt = stmt.on_conflict_do_update(
        index_elements=['key'],
        set_={'last_synced_at': timestamp}
    )
    session.execute(stmt)
    session.commit()


def get_tickers_for_ohlc(session, limit=None, min_market_cap=None):
    """Get tickers to fetch OHLC for, ordered by market cap"""
    # Check for test mode limit first
    test_limit = get_test_ticker_limit()
    if test_limit:
        logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
        limit = test_limit
    
    query = session.query(Ticker.ticker).filter(
        Ticker.is_actively_trading == True
    ).order_by(Ticker.market_cap.desc().nullslast())
    
    if min_market_cap:
        query = query.filter(Ticker.market_cap >= min_market_cap)
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def get_all_last_ohlc_dates(session, tickers):
    """
    Get last OHLC date for ALL tickers in a single query.
    Returns dict: {ticker: last_date}
    """
    if not tickers:
        return {}
    
    # Single query to get max date for all tickers at once
    result = session.query(
        OHLC.ticker,
        func.max(OHLC.date)
    ).filter(
        OHLC.ticker.in_(tickers)
    ).group_by(OHLC.ticker).all()
    
    return {ticker: last_date for ticker, last_date in result}


def fetch_ticker_data(args):
    """
    Worker function for concurrent API calls.
    Returns (ticker, ohlc_records, success)
    """
    ticker, api_key, from_date, to_date, rate_limiter = args
    
    prices = fetch_historical_prices(api_key, ticker, from_date, to_date, rate_limiter)
    
    if not prices:
        return (ticker, [], False)
    
    # Build OHLC records
    ohlc_records = []
    for p in prices:
        date = parse_date(p.get('date'))
        if not date:
            continue
        
        ohlc_records.append({
            'ticker': ticker,
            'date': date,
            'open': parse_float(p.get('open')),
            'high': parse_float(p.get('high')),
            'low': parse_float(p.get('low')),
            'close': parse_float(p.get('close')),
            'volume': parse_int(p.get('volume'))
        })
    
    return (ticker, ohlc_records, True)


def process_ohlc_concurrent(engine, session, api_key, tickers, days_back=365, 
                            max_workers=10, batch_size=50):
    """
    Fetch and process OHLC for tickers using concurrent API calls.
    
    - max_workers: Number of concurrent API request threads
    - batch_size: Number of tickers to accumulate before writing to DB
    """
    records_inserted = 0
    tickers_processed = 0
    tickers_failed = 0
    tickers_skipped = 0

    # Initialize rate limiter: 300 calls per minute, with some safety margin
    rate_limiter = RateLimiter(calls_per_minute=290)

    # Pre-fetch all last dates in a single query
    logger.info("Fetching last OHLC dates for all tickers...")
    last_dates = get_all_last_ohlc_dates(session, tickers)
    logger.info(f"Found existing data for {len(last_dates)} tickers")

    to_date = datetime.now().date()
    default_from_date = to_date - timedelta(days=days_back)

    # Build work items: (ticker, api_key, from_date, to_date, rate_limiter)
    work_items = []
    for ticker in tickers:
        last_date = last_dates.get(ticker)
        
        if last_date:
            from_date = last_date + timedelta(days=1)
            if from_date >= to_date:
                # Already up to date
                tickers_skipped += 1
                continue
        else:
            from_date = default_from_date
        
        work_items.append((ticker, api_key, from_date, to_date, rate_limiter))

    logger.info(f"Tickers already up-to-date (skipped): {tickers_skipped}")
    logger.info(f"Tickers to fetch: {len(work_items)}")
    
    if not work_items:
        return tickers_skipped, 0, 0

    # Initialize progress tracker
    total_to_process = len(work_items)
    progress = ProgressTracker(total_to_process, logger, update_interval=10, prefix="OHLC:")

    # Process in batches with concurrent API calls
    accumulated_records = []
    processed_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all work items
        future_to_ticker = {
            executor.submit(fetch_ticker_data, item): item[0] 
            for item in work_items
        }
        
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                ticker, ohlc_records, success = future.result()
                
                if success and ohlc_records:
                    accumulated_records.extend(ohlc_records)
                    tickers_processed += 1
                elif not success:
                    tickers_failed += 1
                else:
                    tickers_processed += 1  # Success but no new data
                
                processed_count += 1
                
                # Write to DB when batch is full
                if len(accumulated_records) >= batch_size * 250:  # ~250 records per ticker avg
                    count = bulk_upsert_ohlc(engine, accumulated_records)
                    records_inserted += count
                    accumulated_records = []  # Clear memory
                
                progress.update(processed_count, f"| Records: {records_inserted} | Queue: {len(accumulated_records)}")
                
            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
                tickers_failed += 1
                processed_count += 1

    # Write remaining records
    if accumulated_records:
        count = bulk_upsert_ohlc(engine, accumulated_records)
        records_inserted += count

    progress.finish(f"- Total records: {records_inserted}")
    return tickers_processed + tickers_skipped, records_inserted, tickers_failed


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed OHLC table from FMP Historical Price API')
    parser.add_argument('--limit', type=int, default=None,
                        help='Max tickers to process (default: all)')
    parser.add_argument('--days', type=int, default=730,
                        help='Days of history to fetch for new tickers (default: 730)')
    parser.add_argument('--workers', type=int, default=10,
                        help='Number of concurrent API workers (default: 10)')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='Tickers to accumulate before DB write (default: 50)')
    parser.add_argument('--min-market-cap', type=int, default=None,
                        help='Minimum market cap filter (e.g., 1000000000 for $1B)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("OHLC Seeding from FMP Historical Price API (Optimized)")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    api_key = get_api_key()
    
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        tickers = get_tickers_for_ohlc(session, limit=args.limit, min_market_cap=args.min_market_cap)
        
        if not tickers:
            logger.info("No tickers to process.")
            return
        
        logger.info(f"Tickers to check: {len(tickers)}")
        logger.info(f"Days of history: {args.days}")
        logger.info(f"Concurrent workers: {args.workers}")
        logger.info(f"Batch size: {args.batch_size} tickers")
        logger.info(f"Rate limit: 290 calls/min (with safety margin)")

        # Estimate based on rate limit: ~290 calls/min with concurrency
        calls_per_min = 290
        estimated_minutes = len(tickers) / calls_per_min
        logger.info(f"Estimated time: ~{estimated_minutes:.1f} minutes (rate-limited)")
        logger.info("-" * 60)
        
        processed, inserted, failed = process_ohlc_concurrent(
            engine, session, api_key, tickers,
            days_back=args.days,
            max_workers=args.workers,
            batch_size=args.batch_size
        )
        
        # Update sync metadata
        update_sync_metadata(session, 'ohlc_last_sync')
        
        logger.info("=" * 60)
        logger.info("Results:")
        logger.info(f"  Tickers processed: {processed}")
        logger.info(f"  Records inserted: {inserted}")
        logger.info(f"  Failed fetches: {failed}")
        
        total_ohlc = session.query(OHLC).count()
        logger.info(f"  Total OHLC records: {total_ohlc}")
        
        elapsed = time.time() - start_time
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        
        # Calculate effective API rate
        if elapsed > 0:
            effective_rate = (processed - failed) / (elapsed / 60)
            logger.info(f"  Effective API rate: {effective_rate:.1f} calls/min")
        
        logger.info("=" * 60)
        logger.info("✅ OHLC seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{processed} tickers processed, {inserted} records inserted', total_ohlc, duration_seconds=elapsed)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted. Saving progress...")
        session.commit()
        update_sync_metadata(session, 'ohlc_last_sync')
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
