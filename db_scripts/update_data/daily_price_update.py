#!/usr/bin/env python3
"""
Daily Price Update Script (FMP API)
Fetches today's stock price data using FMP and updates the OHLC table.

OPTIMIZED:
- Concurrent API calls with rate limiting (290/min)
- Bulk upsert for efficient DB writes
"""

import os
import sys
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import requests
import time
import pytz
import argparse

# Add backend and db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Base, Ticker, OHLC
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration, ProgressTracker, get_test_ticker_limit
from db_scripts.fmp_utils import RateLimiter

# Script name for logging
SCRIPT_NAME = 'daily_price_update'
logger = get_logger(SCRIPT_NAME)

# Load environment variables
load_dotenv()

# FMP API configuration
FMP_API_KEY = os.getenv('FMP_API_KEY')
if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables")

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_eastern_date():
    """Get current date in Eastern Time (handles EST/EDT automatically)"""
    eastern = pytz.timezone('US/Eastern')
    eastern_now = datetime.now(eastern)
    return eastern_now.date()


def get_eastern_datetime():
    """Get current datetime in Eastern Time (handles EST/EDT automatically)"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)


def init_db():
    """Initialize database connection"""
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment variables")
    
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session(), engine


def get_existing_tickers(session):
    """Get all tickers that exist in the Ticker table"""
    try:
        tickers = session.query(Ticker.ticker).filter(
            Ticker.is_actively_trading == True
        ).all()
        ticker_list = [t[0] for t in tickers]
        logger.info(f"Found {len(ticker_list)} actively trading tickers in database")
        
        # Apply test mode limit if set
        test_limit = get_test_ticker_limit()
        if test_limit:
            ticker_list = ticker_list[:test_limit]
            logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
        
        return ticker_list
    except Exception as e:
        logger.error(f"Error fetching existing tickers: {str(e)}")
        return []


def parse_float(value):
    """Parse float value safely"""
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def parse_int(value):
    """Parse integer value safely"""
    try:
        return int(float(value)) if value is not None else None
    except (ValueError, TypeError):
        return None


def fetch_single_ticker_eod(ticker, target_date, api_key):
    """
    Fetch EOD price for a single ticker.
    Returns (ticker, price_data) or (ticker, None)
    """
    date_str = target_date.strftime('%Y-%m-%d')
    
    try:
        url = f"{BASE_URL}/historical-price-eod/full"
        params = {
            'apikey': api_key,
            'symbol': ticker,
            'from': date_str,
            'to': date_str
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle list or dict response
        historical = data if isinstance(data, list) else data.get('historical', [])
        
        if historical:
            item = historical[0]
            return (ticker, {
                'ticker': ticker,
                'date': datetime.strptime(item['date'], '%Y-%m-%d').date(),
                'open': parse_float(item.get('open')),
                'high': parse_float(item.get('high')),
                'low': parse_float(item.get('low')),
                'close': parse_float(item.get('close')),
                'volume': parse_int(item.get('volume'))
            })
        
        return (ticker, None)
        
    except Exception:
        return (ticker, None)


def fetch_eod_prices_concurrent(tickers, target_date, api_key, max_workers=10):
    """
    Fetch EOD price data concurrently with rate limiting.
    
    Args:
        tickers: List of ticker symbols
        target_date: The date to fetch prices for
        api_key: FMP API key
        max_workers: Number of concurrent workers
    
    Returns:
        List of price data dictionaries
    """
    total = len(tickers)
    date_str = target_date.strftime('%Y-%m-%d')
    
    logger.info(f"Fetching EOD prices for {total} tickers (date: {date_str})")
    logger.info(f"Workers: {max_workers}, Rate limit: 290 calls/min")
    
    # Estimate time based on rate limit
    estimated_minutes = total / 290
    logger.info(f"Estimated time: ~{estimated_minutes:.1f} minutes")
    logger.info("-" * 60)
    
    logger.info("📡 Phase 1: Fetching EOD prices from API...")
    
    rate_limiter = RateLimiter(calls_per_minute=290)
    progress = ProgressTracker(total, logger, update_interval=100, prefix="Prices:")
    
    all_prices = []
    success_count = 0
    error_count = 0
    processed = 0
    start_time = time.time()
    
    def worker(ticker):
        rate_limiter.acquire()
        return fetch_single_ticker_eod(ticker, target_date, api_key)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, t): t for t in tickers}
        
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                _, price_data = future.result()
                if price_data and price_data.get('close') is not None:
                    all_prices.append(price_data)
                    success_count += 1
                else:
                    error_count += 1
            except Exception:
                error_count += 1
            
            processed += 1
            progress.update(processed, f"| Fetched: {success_count}, Missing: {error_count}")
    
    # Final summary
    total_time = time.time() - start_time
    progress.finish(f"- Fetched: {len(all_prices)}, Missing: {error_count}")
    
    if total_time > 0:
        logger.info(f"Effective rate: {processed / (total_time / 60):.1f} calls/min")
    
    return all_prices


def bulk_upsert_prices(session, engine, price_records):
    """
    Bulk upsert price records using PostgreSQL's ON CONFLICT functionality.
    This will automatically overwrite existing records for the same ticker and date.
    """
    if not price_records:
        logger.warning("No price records to upsert")
        return 0
    
    # Deduplicate records by (ticker, date) - keep first occurrence (API returns latest first)
    # Prevents "ON CONFLICT DO UPDATE command cannot affect row a second time" error
    seen = {}
    for rec in price_records:
        key = (rec['ticker'], rec['date'])
        if key not in seen:
            seen[key] = rec
    price_records = list(seen.values())
    
    logger.info(f"Starting bulk upsert of {len(price_records)} price records...")
    
    try:
        # Use PostgreSQL's INSERT ... ON CONFLICT for efficient upserts
        stmt = insert(OHLC).values(price_records)
        
        # Define what to do on conflict (use the constraint name)
        stmt = stmt.on_conflict_do_update(
            constraint='uq_ohlc',
            set_=dict(
                open=stmt.excluded.open,
                high=stmt.excluded.high,
                low=stmt.excluded.low,
                close=stmt.excluded.close,
                volume=stmt.excluded.volume
            )
        )
        
        # Execute the bulk upsert
        session.execute(stmt)
        session.commit()
        
        logger.info(f"Successfully upserted {len(price_records)} price records")
        return len(price_records)
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error in bulk upsert: {str(e)}")
        return 0


def main():
    """Main function to fetch today's price data and update the database"""
    parser = argparse.ArgumentParser(description='Daily price update from FMP API')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD). Defaults to today.')
    parser.add_argument('--workers', type=int, default=10,
                        help='Number of concurrent API workers (default: 10)')
    args = parser.parse_args()
    
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting Daily Price Update (Optimized) ===")
    logger.info("=" * 60)
    
    session, engine = init_db()
    
    try:
        # Step 1: Get existing tickers from our database
        existing_tickers = get_existing_tickers(session)
        if not existing_tickers:
            logger.error("No existing tickers found in database. Run initial data fetch first.")
            return
        
        # Determine target date
        if args.date:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        else:
            target_date = get_eastern_date()
        
        logger.info(f"Target date: {target_date}")
        
        # Step 2: Fetch price data concurrently
        price_records = fetch_eod_prices_concurrent(
            existing_tickers, 
            target_date, 
            FMP_API_KEY,
            max_workers=args.workers
        )
        
        # Step 3: Bulk upsert the price data
        total_time = time.time() - overall_start
        if price_records:
            logger.info("💾 Phase 2: Saving prices to database...")
            upserted_count = bulk_upsert_prices(session, engine, price_records)
            logger.info(f"💾 DB Write: {upserted_count:,} records saved for {target_date}")
            write_summary(SCRIPT_NAME, 'SUCCESS', f'Updated prices for {target_date}', upserted_count, duration_seconds=total_time)
        else:
            logger.warning("No price data fetched")
            write_summary(SCRIPT_NAME, 'WARNING', 'No price data fetched', duration_seconds=total_time)
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        session.rollback()
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== Daily Price Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()
