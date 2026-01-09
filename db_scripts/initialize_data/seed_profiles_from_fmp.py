"""
Seed company_profiles table from FMP Profile API.

Uses the /stable/profile?symbol=X endpoint to fetch detailed company info.

OPTIMIZED:
- Concurrent API calls with rate limiting (290/min)
- Bulk inserts instead of row-by-row

Reference: https://site.financialmodelingprep.com/developer/docs#search
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

# Add backend and db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, CompanyProfile
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration, ProgressTracker, get_test_ticker_limit
from db_scripts.fmp_utils import RateLimiter, bulk_upsert_simple

# Script name for logging
SCRIPT_NAME = 'seed_profiles_from_fmp'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


def fetch_profile(api_key, symbol):
    """Fetch profile for a single symbol from FMP API"""
    url = f'{BASE_URL}/profile'
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


def safe_str(value):
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def parse_float(value):
    try:
        return float(value) if value is not None and value != '' else None
    except (ValueError, TypeError):
        return None


def parse_int(value):
    try:
        return int(float(value)) if value is not None and value != '' else None
    except (ValueError, TypeError):
        return None


def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        return None


def process_profile_data(ticker, profile):
    """Convert API response to database record"""
    if not profile:
        return None
    
    return {
        'ticker': ticker,
        'cik': safe_str(profile.get('cik')),
        'isin': safe_str(profile.get('isin')),
        'cusip': safe_str(profile.get('cusip')),
        'description': safe_str(profile.get('description')),
        'ceo': safe_str(profile.get('ceo')),
        'website': safe_str(profile.get('website')),
        'phone': safe_str(profile.get('phone')),
        'address': safe_str(profile.get('address')),
        'city': safe_str(profile.get('city')),
        'state': safe_str(profile.get('state')),
        'zip': safe_str(profile.get('zip')),
        'full_time_employees': parse_int(profile.get('fullTimeEmployees')),
        'ipo_date': parse_date(profile.get('ipoDate')),
        'image': safe_str(profile.get('image')),
        'currency': safe_str(profile.get('currency')),
        'vol_avg': parse_int(profile.get('averageVolume')),
        'last_div': parse_float(profile.get('lastDividend')),
        'price': parse_float(profile.get('price')),
        'range_52_week': safe_str(profile.get('range')),
        'change': parse_float(profile.get('change')),
        'change_percentage': parse_float(profile.get('changePercentage')),
        'is_adr': profile.get('isAdr', False),
        'default_image': profile.get('defaultImage', False),
        'updated_at': datetime.utcnow()
    }


def get_tickers_without_profiles(session, limit=None):
    """Get tickers that don't have profiles yet"""
    test_limit = get_test_ticker_limit()
    if test_limit:
        logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
        limit = test_limit
    
    subq = session.query(CompanyProfile.ticker)
    query = session.query(Ticker.ticker).filter(
        ~Ticker.ticker.in_(subq)
    ).order_by(Ticker.market_cap.desc().nullslast())
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def get_all_tickers(session, limit=None):
    """Get all tickers ordered by market cap"""
    test_limit = get_test_ticker_limit()
    if test_limit:
        logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
        limit = test_limit
    
    query = session.query(Ticker.ticker).order_by(
        Ticker.market_cap.desc().nullslast()
    )
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed company_profiles from FMP Profile API')
    parser.add_argument('--limit', type=int, default=None, help='Max tickers to process')
    parser.add_argument('--workers', type=int, default=10, help='Concurrent API workers (default: 10)')
    parser.add_argument('--batch-size', type=int, default=100, help='DB write batch size (default: 100)')
    parser.add_argument('--missing-only', action='store_true', help='Only fetch profiles for tickers without existing profiles')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Profile Seeding from FMP Profile API (Optimized)")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    api_key = get_api_key()
    
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get tickers to process
        if args.missing_only:
            logger.info("Mode: Fetching only tickers without existing profiles")
            tickers = get_tickers_without_profiles(session, limit=args.limit)
        else:
            logger.info("Mode: Fetching all tickers (will update existing)")
            tickers = get_all_tickers(session, limit=args.limit)
        
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
        progress = ProgressTracker(len(tickers), logger, update_interval=50, prefix="Profiles:")
        
        all_records = []
        success_count = 0
        failed_count = 0
        processed = 0
        
        def worker(ticker):
            rate_limiter.acquire()
            profile = fetch_profile(api_key, ticker)
            if profile:
                record = process_profile_data(ticker, profile)
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
                    bulk_upsert_simple(
                        engine, 'company_profiles', all_records,
                        primary_key='ticker',
                        update_columns=['cik', 'isin', 'cusip', 'description', 'ceo', 
                                       'website', 'phone', 'address', 'city', 'state', 'zip',
                                       'full_time_employees', 'ipo_date', 'image', 'currency',
                                       'vol_avg', 'last_div', 'price', 'range_52_week',
                                       'change', 'change_percentage', 'is_adr', 'default_image',
                                       'updated_at']
                    )
                    all_records = []
                
                progress.update(processed, f"| Saved: {success_count}")

        # Write remaining
        if all_records:
            bulk_upsert_simple(
                engine, 'company_profiles', all_records,
                primary_key='ticker',
                update_columns=['cik', 'isin', 'cusip', 'description', 'ceo', 
                               'website', 'phone', 'address', 'city', 'state', 'zip',
                               'full_time_employees', 'ipo_date', 'image', 'currency',
                               'vol_avg', 'last_div', 'price', 'range_52_week',
                               'change', 'change_percentage', 'is_adr', 'default_image',
                               'updated_at']
            )

        progress.finish(f"- Saved: {success_count}, Failed: {failed_count}")
        
        total_profiles = session.query(CompanyProfile).count()
        elapsed = time.time() - start_time
        
        logger.info("=" * 60)
        logger.info(f"  Profiles saved: {success_count}")
        logger.info(f"  Failed fetches: {failed_count}")
        logger.info(f"  Total profiles in database: {total_profiles}")
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        if elapsed > 0:
            logger.info(f"  Effective rate: {processed / (elapsed / 60):.1f} calls/min")
        logger.info("=" * 60)
        logger.info("✅ Profile seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{success_count} profiles saved, {failed_count} failed', total_profiles, duration_seconds=elapsed)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted by user. Committing current progress...")
        session.commit()
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'INTERRUPTED', 'Progress saved', duration_seconds=elapsed)
    except Exception as e:
        logger.error(f"Error in profile seeding: {e}")
        session.rollback()
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=elapsed)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()
