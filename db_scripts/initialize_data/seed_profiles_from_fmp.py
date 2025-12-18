"""
Seed company_profiles table from FMP Profile API.

Uses the /stable/profile?symbol=X endpoint to fetch detailed company info.
Since the stable API doesn't support batch requests, profiles are fetched individually.

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
import argparse

# Add backend and db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, CompanyProfile
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'seed_profiles_from_fmp'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    """Get FMP API key from environment"""
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


def fetch_profile(api_key, symbol):
    """Fetch profile for a single symbol from FMP API"""
    url = f'{BASE_URL}/profile'
    params = {
        'symbol': symbol,
        'apikey': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.ok:
            data = response.json()
            if data and len(data) > 0:
                return data[0]
        return None
        
    except Exception as e:
        logger.error(f"Error fetching profile for {symbol}: {e}")
        return None


def safe_str(value):
    """Safely convert value to stripped string or None"""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def parse_float(value):
    """Parse float value"""
    try:
        return float(value) if value is not None and value != '' else None
    except (ValueError, TypeError):
        return None


def parse_int(value):
    """Parse integer value"""
    try:
        return int(float(value)) if value is not None and value != '' else None
    except (ValueError, TypeError):
        return None


def parse_date(date_str):
    """Parse date string to date object"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        return None


def upsert_profile(session, profile_data):
    """Upsert a single company profile"""
    try:
        stmt = insert(CompanyProfile).values(**profile_data)
        update_dict = {k: stmt.excluded[k] for k in profile_data.keys() if k != 'ticker'}
        stmt = stmt.on_conflict_do_update(index_elements=['ticker'], set_=update_dict)
        session.execute(stmt)
        return True
    except Exception as e:
        logger.error(f"Error upserting profile: {e}")
        return False


def get_tickers_without_profiles(session, limit=None):
    """Get tickers that don't have profiles yet"""
    from sqlalchemy import and_, not_, exists
    from sqlalchemy.orm import aliased
    
    # Subquery to find tickers with profiles
    subq = session.query(CompanyProfile.ticker)
    
    # Get tickers not in the subquery
    query = session.query(Ticker.ticker).filter(
        ~Ticker.ticker.in_(subq)
    ).order_by(Ticker.market_cap.desc().nullslast())
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def get_all_tickers(session, limit=None):
    """Get all tickers ordered by market cap"""
    query = session.query(Ticker.ticker).order_by(
        Ticker.market_cap.desc().nullslast()
    )
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def process_profiles(session, api_key, tickers, delay=0.15, commit_every=50):
    """Fetch and process profiles for given tickers"""
    
    profiles_success = 0
    profiles_failed = 0
    
    for i, ticker in enumerate(tickers):
        # Fetch profile
        profile = fetch_profile(api_key, ticker)
        
        if not profile:
            profiles_failed += 1
            if (i + 1) % 100 == 0:
                logger.info(f"  Progress: {i+1}/{len(tickers)} (success: {profiles_success}, failed: {profiles_failed})")
            time.sleep(delay)
            continue
        
        # Build company_profiles data
        profile_data = {
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
        
        if upsert_profile(session, profile_data):
            profiles_success += 1
        
        # Commit periodically
        if (i + 1) % commit_every == 0:
            session.commit()
            logger.info(f"  Progress: {i+1}/{len(tickers)} (success: {profiles_success})")
        
        # Rate limiting
        time.sleep(delay)
    
    # Final commit
    session.commit()
    
    return profiles_success, profiles_failed


# format_duration is imported from db_scripts.logger


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed company_profiles from FMP Profile API')
    parser.add_argument('--limit', type=int, default=None,
                        help='Max tickers to process (default: all)')
    parser.add_argument('--delay', type=float, default=0.15,
                        help='Delay between API calls in seconds (default: 0.15)')
    parser.add_argument('--missing-only', action='store_true',
                        help='Only fetch profiles for tickers without existing profiles')
    parser.add_argument('--commit-every', type=int, default=50,
                        help='Commit to database every N records (default: 50)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Profile Seeding from FMP Profile API")
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
        logger.info(f"API delay: {args.delay}s between calls")
        
        estimated_time = len(tickers) * args.delay / 60
        logger.info(f"Estimated time: ~{estimated_time:.1f} minutes")
        logger.info("-" * 60)
        
        # Process profiles
        profiles_ok, failed = process_profiles(
            session, api_key, tickers, 
            delay=args.delay, 
            commit_every=args.commit_every
        )
        
        logger.info("=" * 60)
        logger.info("Results:")
        logger.info(f"  Profiles saved: {profiles_ok}")
        logger.info(f"  Failed fetches: {failed}")
        
        total_profiles = session.query(CompanyProfile).count()
        logger.info(f"  Total profiles in database: {total_profiles}")
        
        elapsed = time.time() - start_time
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ Profile seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{profiles_ok} profiles saved, {failed} failed', total_profiles)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted by user. Committing current progress...")
        session.commit()
        logger.info("Progress saved.")
        write_summary(SCRIPT_NAME, 'INTERRUPTED', 'Progress saved')
    except Exception as e:
        logger.error(f"Error in profile seeding: {e}")
        session.rollback()
        write_summary(SCRIPT_NAME, 'FAILED', str(e))
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()

