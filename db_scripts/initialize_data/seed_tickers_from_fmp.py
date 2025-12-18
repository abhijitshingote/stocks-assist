"""
Seed tickers table from FMP Stock Screener API.

Uses the /stable/company-screener endpoint to fetch US stocks that are:
- Actively trading
- Not ETFs
- Not Funds
- Single share class only

Populates the tickers table with basic company info and market metrics.

Reference: https://site.financialmodelingprep.com/developer/docs#search
"""

from datetime import datetime
import os
import sys
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import pytz

# Add backend and db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'seed_tickers_from_fmp'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    """Get FMP API key from environment"""
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


def get_eastern_datetime():
    """Get current datetime in Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)


def fetch_us_stocks(api_key, exchanges=None, limit=10000):
    """
    Fetch US stocks from FMP Stock Screener API.
    
    Filters:
    - isActivelyTrading=true
    - isEtf=false
    - isFund=false
    - includeAllShareClasses=false
    """
    if exchanges is None:
        exchanges = ['NASDAQ', 'NYSE', 'AMEX']
    
    exchange_str = ','.join(exchanges)
    
    url = f'{BASE_URL}/company-screener'
    params = {
        'exchangeShortName': exchange_str,
        'isActivelyTrading': 'true',
        'isEtf': 'false',
        'isFund': 'false',
        'includeAllShareClasses': 'false',
        'limit': limit,
        'apikey': api_key
    }
    
    logger.info(f"Fetching stocks from FMP Stock Screener...")
    logger.info(f"  Exchanges: {exchange_str}")
    logger.info(f"  Filters: activelyTrading=true, isEtf=false, isFund=false, includeAllShareClasses=false")
    
    try:
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"✓ Fetched {len(data)} stocks from screener")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching from screener: {e}")
        return []


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


def upsert_tickers_batch(session, ticker_batch):
    """Upsert a batch of tickers using PostgreSQL ON CONFLICT"""
    if not ticker_batch:
        return 0, 0

    try:
        # Deduplicate within batch
        ticker_map = {t['ticker']: t for t in ticker_batch}
        deduplicated = list(ticker_map.values())

        # Count existing
        tickers = [t['ticker'] for t in deduplicated]
        existing_count = session.query(Ticker).filter(Ticker.ticker.in_(tickers)).count()

        # Upsert
        for data in deduplicated:
            stmt = insert(Ticker).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k != 'ticker'}
            stmt = stmt.on_conflict_do_update(index_elements=['ticker'], set_=update_dict)
            session.execute(stmt)

        session.commit()
        
        new_count = max(0, len(deduplicated) - existing_count)
        return new_count, existing_count

    except Exception as e:
        logger.error(f"Error upserting tickers: {e}")
        session.rollback()
        return 0, 0


def process_stocks(session, stocks, batch_size=500):
    """Process stocks and insert into tickers table"""
    
    tickers_new_total = 0
    tickers_updated_total = 0
    
    ticker_batch = []
    now = get_eastern_datetime()
    
    for i, stock in enumerate(stocks):
        symbol = safe_str(stock.get('symbol'))
        if not symbol:
            continue
        
        symbol = symbol.upper()
        
        # Build ticker data from screener response
        # Maps to the new tickers table schema
        ticker_data = {
            'ticker': symbol,
            'company_name': safe_str(stock.get('companyName')),
            'exchange': safe_str(stock.get('exchange')),
            'exchange_short_name': safe_str(stock.get('exchangeShortName')),
            'sector': safe_str(stock.get('sector')),
            'industry': safe_str(stock.get('industry')),
            'country': safe_str(stock.get('country')),
            'price': parse_float(stock.get('price')),
            'market_cap': parse_int(stock.get('marketCap')),
            'beta': parse_float(stock.get('beta')),
            'volume': parse_int(stock.get('volume')),
            'last_annual_dividend': parse_float(stock.get('lastAnnualDividend')),
            'is_etf': stock.get('isEtf', False),
            'is_fund': stock.get('isFund', False),
            'is_actively_trading': stock.get('isActivelyTrading', True),
            'created_at': now,
            'updated_at': now
        }
        ticker_batch.append(ticker_data)
        
        # Process in batches
        if len(ticker_batch) >= batch_size:
            new_t, updated_t = upsert_tickers_batch(session, ticker_batch)
            tickers_new_total += new_t
            tickers_updated_total += updated_t
            
            processed = i + 1
            logger.info(f"  Processed {processed}/{len(stocks)} stocks...")
            
            ticker_batch = []
    
    # Process remaining
    if ticker_batch:
        new_t, updated_t = upsert_tickers_batch(session, ticker_batch)
        tickers_new_total += new_t
        tickers_updated_total += updated_t
    
    return tickers_new_total, tickers_updated_total


# format_duration is imported from db_scripts.logger


def main():
    """Main function to seed tickers table from FMP API"""
    import argparse
    import time as time_module
    
    start_time = time_module.time()
    
    parser = argparse.ArgumentParser(description='Seed tickers table from FMP Stock Screener API')
    parser.add_argument('--limit', type=int, default=10000,
                        help='Max stocks to fetch (default: 10000)')
    parser.add_argument('--batch-size', type=int, default=500,
                        help='Database batch size (default: 500)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Tickers Table Seeding from FMP Stock Screener API")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    api_key = get_api_key()
    
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Fetch stocks from screener
        stocks = fetch_us_stocks(api_key, limit=args.limit)
        
        if not stocks:
            logger.error("No stocks fetched from FMP API.")
            return
        
        # Process and insert
        logger.info(f"Processing {len(stocks)} stocks...")
        new_t, upd_t = process_stocks(session, stocks, batch_size=args.batch_size)
        
        logger.info("=" * 60)
        logger.info("Results:")
        logger.info(f"  Tickers: {new_t} new, {upd_t} updated")
        
        total_tickers = session.query(Ticker).count()
        logger.info(f"  Total tickers in database: {total_tickers}")
        
        elapsed = time_module.time() - start_time
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ Tickers seeding completed successfully!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{new_t} new, {upd_t} updated tickers', total_tickers, duration_seconds=elapsed)

    except Exception as e:
        logger.error(f"Error in tickers seeding: {e}")
        session.rollback()
        elapsed = time_module.time() - start_time
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=elapsed)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()
