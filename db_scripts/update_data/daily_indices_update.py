#!/usr/bin/env python3
"""
Daily Indices Update Script (FMP API)
Fetches today's index/ETF price data using FMP and updates the IndexPrice table.
Updates SPY, QQQ, and IWM with latest daily data.
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import requests
import time
import pytz
import argparse

# Add backend and db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Base, Index, IndexPrice
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'daily_indices_update'
logger = get_logger(SCRIPT_NAME)

# Load environment variables
load_dotenv()

# FMP API configuration
FMP_API_KEY = os.getenv('FMP_API_KEY')
if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables")

BASE_URL = 'https://financialmodelingprep.com/stable'

# ETF symbols we track
ETF_SYMBOLS = ['SPY', 'QQQ', 'IWM']


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


def fetch_index_price_data(symbol, days_back=5):
    """
    Fetch recent price data for a single index/ETF from FMP
    
    Args:
        symbol: The ETF symbol (SPY, QQQ, IWM)
        days_back: Number of days to fetch (default 5 to ensure we get latest)
    
    Returns:
        List of price records
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Fetching {days_back} days of data for {symbol}...")
        
        url = f"{BASE_URL}/historical-price-eod/full"
        params = {
            'apikey': FMP_API_KEY,
            'symbol': symbol,
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d')
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle different response formats
        if isinstance(data, list):
            historical = data
        elif 'historical' in data:
            historical = data['historical']
        else:
            logger.warning(f"No data returned from FMP for {symbol}")
            return []
        
        if not historical:
            logger.warning(f"No data returned from FMP for {symbol}")
            return []
        
        price_data = []
        for item in historical:
            price_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
            price_data.append({
                'date': price_date,
                'open_price': parse_float(item.get('open')),
                'high_price': parse_float(item.get('high')),
                'low_price': parse_float(item.get('low')),
                'close_price': parse_float(item.get('close')),
                'volume': parse_int(item.get('volume'))
            })
        
        # Sort by date ascending
        price_data.sort(key=lambda x: x['date'])
        
        logger.info(f"✓ Fetched {len(price_data)} records for {symbol}")
        return price_data
        
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {str(e)}")
        return []


def fetch_current_quote(symbol):
    """
    Fetch current quote for a single symbol (for intraday updates).
    
    Args:
        symbol: The ETF symbol
    
    Returns:
        Price data dictionary or None
    """
    try:
        url = f"{BASE_URL}/quote/{symbol}"
        params = {'apikey': FMP_API_KEY}
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data and len(data) > 0:
            quote = data[0]
            today = get_eastern_date()
            
            return {
                'date': today,
                'open_price': parse_float(quote.get('open')),
                'high_price': parse_float(quote.get('dayHigh')),
                'low_price': parse_float(quote.get('dayLow')),
                'close_price': parse_float(quote.get('price')),
                'volume': parse_int(quote.get('volume'))
            }
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching quote for {symbol}: {str(e)}")
        return None


def upsert_index_prices(session, symbol, price_data):
    """
    Upsert index price data into the index_prices table
    
    Args:
        session: Database session
        symbol: Index/ETF symbol (SPY, QQQ, IWM)
        price_data: List of price records
    
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    if not price_data:
        return 0, 0
    
    try:
        inserted_count = 0
        updated_count = 0
        now = get_eastern_datetime()
        
        for data in price_data:
            # Check if record exists
            existing = session.query(IndexPrice).filter(
                IndexPrice.symbol == symbol,
                IndexPrice.date == data['date']
            ).first()
            
            stmt = insert(IndexPrice).values(
                symbol=symbol,
                date=data['date'],
                open_price=data['open_price'],
                high_price=data['high_price'],
                low_price=data['low_price'],
                close_price=data['close_price'],
                volume=data['volume'],
                created_at=now,
                updated_at=now
            )
            
            # Update on conflict
            update_dict = {
                'open_price': stmt.excluded.open_price,
                'high_price': stmt.excluded.high_price,
                'low_price': stmt.excluded.low_price,
                'close_price': stmt.excluded.close_price,
                'volume': stmt.excluded.volume,
                'updated_at': stmt.excluded.updated_at
            }
            
            stmt = stmt.on_conflict_do_update(
                constraint='uq_index_price',
                set_=update_dict
            )
            
            session.execute(stmt)
            
            if existing:
                updated_count += 1
            else:
                inserted_count += 1
        
        session.commit()
        return inserted_count, updated_count
        
    except Exception as e:
        logger.error(f"Error upserting price data for {symbol}: {str(e)}")
        session.rollback()
        return 0, 0


def update_all_indices(session, days_back=5, use_quote=False):
    """
    Update all tracked indices with latest price data
    
    Args:
        session: Database session
        days_back: Number of days to fetch historical data
        use_quote: If True, use quote endpoint for current day only
    
    Returns:
        Dictionary with update statistics
    """
    stats = {
        'total_inserted': 0,
        'total_updated': 0,
        'success': [],
        'failed': []
    }
    
    for symbol in ETF_SYMBOLS:
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Updating {symbol}")
            logger.info(f"{'='*60}")
            
            # Fetch price data
            if use_quote:
                # Use quote endpoint for current day only
                price_data = []
                quote = fetch_current_quote(symbol)
                if quote:
                    price_data = [quote]
            else:
                # Use historical endpoint for multiple days
                price_data = fetch_index_price_data(symbol, days_back=days_back)
            
            if not price_data:
                logger.warning(f"No price data fetched for {symbol}")
                stats['failed'].append(symbol)
                continue
            
            # Upsert into database
            inserted, updated = upsert_index_prices(session, symbol, price_data)
            
            stats['total_inserted'] += inserted
            stats['total_updated'] += updated
            stats['success'].append(symbol)
            
            logger.info(f"✓ {symbol}: {inserted} new, {updated} updated records")
            
            # Rate limiting - be nice to FMP API
            time.sleep(0.3)
            
        except Exception as e:
            logger.error(f"Error updating {symbol}: {str(e)}")
            stats['failed'].append(symbol)
            continue
    
    return stats


def main():
    """Main execution function"""
    overall_start = time.time()
    
    parser = argparse.ArgumentParser(description='Daily indices update from FMP API')
    parser.add_argument('--days', type=int, default=5,
                        help='Number of days of history to fetch (default: 5)')
    parser.add_argument('--quote', action='store_true',
                        help='Use quote endpoint for current day only (faster)')
    parser.add_argument('--symbols', type=str, nargs='+',
                        help='Specific symbols to update (default: SPY QQQ IWM)')
    args = parser.parse_args()
    
    # Override symbols if provided
    global ETF_SYMBOLS
    if args.symbols:
        ETF_SYMBOLS = [s.upper() for s in args.symbols]
    
    logger.info("="*80)
    logger.info("DAILY INDICES UPDATE SCRIPT (FMP)")
    logger.info(f"Started at: {get_eastern_datetime().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("="*80)
    
    try:
        # Initialize database
        session, engine = init_db()
        
        # Check if indices exist in database (optional - proceed even if table doesn't match)
        try:
            index_count = session.query(Index).count()
            if index_count == 0:
                logger.warning("No indices found in database. Will still update index_prices table.")
            else:
                logger.info(f"Found {index_count} indices in database")
        except Exception as e:
            logger.warning(f"Could not query indices table: {e}. Proceeding with index_prices update.")
            session.rollback()  # Clear the failed transaction
        
        logger.info(f"ETFs to update: {ETF_SYMBOLS}")
        
        # Update all indices
        stats = update_all_indices(session, days_back=args.days, use_quote=args.quote)
        
        # Print summary
        logger.info("\n" + "="*80)
        logger.info("UPDATE SUMMARY")
        logger.info("="*80)
        logger.info(f"Successfully updated: {', '.join(stats['success']) if stats['success'] else 'None'}")
        if stats['failed']:
            logger.info(f"Failed to update: {', '.join(stats['failed'])}")
        logger.info(f"Total new records: {stats['total_inserted']}")
        logger.info(f"Total updated records: {stats['total_updated']}")
        logger.info(f"Completed at: {get_eastern_datetime().strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info("="*80)
        
        total_records = stats['total_inserted'] + stats['total_updated']
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'SUCCESS', f"Updated {', '.join(stats['success'])}", total_records, duration_seconds=total_time)
        
        # Close session
        session.close()
        logger.info("=" * 60)
        logger.info(f"=== Daily Indices Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)
        
    except Exception as e:
        logger.error(f"Fatal error in daily update: {str(e)}", exc_info=True)
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
        flush_logger(SCRIPT_NAME)
        sys.exit(1)


if __name__ == '__main__':
    main()
