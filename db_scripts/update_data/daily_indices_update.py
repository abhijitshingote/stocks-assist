#!/usr/bin/env python3
"""
Daily Indices Update Script
Fetches today's index/ETF price data using Polygon.io and updates the IndexPrice table.
Updates SPX (via SPY), QQQ, and IWM with latest daily data.
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
from polygon import RESTClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import time
import pytz

# Add backend to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import Base, Index, IndexPrice

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_indices_update.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Polygon.io client
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY not found in environment variables")

client = RESTClient(POLYGON_API_KEY)

# Index symbols we track
INDEX_SYMBOLS = {
    'SPX': 'SPY',  # Track S&P 500 via SPY ETF
    'QQQ': 'QQQ',  # Nasdaq-100 ETF
    'IWM': 'IWM'   # Russell 2000 ETF
}

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

def fetch_index_price_data(polygon_symbol, days_back=5):
    """
    Fetch recent price data for a single index/ETF from Polygon
    
    Args:
        polygon_symbol: The Polygon symbol (SPY, QQQ, IWM)
        days_back: Number of days to fetch (default 5 to ensure we get latest)
    
    Returns:
        List of price records
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Fetching {days_back} days of data for {polygon_symbol}...")
        
        # Get daily aggregates
        aggs = client.get_aggs(
            polygon_symbol,
            multiplier=1,
            timespan="day",
            from_=start_date.strftime('%Y-%m-%d'),
            to=end_date.strftime('%Y-%m-%d')
        )
        
        if not aggs:
            logger.warning(f"No data returned from Polygon for {polygon_symbol}")
            return []
        
        price_data = []
        for agg in aggs:
            price_date = datetime.fromtimestamp(agg.timestamp / 1000).date()
            price_data.append({
                'date': price_date,
                'open_price': agg.open,
                'high_price': agg.high,
                'low_price': agg.low,
                'close_price': agg.close,
                'volume': agg.volume
            })
        
        logger.info(f"✓ Fetched {len(price_data)} records for {polygon_symbol}")
        return price_data
        
    except Exception as e:
        logger.error(f"Error fetching data for {polygon_symbol}: {str(e)}")
        return []

def upsert_index_prices(session, symbol, price_data):
    """
    Upsert index price data into the index_price table
    
    Args:
        session: Database session
        symbol: Index symbol (SPX, QQQ, IWM)
        price_data: List of price records
    
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    if not price_data:
        return 0, 0
    
    try:
        inserted_count = 0
        updated_count = 0
        
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
                created_at=get_eastern_datetime(),
                updated_at=get_eastern_datetime()
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
                index_elements=['symbol', 'date'],
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

def update_all_indices(session):
    """
    Update all tracked indices with latest price data
    
    Returns:
        Dictionary with update statistics
    """
    stats = {
        'total_inserted': 0,
        'total_updated': 0,
        'success': [],
        'failed': []
    }
    
    for symbol, polygon_symbol in INDEX_SYMBOLS.items():
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Updating {symbol} (via {polygon_symbol})")
            logger.info(f"{'='*60}")
            
            # Fetch price data (last 5 days to ensure we get latest)
            price_data = fetch_index_price_data(polygon_symbol, days_back=5)
            
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
            
            # Rate limiting - be nice to Polygon API
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error updating {symbol}: {str(e)}")
            stats['failed'].append(symbol)
            continue
    
    return stats

def main():
    """Main execution function"""
    logger.info("="*80)
    logger.info("DAILY INDICES UPDATE SCRIPT")
    logger.info(f"Started at: {get_eastern_datetime().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("="*80)
    
    try:
        # Initialize database
        session, engine = init_db()
        
        # Check if indices exist in database
        index_count = session.query(Index).count()
        if index_count == 0:
            logger.error("No indices found in database. Please run seed_index_prices_polygon.py first.")
            return
        
        logger.info(f"Found {index_count} indices in database")
        
        # Update all indices
        stats = update_all_indices(session)
        
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
        
        # Close session
        session.close()
        
    except Exception as e:
        logger.error(f"Fatal error in daily update: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()

