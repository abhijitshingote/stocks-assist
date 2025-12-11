#!/usr/bin/env python3
"""
Daily Price Update Script
Fetches today's stock price data using Polygon.io snapshot API and updates the Price table.
Overwrites existing data for today's date if it exists.
"""

import os
import sys
import logging
from datetime import datetime, date
from polygon import RESTClient
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import requests
import time
import pytz

# Add backend to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import Base, Stock, Price

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_price_update.log'),
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
    """Get all tickers that exist in the Stock table"""
    try:
        tickers = session.query(Stock.ticker).all()
        ticker_list = [t[0] for t in tickers]
        logger.info(f"Found {len(ticker_list)} existing tickers in database")
        return ticker_list
    except Exception as e:
        logger.error(f"Error fetching existing tickers: {str(e)}")
        return []

def fetch_market_snapshot():
    """
    Fetch today's market snapshot data for all tickers using Polygon's snapshot endpoint
    """
    logger.info("Fetching today's market snapshot data...")
    
    try:
        # Use the v2 snapshot endpoint for all US stocks
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        params = {
            'apiKey': POLYGON_API_KEY,
            'include_otc': 'false'  # Exclude OTC securities
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('status') == 'OK' and 'tickers' in data:
            logger.info(f"Successfully fetched snapshot data for {len(data['tickers'])} tickers")
            return data['tickers']
        else:
            logger.error(f"API response error: {data.get('status', 'Unknown error')}")
            return []
            
    except Exception as e:
        logger.error(f"Error fetching market snapshot: {str(e)}")
        return []

def fetch_specific_tickers_snapshot(tickers, batch_size=100):
    """
    Fetch snapshot data for specific tickers in batches using the v3 snapshot endpoint
    This is used as a fallback if the full market snapshot doesn't work
    """
    logger.info(f"Fetching snapshot data for {len(tickers)} specific tickers...")
    
    all_snapshots = []
    
    # Process tickers in batches
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        ticker_list = ','.join(batch)
        
        try:
            url = f"https://api.polygon.io/v3/snapshot"
            params = {
                'ticker.any_of': ticker_list,
                'apiKey': POLYGON_API_KEY
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'OK' and 'results' in data:
                all_snapshots.extend(data['results'])
                logger.info(f"Batch {i//batch_size + 1}: fetched {len(data['results'])} tickers")
            else:
                logger.warning(f"No data for batch {i//batch_size + 1}: {data.get('status', 'Unknown')}")
            
            # Rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Error fetching batch {i//batch_size + 1}: {str(e)}")
            continue
    
    logger.info(f"Total snapshots fetched: {len(all_snapshots)}")
    return all_snapshots

def process_v2_snapshot_data(snapshots, existing_tickers):
    """
    Process v2 snapshot data and prepare for database insertion
    v2 format: {'ticker': 'AAPL', 'day': {'o': 150, 'h': 155, 'l': 149, 'c': 154, 'v': 1000000}, 'lastTrade': {'t': timestamp}}
    """
    price_records = []
    processed_count = 0
    
    logger.info(f"Processing v2 snapshot data for {len(snapshots)} tickers...")
    
    for snapshot in snapshots:
        try:
            ticker = snapshot.get('ticker')
            if not ticker or ticker not in existing_tickers:
                continue
            
            # Extract today's price data
            day_data = snapshot.get('day', {})
            
            # Skip if no day data available
            if not day_data:
                continue
            
            # Extract the actual trading date from the API response
            trading_date = None
            last_trade_timestamp = None
            
            # Prefer updated field first (nanosecond timestamp)
            timestamp_ns = snapshot.get('updated')
            if timestamp_ns:
                try:
                    # Convert nanosecond timestamp to seconds and then to datetime
                    timestamp_seconds = timestamp_ns / 1_000_000_000
                    utc_dt = datetime.fromtimestamp(timestamp_seconds, tz=pytz.UTC)
                    eastern = pytz.timezone('US/Eastern')
                    eastern_dt = utc_dt.astimezone(eastern)
                    trading_date = eastern_dt.date()
                    last_trade_timestamp = eastern_dt
                except (ValueError, TypeError, OSError) as e:
                    logger.warning(f"Could not parse updated timestamp {timestamp_ns} for {ticker}: {e}")
            
            # Fallback to min.t if updated is not available
            if not trading_date:
                min_data = snapshot.get('min', {})
                if 't' in min_data:
                    try:
                        timestamp_ms = min_data['t']
                        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                        eastern_dt = dt.astimezone(pytz.timezone('US/Eastern'))
                        trading_date = eastern_dt.date()
                        last_trade_timestamp = eastern_dt
                    except (ValueError, TypeError, OSError) as e:
                        logger.warning(f"Could not parse min timestamp {timestamp_ms} for {ticker}: {e}")
            
            # If we still don't have a trading date, fall back to today (should rarely happen)
            if not trading_date:
                trading_date = get_eastern_date()
                logger.warning(f"No timestamp data found for {ticker}, using current date: {trading_date}")
            
            # Create price record
            price_record = {
                'ticker': ticker,
                'date': trading_date,  # Use actual trading date from API
                'open_price': day_data.get('o'),
                'high_price': day_data.get('h'),
                'low_price': day_data.get('l'),
                'close_price': day_data.get('c'),
                'volume': day_data.get('v'),
                'last_traded_timestamp': last_trade_timestamp,
                'created_at': get_eastern_datetime(),
                'updated_at': get_eastern_datetime()
            }
            
            # Only add if we have at least a close price
            if price_record['close_price'] is not None:
                price_records.append(price_record)
                processed_count += 1
                
        except Exception as e:
            logger.error(f"Error processing v2 snapshot for {snapshot.get('ticker', 'unknown')}: {str(e)}")
            continue
    
    logger.info(f"Processed {processed_count} valid price records from v2 snapshot data")
    return price_records

def process_v3_snapshot_data(snapshots, existing_tickers):
    """
    Process v3 snapshot data and prepare for database insertion
    v3 format: {'ticker': 'AAPL', 'value': 154, 'session': {'change': 2, 'change_percent': 1.3, ...}, 'last_trade': {'participant_timestamp': timestamp}}
    """
    price_records = []
    processed_count = 0
    
    logger.info(f"Processing v3 snapshot data for {len(snapshots)} tickers...")
    
    for snapshot in snapshots:
        try:
            ticker = snapshot.get('ticker')
            if not ticker or ticker not in existing_tickers:
                continue
            
            # V3 snapshot has different structure
            session_data = snapshot.get('session', {})
            market_status = snapshot.get('market_status', 'closed')
            
            # Skip if market is not open and no session data
            if not session_data:
                continue
            
            # Extract the actual trading date from the v3 API response
            trading_date = None
            last_trade_timestamp = None
            
            # Prefer updated field first (nanosecond timestamp)
            timestamp_ns = snapshot.get('updated')
            if timestamp_ns:
                try:
                    timestamp_seconds = timestamp_ns / 1_000_000_000
                    utc_dt = datetime.fromtimestamp(timestamp_seconds, tz=pytz.UTC)
                    eastern = pytz.timezone('US/Eastern')
                    eastern_dt = utc_dt.astimezone(eastern)
                    trading_date = eastern_dt.date()
                    last_trade_timestamp = eastern_dt
                except (ValueError, TypeError, OSError) as e:
                    logger.warning(f"Could not parse updated timestamp {timestamp_ns} for {ticker}: {e}")
            
            # Fallback to last_minute.t timestamp
            if not trading_date:
                last_minute = snapshot.get('last_minute', {})
                if 't' in last_minute:
                    try:
                        timestamp_ms = last_minute['t']
                        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                        eastern_dt = dt.astimezone(pytz.timezone('US/Eastern'))
                        trading_date = eastern_dt.date()
                        last_trade_timestamp = eastern_dt
                    except (ValueError, TypeError, OSError) as e:
                        logger.warning(f"Could not parse last_minute timestamp {timestamp_ms} for {ticker}: {e}")
            
            # Fallback to last_trade timestamp fields
            if not trading_date:
                last_trade = snapshot.get('last_trade')
                if last_trade:
                    timestamp_ns = (last_trade.get('participant_timestamp') or 
                                  last_trade.get('sip_timestamp') or 
                                  last_trade.get('timeframe'))
                    if timestamp_ns:
                        try:
                            timestamp_seconds = timestamp_ns / 1_000_000_000
                            utc_dt = datetime.fromtimestamp(timestamp_seconds, tz=pytz.UTC)
                            eastern = pytz.timezone('US/Eastern')
                            eastern_dt = utc_dt.astimezone(eastern)
                            trading_date = eastern_dt.date()
                            last_trade_timestamp = eastern_dt
                        except (ValueError, TypeError, OSError) as e:
                            logger.warning(f"Could not parse last_trade timestamp {timestamp_ns} for {ticker}: {e}")
            
            # Final fallback to current date
            if not trading_date:
                trading_date = get_eastern_date()
                logger.warning(f"No timestamp data found for {ticker}, using current date: {trading_date}")
            
            # For v3, we need to use different field names
            price_record = {
                'ticker': ticker,
                'date': trading_date,  # Use actual trading date from API
                'open_price': session_data.get('open'),
                'high_price': session_data.get('high'),
                'low_price': session_data.get('low'),
                'close_price': session_data.get('close') or snapshot.get('value'),  # value is current price
                'volume': session_data.get('volume'),
                'last_traded_timestamp': last_trade_timestamp,
                'created_at': get_eastern_datetime(),
                'updated_at': get_eastern_datetime()
            }
            
            # Only add if we have at least a close price
            if price_record['close_price'] is not None:
                price_records.append(price_record)
                processed_count += 1
                
        except Exception as e:
            logger.error(f"Error processing v3 snapshot for {snapshot.get('ticker', 'unknown')}: {str(e)}")
            continue
    
    logger.info(f"Processed {processed_count} valid price records from v3 snapshot data")
    return price_records

def bulk_upsert_prices(session, engine, price_records):
    """
    Bulk upsert price records using PostgreSQL's ON CONFLICT functionality
    This will automatically overwrite existing records for the same ticker and date
    """
    if not price_records:
        logger.warning("No price records to upsert")
        return 0
    
    logger.info(f"Starting bulk upsert of {len(price_records)} price records...")
    
    try:
        # Use PostgreSQL's INSERT ... ON CONFLICT for efficient upserts
        stmt = insert(Price).values(price_records)
        
        # Define what to do on conflict (use the constraint name)
        stmt = stmt.on_conflict_do_update(
            constraint='_ticker_date_uc',
            set_=dict(
                open_price=stmt.excluded.open_price,
                high_price=stmt.excluded.high_price,
                low_price=stmt.excluded.low_price,
                close_price=stmt.excluded.close_price,
                volume=stmt.excluded.volume,
                last_traded_timestamp=stmt.excluded.last_traded_timestamp,
                updated_at=stmt.excluded.updated_at
            )
        )
        
        # Execute the bulk upsert
        result = session.execute(stmt)
        session.commit()
        
        logger.info(f"Successfully upserted {len(price_records)} price records")
        return len(price_records)
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error in bulk upsert: {str(e)}")
        return 0

def main():
    """Main function to fetch today's price data and update the database"""
    logger.info("=== Starting Daily Price Update ===")
    
    session, engine = init_db()
    
    try:
        # Step 1: Get existing tickers from our database
        existing_tickers = get_existing_tickers(session)
        if not existing_tickers:
            logger.error("No existing tickers found in database. Run initial data fetch first.")
            return
        
        existing_tickers_set = set(existing_tickers)
        
        # Step 2: Fetch today's market snapshot
        logger.info("Attempting to fetch full market snapshot...")
        snapshots = fetch_market_snapshot()
        
        price_records = []
        
        if snapshots:
            # Process v2 snapshot data
            price_records = process_v2_snapshot_data(snapshots, existing_tickers_set)
        else:
            # Fallback: fetch specific tickers
            logger.info("Full market snapshot failed, trying specific tickers...")
            snapshots = fetch_specific_tickers_snapshot(existing_tickers, batch_size=50)
            if snapshots:
                price_records = process_v3_snapshot_data(snapshots, existing_tickers_set)
        
        # Step 3: Bulk upsert the price data
        if price_records:
            upserted_count = bulk_upsert_prices(session, engine, price_records)
            logger.info(f"Daily price update completed. Updated {upserted_count} records for {get_eastern_date()}")
        else:
            logger.warning("No price records to update")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        session.rollback()
    finally:
        session.close()
        logger.info("=== Daily Price Update Completed ===")

if __name__ == "__main__":
    main() 