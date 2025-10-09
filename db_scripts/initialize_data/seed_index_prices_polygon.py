"""
Seed Index and IndexPrice tables with data from Polygon.io API

This script:
1. Creates/updates index metadata in the 'index' table
2. Fetches historical price data from Polygon.io
3. Populates the 'index_price' table with daily price data

Indices/ETFs included: SPX (S&P 500), QQQ (Nasdaq 100), IWM (Russell 2000)
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from polygon import RESTClient
import logging
import pytz
import time
import requests

# Add backend directory to Python path to import models
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import Index, IndexPrice, get_eastern_datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Define indices to track with their Polygon/FMP symbols
INDICES = {
    'SPX': {
        'polygon_symbol': 'SPY',  # Use SPY ETF as proxy (I:SPX requires higher tier)
        'data_source': 'polygon',
        'name': 'S&P 500 (SPY)',
        'description': 'SPDR S&P 500 ETF Trust - Tracks the S&P 500 Index',
        'index_type': 'etf'
    },
    'QQQ': {
        'polygon_symbol': 'QQQ',  # ETFs use their ticker directly
        'data_source': 'polygon',
        'name': 'Nasdaq-100 (QQQ)',
        'description': 'Invesco QQQ Trust - Tracks the Nasdaq-100 Index',
        'index_type': 'etf'
    },
    'IWM': {
        'polygon_symbol': 'IWM',
        'data_source': 'polygon',
        'name': 'Russell 2000 (IWM)',
        'description': 'iShares Russell 2000 ETF - Tracks the Russell 2000 small cap index',
        'index_type': 'etf'
    }
}


def get_polygon_client():
    """Get Polygon API client"""
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise ValueError("POLYGON_API_KEY environment variable is not set")
    return RESTClient(api_key)


def upsert_index(session, symbol, index_data):
    """Upsert index metadata into the index table"""
    try:
        stmt = insert(Index).values(
            symbol=symbol,
            yahoo_symbol=index_data.get('yahoo_symbol', symbol),  # Use symbol as fallback
            name=index_data['name'],
            description=index_data['description'],
            index_type=index_data['index_type'],
            is_active=True,
            created_at=get_eastern_datetime(),
            updated_at=get_eastern_datetime()
        )

        # Update all fields except symbol on conflict
        update_dict = {
            'name': stmt.excluded.name,
            'description': stmt.excluded.description,
            'index_type': stmt.excluded.index_type,
            'is_active': stmt.excluded.is_active,
            'updated_at': stmt.excluded.updated_at
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol'],
            set_=update_dict
        )

        session.execute(stmt)
        session.commit()
        logger.info(f"✓ Upserted index metadata for {symbol}")
        return True

    except Exception as e:
        logger.error(f"Error upserting index {symbol}: {str(e)}")
        session.rollback()
        return False


def download_index_history_polygon(client, polygon_symbol, days=1825):
    """Download historical price data from Polygon API (default: 5 years for 200 DMA)"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"Downloading {days} days of data for {polygon_symbol} from Polygon...")
        
        # Get aggregates (daily bars)
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
        
        logger.info(f"✓ Downloaded {len(price_data)} rows from Polygon for {polygon_symbol}")
        return price_data
        
    except Exception as e:
        logger.error(f"Error downloading from Polygon for {polygon_symbol}: {str(e)}")
        return []


def download_index_history_fmp(fmp_symbol, days=1825):
    """Download historical price data from FMP API (default: 5 years for 200 DMA)"""
    try:
        api_key = os.getenv('FMP_API_KEY')
        if not api_key:
            raise ValueError("FMP_API_KEY environment variable is not set")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"Downloading {days} days of data for {fmp_symbol} from FMP...")
        
        # FMP historical price endpoint
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{fmp_symbol}"
        params = {
            'apikey': api_key,
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d')
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'historical' not in data or not data['historical']:
            logger.warning(f"No data returned from FMP for {fmp_symbol}")
            return []
        
        price_data = []
        for item in data['historical']:
            price_data.append({
                'date': datetime.strptime(item['date'], '%Y-%m-%d').date(),
                'open_price': item.get('open'),
                'high_price': item.get('high'),
                'low_price': item.get('low'),
                'close_price': item.get('close'),
                'volume': item.get('volume', 0)
            })
        
        # Sort by date (oldest first)
        price_data.sort(key=lambda x: x['date'])
        
        logger.info(f"✓ Downloaded {len(price_data)} rows from FMP for {fmp_symbol}")
        return price_data
        
    except Exception as e:
        logger.error(f"Error downloading from FMP for {fmp_symbol}: {str(e)}")
        return []


def upsert_index_prices(session, symbol, price_data):
    """Upsert index price data into the index_price table"""
    if not price_data:
        logger.warning(f"No price data to insert for {symbol}")
        return 0

    try:
        inserted_count = 0
        updated_count = 0
        
        for data in price_data:
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

            # Check if record exists before upserting
            existing = session.query(IndexPrice).filter(
                IndexPrice.symbol == symbol,
                IndexPrice.date == data['date']
            ).first()

            if existing:
                updated_count += 1
            else:
                inserted_count += 1

            # Update all fields except symbol and date on conflict
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

        session.commit()
        logger.info(f"✓ Processed {len(price_data)} price records for {symbol} ({inserted_count} new, {updated_count} updated)")
        return len(price_data)

    except Exception as e:
        logger.error(f"Error upserting price data for {symbol}: {str(e)}")
        session.rollback()
        return 0


def main():
    """Main function to seed index data from Polygon"""
    import argparse

    parser = argparse.ArgumentParser(description='Seed index/ETF data from Polygon API')
    parser.add_argument('--days', type=int, default=1825, 
                       help='Number of days of historical data to fetch (default: 1825 = 5 years for 200 DMA)')
    parser.add_argument('--symbols', type=str, nargs='+',
                       help='Specific symbols to update (default: all defined indices)')
    args = parser.parse_args()

    print(f"Starting index data seeding from Polygon API ({args.days} days)...")

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get Polygon client
        client = get_polygon_client()
        
        # Determine which indices to process
        symbols_to_process = args.symbols if args.symbols else INDICES.keys()
        
        total_prices = 0
        for symbol in symbols_to_process:
            if symbol not in INDICES:
                logger.warning(f"Unknown symbol {symbol}, skipping...")
                continue

            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {symbol} ({INDICES[symbol]['name']})")
            logger.info(f"{'='*60}")

            # Step 1: Upsert index metadata
            if not upsert_index(session, symbol, INDICES[symbol]):
                logger.error(f"Failed to upsert index metadata for {symbol}, skipping price data")
                continue

            # Step 2: Download price history from appropriate source
            data_source = INDICES[symbol].get('data_source', 'polygon')
            
            if data_source == 'fmp':
                fmp_symbol = INDICES[symbol].get('fmp_symbol', symbol)
                price_data = download_index_history_fmp(fmp_symbol, days=args.days)
            else:  # polygon
                polygon_symbol = INDICES[symbol]['polygon_symbol']
                price_data = download_index_history_polygon(client, polygon_symbol, days=args.days)

            # Step 3: Upsert price data
            if price_data:
                count = upsert_index_prices(session, symbol, price_data)
                total_prices += count
                
            # Rate limiting - be nice to Polygon API
            time.sleep(0.5)

        # Final statistics
        total_indices = session.query(Index).count()
        total_index_prices = session.query(IndexPrice).count()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"✅ Index seeding completed successfully!")
        logger.info(f"   Total indices in database: {total_indices}")
        logger.info(f"   Total index price records: {total_index_prices}")
        logger.info(f"   New/updated prices this run: {total_prices}")
        logger.info(f"{'='*60}")

    except Exception as e:
        logger.error(f"Error in index seeding process: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    main()

