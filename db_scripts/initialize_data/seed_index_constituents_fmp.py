"""
Seed index constituents from FMP API.

PURPOSE: Populate which stocks belong to which index.
TABLES UPDATED:
  - indices: Index definitions (DJI, SPX, NDX)
  - index_components: Mapping of index → ticker

This script fetches index constituents from FMP API endpoints:
  - /stable/dowjones-constituent
  - /stable/sp500-constituent  
  - /stable/nasdaq-constituent

Run AFTER seed_tickers_from_fmp.py (needs tickers table populated).
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
import logging
import argparse
import pytz

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import Ticker, Index, IndexComponents, SyncMetadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'

# Index definitions with their FMP API endpoints
INDEX_DEFINITIONS = {
    'DJI': {
        'name': 'Dow Jones Industrial Average',
        'endpoint': 'dowjones-constituent'
    },
    'SPX': {
        'name': 'S&P 500',
        'endpoint': 'sp500-constituent'
    },
    'NDX': {
        'name': 'NASDAQ 100',
        'endpoint': 'nasdaq-constituent'
    }
}


def get_eastern_now():
    """Get current datetime in US/Eastern timezone"""
    return datetime.now(pytz.timezone("US/Eastern"))


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


def fetch_index_constituents(api_key, endpoint):
    """Fetch index constituents from FMP API"""
    url = f'{BASE_URL}/{endpoint}'
    params = {'apikey': api_key}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict) and 'symbol' in data[0]:
                return [item['symbol'] for item in data]
        
        logger.warning(f"Unexpected response format from {endpoint}")
        return None
    except Exception as e:
        logger.error(f"Error fetching from {endpoint}: {e}")
        return None


def upsert_index(session, symbol, name):
    """Upsert an index definition"""
    try:
        now = get_eastern_now()
        stmt = insert(Index).values(
            symbol=symbol,
            name=name,
            index_type='index',
            is_active=True,
            created_at=now,
            updated_at=now
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol'],
            set_={'name': name, 'updated_at': now}
        )
        session.execute(stmt)
        session.commit()
        return True
    except Exception as e:
        logger.error(f"Error upserting index {symbol}: {e}")
        session.rollback()
        return False


def upsert_index_components(session, index_symbol, tickers):
    """Upsert index components, only for tickers that exist in tickers table"""
    if not tickers:
        return 0, 0
    
    # Get existing tickers in our database
    existing_tickers = set(
        t[0] for t in session.query(Ticker.ticker).filter(
            Ticker.ticker.in_(tickers)
        ).all()
    )
    
    added = 0
    skipped = 0
    
    for ticker in tickers:
        if ticker not in existing_tickers:
            skipped += 1
            continue
        
        try:
            stmt = insert(IndexComponents).values(
                index_symbol=index_symbol,
                ticker=ticker
            )
            stmt = stmt.on_conflict_do_nothing(constraint='uq_index_component')
            result = session.execute(stmt)
            if result.rowcount > 0:
                added += 1
        except Exception as e:
            logger.error(f"Error adding {ticker} to {index_symbol}: {e}")
    
    session.commit()
    return added, skipped


def update_sync_metadata(session, key):
    """Update sync metadata"""
    stmt = insert(SyncMetadata).values(key=key, last_synced_at=datetime.utcnow())
    stmt = stmt.on_conflict_do_update(
        index_elements=['key'],
        set_={'last_synced_at': datetime.utcnow()}
    )
    session.execute(stmt)
    session.commit()


def seed_index(session, api_key, index_symbol):
    """Seed a single index and its constituents"""
    if index_symbol not in INDEX_DEFINITIONS:
        logger.error(f"Unknown index: {index_symbol}")
        return False
    
    index_def = INDEX_DEFINITIONS[index_symbol]
    logger.info(f"Processing {index_symbol} ({index_def['name']})...")
    
    # Fetch constituents from FMP API
    components = fetch_index_constituents(api_key, index_def['endpoint'])
    
    if not components:
        logger.error(f"  Failed to fetch constituents for {index_symbol}")
        return False
    
    logger.info(f"  Fetched {len(components)} constituents from FMP API")
    
    # Upsert index
    if not upsert_index(session, index_symbol, index_def['name']):
        return False
    
    # Upsert components
    added, skipped = upsert_index_components(session, index_symbol, components)
    logger.info(f"  Added: {added}, Skipped (not in tickers table): {skipped}")
    
    return True


def format_duration(seconds):
    """Format duration in hours, minutes, seconds"""
    hrs = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hrs > 0:
        return f"{hrs}h {mins}m {secs}s"
    elif mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed index constituents from FMP API')
    parser.add_argument('--index', type=str, default=None,
                        help='Specific index to seed (DJI, SPX, NDX). Default: all')
    parser.add_argument('--delay', type=float, default=0.3,
                        help='Delay between API calls (default: 0.3s)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Index Constituents Seeding from FMP")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    api_key = get_api_key()
    
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        indices_to_process = [args.index] if args.index else list(INDEX_DEFINITIONS.keys())
        
        for index_symbol in indices_to_process:
            seed_index(session, api_key, index_symbol)
            time.sleep(args.delay)
            print()
        
        # Update sync metadata
        update_sync_metadata(session, 'index_constituents_last_sync')
        
        logger.info("=" * 60)
        logger.info("Summary:")
        total_indices = session.query(Index).count()
        total_components = session.query(IndexComponents).count()
        logger.info(f"  Total indices: {total_indices}")
        logger.info(f"  Total index components: {total_components}")
        
        # Show breakdown per index
        for idx in session.query(Index).all():
            count = session.query(IndexComponents).filter(
                IndexComponents.index_symbol == idx.symbol
            ).count()
            if count > 0:
                logger.info(f"    {idx.symbol}: {count} components")
        
        elapsed = time.time() - start_time
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ Index constituents seeding completed!")

    except Exception as e:
        logger.error(f"Error: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    main()
