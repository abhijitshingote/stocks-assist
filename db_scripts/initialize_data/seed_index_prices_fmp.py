"""
Seed index/ETF price history from FMP API.

PURPOSE: Populate daily OHLC price data for market indices/ETFs.
TABLES UPDATED:
  - index_prices: Daily OHLC price data

Uses ETFs as proxies since actual index prices require higher tier:
  - SPY → S&P 500 proxy
  - QQQ → Nasdaq-100 proxy
  - IWM → Russell 2000 proxy
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import time
import requests
import pytz
import argparse

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import IndexPrice, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'seed_index_prices_fmp'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'

# ETFs to track
ETFS = ['SPY', 'QQQ', 'IWM']


def get_eastern_now():
    """Get current datetime in US/Eastern timezone"""
    return datetime.now(pytz.timezone("US/Eastern"))


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


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


def download_price_history(symbol, api_key, days=1825):
    """Download historical price data from FMP API"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"  Downloading {days} days of price data...")
        
        url = f"{BASE_URL}/historical-price-eod/full"
        params = {
            'apikey': api_key,
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
            logger.warning(f"  No data returned from FMP")
            return []
        
        if not historical:
            logger.warning(f"  No data returned from FMP")
            return []
        
        price_data = []
        for item in historical:
            price_data.append({
                'date': datetime.strptime(item['date'], '%Y-%m-%d').date(),
                'open_price': parse_float(item.get('open')),
                'high_price': parse_float(item.get('high')),
                'low_price': parse_float(item.get('low')),
                'close_price': parse_float(item.get('close')),
                'volume': parse_int(item.get('volume', 0))
            })
        
        price_data.sort(key=lambda x: x['date'])
        logger.info(f"  Downloaded {len(price_data)} price records")
        return price_data
        
    except Exception as e:
        logger.error(f"  Error downloading prices: {str(e)}")
        return []


def upsert_prices(session, symbol, price_data):
    """Upsert price data into index_prices table"""
    if not price_data:
        return 0, 0

    try:
        inserted_count = 0
        updated_count = 0
        now = get_eastern_now()
        
        for data in price_data:
            existing = session.query(IndexPrice).filter(
                IndexPrice.symbol == symbol,
                IndexPrice.date == data['date']
            ).first()

            if existing:
                updated_count += 1
            else:
                inserted_count += 1

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

            stmt = stmt.on_conflict_do_update(
                constraint='uq_index_price',
                set_={
                    'open_price': stmt.excluded.open_price,
                    'high_price': stmt.excluded.high_price,
                    'low_price': stmt.excluded.low_price,
                    'close_price': stmt.excluded.close_price,
                    'volume': stmt.excluded.volume,
                    'updated_at': stmt.excluded.updated_at
                }
            )
            session.execute(stmt)

        session.commit()
        logger.info(f"  Processed: {inserted_count} new, {updated_count} updated")
        return inserted_count, updated_count

    except Exception as e:
        logger.error(f"  Error upserting prices: {str(e)}")
        session.rollback()
        return 0, 0


def update_sync_metadata(session, key):
    """Update sync metadata tracking"""
    stmt = insert(SyncMetadata).values(key=key, last_synced_at=datetime.utcnow())
    stmt = stmt.on_conflict_do_update(
        index_elements=['key'],
        set_={'last_synced_at': datetime.utcnow()}
    )
    session.execute(stmt)
    session.commit()


# format_duration is imported from db_scripts.logger


def main():
    start_time = time.time()

    parser = argparse.ArgumentParser(description='Seed ETF price history from FMP')
    parser.add_argument('--days', type=int, default=1825, 
                       help='Days of history (default: 1825 = 5 years)')
    parser.add_argument('--symbols', type=str, nargs='+',
                       help='Specific ETFs (default: SPY QQQ IWM)')
    parser.add_argument('--delay', type=float, default=0.5,
                       help='Delay between API calls (default: 0.5s)')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Index/ETF Price Seeding from FMP")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    api_key = get_api_key()

    try:
        symbols = args.symbols if args.symbols else ETFS
        
        logger.info(f"ETFs to process: {symbols}")
        logger.info(f"Days of history: {args.days}")
        logger.info("-" * 60)
        
        total_inserted = 0
        total_updated = 0
        
        for symbol in symbols:
            logger.info(f"\nProcessing {symbol}...")

            price_data = download_price_history(symbol, api_key, days=args.days)
            if price_data:
                inserted, updated = upsert_prices(session, symbol, price_data)
                total_inserted += inserted
                total_updated += updated
                
            time.sleep(args.delay)

        update_sync_metadata(session, 'index_prices_last_sync')

        total_prices = session.query(IndexPrice).count()
        
        elapsed = time.time() - start_time
        logger.info("\n" + "=" * 60)
        logger.info("Results:")
        logger.info(f"  Total index_prices records: {total_prices}")
        logger.info(f"  New this run: {total_inserted}")
        logger.info(f"  Updated this run: {total_updated}")
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ Index price seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{total_inserted} new, {total_updated} updated', total_prices, duration_seconds=elapsed)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted. Saving progress...")
        session.commit()
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'INTERRUPTED', 'Progress saved', duration_seconds=elapsed)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        session.rollback()
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=elapsed)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()
