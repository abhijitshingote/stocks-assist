"""
Seed index/ETF price history from Yahoo Finance (yfinance).

PURPOSE: Populate daily OHLC price data for market indices/ETFs.
TABLES UPDATED:
  - index_prices: Daily OHLC price data

Uses ETFs as proxies for indices:
  - SPY → S&P 500 proxy
  - QQQ → Nasdaq-100 proxy
  - IWM → Russell 2000 proxy

This script uses yfinance which is FREE and doesn't require an API key.
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import logging
import time
import pytz
import argparse

try:
    import yfinance as yf
except ImportError:
    print("Error: yfinance not installed. Run: pip install yfinance")
    sys.exit(1)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import IndexPrice, SyncMetadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# ETFs to track
ETFS = ['SPY', 'QQQ', 'IWM']


def get_eastern_now():
    """Get current datetime in US/Eastern timezone"""
    return datetime.now(pytz.timezone("US/Eastern"))


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


def download_price_history(symbol, days=1825):
    """Download historical price data from Yahoo Finance"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"  Downloading {days} days of price data from Yahoo Finance...")
        
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date.strftime('%Y-%m-%d'), 
                           end=end_date.strftime('%Y-%m-%d'))
        
        if df.empty:
            logger.warning(f"  No data returned from Yahoo Finance")
            return []
        
        price_data = []
        for date, row in df.iterrows():
            price_data.append({
                'date': date.date(),
                'open_price': parse_float(row.get('Open')),
                'high_price': parse_float(row.get('High')),
                'low_price': parse_float(row.get('Low')),
                'close_price': parse_float(row.get('Close')),
                'volume': parse_int(row.get('Volume', 0))
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


def format_duration(seconds):
    """Format duration"""
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

    parser = argparse.ArgumentParser(description='Seed ETF price history from Yahoo Finance')
    parser.add_argument('--days', type=int, default=1825, 
                       help='Days of history (default: 1825 = 5 years)')
    parser.add_argument('--symbols', type=str, nargs='+',
                       help='Specific ETFs (default: SPY QQQ IWM)')
    parser.add_argument('--delay', type=float, default=0.5,
                       help='Delay between API calls (default: 0.5s)')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Index/ETF Price Seeding from Yahoo Finance (yfinance)")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        symbols = args.symbols if args.symbols else ETFS
        
        logger.info(f"ETFs to process: {symbols}")
        logger.info(f"Days of history: {args.days}")
        logger.info("-" * 60)
        
        total_inserted = 0
        total_updated = 0
        
        for symbol in symbols:
            logger.info(f"\nProcessing {symbol}...")

            price_data = download_price_history(symbol, days=args.days)
            if price_data:
                inserted, updated = upsert_prices(session, symbol, price_data)
                total_inserted += inserted
                total_updated += updated
                
            time.sleep(args.delay)

        update_sync_metadata(session, 'index_prices_last_sync')

        total_prices = session.query(IndexPrice).count()
        
        logger.info("\n" + "=" * 60)
        logger.info("Results:")
        logger.info(f"  Total index_prices records: {total_prices}")
        logger.info(f"  New this run: {total_inserted}")
        logger.info(f"  Updated this run: {total_updated}")
        logger.info(f"  Time taken: {format_duration(time.time() - start_time)}")
        logger.info("=" * 60)
        logger.info("✅ Index price seeding completed!")

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted. Saving progress...")
        session.commit()
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    main()

