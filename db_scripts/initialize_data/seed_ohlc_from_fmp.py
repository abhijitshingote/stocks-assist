"""
Seed OHLC (price history) table from FMP Historical Price API.

Uses the /stable/historical-price-eod/full endpoint to fetch daily OHLC data.

Reference: https://site.financialmodelingprep.com/developer/docs
"""

from datetime import datetime, timedelta
import os
import sys
import time
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import argparse

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, OHLC, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'seed_ohlc_from_fmp'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


def fetch_historical_prices(api_key, symbol, from_date=None, to_date=None):
    """Fetch historical OHLC data for a symbol"""
    url = f'{BASE_URL}/historical-price-eod/full'
    params = {
        'symbol': symbol,
        'apikey': api_key
    }
    
    if from_date:
        params['from'] = from_date.strftime('%Y-%m-%d')
    if to_date:
        params['to'] = to_date.strftime('%Y-%m-%d')
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.ok:
            data = response.json()
            return data if isinstance(data, list) else []
        return []
    except Exception as e:
        logger.error(f"Error fetching prices for {symbol}: {e}")
        return []


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


def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        return None


def upsert_ohlc_batch(session, ohlc_batch):
    """Upsert OHLC records"""
    if not ohlc_batch:
        return 0
    
    try:
        for data in ohlc_batch:
            stmt = insert(OHLC).values(**data)
            stmt = stmt.on_conflict_do_update(
                constraint='uq_ohlc',
                set_={k: stmt.excluded[k] for k in data.keys() if k not in ['ticker', 'date']}
            )
            session.execute(stmt)
        session.commit()
        return len(ohlc_batch)
    except Exception as e:
        logger.error(f"Error upserting OHLC: {e}")
        session.rollback()
        return 0


def update_sync_metadata(session, key, timestamp=None):
    """Update sync metadata tracking"""
    if timestamp is None:
        timestamp = datetime.utcnow()
    
    stmt = insert(SyncMetadata).values(key=key, last_synced_at=timestamp)
    stmt = stmt.on_conflict_do_update(
        index_elements=['key'],
        set_={'last_synced_at': timestamp}
    )
    session.execute(stmt)
    session.commit()


def get_tickers_for_ohlc(session, limit=None, min_market_cap=None):
    """Get tickers to fetch OHLC for, ordered by market cap"""
    query = session.query(Ticker.ticker).filter(
        Ticker.is_actively_trading == True
    ).order_by(Ticker.market_cap.desc().nullslast())
    
    if min_market_cap:
        query = query.filter(Ticker.market_cap >= min_market_cap)
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def get_last_ohlc_date(session, ticker):
    """Get the last OHLC date for a ticker"""
    result = session.query(func.max(OHLC.date)).filter(OHLC.ticker == ticker).scalar()
    return result


def process_ohlc(session, api_key, tickers, days_back=365, delay=0.15, commit_every=10):
    """Fetch and process OHLC for tickers"""
    
    records_inserted = 0
    tickers_processed = 0
    tickers_failed = 0
    
    to_date = datetime.now().date()
    default_from_date = to_date - timedelta(days=days_back)
    
    for i, ticker in enumerate(tickers):
        # Check last date we have for this ticker
        last_date = get_last_ohlc_date(session, ticker)
        
        if last_date:
            # Fetch only new data (from day after last date)
            from_date = last_date + timedelta(days=1)
            if from_date >= to_date:
                # Already up to date
                tickers_processed += 1
                continue
        else:
            from_date = default_from_date
        
        # Fetch prices
        prices = fetch_historical_prices(api_key, ticker, from_date, to_date)
        
        if not prices:
            tickers_failed += 1
            time.sleep(delay)
            continue
        
        # Build OHLC records
        ohlc_batch = []
        for p in prices:
            date = parse_date(p.get('date'))
            if not date:
                continue
            
            ohlc_batch.append({
                'ticker': ticker,
                'date': date,
                'open': parse_float(p.get('open')),
                'high': parse_float(p.get('high')),
                'low': parse_float(p.get('low')),
                'close': parse_float(p.get('close')),
                'volume': parse_int(p.get('volume'))
            })
        
        # Upsert
        count = upsert_ohlc_batch(session, ohlc_batch)
        records_inserted += count
        tickers_processed += 1
        
        if (i + 1) % commit_every == 0:
            logger.info(f"  Progress: {i+1}/{len(tickers)} tickers, {records_inserted} records")
        
        time.sleep(delay)
    
    return tickers_processed, records_inserted, tickers_failed


# format_duration is imported from db_scripts.logger


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed OHLC table from FMP Historical Price API')
    parser.add_argument('--limit', type=int, default=None,
                        help='Max tickers to process (default: all)')
    parser.add_argument('--days', type=int, default=365,
                        help='Days of history to fetch for new tickers (default: 365)')
    parser.add_argument('--delay', type=float, default=0.15,
                        help='Delay between API calls (default: 0.15s)')
    parser.add_argument('--min-market-cap', type=int, default=None,
                        help='Minimum market cap filter (e.g., 1000000000 for $1B)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("OHLC Seeding from FMP Historical Price API")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    api_key = get_api_key()
    
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        tickers = get_tickers_for_ohlc(session, limit=args.limit, min_market_cap=args.min_market_cap)
        
        if not tickers:
            logger.info("No tickers to process.")
            return
        
        logger.info(f"Tickers to process: {len(tickers)}")
        logger.info(f"Days of history: {args.days}")
        logger.info(f"API delay: {args.delay}s")
        
        estimated_time = len(tickers) * args.delay / 60
        logger.info(f"Estimated time: ~{estimated_time:.1f} minutes")
        logger.info("-" * 60)
        
        processed, inserted, failed = process_ohlc(
            session, api_key, tickers,
            days_back=args.days,
            delay=args.delay
        )
        
        # Update sync metadata
        update_sync_metadata(session, 'ohlc_last_sync')
        
        logger.info("=" * 60)
        logger.info("Results:")
        logger.info(f"  Tickers processed: {processed}")
        logger.info(f"  Records inserted: {inserted}")
        logger.info(f"  Failed fetches: {failed}")
        
        total_ohlc = session.query(OHLC).count()
        logger.info(f"  Total OHLC records: {total_ohlc}")
        
        elapsed = time.time() - start_time
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ OHLC seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{processed} tickers processed, {inserted} records inserted', total_ohlc, duration_seconds=elapsed)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted. Saving progress...")
        session.commit()
        update_sync_metadata(session, 'ohlc_last_sync')
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'INTERRUPTED', 'Progress saved', duration_seconds=elapsed)
    except Exception as e:
        logger.error(f"Error: {e}")
        session.rollback()
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=elapsed)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()

