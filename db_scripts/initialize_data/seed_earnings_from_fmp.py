"""
Seed earnings table from FMP API.
Source: /stable/earnings?symbol=X
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

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, Earnings, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration, ProgressTracker, estimate_processing_time, get_test_ticker_limit

# Script name for logging
SCRIPT_NAME = 'seed_earnings_from_fmp'
logger = get_logger(SCRIPT_NAME)
load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found")
    return api_key


# format_duration is imported from db_scripts.logger


def fetch_earnings(api_key, symbol):
    url = f'{BASE_URL}/earnings'
    params = {'symbol': symbol, 'apikey': api_key}
    try:
        r = requests.get(url, params=params, timeout=30)
        return r.json() if r.ok else []
    except:
        return []


def parse_float(v):
    try: return float(v) if v is not None else None
    except: return None

def parse_int(v):
    try: return int(float(v)) if v is not None else None
    except: return None

def parse_date(s):
    try: return datetime.strptime(s, '%Y-%m-%d').date() if s else None
    except: return None


def upsert_earnings(session, ticker, earnings_list):
    count = 0
    for e in earnings_list:
        date = parse_date(e.get('date'))
        if not date: continue
        
        data = {
            'ticker': ticker, 'date': date,
            'eps_actual': parse_float(e.get('epsActual')),
            'eps_estimated': parse_float(e.get('epsEstimated')),
            'revenue_actual': parse_int(e.get('revenueActual')),
            'revenue_estimated': parse_int(e.get('revenueEstimated'))
        }
        try:
            stmt = insert(Earnings).values(**data)
            stmt = stmt.on_conflict_do_update(constraint='uq_earnings',
                set_={k: stmt.excluded[k] for k in data if k not in ['ticker', 'date']})
            session.execute(stmt)
            count += 1
        except: pass
    return count


def get_tickers(session, limit=None):
    # Check for test mode limit first
    test_limit = get_test_ticker_limit()
    if test_limit:
        logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
        limit = test_limit
    
    q = session.query(Ticker.ticker).filter(Ticker.is_actively_trading == True)
    q = q.order_by(Ticker.market_cap.desc().nullslast())
    if limit: q = q.limit(limit)
    return [t[0] for t in q.all()]


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description='Seed earnings from FMP')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--delay', type=float, default=0.15)
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Earnings Seeding")
    logger.info("=" * 60)

    engine = create_engine(os.getenv('DATABASE_URL'))
    Session = sessionmaker(bind=engine)
    session = Session()
    api_key = get_api_key()

    try:
        tickers = get_tickers(session, args.limit)
        logger.info(f"Tickers: {len(tickers)}")

        estimated_time = estimate_processing_time(SCRIPT_NAME, len(tickers))
        logger.info(f"Estimated time: ~{estimated_time:.1f} minutes")

        # Initialize progress tracker
        progress = ProgressTracker(len(tickers), logger, update_interval=50, prefix="Earnings:")

        total = 0
        for i, ticker in enumerate(tickers):
            earnings = fetch_earnings(api_key, ticker)
            total += upsert_earnings(session, ticker, earnings)
            if (i + 1) % 50 == 0:
                session.commit()
            progress.update(i + 1, f"| Records: {total}")
            time.sleep(args.delay)

        session.commit()
        progress.finish(f"- Total records: {total}")
        stmt = insert(SyncMetadata).values(key='earnings_sync', last_synced_at=datetime.utcnow())
        stmt = stmt.on_conflict_do_update(index_elements=['key'], set_={'last_synced_at': datetime.utcnow()})
        session.execute(stmt)
        session.commit()

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"  Records: {total}")
        logger.info(f"  Total in DB: {session.query(Earnings).count()}")
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ Earnings seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{len(tickers)} tickers processed', total, duration_seconds=elapsed)

    except KeyboardInterrupt:
        session.commit()
        logger.info("Interrupted, progress saved.")
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'INTERRUPTED', 'Progress saved', duration_seconds=elapsed)
    except Exception as e:
        logger.error(f"Error: {e}")
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=elapsed)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()

