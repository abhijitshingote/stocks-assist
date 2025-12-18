"""
Seed analyst_estimates table from FMP API.
Source: /stable/analyst-estimates?symbol=X&period=annual
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
from models import Ticker, AnalystEstimates, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'seed_analyst_estimates_from_fmp'
logger = get_logger(SCRIPT_NAME)
load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found")
    return api_key


# format_duration is imported from db_scripts.logger


def fetch_analyst_estimates(api_key, symbol, limit=10):
    url = f'{BASE_URL}/analyst-estimates'
    params = {'symbol': symbol, 'period': 'annual', 'limit': limit, 'apikey': api_key}
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


def upsert_estimates(session, ticker, estimates):
    count = 0
    for e in estimates:
        date = parse_date(e.get('date'))
        if not date: continue
        
        data = {
            'ticker': ticker, 'date': date,
            'revenue_avg': parse_int(e.get('revenueAvg')),
            'revenue_low': parse_int(e.get('revenueLow')),
            'revenue_high': parse_int(e.get('revenueHigh')),
            'ebitda_avg': parse_int(e.get('ebitdaAvg')),
            'ebit_avg': parse_int(e.get('ebitAvg')),
            'net_income_avg': parse_int(e.get('netIncomeAvg')),
            'eps_avg': parse_float(e.get('epsAvg')),
            'eps_low': parse_float(e.get('epsLow')),
            'eps_high': parse_float(e.get('epsHigh')),
            'num_analysts_revenue': parse_int(e.get('numAnalystsRevenue')),
            'num_analysts_eps': parse_int(e.get('numAnalystsEps'))
        }
        try:
            stmt = insert(AnalystEstimates).values(**data)
            stmt = stmt.on_conflict_do_update(constraint='uq_analyst_est',
                set_={k: stmt.excluded[k] for k in data if k not in ['ticker', 'date']})
            session.execute(stmt)
            count += 1
        except: pass
    return count


def get_tickers(session, limit=None):
    q = session.query(Ticker.ticker).filter(Ticker.is_actively_trading == True)
    q = q.order_by(Ticker.market_cap.desc().nullslast())
    if limit: q = q.limit(limit)
    return [t[0] for t in q.all()]


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description='Seed analyst_estimates from FMP')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--delay', type=float, default=0.15)
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Analyst Estimates Seeding")
    logger.info("=" * 60)

    engine = create_engine(os.getenv('DATABASE_URL'))
    Session = sessionmaker(bind=engine)
    session = Session()
    api_key = get_api_key()

    try:
        tickers = get_tickers(session, args.limit)
        logger.info(f"Tickers: {len(tickers)}")
        
        total = 0
        for i, ticker in enumerate(tickers):
            estimates = fetch_analyst_estimates(api_key, ticker)
            total += upsert_estimates(session, ticker, estimates)
            if (i + 1) % 50 == 0:
                session.commit()
                logger.info(f"  Progress: {i+1}/{len(tickers)}")
            time.sleep(args.delay)
        
        session.commit()
        stmt = insert(SyncMetadata).values(key='analyst_estimates_sync', last_synced_at=datetime.utcnow())
        stmt = stmt.on_conflict_do_update(index_elements=['key'], set_={'last_synced_at': datetime.utcnow()})
        session.execute(stmt)
        session.commit()

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"  Records: {total}")
        logger.info(f"  Total in DB: {session.query(AnalystEstimates).count()}")
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ Analyst estimates seeding completed!")
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

