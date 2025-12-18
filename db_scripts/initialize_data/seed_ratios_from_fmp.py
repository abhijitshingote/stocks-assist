"""
Seed ratios_ttm table from FMP API.

Uses:
- /stable/ratios-ttm?symbol=X - for P/E, ROE, margins, etc.

Reference: https://site.financialmodelingprep.com/developer/docs
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
from models import Ticker, RatiosTTM, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'seed_ratios_from_fmp'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


def fetch_ratios_ttm(api_key, symbol):
    """Fetch TTM ratios"""
    url = f'{BASE_URL}/ratios-ttm'
    params = {'symbol': symbol, 'apikey': api_key}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.ok:
            data = response.json()
            if data and len(data) > 0:
                return data[0]
        return None
    except:
        return None


def parse_float(value):
    try:
        return float(value) if value is not None else None
    except:
        return None


def upsert_ratios_ttm(session, ticker, ratios):
    """Upsert TTM ratios"""
    if not ratios:
        return False
    
    data = {
        'ticker': ticker,
        'gross_profit_margin': parse_float(ratios.get('grossProfitMarginTTM')),
        'operating_profit_margin': parse_float(ratios.get('operatingProfitMarginTTM')),
        'net_profit_margin': parse_float(ratios.get('netProfitMarginTTM')),
        'pe_ratio': parse_float(ratios.get('priceToEarningsRatioTTM')),
        'peg_ratio': parse_float(ratios.get('priceToEarningsGrowthRatioTTM')),
        'price_to_book': parse_float(ratios.get('priceToBookRatioTTM')),
        'price_to_sales': parse_float(ratios.get('priceToSalesRatioTTM')),
        'price_to_free_cash_flow': parse_float(ratios.get('priceToFreeCashFlowRatioTTM')),
        'current_ratio': parse_float(ratios.get('currentRatioTTM')),
        'quick_ratio': parse_float(ratios.get('quickRatioTTM')),
        'cash_ratio': parse_float(ratios.get('cashRatioTTM')),
        'debt_to_equity': parse_float(ratios.get('debtToEquityRatioTTM')),
        'debt_to_assets': parse_float(ratios.get('debtToAssetsRatioTTM')),
        'asset_turnover': parse_float(ratios.get('assetTurnoverTTM')),
        'inventory_turnover': parse_float(ratios.get('inventoryTurnoverTTM')),
        'receivables_turnover': parse_float(ratios.get('receivablesTurnoverTTM')),
        'return_on_assets': parse_float(ratios.get('returnOnAssetsTTM')),
        'return_on_equity': parse_float(ratios.get('returnOnEquityTTM')),
        'updated_at': datetime.utcnow()
    }
    
    try:
        sql = insert(RatiosTTM).values(**data)
        sql = sql.on_conflict_do_update(
            index_elements=['ticker'],
            set_={k: sql.excluded[k] for k in data.keys() if k != 'ticker'}
        )
        session.execute(sql)
        return True
    except Exception as e:
        logger.error(f"Error inserting ratios for {ticker}: {e}")
        return False


def update_sync_metadata(session, key):
    stmt = insert(SyncMetadata).values(key=key, last_synced_at=datetime.utcnow())
    stmt = stmt.on_conflict_do_update(index_elements=['key'], set_={'last_synced_at': datetime.utcnow()})
    session.execute(stmt)
    session.commit()


def get_tickers(session, limit=None):
    query = session.query(Ticker.ticker).filter(
        Ticker.is_actively_trading == True
    ).order_by(Ticker.market_cap.desc().nullslast())
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def process_ratios(session, api_key, tickers, delay=0.15, commit_every=50):
    """Fetch and process TTM ratios"""
    
    ratios_count = 0
    failed = 0
    
    for i, ticker in enumerate(tickers):
        ratios = fetch_ratios_ttm(api_key, ticker)
        if ratios and upsert_ratios_ttm(session, ticker, ratios):
            ratios_count += 1
        else:
            failed += 1
        
        if (i + 1) % commit_every == 0:
            session.commit()
            logger.info(f"  Progress: {i+1}/{len(tickers)} (ratios: {ratios_count})")
        
        time.sleep(delay)
    
    session.commit()
    return ratios_count, failed


# format_duration is imported from db_scripts.logger


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed ratios_ttm from FMP')
    parser.add_argument('--limit', type=int, default=None, help='Max tickers to process')
    parser.add_argument('--delay', type=float, default=0.15, help='Delay between API calls (default: 0.15s)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Ratios TTM Seeding from FMP API")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    api_key = get_api_key()
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        tickers = get_tickers(session, limit=args.limit)
        
        if not tickers:
            logger.info("No tickers to process.")
            return
        
        logger.info(f"Tickers to process: {len(tickers)}")
        logger.info(f"API delay: {args.delay}s")
        
        estimated = len(tickers) * args.delay / 60
        logger.info(f"Estimated time: ~{estimated:.1f} minutes")
        logger.info("-" * 60)
        
        ratios, failed = process_ratios(session, api_key, tickers, delay=args.delay)
        
        update_sync_metadata(session, 'ratios_last_sync')
        
        logger.info("=" * 60)
        logger.info("Results:")
        logger.info(f"  Ratios TTM records: {ratios}")
        logger.info(f"  Failed fetches: {failed}")
        
        total_ratios = session.query(RatiosTTM).count()
        logger.info(f"  Total ratios TTM: {total_ratios}")
        
        elapsed = time.time() - start_time
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ Ratios seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{ratios} ratios saved, {failed} failed', total_ratios)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted. Saving progress...")
        session.commit()
        update_sync_metadata(session, 'ratios_last_sync')
        write_summary(SCRIPT_NAME, 'INTERRUPTED', 'Progress saved')
    except Exception as e:
        logger.error(f"Error: {e}")
        session.rollback()
        write_summary(SCRIPT_NAME, 'FAILED', str(e))
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()
