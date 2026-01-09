"""
Seed ratios_ttm table from FMP API.

Uses:
- /stable/ratios-ttm?symbol=X - for P/E, ROE, margins, etc.

OPTIMIZED:
- Concurrent API calls with rate limiting (290/min)
- Bulk inserts instead of row-by-row

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
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, RatiosTTM, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration, ProgressTracker, get_test_ticker_limit
from db_scripts.fmp_utils import RateLimiter, bulk_upsert_simple

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


def process_ratios_data(ticker, ratios):
    """Convert API response to database record"""
    if not ratios:
        return None
    
    return {
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


def update_sync_metadata(session, key):
    stmt = insert(SyncMetadata).values(key=key, last_synced_at=datetime.utcnow())
    stmt = stmt.on_conflict_do_update(index_elements=['key'], set_={'last_synced_at': datetime.utcnow()})
    session.execute(stmt)
    session.commit()


def get_tickers(session, limit=None):
    test_limit = get_test_ticker_limit()
    if test_limit:
        logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
        limit = test_limit
    
    query = session.query(Ticker.ticker).filter(
        Ticker.is_actively_trading == True
    ).order_by(Ticker.market_cap.desc().nullslast())
    
    if limit:
        query = query.limit(limit)
    
    return [t[0] for t in query.all()]


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed ratios_ttm from FMP')
    parser.add_argument('--limit', type=int, default=None, help='Max tickers to process')
    parser.add_argument('--workers', type=int, default=10, help='Concurrent API workers (default: 10)')
    parser.add_argument('--batch-size', type=int, default=100, help='DB write batch size (default: 100)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Ratios TTM Seeding from FMP API (Optimized)")
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
        logger.info(f"Workers: {args.workers}")
        logger.info(f"Rate limit: 290 calls/min")
        
        estimated_minutes = len(tickers) / 290
        logger.info(f"Estimated time: ~{estimated_minutes:.1f} minutes")
        logger.info("-" * 60)
        
        rate_limiter = RateLimiter(calls_per_minute=290)
        progress = ProgressTracker(len(tickers), logger, update_interval=50, prefix="Ratios:")
        
        all_records = []
        success_count = 0
        failed_count = 0
        processed = 0
        
        def worker(ticker):
            rate_limiter.acquire()
            ratios = fetch_ratios_ttm(api_key, ticker)
            if ratios:
                record = process_ratios_data(ticker, ratios)
                return (ticker, record, True)
            return (ticker, None, False)
        
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(worker, t): t for t in tickers}
            
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    _, record, success = future.result()
                    if success and record:
                        all_records.append(record)
                        success_count += 1
                    elif not success:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.debug(f"Error processing {ticker}: {e}")
                
                processed += 1
                
                # Batch write to DB
                if len(all_records) >= args.batch_size:
                    bulk_upsert_simple(
                        engine, 'ratios_ttm', all_records,
                        primary_key='ticker',
                        update_columns=['gross_profit_margin', 'operating_profit_margin', 
                                       'net_profit_margin', 'pe_ratio', 'peg_ratio',
                                       'price_to_book', 'price_to_sales', 'price_to_free_cash_flow',
                                       'current_ratio', 'quick_ratio', 'cash_ratio',
                                       'debt_to_equity', 'debt_to_assets', 'asset_turnover',
                                       'inventory_turnover', 'receivables_turnover',
                                       'return_on_assets', 'return_on_equity', 'updated_at']
                    )
                    all_records = []
                
                progress.update(processed, f"| Saved: {success_count}")

        # Write remaining
        if all_records:
            bulk_upsert_simple(
                engine, 'ratios_ttm', all_records,
                primary_key='ticker',
                update_columns=['gross_profit_margin', 'operating_profit_margin', 
                               'net_profit_margin', 'pe_ratio', 'peg_ratio',
                               'price_to_book', 'price_to_sales', 'price_to_free_cash_flow',
                               'current_ratio', 'quick_ratio', 'cash_ratio',
                               'debt_to_equity', 'debt_to_assets', 'asset_turnover',
                               'inventory_turnover', 'receivables_turnover',
                               'return_on_assets', 'return_on_equity', 'updated_at']
            )

        progress.finish(f"- Saved: {success_count}, Failed: {failed_count}")
        
        update_sync_metadata(session, 'ratios_last_sync')
        
        total_ratios = session.query(RatiosTTM).count()
        elapsed = time.time() - start_time
        
        logger.info("=" * 60)
        logger.info(f"  Ratios TTM records: {success_count}")
        logger.info(f"  Failed fetches: {failed_count}")
        logger.info(f"  Total ratios TTM: {total_ratios}")
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        if elapsed > 0:
            logger.info(f"  Effective rate: {processed / (elapsed / 60):.1f} calls/min")
        logger.info("=" * 60)
        logger.info("✅ Ratios seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{success_count} ratios saved, {failed_count} failed', total_ratios, duration_seconds=elapsed)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted. Saving progress...")
        session.commit()
        update_sync_metadata(session, 'ratios_last_sync')
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
