"""
Backfill analyst_estimates for tickers the main seeder dropped (HTTP 429 / empty).

Does not change the full-universe seeder. Only fetches tickers with 0 rows.
Retryable FMP failures (429/5xx) get extra passes.
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, AnalystEstimates, SyncMetadata
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration, ProgressTracker, get_test_ticker_limit
from db_scripts.fmp_utils import RateLimiter, bulk_upsert

SCRIPT_NAME = 'backfill_missing_analyst_estimates'
logger = get_logger(SCRIPT_NAME)
load_dotenv()

BASE_URL = 'https://financialmodelingprep.com/stable'
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
UPSERT_UPDATE_COLUMNS = [
    'revenue_avg', 'revenue_low', 'revenue_high',
    'ebitda_avg', 'ebit_avg', 'net_income_avg',
    'eps_avg', 'eps_low', 'eps_high',
    'num_analysts_revenue', 'num_analysts_eps',
]


def get_api_key():
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found")
    return api_key


def fetch_estimates(api_key, symbol, limit=10):
    """('ok', list) or ('retry', []). Empty list on 200 = genuine no coverage."""
    url = f'{BASE_URL}/analyst-estimates'
    params = {'symbol': symbol, 'period': 'annual', 'limit': limit, 'apikey': api_key}
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code in RETRYABLE_STATUS or not r.ok:
            return 'retry', []
        data = r.json()
        if isinstance(data, dict) or not isinstance(data, list):
            return 'retry', []
        return 'ok', data
    except Exception:
        return 'retry', []


def parse_float(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def parse_int(v):
    try:
        return int(float(v)) if v is not None else None
    except Exception:
        return None


def parse_date(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d').date() if s else None
    except Exception:
        return None


def process_estimates_data(ticker, estimates):
    if not isinstance(estimates, list):
        return []
    records_by_date = {}
    for e in estimates:
        if not isinstance(e, dict):
            continue
        date = parse_date(e.get('date'))
        if not date or date in records_by_date:
            continue
        records_by_date[date] = {
            'ticker': ticker,
            'date': date,
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
            'num_analysts_eps': parse_int(e.get('numAnalystsEps')),
        }
    return list(records_by_date.values())


def upsert_estimates(engine, records):
    if not records:
        return 0
    return bulk_upsert(
        engine, 'analyst_estimates', records,
        conflict_constraint='uq_analyst_est',
        conflict_columns=['ticker', 'date'],
        update_columns=UPSERT_UPDATE_COLUMNS,
    )


def missing_tickers(session, limit=None):
    test_limit = get_test_ticker_limit()
    if test_limit:
        logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
        limit = test_limit
    existing = session.query(AnalystEstimates.ticker).distinct()
    q = (
        session.query(Ticker.ticker)
        .filter(Ticker.is_actively_trading == True)
        .filter(~Ticker.ticker.in_(existing))
        .order_by(Ticker.market_cap.desc().nullslast())
    )
    if limit:
        q = q.limit(limit)
    return [t[0] for t in q.all()]


def fetch_pass(tickers, api_key, rate_limiter, engine, workers, batch_size, prefix):
    progress = ProgressTracker(len(tickers), logger, update_interval=50, prefix=prefix)
    all_records = []
    retry_tickers = []
    success_count = 0
    processed = 0
    total_records = 0
    db_batch_num = 0

    def worker(ticker):
        rate_limiter.acquire()
        status, data = fetch_estimates(api_key, ticker)
        if status != 'ok':
            return ticker, [], 'retry'
        return ticker, process_estimates_data(ticker, data), 'ok'

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(worker, t): t for t in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                _, records, status = future.result()
                if status == 'retry':
                    retry_tickers.append(ticker)
                else:
                    success_count += 1
                    if records:
                        all_records.extend(records)
            except Exception as e:
                retry_tickers.append(ticker)
                logger.debug(f"Error processing {ticker}: {e}")
            processed += 1
            if len(all_records) >= batch_size * 10:
                db_batch_num += 1
                written = upsert_estimates(engine, all_records)
                total_records += written
                progress.log_db_write(written, db_batch_num)
                all_records = []
            progress.update(
                processed,
                f"| Fetched: {success_count}, Retry: {len(retry_tickers)}, "
                f"Records: {total_records + len(all_records)}",
            )

    if all_records:
        db_batch_num += 1
        written = upsert_estimates(engine, all_records)
        total_records += written
        progress.log_db_write(written, db_batch_num)
    progress.finish(
        f"- Fetched: {success_count}, Retry: {len(retry_tickers)}, Records: {total_records}"
    )
    return retry_tickers, success_count, total_records


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description='Backfill missing analyst_estimates from FMP')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--workers', type=int, default=10)
    parser.add_argument('--batch-size', type=int, default=100)
    parser.add_argument('--retries', type=int, default=2)
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Backfill missing analyst estimates")
    logger.info("=" * 60)

    engine = create_engine(os.getenv('DATABASE_URL'))
    Session = sessionmaker(bind=engine)
    session = Session()
    api_key = get_api_key()

    try:
        tickers = missing_tickers(session, args.limit)
        logger.info(f"Missing tickers: {len(tickers)}")
        if not tickers:
            logger.info("Nothing to backfill.")
            write_summary(SCRIPT_NAME, 'SUCCESS', 'Nothing to backfill', 0,
                          duration_seconds=time.time() - start_time)
            return

        rate_limiter = RateLimiter(calls_per_minute=290)
        pending, success_count, total_records = fetch_pass(
            tickers, api_key, rate_limiter, engine, args.workers, args.batch_size,
            prefix="Backfill:",
        )
        for attempt in range(1, args.retries + 1):
            if not pending:
                break
            logger.info(f"Retry pass {attempt}/{args.retries}: {len(pending)} tickers")
            pending, recovered, extra = fetch_pass(
                pending, api_key, rate_limiter, engine, args.workers, args.batch_size,
                prefix=f"Backfill-retry{attempt}:",
            )
            success_count += recovered
            total_records += extra

        failed_count = len(pending)
        if failed_count:
            logger.warning(f"Still missing after retries: {failed_count} (sample: {', '.join(pending[:20])})")

        stmt = insert(SyncMetadata).values(
            key='analyst_estimates_backfill_sync', last_synced_at=datetime.utcnow()
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['key'], set_={'last_synced_at': datetime.utcnow()}
        )
        session.execute(stmt)
        session.commit()

        total = session.query(AnalystEstimates).count()
        elapsed = time.time() - start_time
        logger.info(f"  Filled: {success_count}, Failed: {failed_count}, Total in DB: {total}")
        logger.info(f"  Time: {format_duration(elapsed)}")
        write_summary(
            SCRIPT_NAME, 'SUCCESS',
            f'{success_count} tickers backfilled, {failed_count} still missing',
            total, duration_seconds=elapsed,
        )
    except KeyboardInterrupt:
        session.commit()
        write_summary(SCRIPT_NAME, 'INTERRUPTED', 'Progress saved',
                      duration_seconds=time.time() - start_time)
    except Exception as e:
        logger.error(f"Error: {e}")
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=time.time() - start_time)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()
