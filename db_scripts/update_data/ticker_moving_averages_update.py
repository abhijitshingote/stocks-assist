#!/usr/bin/env python3
"""
Ticker Moving Averages Update Script
Computes and stores daily SMAs and EMAs for all stocks.

This script computes:
- dma_50: 50-day simple moving average
- dma_200: 200-day simple moving average
- ema_10: 10-day exponential moving average
- ema_20: 20-day exponential moving average

Rows are only written for actively traded non-ETF/non-fund tickers, which is
what market_breadth_update.py counts against.
"""

import argparse
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import time
import pytz
import pandas as pd

# Add backend and db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Base, TickerMovingAverages
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'ticker_moving_averages_update'
logger = get_logger(SCRIPT_NAME)

# Load environment variables
load_dotenv()


def get_eastern_datetime():
    """Get current datetime in Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)


def init_db():
    """Initialize database connection"""
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment variables")

    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session(), engine


def get_last_processed_date(connection):
    """Get the last date that was processed in ticker_moving_averages table"""
    result = connection.execute(text("SELECT MAX(date) FROM ticker_moving_averages"))
    return result.scalar()


def compute_and_load_dmas(connection, start_date=None):
    """
    Compute 50/200-day SMAs for all dates and insert into ticker_moving_averages.
    Uses INSERT ... ON CONFLICT to handle updates for existing dates.
    """
    logger.info("Computing DMA data...")

    date_filter = ""
    if start_date:
        date_filter = f"AND dc.date > '{start_date}'"

    dma_query = f"""
    WITH eligible_tickers AS (
        SELECT t.ticker
        FROM tickers t
        WHERE t.is_actively_trading = TRUE
          AND t.is_etf = FALSE
          AND t.is_fund = FALSE
    ),
    dma_calc AS (
        SELECT
            o.ticker,
            o.date,
            ROUND(AVG(o.close) OVER (
                PARTITION BY o.ticker
                ORDER BY o.date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            )::numeric, 2) AS dma_50,
            ROUND(AVG(o.close) OVER (
                PARTITION BY o.ticker
                ORDER BY o.date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            )::numeric, 2) AS dma_200,
            COUNT(o.close) OVER (
                PARTITION BY o.ticker
                ORDER BY o.date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ) AS dma_50_count,
            COUNT(o.close) OVER (
                PARTITION BY o.ticker
                ORDER BY o.date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ) AS dma_200_count
        FROM ohlc o
    )
    INSERT INTO ticker_moving_averages (ticker, date, dma_50, dma_200, ema_10, ema_20)
    SELECT
        dc.ticker,
        dc.date,
        CASE WHEN dc.dma_50_count >= 50 THEN dc.dma_50 ELSE NULL END,
        CASE WHEN dc.dma_200_count >= 200 THEN dc.dma_200 ELSE NULL END,
        NULL,  -- ema_10 calculated separately via Python
        NULL   -- ema_20 calculated separately via Python
    FROM dma_calc dc
    JOIN eligible_tickers et ON et.ticker = dc.ticker
    WHERE 1=1
    {date_filter}
    ON CONFLICT (ticker, date)
    DO UPDATE SET
        dma_50 = EXCLUDED.dma_50,
        dma_200 = EXCLUDED.dma_200
    """

    connection.execute(text(dma_query))
    connection.commit()

    total_count = connection.execute(text("SELECT COUNT(*) FROM ticker_moving_averages")).scalar()
    date_count = connection.execute(text("SELECT COUNT(DISTINCT date) FROM ticker_moving_averages")).scalar()

    logger.info(f"ticker_moving_averages now has {total_count} rows across {date_count} trading days")
    return total_count, date_count


def compute_and_update_ema(connection, start_date=None):
    """
    Compute EMA-10 and EMA-20 for all stocks using pandas and update
    the ticker_moving_averages table.

    EMA formula: EMA_t = close_t * k + EMA_(t-1) * (1-k)
    where k = 2 / (period + 1)

    For EMA-10: k = 2/11 ≈ 0.1818
    For EMA-20: k = 2/21 ≈ 0.0952
    """
    logger.info("Computing EMA-10 and EMA-20...")

    date_filter = ""
    if start_date:
        # Need to go back further to calculate EMA properly (at least 200 days for warm-up)
        date_filter = f"AND date > '{start_date}'::date - INTERVAL '250 days'"

    ohlc_query = f"""
        SELECT ticker, date, close
        FROM ohlc
        WHERE close IS NOT NULL
        {date_filter}
        ORDER BY ticker, date
    """

    rows = connection.execute(text(ohlc_query)).fetchall()

    if not rows:
        logger.warning("No OHLC data found for EMA calculation")
        return 0

    df = pd.DataFrame(rows, columns=['ticker', 'date', 'close'])
    df['date'] = pd.to_datetime(df['date'])

    logger.info(f"Fetched {len(df)} OHLC records for EMA calculation")

    ema_records = []

    for ticker, group in df.groupby('ticker'):
        group = group.sort_values('date').copy()

        # span parameter: span = (2/k) - 1, so for k=2/(n+1), span = n
        group['ema_10'] = group['close'].ewm(span=10, adjust=False).mean().round(2)
        group['ema_20'] = group['close'].ewm(span=20, adjust=False).mean().round(2)

        if start_date:
            group = group[group['date'] > pd.to_datetime(start_date)]

        for _, row in group.iterrows():
            ema_records.append({
                'ticker': ticker,
                'date': row['date'].date(),
                'ema_10': row['ema_10'],
                'ema_20': row['ema_20']
            })

    logger.info(f"Computed {len(ema_records)} EMA records")

    batch_size = 5000
    updated_count = 0

    for i in range(0, len(ema_records), batch_size):
        batch = ema_records[i:i + batch_size]
        if not batch:
            continue

        values_list = ", ".join([
            f"('{r['ticker']}', '{r['date']}'::date, {r['ema_10']}, {r['ema_20']})"
            for r in batch
        ])

        update_query = f"""
            UPDATE ticker_moving_averages tma
            SET
                ema_10 = v.ema_10,
                ema_20 = v.ema_20
            FROM (VALUES {values_list}) AS v(ticker, date, ema_10, ema_20)
            WHERE tma.ticker = v.ticker AND tma.date = v.date
        """

        connection.execute(text(update_query))
        updated_count += len(batch)

        if (i + batch_size) % 50000 == 0:
            logger.info(f"Updated {updated_count} EMA records...")

    connection.commit()
    logger.info(f"Updated {updated_count} records with EMA values")
    return updated_count


def main():
    """Main function to update ticker_moving_averages table"""
    overall_start = time.time()

    parser = argparse.ArgumentParser(description='Compute daily SMAs and EMAs for all stocks')
    parser.add_argument('--full', action='store_true',
                        help='Recompute every date instead of only those after the last '
                             'processed one. Needed after deeper OHLC history is loaded, '
                             'since the incremental path only ever fills forward.')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("=== Starting Ticker Moving Averages Update ===")
    logger.info("=" * 60)

    session, engine = init_db()

    try:
        with engine.connect() as connection:
            last_date = None if args.full else get_last_processed_date(connection)

            if args.full:
                logger.info("Full recompute requested (--full)")

            if last_date:
                logger.info(f"Last processed date: {last_date}")
                logger.info("Running incremental update for new dates...")
                total_count, date_count = compute_and_load_dmas(connection, start_date=last_date)
                ema_count = compute_and_update_ema(connection, start_date=last_date)
            else:
                logger.info("Running full historical load...")
                total_count, date_count = compute_and_load_dmas(connection)
                ema_count = compute_and_update_ema(connection)

            logger.info(f"Successfully updated ticker_moving_averages: {total_count} records, {date_count} trading days, {ema_count} EMA updates")
            total_time = time.time() - overall_start
            write_summary(SCRIPT_NAME, 'SUCCESS', f'Updated {date_count} trading days, {ema_count} EMAs', total_count, duration_seconds=total_time)

    except Exception as e:
        logger.error(f"Error in ticker moving averages update: {str(e)}")
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
        raise
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== Ticker Moving Averages Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()
