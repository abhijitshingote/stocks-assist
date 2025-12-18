#!/usr/bin/env python3
"""
Historical RSI Update Script
Computes and stores daily RSI percentile rankings for all stocks.

This script computes:
- rsi_global: RSI percentile rank (1-100) across all stocks for each date
- rsi_mktcap: RSI percentile rank (1-100) within market cap bucket for each date

The RSI is computed as a weighted relative strength vs SPY:
- 40% weight on 4-day return difference
- 40% weight on 13-day return difference
- 20% weight on 20-day return difference
"""

import os
import sys
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import time
import pytz

# Add backend and db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Base, HistoricalRSI
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'historical_rsi_update'
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
    """Get the last date that was processed in historical_rsi table"""
    result = connection.execute(text("SELECT MAX(date) FROM historical_rsi"))
    last_date = result.scalar()
    return last_date


def get_available_dates(connection, after_date=None):
    """Get list of dates available in OHLC that have SPY data"""
    query = """
        SELECT DISTINCT o.date
        FROM ohlc o
        JOIN index_prices ip ON o.date = ip.date AND ip.symbol = 'SPY'
        WHERE 1=1
    """
    if after_date:
        query += f" AND o.date > '{after_date}'"
    query += " ORDER BY o.date"
    
    result = connection.execute(text(query))
    return [row[0] for row in result.fetchall()]


def compute_and_load_historical_rsi(connection, start_date=None):
    """
    Compute historical RSI for all dates and insert into historical_rsi table.
    Uses INSERT ... ON CONFLICT to handle updates for existing dates.
    """
    logger.info("Computing historical RSI data...")
    
    # Build date filter if needed
    date_filter = ""
    if start_date:
        date_filter = f"AND rs.date > '{start_date}'"
    
    # The comprehensive SQL query to compute historical RSI
    rsi_query = f"""
    WITH stock_returns AS (
        SELECT
            o.ticker,
            o.date,
            (o.close / LAG(o.close, 4)  OVER (PARTITION BY o.ticker ORDER BY o.date) - 1) AS ret_4,
            (o.close / LAG(o.close, 13) OVER (PARTITION BY o.ticker ORDER BY o.date) - 1) AS ret_13,
            (o.close / LAG(o.close, 20) OVER (PARTITION BY o.ticker ORDER BY o.date) - 1) AS ret_20
        FROM ohlc o
    ),
    spy_returns AS (
        SELECT
            ip.date,
            (ip.close_price / LAG(ip.close_price, 4)  OVER (ORDER BY ip.date) - 1) AS ret_4,
            (ip.close_price / LAG(ip.close_price, 13) OVER (ORDER BY ip.date) - 1) AS ret_13,
            (ip.close_price / LAG(ip.close_price, 20) OVER (ORDER BY ip.date) - 1) AS ret_20
        FROM index_prices ip
        WHERE ip.symbol = 'SPY'
    ),
    rel_strength AS (
        SELECT
            s.ticker,
            s.date,
            0.4 * (s.ret_4  - sp.ret_4) +
            0.4 * (s.ret_13 - sp.ret_13) +
            0.2 * (s.ret_20 - sp.ret_20) AS rel_strength_raw
        FROM stock_returns s
        JOIN spy_returns sp ON sp.date = s.date
    ),
    market_cap_bucket AS (
        SELECT
            t.ticker,
            t.market_cap,
            CASE
                WHEN t.market_cap < 200000000 THEN 'micro'
                WHEN t.market_cap < 2000000000 THEN 'small'
                WHEN t.market_cap < 20000000000 THEN 'mid'
                WHEN t.market_cap < 100000000000 THEN 'large'
                ELSE 'mega'
            END AS mktcap_bucket
        FROM tickers t
        WHERE t.is_actively_trading = TRUE
          AND t.is_etf = FALSE
          AND t.is_fund = FALSE
    ),
    rsi_ranked AS (
        SELECT
            rs.ticker,
            rs.date,
            mcb.market_cap,
            mcb.mktcap_bucket,
            NTILE(100) OVER (
                PARTITION BY rs.date
                ORDER BY rs.rel_strength_raw
            ) AS rsi_global,
            NTILE(100) OVER (
                PARTITION BY rs.date, mcb.mktcap_bucket
                ORDER BY rs.rel_strength_raw
            ) AS rsi_mktcap
        FROM rel_strength rs
        JOIN market_cap_bucket mcb ON rs.ticker = mcb.ticker
        WHERE rs.rel_strength_raw IS NOT NULL
        {date_filter}
    )
    INSERT INTO historical_rsi (ticker, date, rsi_global, rsi_mktcap)
    SELECT
        ticker,
        date,
        rsi_global,
        rsi_mktcap
    FROM rsi_ranked
    ON CONFLICT (ticker, date) 
    DO UPDATE SET 
        rsi_global = EXCLUDED.rsi_global,
        rsi_mktcap = EXCLUDED.rsi_mktcap
    """
    
    result = connection.execute(text(rsi_query))
    connection.commit()
    
    # Get count of rows in table
    count_result = connection.execute(text("SELECT COUNT(*) FROM historical_rsi"))
    total_count = count_result.scalar()
    
    # Get count of distinct dates
    date_count_result = connection.execute(text("SELECT COUNT(DISTINCT date) FROM historical_rsi"))
    date_count = date_count_result.scalar()
    
    logger.info(f"Historical RSI table now has {total_count} rows across {date_count} trading days")
    return total_count, date_count


def main():
    """Main function to update historical_rsi table"""
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting Historical RSI Update ===")
    logger.info("=" * 60)
    
    session, engine = init_db()
    
    try:
        with engine.connect() as connection:
            # Check if we should do incremental update
            last_date = get_last_processed_date(connection)
            
            if last_date:
                logger.info(f"Last processed date: {last_date}")
                logger.info("Running incremental update for new dates...")
                total_count, date_count = compute_and_load_historical_rsi(connection, start_date=last_date)
            else:
                logger.info("No existing data found. Running full historical load...")
                total_count, date_count = compute_and_load_historical_rsi(connection)
            
            logger.info(f"Successfully updated historical_rsi: {total_count} records, {date_count} trading days")
            total_time = time.time() - overall_start
            write_summary(SCRIPT_NAME, 'SUCCESS', f'Updated {date_count} trading days', total_count, duration_seconds=total_time)
            
    except Exception as e:
        logger.error(f"Error in historical RSI update: {str(e)}")
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
        raise
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== Historical RSI Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()

