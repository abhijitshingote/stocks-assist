#!/usr/bin/env python3
"""
RSI Indices Update Script
Truncates and reloads the rsi_indices table with RSI rankings within index universes.

This script computes RSI percentile rankings for stocks within:
- SPX (S&P 500)
- NDX (NASDAQ 100)
- DJI (Dow Jones Industrial Average)

The RSI is computed as a weighted relative strength vs SPY, then ranked within
each index universe to give an apples-to-apples comparison.

Join with stock_metrics table to get additional stock data (price, returns, etc.)
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
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'rsi_indices_update'
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
    Session = sessionmaker(bind=engine)
    return Session(), engine


def truncate_rsi_indices(connection):
    """Truncate the rsi_indices table"""
    logger.info("Truncating rsi_indices table...")
    connection.execute(text("TRUNCATE TABLE rsi_indices"))
    logger.info("rsi_indices table truncated")


def compute_and_load_rsi_indices(connection):
    """
    Compute RSI rankings within each index universe and insert into rsi_indices table.
    """
    logger.info("Computing RSI indices metrics...")
    
    metrics_query = """
    WITH latest_date AS (
        SELECT MAX(date) as max_date FROM ohlc
    ),
    -- Compute stock returns (same logic as stock_metrics)
    stock_returns AS (
        SELECT
            o.ticker,
            o.date,
            (o.close / NULLIF(LAG(o.close, 4) OVER (PARTITION BY o.ticker ORDER BY o.date), 0) - 1) AS ret_4,
            (o.close / NULLIF(LAG(o.close, 13) OVER (PARTITION BY o.ticker ORDER BY o.date), 0) - 1) AS ret_13,
            (o.close / NULLIF(LAG(o.close, 20) OVER (PARTITION BY o.ticker ORDER BY o.date), 0) - 1) AS ret_20
        FROM ohlc o
    ),
    -- Compute SPY returns
    spy_returns AS (
        SELECT
            ip.date,
            (ip.close_price / NULLIF(LAG(ip.close_price, 4) OVER (ORDER BY ip.date), 0) - 1) AS ret_4,
            (ip.close_price / NULLIF(LAG(ip.close_price, 13) OVER (ORDER BY ip.date), 0) - 1) AS ret_13,
            (ip.close_price / NULLIF(LAG(ip.close_price, 20) OVER (ORDER BY ip.date), 0) - 1) AS ret_20
        FROM index_prices ip
        WHERE ip.symbol = 'SPY'
    ),
    -- Weighted relative return vs SPY
    rel_strength AS (
        SELECT
            s.ticker,
            s.date,
            0.4 * (s.ret_4 - sp.ret_4) +
            0.4 * (s.ret_13 - sp.ret_13) +
            0.2 * (s.ret_20 - sp.ret_20) AS rel_strength_raw
        FROM stock_returns s
        JOIN spy_returns sp ON sp.date = s.date
        WHERE s.ret_4 IS NOT NULL 
          AND s.ret_13 IS NOT NULL 
          AND s.ret_20 IS NOT NULL
          AND sp.ret_4 IS NOT NULL
    ),
    -- Get index memberships
    index_membership AS (
        SELECT
            t.ticker,
            BOOL_OR(ic.index_symbol = 'SPX') AS is_spx,
            BOOL_OR(ic.index_symbol = 'NDX') AS is_ndx,
            BOOL_OR(ic.index_symbol = 'DJI') AS is_dji
        FROM tickers t
        JOIN index_components ic ON t.ticker = ic.ticker
        WHERE t.is_actively_trading = TRUE
          AND t.is_etf = FALSE
          AND t.is_fund = FALSE
          AND ic.index_symbol IN ('SPX', 'NDX', 'DJI')
        GROUP BY t.ticker
    ),
    -- Latest relative strength with index membership
    latest_rel_strength AS (
        SELECT
            rs.ticker,
            rs.rel_strength_raw,
            im.is_spx,
            im.is_ndx,
            im.is_dji
        FROM rel_strength rs
        JOIN index_membership im ON rs.ticker = im.ticker
        WHERE rs.date = (SELECT max_date FROM latest_date)
    ),
    -- RSI within SPX
    rsi_spx AS (
        SELECT
            ticker,
            NTILE(100) OVER (ORDER BY rel_strength_raw) AS rsi_spx
        FROM latest_rel_strength
        WHERE is_spx = TRUE
    ),
    -- RSI within NDX
    rsi_ndx AS (
        SELECT
            ticker,
            NTILE(100) OVER (ORDER BY rel_strength_raw) AS rsi_ndx
        FROM latest_rel_strength
        WHERE is_ndx = TRUE
    ),
    -- RSI within DJI
    rsi_dji AS (
        SELECT
            ticker,
            NTILE(100) OVER (ORDER BY rel_strength_raw) AS rsi_dji
        FROM latest_rel_strength
        WHERE is_dji = TRUE
    )
    INSERT INTO rsi_indices (
        ticker,
        is_spx,
        is_ndx,
        is_dji,
        rsi_spx,
        rsi_ndx,
        rsi_dji,
        updated_at
    )
    SELECT 
        im.ticker,
        im.is_spx,
        im.is_ndx,
        im.is_dji,
        rs_spx.rsi_spx,
        rs_ndx.rsi_ndx,
        rs_dji.rsi_dji,
        CURRENT_TIMESTAMP as updated_at
    FROM index_membership im
    LEFT JOIN rsi_spx rs_spx ON im.ticker = rs_spx.ticker
    LEFT JOIN rsi_ndx rs_ndx ON im.ticker = rs_ndx.ticker
    LEFT JOIN rsi_dji rs_dji ON im.ticker = rs_dji.ticker
    """
    
    result = connection.execute(text(metrics_query))
    connection.commit()
    
    # Get count of inserted rows
    count_result = connection.execute(text("SELECT COUNT(*) FROM rsi_indices"))
    count = count_result.scalar()
    
    # Get breakdown by index
    spx_count = connection.execute(text("SELECT COUNT(*) FROM rsi_indices WHERE is_spx = TRUE")).scalar()
    ndx_count = connection.execute(text("SELECT COUNT(*) FROM rsi_indices WHERE is_ndx = TRUE")).scalar()
    dji_count = connection.execute(text("SELECT COUNT(*) FROM rsi_indices WHERE is_dji = TRUE")).scalar()
    
    logger.info(f"Inserted {count} rows into rsi_indices table")
    logger.info(f"  SPX constituents: {spx_count}")
    logger.info(f"  NDX constituents: {ndx_count}")
    logger.info(f"  DJI constituents: {dji_count}")
    
    return count


def main():
    """Main function to update rsi_indices table"""
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting RSI Indices Update ===")
    logger.info("=" * 60)
    
    session, engine = init_db()
    
    try:
        with engine.connect() as connection:
            # Step 1: Truncate the table
            truncate_rsi_indices(connection)
            
            # Step 2: Compute and load metrics
            count = compute_and_load_rsi_indices(connection)
            
            logger.info(f"Successfully updated rsi_indices with {count} records")
            write_summary(SCRIPT_NAME, 'SUCCESS', 'Updated RSI indices rankings', count)
            
    except Exception as e:
        logger.error(f"Error in RSI indices update: {str(e)}")
        write_summary(SCRIPT_NAME, 'FAILED', str(e))
        raise
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== RSI Indices Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()
