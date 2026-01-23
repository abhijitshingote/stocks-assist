#!/usr/bin/env python3
"""
Historical RSI Update Script
Computes and stores daily RSI percentile rankings, DMAs, and EMAs for all stocks.

This script computes:
- rsi_global: RSI percentile rank (1-100) across all stocks for each date
- rsi_mktcap: RSI percentile rank (1-100) within market cap bucket for each date
- dma_50: 50-day simple moving average
- dma_200: 200-day simple moving average
- ema_10: 10-day exponential moving average
- ema_20: 20-day exponential moving average

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
import pandas as pd

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
    Compute historical RSI and DMAs for all dates and insert into historical_rsi table.
    Uses INSERT ... ON CONFLICT to handle updates for existing dates.
    """
    logger.info("Computing historical RSI and DMA data...")
    
    # Build date filter if needed
    date_filter = ""
    if start_date:
        date_filter = f"AND rs.date > '{start_date}'"
    
    # The comprehensive SQL query to compute historical RSI and DMAs
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
    -- Calculate DMAs using window functions
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
    INSERT INTO historical_rsi (ticker, date, rsi_global, rsi_mktcap, dma_50, dma_200, ema_10, ema_20)
    SELECT
        rr.ticker,
        rr.date,
        rr.rsi_global,
        rr.rsi_mktcap,
        CASE WHEN dc.dma_50_count >= 50 THEN dc.dma_50 ELSE NULL END,
        CASE WHEN dc.dma_200_count >= 200 THEN dc.dma_200 ELSE NULL END,
        NULL,  -- ema_10 calculated separately via Python
        NULL   -- ema_20 calculated separately via Python
    FROM rsi_ranked rr
    LEFT JOIN dma_calc dc ON rr.ticker = dc.ticker AND rr.date = dc.date
    ON CONFLICT (ticker, date) 
    DO UPDATE SET 
        rsi_global = EXCLUDED.rsi_global,
        rsi_mktcap = EXCLUDED.rsi_mktcap,
        dma_50 = EXCLUDED.dma_50,
        dma_200 = EXCLUDED.dma_200
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


def compute_and_update_ema(connection, start_date=None):
    """
    Compute EMA-10 and EMA-20 for all stocks using pandas and update the historical_rsi table.
    
    EMA formula: EMA_t = close_t * k + EMA_(t-1) * (1-k)
    where k = 2 / (period + 1)
    
    For EMA-10: k = 2/11 ≈ 0.1818
    For EMA-20: k = 2/21 ≈ 0.0952
    """
    logger.info("Computing EMA-10 and EMA-20...")
    
    # Build date filter for OHLC query
    date_filter = ""
    if start_date:
        # Need to go back further to calculate EMA properly (at least 200 days for warm-up)
        date_filter = f"WHERE date > '{start_date}'::date - INTERVAL '250 days'"
    
    # Fetch OHLC data
    ohlc_query = f"""
        SELECT ticker, date, close
        FROM ohlc
        {date_filter}
        ORDER BY ticker, date
    """
    
    result = connection.execute(text(ohlc_query))
    rows = result.fetchall()
    
    if not rows:
        logger.warning("No OHLC data found for EMA calculation")
        return 0
    
    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=['ticker', 'date', 'close'])
    df['date'] = pd.to_datetime(df['date'])
    
    logger.info(f"Fetched {len(df)} OHLC records for EMA calculation")
    
    # Calculate EMA for each ticker
    ema_records = []
    
    for ticker, group in df.groupby('ticker'):
        group = group.sort_values('date').copy()
        
        # Calculate EMAs using pandas ewm (exponential weighted moving)
        # span parameter: span = (2/k) - 1, so for k=2/(n+1), span = n
        group['ema_10'] = group['close'].ewm(span=10, adjust=False).mean()
        group['ema_20'] = group['close'].ewm(span=20, adjust=False).mean()
        
        # Round to 2 decimal places
        group['ema_10'] = group['ema_10'].round(2)
        group['ema_20'] = group['ema_20'].round(2)
        
        # Filter to only dates after start_date if specified
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
    
    # Update historical_rsi table in batches
    batch_size = 5000
    updated_count = 0
    
    for i in range(0, len(ema_records), batch_size):
        batch = ema_records[i:i + batch_size]
        
        # Build batch update using CASE statements for efficiency
        if batch:
            # Use temporary table approach for bulk update
            # First, create temp values
            values_list = ", ".join([
                f"('{r['ticker']}', '{r['date']}'::date, {r['ema_10']}, {r['ema_20']})"
                for r in batch
            ])
            
            update_query = f"""
                UPDATE historical_rsi hr
                SET 
                    ema_10 = v.ema_10,
                    ema_20 = v.ema_20
                FROM (VALUES {values_list}) AS v(ticker, date, ema_10, ema_20)
                WHERE hr.ticker = v.ticker AND hr.date = v.date
            """
            
            connection.execute(text(update_query))
            updated_count += len(batch)
            
            if (i + batch_size) % 50000 == 0:
                logger.info(f"Updated {updated_count} EMA records...")
    
    connection.commit()
    logger.info(f"Updated {updated_count} records with EMA values")
    return updated_count


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
                # Calculate EMAs for new dates
                ema_count = compute_and_update_ema(connection, start_date=last_date)
            else:
                logger.info("No existing data found. Running full historical load...")
                total_count, date_count = compute_and_load_historical_rsi(connection)
                # Calculate EMAs for all dates
                ema_count = compute_and_update_ema(connection)
            
            logger.info(f"Successfully updated historical_rsi: {total_count} records, {date_count} trading days, {ema_count} EMA updates")
            total_time = time.time() - overall_start
            write_summary(SCRIPT_NAME, 'SUCCESS', f'Updated {date_count} trading days, {ema_count} EMAs', total_count, duration_seconds=total_time)
            
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

