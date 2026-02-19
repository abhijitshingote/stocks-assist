#!/usr/bin/env python3
"""
Market Breadth Update Script
Computes and stores daily market breadth metrics:
- Stocks above/below 50DMA
- Stocks above/below 200DMA
- Stocks that moved 4%+ higher/lower in a day
"""

import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import time
import pytz

# Add backend and db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Base, MarketBreadth
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'market_breadth_update'
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
    """Get the last date that was processed in market_breadth table"""
    result = connection.execute(text("SELECT MAX(date) FROM market_breadth"))
    last_date = result.scalar()
    return last_date


def compute_and_load_breadth(connection, start_date=None):
    """
    Compute market breadth metrics for all dates and insert into market_breadth table.
    Uses INSERT ... ON CONFLICT to handle updates for existing dates.
    """
    logger.info("Computing market breadth data...")
    
    # Build date filter if needed
    date_filter = ""
    if start_date:
        date_filter = f"AND o.date > '{start_date}'"
    
    # The comprehensive SQL query to compute market breadth
    breadth_query = f"""
    WITH daily_data AS (
        SELECT
            o.date,
            o.ticker,
            o.close,
            h.dma_50,
            h.dma_200,
            LAG(o.close) OVER (PARTITION BY o.ticker ORDER BY o.date) AS prev_close
        FROM ohlc o
        JOIN historical_rsi h ON o.ticker = h.ticker AND o.date = h.date
        JOIN tickers t ON o.ticker = t.ticker
        WHERE t.is_actively_trading = TRUE
          AND t.is_etf = FALSE
          AND t.is_fund = FALSE
          {date_filter}
    ),
    daily_returns AS (
        SELECT
            date,
            ticker,
            close,
            dma_50,
            dma_200,
            CASE WHEN prev_close > 0 THEN ((close - prev_close) / prev_close * 100) ELSE NULL END AS daily_return
        FROM daily_data
        WHERE prev_close IS NOT NULL
    ),
    breadth_metrics AS (
        SELECT
            date,
            COUNT(*) AS total_stocks,
            COUNT(*) FILTER (WHERE close > dma_50 AND dma_50 IS NOT NULL) AS above_50dma,
            COUNT(*) FILTER (WHERE close < dma_50 AND dma_50 IS NOT NULL) AS below_50dma,
            COUNT(*) FILTER (WHERE close > dma_200 AND dma_200 IS NOT NULL) AS above_200dma,
            COUNT(*) FILTER (WHERE close < dma_200 AND dma_200 IS NOT NULL) AS below_200dma,
            COUNT(*) FILTER (WHERE daily_return >= 4) AS up_4pct,
            COUNT(*) FILTER (WHERE daily_return <= -4) AS down_4pct
        FROM daily_returns
        WHERE dma_50 IS NOT NULL OR dma_200 IS NOT NULL
        GROUP BY date
    )
    INSERT INTO market_breadth (date, above_50dma, below_50dma, above_200dma, below_200dma, up_4pct, down_4pct, total_stocks, created_at, updated_at)
    SELECT
        date,
        above_50dma,
        below_50dma,
        above_200dma,
        below_200dma,
        up_4pct,
        down_4pct,
        total_stocks,
        NOW(),
        NOW()
    FROM breadth_metrics
    ON CONFLICT (date) 
    DO UPDATE SET 
        above_50dma = EXCLUDED.above_50dma,
        below_50dma = EXCLUDED.below_50dma,
        above_200dma = EXCLUDED.above_200dma,
        below_200dma = EXCLUDED.below_200dma,
        up_4pct = EXCLUDED.up_4pct,
        down_4pct = EXCLUDED.down_4pct,
        total_stocks = EXCLUDED.total_stocks,
        updated_at = NOW()
    """
    
    result = connection.execute(text(breadth_query))
    connection.commit()
    
    # Get count of rows in table
    count_result = connection.execute(text("SELECT COUNT(*) FROM market_breadth"))
    total_count = count_result.scalar()
    
    # Get date range
    range_result = connection.execute(text("SELECT MIN(date), MAX(date) FROM market_breadth"))
    min_date, max_date = range_result.fetchone()
    
    logger.info(f"Market breadth table now has {total_count} rows")
    if min_date and max_date:
        logger.info(f"Date range: {min_date} to {max_date}")
    
    return total_count


def main():
    """Main function to update market_breadth table"""
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting Market Breadth Update ===")
    logger.info("=" * 60)
    
    session, engine = init_db()
    
    try:
        with engine.connect() as connection:
            # Check if we should do incremental update
            last_date = get_last_processed_date(connection)
            
            if last_date:
                logger.info(f"Last processed date: {last_date}")
                logger.info("Running incremental update for new dates...")
                total_count = compute_and_load_breadth(connection, start_date=last_date)
            else:
                logger.info("No existing data found. Running full historical load...")
                total_count = compute_and_load_breadth(connection)
            
            logger.info(f"Successfully updated market_breadth: {total_count} records")
            total_time = time.time() - overall_start
            write_summary(SCRIPT_NAME, 'SUCCESS', f'Updated {total_count} days of breadth data', total_count, duration_seconds=total_time)
            
    except Exception as e:
        logger.error(f"Error in market breadth update: {str(e)}")
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
        raise
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== Market Breadth Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()
