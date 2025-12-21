#!/usr/bin/env python3
"""
Volume Spike & Gapper Update Script
Truncates and reloads the stock_volspike_gapper table with pre-computed metrics.

This script computes:
- Volume Spike: Stock's daily volume ≥ 3.5× its prior 20-trading-day average 
  (which must be > 0) and the close is ≥ 3% above previous day close within the last 30 days.
- Gapper: Stock's low > previous day close × 1.05 or close > previous day close × 1.15 
  within the last 30 days.
"""

import os
import sys
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'volspike_gapper_update'
logger = get_logger(SCRIPT_NAME)

# Load environment variables
load_dotenv()


def init_db():
    """Initialize database connection"""
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment variables")
    
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session(), engine


def truncate_stock_volspike_gapper(connection):
    """Truncate the stock_volspike_gapper table"""
    logger.info("Truncating stock_volspike_gapper table...")
    connection.execute(text("TRUNCATE TABLE stock_volspike_gapper"))
    logger.info("stock_volspike_gapper table truncated")


def compute_and_load_volspike_gapper(connection):
    """
    Compute volume spike and gapper metrics using the comprehensive SQL query 
    and insert into stock_volspike_gapper table.
    """
    logger.info("Computing volume spike and gapper metrics...")
    
    # The comprehensive SQL query to compute volume spike and gapper metrics
    metrics_query = """
    WITH params AS (
        SELECT
            30  AS vol_spike_lookback_days,   -- lookback window for spike days
            20  AS avg_days,                  -- prior trading days for avg volume
            3.5 AS spike_mult,                -- volume spike threshold
            0.03 AS min_daily_gain,           -- +3% close-to-close gain
            0.05 AS min_gap_pct,              -- min gap % for gapper condition
            30  AS gap_lookback_days          -- lookback window for gapper
    ),

    -- Rank all trading days per ticker
    ranked_ohlc AS (
        SELECT
            o.ticker,
            o.date,
            o.close,
            o.low,
            o.volume,
            ROW_NUMBER() OVER (
                PARTITION BY o.ticker
                ORDER BY o.date
            ) AS rn
        FROM ohlc o
    ),

    -- Candidate spike days (last N calendar days, trading-day ranked)
    candidate_days AS (
        SELECT
            r.*
        FROM ranked_ohlc r
        CROSS JOIN params p
        WHERE r.date >= (
            SELECT MAX(date) FROM ohlc
        ) - (p.vol_spike_lookback_days || ' days')::INTERVAL
    ),

    -- Compute prior 20 trading-day average volume + previous close
    volume_baseline AS (
        SELECT
            cd.ticker,
            cd.date,
            cd.rn,
            cd.volume AS day_volume,
            cd.close  AS day_close,
            pc.close  AS prev_close,
            AVG(prev.volume) AS avg_volume_20d
        FROM candidate_days cd
        JOIN ranked_ohlc prev
          ON prev.ticker = cd.ticker
         AND prev.rn BETWEEN cd.rn - 20 AND cd.rn - 1
        JOIN ranked_ohlc pc
          ON pc.ticker = cd.ticker
         AND pc.rn = cd.rn - 1
        WHERE pc.close IS NOT NULL
          AND pc.close > 0
        GROUP BY
            cd.ticker, cd.date, cd.rn,
            cd.volume, cd.close, pc.close
        HAVING COUNT(prev.volume) = 20
           AND AVG(prev.volume) > 0
    ),

    -- Apply volume spike + price confirmation
    volume_spikes AS (
        SELECT
            vb.ticker,
            vb.date AS spike_date,
            vb.day_volume,
            vb.avg_volume_20d,
            ROUND(
                vb.day_volume / NULLIF(vb.avg_volume_20d, 0),
                2
            ) AS volume_ratio,
            ROUND(
                (vb.day_close / NULLIF(vb.prev_close, 0) - 1)::numeric,
                4
            ) AS daily_return
        FROM volume_baseline vb
        CROSS JOIN params p
        WHERE vb.day_volume >= p.spike_mult * vb.avg_volume_20d
          AND vb.day_close >= vb.prev_close * (1 + p.min_daily_gain)
    ),

    -- Candidate gap days (gapper logic)
    candidate_gap_days AS (
        SELECT
            r.*
        FROM ranked_ohlc r
        CROSS JOIN params p
        WHERE r.date >= (
            SELECT MAX(date) FROM ohlc
        ) - (p.gap_lookback_days || ' days')::INTERVAL
    ),

    gap_days AS (
        SELECT
            r.ticker,
            r.date AS gap_date,
            r.close
        FROM candidate_gap_days r
        JOIN ranked_ohlc prev
          ON prev.ticker = r.ticker
         AND prev.rn = r.rn - 1
        CROSS JOIN params p
        WHERE prev.close > 0
          AND (
                r.low > prev.close * (1 + p.min_gap_pct)
             OR r.close / prev.close > 1.15
          )
    ),

    gap_returns AS (
        SELECT
            gd.ticker,
            (gd.close / prev.close - 1)::numeric AS daily_return
        FROM gap_days gd
        JOIN ranked_ohlc prev
          ON prev.ticker = gd.ticker
         AND prev.date = gd.gap_date - INTERVAL '1 day'
    )

    INSERT INTO stock_volspike_gapper (
        ticker,
        spike_day_count,
        avg_volume_spike,
        volume_spike_days,
        gapper_day_count,
        avg_return_gapper,
        gap_days,
        updated_at
    )
    SELECT
        sm.ticker,
        COUNT(DISTINCT vs.spike_date) AS spike_day_count,
        ROUND(AVG(vs.volume_ratio)::numeric, 2) AS avg_volume_spike,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT vs.spike_date ORDER BY vs.spike_date), ',') AS volume_spike_days,
        COUNT(DISTINCT gd.gap_date) AS gapper_day_count,
        ROUND(AVG(gr.daily_return), 4) AS avg_return_gapper,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT gd.gap_date ORDER BY gd.gap_date), ',') AS gap_days,
        CURRENT_TIMESTAMP as updated_at
    FROM stock_metrics sm
    LEFT JOIN volume_spikes vs ON vs.ticker = sm.ticker
    LEFT JOIN gap_days gd ON gd.ticker = sm.ticker
    LEFT JOIN gap_returns gr ON gr.ticker = sm.ticker
    GROUP BY sm.ticker
    """
    
    result = connection.execute(text(metrics_query))
    connection.commit()
    
    # Get count of inserted rows
    count_result = connection.execute(text("SELECT COUNT(*) FROM stock_volspike_gapper"))
    count = count_result.scalar()
    
    logger.info(f"Inserted {count} rows into stock_volspike_gapper table")
    return count


def main():
    """Main function to update stock_volspike_gapper table"""
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting Volume Spike & Gapper Update ===")
    logger.info("=" * 60)
    
    session, engine = init_db()
    
    try:
        with engine.connect() as connection:
            # Step 1: Truncate the table
            truncate_stock_volspike_gapper(connection)
            
            # Step 2: Compute and load metrics
            count = compute_and_load_volspike_gapper(connection)
            
            logger.info(f"Successfully updated stock_volspike_gapper with {count} records")
            total_time = time.time() - overall_start
            write_summary(SCRIPT_NAME, 'SUCCESS', 'Updated volume spike & gapper', count, duration_seconds=total_time)
            
    except Exception as e:
        logger.error(f"Error in volume spike & gapper update: {str(e)}")
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
        raise
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== Volume Spike & Gapper Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()

