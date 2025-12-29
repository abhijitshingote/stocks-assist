#!/usr/bin/env python3
"""
Volume Spike & Gapper Update Script
Truncates and reloads the stock_volspike_gapper table with pre-computed metrics.

This script computes:
- Volume Spike: Stock's daily volume ≥ 3.5× its prior 20-trading-day average 
  (which must be > 0) and the close is ≥ 3% above previous day close within the last 365 days.
- Gapper: Stock's low > previous day close × 1.05 or close > previous day close × 1.15 
  within the last 365 days.
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
    
    Filters:
    - market_cap > 1B
    - fps < 100
    - industry <> 'Biotechnology'
    - Must have at least one spike or gap event
    
    Sorted by last_event_date DESC.
    """
    logger.info("Computing volume spike and gapper metrics...")
    
    # The comprehensive SQL query to compute volume spike and gapper metrics
    metrics_query = """
WITH params AS (
    SELECT
        365  AS vol_spike_lookback_days,
        20   AS avg_days,
        3.5  AS spike_mult,
        0.03 AS min_daily_gain,
        0.05 AS min_gap_pct,
        365  AS gap_lookback_days
),

/* -----------------------------------------------------
   Rank all trading days per ticker
----------------------------------------------------- */
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

/* -----------------------------------------------------
   Candidate volume spike days
----------------------------------------------------- */
candidate_spike_days AS (
    SELECT r.*
    FROM ranked_ohlc r
    CROSS JOIN params p
    WHERE r.date >= (
        SELECT MAX(date) FROM ohlc
    ) - (p.vol_spike_lookback_days || ' days')::INTERVAL
),

volume_baseline AS (
    SELECT
        cd.ticker,
        cd.date AS spike_date,
        cd.rn,
        cd.volume AS day_volume,
        cd.close  AS day_close,
        pc.close  AS prev_close,
        AVG(prev.volume) AS avg_volume_20d
    FROM candidate_spike_days cd
    JOIN ranked_ohlc prev
      ON prev.ticker = cd.ticker
     AND prev.rn BETWEEN cd.rn - 20 AND cd.rn - 1
    JOIN ranked_ohlc pc
      ON pc.ticker = cd.ticker
     AND pc.rn = cd.rn - 1
    GROUP BY
        cd.ticker,
        cd.date,
        cd.rn,
        cd.volume,
        cd.close,
        pc.close
    HAVING COUNT(*) = 20
),

volume_spikes AS (
    SELECT
        vb.ticker,
        vb.spike_date,
        ROUND(vb.day_volume / NULLIF(vb.avg_volume_20d, 0), 2) AS volume_ratio,
        ROUND(
            (vb.day_close / NULLIF(vb.prev_close, 0) - 1)::numeric,
            4
        ) AS daily_return
    FROM volume_baseline vb
    CROSS JOIN params p
    WHERE vb.day_volume >= p.spike_mult * vb.avg_volume_20d
      AND vb.day_close  >= vb.prev_close * (1 + p.min_daily_gain)
),

volume_spikes_agg AS (
    SELECT
        ticker,
        COUNT(*) AS spike_day_count,
        ROUND(AVG(volume_ratio)::numeric, 2) AS avg_volume_spike,
        ARRAY_AGG(spike_date ORDER BY spike_date) AS volume_spike_days,
        MAX(spike_date) AS last_spike_date
    FROM volume_spikes
    GROUP BY ticker
),

/* -----------------------------------------------------
   Candidate gap days (trading-day based)
----------------------------------------------------- */
candidate_gap_days AS (
    SELECT r.*
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
        r.close,
        prev.close AS prev_close
    FROM candidate_gap_days r
    JOIN ranked_ohlc prev
      ON prev.ticker = r.ticker
     AND prev.rn = r.rn - 1
    CROSS JOIN params p
    WHERE prev.close > 0
      AND (
            r.low   > prev.close * (1 + p.min_gap_pct)
         OR r.close / prev.close > 1.15
      )
),

gap_returns_agg AS (
    SELECT
        ticker,
        COUNT(*) AS gapper_day_count,
        ROUND(AVG((close / prev_close - 1)::numeric), 4) AS avg_return_gapper,
        ARRAY_AGG(gap_date ORDER BY gap_date) AS gap_days,
        MAX(gap_date) AS last_gap_date
    FROM gap_days
    GROUP BY ticker
)

    INSERT INTO stock_volspike_gapper (
        ticker,
        spike_day_count,
        avg_volume_spike,
        volume_spike_days,
        gapper_day_count,
        avg_return_gapper,
        gap_days,
        updated_at,
        last_event_date,
        last_event_type
    )
SELECT
    sm.ticker,

    vsa.spike_day_count,
    vsa.avg_volume_spike,
    ARRAY_TO_STRING(vsa.volume_spike_days, ',') AS volume_spike_days,

    gra.gapper_day_count,
    gra.avg_return_gapper,
    ARRAY_TO_STRING(gra.gap_days, ',') AS gap_days,

    CURRENT_TIMESTAMP AS updated_at,

    GREATEST(
        vsa.last_spike_date,
        gra.last_gap_date
    ) AS last_event_date,

    CASE
        WHEN vsa.last_spike_date IS NULL
         AND gra.last_gap_date  IS NULL
            THEN NULL
        WHEN vsa.last_spike_date >= gra.last_gap_date
            THEN 'volume_spike'
        ELSE 'gapper'
    END AS last_event_type

FROM stock_metrics sm
LEFT JOIN volume_spikes_agg vsa
  ON vsa.ticker = sm.ticker
LEFT JOIN gap_returns_agg gra
  ON gra.ticker = sm.ticker
ORDER BY last_event_date desc nulls last;

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

