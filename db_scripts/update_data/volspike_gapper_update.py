#!/usr/bin/env python3
"""
Volume Spike & Gapper Update Script
Truncates and reloads the stock_volspike_gapper table with pre-computed metrics.

One event-day per ticker/date. A day qualifies if either (or both) fire:

- Volume: daily volume ≥ 3.5× prior 20-trading-day average (avg > 0)
  and close ≥ 3% above previous close.
- Gap: low > previous close × 1.05, or close / previous close > 1.15.

Both metrics are stored on every trigger: volume_ratio and close/prev−1 return.
last_event_type is 'volume_spike' | 'gapper' | 'both' (which criteria fired).
last_event_magnitude is always the volume ratio; last_event_return is always
the event-day return. volume_spike_days / gap_days remain the qualification
subsets (days that met each criterion).
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
    Compute unified event-day metrics and insert into stock_volspike_gapper.

    Filters at insert: all stock_metrics rows (API keeps those with ≥1 event).
    Sorted by last_event_date DESC.
    """
    logger.info("Computing volume spike and gapper metrics...")
    
    metrics_query = """
WITH params AS (
    SELECT
        365  AS lookback_days,
        3.5  AS spike_mult,
        0.03 AS min_daily_gain,
        0.05 AS min_gap_pct
),

ranked_ohlc AS (
    SELECT
        o.ticker,
        o.date,
        o.close,
        o.low,
        o.volume,
        LAG(o.close, 1) OVER (PARTITION BY o.ticker ORDER BY o.date) AS prev_close,
        AVG(o.volume) OVER (
            PARTITION BY o.ticker ORDER BY o.date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_volume_20d
    FROM ohlc o
),

latest_date AS (
    SELECT MAX(date) AS max_date FROM ohlc
),

event_days AS (
    SELECT
        r.ticker,
        r.date AS event_date,
        ROUND(r.volume / NULLIF(r.avg_volume_20d, 0), 2) AS volume_ratio,
        ROUND((r.close / NULLIF(r.prev_close, 0) - 1)::numeric, 4) AS daily_return,
        (
            r.avg_volume_20d IS NOT NULL
            AND r.avg_volume_20d > 0
            AND r.prev_close > 0
            AND r.volume >= p.spike_mult * r.avg_volume_20d
            AND r.close >= r.prev_close * (1 + p.min_daily_gain)
        ) AS is_spike,
        (
            r.prev_close > 0
            AND (
                r.low > r.prev_close * (1 + p.min_gap_pct)
                OR r.close / r.prev_close > 1.15
            )
        ) AS is_gap
    FROM ranked_ohlc r
    CROSS JOIN params p
    CROSS JOIN latest_date ld
    WHERE r.date >= ld.max_date - (p.lookback_days || ' days')::INTERVAL
),

events AS (
    SELECT * FROM event_days WHERE is_spike OR is_gap
),

volume_spikes_agg AS (
    SELECT
        ticker,
        COUNT(*) AS spike_day_count,
        ROUND(AVG(volume_ratio)::numeric, 2) AS avg_volume_spike,
        STRING_AGG(event_date::text, ',' ORDER BY event_date) AS volume_spike_days
    FROM events
    WHERE is_spike
    GROUP BY ticker
),

gap_returns_agg AS (
    SELECT
        ticker,
        COUNT(*) AS gapper_day_count,
        ROUND(AVG(daily_return)::numeric, 4) AS avg_return_gapper,
        STRING_AGG(event_date::text, ',' ORDER BY event_date) AS gap_days
    FROM events
    WHERE is_gap
    GROUP BY ticker
),

last_event AS (
    SELECT DISTINCT ON (ticker)
        ticker,
        event_date AS last_event_date,
        volume_ratio AS last_event_magnitude,
        daily_return AS last_event_return,
        CASE
            WHEN is_spike AND is_gap THEN 'both'
            WHEN is_spike THEN 'volume_spike'
            ELSE 'gapper'
        END AS last_event_type
    FROM events
    ORDER BY ticker, event_date DESC
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
    last_event_type,
    last_event_magnitude,
    last_event_return
)
SELECT
    sm.ticker,
    vsa.spike_day_count,
    vsa.avg_volume_spike,
    vsa.volume_spike_days,
    gra.gapper_day_count,
    gra.avg_return_gapper,
    gra.gap_days,
    CURRENT_TIMESTAMP AS updated_at,
    le.last_event_date,
    le.last_event_type,
    le.last_event_magnitude,
    le.last_event_return
FROM stock_metrics sm
LEFT JOIN volume_spikes_agg vsa ON vsa.ticker = sm.ticker
LEFT JOIN gap_returns_agg gra ON gra.ticker = sm.ticker
LEFT JOIN last_event le ON le.ticker = sm.ticker
ORDER BY last_event_date DESC NULLS LAST;
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
