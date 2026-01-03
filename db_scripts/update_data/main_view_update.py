#!/usr/bin/env python3
"""
Main View Update Script
Truncates and reloads the main_view table with combined metrics, volspike/gapper, and tags.

This script combines:
- All columns from stock_metrics
- Volume spike and gapper data from stock_volspike_gapper
- Computed tags based on various criteria
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
from models import Base, MainView
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'main_view_update'
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


def truncate_main_view(connection):
    """Truncate the main_view table"""
    logger.info("Truncating main_view table...")
    connection.execute(text("TRUNCATE TABLE main_view"))
    logger.info("main_view table truncated")


def compute_and_load_main_view(connection):
    """
    Compute main_view data using the comprehensive SQL query and insert into main_view table.
    Combines stock_metrics with stock_volspike_gapper and computes tags.
    """
    logger.info("Computing main_view data...")
    
    # The comprehensive SQL query to compute all metrics with tags
    main_view_query = """
    INSERT INTO main_view (
        ticker,
        company_name,
        country,
        sector,
        industry,
        ipo_date,
        market_cap,
        current_price,
        range_52_week,
        volume,
        dollar_volume,
        avg_vol_10d,
        vol_vs_10d_avg,
        dr_1,
        dr_5,
        dr_20,
        dr_60,
        dr_120,
        atr20,
        pe,
        ps_ttm,
        fpe,
        fps,
        rev_growth_t_minus_1,
        rev_growth_t,
        rev_growth_t_plus_1,
        rev_growth_t_plus_2,
        avg_rev_growth,
        eps_growth_t_minus_1,
        eps_growth_t,
        eps_growth_t_plus_1,
        eps_growth_t_plus_2,
        avg_eps_growth,
        rsi,
        rsi_mktcap,
        short_float,
        short_ratio,
        short_interest,
        low_float,
        spike_day_count,
        avg_volume_spike,
        volume_spike_days,
        gapper_day_count,
        avg_return_gapper,
        gap_days,
        last_event_date,
        last_event_type,
        tags,
        updated_at
    )
    SELECT
        COALESCE(sm.ticker, svg.ticker) as ticker,
        sm.company_name,
        sm.country,
        sm.sector,
        sm.industry,
        sm.ipo_date,
        sm.market_cap,
        sm.current_price,
        sm.range_52_week,
        sm.volume,
        sm.dollar_volume,
        sm.avg_vol_10d,
        sm.vol_vs_10d_avg,
        sm.dr_1,
        sm.dr_5,
        sm.dr_20,
        sm.dr_60,
        sm.dr_120,
        sm.atr20,
        sm.pe,
        sm.ps_ttm,
        sm.fpe,
        sm.fps,
        sm.rev_growth_t_minus_1,
        sm.rev_growth_t,
        sm.rev_growth_t_plus_1,
        sm.rev_growth_t_plus_2,
        sm.avg_rev_growth,
        sm.eps_growth_t_minus_1,
        sm.eps_growth_t,
        sm.eps_growth_t_plus_1,
        sm.eps_growth_t_plus_2,
        sm.avg_eps_growth,
        sm.rsi,
        sm.rsi_mktcap,
        sm.short_float,
        sm.short_ratio,
        sm.short_interest,
        sm.low_float,
        svg.spike_day_count,
        svg.avg_volume_spike,
        svg.volume_spike_days,
        svg.gapper_day_count,
        svg.avg_return_gapper,
        svg.gap_days,
        svg.last_event_date,
        svg.last_event_type,
        -- Combined additive tags with duplicate handling
        ARRAY_TO_STRING(
            ARRAY(
                SELECT DISTINCT tag
                FROM (
                    SELECT
                        CASE
                            WHEN tag LIKE '%(%' THEN tag  -- keep dated version
                            ELSE tag
                        END AS tag
                    FROM UNNEST(ARRAY[
                        -- High growth from stock_metrics
                        CASE WHEN sm.avg_rev_growth > 25 AND sm.rev_growth_t_plus_2 > 15 THEN 'high_growth' END,
                        -- Daily return tags from stock_metrics
                        CASE WHEN sm.dr_5   > 10  THEN 'dr_5 > 10'   END,
                        CASE WHEN sm.dr_20  > 20  THEN 'dr_20 > 20'  END,
                        CASE WHEN sm.dr_60  > 50  THEN 'dr_60 > 50'  END,
                        CASE WHEN sm.dr_120 > 100 THEN 'dr_120 > 100' END,
                        -- Vol spike / gapper tags only if last_event_date within 60 days
                        CASE WHEN svg.spike_day_count > 0 AND svg.last_event_date >= CURRENT_DATE - INTERVAL '60 days'
                             THEN 'volume_spike' END,
                        CASE WHEN svg.gapper_day_count > 0 AND svg.last_event_date >= CURRENT_DATE - INTERVAL '60 days'
                             THEN 'gapper' END,
                        -- Last event type only if recent
                        CASE WHEN svg.last_event_date >= CURRENT_DATE - INTERVAL '60 days'
                             THEN svg.last_event_type || ' (' || TO_CHAR(svg.last_event_date, 'YYYY-MM-DD') || ')' END
                    ]) AS tag
                ) sub
                WHERE tag IS NOT NULL
                -- Remove plain tag if a dated version exists
                AND NOT EXISTS (
                    SELECT 1
                    FROM UNNEST(ARRAY[
                        CASE WHEN svg.last_event_date >= CURRENT_DATE - INTERVAL '60 days'
                             THEN svg.last_event_type || ' (' || TO_CHAR(svg.last_event_date, 'YYYY-MM-DD') || ')' END
                    ]) AS dated_tag
                    WHERE dated_tag LIKE tag || ' (%'
                )
            ),
            ', '
        ) AS tags,
        CURRENT_TIMESTAMP as updated_at
    FROM stock_metrics sm
    FULL OUTER JOIN stock_volspike_gapper svg
        ON sm.ticker = svg.ticker
    -- Keep all rows from either table
    WHERE
        ((sm.ticker IS NULL
            OR sm.dr_120 > 100
            OR sm.dr_60  > 50
            OR sm.dr_20  > 20
            OR sm.dr_5   > 10
            OR (sm.avg_rev_growth > 25 AND (sm.rev_growth_t_plus_2 > 15 OR sm.rev_growth_t_plus_2 IS NULL))
        ) 
        OR (svg.ticker IS NOT NULL 
            AND svg.last_event_date >= CURRENT_DATE - INTERVAL '60 days'
            AND (svg.spike_day_count > 0 OR svg.gapper_day_count > 0)))
        AND 
        ((sm.ticker IS NULL OR sm.market_cap > 1000000000)
        AND (sm.ticker IS NULL OR (sm.fps is  null or sm.fps < 100))
        AND (sm.ticker IS NULL OR sm.industry <> 'Biotechnology')
        AND sm.avg_vol_10d > 50000       
        AND sm.dollar_volume > 10000000  
        AND sm.current_price > 3 )
    ORDER BY sm.dr_20 DESC
    """
    
    result = connection.execute(text(main_view_query))
    connection.commit()
    
    # Get count of inserted rows
    count_result = connection.execute(text("SELECT COUNT(*) FROM main_view"))
    count = count_result.scalar()
    
    logger.info(f"Inserted {count} rows into main_view table")
    return count


def main():
    """Main function to update main_view table"""
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting Main View Update ===")
    logger.info("=" * 60)
    
    session, engine = init_db()
    
    try:
        with engine.connect() as connection:
            # Step 1: Truncate the table
            truncate_main_view(connection)
            
            # Step 2: Compute and load main_view data
            count = compute_and_load_main_view(connection)
            
            logger.info(f"Successfully updated main_view with {count} records")
            total_time = time.time() - overall_start
            write_summary(SCRIPT_NAME, 'SUCCESS', 'Updated main_view', count, duration_seconds=total_time)
            
    except Exception as e:
        logger.error(f"Error in main_view update: {str(e)}")
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
        raise
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== Main View Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()

