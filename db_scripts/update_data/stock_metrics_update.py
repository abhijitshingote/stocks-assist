#!/usr/bin/env python3
"""
Stock Metrics Update Script
Truncates and reloads the stock_metrics table with pre-computed metrics.

This script computes various metrics including:
- Price change percentages (1, 5, 20, 60, 120 days)
- Volume vs 10-day average
- ATR20 (Average True Range as %)
- P/E, P/S (TTM and Forward)
- RSI (Relative Strength percentile vs SPY)
- YoY Revenue Growth (t-1, t, t+1, t+2)
- YoY EPS Growth (t-1, t, t+1, t+2)
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
from models import Base, StockMetrics
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'stock_metrics_update'
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


def truncate_stock_metrics(connection):
    """Truncate the stock_metrics table"""
    logger.info("Truncating stock_metrics table...")
    connection.execute(text("TRUNCATE TABLE stock_metrics"))
    logger.info("stock_metrics table truncated")


def compute_and_load_metrics(connection):
    """
    Compute stock metrics using the comprehensive SQL query and insert into stock_metrics table.
    """
    logger.info("Computing stock metrics...")
    
    # The comprehensive SQL query to compute all metrics
    metrics_query = """
    WITH latest_date AS (
        SELECT MAX(date) as max_date FROM ohlc
    ),
    -- Parameters for YoY growth calculations
    params AS (
        SELECT EXTRACT(YEAR FROM CURRENT_DATE)::int AS current_year
    ),
    -- YoY Revenue Growth from analyst estimates
    revenue_growth_yoy AS (
        SELECT
            e.ticker,
            e.date,
            EXTRACT(YEAR FROM e.date)::int AS year,
            ROUND(
                (
                    (e.revenue_avg - LAG(e.revenue_avg) OVER (PARTITION BY e.ticker ORDER BY e.date))::numeric
                    / NULLIF(LAG(e.revenue_avg) OVER (PARTITION BY e.ticker ORDER BY e.date), 0)::numeric
                    * 100
                ),
                2
            ) AS yoy_revenue_growth_pct
        FROM analyst_estimates e
    ),
    -- YoY EPS Growth from analyst estimates
    eps_growth_yoy AS (
        SELECT
            e.ticker,
            e.date,
            EXTRACT(YEAR FROM e.date)::int AS year,
            ROUND(
                (
                    (e.eps_avg - LAG(e.eps_avg) OVER (PARTITION BY e.ticker ORDER BY e.date))::numeric
                    / NULLIF(LAG(e.eps_avg) OVER (PARTITION BY e.ticker ORDER BY e.date), 0)::numeric
                    * 100
                ),
                2
            ) AS yoy_eps_growth_pct
        FROM analyst_estimates e
    ),
    -- Dynamic growth pivoted by year
    dynamic_growth AS (
        SELECT
            rg.ticker,
            -- Revenue Growth
            MAX(CASE WHEN rg.year = p.current_year - 1 THEN rg.yoy_revenue_growth_pct END) AS rev_growth_t_minus_1,
            MAX(CASE WHEN rg.year = p.current_year     THEN rg.yoy_revenue_growth_pct END) AS rev_growth_t,
            MAX(CASE WHEN rg.year = p.current_year + 1 THEN rg.yoy_revenue_growth_pct END) AS rev_growth_t_plus_1,
            MAX(CASE WHEN rg.year = p.current_year + 2 THEN rg.yoy_revenue_growth_pct END) AS rev_growth_t_plus_2,
            -- EPS Growth
            MAX(CASE WHEN eg.year = p.current_year - 1 THEN eg.yoy_eps_growth_pct END) AS eps_growth_t_minus_1,
            MAX(CASE WHEN eg.year = p.current_year     THEN eg.yoy_eps_growth_pct END) AS eps_growth_t,
            MAX(CASE WHEN eg.year = p.current_year + 1 THEN eg.yoy_eps_growth_pct END) AS eps_growth_t_plus_1,
            MAX(CASE WHEN eg.year = p.current_year + 2 THEN eg.yoy_eps_growth_pct END) AS eps_growth_t_plus_2
        FROM revenue_growth_yoy rg
        LEFT JOIN eps_growth_yoy eg ON rg.ticker = eg.ticker AND rg.year = eg.year
        CROSS JOIN params p
        GROUP BY rg.ticker, p.current_year
    ),
    price_changes AS (
        SELECT 
            o_today.ticker,
            o_today.close as current_close,
            o_today.volume as today_volume,
            ROUND(((o_today.close - o_1d.close) / NULLIF(o_1d.close, 0) * 100)::numeric, 2) as dr_1,
            ROUND(((o_today.close - o_5d.close) / NULLIF(o_5d.close, 0) * 100)::numeric, 2) as dr_5,
            ROUND(((o_today.close - o_20d.close) / NULLIF(o_20d.close, 0) * 100)::numeric, 2) as dr_20,
            ROUND(((o_today.close - o_60d.close) / NULLIF(o_60d.close, 0) * 100)::numeric, 2) as dr_60,
            ROUND(((o_today.close - o_120d.close) / NULLIF(o_120d.close, 0) * 100)::numeric, 2) as dr_120
        FROM ohlc o_today
        CROSS JOIN latest_date ld
        LEFT JOIN LATERAL (
            SELECT close FROM ohlc 
            WHERE ticker = o_today.ticker AND date < ld.max_date 
            ORDER BY date DESC LIMIT 1
        ) o_1d ON TRUE
        LEFT JOIN LATERAL (
            SELECT close FROM ohlc 
            WHERE ticker = o_today.ticker AND date < ld.max_date 
            ORDER BY date DESC OFFSET 4 LIMIT 1
        ) o_5d ON TRUE
        LEFT JOIN LATERAL (
            SELECT close FROM ohlc 
            WHERE ticker = o_today.ticker AND date < ld.max_date 
            ORDER BY date DESC OFFSET 19 LIMIT 1
        ) o_20d ON TRUE
        LEFT JOIN LATERAL (
            SELECT close FROM ohlc 
            WHERE ticker = o_today.ticker AND date < ld.max_date 
            ORDER BY date DESC OFFSET 59 LIMIT 1
        ) o_60d ON TRUE
        LEFT JOIN LATERAL (
            SELECT close FROM ohlc 
            WHERE ticker = o_today.ticker AND date < ld.max_date 
            ORDER BY date DESC OFFSET 119 LIMIT 1
        ) o_120d ON TRUE
        WHERE o_today.date = ld.max_date
    ),
    volume_avg AS (
        SELECT 
            o.ticker,
            AVG(o.volume) as avg_vol_10d
        FROM ohlc o
        CROSS JOIN latest_date ld
        WHERE o.date < ld.max_date
          AND o.date >= (ld.max_date - INTERVAL '20 days')
        GROUP BY o.ticker
        HAVING COUNT(*) >= 10
    ),
    atr20 AS (
        SELECT
            ticker,
            AVG(tr_pct) AS atr20
        FROM (
            SELECT
                o.ticker,
                (
                    GREATEST(
                        o.high - o.low,
                        ABS(o.high - LAG(o.close) OVER w),
                        ABS(o.low  - LAG(o.close) OVER w)
                    )
                    / LAG(o.close) OVER w
                ) * 100.0 AS tr_pct,
                ROW_NUMBER() OVER (
                    PARTITION BY o.ticker
                    ORDER BY o.date DESC
                ) AS rn
            FROM ohlc o
            WINDOW w AS (
                PARTITION BY o.ticker
                ORDER BY o.date
            )
        ) t
        WHERE rn <= 20
        GROUP BY ticker
    ),
    ttm_financials AS (
        SELECT
            ticker,
            SUM(eps_actual) AS eps_ttm,
            SUM(revenue_actual) AS revenue_ttm
        FROM (
            SELECT
                e.*,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM earnings e
            CROSS JOIN latest_date ld
            WHERE e.date <= ld.max_date
              AND e.eps_actual IS NOT NULL
              AND e.revenue_actual IS NOT NULL
        ) sub
        WHERE rn <= 4
        GROUP BY ticker
    ),
    forward_estimates AS (
        -- Forward = current year + 1 (e.g., if we're in 2025, pick FY2026 estimates)
        SELECT DISTINCT ON (ticker)
            eps_avg,
            revenue_avg,
            ticker
        FROM analyst_estimates a
        WHERE EXTRACT(YEAR FROM a.date) = EXTRACT(YEAR FROM CURRENT_DATE) + 1
          AND a.eps_avg IS NOT NULL
          AND a.revenue_avg IS NOT NULL
        ORDER BY ticker, a.date ASC
    ),
    -- Compute stock returns
    stock_returns AS (
        SELECT
            o.ticker,
            o.date,
            (o.close / LAG(o.close, 4) OVER (PARTITION BY o.ticker ORDER BY o.date) - 1) AS ret_4,
            (o.close / LAG(o.close, 13) OVER (PARTITION BY o.ticker ORDER BY o.date) - 1) AS ret_13,
            (o.close / LAG(o.close, 20) OVER (PARTITION BY o.ticker ORDER BY o.date) - 1) AS ret_20
        FROM ohlc o
    ),
    -- Compute SPY returns
    spy_returns AS (
        SELECT
            ip.date,
            (ip.close_price / LAG(ip.close_price, 4) OVER (ORDER BY ip.date) - 1) AS ret_4,
            (ip.close_price / LAG(ip.close_price, 13) OVER (ORDER BY ip.date) - 1) AS ret_13,
            (ip.close_price / LAG(ip.close_price, 20) OVER (ORDER BY ip.date) - 1) AS ret_20
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
    ),
    -- Market cap buckets for RSI within market cap
    market_cap_bucket AS (
        SELECT
            t.ticker,
            CASE
                WHEN t.market_cap < 200000000 THEN 'micro'
                WHEN t.market_cap < 2000000000 THEN 'small'
                WHEN t.market_cap < 20000000000 THEN 'mid'
                WHEN t.market_cap < 100000000000 THEN 'large'
                ELSE 'mega'
            END AS mktcap_bucket
        FROM tickers t
    )
    INSERT INTO stock_metrics (
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
        eps_growth_t_minus_1,
        eps_growth_t,
        eps_growth_t_plus_1,
        eps_growth_t_plus_2,
        rsi,
        rsi_mktcap,
        short_float,
        short_ratio,
        short_interest,
        low_float,
        updated_at
    )
    SELECT 
        t.ticker,
        t.company_name,
        t.country,
        t.sector,
        t.industry,
        cp.ipo_date,
        t.market_cap,
        pc.current_close as current_price,
        cp.range_52_week,
        pc.today_volume as volume,
        ROUND((pc.current_close::numeric * pc.today_volume::numeric), 0) as dollar_volume,
        ROUND(va.avg_vol_10d::numeric, 2) as avg_vol_10d,
        ROUND((pc.today_volume::numeric / NULLIF(va.avg_vol_10d::numeric, 0)), 2) as vol_vs_10d_avg,
        pc.dr_1,
        pc.dr_5,
        pc.dr_20,
        pc.dr_60,
        pc.dr_120,
        ROUND(atr20.atr20::numeric, 2) AS atr20,
        ROUND((pc.current_close::numeric / NULLIF(tf.eps_ttm::numeric, 0)), 2) AS pe,
        ROUND((t.market_cap::numeric / NULLIF(tf.revenue_ttm::numeric, 0)), 2) AS ps_ttm,
        ROUND((pc.current_close::numeric / NULLIF(fe.eps_avg::numeric, 0)), 2) AS fpe,
        ROUND((t.market_cap::numeric / NULLIF(fe.revenue_avg::numeric, 0)), 2) AS fps,
        dg.rev_growth_t_minus_1,
        dg.rev_growth_t,
        dg.rev_growth_t_plus_1,
        dg.rev_growth_t_plus_2,
        dg.eps_growth_t_minus_1,
        dg.eps_growth_t,
        dg.eps_growth_t_plus_1,
        dg.eps_growth_t_plus_2,
        rs.rsi,
        rs_mc.rsi_mktcap,
        NULL::FLOAT as short_float,
        NULL::FLOAT as short_ratio,
        NULL::FLOAT as short_interest,
        NULL::BOOLEAN as low_float,
        CURRENT_TIMESTAMP as updated_at
    FROM tickers t
    LEFT JOIN company_profiles cp ON t.ticker = cp.ticker
    LEFT JOIN price_changes pc ON t.ticker = pc.ticker
    LEFT JOIN volume_avg va ON t.ticker = va.ticker
    LEFT JOIN atr20 atr20 ON t.ticker = atr20.ticker
    LEFT JOIN ttm_financials tf ON t.ticker = tf.ticker
    LEFT JOIN forward_estimates fe ON t.ticker = fe.ticker
    LEFT JOIN dynamic_growth dg ON t.ticker = dg.ticker
    LEFT JOIN (
        SELECT
            ticker,
            NTILE(100) OVER (ORDER BY rel_strength_raw) AS rsi
        FROM rel_strength
        WHERE date = (SELECT max_date FROM latest_date)
    ) rs ON t.ticker = rs.ticker
    LEFT JOIN (
        SELECT
            rs.ticker,
            NTILE(100) OVER (
                PARTITION BY mcb.mktcap_bucket
                ORDER BY rs.rel_strength_raw
            ) AS rsi_mktcap
        FROM rel_strength rs
        JOIN market_cap_bucket mcb ON rs.ticker = mcb.ticker
        WHERE rs.date = (SELECT max_date FROM latest_date)
    ) rs_mc ON t.ticker = rs_mc.ticker
    WHERE t.is_actively_trading = TRUE
      AND t.is_etf = FALSE
      AND t.is_fund = FALSE
    """
    
    result = connection.execute(text(metrics_query))
    connection.commit()
    
    # Get count of inserted rows
    count_result = connection.execute(text("SELECT COUNT(*) FROM stock_metrics"))
    count = count_result.scalar()
    
    logger.info(f"Inserted {count} rows into stock_metrics table")
    return count


def main():
    """Main function to update stock_metrics table"""
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting Stock Metrics Update ===")
    logger.info("=" * 60)
    
    session, engine = init_db()
    
    try:
        with engine.connect() as connection:
            # Step 1: Truncate the table
            truncate_stock_metrics(connection)
            
            # Step 2: Compute and load metrics
            count = compute_and_load_metrics(connection)
            
            logger.info(f"Successfully updated stock_metrics with {count} records")
            total_time = time.time() - overall_start
            write_summary(SCRIPT_NAME, 'SUCCESS', 'Updated stock metrics', count, duration_seconds=total_time)
            
    except Exception as e:
        logger.error(f"Error in stock metrics update: {str(e)}")
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
        raise
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== Stock Metrics Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()

