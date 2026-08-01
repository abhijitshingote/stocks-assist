#!/usr/bin/env python3
"""
RS Screener Update Script
Computes multi-timeframe relative strength vs SPY and stores results with
percentile ranks within market cap buckets.

Timeframes: 2D, 5D, 10D, 20D, 60D
RS = stock return - SPY return over each window
Ranks = NTILE(100) within market cap bucket for each timeframe

Also computes IBD / MarketSmith style metrics (reverse-engineered, Skyte method):
- rs_vs_spy: (1+strength_stock)/(1+strength_spx)*100 where strength is the
  weighted trailing 12-month return 0.4*ROC(63)+0.2*ROC(126)+0.2*ROC(189)+
  0.2*ROC(252). 100 = in line with SPX; >100 = outperforming.
- rs_rating (1-99): percentile of weighted strength across the whole eligible
  universe (SPX normalization is a per-day constant, so ranking is unaffected).
- rs_line_new_high: RS Line (close / SPY close) at a 252-trading-day high.
"""

import os
import sys
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Base, RsScreener
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

SCRIPT_NAME = 'rs_screener_update'
logger = get_logger(SCRIPT_NAME)

load_dotenv()


def init_db():
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment variables")
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session(), engine


def compute_rs_screener(connection):
    """
    Compute relative strength for the latest trading day across 5 timeframes,
    rank within market cap buckets, and upsert into rs_screener.
    """
    logger.info("Computing multi-timeframe relative strength...")

    query = """
    WITH latest_date AS (
        SELECT MAX(o.date) AS d
        FROM ohlc o
        JOIN index_prices ip ON o.date = ip.date AND ip.symbol = 'SPY'
    ),
    stock_returns AS (
        SELECT
            o.ticker,
            o.date,
            (o.close / NULLIF(LAG(o.close, 2)  OVER (PARTITION BY o.ticker ORDER BY o.date), 0) - 1) AS ret_2,
            (o.close / NULLIF(LAG(o.close, 5)  OVER (PARTITION BY o.ticker ORDER BY o.date), 0) - 1) AS ret_5,
            (o.close / NULLIF(LAG(o.close, 10) OVER (PARTITION BY o.ticker ORDER BY o.date), 0) - 1) AS ret_10,
            (o.close / NULLIF(LAG(o.close, 20) OVER (PARTITION BY o.ticker ORDER BY o.date), 0) - 1) AS ret_20,
            (o.close / NULLIF(LAG(o.close, 60) OVER (PARTITION BY o.ticker ORDER BY o.date), 0) - 1) AS ret_60
        FROM ohlc o
    ),
    spy_returns AS (
        SELECT
            ip.date,
            (ip.close_price / NULLIF(LAG(ip.close_price, 2)  OVER (ORDER BY ip.date), 0) - 1) AS ret_2,
            (ip.close_price / NULLIF(LAG(ip.close_price, 5)  OVER (ORDER BY ip.date), 0) - 1) AS ret_5,
            (ip.close_price / NULLIF(LAG(ip.close_price, 10) OVER (ORDER BY ip.date), 0) - 1) AS ret_10,
            (ip.close_price / NULLIF(LAG(ip.close_price, 20) OVER (ORDER BY ip.date), 0) - 1) AS ret_20,
            (ip.close_price / NULLIF(LAG(ip.close_price, 60) OVER (ORDER BY ip.date), 0) - 1) AS ret_60
        FROM index_prices ip
        WHERE ip.symbol = 'SPY'
    ),
    rel_strength AS (
        SELECT
            s.ticker,
            s.date,
            ROUND((s.ret_2  - sp.ret_2 )::numeric * 100, 2) AS rs_2d,
            ROUND((s.ret_5  - sp.ret_5 )::numeric * 100, 2) AS rs_5d,
            ROUND((s.ret_10 - sp.ret_10)::numeric * 100, 2) AS rs_10d,
            ROUND((s.ret_20 - sp.ret_20)::numeric * 100, 2) AS rs_20d,
            ROUND((s.ret_60 - sp.ret_60)::numeric * 100, 2) AS rs_60d
        FROM stock_returns s
        JOIN spy_returns sp ON sp.date = s.date
        JOIN latest_date ld ON s.date = ld.d
        WHERE s.ret_2 IS NOT NULL
          AND s.ret_5 IS NOT NULL
          AND s.ret_10 IS NOT NULL
          AND s.ret_20 IS NOT NULL
          AND s.ret_60 IS NOT NULL
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
    -- IBD / MarketSmith RS Rating: weighted trailing 12-month performance,
    -- most recent quarter (63 td) weighted 2x. Weighted strength is normalized
    -- against SPX's own weighted strength ((1+stock)/(1+spx)*100, Skyte method),
    -- then percentile-ranked 1-99 across the whole eligible universe.
    ibd_perf AS (
        SELECT
            o.ticker,
            o.date,
            o.close AS c0,
            LAG(o.close, 63)  OVER w AS c63,
            LAG(o.close, 126) OVER w AS c126,
            LAG(o.close, 189) OVER w AS c189,
            LAG(o.close, 252) OVER w AS c252
        FROM ohlc o
        WINDOW w AS (PARTITION BY o.ticker ORDER BY o.date)
    ),
    ibd_latest AS (
        SELECT
            p.ticker,
            (
                0.4 * (p.c0 / NULLIF(p.c63,  0) - 1)
              + 0.2 * (p.c0 / NULLIF(p.c126, 0) - 1)
              + 0.2 * (p.c0 / NULLIF(p.c189, 0) - 1)
              + 0.2 * (p.c0 / NULLIF(p.c252, 0) - 1)
            ) AS strength
        FROM ibd_perf p
        JOIN latest_date ld ON p.date = ld.d
        WHERE p.c63 IS NOT NULL AND p.c126 IS NOT NULL
          AND p.c189 IS NOT NULL AND p.c252 IS NOT NULL
    ),
    -- SPX (SPY) weighted strength: single scalar for the latest day
    spy_strength AS (
        SELECT
            (
                0.4 * (q.s0 / NULLIF(q.s63,  0) - 1)
              + 0.2 * (q.s0 / NULLIF(q.s126, 0) - 1)
              + 0.2 * (q.s0 / NULLIF(q.s189, 0) - 1)
              + 0.2 * (q.s0 / NULLIF(q.s252, 0) - 1)
            ) AS strength
        FROM (
            SELECT
                ip.date,
                ip.close_price AS s0,
                LAG(ip.close_price, 63)  OVER (ORDER BY ip.date) AS s63,
                LAG(ip.close_price, 126) OVER (ORDER BY ip.date) AS s126,
                LAG(ip.close_price, 189) OVER (ORDER BY ip.date) AS s189,
                LAG(ip.close_price, 252) OVER (ORDER BY ip.date) AS s252
            FROM index_prices ip
            WHERE ip.symbol = 'SPY'
        ) q
        JOIN latest_date ld ON q.date = ld.d
    ),
    ibd_ranked AS (
        SELECT
            il.ticker,
            ROUND(((1 + il.strength) / NULLIF(1 + ss.strength, 0) * 100)::numeric, 2) AS rs_vs_spy,
            NTILE(99) OVER (ORDER BY il.strength) AS rs_rating
        FROM ibd_latest il
        CROSS JOIN spy_strength ss
        JOIN market_cap_bucket mcb ON il.ticker = mcb.ticker
    ),
    -- RS Line (close / SPY close) new-high flag over trailing 252 trading days
    rs_line AS (
        SELECT
            o.ticker,
            o.date,
            o.close / NULLIF(ip.close_price, 0) AS rsl
        FROM ohlc o
        JOIN index_prices ip ON o.date = ip.date AND ip.symbol = 'SPY'
    ),
    rs_line_flagged AS (
        SELECT
            rl.ticker,
            rl.date,
            rl.rsl,
            MAX(rl.rsl) OVER (
                PARTITION BY rl.ticker ORDER BY rl.date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) AS rsl_max_252,
            COUNT(*) OVER (
                PARTITION BY rl.ticker ORDER BY rl.date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) AS cnt
        FROM rs_line rl
    ),
    rs_line_latest AS (
        SELECT
            f.ticker,
            (f.cnt >= 60 AND f.rsl >= f.rsl_max_252) AS rs_line_new_high
        FROM rs_line_flagged f
        JOIN latest_date ld ON f.date = ld.d
    ),
    ranked AS (
        SELECT
            rs.ticker,
            rs.rs_2d,
            rs.rs_5d,
            rs.rs_10d,
            rs.rs_20d,
            rs.rs_60d,
            NTILE(100) OVER (PARTITION BY mcb.mktcap_bucket ORDER BY rs.rs_2d)  AS rs_2d_rank,
            NTILE(100) OVER (PARTITION BY mcb.mktcap_bucket ORDER BY rs.rs_5d)  AS rs_5d_rank,
            NTILE(100) OVER (PARTITION BY mcb.mktcap_bucket ORDER BY rs.rs_10d) AS rs_10d_rank,
            NTILE(100) OVER (PARTITION BY mcb.mktcap_bucket ORDER BY rs.rs_20d) AS rs_20d_rank,
            NTILE(100) OVER (PARTITION BY mcb.mktcap_bucket ORDER BY rs.rs_60d) AS rs_60d_rank
        FROM rel_strength rs
        JOIN market_cap_bucket mcb ON rs.ticker = mcb.ticker
    )
    INSERT INTO rs_screener (
        ticker, company_name, sector, industry, market_cap, current_price,
        rs_2d, rs_5d, rs_10d, rs_20d, rs_60d,
        rs_2d_rank, rs_5d_rank, rs_10d_rank, rs_20d_rank, rs_60d_rank,
        rs_rating, rs_vs_spy, rs_line_new_high,
        updated_at
    )
    SELECT
        r.ticker,
        t.company_name,
        t.sector,
        t.industry,
        t.market_cap,
        sm.current_price,
        r.rs_2d, r.rs_5d, r.rs_10d, r.rs_20d, r.rs_60d,
        r.rs_2d_rank, r.rs_5d_rank, r.rs_10d_rank, r.rs_20d_rank, r.rs_60d_rank,
        ibd.rs_rating, ibd.rs_vs_spy, rll.rs_line_new_high,
        NOW()
    FROM ranked r
    JOIN tickers t ON r.ticker = t.ticker
    LEFT JOIN stock_metrics sm ON r.ticker = sm.ticker
    LEFT JOIN ibd_ranked ibd ON r.ticker = ibd.ticker
    LEFT JOIN rs_line_latest rll ON r.ticker = rll.ticker
    ON CONFLICT (ticker)
    DO UPDATE SET
        company_name = EXCLUDED.company_name,
        sector = EXCLUDED.sector,
        industry = EXCLUDED.industry,
        market_cap = EXCLUDED.market_cap,
        current_price = EXCLUDED.current_price,
        rs_2d = EXCLUDED.rs_2d,
        rs_5d = EXCLUDED.rs_5d,
        rs_10d = EXCLUDED.rs_10d,
        rs_20d = EXCLUDED.rs_20d,
        rs_60d = EXCLUDED.rs_60d,
        rs_2d_rank = EXCLUDED.rs_2d_rank,
        rs_5d_rank = EXCLUDED.rs_5d_rank,
        rs_10d_rank = EXCLUDED.rs_10d_rank,
        rs_20d_rank = EXCLUDED.rs_20d_rank,
        rs_60d_rank = EXCLUDED.rs_60d_rank,
        rs_rating = EXCLUDED.rs_rating,
        rs_vs_spy = EXCLUDED.rs_vs_spy,
        rs_line_new_high = EXCLUDED.rs_line_new_high,
        updated_at = NOW()
    """

    result = connection.execute(text(query))
    connection.commit()

    count_result = connection.execute(text("SELECT COUNT(*) FROM rs_screener"))
    total = count_result.scalar()
    logger.info(f"RS Screener table now has {total} rows")
    return total


def main():
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting RS Screener Update ===")
    logger.info("=" * 60)

    session, engine = init_db()

    try:
        with engine.connect() as connection:
            total = compute_rs_screener(connection)
            total_time = time.time() - overall_start
            write_summary(SCRIPT_NAME, 'SUCCESS', f'Updated {total} stocks', total, duration_seconds=total_time)

    except Exception as e:
        logger.error(f"Error in RS Screener update: {str(e)}")
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
        raise
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== RS Screener Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()
