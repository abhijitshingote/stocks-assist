#!/usr/bin/env python3
"""
Revenue Acceleration Update
Builds `revenue_acceleration` from the quarterly `earnings` table
(revenue_actual per report date, revenue_estimated for the next report).

Per ticker:
- Clean series: revenue_actual > 0, date <= today; actual outside EST_BAND x consensus
  dropped (FMP half-year/unit errors, e.g. CX, BUD); reports <= DUP_GAP_DAYS apart
  collapse to the later one; semiannual reporters (median gap > SEMI_GAP_DAYS) skipped;
  a quarter that is > SPIKE_X above/below both neighbours is dropped (milestone
  payments, annual/half-year figures landing on a quarter date).
- Quarterly YoY: each quarter vs the report closest to 365d earlier (300-430d window).
- rev_accel_1q / 2q: yoy_q0 - yoy_q1 / yoy_q2 (pp). rev_accel_streak: consecutive
  quarters with yoy rising, ending at q0.
- TTM YoY: sum(q0..q3) / sum(q4..q7) - 1; rev_ttm_accel = ttm_yoy - ttm_yoy one quarter earlier.
- Forward: next report's consensus vs year-ago quarter; rev_fwd_accel = fwd_yoy_est - yoy_q0.
- rev_accel_score = 100 * weighted log-ratio accel (see SCORE_W), so 10%->20% and
  100%->120% score comparably instead of pp-accel favouring tiny bases.
"""

import math
import os
import statistics
import sys
import time
from collections import defaultdict
from datetime import date, timedelta

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Base, RevenueAcceleration
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

SCRIPT_NAME = 'revenue_accel_update'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

LOOKBACK_DAYS = 1500
DUP_GAP_DAYS = 20
SEMI_GAP_DAYS = 140
STALE_DAYS = 150
SPIKE_X = 2.5
EST_BAND = (0.5, 2.0)
YOY_WIN = (300, 430)
N_SERIES = 8
SCORE_W = {'q1': 0.4, 'q2': 0.3, 'ttm': 0.3}


def init_db():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment variables")
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)(), engine


def pct(a, b):
    if a is None or b is None or b <= 0:
        return None
    return (a / b - 1) * 100


def log_accel(a, b):
    if a is None or b is None:
        return None
    return math.log(max(1 + a / 100, 0.1) / max(1 + b / 100, 0.1))


def clean_series(rows, today):
    """rows: [(date, rev_actual, rev_est)] ascending → reported [(d, rev, est)], next (d, est)."""
    reported, nxt = [], None
    for d, rev, est in rows:
        if d > today:
            if nxt is None and est and est > 0:
                nxt = (d, est)
            continue
        if not rev or rev <= 0:
            continue
        if est and est > 0 and not (EST_BAND[0] <= rev / est <= EST_BAND[1]):
            continue
        if reported and (d - reported[-1][0]).days <= DUP_GAP_DAYS:
            reported[-1] = (d, rev, est)
        else:
            reported.append((d, rev, est))

    keep = [True] * len(reported)
    for i in range(1, len(reported) - 1):
        r, p, n = reported[i][1], reported[i - 1][1], reported[i + 1][1]
        if (r > SPIKE_X * p and r > SPIKE_X * n) or (r * SPIKE_X < p and r * SPIKE_X < n):
            keep[i] = False
    return [q for q, k in zip(reported, keep) if k], nxt


def year_ago(quarters, d):
    """Revenue of the report closest to 365d before d, within YOY_WIN."""
    best, best_off = None, None
    for qd, rev, _ in quarters:
        gap = (d - qd).days
        if YOY_WIN[0] <= gap <= YOY_WIN[1]:
            off = abs(gap - 365)
            if best_off is None or off < best_off:
                best, best_off = rev, off
    return best


def ttm_yoy(desc, start):
    """TTM YoY using desc[start..start+3] vs desc[start+4..start+7]; None if gaps."""
    if len(desc) < start + 8:
        return None
    for i in (start, start + 3):
        gap = (desc[i][0] - desc[i + 4][0]).days
        if not (YOY_WIN[0] <= gap <= YOY_WIN[1]):
            return None
    cur = sum(q[1] for q in desc[start:start + 4])
    prev = sum(q[1] for q in desc[start + 4:start + 8])
    return pct(cur, prev)


def compute_ticker(rows, today):
    quarters, nxt = clean_series(rows, today)
    if len(quarters) < 6:
        return None
    gaps = [(quarters[i][0] - quarters[i - 1][0]).days for i in range(1, len(quarters))]
    if statistics.median(gaps[-8:]) > SEMI_GAP_DAYS:
        return None
    if (today - quarters[-1][0]).days > STALE_DAYS:
        return None

    desc = quarters[::-1]
    yoy = [pct(q[1], year_ago(quarters, q[0])) for q in desc]
    y = lambda i: yoy[i] if i < len(yoy) else None

    streak = 0
    for i in range(len(yoy) - 1):
        if yoy[i] is None or yoy[i + 1] is None or yoy[i] <= yoy[i + 1]:
            break
        streak += 1

    t0, t1 = ttm_yoy(desc, 0), ttm_yoy(desc, 1)
    q0d, q0_rev, q0_est = desc[0]

    fwd_yoy = None
    if nxt:
        fwd_yoy = pct(nxt[1], year_ago(quarters, nxt[0]))

    parts = {
        'q1': log_accel(y(0), y(1)),
        'q2': log_accel(y(0), y(2)),
        'ttm': log_accel(t0, t1),
    }
    score = None
    if parts['q1'] is not None:
        wsum = sum(SCORE_W[k] for k, v in parts.items() if v is not None)
        score = 100 * sum(SCORE_W[k] * v for k, v in parts.items() if v is not None) / wsum

    diff = lambda a, b: (a - b) if a is not None and b is not None else None
    rnd = lambda v, n=2: round(v, n) if v is not None else None

    series = [
        {'d': q[0].isoformat(), 'rev': q[1], 'yoy': rnd(yoy[i], 1)}
        for i, q in enumerate(desc[:N_SERIES])
    ][::-1]

    return {
        'q0_date': q0d,
        'q0_rev': q0_rev,
        'q0_rev_est': q0_est,
        'next_date': nxt[0] if nxt else None,
        'next_rev_est': nxt[1] if nxt else None,
        'ttm_rev': sum(q[1] for q in desc[:4]) if len(desc) >= 4 else None,
        'rev_yoy_q0': rnd(y(0)),
        'rev_yoy_q1': rnd(y(1)),
        'rev_yoy_q2': rnd(y(2)),
        'rev_yoy_q3': rnd(y(3)),
        'rev_qoq_q0': rnd(pct(desc[0][1], desc[1][1])),
        'rev_accel_1q': rnd(diff(y(0), y(1))),
        'rev_accel_2q': rnd(diff(y(0), y(2))),
        'rev_accel_streak': streak,
        'rev_ttm_yoy': rnd(t0),
        'rev_ttm_yoy_prev': rnd(t1),
        'rev_ttm_accel': rnd(diff(t0, t1)),
        'rev_fwd_yoy_est': rnd(fwd_yoy),
        'rev_fwd_accel': rnd(diff(fwd_yoy, y(0))),
        'rev_surprise_q0': rnd(pct(q0_rev, q0_est)),
        'rev_accel_score': rnd(score),
        'rev_quarters': series,
    }


def compute_revenue_acceleration(session):
    today = date.today()
    rows = session.execute(text("""
        SELECT e.ticker, e.date, e.revenue_actual, e.revenue_estimated
        FROM earnings e
        JOIN tickers t ON t.ticker = e.ticker
        WHERE t.is_actively_trading = TRUE
          AND e.date >= :since
        ORDER BY e.ticker, e.date
    """), {'since': today - timedelta(days=LOOKBACK_DAYS)}).fetchall()

    by_ticker = defaultdict(list)
    for tk, d, rev, est in rows:
        by_ticker[tk].append((d, rev, est))
    logger.info(f"Loaded {len(rows)} earnings rows for {len(by_ticker)} tickers")

    records = []
    for tk, series in by_ticker.items():
        rec = compute_ticker(series, today)
        if rec:
            rec['ticker'] = tk
            records.append(rec)
    logger.info(f"Computed {len(records)} tickers")

    session.query(RevenueAcceleration).delete()
    session.bulk_insert_mappings(RevenueAcceleration, records)
    session.commit()
    return len(records)


def main():
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting Revenue Acceleration Update ===")
    logger.info("=" * 60)

    session, _ = init_db()
    try:
        total = compute_revenue_acceleration(session)
        write_summary(SCRIPT_NAME, 'SUCCESS', f'Updated {total} stocks', total,
                      duration_seconds=time.time() - overall_start)
    except Exception as e:
        session.rollback()
        logger.error(f"Error in Revenue Acceleration update: {e}")
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=time.time() - overall_start)
        raise
    finally:
        session.close()
        logger.info(f"=== Revenue Acceleration Update Completed in "
                    f"{format_duration(time.time() - overall_start)} ===")
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()
