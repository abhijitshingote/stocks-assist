import argparse
import logging
from datetime import timedelta
from pathlib import Path
from typing import List, Optional

from sqlalchemy import func, desc

from models import Stock, Price, BlackList, init_db

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# -----------------------------------------------------------------------------
# Helper utilities
# -----------------------------------------------------------------------------

def get_last_n_trading_dates(session, n: int):
    """Return a list of the *last n* distinct trading dates (most-recent first)."""
    return [d[0] for d in session.query(Price.date).distinct().order_by(Price.date.desc()).limit(n).all()]


def human_number(val: float, dollars: bool = False):
    if val is None:
        return "N/A"
    unit = "$" if dollars else ""
    if val >= 1_000_000_000:
        return f"{unit}{val / 1_000_000_000:.1f}B"
    if val >= 1_000_000:
        return f"{unit}{val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"{unit}{val / 1_000:.1f}K"
    return f"{unit}{val:.0f}"


# -----------------------------------------------------------------------------
# Core diagnostic routine (mimics get_momentum_stocks criteria)
# -----------------------------------------------------------------------------

def diagnose_tickers(session, return_days: int, tickers: Optional[List[str]], above_200m: bool):
    # -- set up required dates -------------------------------------------------
    trading_dates = get_last_n_trading_dates(session, return_days + 1)
    if len(trading_dates) < return_days + 1:
        logger.error("Only %d trading dates available; need %d", len(trading_dates), return_days + 1)
        return

    latest_date = trading_dates[0]
    start_date = trading_dates[-1]

    ten_trading_dates = get_last_n_trading_dates(session, 10)
    ten_days_ago = ten_trading_dates[-1] if ten_trading_dates else latest_date

    # previous trading date for 1-day price change
    prev_trading_dates = get_last_n_trading_dates(session, 2)
    prev_date = prev_trading_dates[1] if len(prev_trading_dates) > 1 else latest_date

    # Build blacklist set to replicate exclusion
    blacklisted = {t[0] for t in session.query(BlackList.ticker).all()}

    # If user did not supply tickers, grab *all* actively-trading stocks
    if not tickers:
        tickers = [t[0] for t in session.query(Stock.ticker).all()]

    # Pre-fetch price snapshots we need for efficiency --------------------------------
    # today/ latest close
    today_prices = {
        t: p for t, p in session.query(Price.ticker, Price.close_price)
        .filter(Price.ticker.in_(tickers), Price.date == latest_date).all()
    }
    # start_date close (return measurement)
    start_prices = {
        t: p for t, p in session.query(Price.ticker, Price.close_price)
        .filter(Price.ticker.in_(tickers), Price.date == start_date).all()
    }
    # prev day close (for 1-day price change est – not part of filter but interesting)
    prev_prices = {
        t: p for t, p in session.query(Price.ticker, Price.close_price)
        .filter(Price.ticker.in_(tickers), Price.date == prev_date).all()
    }
    # 10-day averages
    avg_stats = {
        t: (avg_price, avg_dollar_vol, avg_vol)
        for t, avg_price, avg_dollar_vol, avg_vol in session.query(
            Price.ticker,
            func.avg(Price.close_price),
            func.avg(Price.volume * Price.close_price),
            func.avg(Price.volume),
        ).filter(
            Price.ticker.in_(tickers),
            Price.date >= ten_days_ago,
            Price.date <= latest_date,
        ).group_by(Price.ticker).all()
    }

    # -------------------------------------------------------------------------
    for tk in tickers:
        stock = session.query(Stock).filter(Stock.ticker == tk).first()
        if not stock:
            print(f"{tk}: not in Stock table")
            continue

        reasons = []

        # Blacklist check
        if tk in blacklisted:
            reasons.append("BLACKLISTED")

        # Market-cap filter
        if above_200m and (stock.market_cap or 0) < 200_000_000:
            reasons.append("market_cap < 200M")
        if not above_200m and (stock.market_cap or 0) >= 200_000_000:
            reasons.append("market_cap >= 200M")

        # Pricing snapshots ----------------------------------------------------
        today_price = today_prices.get(tk)
        start_price = start_prices.get(tk)
        if not start_price or start_price == 0:
            reasons.append("missing start_price")

        pct_move = None
        if today_price and start_price:
            pct_move = (today_price - start_price) / start_price * 100
            if pct_move < 5:
                reasons.append(f"{return_days}d_change < 5% ({pct_move:.1f}%)")
        else:
            reasons.append("cannot compute return")

        # 10-day stats ----------------------------------------------------------
        avg_price, avg_dollar_vol, avg_vol = avg_stats.get(tk, (None, None, None))
        if avg_price is None or avg_price < 5:
            reasons.append("avg_10day_price < $5")
        if avg_dollar_vol is None or avg_dollar_vol < 10_000_000:
            reasons.append("avg_dollar_vol < $10M")

        # Assemble report ------------------------------------------------------
        print("\n===", tk, stock.company_name, "===")
        print("latest: ", latest_date, " start: ", start_date)
        print("today price:", today_price, " start_price:", start_price, f"({pct_move:.1f}% if both)" if pct_move is not None else "")
        print("avg_10day_price:", avg_price, " avg_dollar_vol:", human_number(avg_dollar_vol, True))
        print("market_cap:", human_number(stock.market_cap, True))

        if reasons:
            print("→ FILTERED OUT. Reasons:")
            for r in reasons:
                print("   -", r)
        else:
            print("✅ PASSES all get_momentum_stocks criteria")


# -----------------------------------------------------------------------------
# CLI entrypoint
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Diagnose why specific tickers are excluded by get_momentum_stocks filter")
    parser.add_argument("--days", type=int, required=True, help="Return period (e.g. 1, 5, 20, 60, 120)")
    parser.add_argument("--tickers", type=str, help="Comma-separated list of tickers to inspect. If omitted, all tickers are processed (slow).")
    parser.add_argument("--below200m", action="store_true", help="Analyse the <200M market-cap branch instead of >=200M (default)")
    args = parser.parse_args()

    tickers_list = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None

    sess = init_db()
    try:
        diagnose_tickers(sess, args.days, tickers_list, above_200m=not args.below200m)
    finally:
        sess.close() 