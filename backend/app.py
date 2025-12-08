from flask import Flask, jsonify, request
from flask_cors import CORS
from models import Stock, Price, Comment, Flags, ConciseNote, Earnings, StockRSI, Index, IndexPrice, init_db
from sqlalchemy import desc, func, and_, or_, case, text
from datetime import datetime, timedelta
import os
import logging
import pytz
import requests
from dotenv import load_dotenv
import json
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Enable CORS for all routes
CORS(app, resources={r"/api/*": {"origins": "*"}})

session = init_db()

# Global default weights for All Returns
DEFAULT_PRIORITY_WEIGHTS = {
    '1d': 30,
    '5d': 20,
    '20d': 10,
    '60d': 1,
    '120d': 1
}

# Minimum % move thresholds per period – easy to tweak
RETURN_THRESHOLDS = {1: 5, 5: 10, 20: 15, 60: 20, 120: 30}

# Market cap categories
MARKET_CAP_CATEGORIES = {
    'micro': {'label': 'Micro Cap', 'min': 0, 'max': 200000000, 'description': 'Up to $200M'},
    'small': {'label': 'Small Cap', 'min': 200000000, 'max': 2000000000, 'description': '$200M - $2B'},
    'mid': {'label': 'Mid Cap', 'min': 2000000000, 'max': 20000000000, 'description': '$2B - $20B'},
    'large': {'label': 'Large Cap', 'min': 20000000000, 'max': 100000000000, 'description': '$20B - $100B'},
    'mega': {'label': 'Mega Cap', 'min': 100000000000, 'max': None, 'description': 'Over $100B'}
}

# ------------------------------------------------------------------
# Global sector / industry exclusions for all scanner pages
# ------------------------------------------------------------------
# Modify these sets to exclude entire sectors or industries from every
# screener endpoint. For example we start by excluding the
# Biotechnology industry.
GLOBAL_EXCLUDE = {
    'sector': set(),               # e.g. {'Healthcare'}
    'industry': {'Biotechnology'}  # e.g. {'Biotechnology', 'Shell Companies'}
}

def apply_global_exclude_filters(query):
    """Apply the GLOBAL_EXCLUDE filters to a SQLAlchemy query.

    The passed ``query`` *must* already include ``Stock`` in its FROM / JOIN
    clause. We then filter out any rows whose ``sector`` or ``industry``
    values match the configured sets above.
    """
    sectors    = GLOBAL_EXCLUDE.get('sector', set())
    industries = GLOBAL_EXCLUDE.get('industry', set())

    if sectors:
        query = query.filter(~Stock.sector.in_(list(sectors)))
    if industries:
        query = query.filter(~Stock.industry.in_(list(industries)))
    return query

def is_common_stock(symbol):
    """
    Conservative filter: excludes only obvious non-common-stock tickers.
    Keeps all others to avoid dropping legitimate common stocks.
    """
    if not symbol or not isinstance(symbol, str):
        return False

    s = symbol.upper().strip()

    # Allow foreign or class shares (BRK.B, RY.TO, etc.)
    if '.' in s:
        return True

    # Basic sanity check: exclude extremely long tickers
    if len(s) > 7:
        return False

    # Explicit exclusions by common suffix patterns
    bad_suffixes = (
        "W", "WS", "WT", "WW",  # Warrants
        "R", "RT",              # Rights
        "U",                    # Units (SPAC)
        "P", "L", "N", "M"      # Preferred or notes
    )

    # Exclude obvious "junk" patterns
    junk_keywords = ("WARRANT", "RIGHT", "UNIT")

    if any(kw in s for kw in junk_keywords):
        return False

    # Exclude certain suffixes only if ticker length > 4
    # (so we don't accidentally drop legit 1–4 letter tickers like FORD, SONY)
    for suf in bad_suffixes:
        if s.endswith(suf) and len(s) > len(suf) + 3:
            return False

    # Otherwise, keep it — err on inclusion
    return True

BACKUP_DIR = Path(os.getenv("USER_DATA_DIR", "user_data"))
BACKUP_DIR.mkdir(exist_ok=True)
COMMENTS_BACKUP_FILE = BACKUP_DIR / "comments_backup.json"  # NEW constant for comments backup

def get_eastern_datetime():
    """Get current datetime in Eastern Time (handles EST/EDT automatically)"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)

def get_rewind_date():
    """Get the rewind date from request parameters, if set.

    Returns:
        datetime.date or None: The rewind date if specified in the request, None otherwise
    """
    rewind_date_str = request.args.get('rewind_date')
    if rewind_date_str:
        try:
            return datetime.strptime(rewind_date_str, '%Y-%m-%d').date()
        except ValueError:
            logger.warning(f"Invalid rewind_date format: {rewind_date_str}")
    return None

# Economic Calendar Cache
ECONOMIC_CALENDAR_CACHE_FILE = BACKUP_DIR / "economic_calendar_cache.json"
CACHE_MAX_AGE_HOURS = 12  # Cache expires after 12 hours

def get_cached_economic_calendar():
    """
    Get economic calendar from cache if available and not expired
    
    Returns:
        dict or None: Cached data if valid, None otherwise
    """
    try:
        if not ECONOMIC_CALENDAR_CACHE_FILE.exists():
            return None
        
        with open(ECONOMIC_CALENDAR_CACHE_FILE, 'r') as f:
            cached = json.load(f)
        
        # Check if cache is still valid
        cached_time = datetime.strptime(cached['timestamp'], '%Y-%m-%d %H:%M:%S')
        cached_time = pytz.timezone('US/Eastern').localize(cached_time)
        current_time = get_eastern_datetime()
        
        age_hours = (current_time - cached_time).total_seconds() / 3600
        
        if age_hours < CACHE_MAX_AGE_HOURS:
            logger.info(f"Using cached economic calendar (age: {age_hours:.1f} hours)")
            cached['cached'] = True
            return cached
        else:
            logger.info(f"Cache expired (age: {age_hours:.1f} hours)")
            return None
            
    except Exception as e:
        logger.error(f"Error reading economic calendar cache: {str(e)}")
        return None

def save_economic_calendar_cache(data):
    """
    Save economic calendar data to cache file
    
    Args:
        data: Dictionary containing events and timestamp
    """
    try:
        with open(ECONOMIC_CALENDAR_CACHE_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved economic calendar to cache ({len(data.get('events', []))} events)")
    except Exception as e:
        logger.error(f"Error saving economic calendar cache: {str(e)}")

def get_filtered_price_query():
    """Get a Price query filtered by the rewind date if specified.

    If rewind_date is provided in request parameters, only returns price data
    up to that date. Otherwise returns all price data.

    Returns:
        SQLAlchemy Query: Base Price query with optional date filtering
    """
    base_query = session.query(Price)
    rewind_date = get_rewind_date()

    if rewind_date:
        base_query = base_query.filter(Price.date <= rewind_date)

    return base_query

def get_latest_price_date():
    """Get the most recent price date available, considering rewind_date if specified.

    Returns:
        datetime.date or None: The latest price date, or None if no prices found
    """
    try:
        query = get_filtered_price_query()
        latest_price = query.order_by(desc(Price.date)).first()

        if latest_price:
            return latest_price.date
        else:
            logger.warning("No price data found")
            return None

    except Exception as e:
        logger.error(f"Error getting latest price date: {str(e)}")
        return None

def get_trading_date_n_days_ago(reference_date, n_trading_days):
    """Get the date that is N trading days before the reference date.
    
    This function queries the Price table to find actual trading days,
    avoiding the calendar days bug where weekends/holidays are counted.
    
    Args:
        reference_date: The reference date (typically latest price date)
        n_trading_days: Number of trading days to go back
    
    Returns:
        datetime.date or None: The date N trading days ago, or None if not found
    """
    try:
        query = get_filtered_price_query()
        
        # Get distinct dates from Price table, ordered descending
        # Use a reference ticker (SPY or any liquid stock) or just get any distinct dates
        trading_dates = query.filter(
            Price.date <= reference_date
        ).with_entities(Price.date).distinct().order_by(desc(Price.date)).limit(n_trading_days + 1).all()
        
        if len(trading_dates) > n_trading_days:
            return trading_dates[n_trading_days].date
        elif trading_dates:
            # If we don't have enough data, return the oldest date we have
            return trading_dates[-1].date
        else:
            logger.warning(f"No trading dates found before {reference_date}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting trading date {n_trading_days} days ago: {str(e)}")
        return None

def process_citations_in_content(content, citations):
    """Process citations in content and return formatted content with citations"""
    if not citations or not content:
        return content

    # Create a mapping of citation markers to citation data
    citation_map = {}
    for citation in citations:
        marker = citation.get('marker', '')
        if marker:
            citation_map[marker] = citation

    # Replace citation markers with formatted citations
    processed_content = content
    for marker, citation_data in citation_map.items():
        url = citation_data.get('url', '')
        title = citation_data.get('title', '')

        if url and title:
            # Create a formatted citation link
            citation_html = f'<a href="{url}" target="_blank" rel="noopener noreferrer" class="citation-link">[{marker}] {title}</a>'
            processed_content = processed_content.replace(f'[{marker}]', citation_html)
        elif url:
            citation_html = f'<a href="{url}" target="_blank" rel="noopener noreferrer" class="citation-link">[{marker}]</a>'
            processed_content = processed_content.replace(f'[{marker}]', citation_html)

    return processed_content

def export_user_lists():
    """Export comments, flags, concise notes, and earnings to JSON backup files."""
    try:
        # Export comments
        comments = session.query(Comment).all()
        comments_data = []
        for comment in comments:
            comments_data.append({
                'id': comment.id,
                'ticker': comment.ticker,
                'comment_text': comment.comment_text,
                'comment_type': comment.comment_type,
                'status': comment.status,
                'ai_source': comment.ai_source,
                'reviewed_by': comment.reviewed_by,
                'reviewed_at': comment.reviewed_at.isoformat() if comment.reviewed_at else None,
                'created_at': comment.created_at.isoformat() if comment.created_at else None,
                'updated_at': comment.updated_at.isoformat() if comment.updated_at else None
            })

        with open(COMMENTS_BACKUP_FILE, 'w', encoding='utf-8') as f:
            json.dump(comments_data, f, indent=2, ensure_ascii=False)

        # Export flags
        flags = session.query(Flags).all()
        flags_data = []
        for flag in flags:
            flags_data.append({
                'ticker': flag.ticker,
                'is_reviewed': flag.is_reviewed,
                'is_shortlisted': flag.is_shortlisted,
                'is_blacklisted': flag.is_blacklisted
            })

        with open(BACKUP_DIR / 'flags_backup.json', 'w', encoding='utf-8') as f:
            json.dump(flags_data, f, indent=2, ensure_ascii=False)

        # Export concise notes
        concise_notes = session.query(ConciseNote).all()
        concise_notes_data = []
        for note in concise_notes:
            concise_notes_data.append({
                'ticker': note.ticker,
                'note': note.note
            })

        with open(BACKUP_DIR / 'concise_notes_backup.json', 'w', encoding='utf-8') as f:
            json.dump(concise_notes_data, f, indent=2, ensure_ascii=False)

        # Export earnings
        earnings = session.query(Earnings).all()
        earnings_data = []
        for earning in earnings:
            earnings_data.append({
                'ticker': earning.ticker,
                'earnings_date': earning.earnings_date.isoformat() if earning.earnings_date else None,
                'announcement_type': earning.announcement_type,
                'is_confirmed': earning.is_confirmed,
                'quarter': earning.quarter,
                'fiscal_year': earning.fiscal_year,
                'announcement_time': earning.announcement_time,
                'estimated_eps': earning.estimated_eps,
                'actual_eps': earning.actual_eps,
                'revenue_estimate': earning.revenue_estimate,
                'actual_revenue': earning.actual_revenue,
                'source': earning.source,
                'notes': earning.notes,
                'created_at': earning.created_at.isoformat() if earning.created_at else None,
                'updated_at': earning.updated_at.isoformat() if earning.updated_at else None
            })

        with open(BACKUP_DIR / 'earnings_backup.json', 'w', encoding='utf-8') as f:
            json.dump(earnings_data, f, indent=2, ensure_ascii=False)

        logger.info("Successfully exported user lists to backup files")

    except Exception as e:
        logger.error(f"Error exporting user lists: {str(e)}")

def enrich_stock_data(stocks_data):
    """
    Enrich stock data with additional fields:
    - volume_change (e.g., "5.2x")
    - week_52_range (e.g., "$10.50 - $25.30")
    - return_5d, return_20d, return_60d, return_120d
    
    Args:
        stocks_data: List of stock dictionaries with at least 'ticker' field
    
    Returns:
        Enriched list of stock dictionaries
    """
    if not stocks_data:
        return stocks_data
    
    try:
        latest_date = get_latest_price_date()
        if not latest_date:
            logger.warning("No latest price date found for enrichment")
            return stocks_data
        
        # Extract all tickers
        tickers = [stock['ticker'] for stock in stocks_data]
        
        # Build a query to get all necessary price data for all tickers at once
        # We need: latest volume, avg volume (last 20 days), 52-week high/low, historical prices for returns
        
        # Subquery for latest volume
        latest_volume_subquery = session.query(
            Price.ticker,
            Price.volume.label('latest_volume'),
            Price.close_price.label('latest_price')
        ).filter(
            Price.ticker.in_(tickers),
            Price.date == latest_date
        ).subquery()
        
        # Subquery for average volume (last 20 trading days)
        avg_volume_date = get_trading_date_n_days_ago(latest_date, 20)
        if avg_volume_date:
            avg_volume_subquery = session.query(
                Price.ticker,
                func.avg(Price.volume).label('avg_volume')
            ).filter(
                Price.ticker.in_(tickers),
                Price.date >= avg_volume_date,
                Price.date <= latest_date,
                Price.volume.isnot(None)
            ).group_by(Price.ticker).subquery()
        else:
            # If no trading date available, create empty subquery
            avg_volume_subquery = session.query(
                Price.ticker,
                func.avg(Price.volume).label('avg_volume')
            ).filter(False).subquery()
        
        # Subquery for 52-week high and low (252 trading days)
        week_52_date = get_trading_date_n_days_ago(latest_date, 252)
        if not week_52_date:
            # Skip if insufficient data
            week_52_subquery = session.query(
                Price.ticker,
                func.max(Price.close_price).label('high_52w'),
                func.min(Price.close_price).label('low_52w')
            ).filter(False).subquery()
        else:
            week_52_subquery = session.query(
                Price.ticker,
                func.max(Price.close_price).label('high_52w'),
                func.min(Price.close_price).label('low_52w')
            ).filter(
                Price.ticker.in_(tickers),
                Price.date >= week_52_date,
                Price.date <= latest_date,
                Price.close_price.isnot(None),
                Price.close_price > 0
            ).group_by(Price.ticker).subquery()
        
        # Get historical prices at different trading day periods
        trading_day_periods = {
            'price_5d': 5,
            'price_20d': 20,
            'price_60d': 60,
            'price_120d': 120
        }
        
        periods = {}
        for period_key, days in trading_day_periods.items():
            target_date = get_trading_date_n_days_ago(latest_date, days)
            if target_date:
                periods[period_key] = target_date
        
        # Query to get prices at each target trading day (with small window for exact match)
        historical_prices = {}
        for period_key, target_date in periods.items():
            # Get price on or very near the target date (1 day window for exact trading day)
            window_start = target_date - timedelta(days=1)
            window_end = target_date + timedelta(days=1)
            
            period_prices = session.query(
                Price.ticker,
                func.avg(Price.close_price).label('avg_price')
            ).filter(
                Price.ticker.in_(tickers),
                Price.date >= window_start,
                Price.date <= window_end,
                Price.close_price.isnot(None),
                Price.close_price > 0
            ).group_by(Price.ticker).all()
            
            historical_prices[period_key] = {row.ticker: row.avg_price for row in period_prices}
        
        # Execute all subqueries
        volume_data = session.query(
            latest_volume_subquery.c.ticker,
            latest_volume_subquery.c.latest_volume,
            latest_volume_subquery.c.latest_price,
            avg_volume_subquery.c.avg_volume,
            week_52_subquery.c.high_52w,
            week_52_subquery.c.low_52w
        ).outerjoin(
            avg_volume_subquery,
            latest_volume_subquery.c.ticker == avg_volume_subquery.c.ticker
        ).outerjoin(
            week_52_subquery,
            latest_volume_subquery.c.ticker == week_52_subquery.c.ticker
        ).all()
        
        # Build lookup dictionary
        enrichment_data = {}
        for row in volume_data:
            enrichment_data[row.ticker] = {
                'latest_volume': row.latest_volume,
                'latest_price': row.latest_price,
                'avg_volume': row.avg_volume,
                'high_52w': row.high_52w,
                'low_52w': row.low_52w
            }
        
        # Get previous trading day (1 trading day ago)
        previous_date = get_trading_date_n_days_ago(latest_date, 1)
        
        # Query previous day prices (only if we have a valid previous date)
        previous_prices = []
        if previous_date:
            previous_prices = session.query(
                Price.ticker,
                Price.close_price.label('prev_close')
            ).filter(
                Price.ticker.in_(tickers),
                Price.date == previous_date,
                Price.close_price.isnot(None)
            ).all()
        
        # Add previous prices to enrichment data
        for row in previous_prices:
            if row.ticker in enrichment_data:
                enrichment_data[row.ticker]['prev_close'] = row.prev_close
        
        # Enrich each stock
        for stock in stocks_data:
            ticker = stock['ticker']
            enrich = enrichment_data.get(ticker, {})
            
            # Volume change
            latest_vol = enrich.get('latest_volume')
            avg_vol = enrich.get('avg_volume')
            if latest_vol and avg_vol and avg_vol > 0:
                vol_change_ratio = latest_vol / avg_vol
                stock['volume_change'] = f"{vol_change_ratio:.1f}x"
            else:
                stock['volume_change'] = None
            
            # 52-week range
            high_52w = enrich.get('high_52w')
            low_52w = enrich.get('low_52w')
            if high_52w and low_52w:
                stock['week_52_range'] = f"${low_52w:.2f} - ${high_52w:.2f}"
            else:
                stock['week_52_range'] = None
            
            # Calculate 1-day price change (for "Current Price" column display)
            current_price = stock.get('current_price') or stock.get('end_price') or stock.get('price') or enrich.get('latest_price')
            prev_close = enrich.get('prev_close')
            
            if current_price and prev_close and prev_close > 0:
                price_change_1d = current_price - prev_close
                price_change_pct_1d = (price_change_1d / prev_close) * 100
                # Override the price_change and price_change_pct with 1-day values
                stock['price_change'] = round(price_change_1d, 2)
                stock['price_change_pct'] = f"{'+' if price_change_pct_1d >= 0 else ''}{round(price_change_pct_1d, 2)}%"
            
            # Calculate returns for different periods
            
            if current_price:
                # 5-day return
                price_5d = historical_prices['price_5d'].get(ticker)
                if price_5d and price_5d > 0:
                    return_5d_pct = ((current_price - price_5d) / price_5d) * 100
                    stock['return_5d'] = f"{'+' if return_5d_pct >= 0 else ''}{return_5d_pct:.2f}%"
                else:
                    stock['return_5d'] = None
                
                # 20-day return
                price_20d = historical_prices['price_20d'].get(ticker)
                if price_20d and price_20d > 0:
                    return_20d_pct = ((current_price - price_20d) / price_20d) * 100
                    stock['return_20d'] = f"{'+' if return_20d_pct >= 0 else ''}{return_20d_pct:.2f}%"
                else:
                    stock['return_20d'] = None
                
                # 60-day return
                price_60d = historical_prices['price_60d'].get(ticker)
                if price_60d and price_60d > 0:
                    return_60d_pct = ((current_price - price_60d) / price_60d) * 100
                    stock['return_60d'] = f"{'+' if return_60d_pct >= 0 else ''}{return_60d_pct:.2f}%"
                else:
                    stock['return_60d'] = None
                
                # 120-day return
                price_120d = historical_prices['price_120d'].get(ticker)
                if price_120d and price_120d > 0:
                    return_120d_pct = ((current_price - price_120d) / price_120d) * 100
                    stock['return_120d'] = f"{'+' if return_120d_pct >= 0 else ''}{return_120d_pct:.2f}%"
                else:
                    stock['return_120d'] = None
        
        return stocks_data
        
    except Exception as e:
        logger.error(f"Error enriching stock data: {str(e)}")
        # Return original data if enrichment fails
        return stocks_data

def get_momentum_stocks(return_days, above_200m=None, min_return_pct=None, market_cap_category=None):
    """Get stocks with strong momentum over specified period
    
    Args:
        return_days: Number of trading days for the return calculation
        above_200m: (Deprecated) Boolean for backward compatibility
        min_return_pct: Minimum return percentage threshold
        market_cap_category: One of 'micro', 'small', 'mid', 'large', 'mega'
    """
    try:
        # Determine date range based on trading days
        end_date = get_latest_price_date()
        if not end_date:
            return []

        # Use trading days instead of calendar days
        start_date = get_trading_date_n_days_ago(end_date, return_days)
        if not start_date:
            logger.warning(f"Insufficient data to calculate {return_days} trading days ago")
            return []

        # Base query joining Stock and Price tables
        # We'll get OHLCV data from the latest price record in a subquery
        latest_price_subquery = session.query(
            Price.ticker,
            Price.volume,
            Price.open_price.label('open_price'),
            Price.high_price.label('high_price'),
            Price.low_price.label('low_price'),
            Price.close_price.label('latest_close')
        ).filter(
            Price.date == end_date
        ).subquery()
        
        base_query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            Stock.country,
            func.min(Price.close_price).label('start_price'),
            latest_price_subquery.c.latest_close.label('end_price'),
            ((latest_price_subquery.c.latest_close - func.min(Price.close_price)) / func.min(Price.close_price) * 100).label('return_pct'),
            latest_price_subquery.c.volume.label('volume'),
            latest_price_subquery.c.open_price.label('open_price'),
            latest_price_subquery.c.high_price.label('high_price'),
            latest_price_subquery.c.low_price.label('low_price')
        ).join(Price, Stock.ticker == Price.ticker
        ).outerjoin(latest_price_subquery, Stock.ticker == latest_price_subquery.c.ticker)

        # Apply date filtering
        base_query = base_query.filter(Price.date >= start_date, Price.date <= end_date)

        # Filter out invalid prices (null, zero, or negative)
        base_query = base_query.filter(Price.close_price > 0)

        # Apply market cap filter
        if market_cap_category:
            # Use new category system
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                base_query = base_query.filter(Stock.market_cap >= category['min'])
                if category['max'] is not None:
                    base_query = base_query.filter(Stock.market_cap < category['max'])
        elif above_200m is not None:
            # Backward compatibility with old system
            if above_200m:
                base_query = base_query.filter(Stock.market_cap >= 200000000)
            else:
                base_query = base_query.filter(Stock.market_cap < 200000000)

        # Apply global exclusions
        base_query = apply_global_exclude_filters(base_query)

        # Group by stock first (required before HAVING clause)
        base_query = base_query.group_by(
            Stock.ticker, Stock.company_name, Stock.sector,
            Stock.industry, Stock.market_cap, Stock.price, Stock.country,
            latest_price_subquery.c.volume, latest_price_subquery.c.open_price,
            latest_price_subquery.c.high_price, latest_price_subquery.c.low_price,
            latest_price_subquery.c.latest_close
        )

        # Apply minimum return percentage filter using HAVING (not WHERE)
        if min_return_pct is not None:
            base_query = base_query.having(
                ((latest_price_subquery.c.latest_close - func.min(Price.close_price)) / func.min(Price.close_price) * 100) >= min_return_pct
            )
        else:
            # Use default threshold based on period
            threshold = RETURN_THRESHOLDS.get(return_days, 10)
            base_query = base_query.having(
                ((latest_price_subquery.c.latest_close - func.min(Price.close_price)) / func.min(Price.close_price) * 100) >= threshold
            )

        # Order by return percentage and get results
        stocks = base_query.order_by(desc('return_pct')).limit(100).all()

        # Format results and filter for common stocks only
        results = []
        for stock in stocks:
            # Skip non-common stocks (warrants, rights, units, preferred)
            if not is_common_stock(stock.ticker):
                continue
                
            volume = stock.volume if stock.volume else 0
            dollar_volume = (stock.end_price * volume) if volume else 0
            
            # Calculate price change for display
            price_change = stock.end_price - stock.start_price
            price_change_pct = stock.return_pct
            
            results.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'price': stock.price,
                'country': stock.country,
                'start_price': round(stock.start_price, 2),
                'end_price': round(stock.end_price, 2),
                'return_pct': round(stock.return_pct, 2),
                'current_price': round(stock.end_price, 2),
                'price_change': round(price_change, 2),
                'price_change_pct': f"{'+' if price_change_pct >= 0 else ''}{round(price_change_pct, 2)}%",
                'volume': int(volume) if volume else None,
                'dollar_volume': round(dollar_volume, 2) if dollar_volume else None,
                'open': round(stock.open_price, 2) if stock.open_price else None,
                'high': round(stock.high_price, 2) if stock.high_price else None,
                'low': round(stock.low_price, 2) if stock.low_price else None,
                'close': round(stock.end_price, 2)
            })

        # Enrich with additional data
        results = enrich_stock_data(results)
        return results

    except Exception as e:
        logger.error(f"Error getting momentum stocks for {return_days}d: {str(e)}")
        return []

def get_gapper_stocks(above_200m=None, market_cap_category=None):
    """Get stocks that gapped up significantly
    
    Args:
        above_200m: (Deprecated) Boolean for backward compatibility
        market_cap_category: One of 'micro', 'small', 'mid', 'large', 'mega'
    """
    try:
        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        # Find previous trading day (1 trading day ago)
        previous_date = get_trading_date_n_days_ago(latest_date, 1)
        if not previous_date:
            logger.warning("Insufficient data to find previous trading day for gapper stocks")
            return []

        # Subquery for previous day's close
        prev_close_subquery = session.query(
            Price.ticker,
            Price.close_price.label('prev_close')
        ).filter(
            Price.date == previous_date,
            Price.close_price.isnot(None)
        ).subquery()

        # Subquery for today's open
        today_open_subquery = session.query(
            Price.ticker,
            Price.open_price.label('today_open')
        ).filter(
            Price.date == latest_date,
            Price.open_price.isnot(None)
        ).subquery()

        # Main query joining both subqueries
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            prev_close_subquery.c.prev_close,
            today_open_subquery.c.today_open
        ).join(
            prev_close_subquery, Stock.ticker == prev_close_subquery.c.ticker
        ).join(
            today_open_subquery, Stock.ticker == today_open_subquery.c.ticker
        )

        # Apply market cap filter
        if market_cap_category:
            # Use new category system
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                query = query.filter(Stock.market_cap >= category['min'])
                if category['max'] is not None:
                    query = query.filter(Stock.market_cap < category['max'])
        elif above_200m is not None:
            # Backward compatibility with old system
            if above_200m:
                query = query.filter(Stock.market_cap >= 200000000)
            else:
                query = query.filter(Stock.market_cap < 200000000)

        # Apply global exclusions
        query = apply_global_exclude_filters(query)

        # Get results
        gap_stocks = query.all()

        # Calculate gap percentages and filter for common stocks
        results = []
        for stock in gap_stocks:
            # Skip non-common stocks (warrants, rights, units, preferred)
            if not is_common_stock(stock.ticker):
                continue
                
            if stock.prev_close and stock.today_open and stock.prev_close > 0:
                gap_pct = ((stock.today_open - stock.prev_close) / stock.prev_close) * 100
                if gap_pct >= 5:  # Minimum 5% gap
                    price_change = stock.today_open - stock.prev_close
                    current_price = stock.today_open if stock.today_open else stock.price
                    
                    results.append({
                        'ticker': stock.ticker,
                        'company_name': stock.company_name,
                        'sector': stock.sector,
                        'industry': stock.industry,
                        'market_cap': stock.market_cap,
                        'price': current_price,
                        'current_price': round(current_price, 2),
                        'start_price': round(stock.prev_close, 2),
                        'end_price': round(stock.today_open, 2),
                        'price_change': round(price_change, 2),
                        'price_change_pct': f"{'+' if gap_pct >= 0 else ''}{round(gap_pct, 2)}%",
                        'return_pct': round(gap_pct, 2),
                        'prev_close': round(stock.prev_close, 2),
                        'today_open': round(stock.today_open, 2),
                        'gap_pct': round(gap_pct, 2),
                        'volume': None,  # Will be enriched
                        'dollar_volume': None  # Will be enriched
                    })

        # Sort by gap percentage descending and limit to top 100
        results.sort(key=lambda x: x['gap_pct'], reverse=True)
        results = results[:100]
        
        # Enrich with additional data
        results = enrich_stock_data(results)
        return results

    except Exception as e:
        logger.error(f"Error getting gapper stocks: {str(e)}")
        return []

def get_volume_spike_stocks(above_200m=None, market_cap_category=None):
    """Get stocks with significant volume spikes using trading days
    
    Args:
        above_200m: (Deprecated) Boolean for backward compatibility
        market_cap_category: One of 'micro', 'small', 'mid', 'large', 'mega'
    """
    try:
        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        # Calculate average volume over last 20 trading days
        avg_volume_start = get_trading_date_n_days_ago(latest_date, 20)
        if not avg_volume_start:
            logger.warning("Insufficient data to calculate 20-day average volume for volume spike stocks")
            return []
        
        avg_volume_query = session.query(
            Price.ticker,
            func.avg(Price.volume).label('avg_volume')
        ).filter(
            Price.date >= avg_volume_start
        ).group_by(Price.ticker).subquery()

        # Get today's volume and compare with average
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            Price.volume.label('today_volume'),
            Price.close_price.label('today_close'),
            Price.open_price.label('today_open'),
            avg_volume_query.c.avg_volume
        ).join(Price, Stock.ticker == Price.ticker)\
         .join(avg_volume_query, Stock.ticker == avg_volume_query.c.ticker)\
         .filter(Price.date == latest_date)

        # Apply market cap filter
        if market_cap_category:
            # Use new category system
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                query = query.filter(Stock.market_cap >= category['min'])
                if category['max'] is not None:
                    query = query.filter(Stock.market_cap < category['max'])
        elif above_200m is not None:
            # Backward compatibility with old system
            if above_200m:
                query = query.filter(Stock.market_cap >= 200000000)
            else:
                query = query.filter(Stock.market_cap < 200000000)

        # Apply global exclusions
        query = apply_global_exclude_filters(query)

        volume_stocks = query.all()

        # Calculate volume ratios and filter for common stocks
        results = []
        for stock in volume_stocks:
            # Skip non-common stocks (warrants, rights, units, preferred)
            if not is_common_stock(stock.ticker):
                continue
                
            if stock.avg_volume and stock.avg_volume > 0 and stock.today_volume:
                volume_ratio = stock.today_volume / stock.avg_volume
                if volume_ratio >= 2:  # At least 2x average volume
                    # Calculate price change
                    price_change = stock.today_close - stock.today_open if (stock.today_close and stock.today_open) else 0
                    price_change_pct = ((price_change / stock.today_open) * 100) if (stock.today_open and stock.today_open > 0) else 0
                    current_price = stock.today_close if stock.today_close else stock.price
                    
                    # Calculate dollar volume
                    dollar_volume = (current_price * stock.today_volume) if (current_price and stock.today_volume) else 0
                    
                    results.append({
                        'ticker': stock.ticker,
                        'company_name': stock.company_name,
                        'sector': stock.sector,
                        'industry': stock.industry,
                        'market_cap': stock.market_cap,
                        'price': current_price,
                        'current_price': round(current_price, 2) if current_price else None,
                        'start_price': round(stock.today_open, 2) if stock.today_open else None,
                        'end_price': round(stock.today_close, 2) if stock.today_close else None,
                        'price_change': round(price_change, 2),
                        'price_change_pct': f"{'+' if price_change_pct >= 0 else ''}{round(price_change_pct, 2)}%",
                        'return_pct': round(price_change_pct, 2),
                        'volume': int(stock.today_volume),
                        'dollar_volume': round(dollar_volume, 2) if dollar_volume else None,
                        'today_volume': int(stock.today_volume),
                        'avg_volume': round(stock.avg_volume, 0),
                        'volume_ratio': round(volume_ratio, 1),
                        'volume_change': f"{volume_ratio:.1f}x"
                    })

        # Sort by volume ratio descending and limit to top 100
        results.sort(key=lambda x: x['volume_ratio'], reverse=True)
        results = results[:100]
        
        # Enrich with additional data
        results = enrich_stock_data(results)
        return results

    except Exception as e:
        logger.error(f"Error getting volume spike stocks: {str(e)}")
        return []

def get_sea_change_stocks(above_200m=True):
    """Get stocks that have changed direction recently (sea change) using trading days"""
    try:
        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        # Get price data for last 60 trading days
        end_date = latest_date
        start_date = get_trading_date_n_days_ago(end_date, 60)
        if not start_date:
            logger.warning("Insufficient data to calculate 60 trading days ago for sea change stocks")
            return []
        
        # Get date for 10 trading days ago
        date_10d_ago = get_trading_date_n_days_ago(end_date, 10)
        if not date_10d_ago:
            logger.warning("Insufficient data to calculate 10 trading days ago for sea change stocks")
            return []

        # Query for stocks with recent direction change
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            func.avg(Price.close_price).label('avg_price_60d'),
            func.avg(case((Price.date >= date_10d_ago, Price.close_price), else_=None)).label('avg_price_10d'),
            func.max(Price.close_price).label('max_60d'),
            func.min(Price.close_price).label('min_60d')
        ).join(Price, Stock.ticker == Price.ticker)\
         .filter(Price.date >= start_date, Price.date <= end_date)

        # Apply market cap filter
        if above_200m:
            query = query.filter(Stock.market_cap >= 200000000)
        else:
            query = query.filter(Stock.market_cap < 200000000)

        # Apply global exclusions
        query = apply_global_exclude_filters(query)

        sea_change_stocks = query.group_by(
            Stock.ticker, Stock.company_name, Stock.sector,
            Stock.industry, Stock.market_cap, Stock.price
        ).all()

        results = []
        for stock in sea_change_stocks:
            # Skip non-common stocks (warrants, rights, units, preferred)
            if not is_common_stock(stock.ticker):
                continue
                
            if stock.avg_price_60d and stock.avg_price_10d:
                # Calculate momentum shift
                momentum_shift = ((stock.avg_price_10d - stock.avg_price_60d) / stock.avg_price_60d) * 100

                # Calculate volatility (range/high - low)
                if stock.max_60d and stock.min_60d and stock.max_60d > stock.min_60d:
                    volatility = ((stock.max_60d - stock.min_60d) / stock.min_60d) * 100
                else:
                    volatility = 0

                # Only include significant momentum shifts
                if abs(momentum_shift) >= 5:  # At least 5% momentum shift
                    results.append({
                        'ticker': stock.ticker,
                        'company_name': stock.company_name,
                        'sector': stock.sector,
                        'industry': stock.industry,
                        'market_cap': stock.market_cap,
                        'price': stock.price,
                        'avg_price_60d': round(stock.avg_price_60d, 2),
                        'avg_price_10d': round(stock.avg_price_10d, 2),
                        'momentum_shift_pct': round(momentum_shift, 2),
                        'volatility_pct': round(volatility, 2)
                    })

        # Sort by absolute momentum shift descending and limit to top 100
        results.sort(key=lambda x: abs(x['momentum_shift_pct']), reverse=True)
        results = results[:100]
        
        # Enrich with additional data
        results = enrich_stock_data(results)
        return results

    except Exception as e:
        logger.error(f"Error getting sea change stocks: {str(e)}")
        return []

def get_all_returns_data(above_200m=True, weights=None):
    """Calculate all returns data with weighted scoring using trading days"""
    try:
        if weights is None:
            weights = DEFAULT_PRIORITY_WEIGHTS

        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        # Define periods for return calculation (in trading days)
        periods = [
            (1, '1d_return'),
            (5, '5d_return'),
            (20, '20d_return'),
            (60, '60d_return'),
            (120, '120d_return')
        ]

        # Build dynamic query for returns using trading days
        query_parts = []
        for days, label in periods:
            # Get the actual trading date N days ago
            start_date = get_trading_date_n_days_ago(latest_date, days)
            if start_date:
                query_parts.append(
                    func.avg(case((Price.date >= start_date, Price.close_price), else_=None)).label(f'{label}_avg')
                )
            else:
                # If we can't get the trading date, add null placeholder
                query_parts.append(
                    func.avg(case((False, Price.close_price), else_=None)).label(f'{label}_avg')
                )

        # Get start date for overall query filter (120 trading days back)
        filter_start_date = get_trading_date_n_days_ago(latest_date, 120)
        if not filter_start_date:
            logger.warning("Insufficient data to calculate 120 trading days ago for all returns")
            return []

        # Main query
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            *query_parts
        ).join(Price, Stock.ticker == Price.ticker)\
         .filter(Price.date >= filter_start_date)

        # Apply market cap filter
        if above_200m:
            query = query.filter(Stock.market_cap >= 200000000)
        else:
            query = query.filter(Stock.market_cap < 200000000)

        # Apply global exclusions
        query = apply_global_exclude_filters(query)

        stocks = query.group_by(
            Stock.ticker, Stock.company_name, Stock.sector,
            Stock.industry, Stock.market_cap, Stock.price
        ).all()

        results = []
        for stock in stocks:
            # Skip non-common stocks (warrants, rights, units, preferred)
            if not is_common_stock(stock.ticker):
                continue
                
            stock_data = {
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'price': stock.price
            }

            # Calculate returns for each period
            current_price = stock.price
            total_score = 0

            for days, label in periods:
                period_avg = getattr(stock, f'{label}_avg')
                if period_avg and current_price:
                    return_pct = ((current_price - period_avg) / period_avg) * 100
                    stock_data[label.replace('_return', '_pct')] = round(return_pct, 2)

                    # Add weighted score
                    weight = weights.get(label.replace('_return', 'd'), 0)
                    total_score += return_pct * weight
                else:
                    stock_data[label.replace('_return', '_pct')] = None

            stock_data['total_score'] = round(total_score, 2)
            results.append(stock_data)

        # Sort by total score descending and limit to top 100
        results.sort(key=lambda x: x.get('total_score', 0), reverse=True)
        results = results[:100]
        
        # Enrich with additional data
        results = enrich_stock_data(results)
        return results

    except Exception as e:
        logger.error(f"Error getting all returns data: {str(e)}")
        return []

def get_all_returns_data_for_tickers(tickers, above_200m=None):
    """Get all returns data for specific tickers using trading days"""
    try:
        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        # Define periods for return calculation (in trading days)
        periods = [
            (1, '1d_return'),
            (5, '5d_return'),
            (20, '20d_return'),
            (60, '60d_return'),
            (120, '120d_return')
        ]

        # Build dynamic query for returns using trading days
        query_parts = []
        for days, label in periods:
            # Get the actual trading date N days ago
            start_date = get_trading_date_n_days_ago(latest_date, days)
            if start_date:
                query_parts.append(
                    func.avg(case((Price.date >= start_date, Price.close_price), else_=None)).label(f'{label}_avg')
                )
            else:
                # If we can't get the trading date, add null placeholder
                query_parts.append(
                    func.avg(case((False, Price.close_price), else_=None)).label(f'{label}_avg')
                )

        # Get start date for overall query filter (120 trading days back)
        filter_start_date = get_trading_date_n_days_ago(latest_date, 120)
        if not filter_start_date:
            logger.warning("Insufficient data to calculate 120 trading days ago for ticker returns")
            return []

        # Main query
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            *query_parts
        ).join(Price, Stock.ticker == Price.ticker)\
         .filter(Price.date >= filter_start_date)\
         .filter(Stock.ticker.in_(tickers))

        # Apply market cap filter if specified
        if above_200m is not None:
            if above_200m:
                query = query.filter(Stock.market_cap >= 200000000)
            else:
                query = query.filter(Stock.market_cap < 200000000)

        stocks = query.group_by(
            Stock.ticker, Stock.company_name, Stock.sector,
            Stock.industry, Stock.market_cap, Stock.price
        ).all()

        results = []
        for stock in stocks:
            # Skip non-common stocks (warrants, rights, units, preferred)
            if not is_common_stock(stock.ticker):
                continue
                
            stock_data = {
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'price': stock.price
            }

            # Calculate returns for each period
            current_price = stock.price
            total_score = 0

            for days, label in periods:
                period_avg = getattr(stock, f'{label}_avg')
                if period_avg and current_price:
                    return_pct = ((current_price - period_avg) / period_avg) * 100
                    stock_data[label.replace('_return', '_pct')] = round(return_pct, 2)

                    # Add weighted score
                    weight = DEFAULT_PRIORITY_WEIGHTS.get(label.replace('_return', 'd'), 0)
                    total_score += return_pct * weight
                else:
                    stock_data[label.replace('_return', '_pct')] = None

            stock_data['total_score'] = round(total_score, 2)
            results.append(stock_data)

        # Enrich with additional data
        results = enrich_stock_data(results)
        return results

    except Exception as e:
        logger.error(f"Error getting all returns data for tickers: {str(e)}")
        return []

# API Routes Start Here

# Comments API endpoints
@app.route('/api/comments/<ticker>')
def get_comments(ticker):
    """Get all active comments (user + approved AI) for a specific ticker"""
    try:
        ticker = ticker.upper()

        # Get user comments and approved AI comments
        comments = session.query(Comment).filter(
            Comment.ticker == ticker,
            ((Comment.comment_type == 'user') & (Comment.status == 'active')) |
            ((Comment.comment_type == 'ai') & (Comment.status == 'approved'))
        ).order_by(desc(Comment.created_at)).all()

        comments_data = []
        for comment in comments:
            comments_data.append({
                'id': comment.id,
                'ticker': comment.ticker,
                'comment_text': comment.comment_text,
                'comment_type': comment.comment_type,
                'status': comment.status,
                'ai_source': comment.ai_source,
                'created_at': comment.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'formatted_date': comment.created_at.strftime('%B %d, %Y at %I:%M %p')
            })

        return jsonify(comments_data)

    except Exception as e:
        logger.error(f"Error getting comments for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/comments/<ticker>', methods=['POST'])
def add_comment(ticker):
    """Add a new user comment for a specific ticker"""
    try:
        ticker = ticker.upper()
        data = request.get_json()

        if not data or 'comment_text' not in data:
            return jsonify({'error': 'Comment text is required'}), 400

        comment_text = data['comment_text'].strip()
        if not comment_text:
            return jsonify({'error': 'Comment text cannot be empty'}), 400

        # Verify ticker exists
        stock = session.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return jsonify({'error': 'Ticker not found'}), 404

        # Create new user comment
        new_comment = Comment(
            ticker=ticker,
            comment_text=comment_text,
            comment_type='user',
            status='active'
        )

        session.add(new_comment)
        session.commit()
        export_user_lists()  # persist changes including comments

        # Return the created comment
        comment_data = {
            'id': new_comment.id,
            'ticker': new_comment.ticker,
            'comment_text': new_comment.comment_text,
            'comment_type': new_comment.comment_type,
            'status': new_comment.status,
            'ai_source': new_comment.ai_source,
            'created_at': new_comment.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'formatted_date': new_comment.created_at.strftime('%B %d, %Y at %I:%M %p')
        }

        return jsonify(comment_data), 201

    except Exception as e:
        logger.error(f"Error adding comment for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/ai-comments/<ticker>')
def get_pending_ai_comments(ticker):
    """Get pending AI comments for review"""
    try:
        ticker = ticker.upper()

        comments = session.query(Comment).filter(
            Comment.ticker == ticker,
            Comment.comment_type == 'ai',
            Comment.status == 'pending'
        ).order_by(desc(Comment.created_at)).all()

        comments_data = []
        for comment in comments:
            comments_data.append({
                'id': comment.id,
                'ticker': comment.ticker,
                'comment_text': comment.comment_text,
                'comment_type': comment.comment_type,
                'status': comment.status,
                'ai_source': comment.ai_source,
                'created_at': comment.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'formatted_date': comment.created_at.strftime('%B %d, %Y at %I:%M %p')
            })

        return jsonify(comments_data)

    except Exception as e:
        logger.error(f"Error getting pending AI comments for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/ai-comments/<ticker>', methods=['POST'])
def add_ai_comment(ticker):
    """Add a new AI-generated comment for a specific ticker"""
    try:
        ticker = ticker.upper()
        data = request.get_json()

        if not data or 'comment_text' not in data:
            return jsonify({'error': 'Comment text is required'}), 400

        comment_text = data['comment_text'].strip()
        if not comment_text:
            return jsonify({'error': 'Comment text cannot be empty'}), 400

        # Verify ticker exists
        stock = session.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return jsonify({'error': 'Ticker not found'}), 404

        # Create new AI comment
        new_comment = Comment(
            ticker=ticker,
            comment_text=comment_text,
            comment_type='ai',
            status='pending',
            ai_source=data.get('ai_source')
        )

        session.add(new_comment)
        session.commit()

        # Return the created comment
        comment_data = {
            'id': new_comment.id,
            'ticker': new_comment.ticker,
            'comment_text': new_comment.comment_text,
            'comment_type': new_comment.comment_type,
            'status': new_comment.status,
            'ai_source': new_comment.ai_source,
            'created_at': new_comment.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'formatted_date': new_comment.created_at.strftime('%B %d, %Y at %I:%M %p')
        }

        return jsonify(comment_data), 201

    except Exception as e:
        logger.error(f"Error adding AI comment for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/ai-comments/<int:comment_id>/review', methods=['POST'])
def review_ai_comment(comment_id):
    """Review and approve/reject an AI comment"""
    try:
        data = request.get_json()

        if not data or 'action' not in data:
            return jsonify({'error': 'Action is required'}), 400

        action = data['action']
        if action not in ['approve', 'reject']:
            return jsonify({'error': 'Action must be approve or reject'}), 400

        # Get the comment
        comment = session.query(Comment).filter(Comment.id == comment_id).first()
        if not comment:
            return jsonify({'error': 'Comment not found'}), 404

        if comment.comment_type != 'ai' or comment.status != 'pending':
            return jsonify({'error': 'Comment is not pending for review'}), 400

        # Update comment status
        if action == 'approve':
            comment.status = 'approved'
        else:
            comment.status = 'rejected'

        comment.reviewed_by = data.get('reviewed_by')
        comment.reviewed_at = get_eastern_datetime()

        session.commit()
        export_user_lists()  # persist changes

        return jsonify({
            'id': comment.id,
            'status': comment.status,
            'reviewed_at': comment.reviewed_at.strftime('%Y-%m-%d %H:%M:%S')
        })

    except Exception as e:
        logger.error(f"Error reviewing AI comment {comment_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/latest_date')
def get_latest_date():
    """Get the latest available price date and last update timestamp"""
    try:
        latest_date = get_latest_price_date()
        if not latest_date:
            return jsonify({'error': 'No price data available'}), 404
        
        # Get the most recent updated_at timestamp (when daily_price_update.py last ran)
        latest_update = session.query(Price).order_by(desc(Price.updated_at)).first()
        
        response = {
            'latest_date': latest_date.strftime('%Y-%m-%d'),
            'formatted_date': latest_date.strftime('%B %d, %Y')
        }
        
        # Add last update timestamp if available
        if latest_update and latest_update.updated_at:
            response['last_update'] = latest_update.updated_at.strftime('%Y-%m-%d %H:%M:%S')
            response['last_update_formatted'] = latest_update.updated_at.strftime('%b %d, %Y %I:%M %p')
        
        return jsonify(response)

    except Exception as e:
        logger.error(f"Error getting latest date: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Return data API endpoints
@app.route('/api/Return1D-Above200M')
def get_return_1d_above_200m():
    return jsonify(get_momentum_stocks(1, above_200m=True))

@app.route('/api/Return1D-Below200M')
def get_return_1d_below_200m():
    return jsonify(get_momentum_stocks(1, above_200m=False))

@app.route('/api/Return5D-Above200M')
def get_return_5d_above_200m():
    return jsonify(get_momentum_stocks(5, above_200m=True))

@app.route('/api/Return5D-Below200M')
def get_return_5d_below_200m():
    return jsonify(get_momentum_stocks(5, above_200m=False))

@app.route('/api/Return60D-Above200M')
def get_return_60d_above_200m():
    return jsonify(get_momentum_stocks(60, above_200m=True))

@app.route('/api/Return60D-Below200M')
def get_return_60d_below_200m():
    return jsonify(get_momentum_stocks(60, above_200m=False))

@app.route('/api/Return120D-Above200M')
def get_return_120d_above_200m():
    return jsonify(get_momentum_stocks(120, above_200m=True))

@app.route('/api/Return120D-Below200M')
def get_return_120d_below_200m():
    return jsonify(get_momentum_stocks(120, above_200m=False))

@app.route('/api/Return20D-Above200M')
def get_return_20d_above_200m():
    return jsonify(get_momentum_stocks(20, above_200m=True))

@app.route('/api/Return20D-Below200M')
def get_return_20d_below_200m():
    return jsonify(get_momentum_stocks(20, above_200m=False))

# New market cap category endpoints for returns
@app.route('/api/Return1D-MicroCap')
def get_return_1d_micro():
    return jsonify(get_momentum_stocks(1, market_cap_category='micro'))

@app.route('/api/Return1D-SmallCap')
def get_return_1d_small():
    return jsonify(get_momentum_stocks(1, market_cap_category='small'))

@app.route('/api/Return1D-MidCap')
def get_return_1d_mid():
    return jsonify(get_momentum_stocks(1, market_cap_category='mid'))

@app.route('/api/Return1D-LargeCap')
def get_return_1d_large():
    return jsonify(get_momentum_stocks(1, market_cap_category='large'))

@app.route('/api/Return1D-MegaCap')
def get_return_1d_mega():
    return jsonify(get_momentum_stocks(1, market_cap_category='mega'))

@app.route('/api/Return5D-MicroCap')
def get_return_5d_micro():
    return jsonify(get_momentum_stocks(5, market_cap_category='micro'))

@app.route('/api/Return5D-SmallCap')
def get_return_5d_small():
    return jsonify(get_momentum_stocks(5, market_cap_category='small'))

@app.route('/api/Return5D-MidCap')
def get_return_5d_mid():
    return jsonify(get_momentum_stocks(5, market_cap_category='mid'))

@app.route('/api/Return5D-LargeCap')
def get_return_5d_large():
    return jsonify(get_momentum_stocks(5, market_cap_category='large'))

@app.route('/api/Return5D-MegaCap')
def get_return_5d_mega():
    return jsonify(get_momentum_stocks(5, market_cap_category='mega'))

@app.route('/api/Return20D-MicroCap')
def get_return_20d_micro():
    return jsonify(get_momentum_stocks(20, market_cap_category='micro'))

@app.route('/api/Return20D-SmallCap')
def get_return_20d_small():
    return jsonify(get_momentum_stocks(20, market_cap_category='small'))

@app.route('/api/Return20D-MidCap')
def get_return_20d_mid():
    return jsonify(get_momentum_stocks(20, market_cap_category='mid'))

@app.route('/api/Return20D-LargeCap')
def get_return_20d_large():
    return jsonify(get_momentum_stocks(20, market_cap_category='large'))

@app.route('/api/Return20D-MegaCap')
def get_return_20d_mega():
    return jsonify(get_momentum_stocks(20, market_cap_category='mega'))

@app.route('/api/Return60D-MicroCap')
def get_return_60d_micro():
    return jsonify(get_momentum_stocks(60, market_cap_category='micro'))

@app.route('/api/Return60D-SmallCap')
def get_return_60d_small():
    return jsonify(get_momentum_stocks(60, market_cap_category='small'))

@app.route('/api/Return60D-MidCap')
def get_return_60d_mid():
    return jsonify(get_momentum_stocks(60, market_cap_category='mid'))

@app.route('/api/Return60D-LargeCap')
def get_return_60d_large():
    return jsonify(get_momentum_stocks(60, market_cap_category='large'))

@app.route('/api/Return60D-MegaCap')
def get_return_60d_mega():
    return jsonify(get_momentum_stocks(60, market_cap_category='mega'))

@app.route('/api/Return120D-MicroCap')
def get_return_120d_micro():
    return jsonify(get_momentum_stocks(120, market_cap_category='micro'))

@app.route('/api/Return120D-SmallCap')
def get_return_120d_small():
    return jsonify(get_momentum_stocks(120, market_cap_category='small'))

@app.route('/api/Return120D-MidCap')
def get_return_120d_mid():
    return jsonify(get_momentum_stocks(120, market_cap_category='mid'))

@app.route('/api/Return120D-LargeCap')
def get_return_120d_large():
    return jsonify(get_momentum_stocks(120, market_cap_category='large'))

@app.route('/api/Return120D-MegaCap')
def get_return_120d_mega():
    return jsonify(get_momentum_stocks(120, market_cap_category='mega'))

# Gapper API endpoints
@app.route('/api/Gapper-MicroCap')
def get_gapper_micro():
    return jsonify(get_gapper_stocks(market_cap_category='micro'))

@app.route('/api/Gapper-SmallCap')
def get_gapper_small():
    return jsonify(get_gapper_stocks(market_cap_category='small'))

@app.route('/api/Gapper-MidCap')
def get_gapper_mid():
    return jsonify(get_gapper_stocks(market_cap_category='mid'))

@app.route('/api/Gapper-LargeCap')
def get_gapper_large():
    return jsonify(get_gapper_stocks(market_cap_category='large'))

@app.route('/api/Gapper-MegaCap')
def get_gapper_mega():
    return jsonify(get_gapper_stocks(market_cap_category='mega'))

# Volume API endpoints
@app.route('/api/Volume-MicroCap')
def get_volume_micro():
    return jsonify(get_volume_spike_stocks(market_cap_category='micro'))

@app.route('/api/Volume-SmallCap')
def get_volume_small():
    return jsonify(get_volume_spike_stocks(market_cap_category='small'))

@app.route('/api/Volume-MidCap')
def get_volume_mid():
    return jsonify(get_volume_spike_stocks(market_cap_category='mid'))

@app.route('/api/Volume-LargeCap')
def get_volume_large():
    return jsonify(get_volume_spike_stocks(market_cap_category='large'))

@app.route('/api/Volume-MegaCap')
def get_volume_mega():
    return jsonify(get_volume_spike_stocks(market_cap_category='mega'))

# Sea Change API endpoints
@app.route('/api/SeaChange-Above200M')
def get_sea_change_above_200m():
    return jsonify(get_sea_change_stocks(above_200m=True))

@app.route('/api/SeaChange-Below200M')
def get_sea_change_below_200m():
    return jsonify(get_sea_change_stocks(above_200m=False))

# All Returns API endpoints
@app.route('/api/AllReturns-Above200M')
def get_all_returns_above_200m():
    return jsonify(get_all_returns_data(above_200m=True))

@app.route('/api/AllReturns-Below200M')
def get_all_returns_below_200m():
    return jsonify(get_all_returns_data(above_200m=False))

# Stock data API endpoints
@app.route('/api/stocks')
def get_stocks():
    """Get all stocks with basic information"""
    try:
        stocks = session.query(Stock).all()

        stocks_data = []
        for stock in stocks:
            # Skip non-common stocks (warrants, rights, units, preferred)
            if not is_common_stock(stock.ticker):
                continue
                
            stocks_data.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'price': stock.price,
                'exchange': stock.exchange
            })

        return jsonify(stocks_data)

    except Exception as e:
        logger.error(f"Error getting stocks: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/sectors')
def get_sectors():
    """Get unique sectors"""
    try:
        sectors = session.query(Stock.sector).distinct().filter(Stock.sector.isnot(None)).all()
        sectors_list = [sector[0] for sector in sectors if sector[0]]
        return jsonify(sorted(sectors_list))

    except Exception as e:
        logger.error(f"Error getting sectors: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/industries')
def get_industries():
    """Get unique industries"""
    try:
        industries = session.query(Stock.industry).distinct().filter(Stock.industry.isnot(None)).all()
        industries_list = [industry[0] for industry in industries if industry[0]]
        return jsonify(sorted(industries_list))

    except Exception as e:
        logger.error(f"Error getting industries: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/exchanges')
def get_exchanges():
    """Get unique exchanges"""
    try:
        exchanges = session.query(Stock.exchange).distinct().filter(Stock.exchange.isnot(None)).all()
        exchanges_list = [exchange[0] for exchange in exchanges if exchange[0]]
        return jsonify(sorted(exchanges_list))

    except Exception as e:
        logger.error(f"Error getting exchanges: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<ticker>')
def get_stock_details(ticker):
    """Get detailed information for a specific stock"""
    try:
        ticker = ticker.upper()

        stock = session.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return jsonify({'error': 'Stock not found'}), 404

        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return jsonify({'error': 'No price data available'}), 500

        # Get latest price data
        latest_price = session.query(Price).filter(
            Price.ticker == ticker,
            Price.date == latest_date
        ).first()

        # Get previous trading day price for price change calculation
        previous_date = get_trading_date_n_days_ago(latest_date, 1)
        previous_price = None
        if previous_date:
            previous_price = session.query(Price).filter(
                Price.ticker == ticker,
                Price.date == previous_date
            ).first()

        # Calculate 52-week high/low (252 trading days)
        week_52_date = get_trading_date_n_days_ago(latest_date, 252)
        week_52_data = None
        if week_52_date:
            week_52_data = session.query(
                func.max(Price.close_price).label('high_52w'),
                func.min(Price.close_price).label('low_52w')
            ).filter(
                Price.ticker == ticker,
                Price.date >= week_52_date,
                Price.date <= latest_date,
                Price.close_price.isnot(None),
                Price.close_price > 0
            ).first()

        # Calculate average volume (last 20 trading days)
        avg_volume_date = get_trading_date_n_days_ago(latest_date, 20)
        avg_volume = None
        if avg_volume_date:
            avg_volume = session.query(
                func.avg(Price.volume).label('avg_volume')
            ).filter(
                Price.ticker == ticker,
                Price.date >= avg_volume_date,
                Price.date <= latest_date,
                Price.volume.isnot(None)
            ).scalar()

        # Calculate returns for different trading day periods
        returns = {}
        for period, days in [('5d', 5), ('20d', 20), ('60d', 60)]:
            # Get the exact trading day N days ago
            period_date = get_trading_date_n_days_ago(latest_date, days)
            
            if period_date:
                # Get price on or very near the target trading day (1 day window)
                window_start = period_date - timedelta(days=1)
                window_end = period_date + timedelta(days=1)
                
                period_price = session.query(
                    func.avg(Price.close_price).label('avg_price')
                ).filter(
                    Price.ticker == ticker,
                    Price.date >= window_start,
                    Price.date <= window_end,
                    Price.close_price.isnot(None),
                    Price.close_price > 0
                ).scalar()
                
                if period_price and latest_price and latest_price.close_price:
                    returns[f'return_{period}'] = ((latest_price.close_price - period_price) / period_price) * 100
                else:
                    returns[f'return_{period}'] = None
            else:
                returns[f'return_{period}'] = None

        # Calculate price changes
        close_price = latest_price.close_price if latest_price else stock.price
        price_change = None
        price_change_pct = None
        
        if previous_price and previous_price.close_price and latest_price and latest_price.close_price:
            price_change = latest_price.close_price - previous_price.close_price
            price_change_pct = (price_change / previous_price.close_price) * 100

        # Calculate volume change multiplier
        volume_change_multiplier = None
        if latest_price and latest_price.volume and avg_volume and avg_volume > 0:
            volume_change_multiplier = latest_price.volume / avg_volume

        stock_data = {
            'ticker': stock.ticker,
            'company_name': stock.company_name,
            'sector': stock.sector,
            'industry': stock.industry,
            'country': stock.country,
            'market_cap': stock.market_cap,
            'price': stock.price,
            'close_price': close_price,
            'price_change': price_change,
            'price_change_pct': price_change_pct,
            'volume': latest_price.volume if latest_price else None,
            'avg_volume': avg_volume,
            'volume_change_multiplier': volume_change_multiplier,
            'week_52_high': week_52_data.high_52w if week_52_data else None,
            'week_52_low': week_52_data.low_52w if week_52_data else None,
            'return_5d': returns.get('return_5d'),
            'return_20d': returns.get('return_20d'),
            'return_60d': returns.get('return_60d'),
            'shares_outstanding': getattr(stock, 'shares_outstanding', None),
            'shares_float': getattr(stock, 'shares_float', None),
            'short_float': getattr(stock, 'short_float', None),
            'short_ratio': getattr(stock, 'short_ratio', None),
            'short_interest': getattr(stock, 'short_interest', None),
            'beta': stock.beta,
            'vol_avg': stock.vol_avg,
            'last_div': stock.last_div,
            'range': stock.range,
            'changes': stock.changes,
            'currency': stock.currency,
            'cik': stock.cik,
            'isin': stock.isin,
            'cusip': stock.cusip,
            'exchange': stock.exchange,
            'exchange_short_name': stock.exchange_short_name,
            'website': stock.website,
            'description': stock.description,
            'ceo': stock.ceo,
            'full_time_employees': stock.full_time_employees,
            'phone': stock.phone,
            'address': stock.address,
            'city': stock.city,
            'state': stock.state,
            'zip': stock.zip,
            'dcf_diff': stock.dcf_diff,
            'dcf': stock.dcf,
            'ipo_date': stock.ipo_date.strftime('%Y-%m-%d') if stock.ipo_date else None,
            'default_image': stock.default_image,
            'is_etf': stock.is_etf,
            'is_actively_trading': stock.is_actively_trading,
            'is_adr': stock.is_adr,
            'is_fund': stock.is_fund
        }

        return jsonify(stock_data)

    except Exception as e:
        logger.error(f"Error getting stock details for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<ticker>/prices')
def get_stock_prices(ticker):
    """Get price history for a specific stock"""
    try:
        ticker = ticker.upper()

        # Verify stock exists
        stock = session.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return jsonify({'error': 'Stock not found'}), 404

        # Get price data
        prices = session.query(Price).filter(Price.ticker == ticker)\
                    .order_by(Price.date).all()

        prices_data = []
        for price in prices:
            prices_data.append({
                'date': price.date.strftime('%Y-%m-%d'),
                'open_price': price.open_price,
                'high_price': price.high_price,
                'low_price': price.low_price,
                'close_price': price.close_price,
                'volume': price.volume
            })

        return jsonify(prices_data)

    except Exception as e:
        logger.error(f"Error getting prices for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<ticker>/earnings')
def get_stock_earnings(ticker):
    """Get earnings data for a specific stock"""
    try:
        ticker = ticker.upper()

        earnings = session.query(Earnings).filter(Earnings.ticker == ticker)\
                   .order_by(desc(Earnings.earnings_date)).all()

        earnings_data = []
        for earning in earnings:
            earnings_data.append({
                'earnings_date': earning.earnings_date.strftime('%Y-%m-%d') if earning.earnings_date else None,
                'announcement_type': earning.announcement_type,
                'is_confirmed': earning.is_confirmed,
                'quarter': earning.quarter,
                'fiscal_year': earning.fiscal_year,
                'announcement_time': earning.announcement_time,
                'estimated_eps': earning.estimated_eps,
                'actual_eps': earning.actual_eps,
                'revenue_estimate': earning.revenue_estimate,
                'actual_revenue': earning.actual_revenue,
                'source': earning.source,
                'notes': earning.notes
            })

        return jsonify(earnings_data)

    except Exception as e:
        logger.error(f"Error getting earnings for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/market_cap_distribution')
def get_market_cap_distribution():
    """Get market cap distribution statistics with refined buckets"""
    try:
        # Define refined market cap ranges
        # Micro Cap: ≤ $200M
        # Small Cap: $200M – $2B
        # Mid Cap: $2B – $20B
        # Large Cap: $20B – $100B
        # Mega Cap: > $100B
        ranges = [
            (0, 200000000, "Micro Cap", "Up to $200M"),           # ≤ $200M
            (200000000, 2000000000, "Small Cap", "$200M - $2B"),  # $200M - $2B
            (2000000000, 20000000000, "Mid Cap", "$2B - $20B"),  # $2B - $20B
            (20000000000, 100000000000, "Large Cap", "$20B - $100B"),  # $20B - $100B
            (100000000000, float('inf'), "Mega Cap", "Over $100B")   # > $100B
        ]

        distribution = []
        for min_cap, max_cap, label, description in ranges:
            if max_cap == float('inf'):
                count = session.query(Stock).filter(Stock.market_cap >= min_cap).count()
            else:
                count = session.query(Stock).filter(Stock.market_cap >= min_cap, Stock.market_cap < max_cap).count()

            distribution.append({
                'category': label,
                'range': label,  # Kept for backward compatibility
                'description': description,  # Human-readable range description
                'count': count,
                'min_cap': min_cap,
                'max_cap': max_cap if max_cap != float('inf') else None
            })

        return jsonify(distribution)

    except Exception as e:
        logger.error(f"Error getting market cap distribution: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/perplexity', methods=['POST'])
def get_perplexity_analysis():
    """Get AI analysis from Perplexity API"""
    try:
        data = request.get_json()

        if not data or 'ticker' not in data:
            return jsonify({'error': 'Ticker is required'}), 400

        ticker = data['ticker'].upper()

        # Verify ticker exists
        stock = session.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return jsonify({'error': 'Ticker not found'}), 404

        # Get API key
        api_key = os.getenv('PERPLEXITY_API_KEY')
        if not api_key:
            return jsonify({'error': 'Perplexity API key not configured'}), 500

        # Prepare the prompt
        prompt = f"Provide a comprehensive analysis of {ticker} ({stock.company_name}). Include recent news, financial performance, market position, and future outlook. Be concise but informative."

        # Make API call to Perplexity
        response = requests.post(
            'https://api.perplexity.ai/chat/completions',
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'llama-3.1-sonar-large-128k-online',
                'messages': [
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                'temperature': 0.2,
                'top_p': 0.9,
                'return_citations': True,
                'search_domain_filter': ['perplexity.ai'],
                'return_images': False,
                'return_related_questions': False,
                'search_recency_filter': 'month'
            }
        )

        if response.status_code != 200:
            return jsonify({'error': f'Perplexity API error: {response.status_code}'}), 500

        result = response.json()
        content = result['choices'][0]['message']['content']
        citations = result['citations'] if 'citations' in result else []

        # Process citations in content
        processed_content = process_citations_in_content(content, citations)

        return jsonify({
            'ticker': ticker,
            'company_name': stock.company_name,
            'analysis': processed_content,
            'citations': citations,
            'timestamp': get_eastern_datetime().strftime('%Y-%m-%d %H:%M:%S')
        })

    except Exception as e:
        logger.error(f"Error getting Perplexity analysis for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/economic-calendar', methods=['GET'])
def get_economic_calendar():
    """Get upcoming US economic calendar events using Perplexity API (cached)"""
    try:
        # Check if we should use cache
        force_refresh = request.args.get('refresh', '').lower() == 'true'
        
        # Try to get from cache if not forcing refresh
        if not force_refresh:
            cached_data = get_cached_economic_calendar()
            if cached_data:
                return jsonify(cached_data)
        
        # If no cache or force refresh, fetch from Perplexity
        logger.info("Fetching economic calendar from Perplexity API...")
        
        # Get API key
        api_key = os.getenv('PERPLEXITY_API_KEY')
        if not api_key:
            return jsonify({'error': 'Perplexity API key not configured'}), 500

        # Prepare the prompt for economic calendar events
        prompt = (
            "Provide a comprehensive economic calendar of market-moving events for the next 2 weeks "
            "that could impact US equities. Include ALL of the following categories:\n\n"
            "**US Economic Data (HIGHEST PRIORITY):**\n"
            "- CPI, PPI, PCE inflation reports\n"
            "- Non-farm payrolls, unemployment rate, jobless claims\n"
            "- Retail sales, consumer sentiment (Michigan, Conference Board)\n"
            "- PMI reports (ISM Manufacturing/Services, S&P Global)\n"
            "- Housing data (starts, permits, existing home sales)\n"
            "- GDP reports, industrial production, durable goods\n"
            "- Fed Beige Book, Fed speakers (especially Chair Powell)\n\n"
            "**Central Banks:**\n"
            "- FOMC meetings, minutes, and rate decisions\n"
            "- ECB, BOJ, BOE meetings and rate decisions\n"
            "- Central bank speeches and press conferences\n\n"
            "**Major Earnings:**\n"
            "- Mega-cap tech: AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA\n"
            "- Major financials: JPM, BAC, GS, MS, WFC, C\n"
            "- Other index movers and sector leaders\n\n"
            "**Market Events:**\n"
            "- Monthly/quarterly options expiration (OPEX)\n"
            "- Triple witching, index rebalancing\n"
            "- Treasury auctions (10Y, 30Y)\n\n"
            "**Government/Policy:**\n"
            "- Congressional votes, debt ceiling\n"
            "- Trade policy announcements\n"
            "- Regulatory decisions (SEC, FDA approvals)\n\n"
            "For each event provide:\n"
            "1. Specific date (e.g., 'Oct 10', 'Oct 15')\n"
            "2. Time in ET (e.g., '8:30 AM ET', '2:00 PM ET') - if known\n"
            "3. Event name (be specific)\n"
            "4. One sentence on why it matters to markets\n"
            '5. Importance: "high" (Fed, CPI, jobs, major tech earnings), "medium" (regional data, sector earnings), "low" (minor data)\n\n'
            "Format as JSON array:\n"
            "[\n"
            '  {"date": "Oct 10", "time": "8:30 AM ET", "event": "US CPI (September)", "description": "Core inflation critical for Fed policy path", "importance": "high"},\n'
            '  {"date": "Oct 15", "time": "Before Market", "event": "JPMorgan Q3 Earnings", "description": "Kicks off bank earnings, sets tone for financial sector", "importance": "high"}\n'
            "]\n\n"
            "Return ONLY the JSON array with 25-35 events, no markdown formatting or additional text. "
            "Sort by date chronologically."
        )

        # Make API call to Perplexity
        response = requests.post(
            'https://api.perplexity.ai/chat/completions',
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'sonar-pro',
                'messages': [
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                'temperature': 0.2,
                'stream': False
            },
            timeout=60
        )

        if response.status_code != 200:
            logger.error(f"Perplexity API error: {response.status_code} - {response.text}")
            return jsonify({'error': f'Perplexity API error: {response.status_code}'}), 500

        result = response.json()
        content = result.get('choices', [{}])[0].get('message', {}).get('content', '')
        
        if not content:
            return jsonify({'error': 'No content received from Perplexity'}), 500

        # Try to parse JSON from the response
        import json
        import re
        
        # Extract JSON array from response (handle markdown code blocks)
        json_match = re.search(r'\[[\s\S]*\]', content)
        if json_match:
            events_json = json_match.group(0)
            try:
                events = json.loads(events_json)
            except json.JSONDecodeError:
                # If JSON parsing fails, return raw content
                logger.warning("Failed to parse JSON from Perplexity response")
                events = []
                # Try to extract events from plain text
                lines = content.strip().split('\n')
                for line in lines:
                    if line.strip() and not line.startswith('#'):
                        events.append({
                            'date': '',
                            'time': '',
                            'event': line.strip(),
                            'description': '',
                            'importance': 'medium'
                        })
        else:
            events = []
            lines = content.strip().split('\n')
            for line in lines:
                if line.strip() and not line.startswith('#'):
                    events.append({
                        'date': '',
                        'time': '',
                        'event': line.strip(),
                        'description': '',
                        'importance': 'medium'
                    })

        result = {
            'events': events,
            'timestamp': get_eastern_datetime().strftime('%Y-%m-%d %H:%M:%S'),
            'cached': False
        }
        
        # Save to cache
        save_economic_calendar_cache(result)
        
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error getting economic calendar: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/comments/<int:comment_id>', methods=['DELETE'])
def delete_comment(comment_id):
    """Delete a comment"""
    try:
        comment = session.query(Comment).filter(Comment.id == comment_id).first()
        if not comment:
            return jsonify({'error': 'Comment not found'}), 404

        # Only allow deleting user comments or rejected AI comments
        if comment.comment_type == 'ai' and comment.status == 'approved':
            return jsonify({'error': 'Cannot delete approved AI comments'}), 400

        session.delete(comment)
        session.commit()
        export_user_lists()  # persist changes

        return jsonify({'message': 'Comment deleted successfully'})

    except Exception as e:
        logger.error(f"Error deleting comment {comment_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/comments/<int:comment_id>', methods=['PUT'])
def edit_comment(comment_id):
    """Edit a comment"""
    try:
        data = request.get_json()

        if not data or 'comment_text' not in data:
            return jsonify({'error': 'Comment text is required'}), 400

        comment_text = data['comment_text'].strip()
        if not comment_text:
            return jsonify({'error': 'Comment text cannot be empty'}), 400

        comment = session.query(Comment).filter(Comment.id == comment_id).first()
        if not comment:
            return jsonify({'error': 'Comment not found'}), 404

        # Only allow editing user comments
        if comment.comment_type != 'user':
            return jsonify({'error': 'Can only edit user comments'}), 400

        comment.comment_text = comment_text
        comment.updated_at = get_eastern_datetime()

        session.commit()
        export_user_lists()  # persist changes

        return jsonify({
            'id': comment.id,
            'comment_text': comment.comment_text,
            'updated_at': comment.updated_at.strftime('%Y-%m-%d %H:%M:%S')
        })

    except Exception as e:
        logger.error(f"Error editing comment {comment_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/finviz-news/<ticker>')
def get_finviz_news(ticker):
    """Get news from Finviz for a specific ticker"""
    try:
        ticker = ticker.upper()

        # Verify ticker exists
        stock = session.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return jsonify({'error': 'Ticker not found'}), 404

        # Scrape Finviz news
        url = f'https://finviz.com/quote.ashx?t={ticker}'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return jsonify({'error': 'Failed to fetch news from Finviz'}), 500

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find news table
        news_table = soup.find('table', {'class': 'fullview-news-outer'})
        if not news_table:
            return jsonify([])  # No news found

        news_items = []
        rows = news_table.find_all('tr')

        for row in rows:
            try:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    # Get time and headline
                    time_cell = cells[0]
                    headline_cell = cells[1]

                    # Extract time - try multiple methods
                    time_text = ""
                    # Try finding div first
                    time_div = time_cell.find('div')
                    if time_div:
                        time_text = time_div.get_text(strip=True)
                    # If no div, try getting text directly from td
                    if not time_text:
                        time_text = time_cell.get_text(strip=True)

                    # Extract headline and link
                    headline_link = headline_cell.find('a')
                    if headline_link:
                        headline = headline_link.get_text(strip=True)
                        link = headline_link.get('href')

                        # Get source if available
                        source_div = headline_cell.find('span', {'class': 'news-link-right'})
                        source = source_div.get_text(strip=True) if source_div else ""

                        news_items.append({
                            'time': time_text if time_text else 'N/A',
                            'headline': headline,
                            'link': link,
                            'source': source
                        })

            except Exception as e:
                logger.warning(f"Error parsing news row: {str(e)}")
                continue

        return jsonify(news_items[:20])  # Return top 20 news items

    except Exception as e:
        logger.error(f"Error getting Finviz news for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flags/<ticker>', methods=['GET'])
def get_flags(ticker):
    """Get flags for a specific ticker"""
    try:
        ticker = ticker.upper()

        flags = session.query(Flags).filter(Flags.ticker == ticker).first()
        if not flags:
            return jsonify({
                'ticker': ticker,
                'is_reviewed': False,
                'is_shortlisted': False,
                'is_blacklisted': False
            })

        return jsonify({
            'ticker': flags.ticker,
            'is_reviewed': flags.is_reviewed,
            'is_shortlisted': flags.is_shortlisted,
            'is_blacklisted': flags.is_blacklisted
        })

    except Exception as e:
        logger.error(f"Error getting flags for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flags', methods=['POST'])
def update_flags():
    """Update flags for a specific ticker"""
    try:
        data = request.get_json()

        if not data or 'ticker' not in data:
            return jsonify({'error': 'Ticker is required'}), 400

        ticker = data['ticker'].upper()

        # Verify ticker exists
        stock = session.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return jsonify({'error': 'Ticker not found'}), 404

        # Get or create flags entry
        flags = session.query(Flags).filter(Flags.ticker == ticker).first()
        if not flags:
            flags = Flags(ticker=ticker)
            session.add(flags)

        # Update flags
        if 'is_reviewed' in data:
            flags.is_reviewed = data['is_reviewed']
        if 'is_shortlisted' in data:
            flags.is_shortlisted = data['is_shortlisted']
        if 'is_blacklisted' in data:
            flags.is_blacklisted = data['is_blacklisted']

        session.commit()
        export_user_lists()  # persist changes

        return jsonify({
            'ticker': flags.ticker,
            'is_reviewed': flags.is_reviewed,
            'is_shortlisted': flags.is_shortlisted,
            'is_blacklisted': flags.is_blacklisted
        })

    except Exception as e:
        logger.error(f"Error updating flags for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flags/batch', methods=['POST'])
def update_flags_batch():
    """Update flags for multiple tickers"""
    try:
        data = request.get_json()

        if not data or 'tickers' not in data:
            return jsonify({'error': 'Tickers list is required'}), 400

        tickers = data['tickers']
        if not isinstance(tickers, list):
            return jsonify({'error': 'Tickers must be a list'}), 400

        updated_flags = []

        for ticker in tickers:
            ticker = ticker.upper()

            # Verify ticker exists
            stock = session.query(Stock).filter(Stock.ticker == ticker).first()
            if not stock:
                continue  # Skip invalid tickers

            # Get or create flags entry
            flags = session.query(Flags).filter(Flags.ticker == ticker).first()
            if not flags:
                flags = Flags(ticker=ticker)
                session.add(flags)

            # Update flags
            if 'is_reviewed' in data:
                flags.is_reviewed = data['is_reviewed']
            if 'is_shortlisted' in data:
                flags.is_shortlisted = data['is_shortlisted']
            if 'is_blacklisted' in data:
                flags.is_blacklisted = data['is_blacklisted']

            updated_flags.append({
                'ticker': flags.ticker,
                'is_reviewed': flags.is_reviewed,
                'is_shortlisted': flags.is_shortlisted,
                'is_blacklisted': flags.is_blacklisted
            })

        session.commit()
        export_user_lists()  # persist changes

        return jsonify(updated_flags)

    except Exception as e:
        logger.error(f"Error updating flags batch: {str(e)}")
        return jsonify({'error': str(e)}), 500

def get_reviewed_ticker_set():
    """Get set of reviewed tickers"""
    try:
        reviewed = session.query(Flags.ticker).filter(Flags.is_reviewed == True).all()
        return {row[0] for row in reviewed}
    except Exception:
        return set()

def get_blacklisted_ticker_set():
    """Get set of blacklisted tickers"""
    try:
        blacklisted = session.query(Flags.ticker).filter(Flags.is_blacklisted == True).all()
        return {row[0] for row in blacklisted}
    except Exception:
        return set()

def get_shortlisted_ticker_set():
    """Get set of shortlisted tickers"""
    try:
        shortlisted = session.query(Flags.ticker).filter(Flags.is_shortlisted == True).all()
        return {row[0] for row in shortlisted}
    except Exception:
        return set()

def build_flag_payload(flag_rows):
    """Build payload for flag-related endpoints"""
    payload = []
    for row in flag_rows:
        payload.append({
            'ticker': row.ticker,
            'company_name': row.company_name,
            'sector': row.sector,
            'industry': row.industry,
            'market_cap': row.market_cap,
            'price': row.price,
            'current_price': row.price,  # Add current_price for enrichment
            'is_reviewed': row.is_reviewed if hasattr(row, 'is_reviewed') else False,
            'is_shortlisted': row.is_shortlisted if hasattr(row, 'is_shortlisted') else False,
            'is_blacklisted': row.is_blacklisted if hasattr(row, 'is_blacklisted') else False
        })
    return payload

@app.route('/api/shortlist')
def api_shortlist():
    """Get shortlisted stocks"""
    try:
        # Get shortlisted tickers
        shortlisted_tickers = get_shortlisted_ticker_set()

        if not shortlisted_tickers:
            return jsonify([])

        # Get stock data for shortlisted tickers
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            Flags.is_reviewed,
            Flags.is_shortlisted,
            Flags.is_blacklisted
        ).join(Flags, Stock.ticker == Flags.ticker)\
         .filter(Stock.ticker.in_(shortlisted_tickers))

        # Apply global exclusions
        query = apply_global_exclude_filters(query)

        flag_rows = query.all()
        payload = build_flag_payload(flag_rows)
        
        # Enrich with additional data including 1-day price change
        payload = enrich_stock_data(payload)

        return jsonify(payload)

    except Exception as e:
        logger.error(f"Error getting shortlist: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/blacklist')
def api_blacklist():
    """Get blacklisted stocks"""
    try:
        # Get blacklisted tickers
        blacklisted_tickers = get_blacklisted_ticker_set()

        if not blacklisted_tickers:
            return jsonify([])

        # Get stock data for blacklisted tickers
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            Flags.is_reviewed,
            Flags.is_shortlisted,
            Flags.is_blacklisted
        ).join(Flags, Stock.ticker == Flags.ticker)\
         .filter(Stock.ticker.in_(blacklisted_tickers))

        # Apply global exclusions
        query = apply_global_exclude_filters(query)

        flag_rows = query.all()
        payload = build_flag_payload(flag_rows)
        
        # Enrich with additional data including 1-day price change
        payload = enrich_stock_data(payload)

        return jsonify(payload)

    except Exception as e:
        logger.error(f"Error getting blacklist: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/concise-notes/<ticker>')
def get_concise_note(ticker):
    """Get concise note for a specific ticker"""
    try:
        ticker = ticker.upper()

        note = session.query(ConciseNote).filter(ConciseNote.ticker == ticker).first()
        if not note:
            return jsonify({'ticker': ticker, 'note': ''})

        return jsonify({
            'ticker': note.ticker,
            'note': note.note
        })

    except Exception as e:
        logger.error(f"Error getting concise note for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/concise-notes/<ticker>', methods=['PUT'])
def update_concise_note(ticker):
    """Update concise note for a specific ticker"""
    try:
        ticker = ticker.upper()
        data = request.get_json()

        if not data or 'note' not in data:
            return jsonify({'error': 'Note text is required'}), 400

        note_text = data['note'].strip()

        # Get or create note entry
        note = session.query(ConciseNote).filter(ConciseNote.ticker == ticker).first()
        if not note:
            note = ConciseNote(ticker=ticker)
            session.add(note)

        note.note = note_text

        session.commit()
        export_user_lists()  # persist changes

        return jsonify({
            'ticker': note.ticker,
            'note': note.note
        })

    except Exception as e:
        logger.error(f"Error updating concise note for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/rsi')
def api_rsi():
    """API endpoint to fetch RSI data with stock information"""
    try:
        # Join StockRSI with Stock to get company info
        query = session.query(
            StockRSI.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            StockRSI.rsi_spx_1week,
            StockRSI.rsi_spx_1month,
            StockRSI.rsi_spx_3month,
            StockRSI.rsi_qqq_1week,
            StockRSI.rsi_qqq_1month,
            StockRSI.rsi_qqq_3month,
            StockRSI.rsi_spx_1week_percentile,
            StockRSI.rsi_spx_1month_percentile,
            StockRSI.rsi_spx_3month_percentile,
            StockRSI.rsi_qqq_1week_percentile,
            StockRSI.rsi_qqq_1month_percentile,
            StockRSI.rsi_qqq_3month_percentile,
            StockRSI.rsi_spx_correction,
            StockRSI.rsi_spx_correction_percentile,
            StockRSI.last_updated
        ).join(Stock, StockRSI.ticker == Stock.ticker)

        # Apply global exclusions
        query = apply_global_exclude_filters(query)

        results = query.all()

        # Helper function to safely handle numeric values including NaN
        def safe_numeric_value(value, decimal_places=2):
            if value is None:
                return None
            # Check for NaN values
            if str(value).lower() == 'nan' or (hasattr(value, '__ne__') and value != value):
                return None
            try:
                return round(float(value), decimal_places)
            except (ValueError, TypeError):
                return None

        rsi_data = []
        for row in results:
            rsi_data.append({
                'ticker': row.ticker,
                'company_name': row.company_name,
                'sector': row.sector,
                'industry': row.industry,
                'market_cap': row.market_cap,
                'rsi_spx_1week': safe_numeric_value(row.rsi_spx_1week),
                'rsi_spx_1month': safe_numeric_value(row.rsi_spx_1month),
                'rsi_spx_3month': safe_numeric_value(row.rsi_spx_3month),
                'rsi_qqq_1week': safe_numeric_value(row.rsi_qqq_1week),
                'rsi_qqq_1month': safe_numeric_value(row.rsi_qqq_1month),
                'rsi_qqq_3month': safe_numeric_value(row.rsi_qqq_3month),
                'rsi_spx_1week_percentile': safe_numeric_value(row.rsi_spx_1week_percentile),
                'rsi_spx_1month_percentile': safe_numeric_value(row.rsi_spx_1month_percentile),
                'rsi_spx_3month_percentile': safe_numeric_value(row.rsi_spx_3month_percentile),
                'rsi_qqq_1week_percentile': safe_numeric_value(row.rsi_qqq_1week_percentile),
                'rsi_qqq_1month_percentile': safe_numeric_value(row.rsi_qqq_1month_percentile),
                'rsi_qqq_3month_percentile': safe_numeric_value(row.rsi_qqq_3month_percentile),
                'rsi_spx_correction': safe_numeric_value(row.rsi_spx_correction),
                'rsi_spx_correction_percentile': safe_numeric_value(row.rsi_spx_correction_percentile),
                'last_updated': row.last_updated.strftime('%Y-%m-%d %H:%M:%S') if row.last_updated else None
            })

        return jsonify(rsi_data)

    except Exception as e:
        logger.error(f"Error getting RSI data: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# INDEX/ETF ENDPOINTS
# ============================================================================

@app.route('/api/indices', methods=['GET'])
def get_indices():
    """Get all indices with current prices and moving averages"""
    try:
        # Get all indices
        indices = session.query(Index).filter(Index.is_active == True).all()
        
        result = []
        for index in indices:
            # Get latest price
            latest_price = session.query(IndexPrice).filter(
                IndexPrice.symbol == index.symbol
            ).order_by(desc(IndexPrice.date)).first()
            
            # Get price history for moving averages (last 250 trading days ~ 1 year)
            prices = session.query(IndexPrice).filter(
                IndexPrice.symbol == index.symbol
            ).order_by(desc(IndexPrice.date)).limit(250).all()
            
            if not prices:
                continue
                
            # Calculate moving averages
            close_prices = [p.close_price for p in reversed(prices)]
            
            ma_10 = np.mean(close_prices[-10:]) if len(close_prices) >= 10 else None
            ma_20 = np.mean(close_prices[-20:]) if len(close_prices) >= 20 else None
            ma_50 = np.mean(close_prices[-50:]) if len(close_prices) >= 50 else None
            ma_200 = np.mean(close_prices[-200:]) if len(close_prices) >= 200 else None
            
            # Calculate returns
            ret_1d = ((close_prices[-1] - close_prices[-2]) / close_prices[-2] * 100) if len(close_prices) >= 2 else None
            ret_5d = ((close_prices[-1] - close_prices[-6]) / close_prices[-6] * 100) if len(close_prices) >= 6 else None
            ret_20d = ((close_prices[-1] - close_prices[-21]) / close_prices[-21] * 100) if len(close_prices) >= 21 else None
            ret_60d = ((close_prices[-1] - close_prices[-61]) / close_prices[-61] * 100) if len(close_prices) >= 61 else None
            
            # Calculate all-time high and distance
            all_time_high = max(close_prices) if close_prices else None
            ath_distance = ((close_prices[-1] - all_time_high) / all_time_high * 100) if all_time_high else None
            
            # Find previous high (last significant peak before current price)
            # Look for the highest price in the last 60 days (excluding most recent 5 days)
            prev_high = None
            prev_high_distance = None
            if len(close_prices) >= 60:
                # Get prices from 5 to 60 days ago
                lookback_prices = close_prices[-60:-5]
                prev_high = max(lookback_prices) if lookback_prices else None
                if prev_high and close_prices[-1]:
                    prev_high_distance = ((close_prices[-1] - prev_high) / prev_high * 100)
            
            result.append({
                'symbol': index.symbol,
                'name': index.name,
                'description': index.description,
                'index_type': index.index_type,
                'current_price': latest_price.close_price if latest_price else None,
                'date': latest_price.date.isoformat() if latest_price else None,
                'ma_10': round(ma_10, 2) if ma_10 else None,
                'ma_20': round(ma_20, 2) if ma_20 else None,
                'ma_50': round(ma_50, 2) if ma_50 else None,
                'ma_200': round(ma_200, 2) if ma_200 else None,
                'return_1d': round(ret_1d, 2) if ret_1d else None,
                'return_5d': round(ret_5d, 2) if ret_5d else None,
                'return_20d': round(ret_20d, 2) if ret_20d else None,
                'return_60d': round(ret_60d, 2) if ret_60d else None,
                'all_time_high': round(all_time_high, 2) if all_time_high else None,
                'ath_distance': round(ath_distance, 2) if ath_distance else None,
                'prev_high': round(prev_high, 2) if prev_high else None,
                'prev_high_distance': round(prev_high_distance, 2) if prev_high_distance else None,
            })
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting indices: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/indices/<symbol>/prices', methods=['GET'])
def get_index_prices(symbol):
    """Get price history for a specific index"""
    try:
        # Get index info
        index = session.query(Index).filter(Index.symbol == symbol).first()
        if not index:
            return jsonify({'error': 'Index not found'}), 404
        
        # Get days parameter (default 365)
        days = request.args.get('days', 365, type=int)
        
        # Get prices
        prices = session.query(IndexPrice).filter(
            IndexPrice.symbol == symbol
        ).order_by(desc(IndexPrice.date)).limit(days).all()
        
        if not prices:
            return jsonify({'error': 'No price data found'}), 404
        
        # Reverse to get chronological order
        prices = list(reversed(prices))
        
        result = {
            'symbol': index.symbol,
            'name': index.name,
            'prices': [{
                'date': p.date.isoformat(),
                'open_price': p.open_price,
                'high_price': p.high_price,
                'low_price': p.low_price,
                'close_price': p.close_price,
                'volume': p.volume
            } for p in prices]
        }
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting index prices for {symbol}: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# GAINER REPORT ENDPOINT
# ============================================================================

@app.route('/api/test-gainer-report', methods=['POST'])
def test_gainer_report():
    """Test endpoint - returns JSON report without PDF/email"""
    try:
        data = request.json
        date_str = data.get('date')
        market_cap = data.get('market_cap', 'mega')
        limit = data.get('limit', 10)
        
        if not date_str:
            return jsonify({'error': 'Date is required'}), 400
        
        # Parse date
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        # Get gainers
        rewind_date = date_obj.strftime('%Y-%m-%d')
        original_args = request.args
        
        from werkzeug.datastructures import ImmutableMultiDict
        new_args = dict(original_args)
        new_args['rewind_date'] = rewind_date
        request.args = ImmutableMultiDict(new_args)
        
        try:
            gainers = get_momentum_stocks(1, market_cap_category=market_cap)
        finally:
            request.args = original_args
        
        if not gainers:
            return jsonify({'error': 'No gainers found for the specified date'}), 404
        
        # Limit and return
        gainers = gainers[:limit]
        
        return jsonify({
            'success': True,
            'date': date_str,
            'market_cap': market_cap,
            'gainers_count': len(gainers),
            'gainers': gainers
        })
        
    except Exception as e:
        logger.error(f"Error in test gainer report: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/generate-gainer-report', methods=['POST'])
def generate_gainer_report():
    """Generate daily gainer report and optionally email as PDF"""
    # Try importing dependencies first
    try:
        import markdown
        from weasyprint import HTML
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.application import MIMEApplication
        from io import BytesIO
    except ImportError as ie:
        return jsonify({
            'error': f'Missing dependency: {str(ie)}. Please rebuild Docker containers.',
            'instructions': 'Run: docker compose build && docker compose up -d'
        }), 500
    
    try:
        data = request.json
        date_str = data.get('date')
        market_cap = data.get('market_cap', 'mega')
        limit = data.get('limit', 10)
        email_to = data.get('email')
        
        if not date_str:
            return jsonify({'error': 'Date is required'}), 400
        
        # Parse date
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        # Get gainers from API
        rewind_date = date_obj.strftime('%Y-%m-%d')
        endpoint_map = {
            'micro': '/api/Return1D-MicroCap',
            'small': '/api/Return1D-SmallCap',
            'mid': '/api/Return1D-MidCap',
            'large': '/api/Return1D-LargeCap',
            'mega': '/api/Return1D-MegaCap'
        }
        
        endpoint = endpoint_map.get(market_cap)
        if not endpoint:
            return jsonify({'error': 'Invalid market cap category'}), 400
        
        # Temporarily set rewind date in request args for internal call
        # Save original args
        original_args = request.args
        
        # Create new args dict with rewind_date
        from werkzeug.datastructures import ImmutableMultiDict
        new_args = dict(original_args)
        new_args['rewind_date'] = rewind_date
        request.args = ImmutableMultiDict(new_args)
        
        try:
            # Get gainers using the momentum function
            gainers = get_momentum_stocks(1, market_cap_category=market_cap)
        finally:
            # Restore original args
            request.args = original_args
        
        if not gainers:
            return jsonify({'error': 'No gainers found for the specified date'}), 404
        
        # Limit gainers
        gainers = gainers[:limit]
        
        # Generate compact markdown report
        date_formatted = date_obj.strftime('%B %d, %Y')
        md_content = f"# Daily Gainers Report - {date_formatted}\n\n"
        md_content += f"**Market Cap:** {market_cap.title()} | **Total Gainers:** {len(gainers)}\n\n"
        
        # Get Perplexity analysis for each stock
        for i, stock in enumerate(gainers, 1):
            ticker = stock['ticker']
            company_name = stock.get('company_name', ticker)
            return_pct = stock['return_pct']
            
            # Query Perplexity
            perplexity_analysis = query_perplexity_api(ticker, company_name, date_formatted, return_pct)
            
            # Build compact markdown for this stock
            md_content += f"## {i}. {company_name} ({ticker})\n\n"
            md_content += f"**Return:** {return_pct}% | **Price:** ${stock.get('start_price')} → ${stock.get('end_price')} | **Mkt Cap:** ${stock.get('market_cap', 0)/1e9:.1f}B\n\n"
            
            # Compact OHLC line
            if stock.get('open') and stock.get('high') and stock.get('low') and stock.get('close'):
                md_content += f"**OHLC:** O: ${stock['open']} | H: ${stock['high']} | L: ${stock['low']} | C: ${stock['close']}"
                
                # Close vs high on same line
                if stock['high'] > 0:
                    close_below_high = ((stock['high'] - stock['close']) / stock['high']) * 100
                    md_content += f" | **Close:** {close_below_high:.1f}% below high"
                md_content += "\n\n"
            
            # Volume and sector on one line
            volume_sector_line = ""
            if stock.get('volume'):
                volume_sector_line = f"**Volume:** {stock['volume']:,}"
                if stock.get('volume_change'):
                    volume_sector_line += f" ({stock['volume_change']} avg)"
            if stock.get('sector'):
                if volume_sector_line:
                    volume_sector_line += f" | **Sector:** {stock.get('sector')}"
                else:
                    volume_sector_line = f"**Sector:** {stock.get('sector')}"
            if volume_sector_line:
                md_content += f"{volume_sector_line}\n\n"
            
            md_content += f"**Analysis:** {perplexity_analysis}\n\n---\n\n"
        
        # Convert markdown to HTML
        html_content = markdown.markdown(md_content)
        
        # Wrap in basic HTML structure with compact styling
        html_full = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                @page {{
                    size: letter;
                    margin: 0.4in 0.5in;
                }}
                body {{
                    font-family: Arial, sans-serif;
                    font-size: 9pt;
                    line-height: 1.2;
                    margin: 0;
                    padding: 0;
                }}
                h1 {{
                    color: #1a1a1a;
                    font-size: 14pt;
                    font-weight: bold;
                    margin: 0 0 8px 0;
                    padding-bottom: 4px;
                    border-bottom: 2px solid #2c5aa0;
                }}
                h2 {{
                    color: #2c5aa0;
                    font-size: 11pt;
                    font-weight: bold;
                    margin: 10px 0 4px 0;
                }}
                p {{
                    margin: 3px 0;
                }}
                strong {{
                    color: #1a1a1a;
                    font-weight: 600;
                }}
                hr {{
                    border: none;
                    border-top: 1px solid #ccc;
                    margin: 8px 0;
                }}
            </style>
        </head>
        <body>
            {html_content}
        </body>
        </html>
        """
        
        # Generate PDF
        pdf_buffer = BytesIO()
        HTML(string=html_full).write_pdf(pdf_buffer)
        pdf_buffer.seek(0)
        pdf_data = pdf_buffer.read()
        
        # If email is provided, send it
        if email_to:
            success = send_email_with_pdf(email_to, date_formatted, market_cap, pdf_data)
            if not success:
                return jsonify({'error': 'Failed to send email'}), 500
            
            return jsonify({
                'success': True,
                'message': f'Report generated and emailed to {email_to}',
                'gainers_count': len(gainers)
            })
        else:
            # Return PDF as response
            from flask import send_file
            return send_file(
                BytesIO(pdf_data),
                mimetype='application/pdf',
                as_attachment=True,
                download_name=f'gainer_report_{date_str}_{market_cap}.pdf'
            )
    
    except Exception as e:
        logger.error(f"Error generating gainer report: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/send-daily-report', methods=['POST'])
def send_daily_report():
    """Generate comprehensive multi-cap daily report and email it"""
    try:
        import markdown
        from weasyprint import HTML
        from io import BytesIO
    except ImportError as ie:
        return jsonify({
            'error': f'Missing dependency: {str(ie)}. Please rebuild Docker containers.',
            'instructions': 'Run: docker compose build && docker compose up -d'
        }), 500
    
    try:
        data = request.json
        date_str = data.get('date')
        email_to = data.get('email', 'absunny2021@gmail.com')
        
        if not date_str:
            return jsonify({'error': 'Date is required'}), 400
        
        # Parse date
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        date_formatted = date_obj.strftime('%B %d, %Y')
        rewind_date = date_obj.strftime('%Y-%m-%d')
        
        # Get gainers from all cap categories
        cap_categories = {
            'micro': {'name': 'Micro Cap (<$200M)', 'limit': 10},
            'small': {'name': 'Small Cap ($200M-$2B)', 'limit': None},
            'mid': {'name': 'Mid Cap ($2B-$20B)', 'limit': None},
            'large': {'name': 'Large Cap ($20B-$100B)', 'limit': None},
            'mega': {'name': 'Mega Cap (>$100B)', 'limit': None}
        }
        
        all_gainers = {}
        total_stocks = 0
        
        # Temporarily set rewind date in request args
        original_args = request.args
        from werkzeug.datastructures import ImmutableMultiDict
        new_args = dict(original_args)
        new_args['rewind_date'] = rewind_date
        request.args = ImmutableMultiDict(new_args)
        
        try:
            # Get gainers for each category
            for cap_key, cap_info in cap_categories.items():
                try:
                    gainers = get_momentum_stocks(1, market_cap_category=cap_key)
                    if gainers:
                        limit = cap_info['limit']
                        if limit:
                            gainers = gainers[:limit]
                        all_gainers[cap_key] = {
                            'name': cap_info['name'],
                            'stocks': gainers
                        }
                        total_stocks += len(gainers)
                        logger.info(f"Retrieved {len(gainers)} gainers for {cap_key}")
                except Exception as e:
                    logger.error(f"Error getting gainers for {cap_key}: {str(e)}")
                    all_gainers[cap_key] = {
                        'name': cap_info['name'],
                        'stocks': []
                    }
        finally:
            # Restore original args
            request.args = original_args
        
        if total_stocks == 0:
            return jsonify({'error': 'No gainers found for the specified date'}), 404
        
        # Generate comprehensive markdown report
        md_content = f"# Daily Gainers Report - {date_formatted}\n\n"
        md_content += f"**Comprehensive Multi-Cap Analysis** | **Total Stocks:** {total_stocks}\n\n"
        md_content += "---\n\n"
        
        # Process each cap category
        for cap_key in ['mega', 'large', 'mid', 'small', 'micro']:
            if cap_key not in all_gainers or not all_gainers[cap_key]['stocks']:
                continue
            
            cap_data = all_gainers[cap_key]
            md_content += f"# {cap_data['name']}\n\n"
            md_content += f"**Count:** {len(cap_data['stocks'])} stocks\n\n"
            
            # Get Perplexity analysis for each stock
            for i, stock in enumerate(cap_data['stocks'], 1):
                ticker = stock['ticker']
                company_name = stock.get('company_name', ticker)
                return_pct = stock['return_pct']
                
                # Query Perplexity
                perplexity_analysis = query_perplexity_api(ticker, company_name, date_formatted, return_pct)
                
                # Build compact markdown for this stock
                md_content += f"## {i}. {company_name} ({ticker})\n\n"
                md_content += f"**Return:** {return_pct}% | **Price:** ${stock.get('start_price')} → ${stock.get('end_price')} | **Mkt Cap:** ${stock.get('market_cap', 0)/1e9:.1f}B\n\n"
                
                # Compact OHLC line
                if stock.get('open') and stock.get('high') and stock.get('low') and stock.get('close'):
                    md_content += f"**OHLC:** O: ${stock['open']} | H: ${stock['high']} | L: ${stock['low']} | C: ${stock['close']}"
                    
                    # Close vs high on same line
                    if stock['high'] > 0:
                        close_below_high = ((stock['high'] - stock['close']) / stock['high']) * 100
                        md_content += f" | **Close:** {close_below_high:.1f}% below high"
                    md_content += "\n\n"
                
                # Volume and sector on one line
                volume_sector_line = ""
                if stock.get('volume'):
                    volume_sector_line = f"**Volume:** {stock['volume']:,}"
                    if stock.get('volume_change'):
                        volume_sector_line += f" ({stock['volume_change']} avg)"
                if stock.get('sector'):
                    if volume_sector_line:
                        volume_sector_line += f" | **Sector:** {stock.get('sector')}"
                    else:
                        volume_sector_line = f"**Sector:** {stock.get('sector')}"
                if volume_sector_line:
                    md_content += f"{volume_sector_line}\n\n"
                
                md_content += f"**Analysis:** {perplexity_analysis}\n\n---\n\n"
            
            # Add section separator between cap categories
            md_content += "\n\n"
        
        # Convert markdown to HTML with compact styling
        html_content = markdown.markdown(md_content)
        
        html_full = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                @page {{
                    size: letter;
                    margin: 0.4in 0.5in;
                }}
                body {{
                    font-family: Arial, sans-serif;
                    font-size: 9pt;
                    line-height: 1.2;
                    margin: 0;
                    padding: 0;
                }}
                h1 {{
                    color: #1a1a1a;
                    font-size: 14pt;
                    font-weight: bold;
                    margin: 0 0 8px 0;
                    padding-bottom: 4px;
                    border-bottom: 2px solid #2c5aa0;
                }}
                h2 {{
                    color: #2c5aa0;
                    font-size: 11pt;
                    font-weight: bold;
                    margin: 10px 0 4px 0;
                }}
                p {{
                    margin: 3px 0;
                }}
                strong {{
                    color: #1a1a1a;
                    font-weight: 600;
                }}
                hr {{
                    border: none;
                    border-top: 1px solid #ccc;
                    margin: 8px 0;
                }}
            </style>
        </head>
        <body>
            {html_content}
        </body>
        </html>
        """
        
        # Generate PDF
        pdf_buffer = BytesIO()
        HTML(string=html_full).write_pdf(pdf_buffer)
        pdf_buffer.seek(0)
        pdf_data = pdf_buffer.read()
        
        # Send email with PDF
        success = send_email_with_pdf(
            email_to, 
            date_formatted, 
            'Multi-Cap', 
            pdf_data,
            filename=f'daily_report_{date_str}_all_caps.pdf'
        )
        
        if not success:
            return jsonify({'error': 'Failed to send email'}), 500
        
        return jsonify({
            'success': True,
            'message': f'Daily report generated and emailed to {email_to}',
            'total_stocks': total_stocks,
            'breakdown': {cap: len(all_gainers[cap]['stocks']) for cap in all_gainers}
        })
    
    except Exception as e:
        logger.error(f"Error sending daily report: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def query_perplexity_api(ticker, company_name, date_str, return_pct):
    """Query Perplexity API for stock movement analysis"""
    try:
        url = "https://api.perplexity.ai/chat/completions"
        headers = {
            "Authorization": f"Bearer {os.getenv('PERPLEXITY_API_KEY')}",
            "Content-Type": "application/json"
        }
        
        entity = f"{company_name} ({ticker})" if company_name else ticker
        
        prompt = f"""Why did {entity} stock move up {return_pct:.2f}% on {date_str}?

Search for specific events, news, or developments on or just before {date_str} including:
• Earnings reports, analyst upgrades, product launches, partnerships
• Sector developments, regulatory changes, market trends
• Media coverage (CNBC, Bloomberg), social media trends
• Technical factors (short squeezes, institutional buying)

Provide a concise summary (3-5 sentences) with specific sources and dates. If no catalyst found, note if it's part of broader sector/market moves."""

        data = {
            "model": "sonar-pro",
            "messages": [{"role": "user", "content": prompt}],
            "stream": False
        }
        
        response = requests.post(url, headers=headers, json=data, timeout=60)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"Error querying Perplexity for {ticker}: {str(e)}")
        return f"Unable to generate analysis: {str(e)}"


def send_email_with_pdf(to_email, date_str, market_cap, pdf_data, filename=None):
    """Send email with PDF attachment"""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication
    
    try:
        # Email configuration from environment variables
        smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        smtp_user = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')
        from_email = os.getenv('FROM_EMAIL', smtp_user)
        
        if not smtp_user or not smtp_password:
            logger.error("SMTP credentials not configured")
            return False
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = f'Daily Gainers Report - {date_str} ({market_cap.title()} Cap)'
        
        # Email body
        body = f"""
        Please find attached your Daily Gainers Report for {date_str} ({market_cap.title()} Cap).
        
        This report analyzes the top 1-day gainers with AI-powered insights.
        
        Best regards,
        Stocks Assist
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Attach PDF
        pdf_attachment = MIMEApplication(pdf_data, _subtype='pdf')
        pdf_filename = filename or f'gainer_report_{date_str}_{market_cap}.pdf'
        pdf_attachment.add_header('Content-Disposition', 'attachment', 
                                 filename=pdf_filename)
        msg.attach(pdf_attachment)
        
        # Send email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Email sent successfully to {to_email}")
        return True
    
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")
        return False


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
