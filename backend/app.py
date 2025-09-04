from flask import Flask, jsonify, request
from models import Stock, Price, Comment, Flags, ConciseNote, Earnings, StockRSI, init_db
from sqlalchemy import desc, func, and_, text
from datetime import datetime, timedelta
import os
import logging
import pytz
import requests
from dotenv import load_dotenv
import json
from pathlib import Path
from bs4 import BeautifulSoup

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
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

def get_momentum_stocks(return_days, above_200m=True, min_return_pct=None):
    """Get stocks with strong momentum over specified period"""
    try:
        # Determine date range based on return_days
        end_date = get_latest_price_date()
        if not end_date:
            return []

        start_date = end_date - timedelta(days=return_days)

        # Base query joining Stock and Price tables
        base_query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            func.min(Price.close_price).label('start_price'),
            func.max(Price.close_price).label('end_price'),
            ((func.max(Price.close_price) - func.min(Price.close_price)) / func.min(Price.close_price) * 100).label('return_pct')
        ).join(Price, Stock.ticker == Price.ticker)

        # Apply date filtering
        base_query = base_query.filter(Price.date >= start_date, Price.date <= end_date)

        # Apply market cap filter
        if above_200m:
            base_query = base_query.filter(Stock.market_cap >= 200000000)
        else:
            base_query = base_query.filter(Stock.market_cap < 200000000)

        # Apply minimum return percentage filter
        if min_return_pct is not None:
            base_query = base_query.filter(
                ((func.max(Price.close_price) - func.min(Price.close_price)) / func.min(Price.close_price) * 100) >= min_return_pct
            )
        else:
            # Use default threshold based on period
            threshold = RETURN_THRESHOLDS.get(return_days, 10)
            base_query = base_query.filter(
                ((func.max(Price.close_price) - func.min(Price.close_price)) / func.min(Price.close_price) * 100) >= threshold
            )

        # Apply global exclusions
        base_query = apply_global_exclude_filters(base_query)

        # Group by stock and order by return percentage
        stocks = base_query.group_by(
            Stock.ticker, Stock.company_name, Stock.sector,
            Stock.industry, Stock.market_cap, Stock.price
        ).order_by(desc('return_pct')).limit(100).all()

        # Format results
        results = []
        for stock in stocks:
            results.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'price': stock.price,
                'start_price': round(stock.start_price, 2),
                'end_price': round(stock.end_price, 2),
                'return_pct': round(stock.return_pct, 2)
            })

        return results

    except Exception as e:
        logger.error(f"Error getting momentum stocks for {return_days}d: {str(e)}")
        return []

def get_gapper_stocks(above_200m=True):
    """Get stocks that gapped up significantly"""
    try:
        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        previous_date = latest_date - timedelta(days=1)

        # Find previous trading day (skip weekends)
        while previous_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            previous_date -= timedelta(days=1)

        # Query for gap-ups
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            Price.close_price.label('prev_close'),
            Price.open_price.label('today_open')
        ).join(Price, Stock.ticker == Price.ticker)

        # Get yesterday's close and today's open
        query = query.filter(
            or_(
                and_(Price.date == previous_date, Price.close_price.isnot(None)),
                and_(Price.date == latest_date, Price.open_price.isnot(None))
            )
        )

        # Apply market cap filter
        if above_200m:
            query = query.filter(Stock.market_cap >= 200000000)
        else:
            query = query.filter(Stock.market_cap < 200000000)

        # Apply global exclusions
        query = apply_global_exclude_filters(query)

        # Group by stock to get both dates
        gap_stocks = query.group_by(Stock.ticker).having(
            func.count(Price.date) == 2  # Must have both dates
        ).all()

        # Calculate gap percentages and filter
        results = []
        for stock in gap_stocks:
            if stock.prev_close and stock.today_open:
                gap_pct = ((stock.today_open - stock.prev_close) / stock.prev_close) * 100
                if gap_pct >= 5:  # Minimum 5% gap
                    results.append({
                        'ticker': stock.ticker,
                        'company_name': stock.company_name,
                        'sector': stock.sector,
                        'industry': stock.industry,
                        'market_cap': stock.market_cap,
                        'price': stock.price,
                        'prev_close': round(stock.prev_close, 2),
                        'today_open': round(stock.today_open, 2),
                        'gap_pct': round(gap_pct, 2)
                    })

        # Sort by gap percentage descending and limit to top 100
        results.sort(key=lambda x: x['gap_pct'], reverse=True)
        return results[:100]

    except Exception as e:
        logger.error(f"Error getting gapper stocks: {str(e)}")
        return []

def get_volume_spike_stocks(above_200m=True):
    """Get stocks with significant volume spikes"""
    try:
        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        # Calculate average volume over last 20 trading days
        avg_volume_query = session.query(
            Price.ticker,
            func.avg(Price.volume).label('avg_volume')
        ).filter(
            Price.date >= latest_date - timedelta(days=30)  # Roughly 20 trading days
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
        if above_200m:
            query = query.filter(Stock.market_cap >= 200000000)
        else:
            query = query.filter(Stock.market_cap < 200000000)

        # Apply global exclusions
        query = apply_global_exclude_filters(query)

        volume_stocks = query.all()

        # Calculate volume ratios and filter
        results = []
        for stock in volume_stocks:
            if stock.avg_volume and stock.avg_volume > 0:
                volume_ratio = stock.today_volume / stock.avg_volume
                if volume_ratio >= 2:  # At least 2x average volume
                    results.append({
                        'ticker': stock.ticker,
                        'company_name': stock.company_name,
                        'sector': stock.sector,
                        'industry': stock.industry,
                        'market_cap': stock.market_cap,
                        'price': stock.price,
                        'today_volume': int(stock.today_volume),
                        'avg_volume': round(stock.avg_volume, 0),
                        'volume_ratio': round(volume_ratio, 1),
                        'today_close': round(stock.today_close, 2),
                        'today_open': round(stock.today_open, 2)
                    })

        # Sort by volume ratio descending and limit to top 100
        results.sort(key=lambda x: x['volume_ratio'], reverse=True)
        return results[:100]

    except Exception as e:
        logger.error(f"Error getting volume spike stocks: {str(e)}")
        return []

def get_sea_change_stocks(above_200m=True):
    """Get stocks that have changed direction recently (sea change)"""
    try:
        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        # Get price data for last 60 days
        end_date = latest_date
        start_date = end_date - timedelta(days=60)

        # Query for stocks with recent direction change
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            Stock.price,
            func.avg(Price.close_price).label('avg_price_60d'),
            func.avg(case((Price.date >= end_date - timedelta(days=10), Price.close_price), else_=None)).label('avg_price_10d'),
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
        return results[:100]

    except Exception as e:
        logger.error(f"Error getting sea change stocks: {str(e)}")
        return []

def get_all_returns_data(above_200m=True, weights=None):
    """Calculate all returns data with weighted scoring"""
    try:
        if weights is None:
            weights = DEFAULT_PRIORITY_WEIGHTS

        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        # Define periods for return calculation
        periods = [
            (1, '1d_return'),
            (5, '5d_return'),
            (20, '20d_return'),
            (60, '60d_return'),
            (120, '120d_return')
        ]

        # Build dynamic query for returns
        query_parts = []
        for days, label in periods:
            start_date = latest_date - timedelta(days=days)
            query_parts.append(
                func.avg(case((Price.date >= start_date, Price.close_price), else_=None)).label(f'{label}_avg')
            )

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
         .filter(Price.date >= latest_date - timedelta(days=120))

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
        return results[:100]

    except Exception as e:
        logger.error(f"Error getting all returns data: {str(e)}")
        return []

def get_all_returns_data_for_tickers(tickers, above_200m=None):
    """Get all returns data for specific tickers"""
    try:
        # Get latest price date
        latest_date = get_latest_price_date()
        if not latest_date:
            return []

        # Define periods for return calculation
        periods = [
            (1, '1d_return'),
            (5, '5d_return'),
            (20, '20d_return'),
            (60, '60d_return'),
            (120, '120d_return')
        ]

        # Build dynamic query for returns
        query_parts = []
        for days, label in periods:
            start_date = latest_date - timedelta(days=days)
            query_parts.append(
                func.avg(case((Price.date >= start_date, Price.close_price), else_=None)).label(f'{label}_avg')
            )

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
         .filter(Price.date >= latest_date - timedelta(days=120))\
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
    """Get the latest available price date"""
    try:
        latest_date = get_latest_price_date()
        if latest_date:
            return jsonify({
                'latest_date': latest_date.strftime('%Y-%m-%d'),
                'formatted_date': latest_date.strftime('%B %d, %Y')
            })
        else:
            return jsonify({'error': 'No price data available'}), 404

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

# Gapper API endpoints
@app.route('/api/Gapper-Above200M')
def get_gapper_above_200m():
    return jsonify(get_gapper_stocks(above_200m=True))

@app.route('/api/Gapper-Below200M')
def get_gapper_below_200m():
    return jsonify(get_gapper_stocks(above_200m=False))

# Volume API endpoints
@app.route('/api/Volume-Above200M')
def get_volume_above_200m():
    return jsonify(get_volume_spike_stocks(above_200m=True))

@app.route('/api/Volume-Below200M')
def get_volume_below_200m():
    return jsonify(get_volume_spike_stocks(above_200m=False))

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

        stock_data = {
            'ticker': stock.ticker,
            'company_name': stock.company_name,
            'sector': stock.sector,
            'industry': stock.industry,
            'market_cap': stock.market_cap,
            'price': stock.price,
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
    """Get market cap distribution statistics"""
    try:
        # Get market cap ranges
        ranges = [
            (0, 500000000, "Under $500M"),
            (500000000, 1000000000, "500M - 1B"),
            (1000000000, 5000000000, "1B - 5B"),
            (5000000000, 10000000000, "5B - 10B"),
            (10000000000, 50000000000, "10B - 50B"),
            (50000000000, float('inf'), "Over $50B")
        ]

        distribution = []
        for min_cap, max_cap, label in ranges:
            if max_cap == float('inf'):
                count = session.query(Stock).filter(Stock.market_cap >= min_cap).count()
            else:
                count = session.query(Stock).filter(Stock.market_cap >= min_cap, Stock.market_cap < max_cap).count()

            distribution.append({
                'range': label,
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

                    # Extract time
                    time_div = time_cell.find('div')
                    time_text = time_div.get_text(strip=True) if time_div else ""

                    # Extract headline and link
                    headline_link = headline_cell.find('a')
                    if headline_link:
                        headline = headline_link.get_text(strip=True)
                        link = headline_link.get('href')

                        # Get source if available
                        source_div = headline_cell.find('span', {'class': 'news-link-right'})
                        source = source_div.get_text(strip=True) if source_div else ""

                        news_items.append({
                            'time': time_text,
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
