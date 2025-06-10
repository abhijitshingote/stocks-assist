from flask import Flask, render_template, jsonify, request
from models import Stock, Price, Comment, init_db
from sqlalchemy import desc, func, and_, text
from datetime import datetime, timedelta
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(
    __name__,
    static_folder='static',
    template_folder='templates'
)
session = init_db()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/Return1D')
def return_1d():
    return render_template('return_1d.html')

@app.route('/Return5D')
def return_5d():
    return render_template('return_5d.html')

@app.route('/Return20D')
def return_20d():
    return render_template('return_20d.html')

@app.route('/Return60D')
def return_60d():
    return render_template('return_60d.html')

@app.route('/Return120D')
def return_120d():
    return render_template('return_120d.html')

@app.route('/Gapper')
def gapper():
    return render_template('gapper.html')

@app.route('/Volume')
def volume():
    return render_template('volume.html')

@app.route('/AllReturns')
def all_returns():
    return render_template('all_returns.html')

@app.route('/ticker/<ticker>')
def ticker_detail(ticker):
    """Ticker detail page with company info and candlestick chart"""
    return render_template('ticker_detail.html', ticker=ticker.upper())

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
    """Add a new AI comment (pending review)"""
    try:
        ticker = ticker.upper()
        data = request.get_json()
        
        if not data or 'comment_text' not in data:
            return jsonify({'error': 'Comment text is required'}), 400
        
        comment_text = data['comment_text'].strip()
        if not comment_text:
            return jsonify({'error': 'Comment text cannot be empty'}), 400
        
        ai_source = data.get('ai_source', 'unknown')
        
        # Verify ticker exists
        stock = session.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            return jsonify({'error': 'Ticker not found'}), 404
        
        # Create new AI comment (pending review)
        new_comment = Comment(
            ticker=ticker,
            comment_text=comment_text,
            comment_type='ai',
            status='pending',
            ai_source=ai_source
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
    """Approve or reject an AI comment"""
    try:
        data = request.get_json()
        
        if not data or 'action' not in data:
            return jsonify({'error': 'Action is required (approve or reject)'}), 400
        
        action = data['action'].lower()
        if action not in ['approve', 'reject']:
            return jsonify({'error': 'Action must be approve or reject'}), 400
        
        reviewed_by = data.get('reviewed_by', 'user')
        
        # Find the comment
        comment = session.query(Comment).filter(
            Comment.id == comment_id,
            Comment.comment_type == 'ai',
            Comment.status == 'pending'
        ).first()
        
        if not comment:
            return jsonify({'error': 'Pending AI comment not found'}), 404
        
        # Update comment status
        comment.status = 'approved' if action == 'approve' else 'rejected'
        comment.reviewed_by = reviewed_by
        comment.reviewed_at = datetime.now()
        
        session.commit()
        
        # Return updated comment
        comment_data = {
            'id': comment.id,
            'ticker': comment.ticker,
            'comment_text': comment.comment_text,
            'comment_type': comment.comment_type,
            'status': comment.status,
            'ai_source': comment.ai_source,
            'reviewed_by': comment.reviewed_by,
            'reviewed_at': comment.reviewed_at.strftime('%Y-%m-%d %H:%M:%S') if comment.reviewed_at else None,
            'created_at': comment.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'formatted_date': comment.created_at.strftime('%B %d, %Y at %I:%M %p')
        }
        
        return jsonify(comment_data)
        
    except Exception as e:
        logger.error(f"Error reviewing AI comment {comment_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/latest_date')
def get_latest_date():
    """Get the latest date for which we have price data"""
    try:
        latest_date = session.query(func.max(Price.date)).scalar()
        return jsonify({
            'latest_date': latest_date.strftime('%Y-%m-%d') if latest_date else None
        })
    except Exception as e:
        logger.error(f"Error getting latest date: {str(e)}")
        return jsonify({'error': str(e)}), 500

def get_momentum_stocks(return_days, above_200m=True):
    """Helper function to get momentum stocks for different return periods"""
    try:
        # Get the latest date for which we have price data
        latest_date = session.query(func.max(Price.date)).scalar()
        if not latest_date:
            return []
        
        # Get the actual trading dates for the return period (not calendar days)
        trading_dates = session.query(Price.date).distinct().order_by(Price.date.desc()).limit(return_days + 1).all()
        if len(trading_dates) < return_days + 1:
            logger.warning(f"Only {len(trading_dates)} trading dates available, need at least {return_days + 1}")
            return []
        
        start_date = trading_dates[-1][0]  # The (return_days)th trading day back
        
        # Get the last 10 actual trading dates for average calculations
        last_10_trading_dates = session.query(Price.date).distinct().order_by(Price.date.desc()).limit(10).all()
        if len(last_10_trading_dates) < 10:
            ten_days_ago = last_10_trading_dates[-1][0]
        else:
            ten_days_ago = last_10_trading_dates[-1][0]  # 10th trading day back
        
        logger.info(f"Calculating {return_days}D returns from {start_date} to {latest_date} (actual trading days)")
        
        # Get start prices (prices from the start_date)
        start_prices = session.query(
            Price.ticker,
            Price.close_price.label('start_price'),
            Price.date.label('start_date')
        ).filter(
            Price.date == start_date
        ).subquery()
        
        end_prices = session.query(
            Price.ticker,
            Price.close_price.label('end_price'),
            Price.date.label('end_date')
        ).filter(
            Price.date == latest_date
        ).subquery()
        
        # Calculate average prices and volumes over the last 10 trading days
        avg_stats = session.query(
            Price.ticker,
            func.avg(Price.close_price).label('avg_10day_price'),
            func.avg(Price.volume * Price.close_price).label('avg_dollar_volume')
        ).filter(
            Price.date >= ten_days_ago,
            Price.date <= latest_date
        ).group_by(Price.ticker).subquery()
        
        # Combine all the data
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            end_prices.c.end_price.label('current_price'),
            avg_stats.c.avg_10day_price,
            avg_stats.c.avg_dollar_volume,
            ((end_prices.c.end_price - start_prices.c.start_price) / 
             start_prices.c.start_price * 100).label('price_change_pct')
        ).join(
            start_prices,
            Stock.ticker == start_prices.c.ticker
        ).join(
            end_prices,
            Stock.ticker == end_prices.c.ticker
        ).join(
            avg_stats,
            Stock.ticker == avg_stats.c.ticker
        )
        
        # Apply market cap filter
        if above_200m:
            query = query.filter(Stock.market_cap >= 200000000)  # Market cap >= $200M
        else:
            query = query.filter(Stock.market_cap < 200000000)   # Market cap < $200M
            
        # Apply other filters
        query = query.filter(
            avg_stats.c.avg_10day_price >= 5,  # 10 trading day average price > $5
            avg_stats.c.avg_dollar_volume >= 10000000,  # Average daily dollar volume > $10M
            ((end_prices.c.end_price - start_prices.c.start_price) / 
             start_prices.c.start_price * 100) >= 5  # price change > 5%
        ).order_by(desc('price_change_pct'))
        
        momentum_stocks = query.all()
        
        result = []
        for stock in momentum_stocks:
            # Format market cap
            market_cap = stock.market_cap
            if market_cap >= 1000000000:  # >= 1B
                market_cap_formatted = f"{market_cap / 1000000000:.1f}B"
                # Remove trailing .0
                if market_cap_formatted.endswith('.0B'):
                    market_cap_formatted = market_cap_formatted[:-3] + 'B'
            else:  # < 1B, show in millions
                market_cap_formatted = f"{market_cap / 1000000:.0f}M"
            
            result.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': market_cap_formatted,
                'current_price': f"${stock.current_price:.2f}",
                'avg_10day_price': f"${stock.avg_10day_price:.2f}",
                'avg_dollar_volume': f"${stock.avg_dollar_volume:,.0f}",
                'price_change_pct': f"{stock.price_change_pct:.1f}%",
                'latest_date': latest_date.strftime('%Y-%m-%d')
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_momentum_stocks: {str(e)}")
        raise e

# 1D Return APIs
@app.route('/api/Return1D-Above200M')
def get_return_1d_above_200m():
    try:
        result = get_momentum_stocks(1, above_200m=True)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_1d_above_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/Return1D-Below200M')
def get_return_1d_below_200m():
    try:
        result = get_momentum_stocks(1, above_200m=False)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_1d_below_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 5D Return APIs
@app.route('/api/Return5D-Above200M')
def get_return_5d_above_200m():
    try:
        result = get_momentum_stocks(5, above_200m=True)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_5d_above_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/Return5D-Below200M')
def get_return_5d_below_200m():
    try:
        result = get_momentum_stocks(5, above_200m=False)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_5d_below_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 60D Return APIs
@app.route('/api/Return60D-Above200M')
def get_return_60d_above_200m():
    try:
        result = get_momentum_stocks(60, above_200m=True)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_60d_above_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/Return60D-Below200M')
def get_return_60d_below_200m():
    try:
        result = get_momentum_stocks(60, above_200m=False)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_60d_below_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 120D Return APIs
@app.route('/api/Return120D-Above200M')
def get_return_120d_above_200m():
    try:
        result = get_momentum_stocks(120, above_200m=True)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_120d_above_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/Return120D-Below200M')
def get_return_120d_below_200m():
    try:
        result = get_momentum_stocks(120, above_200m=False)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_120d_below_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

def get_gapper_stocks(above_200m=True):
    """Helper function to get gapper stocks based on specific criteria"""
    try:
        # Get the latest date for which we have price data
        latest_date = session.query(func.max(Price.date)).scalar()
        if not latest_date:
            return []
        
        # Get the last 5 actual trading dates (not calendar days)
        last_5_trading_dates = session.query(Price.date).distinct().order_by(Price.date.desc()).limit(5).all()
        if len(last_5_trading_dates) < 5:
            logger.warning(f"Only {len(last_5_trading_dates)} trading dates available, need at least 5")
            return []
        
        earliest_date_5d = last_5_trading_dates[-1][0]  # 5th trading day back
        
        # Get the last 50 actual trading dates for volume calculation
        last_50_trading_dates = session.query(Price.date).distinct().order_by(Price.date.desc()).limit(50).all()
        if len(last_50_trading_dates) < 50:
            # Use whatever dates we have if less than 50
            earliest_date_50d = last_50_trading_dates[-1][0]
        else:
            earliest_date_50d = last_50_trading_dates[-1][0]  # 50th trading day back
        
        logger.info(f"Calculating gapper stocks for last 5 trading days: {earliest_date_5d} to {latest_date}")
        
        # Get the current price (latest date)
        current_prices = session.query(
            Price.ticker,
            Price.close_price.label('current_price'),
            Price.volume.label('current_volume')
        ).filter(
            Price.date == latest_date
        ).subquery()
        
        # Get 50-day average volume for each stock using actual trading dates
        avg_volume_50d = session.query(
            Price.ticker,
            func.avg(Price.volume).label('avg_volume_50d')
        ).filter(
            Price.date >= earliest_date_50d,
            Price.date <= latest_date
        ).group_by(Price.ticker).subquery()
        
        # Get daily price data for the last 5 trading days with previous day's close
        price_analysis = session.query(
            Price.ticker,
            Price.date,
            Price.open_price,
            Price.high_price,
            Price.low_price,
            Price.close_price,
            Price.volume,
            func.lag(Price.close_price, 1).over(
                partition_by=Price.ticker,
                order_by=Price.date
            ).label('prev_close')
        ).filter(
            Price.date >= earliest_date_5d,
            Price.date <= latest_date
        ).subquery()
        
        # Find stocks that meet gapper criteria in the last 5 trading days
        gapper_criteria = session.query(
            price_analysis.c.ticker,
            func.bool_or(
                # Low was more than 5% above previous day's close
                (price_analysis.c.low_price > price_analysis.c.prev_close * 1.05) |
                # OR closing price was 15% higher than previous day's close
                (price_analysis.c.close_price > price_analysis.c.prev_close * 1.15)
            ).label('is_gapper'),
            func.max(Price.volume).label('max_volume_5d')
        ).select_from(
            price_analysis.join(
                Price,
                (price_analysis.c.ticker == Price.ticker) &
                (price_analysis.c.date == Price.date)
            )
        ).filter(
            price_analysis.c.prev_close.isnot(None)  # Ensure we have previous day data
        ).group_by(price_analysis.c.ticker).subquery()
        
        # Main query combining all criteria
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            current_prices.c.current_price,
            avg_volume_50d.c.avg_volume_50d,
            gapper_criteria.c.max_volume_5d
        ).join(
            current_prices,
            Stock.ticker == current_prices.c.ticker
        ).join(
            avg_volume_50d,
            Stock.ticker == avg_volume_50d.c.ticker
        ).join(
            gapper_criteria,
            Stock.ticker == gapper_criteria.c.ticker
        ).filter(
            # Current price > $3
            current_prices.c.current_price > 3,
            # Average volume over past 50 days > 500,000
            avg_volume_50d.c.avg_volume_50d > 500000,
            # At least one day in past 5 trading days had volume > 4,000,000
            gapper_criteria.c.max_volume_5d > 4000000,
            # Must meet gapper criteria
            gapper_criteria.c.is_gapper == True
        )
        
        # Apply market cap filter
        if above_200m:
            query = query.filter(Stock.market_cap >= 200000000)  # Market cap >= $200M
        else:
            query = query.filter(Stock.market_cap < 200000000)   # Market cap < $200M
            
        # Order by market cap descending
        query = query.order_by(desc(Stock.market_cap))
        
        gapper_stocks = query.all()
        
        result = []
        for stock in gapper_stocks:
            # Format market cap
            market_cap = stock.market_cap
            if market_cap >= 1000000000:  # >= 1B
                market_cap_formatted = f"{market_cap / 1000000000:.1f}B"
                # Remove trailing .0
                if market_cap_formatted.endswith('.0B'):
                    market_cap_formatted = market_cap_formatted[:-3] + 'B'
            else:  # < 1B, show in millions
                market_cap_formatted = f"{market_cap / 1000000:.0f}M"
            
            result.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': market_cap_formatted,
                'current_price': f"${stock.current_price:.2f}",
                'avg_volume_50d': f"{stock.avg_volume_50d:,.0f}",
                'max_volume_5d': f"{stock.max_volume_5d:,.0f}",
                'latest_date': latest_date.strftime('%Y-%m-%d')
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_gapper_stocks: {str(e)}")
        raise e

# Gapper Screen APIs
@app.route('/api/Gapper-Above200M')
def get_gapper_above_200m():
    try:
        result = get_gapper_stocks(above_200m=True)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_gapper_above_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/Gapper-Below200M')
def get_gapper_below_200m():
    try:
        result = get_gapper_stocks(above_200m=False)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_gapper_below_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

def get_volume_spike_stocks(above_200m=True):
    """Helper function to get volume spike stocks based on specific criteria"""
    try:
        # Get the latest date (today) and yesterday
        latest_date = session.query(func.max(Price.date)).scalar()
        if not latest_date:
            return []
        
        # Get the last 2 trading dates to get today and yesterday
        last_2_trading_dates = session.query(Price.date).distinct().order_by(Price.date.desc()).limit(2).all()
        if len(last_2_trading_dates) < 2:
            logger.warning(f"Need at least 2 trading dates, only {len(last_2_trading_dates)} available")
            return []
        
        today = last_2_trading_dates[0][0]
        yesterday = last_2_trading_dates[1][0]
        
        # Get the 5 trading days before yesterday (excluding yesterday)
        # We need 7 dates total: today, yesterday, + 5 days before yesterday
        last_7_trading_dates = session.query(Price.date).distinct().order_by(Price.date.desc()).limit(7).all()
        if len(last_7_trading_dates) < 7:
            logger.warning(f"Need at least 7 trading dates, only {len(last_7_trading_dates)} available")
            return []
        
        # The 5 days before yesterday (excluding today and yesterday)
        five_days_before_yesterday = [date[0] for date in last_7_trading_dates[2:]]  # Skip today and yesterday
        
        logger.info(f"Volume screening: today={today}, yesterday={yesterday}, 5 days before: {five_days_before_yesterday}")
        
        # Get today's closing prices
        today_prices = session.query(
            Price.ticker,
            Price.close_price.label('today_close')
        ).filter(
            Price.date == today
        ).subquery()
        
        # Get yesterday's data (volume and close)
        yesterday_data = session.query(
            Price.ticker,
            Price.volume.label('yesterday_volume'),
            Price.close_price.label('yesterday_close')
        ).filter(
            Price.date == yesterday
        ).subquery()
        
        # Calculate average volume for the 5 days before yesterday (excluding yesterday)
        avg_volume_5d = session.query(
            Price.ticker,
            func.avg(Price.volume).label('avg_volume_5d')
        ).filter(
            Price.date.in_(five_days_before_yesterday)
        ).group_by(Price.ticker).subquery()
        
        # Main query combining all criteria
        query = session.query(
            Stock.ticker,
            Stock.company_name,
            Stock.sector,
            Stock.industry,
            Stock.market_cap,
            today_prices.c.today_close,
            yesterday_data.c.yesterday_close,
            yesterday_data.c.yesterday_volume,
            avg_volume_5d.c.avg_volume_5d,
            ((today_prices.c.today_close - yesterday_data.c.yesterday_close) / 
             yesterday_data.c.yesterday_close * 100).label('price_change_pct')
        ).join(
            today_prices,
            Stock.ticker == today_prices.c.ticker
        ).join(
            yesterday_data,
            Stock.ticker == yesterday_data.c.ticker
        ).join(
            avg_volume_5d,
            Stock.ticker == avg_volume_5d.c.ticker
        ).filter(
            # Yesterday's volume > 5x average of previous 5 days
            yesterday_data.c.yesterday_volume > avg_volume_5d.c.avg_volume_5d * 5,
            # Today's close > 3% higher than yesterday's close
            today_prices.c.today_close > yesterday_data.c.yesterday_close * 1.03,
            # 5-day average volume > 50,000
            avg_volume_5d.c.avg_volume_5d > 50000
        )
        
        # Apply market cap filter
        if above_200m:
            query = query.filter(Stock.market_cap >= 200000000)  # Market cap >= $200M
        else:
            query = query.filter(Stock.market_cap < 200000000)   # Market cap < $200M
            
        # Order by price change percentage descending
        query = query.order_by(desc('price_change_pct'))
        
        volume_stocks = query.all()
        
        result = []
        for stock in volume_stocks:
            # Format market cap
            market_cap = stock.market_cap
            if market_cap >= 1000000000:  # >= 1B
                market_cap_formatted = f"{market_cap / 1000000000:.1f}B"
                # Remove trailing .0
                if market_cap_formatted.endswith('.0B'):
                    market_cap_formatted = market_cap_formatted[:-3] + 'B'
            else:  # < 1B, show in millions
                market_cap_formatted = f"{market_cap / 1000000:.0f}M"
            
            # Calculate volume multiplier
            volume_multiplier = stock.yesterday_volume / stock.avg_volume_5d if stock.avg_volume_5d > 0 else 0
            
            result.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': market_cap_formatted,
                'today_close': f"${stock.today_close:.2f}",
                'yesterday_close': f"${stock.yesterday_close:.2f}",
                'price_change_pct': f"{stock.price_change_pct:.1f}%",
                'yesterday_volume': f"{stock.yesterday_volume:,.0f}",
                'avg_volume_5d': f"{stock.avg_volume_5d:,.0f}",
                'volume_multiplier': f"{volume_multiplier:.1f}x",
                'today_date': today.strftime('%Y-%m-%d'),
                'yesterday_date': yesterday.strftime('%Y-%m-%d')
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_volume_spike_stocks: {str(e)}")
        raise e

# Volume Screen APIs
@app.route('/api/Volume-Above200M')
def get_volume_above_200m():
    try:
        result = get_volume_spike_stocks(above_200m=True)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_volume_above_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/Volume-Below200M')
def get_volume_below_200m():
    try:
        result = get_volume_spike_stocks(above_200m=False)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_volume_below_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

def get_all_returns_data(above_200m=True, weights=None):
    """Helper function to get all return periods data in one consolidated view"""
    try:
        # Default weights
        if weights is None:
            weights = {
                '1d': 3.0,
                '5d': 2.5,
                '20d': 2.0,
                '60d': 1.0,
                '120d': 1.0
            }
        
        # Get data for all return periods
        returns_1d = get_momentum_stocks(1, above_200m=above_200m)
        returns_5d = get_momentum_stocks(5, above_200m=above_200m)
        returns_20d = get_momentum_stocks(20, above_200m=above_200m)
        returns_60d = get_momentum_stocks(60, above_200m=above_200m)
        returns_120d = get_momentum_stocks(120, above_200m=above_200m)
        
        # Create dictionaries for quick lookup by ticker
        returns_data = {}
        
        # Process each return period
        for period_data, period_name in [
            (returns_1d, '1d'),
            (returns_5d, '5d'),
            (returns_20d, '20d'),
            (returns_60d, '60d'),
            (returns_120d, '120d')
        ]:
            for stock in period_data:
                ticker = stock['ticker']
                if ticker not in returns_data:
                    # Initialize stock data with basic info
                    returns_data[ticker] = {
                        'ticker': stock['ticker'],
                        'company_name': stock['company_name'],
                        'sector': stock['sector'],
                        'industry': stock['industry'],
                        'market_cap': stock['market_cap'],
                        'current_price': stock['current_price'],
                        'latest_date': stock['latest_date'],
                        'return_periods': [],
                        'priority_score': 0.0,
                        'returns_1d': None,
                        'returns_5d': None,
                        'returns_20d': None,
                        'returns_60d': None,
                        'returns_120d': None
                    }
                
                # Add the return percentage for this period and track which periods it appears in
                returns_data[ticker][f'returns_{period_name}'] = stock['price_change_pct']
                if period_name not in returns_data[ticker]['return_periods']:
                    returns_data[ticker]['return_periods'].append(period_name)
        
        # Calculate priority scores by multiplying weights by actual return values
        for ticker, stock_data in returns_data.items():
            priority_score = 0.0
            
            for period_name in ['1d', '5d', '20d', '60d', '120d']:
                return_value_str = stock_data.get(f'returns_{period_name}')
                if return_value_str is not None:
                    try:
                        # Extract numeric value from return string (e.g., "5.2%" -> 5.2)
                        return_value = float(return_value_str.replace('%', ''))
                        weight = weights.get(period_name, 0)
                        priority_score += weight * return_value
                    except (ValueError, AttributeError):
                        # If return value can't be parsed, treat as 0
                        pass
            
            stock_data['priority_score'] = round(priority_score, 2)
        
        # Convert to list and sort by priority score (descending), then by market cap
        result = list(returns_data.values())
        
        # Sort by priority score first (descending), then by market cap (descending)
        def extract_market_cap_value(market_cap_str):
            if not market_cap_str or market_cap_str == 'N/A':
                return 0
            try:
                if market_cap_str.endswith('B'):
                    return float(market_cap_str[:-1]) * 1000000000
                elif market_cap_str.endswith('M'):
                    return float(market_cap_str[:-1]) * 1000000
                else:
                    return 0
            except:
                return 0
        
        result.sort(key=lambda x: (x['priority_score'], extract_market_cap_value(x['market_cap'])), reverse=True)
        
        logger.info(f"Consolidated returns data: {len(result)} stocks with various return periods, sorted by weighted priority")
        return result
        
    except Exception as e:
        logger.error(f"Error in get_all_returns_data: {str(e)}")
        raise e

# Consolidated Returns APIs
@app.route('/api/AllReturns-Above200M')
def get_all_returns_above_200m():
    try:
        # Get weights from query parameters
        weights = {}
        weights['1d'] = float(request.args.get('weight_1d', 3.0))
        weights['5d'] = float(request.args.get('weight_5d', 2.5))
        weights['20d'] = float(request.args.get('weight_20d', 2.0))
        weights['60d'] = float(request.args.get('weight_60d', 1.0))
        weights['120d'] = float(request.args.get('weight_120d', 1.0))
        
        result = get_all_returns_data(above_200m=True, weights=weights)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_all_returns_above_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/AllReturns-Below200M')
def get_all_returns_below_200m():
    try:
        # Get weights from query parameters
        weights = {}
        weights['1d'] = float(request.args.get('weight_1d', 3.0))
        weights['5d'] = float(request.args.get('weight_5d', 2.5))
        weights['20d'] = float(request.args.get('weight_20d', 2.0))
        weights['60d'] = float(request.args.get('weight_60d', 1.0))
        weights['120d'] = float(request.args.get('weight_120d', 1.0))
        
        result = get_all_returns_data(above_200m=False, weights=weights)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_all_returns_below_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stocks')
def get_stocks():
    sector = request.args.get('sector')
    industry = request.args.get('industry')
    min_market_cap = request.args.get('min_market_cap', type=float)
    max_pe_ratio = request.args.get('max_pe_ratio', type=float)
    
    # Get the latest price data for each stock
    latest_prices = session.query(
        Price.ticker,
        func.max(Price.date).label('max_date')
    ).group_by(Price.ticker).subquery()
    
    # Join Stock with the latest Price data
    query = session.query(
        Stock,
        Price
    ).join(
        latest_prices,
        Stock.ticker == latest_prices.c.ticker
    ).join(
        Price,
        (Price.ticker == latest_prices.c.ticker) & 
        (Price.date == latest_prices.c.max_date)
    )
    
    if sector:
        query = query.filter(Stock.sector == sector)
    if industry:
        query = query.filter(Stock.industry == industry)
    if min_market_cap:
        query = query.filter(Stock.market_cap >= min_market_cap)
    if max_pe_ratio:
        query = query.filter(Stock.pe_ratio <= max_pe_ratio)
    
    results = query.all()
    
    result = []
    for stock, price in results:
        result.append({
            'ticker': stock.ticker,
            'company_name': stock.company_name,
            'sector': stock.sector,
            'industry': stock.industry,
            'close_price': price.close_price,
            'volume': price.volume,
            'market_cap': stock.market_cap,
            'date': price.date.strftime('%Y-%m-%d') if price.date else None
        })
    return jsonify(result)

@app.route('/api/sectors')
def get_sectors():
    sectors = session.query(Stock.sector).distinct().all()
    return jsonify([sector[0] for sector in sectors if sector[0]])

@app.route('/api/industries')
def get_industries():
    industries = session.query(Stock.industry).distinct().all()
    return jsonify([industry[0] for industry in industries if industry[0]])

@app.route('/api/stock/<ticker>')
def get_stock_details(ticker):
    # Get stock details
    db_stock = session.query(Stock).filter_by(ticker=ticker).first()
    if not db_stock:
        return jsonify({'error': 'Stock not found'}), 404
    
    # Get latest price data
    latest_price = session.query(Price).filter_by(ticker=ticker).order_by(Price.date.desc()).first()
    
    return jsonify({
        'ticker': db_stock.ticker,
        'company_name': db_stock.company_name,
        'sector': db_stock.sector,
        'industry': db_stock.industry,
        'open_price': latest_price.open_price if latest_price else None,
        'high_price': latest_price.high_price if latest_price else None,
        'low_price': latest_price.low_price if latest_price else None,
        'close_price': latest_price.close_price if latest_price else None,
        'volume': latest_price.volume if latest_price else None,
        'market_cap': db_stock.market_cap,
        'shares_outstanding': db_stock.shares_outstanding,
        'date': latest_price.date.strftime('%Y-%m-%d') if latest_price and latest_price.date else None
    })

@app.route('/api/stock/<ticker>/prices')
def get_stock_prices(ticker):
    # Get historical price data
    prices = session.query(Price).filter_by(ticker=ticker).order_by(Price.date.desc()).all()
    
    if not prices:
        return jsonify({'error': 'No price data found'}), 404
    
    return jsonify([{
        'date': price.date.strftime('%Y-%m-%d'),
        'open_price': price.open_price,
        'high_price': price.high_price,
        'low_price': price.low_price,
        'close_price': price.close_price,
        'volume': price.volume
    } for price in prices])

# 20D Return APIs
@app.route('/api/Return20D-Above200M')
def get_return_20d_above_200m():
    try:
        result = get_momentum_stocks(20, above_200m=True)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_20d_above_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/Return20D-Below200M')
def get_return_20d_below_200m():
    try:
        result = get_momentum_stocks(20, above_200m=False)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_return_20d_below_200m: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/market_cap_distribution')
def get_market_cap_distribution():
    # Get total count of all stocks
    total_stocks = session.query(func.count(Stock.ticker)).scalar()
    
    # Get count of stocks with missing market cap
    missing_market_cap = session.query(func.count(Stock.ticker)).filter(
        (Stock.market_cap.is_(None)) | (Stock.market_cap == 0)
    ).scalar()
    
    # Define market cap ranges (in billions or millions)
    market_cap_ranges = [
        (0, 0.05),     # 0-50M
        (0.05, 0.1),   # 50M-100M
        (0.1, 0.2),    # 100M-200M
        (0.2, 0.5),    # 200M-500M
        (0.5, 2),      # 500M-2B
        (2, 10),       # 2B-10B
        (10, 50),      # 10B-50B
        (50, 100),     # 50B-100B
        (100, 500),    # 100B-500B
        (500, float('inf'))  # 500B+
    ]
    
    # Prepare the distribution query
    distribution = []
    for min_cap, max_cap in market_cap_ranges:
        # Convert to actual market cap value
        min_market_cap = min_cap * 1e9
        max_market_cap = max_cap * 1e9 if max_cap != float('inf') else float('inf')
        
        # Count stocks in this range (excluding NULL and 0 values)
        if max_cap == float('inf'):
            count = session.query(func.count(Stock.ticker)).filter(
                Stock.market_cap >= min_market_cap,
                Stock.market_cap.isnot(None),
                Stock.market_cap > 0
            ).scalar()
        else:
            count = session.query(func.count(Stock.ticker)).filter(
                Stock.market_cap >= min_market_cap, 
                Stock.market_cap < max_market_cap,
                Stock.market_cap.isnot(None),
                Stock.market_cap > 0
            ).scalar()
        
        # Format range label
        if max_cap < 1:  # Use M for millions
            if max_cap == float('inf'):
                range_label = f'${min_cap * 1000}M+'
            else:
                range_label = f'${min_cap * 1000}M-{max_cap * 1000}M'
        else:  # Use B for billions
            if max_cap == float('inf'):
                range_label = f'${min_cap}B+'
            else:
                range_label = f'${min_cap}B-{max_cap}B'
        
        distribution.append({
            'range': range_label,
            'count': count,
            'sort_value': max_cap  # Add sort value for proper ordering
        })
    
    # Add missing market cap entry if there are any
    if missing_market_cap > 0:
        distribution.append({
            'range': 'Market Cap N/A',
            'count': missing_market_cap,
            'sort_value': -1  # Sort this at the bottom
        })
    
    # Sort distribution by market cap value (highest to lowest)
    distribution.sort(key=lambda x: x['sort_value'], reverse=True)
    
    # Remove sort_value from final output
    for item in distribution:
        del item['sort_value']
    
    # Add total at the beginning
    final_distribution = [
        {
            'range': 'TOTAL',
            'count': total_stocks
        }
    ]
    final_distribution.extend(distribution)
    
    return jsonify(final_distribution)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000) 