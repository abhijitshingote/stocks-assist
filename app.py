from flask import Flask, render_template, jsonify, request
from models import Stock, Price, init_db
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
        
        # Calculate the start date for the return period
        start_date = latest_date - timedelta(days=return_days)
        ten_days_ago = latest_date - timedelta(days=10)
        
        logger.info(f"Calculating {return_days}D returns from {start_date} to {latest_date}")
        
        # Get stocks with data for both start and end dates
        start_prices = session.query(
            Price.ticker,
            Price.close_price.label('start_price'),
            Price.date.label('start_date')
        ).filter(
            Price.date >= start_date,
            Price.date <= latest_date
        ).order_by(
            Price.ticker,
            Price.date.asc()
        ).distinct(Price.ticker).subquery()
        
        end_prices = session.query(
            Price.ticker,
            Price.close_price.label('end_price'),
            Price.date.label('end_date')
        ).filter(
            Price.date == latest_date
        ).subquery()
        
        # Calculate average prices and volumes over the last 10 days
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
            avg_stats.c.avg_10day_price >= 5,  # 10-day average price > $5
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