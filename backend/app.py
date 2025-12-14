"""Flask API for stocks database."""

from flask import Flask, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, func, desc
from sqlalchemy.orm import sessionmaker
from models import (
    Ticker, CompanyProfile, OHLC, Index, IndexComponents,
    RatiosTTM, AnalystEstimates, Earnings, SyncMetadata
)
import os
import logging
from datetime import date

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@db:5432/stocks_db')
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Minimum % move thresholds per period
RETURN_THRESHOLDS = {1: 5, 5: 10, 20: 15, 60: 20, 120: 30}

# Market cap categories
MARKET_CAP_CATEGORIES = {
    'micro': {'min': 0, 'max': 200000000},
    'small': {'min': 200000000, 'max': 2000000000},
    'mid': {'min': 2000000000, 'max': 20000000000},
    'large': {'min': 20000000000, 'max': 100000000000},
    'mega': {'min': 100000000000, 'max': None},
}

# Global sector / industry exclusions for all scanner pages
GLOBAL_EXCLUDE = {
    'sector': set(),
    'industry': {'Biotechnology'}
}


def get_latest_price_date(session):
    """Get the most recent price date available."""
    latest = session.query(func.max(OHLC.date)).scalar()
    return latest


def get_trading_date_n_days_ago(session, reference_date, n_trading_days):
    """Get the date that is N trading days before the reference date."""
    trading_dates = session.query(OHLC.date).filter(
        OHLC.date <= reference_date
    ).distinct().order_by(desc(OHLC.date)).limit(n_trading_days + 1).all()
    
    if len(trading_dates) > n_trading_days:
        return trading_dates[n_trading_days][0]
    return None


def apply_global_exclude_filters(query):
    """Apply the GLOBAL_EXCLUDE filters to a SQLAlchemy query."""
    sectors = GLOBAL_EXCLUDE.get('sector', set())
    industries = GLOBAL_EXCLUDE.get('industry', set())

    if sectors:
        query = query.filter(~Ticker.sector.in_(list(sectors)))
    if industries:
        query = query.filter(~Ticker.industry.in_(list(industries)))
    return query


def get_momentum_stocks(session, return_days, market_cap_category=None):
    """Get stocks with strong momentum over specified period."""
    try:
        end_date = get_latest_price_date(session)
        if not end_date:
            return []

        start_date = get_trading_date_n_days_ago(session, end_date, return_days)
        if not start_date:
            logger.warning(f"Insufficient data to calculate {return_days} trading days ago")
            return []

        # Subquery to get OHLCV data from the latest price record
        latest_price_subquery = session.query(
            OHLC.ticker,
            OHLC.volume,
            OHLC.open.label('open_price'),
            OHLC.high.label('high_price'),
            OHLC.low.label('low_price'),
            OHLC.close.label('latest_close')
        ).filter(
            OHLC.date == end_date
        ).subquery()

        base_query = session.query(
            Ticker.ticker,
            Ticker.company_name,
            Ticker.sector,
            Ticker.industry,
            Ticker.market_cap,
            Ticker.price,
            Ticker.country,
            func.min(OHLC.close).label('start_price'),
            latest_price_subquery.c.latest_close.label('end_price'),
            ((latest_price_subquery.c.latest_close - func.min(OHLC.close)) / func.min(OHLC.close) * 100).label('return_pct'),
            latest_price_subquery.c.volume.label('volume'),
            latest_price_subquery.c.open_price.label('open_price'),
            latest_price_subquery.c.high_price.label('high_price'),
            latest_price_subquery.c.low_price.label('low_price')
        ).join(OHLC, Ticker.ticker == OHLC.ticker
        ).outerjoin(latest_price_subquery, Ticker.ticker == latest_price_subquery.c.ticker)

        # Apply date filtering
        base_query = base_query.filter(OHLC.date >= start_date, OHLC.date <= end_date)

        # Filter out invalid prices
        base_query = base_query.filter(OHLC.close > 0)

        # Apply market cap filter
        if market_cap_category:
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                base_query = base_query.filter(Ticker.market_cap >= category['min'])
                if category['max'] is not None:
                    base_query = base_query.filter(Ticker.market_cap < category['max'])

        # Apply global exclusions
        base_query = apply_global_exclude_filters(base_query)

        # Group by stock
        base_query = base_query.group_by(
            Ticker.ticker, Ticker.company_name, Ticker.sector,
            Ticker.industry, Ticker.market_cap, Ticker.price, Ticker.country,
            latest_price_subquery.c.volume, latest_price_subquery.c.open_price,
            latest_price_subquery.c.high_price, latest_price_subquery.c.low_price,
            latest_price_subquery.c.latest_close
        )

        # Apply return threshold filter
        threshold = RETURN_THRESHOLDS.get(return_days, 10)
        base_query = base_query.having(
            ((latest_price_subquery.c.latest_close - func.min(OHLC.close)) / func.min(OHLC.close) * 100) >= threshold
        )

        # Order by return percentage and get results
        stocks = base_query.order_by(desc('return_pct')).limit(100).all()

        # Format results
        results = []
        for stock in stocks:
            volume = stock.volume if stock.volume else 0
            dollar_volume = (stock.end_price * volume) if volume else 0

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

        return results

    except Exception as e:
        logger.error(f"Error getting momentum stocks for {return_days}d: {str(e)}")
        return []


def get_gapper_stocks(session, market_cap_category=None):
    """Get stocks that gapped up significantly using trading days."""
    latest_date = get_latest_price_date(session)
    if not latest_date:
        return []

    previous_date = get_trading_date_n_days_ago(session, latest_date, 1)
    if not previous_date:
        logger.warning("Insufficient data to find previous trading day for gapper stocks")
        return []

    prev_close_subquery = session.query(
        OHLC.ticker,
        OHLC.close.label('prev_close')
    ).filter(
        OHLC.date == previous_date,
        OHLC.close.isnot(None)
    ).subquery()

    today_open_subquery = session.query(
        OHLC.ticker,
        OHLC.open.label('today_open')
    ).filter(
        OHLC.date == latest_date,
        OHLC.open.isnot(None)
    ).subquery()

    query = session.query(
        Ticker.ticker,
        Ticker.company_name,
        Ticker.sector,
        Ticker.industry,
        Ticker.market_cap,
        Ticker.price,
        prev_close_subquery.c.prev_close,
        today_open_subquery.c.today_open
    ).join(
        prev_close_subquery, Ticker.ticker == prev_close_subquery.c.ticker
    ).join(
        today_open_subquery, Ticker.ticker == today_open_subquery.c.ticker
    )

    if market_cap_category:
        category = MARKET_CAP_CATEGORIES.get(market_cap_category)
        if category:
            query = query.filter(Ticker.market_cap >= category['min'])
            if category['max'] is not None:
                query = query.filter(Ticker.market_cap < category['max'])

    query = apply_global_exclude_filters(query)

    gap_stocks = query.all()

    results = []
    for stock in gap_stocks:
        if stock.prev_close and stock.today_open and stock.prev_close > 0:
            gap_pct = ((stock.today_open - stock.prev_close) / stock.prev_close) * 100
            if gap_pct >= 5:
                price_change = stock.today_open - stock.prev_close
                current_price = stock.today_open if stock.today_open else stock.price

                results.append({
                    'ticker': stock.ticker,
                    'company_name': stock.company_name,
                    'sector': stock.sector,
                    'industry': stock.industry,
                    'market_cap': stock.market_cap,
                    'price': current_price,
                    'current_price': round(current_price, 2) if current_price else None,
                    'start_price': round(stock.prev_close, 2),
                    'end_price': round(stock.today_open, 2),
                    'price_change': round(price_change, 2),
                    'price_change_pct': f"{'+' if gap_pct >= 0 else ''}{round(gap_pct, 2)}%",
                    'return_pct': round(gap_pct, 2),
                    'prev_close': round(stock.prev_close, 2),
                    'today_open': round(stock.today_open, 2),
                    'gap_pct': round(gap_pct, 2),
                    'volume': None,
                    'dollar_volume': None
                })

    results.sort(key=lambda x: x['gap_pct'], reverse=True)
    return results[:100]


def get_volume_spike_stocks(session, market_cap_category=None):
    """Get stocks with significant volume spikes using trading days."""
    latest_date = get_latest_price_date(session)
    if not latest_date:
        return []

    avg_volume_start = get_trading_date_n_days_ago(session, latest_date, 20)
    if not avg_volume_start:
        logger.warning("Insufficient data to calculate 20-day average volume for volume spike stocks")
        return []

    avg_volume_query = session.query(
        OHLC.ticker,
        func.avg(OHLC.volume).label('avg_volume')
    ).filter(
        OHLC.date >= avg_volume_start,
        OHLC.volume.isnot(None)
    ).group_by(OHLC.ticker).subquery()

    today_data = session.query(
        OHLC.ticker,
        OHLC.volume.label('today_volume'),
        OHLC.close.label('today_close'),
        OHLC.open.label('today_open')
    ).filter(
        OHLC.date == latest_date
    ).subquery()

    query = session.query(
        Ticker.ticker,
        Ticker.company_name,
        Ticker.sector,
        Ticker.industry,
        Ticker.market_cap,
        Ticker.price,
        today_data.c.today_volume,
        today_data.c.today_close,
        today_data.c.today_open,
        avg_volume_query.c.avg_volume
    ).join(today_data, Ticker.ticker == today_data.c.ticker)\
     .join(avg_volume_query, Ticker.ticker == avg_volume_query.c.ticker)

    if market_cap_category:
        category = MARKET_CAP_CATEGORIES.get(market_cap_category)
        if category:
            query = query.filter(Ticker.market_cap >= category['min'])
            if category['max'] is not None:
                query = query.filter(Ticker.market_cap < category['max'])

    query = apply_global_exclude_filters(query)

    volume_stocks = query.all()

    results = []
    for stock in volume_stocks:
        if stock.avg_volume and stock.avg_volume > 0 and stock.today_volume:
            volume_ratio = stock.today_volume / stock.avg_volume
            if volume_ratio >= 2:
                price_change = stock.today_close - stock.today_open if (stock.today_close and stock.today_open) else 0
                price_change_pct = ((price_change / stock.today_open) * 100) if (stock.today_open and stock.today_open > 0) else 0
                current_price = stock.today_close if stock.today_close else stock.price
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

    results.sort(key=lambda x: x['volume_ratio'], reverse=True)
    return results[:100]


@app.route('/api/health')
def health():
    try:
        s = Session()
        count = s.query(Ticker).count()
        s.close()
        return jsonify({'status': 'ok', 'tickers': count})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/stats')
def stats():
    s = Session()
    result = {
        'tickers': s.query(Ticker).count(),
        'profiles': s.query(CompanyProfile).count(),
        'ratios_ttm': s.query(RatiosTTM).count(),
        'analyst_estimates': s.query(AnalystEstimates).count(),
        'earnings': s.query(Earnings).count(),
        'ohlc': s.query(OHLC).count(),
        'indices': s.query(Index).count(),
        'index_components': s.query(IndexComponents).count()
    }
    s.close()
    return jsonify(result)


@app.route('/api/latest_date')
def get_latest_date():
    s = Session()
    try:
        latest_date = get_latest_price_date(s)
        if not latest_date:
            return jsonify({'error': 'No price data available'}), 404

        latest_update = s.query(SyncMetadata).order_by(desc(SyncMetadata.last_synced_at)).first()

        response = {
            'latest_date': latest_date.strftime('%Y-%m-%d'),
            'formatted_date': latest_date.strftime('%B %d, %Y')
        }

        if latest_update and latest_update.last_synced_at:
            response['last_update'] = latest_update.last_synced_at.strftime('%Y-%m-%d %H:%M:%S')
            response['last_update_formatted'] = latest_update.last_synced_at.strftime('%b %d, %Y %I:%M %p')

        return jsonify(response)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/stock/<ticker>')
def get_stock_details(ticker):
    """Get detailed information for a specific stock."""
    s = Session()
    try:
        ticker = ticker.upper()

        # Get basic ticker info
        stock = s.query(Ticker).filter(Ticker.ticker == ticker).first()
        if not stock:
            return jsonify({'error': 'Stock not found'}), 404

        # Get company profile (detailed info)
        profile = s.query(CompanyProfile).filter(CompanyProfile.ticker == ticker).first()

        # Get ratios
        ratios = s.query(RatiosTTM).filter(RatiosTTM.ticker == ticker).first()

        # Get latest price date
        latest_date = get_latest_price_date(s)
        if not latest_date:
            return jsonify({'error': 'No price data available'}), 500

        # Get latest price data
        latest_price = s.query(OHLC).filter(
            OHLC.ticker == ticker,
            OHLC.date == latest_date
        ).first()

        # Get previous trading day price for price change calculation
        previous_date = get_trading_date_n_days_ago(s, latest_date, 1)
        previous_price = None
        if previous_date:
            previous_price = s.query(OHLC).filter(
                OHLC.ticker == ticker,
                OHLC.date == previous_date
            ).first()

        # Calculate 52-week high/low (252 trading days)
        week_52_date = get_trading_date_n_days_ago(s, latest_date, 252)
        week_52_high = None
        week_52_low = None
        if week_52_date:
            week_52_data = s.query(
                func.max(OHLC.high).label('high_52w'),
                func.min(OHLC.low).label('low_52w')
            ).filter(
                OHLC.ticker == ticker,
                OHLC.date >= week_52_date,
                OHLC.date <= latest_date,
                OHLC.close.isnot(None),
                OHLC.close > 0
            ).first()
            if week_52_data:
                week_52_high = week_52_data.high_52w
                week_52_low = week_52_data.low_52w

        # Calculate average volume (last 20 trading days)
        avg_volume_date = get_trading_date_n_days_ago(s, latest_date, 20)
        avg_volume = None
        if avg_volume_date:
            avg_volume = s.query(
                func.avg(OHLC.volume).label('avg_volume')
            ).filter(
                OHLC.ticker == ticker,
                OHLC.date >= avg_volume_date,
                OHLC.date <= latest_date,
                OHLC.volume.isnot(None)
            ).scalar()

        # Calculate returns for different trading day periods
        returns = {}
        for period, days in [('1d', 1), ('5d', 5), ('20d', 20), ('60d', 60), ('120d', 120)]:
            period_date = get_trading_date_n_days_ago(s, latest_date, days)
            if period_date:
                period_price = s.query(OHLC.close).filter(
                    OHLC.ticker == ticker,
                    OHLC.date == period_date,
                    OHLC.close.isnot(None),
                    OHLC.close > 0
                ).scalar()
                
                if period_price and latest_price and latest_price.close:
                    returns[f'return_{period}'] = round(
                        ((latest_price.close - period_price) / period_price) * 100, 2
                    )
                else:
                    returns[f'return_{period}'] = None
            else:
                returns[f'return_{period}'] = None

        # Calculate price changes
        close_price = latest_price.close if latest_price else stock.price
        price_change = None
        price_change_pct = None
        
        if previous_price and previous_price.close and latest_price and latest_price.close:
            price_change = round(latest_price.close - previous_price.close, 2)
            price_change_pct = round((price_change / previous_price.close) * 100, 2)

        # Calculate volume change multiplier
        volume_change_multiplier = None
        if latest_price and latest_price.volume and avg_volume and avg_volume > 0:
            volume_change_multiplier = round(latest_price.volume / avg_volume, 2)

        # Get upcoming earnings (next earnings date)
        upcoming_earnings = s.query(Earnings).filter(
            Earnings.ticker == ticker,
            Earnings.date >= date.today()
        ).order_by(Earnings.date).first()

        # Get most recent earnings
        recent_earnings = s.query(Earnings).filter(
            Earnings.ticker == ticker,
            Earnings.date < date.today()
        ).order_by(desc(Earnings.date)).first()

        # Get latest analyst estimates
        analyst_est = s.query(AnalystEstimates).filter(
            AnalystEstimates.ticker == ticker
        ).order_by(desc(AnalystEstimates.date)).first()

        # Get index memberships
        index_memberships = s.query(IndexComponents.index_symbol).filter(
            IndexComponents.ticker == ticker
        ).all()
        indices = [idx[0] for idx in index_memberships]

        # Build response
        stock_data = {
            # Basic info (from Ticker)
            'ticker': stock.ticker,
            'company_name': stock.company_name,
            'sector': stock.sector,
            'industry': stock.industry,
            'country': stock.country,
            'exchange': stock.exchange,
            'exchange_short_name': stock.exchange_short_name,
            'market_cap': stock.market_cap,
            'beta': stock.beta,
            'last_annual_dividend': stock.last_annual_dividend,
            'is_etf': stock.is_etf,
            'is_fund': stock.is_fund,
            'is_actively_trading': stock.is_actively_trading,

            # Price data
            'price': close_price,
            'open': latest_price.open if latest_price else None,
            'high': latest_price.high if latest_price else None,
            'low': latest_price.low if latest_price else None,
            'close': latest_price.close if latest_price else None,
            'volume': latest_price.volume if latest_price else stock.volume,
            'price_date': latest_date.strftime('%Y-%m-%d') if latest_date else None,
            'price_change': price_change,
            'price_change_pct': price_change_pct,
            
            # Volume metrics
            'avg_volume': int(avg_volume) if avg_volume else None,
            'volume_change_multiplier': volume_change_multiplier,
            
            # 52-week range
            'week_52_high': week_52_high,
            'week_52_low': week_52_low,
            
            # Returns
            'return_1d': returns.get('return_1d'),
            'return_5d': returns.get('return_5d'),
            'return_20d': returns.get('return_20d'),
            'return_60d': returns.get('return_60d'),
            'return_120d': returns.get('return_120d'),

            # Index memberships
            'indices': indices,
        }

        # Add profile data if available
        if profile:
            stock_data.update({
                'cik': profile.cik,
                'isin': profile.isin,
                'cusip': profile.cusip,
                'description': profile.description,
                'ceo': profile.ceo,
                'website': profile.website,
                'phone': profile.phone,
                'address': profile.address,
                'city': profile.city,
                'state': profile.state,
                'zip': profile.zip,
                'full_time_employees': profile.full_time_employees,
                'ipo_date': profile.ipo_date.strftime('%Y-%m-%d') if profile.ipo_date else None,
                'image': profile.image,
                'currency': profile.currency,
                'range_52_week': profile.range_52_week,
                'is_adr': profile.is_adr,
            })

        # Add ratios if available
        if ratios:
            stock_data['ratios'] = {
                # Valuation
                'pe_ratio': ratios.pe_ratio,
                'peg_ratio': ratios.peg_ratio,
                'price_to_book': ratios.price_to_book,
                'price_to_sales': ratios.price_to_sales,
                'price_to_free_cash_flow': ratios.price_to_free_cash_flow,
                # Profitability
                'gross_profit_margin': ratios.gross_profit_margin,
                'operating_profit_margin': ratios.operating_profit_margin,
                'net_profit_margin': ratios.net_profit_margin,
                # Liquidity
                'current_ratio': ratios.current_ratio,
                'quick_ratio': ratios.quick_ratio,
                'cash_ratio': ratios.cash_ratio,
                # Leverage
                'debt_to_equity': ratios.debt_to_equity,
                'debt_to_assets': ratios.debt_to_assets,
                # Returns
                'return_on_assets': ratios.return_on_assets,
                'return_on_equity': ratios.return_on_equity,
                # Efficiency
                'asset_turnover': ratios.asset_turnover,
                'inventory_turnover': ratios.inventory_turnover,
                'receivables_turnover': ratios.receivables_turnover,
            }

        # Add earnings data if available
        if upcoming_earnings:
            stock_data['upcoming_earnings'] = {
                'date': upcoming_earnings.date.strftime('%Y-%m-%d'),
                'eps_estimated': upcoming_earnings.eps_estimated,
                'revenue_estimated': upcoming_earnings.revenue_estimated,
            }

        if recent_earnings:
            stock_data['recent_earnings'] = {
                'date': recent_earnings.date.strftime('%Y-%m-%d'),
                'eps_actual': recent_earnings.eps_actual,
                'eps_estimated': recent_earnings.eps_estimated,
                'eps_surprise': round(recent_earnings.eps_actual - recent_earnings.eps_estimated, 4) if recent_earnings.eps_actual and recent_earnings.eps_estimated else None,
                'revenue_actual': recent_earnings.revenue_actual,
                'revenue_estimated': recent_earnings.revenue_estimated,
            }

        # Add analyst estimates if available
        if analyst_est:
            stock_data['analyst_estimates'] = {
                'date': analyst_est.date.strftime('%Y-%m-%d') if analyst_est.date else None,
                'revenue_avg': analyst_est.revenue_avg,
                'revenue_low': analyst_est.revenue_low,
                'revenue_high': analyst_est.revenue_high,
                'eps_avg': analyst_est.eps_avg,
                'eps_low': analyst_est.eps_low,
                'eps_high': analyst_est.eps_high,
                'num_analysts_revenue': analyst_est.num_analysts_revenue,
                'num_analysts_eps': analyst_est.num_analysts_eps,
            }

        return jsonify(stock_data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


# ============================================================================
# Return/Momentum Endpoints
# ============================================================================

# 1D Returns
@app.route('/api/Return1D-MicroCap')
def get_return_1d_micro():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 1, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/Return1D-SmallCap')
def get_return_1d_small():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 1, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/Return1D-MidCap')
def get_return_1d_mid():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 1, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/Return1D-LargeCap')
def get_return_1d_large():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 1, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/Return1D-MegaCap')
def get_return_1d_mega():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 1, market_cap_category='mega'))
    finally:
        s.close()

# 5D Returns
@app.route('/api/Return5D-MicroCap')
def get_return_5d_micro():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 5, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/Return5D-SmallCap')
def get_return_5d_small():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 5, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/Return5D-MidCap')
def get_return_5d_mid():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 5, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/Return5D-LargeCap')
def get_return_5d_large():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 5, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/Return5D-MegaCap')
def get_return_5d_mega():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 5, market_cap_category='mega'))
    finally:
        s.close()

# 20D Returns
@app.route('/api/Return20D-MicroCap')
def get_return_20d_micro():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 20, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/Return20D-SmallCap')
def get_return_20d_small():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 20, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/Return20D-MidCap')
def get_return_20d_mid():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 20, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/Return20D-LargeCap')
def get_return_20d_large():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 20, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/Return20D-MegaCap')
def get_return_20d_mega():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 20, market_cap_category='mega'))
    finally:
        s.close()

# 60D Returns
@app.route('/api/Return60D-MicroCap')
def get_return_60d_micro():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 60, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/Return60D-SmallCap')
def get_return_60d_small():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 60, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/Return60D-MidCap')
def get_return_60d_mid():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 60, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/Return60D-LargeCap')
def get_return_60d_large():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 60, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/Return60D-MegaCap')
def get_return_60d_mega():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 60, market_cap_category='mega'))
    finally:
        s.close()

# 120D Returns
@app.route('/api/Return120D-MicroCap')
def get_return_120d_micro():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 120, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/Return120D-SmallCap')
def get_return_120d_small():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 120, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/Return120D-MidCap')
def get_return_120d_mid():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 120, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/Return120D-LargeCap')
def get_return_120d_large():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 120, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/Return120D-MegaCap')
def get_return_120d_mega():
    s = Session()
    try:
        return jsonify(get_momentum_stocks(s, 120, market_cap_category='mega'))
    finally:
        s.close()


# Gapper Endpoints
@app.route('/api/Gapper-MicroCap')
def get_gapper_micro():
    s = Session()
    try:
        return jsonify(get_gapper_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/Gapper-SmallCap')
def get_gapper_small():
    s = Session()
    try:
        return jsonify(get_gapper_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/Gapper-MidCap')
def get_gapper_mid():
    s = Session()
    try:
        return jsonify(get_gapper_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/Gapper-LargeCap')
def get_gapper_large():
    s = Session()
    try:
        return jsonify(get_gapper_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/Gapper-MegaCap')
def get_gapper_mega():
    s = Session()
    try:
        return jsonify(get_gapper_stocks(s, market_cap_category='mega'))
    finally:
        s.close()


# Volume Spike Endpoints
@app.route('/api/Volume-MicroCap')
def get_volume_micro():
    s = Session()
    try:
        return jsonify(get_volume_spike_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/Volume-SmallCap')
def get_volume_small():
    s = Session()
    try:
        return jsonify(get_volume_spike_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/Volume-MidCap')
def get_volume_mid():
    s = Session()
    try:
        return jsonify(get_volume_spike_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/Volume-LargeCap')
def get_volume_large():
    s = Session()
    try:
        return jsonify(get_volume_spike_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/Volume-MegaCap')
def get_volume_mega():
    s = Session()
    try:
        return jsonify(get_volume_spike_stocks(s, market_cap_category='mega'))
    finally:
        s.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
