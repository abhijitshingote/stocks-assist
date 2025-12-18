"""Flask API for stocks database."""

from flask import Flask, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, func, desc, text
from sqlalchemy.orm import sessionmaker
from models import (
    Ticker, CompanyProfile, OHLC, Index, IndexComponents,
    RatiosTTM, AnalystEstimates, Earnings, SyncMetadata, StockMetrics, RsiIndices
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


def get_stock_metrics_data(session, ticker):
    """Get all stock_metrics fields for a ticker. Returns dict or None if not found."""
    ticker = ticker.upper()
    metrics = session.query(StockMetrics).filter(StockMetrics.ticker == ticker).first()
    
    if not metrics:
        return None
    
    return {
        'ticker': metrics.ticker,
        'company_name': metrics.company_name,
        'country': metrics.country,
        'sector': metrics.sector,
        'industry': metrics.industry,
        'ipo_date': metrics.ipo_date.strftime('%Y-%m-%d') if metrics.ipo_date else None,
        'market_cap': metrics.market_cap,
        'current_price': metrics.current_price,
        'range_52_week': metrics.range_52_week,
        'volume': metrics.volume,
        'dollar_volume': metrics.dollar_volume,
        'vol_vs_10d_avg': metrics.vol_vs_10d_avg,
        'dr_1': metrics.dr_1,
        'dr_5': metrics.dr_5,
        'dr_20': metrics.dr_20,
        'dr_60': metrics.dr_60,
        'dr_120': metrics.dr_120,
        'atr20': metrics.atr20,
        'pe': metrics.pe,
        'ps_ttm': metrics.ps_ttm,
        'fpe': metrics.fpe,
        'fps': metrics.fps,
        'eps_growth': metrics.eps_growth,
        'revenue_growth': metrics.revenue_growth,
        'rsi': metrics.rsi,
        'short_float': metrics.short_float,
        'short_ratio': metrics.short_ratio,
        'short_interest': metrics.short_interest,
        'low_float': metrics.low_float,
        'updated_at': metrics.updated_at.strftime('%Y-%m-%d %H:%M:%S') if metrics.updated_at else None
    }


@app.route('/api/stock/<ticker>')
def get_stock_details(ticker):
    """API endpoint to get stock metrics for a ticker."""
    s = Session()
    try:
        data = get_stock_metrics_data(s, ticker)
        if data is None:
            return jsonify({'error': 'Stock not found'}), 404
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting stock details for {ticker}: {str(e)}")
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


# ============================================================================
# RSI Endpoints (by Market Cap)
# ============================================================================

def get_rsi_stocks(session, market_cap_category):
    """Get stocks with RSI data for a given market cap category, sorted by RSI descending."""
    try:
        query = session.query(
            StockMetrics.ticker,
            StockMetrics.company_name,
            StockMetrics.sector,
            StockMetrics.industry,
            StockMetrics.market_cap,
            StockMetrics.current_price,
            StockMetrics.rsi,
            StockMetrics.dr_1,
            StockMetrics.dr_5,
            StockMetrics.dr_20,
            StockMetrics.pe,
            StockMetrics.fpe,
            StockMetrics.eps_growth,
            StockMetrics.revenue_growth,
            StockMetrics.updated_at
        ).filter(
            StockMetrics.rsi.isnot(None),
            StockMetrics.market_cap.isnot(None)
        )

        # Apply market cap filter
        category = MARKET_CAP_CATEGORIES.get(market_cap_category)
        if category:
            query = query.filter(StockMetrics.market_cap >= category['min'])
            if category['max'] is not None:
                query = query.filter(StockMetrics.market_cap < category['max'])

        # Order by RSI descending (highest RSI first)
        stocks = query.order_by(desc(StockMetrics.rsi)).limit(100).all()

        results = []
        for stock in stocks:
            results.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'current_price': round(stock.current_price, 2) if stock.current_price else None,
                'rsi': stock.rsi,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'pe': round(stock.pe, 2) if stock.pe else None,
                'fpe': round(stock.fpe, 2) if stock.fpe else None,
                'eps_growth': round(stock.eps_growth, 2) if stock.eps_growth else None,
                'revenue_growth': round(stock.revenue_growth, 2) if stock.revenue_growth else None,
                'updated_at': stock.updated_at.strftime('%Y-%m-%d %H:%M:%S') if stock.updated_at else None
            })

        return results

    except Exception as e:
        logger.error(f"Error getting RSI stocks for {market_cap_category}: {str(e)}")
        return []


@app.route('/api/RSI-All')
def get_rsi_all():
    s = Session()
    try:
        return jsonify(get_rsi_stocks(s, market_cap_category=None))
    finally:
        s.close()

@app.route('/api/RSI-MicroCap')
def get_rsi_micro():
    s = Session()
    try:
        return jsonify(get_rsi_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/RSI-SmallCap')
def get_rsi_small():
    s = Session()
    try:
        return jsonify(get_rsi_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/RSI-MidCap')
def get_rsi_mid():
    s = Session()
    try:
        return jsonify(get_rsi_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/RSI-LargeCap')
def get_rsi_large():
    s = Session()
    try:
        return jsonify(get_rsi_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/RSI-MegaCap')
def get_rsi_mega():
    s = Session()
    try:
        return jsonify(get_rsi_stocks(s, market_cap_category='mega'))
    finally:
        s.close()


# ============================================================================
# OHLC Data Endpoint (for charts)
# ============================================================================

@app.route('/api/ohlc/<ticker>')
def get_ohlc_data(ticker):
    """Get OHLC data for a specific ticker (last 365 days)."""
    s = Session()
    try:
        ticker = ticker.upper()
        
        # Get last 365 calendar days of data
        from datetime import timedelta
        end_date = get_latest_price_date(s)
        if not end_date:
            return jsonify({'error': 'No price data available'}), 404
        
        start_date = end_date - timedelta(days=365)
        
        ohlc_data = s.query(
            OHLC.date,
            OHLC.open,
            OHLC.high,
            OHLC.low,
            OHLC.close,
            OHLC.volume
        ).filter(
            OHLC.ticker == ticker,
            OHLC.date >= start_date,
            OHLC.date <= end_date
        ).order_by(OHLC.date.asc()).all()
        
        if not ohlc_data:
            return jsonify({'error': 'No OHLC data found for ticker'}), 404
        
        results = []
        for row in ohlc_data:
            results.append({
                'time': row.date.strftime('%Y-%m-%d'),
                'open': round(row.open, 2) if row.open else None,
                'high': round(row.high, 2) if row.high else None,
                'low': round(row.low, 2) if row.low else None,
                'close': round(row.close, 2) if row.close else None,
                'volume': row.volume
            })
        
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Error getting OHLC data for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


# ============================================================================
# Sector RSI Endpoint
# ============================================================================

@app.route('/api/sector-rsi')
def get_sector_rsi():
    """
    Calculate market-cap weighted RSI for each sector.
    Takes top 10 stocks by market cap in each sector, then calculates
    weighted average RSI using market cap as weights.
    """
    s = Session()
    try:
        # Get all stocks with RSI and market cap data, grouped by sector
        stocks = s.query(
            StockMetrics.ticker,
            StockMetrics.company_name,
            StockMetrics.sector,
            StockMetrics.market_cap,
            StockMetrics.rsi,
            StockMetrics.dr_1,
            StockMetrics.dr_5,
            StockMetrics.dr_20
        ).filter(
            StockMetrics.rsi.isnot(None),
            StockMetrics.market_cap.isnot(None),
            StockMetrics.sector.isnot(None),
            StockMetrics.sector != ''
        ).order_by(
            StockMetrics.sector,
            desc(StockMetrics.market_cap)
        ).all()
        
        # Group stocks by sector and take top 10 by market cap
        from collections import defaultdict
        sector_stocks = defaultdict(list)
        
        for stock in stocks:
            if len(sector_stocks[stock.sector]) < 10:
                sector_stocks[stock.sector].append({
                    'ticker': stock.ticker,
                    'company_name': stock.company_name,
                    'market_cap': stock.market_cap,
                    'rsi': stock.rsi,
                    'dr_1': stock.dr_1,
                    'dr_5': stock.dr_5,
                    'dr_20': stock.dr_20
                })
        
        # Calculate weighted average RSI for each sector
        results = []
        for sector, stocks_list in sector_stocks.items():
            if not stocks_list:
                continue
            
            total_market_cap = sum(s['market_cap'] for s in stocks_list)
            if total_market_cap == 0:
                continue
            
            # Weighted average RSI
            weighted_rsi = sum(
                s['rsi'] * s['market_cap'] for s in stocks_list
            ) / total_market_cap
            
            # Weighted average returns
            weighted_dr_1 = sum(
                (s['dr_1'] or 0) * s['market_cap'] for s in stocks_list
            ) / total_market_cap
            
            weighted_dr_5 = sum(
                (s['dr_5'] or 0) * s['market_cap'] for s in stocks_list
            ) / total_market_cap
            
            weighted_dr_20 = sum(
                (s['dr_20'] or 0) * s['market_cap'] for s in stocks_list
            ) / total_market_cap
            
            results.append({
                'sector': sector,
                'weighted_rsi': round(weighted_rsi, 1),
                'total_market_cap': total_market_cap,
                'stock_count': len(stocks_list),
                'dr_1': round(weighted_dr_1, 2),
                'dr_5': round(weighted_dr_5, 2),
                'dr_20': round(weighted_dr_20, 2),
                'top_stocks': stocks_list
            })
        
        # Sort by weighted RSI descending
        results.sort(key=lambda x: x['weighted_rsi'], reverse=True)
        
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Error calculating sector RSI: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


# ============================================================================
# RSI Market Cap Endpoints (RSI percentile within market cap bucket)
# ============================================================================

def get_rsi_mktcap_stocks(session, market_cap_category):
    """Get stocks with rsi_mktcap data for a given market cap category, sorted by rsi_mktcap descending."""
    try:
        query = session.query(
            StockMetrics.ticker,
            StockMetrics.company_name,
            StockMetrics.sector,
            StockMetrics.industry,
            StockMetrics.market_cap,
            StockMetrics.current_price,
            StockMetrics.rsi,
            StockMetrics.rsi_mktcap,
            StockMetrics.dr_1,
            StockMetrics.dr_5,
            StockMetrics.dr_20,
            StockMetrics.pe,
            StockMetrics.fpe,
            StockMetrics.eps_growth,
            StockMetrics.revenue_growth,
            StockMetrics.updated_at
        ).filter(
            StockMetrics.rsi_mktcap.isnot(None),
            StockMetrics.market_cap.isnot(None)
        )

        # Apply market cap filter
        category = MARKET_CAP_CATEGORIES.get(market_cap_category)
        if category:
            query = query.filter(StockMetrics.market_cap >= category['min'])
            if category['max'] is not None:
                query = query.filter(StockMetrics.market_cap < category['max'])

        # Order by rsi_mktcap descending (highest RSI first)
        stocks = query.order_by(desc(StockMetrics.rsi_mktcap)).limit(100).all()

        results = []
        for stock in stocks:
            results.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'current_price': round(stock.current_price, 2) if stock.current_price else None,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'pe': round(stock.pe, 2) if stock.pe else None,
                'fpe': round(stock.fpe, 2) if stock.fpe else None,
                'eps_growth': round(stock.eps_growth, 2) if stock.eps_growth else None,
                'revenue_growth': round(stock.revenue_growth, 2) if stock.revenue_growth else None,
                'updated_at': stock.updated_at.strftime('%Y-%m-%d %H:%M:%S') if stock.updated_at else None
            })

        return results

    except Exception as e:
        logger.error(f"Error getting RSI MktCap stocks for {market_cap_category}: {str(e)}")
        return []


@app.route('/api/RSIMktCap-All')
def get_rsi_mktcap_all():
    s = Session()
    try:
        return jsonify(get_rsi_mktcap_stocks(s, market_cap_category=None))
    finally:
        s.close()

@app.route('/api/RSIMktCap-MicroCap')
def get_rsi_mktcap_micro():
    s = Session()
    try:
        return jsonify(get_rsi_mktcap_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/RSIMktCap-SmallCap')
def get_rsi_mktcap_small():
    s = Session()
    try:
        return jsonify(get_rsi_mktcap_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/RSIMktCap-MidCap')
def get_rsi_mktcap_mid():
    s = Session()
    try:
        return jsonify(get_rsi_mktcap_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/RSIMktCap-LargeCap')
def get_rsi_mktcap_large():
    s = Session()
    try:
        return jsonify(get_rsi_mktcap_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/RSIMktCap-MegaCap')
def get_rsi_mktcap_mega():
    s = Session()
    try:
        return jsonify(get_rsi_mktcap_stocks(s, market_cap_category='mega'))
    finally:
        s.close()


# ============================================================================
# RSI Momentum Endpoints (RSI MktCap change over 5 trading days)
# ============================================================================

def get_rsi_momentum_stocks(connection, market_cap_category=None):
    """
    Get stocks with highest RSI_mktcap change over 5 trading days.
    Joins with stock_metrics to get enriched data.
    """
    try:
        # Build market cap filter
        mcap_filter = ""
        if market_cap_category:
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                mcap_filter = f"AND sm.market_cap >= {category['min']}"
                if category['max'] is not None:
                    mcap_filter += f" AND sm.market_cap < {category['max']}"
        
        query = f"""
        WITH trading_dates AS (
            SELECT DISTINCT date
            FROM historical_rsi
            ORDER BY date DESC
            LIMIT 6
        ),
        latest_date AS (
            SELECT MAX(date) as max_date FROM trading_dates
        ),
        date_5d_ago AS (
            SELECT MIN(date) as min_date FROM trading_dates
        ),
        rsi_change AS (
            SELECT 
                h_today.ticker,
                h_today.rsi_mktcap as rsi_mktcap_today,
                h_today.rsi_global as rsi_global_today,
                h_5d.rsi_mktcap as rsi_mktcap_5d_ago,
                h_5d.rsi_global as rsi_global_5d_ago,
                (h_today.rsi_mktcap - h_5d.rsi_mktcap) as rsi_mktcap_change,
                (h_today.rsi_global - h_5d.rsi_global) as rsi_global_change
            FROM historical_rsi h_today
            CROSS JOIN latest_date ld
            CROSS JOIN date_5d_ago d5
            LEFT JOIN historical_rsi h_5d 
                ON h_today.ticker = h_5d.ticker 
                AND h_5d.date = d5.min_date
            WHERE h_today.date = ld.max_date
              AND h_5d.rsi_mktcap IS NOT NULL
        )
        SELECT 
            rc.ticker,
            sm.company_name,
            sm.sector,
            sm.industry,
            sm.market_cap,
            sm.current_price,
            rc.rsi_mktcap_today,
            rc.rsi_global_today,
            rc.rsi_mktcap_5d_ago,
            rc.rsi_mktcap_change,
            rc.rsi_global_change,
            sm.dr_1,
            sm.dr_5,
            sm.dr_20,
            sm.eps_growth,
            sm.revenue_growth,
            sm.updated_at
        FROM rsi_change rc
        JOIN stock_metrics sm ON rc.ticker = sm.ticker
        WHERE rc.rsi_mktcap_change IS NOT NULL
        {mcap_filter}
        ORDER BY rc.rsi_mktcap_change DESC
        LIMIT 100
        """
        
        result = connection.execute(text(query))
        rows = result.fetchall()
        
        results = []
        for row in rows:
            results.append({
                'ticker': row[0],
                'company_name': row[1],
                'sector': row[2],
                'industry': row[3],
                'market_cap': row[4],
                'current_price': round(row[5], 2) if row[5] else None,
                'rsi_mktcap': row[6],
                'rsi': row[7],
                'rsi_mktcap_5d_ago': row[8],
                'rsi_mktcap_change': row[9],
                'rsi_global_change': row[10],
                'dr_1': round(row[11], 2) if row[11] else None,
                'dr_5': round(row[12], 2) if row[12] else None,
                'dr_20': round(row[13], 2) if row[13] else None,
                'eps_growth': round(row[14], 2) if row[14] else None,
                'revenue_growth': round(row[15], 2) if row[15] else None,
                'updated_at': row[16].strftime('%Y-%m-%d %H:%M:%S') if row[16] else None
            })
        
        return results
    
    except Exception as e:
        logger.error(f"Error getting RSI momentum stocks: {str(e)}")
        return []


@app.route('/api/RSIMomentum-All')
def get_rsi_momentum_all():
    s = Session()
    try:
        with s.get_bind().connect() as conn:
            return jsonify(get_rsi_momentum_stocks(conn, market_cap_category=None))
    finally:
        s.close()

@app.route('/api/RSIMomentum-MicroCap')
def get_rsi_momentum_micro():
    s = Session()
    try:
        with s.get_bind().connect() as conn:
            return jsonify(get_rsi_momentum_stocks(conn, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/RSIMomentum-SmallCap')
def get_rsi_momentum_small():
    s = Session()
    try:
        with s.get_bind().connect() as conn:
            return jsonify(get_rsi_momentum_stocks(conn, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/RSIMomentum-MidCap')
def get_rsi_momentum_mid():
    s = Session()
    try:
        with s.get_bind().connect() as conn:
            return jsonify(get_rsi_momentum_stocks(conn, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/RSIMomentum-LargeCap')
def get_rsi_momentum_large():
    s = Session()
    try:
        with s.get_bind().connect() as conn:
            return jsonify(get_rsi_momentum_stocks(conn, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/RSIMomentum-MegaCap')
def get_rsi_momentum_mega():
    s = Session()
    try:
        with s.get_bind().connect() as conn:
            return jsonify(get_rsi_momentum_stocks(conn, market_cap_category='mega'))
    finally:
        s.close()


# ============================================================================
# RSI Index Endpoints (RSI within index universes: SPX, NDX, DJI)
# ============================================================================

def get_rsi_index_stocks(session, index_type, market_cap_category=None):
    """
    Get stocks with RSI data for a given index universe.
    Joins rsi_indices with stock_metrics for full stock data.
    Optional market_cap_category filter for view filtering (doesn't affect RSI calculation).
    """
    try:
        # Map index type to column names
        index_map = {
            'spx': {'flag': 'is_spx', 'rsi_col': 'rsi_spx', 'name': 'S&P 500'},
            'ndx': {'flag': 'is_ndx', 'rsi_col': 'rsi_ndx', 'name': 'NASDAQ 100'},
            'dji': {'flag': 'is_dji', 'rsi_col': 'rsi_dji', 'name': 'Dow Jones'}
        }
        
        if index_type not in index_map:
            return []
        
        idx = index_map[index_type]
        
        query = session.query(
            RsiIndices.ticker,
            getattr(RsiIndices, idx['rsi_col']).label('rsi_index'),
            StockMetrics.company_name,
            StockMetrics.sector,
            StockMetrics.industry,
            StockMetrics.market_cap,
            StockMetrics.current_price,
            StockMetrics.rsi,
            StockMetrics.dr_1,
            StockMetrics.dr_5,
            StockMetrics.dr_20,
            StockMetrics.pe,
            StockMetrics.fpe,
            StockMetrics.eps_growth,
            StockMetrics.revenue_growth,
            StockMetrics.updated_at
        ).join(
            StockMetrics, RsiIndices.ticker == StockMetrics.ticker
        ).filter(
            getattr(RsiIndices, idx['flag']) == True,
            getattr(RsiIndices, idx['rsi_col']).isnot(None)
        )
        
        # Apply optional market cap filter (view filter only, doesn't affect RSI calculation)
        if market_cap_category:
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                query = query.filter(StockMetrics.market_cap >= category['min'])
                if category['max'] is not None:
                    query = query.filter(StockMetrics.market_cap < category['max'])
        
        query = query.order_by(desc(getattr(RsiIndices, idx['rsi_col'])))
        stocks = query.all()

        results = []
        for stock in stocks:
            results.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'current_price': round(stock.current_price, 2) if stock.current_price else None,
                'rsi_index': stock.rsi_index,
                'rsi': stock.rsi,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'pe': round(stock.pe, 2) if stock.pe else None,
                'fpe': round(stock.fpe, 2) if stock.fpe else None,
                'eps_growth': round(stock.eps_growth, 2) if stock.eps_growth else None,
                'revenue_growth': round(stock.revenue_growth, 2) if stock.revenue_growth else None,
                'updated_at': stock.updated_at.strftime('%Y-%m-%d %H:%M:%S') if stock.updated_at else None
            })

        return results

    except Exception as e:
        logger.error(f"Error getting RSI index stocks for {index_type}: {str(e)}")
        return []


@app.route('/api/RSI-SPX')
@app.route('/api/RSI-SPX/<market_cap>')
def get_rsi_spx(market_cap=None):
    s = Session()
    try:
        return jsonify(get_rsi_index_stocks(s, index_type='spx', market_cap_category=market_cap))
    finally:
        s.close()

@app.route('/api/RSI-NDX')
@app.route('/api/RSI-NDX/<market_cap>')
def get_rsi_ndx(market_cap=None):
    s = Session()
    try:
        return jsonify(get_rsi_index_stocks(s, index_type='ndx', market_cap_category=market_cap))
    finally:
        s.close()

@app.route('/api/RSI-DJI')
@app.route('/api/RSI-DJI/<market_cap>')
def get_rsi_dji(market_cap=None):
    s = Session()
    try:
        return jsonify(get_rsi_index_stocks(s, index_type='dji', market_cap_category=market_cap))
    finally:
        s.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
