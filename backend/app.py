"""Flask API for stocks database."""

from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, func, desc, text, or_
from sqlalchemy.orm import sessionmaker
from models import (
    Ticker, CompanyProfile, OHLC, Index, IndexComponents,
    RatiosTTM, AnalystEstimates, Earnings, SyncMetadata, StockMetrics, RsiIndices, HistoricalRSI,
    StockVolspikeGapper, MainView, StockNotes, StockPreference
)
import os
import json
import logging
from datetime import date

# Path to persist stock notes (survives database resets)
STOCK_NOTES_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'stock_notes.json')
# Path to persist stock preferences (survives database resets)
STOCK_PREFERENCES_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'stock_preferences.json')

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

# Global liquidity filters (applied to all scanner views)
GLOBAL_LIQUIDITY_FILTERS = {
    'avg_vol_10d_min': 50000,        # Minimum 10-day average volume
    'dollar_volume_min': 10000000,   # Minimum dollar volume ($10M)
    'price_min': 3,                  # Minimum stock price ($3)
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


def apply_global_liquidity_filters(query, metrics_model=StockMetrics):
    """Apply global liquidity filters (avg_vol_10d, dollar_volume, price) to a SQLAlchemy query.
    
    Args:
        query: SQLAlchemy query object
        metrics_model: The model to filter on (default: StockMetrics)
    
    Returns:
        Filtered query
    """
    avg_vol_min = GLOBAL_LIQUIDITY_FILTERS.get('avg_vol_10d_min')
    dollar_vol_min = GLOBAL_LIQUIDITY_FILTERS.get('dollar_volume_min')
    price_min = GLOBAL_LIQUIDITY_FILTERS.get('price_min')
    
    if avg_vol_min:
        query = query.filter(metrics_model.avg_vol_10d >= avg_vol_min)
    if dollar_vol_min:
        query = query.filter(metrics_model.dollar_volume >= dollar_vol_min)
    if price_min:
        query = query.filter(metrics_model.current_price >= price_min)
    
    return query

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
    
    # Get volspike/gapper data
    volspike_gapper = session.query(StockVolspikeGapper).filter(
        StockVolspikeGapper.ticker == ticker
    ).first()
    
    return {
        'ticker': metrics.ticker,
        'company_name': metrics.company_name,
        'country': metrics.country,
        'sector': metrics.sector,
        'industry': metrics.industry,
        'ipo_date': metrics.ipo_date.strftime('%Y-%m-%d') if metrics.ipo_date else None,
        'market_cap': metrics.market_cap,
        'current_price': round(metrics.current_price, 2) if metrics.current_price else None,
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
        'rsi': metrics.rsi,
        'rsi_mktcap': metrics.rsi_mktcap,
        'short_float': metrics.short_float,
        'short_ratio': metrics.short_ratio,
        'short_interest': metrics.short_interest,
        'low_float': metrics.low_float,
        'updated_at': metrics.updated_at.strftime('%Y-%m-%d %H:%M:%S') if metrics.updated_at else None,
        # Volspike/Gapper data
        'spike_day_count': volspike_gapper.spike_day_count if volspike_gapper else 0,
        'avg_volume_spike': volspike_gapper.avg_volume_spike if volspike_gapper else None,
        'volume_spike_days': volspike_gapper.volume_spike_days if volspike_gapper else None,
        'gapper_day_count': volspike_gapper.gapper_day_count if volspike_gapper else 0,
        'avg_return_gapper': volspike_gapper.avg_return_gapper if volspike_gapper else None,
        'gap_days': volspike_gapper.gap_days if volspike_gapper else None,
        'last_event_date': volspike_gapper.last_event_date.strftime('%Y-%m-%d') if volspike_gapper and volspike_gapper.last_event_date else None,
        'last_event_type': volspike_gapper.last_event_type if volspike_gapper else None,
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
            StockMetrics.fps,
            StockMetrics.ipo_date,
            StockMetrics.vol_vs_10d_avg,
            StockMetrics.atr20,
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

        # Apply global liquidity filters
        query = apply_global_liquidity_filters(query)

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
                'fps': round(stock.fps, 2) if stock.fps else None,
                'ipo_date': stock.ipo_date.strftime('%Y-%m-%d') if stock.ipo_date else None,
                'vol_vs_10d_avg': round(stock.vol_vs_10d_avg, 2) if stock.vol_vs_10d_avg else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
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
    """Get OHLC data for a specific ticker with DMAs and RSI MktCap from historical_rsi."""
    s = Session()
    try:
        ticker = ticker.upper()
        
        from datetime import timedelta
        end_date = get_latest_price_date(s)
        if not end_date:
            return jsonify({'error': 'No price data available'}), 404
        
        # Fetch 400 calendar days (enough for 1Y display with buffer)
        start_date = end_date - timedelta(days=400)
        
        # Get OHLC data with historical RSI and DMAs
        ohlc_data = s.query(
            OHLC.date,
            OHLC.open,
            OHLC.high,
            OHLC.low,
            OHLC.close,
            OHLC.volume,
            HistoricalRSI.rsi_mktcap,
            HistoricalRSI.dma_50,
            HistoricalRSI.dma_200
        ).outerjoin(
            HistoricalRSI,
            (OHLC.ticker == HistoricalRSI.ticker) & (OHLC.date == HistoricalRSI.date)
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
                'volume': row.volume,
                'dma_50': float(row.dma_50) if row.dma_50 else None,
                'dma_200': float(row.dma_200) if row.dma_200 else None,
                'rsi_mktcap': row.rsi_mktcap
            })
        
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Error getting OHLC data for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


# ============================================================================
# Earnings EPS Endpoint (for chart annotations)
# ============================================================================

@app.route('/api/earnings-eps/<ticker>')
def get_earnings_eps(ticker):
    """Get actual and estimated EPS data for a specific ticker over the last 365 days."""
    s = Session()
    try:
        ticker = ticker.upper()
        
        from datetime import timedelta
        end_date = get_latest_price_date(s)
        if not end_date:
            return jsonify({'error': 'No price data available'}), 404
        
        # Get earnings from the last 365 days
        start_date = end_date - timedelta(days=365)
        
        # Get earnings data with actual and estimated EPS
        earnings_data = s.query(
            Earnings.date,
            Earnings.eps_actual,
            Earnings.eps_estimated
        ).filter(
            Earnings.ticker == ticker,
            Earnings.date >= start_date,
            Earnings.date <= end_date
        ).order_by(Earnings.date.asc()).all()
        
        results = []
        for row in earnings_data:
            results.append({
                'time': row.date.strftime('%Y-%m-%d'),
                'eps_actual': round(row.eps_actual, 2) if row.eps_actual is not None else None,
                'eps_estimated': round(row.eps_estimated, 2) if row.eps_estimated is not None else None
            })
        
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Error getting earnings EPS data for {ticker}: {str(e)}")
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
            StockMetrics.fps,
            StockMetrics.ipo_date,
            StockMetrics.vol_vs_10d_avg,
            StockMetrics.atr20,
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

        # Apply global liquidity filters
        query = apply_global_liquidity_filters(query)

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
                'fps': round(stock.fps, 2) if stock.fps else None,
                'ipo_date': stock.ipo_date.strftime('%Y-%m-%d') if stock.ipo_date else None,
                'vol_vs_10d_avg': round(stock.vol_vs_10d_avg, 2) if stock.vol_vs_10d_avg else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
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
        
        # Build liquidity filter
        liquidity_filter = ""
        avg_vol_min = GLOBAL_LIQUIDITY_FILTERS.get('avg_vol_10d_min')
        dollar_vol_min = GLOBAL_LIQUIDITY_FILTERS.get('dollar_volume_min')
        price_min = GLOBAL_LIQUIDITY_FILTERS.get('price_min')
        if avg_vol_min:
            liquidity_filter += f" AND sm.avg_vol_10d >= {avg_vol_min}"
        if dollar_vol_min:
            liquidity_filter += f" AND sm.dollar_volume >= {dollar_vol_min}"
        if price_min:
            liquidity_filter += f" AND sm.current_price >= {price_min}"
        
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
            sm.fpe,
            sm.fps,
            sm.ipo_date,
            sm.vol_vs_10d_avg,
            sm.atr20,
            sm.updated_at
        FROM rsi_change rc
        JOIN stock_metrics sm ON rc.ticker = sm.ticker
        WHERE rc.rsi_mktcap_change IS NOT NULL
        {mcap_filter}
        {liquidity_filter}
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
                'fpe': round(row[14], 2) if row[14] else None,
                'fps': round(row[15], 2) if row[15] else None,
                'ipo_date': row[16].strftime('%Y-%m-%d') if row[16] else None,
                'vol_vs_10d_avg': round(row[17], 2) if row[17] else None,
                'atr20': round(row[18], 2) if row[18] else None,
                'updated_at': row[19].strftime('%Y-%m-%d %H:%M:%S') if row[19] else None
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
            StockMetrics.fps,
            StockMetrics.ipo_date,
            StockMetrics.vol_vs_10d_avg,
            StockMetrics.atr20,
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
        
        # Apply global liquidity filters
        query = apply_global_liquidity_filters(query)
        
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
                'fps': round(stock.fps, 2) if stock.fps else None,
                'ipo_date': stock.ipo_date.strftime('%Y-%m-%d') if stock.ipo_date else None,
                'vol_vs_10d_avg': round(stock.vol_vs_10d_avg, 2) if stock.vol_vs_10d_avg else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
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


# ============================================================================
# Top Performance Endpoints (Union of top stocks by 1D, 5D, 20D returns)
# ============================================================================

def get_top_performance_stocks(session, market_cap_category=None):
    """
    Get union of top 30 stocks by dr_1, dr_5, and dr_20 from stock_metrics.
    Returns combined list with all columns for consistency with other pages.
    """
    try:
        # Base query for fetching all needed columns
        def build_query(return_column, limit=30):
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
                StockMetrics.dr_60,
                StockMetrics.dr_120,
                StockMetrics.pe,
                StockMetrics.fpe,
                StockMetrics.fps,
                StockMetrics.ipo_date,
                StockMetrics.vol_vs_10d_avg,
                StockMetrics.atr20,
                StockMetrics.volume,
                StockMetrics.dollar_volume,
                StockMetrics.updated_at
            ).filter(
                getattr(StockMetrics, return_column).isnot(None),
                StockMetrics.market_cap.isnot(None)
            )
            
            # Apply market cap filter
            if market_cap_category:
                category = MARKET_CAP_CATEGORIES.get(market_cap_category)
                if category:
                    query = query.filter(StockMetrics.market_cap >= category['min'])
                    if category['max'] is not None:
                        query = query.filter(StockMetrics.market_cap < category['max'])
            
            # Apply global liquidity filters
            query = apply_global_liquidity_filters(query)
            
            # Order by the specified return column descending
            query = query.order_by(desc(getattr(StockMetrics, return_column))).limit(limit)
            return query
        
        # Get top 30 from each return period
        top_1d = build_query('dr_1').all()
        top_5d = build_query('dr_5').all()
        top_20d = build_query('dr_20').all()
        
        # Combine and deduplicate by ticker
        seen_tickers = set()
        combined_stocks = []
        
        for stock_list in [top_1d, top_5d, top_20d]:
            for stock in stock_list:
                if stock.ticker not in seen_tickers:
                    seen_tickers.add(stock.ticker)
                    combined_stocks.append(stock)
        
        # Format results
        results = []
        for stock in combined_stocks:
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
                'dr_60': round(stock.dr_60, 2) if stock.dr_60 else None,
                'dr_120': round(stock.dr_120, 2) if stock.dr_120 else None,
                'pe': round(stock.pe, 2) if stock.pe else None,
                'fpe': round(stock.fpe, 2) if stock.fpe else None,
                'fps': round(stock.fps, 2) if stock.fps else None,
                'ipo_date': stock.ipo_date.strftime('%Y-%m-%d') if stock.ipo_date else None,
                'vol_vs_10d_avg': round(stock.vol_vs_10d_avg, 2) if stock.vol_vs_10d_avg else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                'volume': int(stock.volume) if stock.volume else None,
                'dollar_volume': round(stock.dollar_volume, 2) if stock.dollar_volume else None,
                'updated_at': stock.updated_at.strftime('%Y-%m-%d %H:%M:%S') if stock.updated_at else None
            })
        
        return results
    
    except Exception as e:
        logger.error(f"Error getting top performance stocks: {str(e)}")
        return []


@app.route('/api/TopPerformance-All')
def get_top_performance_all():
    s = Session()
    try:
        return jsonify(get_top_performance_stocks(s, market_cap_category=None))
    finally:
        s.close()

@app.route('/api/TopPerformance-MicroCap')
def get_top_performance_micro():
    s = Session()
    try:
        return jsonify(get_top_performance_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/TopPerformance-SmallCap')
def get_top_performance_small():
    s = Session()
    try:
        return jsonify(get_top_performance_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/TopPerformance-MidCap')
def get_top_performance_mid():
    s = Session()
    try:
        return jsonify(get_top_performance_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/TopPerformance-LargeCap')
def get_top_performance_large():
    s = Session()
    try:
        return jsonify(get_top_performance_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/TopPerformance-MegaCap')
def get_top_performance_mega():
    s = Session()
    try:
        return jsonify(get_top_performance_stocks(s, market_cap_category='mega'))
    finally:
        s.close()


# ============================================================================
# Volume Spike & Gapper Endpoints
# ============================================================================

def get_volspike_gapper_stocks(session, market_cap_category=None):
    """
    Get stocks with volume spike and/or gapper activity in the last 365 days.
    Joins with stock_metrics for enriched data.
    Sorted by last_event_date descending (most recent events first).
    """
    try:
        query = session.query(
            StockVolspikeGapper.ticker,
            StockVolspikeGapper.spike_day_count,
            StockVolspikeGapper.avg_volume_spike,
            StockVolspikeGapper.volume_spike_days,
            StockVolspikeGapper.gapper_day_count,
            StockVolspikeGapper.avg_return_gapper,
            StockVolspikeGapper.gap_days,
            StockVolspikeGapper.last_event_date,
            StockVolspikeGapper.last_event_type,
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
            StockMetrics.fpe,
            StockMetrics.fps,
            StockMetrics.ipo_date,
            StockMetrics.atr20,
            StockMetrics.volume,
            StockMetrics.dollar_volume,
            StockMetrics.vol_vs_10d_avg,
            StockMetrics.updated_at
        ).join(
            StockMetrics, StockVolspikeGapper.ticker == StockMetrics.ticker
        ).filter(
            # Only include stocks that have at least one spike day or one gap day
            # Use COALESCE to handle NULL values (treat NULL as 0)
            or_(
                func.coalesce(StockVolspikeGapper.spike_day_count, 0) > 0,
                func.coalesce(StockVolspikeGapper.gapper_day_count, 0) > 0
            )
        )

        # Apply market cap filter
        if market_cap_category:
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                query = query.filter(StockMetrics.market_cap >= category['min'])
                if category['max'] is not None:
                    query = query.filter(StockMetrics.market_cap < category['max'])

        # Apply global liquidity filters
        query = apply_global_liquidity_filters(query)

        # Order by last_event_date descending (most recent events first)
        query = query.order_by(
            desc(StockVolspikeGapper.last_event_date)
        )

        stocks = query.all()

        results = []
        for stock in stocks:
            # Parse the date strings back to arrays (filter out empty strings)
            spike_dates = [d for d in (stock.volume_spike_days or '').split(',') if d.strip()]
            gap_dates = [d for d in (stock.gap_days or '').split(',') if d.strip()]
            
            results.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'current_price': round(stock.current_price, 2) if stock.current_price else None,
                'spike_day_count': stock.spike_day_count or 0,
                'avg_volume_spike': float(stock.avg_volume_spike) if stock.avg_volume_spike else None,
                'volume_spike_days': spike_dates,
                'gapper_day_count': stock.gapper_day_count or 0,
                'avg_return_gapper': float(stock.avg_return_gapper) if stock.avg_return_gapper else None,
                'gap_days': gap_dates,
                'last_event_date': stock.last_event_date.strftime('%Y-%m-%d') if stock.last_event_date else None,
                'last_event_type': stock.last_event_type,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'fpe': round(stock.fpe, 2) if stock.fpe else None,
                'fps': round(stock.fps, 2) if stock.fps else None,
                'ipo_date': stock.ipo_date.strftime('%Y-%m-%d') if stock.ipo_date else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                'volume': int(stock.volume) if stock.volume else None,
                'dollar_volume': round(stock.dollar_volume, 2) if stock.dollar_volume else None,
                'vol_vs_10d_avg': float(stock.vol_vs_10d_avg) if stock.vol_vs_10d_avg else None,
                'updated_at': stock.updated_at.strftime('%Y-%m-%d %H:%M:%S') if stock.updated_at else None
            })

        return results

    except Exception as e:
        logger.error(f"Error getting volspike gapper stocks for {market_cap_category}: {str(e)}")
        return []


@app.route('/api/VolspikeGapper-All')
def get_volspike_gapper_all():
    s = Session()
    try:
        return jsonify(get_volspike_gapper_stocks(s, market_cap_category=None))
    finally:
        s.close()

@app.route('/api/VolspikeGapper-MicroCap')
def get_volspike_gapper_micro():
    s = Session()
    try:
        return jsonify(get_volspike_gapper_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/VolspikeGapper-SmallCap')
def get_volspike_gapper_small():
    s = Session()
    try:
        return jsonify(get_volspike_gapper_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/VolspikeGapper-MidCap')
def get_volspike_gapper_mid():
    s = Session()
    try:
        return jsonify(get_volspike_gapper_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/VolspikeGapper-LargeCap')
def get_volspike_gapper_large():
    s = Session()
    try:
        return jsonify(get_volspike_gapper_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/VolspikeGapper-MegaCap')
def get_volspike_gapper_mega():
    s = Session()
    try:
        return jsonify(get_volspike_gapper_stocks(s, market_cap_category='mega'))
    finally:
        s.close()


# ============================================================================
# Main View Endpoints
# ============================================================================

def get_main_view_stocks(session, market_cap_category=None):
    """
    Get stocks from main_view table with optional market cap filter.
    Returns combined metrics, volspike/gapper data, and computed tags.
    """
    try:
        query = session.query(MainView)

        # Apply market cap filter
        if market_cap_category:
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                query = query.filter(MainView.market_cap >= category['min'])
                if category['max'] is not None:
                    query = query.filter(MainView.market_cap < category['max'])

        # Order by market cap descending
        query = query.order_by(desc(MainView.market_cap))

        stocks = query.all()

        results = []
        for stock in stocks:
            # Parse the date strings back to arrays (filter out empty strings)
            spike_dates = [d for d in (stock.volume_spike_days or '').split(',') if d.strip()]
            gap_dates = [d for d in (stock.gap_days or '').split(',') if d.strip()]
            
            results.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'country': stock.country,
                'sector': stock.sector,
                'industry': stock.industry,
                'ipo_date': stock.ipo_date.strftime('%Y-%m-%d') if stock.ipo_date else None,
                'market_cap': stock.market_cap,
                'current_price': round(stock.current_price, 2) if stock.current_price else None,
                'range_52_week': stock.range_52_week,
                'volume': int(stock.volume) if stock.volume else None,
                'dollar_volume': round(stock.dollar_volume, 2) if stock.dollar_volume else None,
                'avg_vol_10d': float(stock.avg_vol_10d) if stock.avg_vol_10d else None,
                'vol_vs_10d_avg': float(stock.vol_vs_10d_avg) if stock.vol_vs_10d_avg else None,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'dr_60': round(stock.dr_60, 2) if stock.dr_60 else None,
                'dr_120': round(stock.dr_120, 2) if stock.dr_120 else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                'pe': round(stock.pe, 2) if stock.pe else None,
                'ps_ttm': round(stock.ps_ttm, 2) if stock.ps_ttm else None,
                'fpe': round(stock.fpe, 2) if stock.fpe else None,
                'fps': round(stock.fps, 2) if stock.fps else None,
                'rev_growth_t_minus_1': round(stock.rev_growth_t_minus_1, 2) if stock.rev_growth_t_minus_1 else None,
                'rev_growth_t': round(stock.rev_growth_t, 2) if stock.rev_growth_t else None,
                'rev_growth_t_plus_1': round(stock.rev_growth_t_plus_1, 2) if stock.rev_growth_t_plus_1 else None,
                'rev_growth_t_plus_2': round(stock.rev_growth_t_plus_2, 2) if stock.rev_growth_t_plus_2 else None,
                'avg_rev_growth': round(stock.avg_rev_growth, 2) if stock.avg_rev_growth else None,
                'eps_growth_t_minus_1': round(stock.eps_growth_t_minus_1, 2) if stock.eps_growth_t_minus_1 else None,
                'eps_growth_t': round(stock.eps_growth_t, 2) if stock.eps_growth_t else None,
                'eps_growth_t_plus_1': round(stock.eps_growth_t_plus_1, 2) if stock.eps_growth_t_plus_1 else None,
                'eps_growth_t_plus_2': round(stock.eps_growth_t_plus_2, 2) if stock.eps_growth_t_plus_2 else None,
                'avg_eps_growth': round(stock.avg_eps_growth, 2) if stock.avg_eps_growth else None,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'short_float': round(stock.short_float, 2) if stock.short_float else None,
                'short_ratio': round(stock.short_ratio, 2) if stock.short_ratio else None,
                'short_interest': round(stock.short_interest, 2) if stock.short_interest else None,
                'low_float': stock.low_float,
                'spike_day_count': stock.spike_day_count or 0,
                'avg_volume_spike': round(stock.avg_volume_spike, 2) if stock.avg_volume_spike else None,
                'volume_spike_days': spike_dates,
                'gapper_day_count': stock.gapper_day_count or 0,
                'avg_return_gapper': round(stock.avg_return_gapper, 4) if stock.avg_return_gapper else None,
                'gap_days': gap_dates,
                'last_event_date': stock.last_event_date.strftime('%Y-%m-%d') if stock.last_event_date else None,
                'last_event_type': stock.last_event_type,
                'tags': stock.tags,
                'updated_at': stock.updated_at.strftime('%Y-%m-%d %H:%M:%S') if stock.updated_at else None
            })

        return results

    except Exception as e:
        logger.error(f"Error getting main_view stocks for {market_cap_category}: {str(e)}")
        return []


@app.route('/api/MainView-All')
def get_main_view_all():
    s = Session()
    try:
        return jsonify(get_main_view_stocks(s, market_cap_category=None))
    finally:
        s.close()

@app.route('/api/MainView-MicroCap')
def get_main_view_micro():
    s = Session()
    try:
        return jsonify(get_main_view_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/MainView-SmallCap')
def get_main_view_small():
    s = Session()
    try:
        return jsonify(get_main_view_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/MainView-MidCap')
def get_main_view_mid():
    s = Session()
    try:
        return jsonify(get_main_view_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/MainView-LargeCap')
def get_main_view_large():
    s = Session()
    try:
        return jsonify(get_main_view_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/MainView-MegaCap')
def get_main_view_mega():
    s = Session()
    try:
        return jsonify(get_main_view_stocks(s, market_cap_category='mega'))
    finally:
        s.close()


# ============================================================
# Stock Notes API Endpoints
# ============================================================

def export_all_notes_to_file():
    """Export all stock notes to JSON file for persistence across database resets."""
    s = Session()
    try:
        notes = s.query(StockNotes).filter(StockNotes.notes.isnot(None), StockNotes.notes != '').all()
        
        data = {}
        for note in notes:
            data[note.ticker] = {
                'notes': note.notes,
                'created_at': note.created_at.isoformat() if note.created_at else None,
                'updated_at': note.updated_at.isoformat() if note.updated_at else None
            }
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(STOCK_NOTES_FILE), exist_ok=True)
        
        with open(STOCK_NOTES_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Exported {len(data)} stock notes to {STOCK_NOTES_FILE}")
    except Exception as e:
        logger.error(f"Error exporting stock notes: {str(e)}")
    finally:
        s.close()


@app.route('/api/stock-notes/<ticker>', methods=['GET'])
def get_stock_notes(ticker):
    """Get notes for a specific stock."""
    s = Session()
    try:
        ticker = ticker.upper()
        note = s.query(StockNotes).filter(StockNotes.ticker == ticker).first()
        if note:
            return jsonify({
                'ticker': note.ticker,
                'notes': note.notes,
                'updated_at': note.updated_at.isoformat() if note.updated_at else None
            })
        return jsonify({
            'ticker': ticker,
            'notes': None,
            'updated_at': None
        })
    finally:
        s.close()


@app.route('/api/stock-notes/<ticker>', methods=['PUT'])
def update_stock_notes(ticker):
    """Create or update notes for a specific stock."""
    s = Session()
    try:
        ticker = ticker.upper()
        data = request.get_json()
        
        if not data or 'notes' not in data:
            return jsonify({'error': 'notes field is required'}), 400
        
        notes_content = data['notes']
        
        # Check if notes already exist for this ticker
        existing = s.query(StockNotes).filter(StockNotes.ticker == ticker).first()
        
        if existing:
            # Update existing notes
            existing.notes = notes_content if notes_content else None
            s.commit()
            
            # Export all notes to file for persistence
            export_all_notes_to_file()
            
            return jsonify({
                'ticker': existing.ticker,
                'notes': existing.notes,
                'updated_at': existing.updated_at.isoformat() if existing.updated_at else None,
                'message': 'Notes updated successfully'
            })
        else:
            # Create new notes entry
            new_note = StockNotes(ticker=ticker, notes=notes_content if notes_content else None)
            s.add(new_note)
            s.commit()
            
            # Export all notes to file for persistence
            export_all_notes_to_file()
            
            return jsonify({
                'ticker': new_note.ticker,
                'notes': new_note.notes,
                'updated_at': new_note.updated_at.isoformat() if new_note.updated_at else None,
                'message': 'Notes created successfully'
            }), 201
    except Exception as e:
        s.rollback()
        logger.error(f"Error updating stock notes for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/stock-notes/batch', methods=['POST'])
def get_stock_notes_batch():
    """Get notes for multiple tickers at once (for screener views)."""
    s = Session()
    try:
        data = request.get_json()
        
        if not data or 'tickers' not in data:
            return jsonify({'error': 'tickers field is required'}), 400
        
        tickers = [t.upper() for t in data['tickers']]
        
        notes = s.query(StockNotes).filter(StockNotes.ticker.in_(tickers)).all()
        
        result = {}
        for note in notes:
            if note.notes:  # Only include non-empty notes
                result[note.ticker] = {
                    'notes': note.notes,
                    'updated_at': note.updated_at.isoformat() if note.updated_at else None
                }
        
        return jsonify(result)
    finally:
        s.close()


# ============================================================
# Stock Preferences API Endpoints
# ============================================================

def export_all_preferences_to_file():
    """Export all stock preferences to JSON file for persistence across database resets."""
    s = Session()
    try:
        prefs = s.query(StockPreference).filter(StockPreference.preference.isnot(None)).all()
        
        data = {}
        for pref in prefs:
            data[pref.ticker] = {
                'preference': pref.preference,
                'created_at': pref.created_at.isoformat() if pref.created_at else None,
                'updated_at': pref.updated_at.isoformat() if pref.updated_at else None
            }
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(STOCK_PREFERENCES_FILE), exist_ok=True)
        
        with open(STOCK_PREFERENCES_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Exported {len(data)} stock preferences to {STOCK_PREFERENCES_FILE}")
    except Exception as e:
        logger.error(f"Error exporting stock preferences: {str(e)}")
    finally:
        s.close()


@app.route('/api/stock-preferences/<ticker>', methods=['GET'])
def get_stock_preference(ticker):
    """Get preference for a specific stock."""
    s = Session()
    try:
        ticker = ticker.upper()
        pref = s.query(StockPreference).filter(StockPreference.ticker == ticker).first()
        if pref:
            return jsonify({
                'ticker': pref.ticker,
                'preference': pref.preference,
                'updated_at': pref.updated_at.isoformat() if pref.updated_at else None
            })
        return jsonify({
            'ticker': ticker,
            'preference': None,
            'updated_at': None
        })
    finally:
        s.close()


@app.route('/api/stock-preferences/<ticker>', methods=['PUT'])
def update_stock_preference(ticker):
    """Create or update preference for a specific stock."""
    s = Session()
    try:
        ticker = ticker.upper()
        data = request.get_json()
        
        if not data or 'preference' not in data:
            return jsonify({'error': 'preference field is required'}), 400
        
        preference_value = data['preference']
        
        # Validate preference value
        if preference_value is not None and preference_value not in ['favorite', 'dislike']:
            return jsonify({'error': 'preference must be "favorite", "dislike", or null'}), 400
        
        # Check if preference already exists for this ticker
        existing = s.query(StockPreference).filter(StockPreference.ticker == ticker).first()
        
        if existing:
            # Update existing preference
            existing.preference = preference_value
            s.commit()
            
            # Export all preferences to file for persistence
            export_all_preferences_to_file()
            
            return jsonify({
                'ticker': existing.ticker,
                'preference': existing.preference,
                'updated_at': existing.updated_at.isoformat() if existing.updated_at else None,
                'message': 'Preference updated successfully'
            })
        else:
            # Create new preference entry
            new_pref = StockPreference(ticker=ticker, preference=preference_value)
            s.add(new_pref)
            s.commit()
            
            # Export all preferences to file for persistence
            export_all_preferences_to_file()
            
            return jsonify({
                'ticker': new_pref.ticker,
                'preference': new_pref.preference,
                'updated_at': new_pref.updated_at.isoformat() if new_pref.updated_at else None,
                'message': 'Preference created successfully'
            }), 201
    except Exception as e:
        s.rollback()
        logger.error(f"Error updating stock preference for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/stock-preferences/batch', methods=['POST'])
def get_stock_preferences_batch():
    """Get preferences for multiple tickers at once."""
    s = Session()
    try:
        data = request.get_json()
        
        if not data or 'tickers' not in data:
            return jsonify({'error': 'tickers field is required'}), 400
        
        tickers = [t.upper() for t in data['tickers']]
        
        prefs = s.query(StockPreference).filter(StockPreference.ticker.in_(tickers)).all()
        
        result = {}
        for pref in prefs:
            if pref.preference:  # Only include non-null preferences
                result[pref.ticker] = {
                    'preference': pref.preference,
                    'updated_at': pref.updated_at.isoformat() if pref.updated_at else None
                }
        
        return jsonify(result)
    finally:
        s.close()


@app.route('/api/stock-preferences/list/<preference_type>')
def list_stock_preferences(preference_type):
    """List all stocks with a specific preference (favorites or dislikes)."""
    s = Session()
    try:
        if preference_type not in ['favorite', 'dislike']:
            return jsonify({'error': 'preference_type must be "favorite" or "dislike"'}), 400
        
        prefs = s.query(StockPreference).filter(StockPreference.preference == preference_type).all()
        
        tickers = [pref.ticker for pref in prefs]
        
        return jsonify({'tickers': tickers, 'count': len(tickers)})
    finally:
        s.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
