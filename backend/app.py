"""Flask API for stocks database."""

from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, func, desc, asc, text, or_
from sqlalchemy.orm import sessionmaker
from models import (
    Ticker, CompanyProfile, OHLC, Index, IndexComponents, IndexPrice,
    RatiosTTM, AnalystEstimates, Earnings, SyncMetadata, StockMetrics, RsiIndices, HistoricalRSI,
    StockVolspikeGapper, MainView, StockNotes, StockPreference, SharesFloat, AbiGeneralNotes, MarketBreadth,
    RsScreener
)
import os
import json
import logging
import time
import pytz
from datetime import date, datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Path to persist stock notes (survives database resets)
STOCK_NOTES_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'stock_notes.json')
# Path to persist stock preferences (survives database resets)
STOCK_PREFERENCES_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'stock_preferences.json')
# Path to persist abi general notes (survives database resets)
ABI_GENERAL_NOTES_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'abi_general_notes.json')
# Path to persist abi watchlist (survives database resets)
ABI_WATCHLIST_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'abi_watchlist.json')
# Path to persist explicit thumbs-down list (separate from watchlist).
# Watchlist `stars: 0` means "unrated"; dislikes are tracked here instead.
ABI_DISLIKES_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'abi_dislikes.json')
# Path to persist per-ticker Abi ticker notes (free-form). Decoupled from
# watchlist / dislikes membership. (Daily screener still consumes only notes
# for tickers on the watchlist - see daily_screener/config.py.)
ABI_TICKER_NOTES_FILE = os.path.join(
    os.path.dirname(__file__), '..', 'user_data', 'abi_ticker_notes.json'
)
# Daily screener feedback (per-date, per-ticker corrections from the user).
# Used by Stage 5 of the daily_screener pipeline to calibrate the judge.
DAILY_SCREENER_FEEDBACK_FILE = os.path.join(
    os.path.dirname(__file__), '..', 'user_data', 'daily_screener_feedback.json'
)

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


_ENG_MONTH_ABBR = (
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
)


def _fmt_est_edt_abbr(dt_eastern):
    """US Eastern: always label EST or EDT (never generic ET or locale zone names)."""
    return 'EDT' if dt_eastern.dst() else 'EST'


def _fmt_time_12h_eng(dt):
    hour12 = dt.hour % 12
    if hour12 == 0:
        hour12 = 12
    ampm = 'AM' if dt.hour < 12 else 'PM'
    return f'{hour12}:{dt.minute:02d} {ampm}'


def _fmt_month_day_eng(d):
    return f'{_ENG_MONTH_ABBR[d.month - 1]} {d.day}'


def _fmt_full_date_eng(d):
    return f'{_ENG_MONTH_ABBR[d.month - 1]} {d.day}, {d.year}'


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
            'formatted_date': _fmt_full_date_eng(latest_date),
        }

        if latest_update and latest_update.last_synced_at:
            raw = latest_update.last_synced_at
            # ETL scripts store naive UTC wall times in sync_metadata.last_synced_at.
            if raw.tzinfo is None:
                utc_dt = pytz.UTC.localize(raw)
            else:
                utc_dt = raw.astimezone(pytz.UTC)
            eastern = pytz.timezone('US/Eastern')
            et_dt = utc_dt.astimezone(eastern)
            abbr = _fmt_est_edt_abbr(et_dt)
            response['last_update'] = utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            response['last_update_formatted'] = (
                f'{_ENG_MONTH_ABBR[et_dt.month - 1]} {et_dt.day}, {et_dt.year} '
                f'{_fmt_time_12h_eng(et_dt)} {abbr}'
            )
            response['nav_data_thru_label'] = (
                f'{_fmt_month_day_eng(latest_date)}, {_fmt_time_12h_eng(et_dt)} {abbr}'
            )

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
    
    # Get shares float data
    shares_float = session.query(SharesFloat).filter(
        SharesFloat.ticker == ticker
    ).first()
    
    # Parse volspike/gapper date strings back to arrays
    spike_dates = [d for d in (volspike_gapper.volume_spike_days or '').split(',') if d.strip()] if volspike_gapper else []
    gap_dates = [d for d in (volspike_gapper.gap_days or '').split(',') if d.strip()] if volspike_gapper else []
    
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
        'avg_vol_10d': metrics.avg_vol_10d,
        'vol_vs_10d_avg': metrics.vol_vs_10d_avg,
        'dr_1': metrics.dr_1,
        'dr_5': metrics.dr_5,
        'dr_20': metrics.dr_20,
        'dr_60': metrics.dr_60,
        'dr_120': metrics.dr_120,
        'atr20': metrics.atr20,
        # P/E trajectory
        'pe_t_minus_1': round(metrics.pe_t_minus_1, 2) if metrics.pe_t_minus_1 else None,
        'pe_t': round(metrics.pe_t, 2) if metrics.pe_t else None,
        'pe_t_plus_1': round(metrics.pe_t_plus_1, 2) if metrics.pe_t_plus_1 else None,
        'pe_t_plus_2': round(metrics.pe_t_plus_2, 2) if metrics.pe_t_plus_2 else None,
        # P/S trajectory
        'ps_t_minus_1': round(metrics.ps_t_minus_1, 2) if metrics.ps_t_minus_1 else None,
        'ps_t': round(metrics.ps_t, 2) if metrics.ps_t else None,
        'ps_t_plus_1': round(metrics.ps_t_plus_1, 2) if metrics.ps_t_plus_1 else None,
        'ps_t_plus_2': round(metrics.ps_t_plus_2, 2) if metrics.ps_t_plus_2 else None,
        # Revenue growth fields
        'rev_growth_t_minus_1': round(metrics.rev_growth_t_minus_1, 2) if metrics.rev_growth_t_minus_1 else None,
        'rev_growth_t': round(metrics.rev_growth_t, 2) if metrics.rev_growth_t else None,
        'rev_growth_t_plus_1': round(metrics.rev_growth_t_plus_1, 2) if metrics.rev_growth_t_plus_1 else None,
        'rev_growth_t_plus_2': round(metrics.rev_growth_t_plus_2, 2) if metrics.rev_growth_t_plus_2 else None,
        # EPS growth fields
        'eps_growth_t_minus_1': round(metrics.eps_growth_t_minus_1, 2) if metrics.eps_growth_t_minus_1 else None,
        'eps_growth_t': round(metrics.eps_growth_t, 2) if metrics.eps_growth_t else None,
        'eps_growth_t_plus_1': round(metrics.eps_growth_t_plus_1, 2) if metrics.eps_growth_t_plus_1 else None,
        'eps_growth_t_plus_2': round(metrics.eps_growth_t_plus_2, 2) if metrics.eps_growth_t_plus_2 else None,
        'rsi': metrics.rsi,
        'rsi_mktcap': metrics.rsi_mktcap,
        'short_float': metrics.short_float,
        'short_ratio': metrics.short_ratio,
        'short_interest': metrics.short_interest,
        'low_float': metrics.low_float,
        # Float/Liquidity data
        'float_shares': shares_float.float_shares if shares_float else None,
        'outstanding_shares': shares_float.outstanding_shares if shares_float else None,
        'free_float': round(shares_float.free_float, 2) if shares_float and shares_float.free_float else None,
        'updated_at': metrics.updated_at.strftime('%Y-%m-%d %H:%M:%S') if metrics.updated_at else None,
        # Volspike/Gapper data
        'spike_day_count': volspike_gapper.spike_day_count if volspike_gapper else 0,
        'avg_volume_spike': volspike_gapper.avg_volume_spike if volspike_gapper else None,
        'volume_spike_days': spike_dates,
        'gapper_day_count': volspike_gapper.gapper_day_count if volspike_gapper else 0,
        'avg_return_gapper': volspike_gapper.avg_return_gapper if volspike_gapper else None,
        'gap_days': gap_dates,
        'last_event_date': volspike_gapper.last_event_date.strftime('%Y-%m-%d') if volspike_gapper and volspike_gapper.last_event_date else None,
        'last_event_type': volspike_gapper.last_event_type if volspike_gapper else None,
        'last_event_magnitude': float(volspike_gapper.last_event_magnitude) if volspike_gapper and volspike_gapper.last_event_magnitude else None,
        'last_event_return': float(volspike_gapper.last_event_return) if volspike_gapper and volspike_gapper.last_event_return else None,
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
            StockMetrics.pe_t,
            StockMetrics.pe_t_plus_1,
            StockMetrics.ps_t,
            StockMetrics.ps_t_plus_1,
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
                'pe_t': round(stock.pe_t, 2) if stock.pe_t else None,
                'pe_t_plus_1': round(stock.pe_t_plus_1, 2) if stock.pe_t_plus_1 else None,
                'ps_t': round(stock.ps_t, 2) if stock.ps_t else None,
                'ps_t_plus_1': round(stock.ps_t_plus_1, 2) if stock.ps_t_plus_1 else None,
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
        
        # Get OHLC data with historical RSI, DMAs, and EMAs
        ohlc_data = s.query(
            OHLC.date,
            OHLC.open,
            OHLC.high,
            OHLC.low,
            OHLC.close,
            OHLC.volume,
            HistoricalRSI.rsi_mktcap,
            HistoricalRSI.dma_50,
            HistoricalRSI.dma_200,
            HistoricalRSI.ema_10,
            HistoricalRSI.ema_20
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
                'ema_10': float(row.ema_10) if row.ema_10 else None,
                'ema_20': float(row.ema_20) if row.ema_20 else None,
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
# Volume Spike Events Endpoint (for chart annotations)
# ============================================================================

@app.route('/api/volspike-events/<ticker>')
def get_volspike_events(ticker):
    """Get volume spike and gap dates for a specific ticker (for chart markers)."""
    s = Session()
    try:
        ticker = ticker.upper()
        
        # Get volspike/gapper data from the table
        volspike_data = s.query(StockVolspikeGapper).filter(
            StockVolspikeGapper.ticker == ticker
        ).first()
        
        if not volspike_data:
            return jsonify({
                'ticker': ticker,
                'spike_days': [],
                'gap_days': []
            })
        
        # Parse date strings back to arrays
        spike_dates = [d.strip() for d in (volspike_data.volume_spike_days or '').split(',') if d.strip()]
        gap_dates = [d.strip() for d in (volspike_data.gap_days or '').split(',') if d.strip()]
        
        return jsonify({
            'ticker': ticker,
            'spike_days': spike_dates,
            'gap_days': gap_dates
        })
    
    except Exception as e:
        logger.error(f"Error getting volspike events for {ticker}: {str(e)}")
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
            StockMetrics.pe_t,
            StockMetrics.pe_t_plus_1,
            StockMetrics.ps_t,
            StockMetrics.ps_t_plus_1,
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
                'pe_t': round(stock.pe_t, 2) if stock.pe_t else None,
                'pe_t_plus_1': round(stock.pe_t_plus_1, 2) if stock.pe_t_plus_1 else None,
                'ps_t': round(stock.ps_t, 2) if stock.ps_t else None,
                'ps_t_plus_1': round(stock.ps_t_plus_1, 2) if stock.ps_t_plus_1 else None,
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
            sm.pe_t_plus_1,
            sm.ps_t_plus_1,
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
                'pe_t_plus_1': round(row[14], 2) if row[14] else None,
                'ps_t_plus_1': round(row[15], 2) if row[15] else None,
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
            StockMetrics.pe_t,
            StockMetrics.pe_t_plus_1,
            StockMetrics.ps_t,
            StockMetrics.ps_t_plus_1,
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
                'pe_t': round(stock.pe_t, 2) if stock.pe_t else None,
                'pe_t_plus_1': round(stock.pe_t_plus_1, 2) if stock.pe_t_plus_1 else None,
                'ps_t': round(stock.ps_t, 2) if stock.ps_t else None,
                'ps_t_plus_1': round(stock.ps_t_plus_1, 2) if stock.ps_t_plus_1 else None,
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
                StockMetrics.country,
                StockMetrics.sector,
                StockMetrics.industry,
                StockMetrics.ipo_date,
                StockMetrics.market_cap,
                StockMetrics.current_price,
                StockMetrics.range_52_week,
                StockMetrics.volume,
                StockMetrics.dollar_volume,
                StockMetrics.avg_vol_10d,
                StockMetrics.vol_vs_10d_avg,
                StockMetrics.ti65,
                StockMetrics.rsi,
                StockMetrics.rsi_mktcap,
                StockMetrics.dr_1,
                StockMetrics.dr_5,
                StockMetrics.dr_20,
                StockMetrics.dr_60,
                StockMetrics.dr_120,
                StockMetrics.atr20,
                StockMetrics.pe_t_minus_1,
                StockMetrics.pe_t,
                StockMetrics.pe_t_plus_1,
                StockMetrics.pe_t_plus_2,
                StockMetrics.ps_t_minus_1,
                StockMetrics.ps_t,
                StockMetrics.ps_t_plus_1,
                StockMetrics.ps_t_plus_2,
                StockMetrics.rev_growth_t_minus_1,
                StockMetrics.rev_growth_t,
                StockMetrics.rev_growth_t_plus_1,
                StockMetrics.rev_growth_t_plus_2,
                StockMetrics.eps_growth_t_minus_1,
                StockMetrics.eps_growth_t,
                StockMetrics.eps_growth_t_plus_1,
                StockMetrics.eps_growth_t_plus_2,
                StockMetrics.short_float,
                StockMetrics.short_ratio,
                StockMetrics.short_interest,
                StockMetrics.low_float,
                StockMetrics.float_shares,
                StockMetrics.outstanding_shares,
                StockMetrics.free_float,
                StockMetrics.updated_at,
                StockVolspikeGapper.last_event_date,
                StockVolspikeGapper.last_event_type,
                StockVolspikeGapper.last_event_magnitude,
                StockVolspikeGapper.last_event_return,
                MainView.tags,
            ).outerjoin(
                StockVolspikeGapper, StockVolspikeGapper.ticker == StockMetrics.ticker
            ).outerjoin(
                MainView, MainView.ticker == StockMetrics.ticker
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
                'vol_vs_10d_avg': round(stock.vol_vs_10d_avg, 2) if stock.vol_vs_10d_avg else None,
                'ti65': round(stock.ti65, 2) if stock.ti65 else None,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'dr_60': round(stock.dr_60, 2) if stock.dr_60 else None,
                'dr_120': round(stock.dr_120, 2) if stock.dr_120 else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                'pe_t_minus_1': round(stock.pe_t_minus_1, 2) if stock.pe_t_minus_1 else None,
                'pe_t': round(stock.pe_t, 2) if stock.pe_t else None,
                'pe_t_plus_1': round(stock.pe_t_plus_1, 2) if stock.pe_t_plus_1 else None,
                'pe_t_plus_2': round(stock.pe_t_plus_2, 2) if stock.pe_t_plus_2 else None,
                'ps_t_minus_1': round(stock.ps_t_minus_1, 2) if stock.ps_t_minus_1 else None,
                'ps_t': round(stock.ps_t, 2) if stock.ps_t else None,
                'ps_t_plus_1': round(stock.ps_t_plus_1, 2) if stock.ps_t_plus_1 else None,
                'ps_t_plus_2': round(stock.ps_t_plus_2, 2) if stock.ps_t_plus_2 else None,
                'rev_growth_t_minus_1': round(stock.rev_growth_t_minus_1, 2) if stock.rev_growth_t_minus_1 else None,
                'rev_growth_t': round(stock.rev_growth_t, 2) if stock.rev_growth_t else None,
                'rev_growth_t_plus_1': round(stock.rev_growth_t_plus_1, 2) if stock.rev_growth_t_plus_1 else None,
                'rev_growth_t_plus_2': round(stock.rev_growth_t_plus_2, 2) if stock.rev_growth_t_plus_2 else None,
                'eps_growth_t_minus_1': round(stock.eps_growth_t_minus_1, 2) if stock.eps_growth_t_minus_1 else None,
                'eps_growth_t': round(stock.eps_growth_t, 2) if stock.eps_growth_t else None,
                'eps_growth_t_plus_1': round(stock.eps_growth_t_plus_1, 2) if stock.eps_growth_t_plus_1 else None,
                'eps_growth_t_plus_2': round(stock.eps_growth_t_plus_2, 2) if stock.eps_growth_t_plus_2 else None,
                'short_float': round(stock.short_float, 2) if stock.short_float else None,
                'short_ratio': round(stock.short_ratio, 2) if stock.short_ratio else None,
                'short_interest': round(stock.short_interest, 2) if stock.short_interest else None,
                'low_float': stock.low_float,
                'float_shares': stock.float_shares,
                'outstanding_shares': stock.outstanding_shares,
                'free_float': round(stock.free_float, 2) if stock.free_float else None,
                'last_event_date': stock.last_event_date.strftime('%Y-%m-%d') if stock.last_event_date else None,
                'last_event_type': stock.last_event_type,
                'last_event_magnitude': float(stock.last_event_magnitude) if stock.last_event_magnitude else None,
                'last_event_return': float(stock.last_event_return) if stock.last_event_return else None,
                'tags': stock.tags,
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
# Bottom Performance (Top Losers) Endpoints
# ============================================================================

def get_bottom_performance_stocks(session, market_cap_category=None):
    """
    Get union of bottom 30 stocks by dr_1, dr_5, and dr_20 from stock_metrics.
    Returns combined list with all columns for consistency with other pages.
    This is the inverse of get_top_performance_stocks - showing biggest losers.
    """
    try:
        def build_query(return_column, limit=30):
            query = session.query(
                StockMetrics.ticker,
                StockMetrics.company_name,
                StockMetrics.country,
                StockMetrics.sector,
                StockMetrics.industry,
                StockMetrics.ipo_date,
                StockMetrics.market_cap,
                StockMetrics.current_price,
                StockMetrics.range_52_week,
                StockMetrics.volume,
                StockMetrics.dollar_volume,
                StockMetrics.avg_vol_10d,
                StockMetrics.vol_vs_10d_avg,
                StockMetrics.ti65,
                StockMetrics.rsi,
                StockMetrics.rsi_mktcap,
                StockMetrics.dr_1,
                StockMetrics.dr_5,
                StockMetrics.dr_20,
                StockMetrics.dr_60,
                StockMetrics.dr_120,
                StockMetrics.atr20,
                StockMetrics.pe_t_minus_1,
                StockMetrics.pe_t,
                StockMetrics.pe_t_plus_1,
                StockMetrics.pe_t_plus_2,
                StockMetrics.ps_t_minus_1,
                StockMetrics.ps_t,
                StockMetrics.ps_t_plus_1,
                StockMetrics.ps_t_plus_2,
                StockMetrics.rev_growth_t_minus_1,
                StockMetrics.rev_growth_t,
                StockMetrics.rev_growth_t_plus_1,
                StockMetrics.rev_growth_t_plus_2,
                StockMetrics.eps_growth_t_minus_1,
                StockMetrics.eps_growth_t,
                StockMetrics.eps_growth_t_plus_1,
                StockMetrics.eps_growth_t_plus_2,
                StockMetrics.short_float,
                StockMetrics.short_ratio,
                StockMetrics.short_interest,
                StockMetrics.low_float,
                StockMetrics.float_shares,
                StockMetrics.outstanding_shares,
                StockMetrics.free_float,
                StockMetrics.updated_at,
                StockVolspikeGapper.last_event_date,
                StockVolspikeGapper.last_event_type,
                StockVolspikeGapper.last_event_magnitude,
                StockVolspikeGapper.last_event_return,
                MainView.tags,
            ).outerjoin(
                StockVolspikeGapper, StockVolspikeGapper.ticker == StockMetrics.ticker
            ).outerjoin(
                MainView, MainView.ticker == StockMetrics.ticker
            ).filter(
                getattr(StockMetrics, return_column).isnot(None),
                StockMetrics.market_cap.isnot(None)
            )

            if market_cap_category:
                category = MARKET_CAP_CATEGORIES.get(market_cap_category)
                if category:
                    query = query.filter(StockMetrics.market_cap >= category['min'])
                    if category['max'] is not None:
                        query = query.filter(StockMetrics.market_cap < category['max'])

            query = apply_global_liquidity_filters(query)
            query = query.order_by(asc(getattr(StockMetrics, return_column))).limit(limit)
            return query

        bottom_1d = build_query('dr_1').all()
        bottom_5d = build_query('dr_5').all()
        bottom_20d = build_query('dr_20').all()

        seen_tickers = set()
        combined_stocks = []

        for stock_list in [bottom_1d, bottom_5d, bottom_20d]:
            for stock in stock_list:
                if stock.ticker not in seen_tickers:
                    seen_tickers.add(stock.ticker)
                    combined_stocks.append(stock)

        results = []
        for stock in combined_stocks:
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
                'vol_vs_10d_avg': round(stock.vol_vs_10d_avg, 2) if stock.vol_vs_10d_avg else None,
                'ti65': round(stock.ti65, 2) if stock.ti65 else None,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'dr_60': round(stock.dr_60, 2) if stock.dr_60 else None,
                'dr_120': round(stock.dr_120, 2) if stock.dr_120 else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                'pe_t_minus_1': round(stock.pe_t_minus_1, 2) if stock.pe_t_minus_1 else None,
                'pe_t': round(stock.pe_t, 2) if stock.pe_t else None,
                'pe_t_plus_1': round(stock.pe_t_plus_1, 2) if stock.pe_t_plus_1 else None,
                'pe_t_plus_2': round(stock.pe_t_plus_2, 2) if stock.pe_t_plus_2 else None,
                'ps_t_minus_1': round(stock.ps_t_minus_1, 2) if stock.ps_t_minus_1 else None,
                'ps_t': round(stock.ps_t, 2) if stock.ps_t else None,
                'ps_t_plus_1': round(stock.ps_t_plus_1, 2) if stock.ps_t_plus_1 else None,
                'ps_t_plus_2': round(stock.ps_t_plus_2, 2) if stock.ps_t_plus_2 else None,
                'rev_growth_t_minus_1': round(stock.rev_growth_t_minus_1, 2) if stock.rev_growth_t_minus_1 else None,
                'rev_growth_t': round(stock.rev_growth_t, 2) if stock.rev_growth_t else None,
                'rev_growth_t_plus_1': round(stock.rev_growth_t_plus_1, 2) if stock.rev_growth_t_plus_1 else None,
                'rev_growth_t_plus_2': round(stock.rev_growth_t_plus_2, 2) if stock.rev_growth_t_plus_2 else None,
                'eps_growth_t_minus_1': round(stock.eps_growth_t_minus_1, 2) if stock.eps_growth_t_minus_1 else None,
                'eps_growth_t': round(stock.eps_growth_t, 2) if stock.eps_growth_t else None,
                'eps_growth_t_plus_1': round(stock.eps_growth_t_plus_1, 2) if stock.eps_growth_t_plus_1 else None,
                'eps_growth_t_plus_2': round(stock.eps_growth_t_plus_2, 2) if stock.eps_growth_t_plus_2 else None,
                'short_float': round(stock.short_float, 2) if stock.short_float else None,
                'short_ratio': round(stock.short_ratio, 2) if stock.short_ratio else None,
                'short_interest': round(stock.short_interest, 2) if stock.short_interest else None,
                'low_float': stock.low_float,
                'float_shares': stock.float_shares,
                'outstanding_shares': stock.outstanding_shares,
                'free_float': round(stock.free_float, 2) if stock.free_float else None,
                'last_event_date': stock.last_event_date.strftime('%Y-%m-%d') if stock.last_event_date else None,
                'last_event_type': stock.last_event_type,
                'last_event_magnitude': float(stock.last_event_magnitude) if stock.last_event_magnitude else None,
                'last_event_return': float(stock.last_event_return) if stock.last_event_return else None,
                'tags': stock.tags,
                'updated_at': stock.updated_at.strftime('%Y-%m-%d %H:%M:%S') if stock.updated_at else None
            })
        
        return results
    
    except Exception as e:
        logger.error(f"Error getting bottom performance stocks: {str(e)}")
        return []


@app.route('/api/BottomPerformance-All')
def get_bottom_performance_all():
    s = Session()
    try:
        return jsonify(get_bottom_performance_stocks(s, market_cap_category=None))
    finally:
        s.close()

@app.route('/api/BottomPerformance-MicroCap')
def get_bottom_performance_micro():
    s = Session()
    try:
        return jsonify(get_bottom_performance_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/BottomPerformance-SmallCap')
def get_bottom_performance_small():
    s = Session()
    try:
        return jsonify(get_bottom_performance_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/BottomPerformance-MidCap')
def get_bottom_performance_mid():
    s = Session()
    try:
        return jsonify(get_bottom_performance_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/BottomPerformance-LargeCap')
def get_bottom_performance_large():
    s = Session()
    try:
        return jsonify(get_bottom_performance_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/BottomPerformance-MegaCap')
def get_bottom_performance_mega():
    s = Session()
    try:
        return jsonify(get_bottom_performance_stocks(s, market_cap_category='mega'))
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
            StockVolspikeGapper.last_event_magnitude,
            StockVolspikeGapper.last_event_return,
            StockMetrics.company_name,
            StockMetrics.country,
            StockMetrics.sector,
            StockMetrics.industry,
            StockMetrics.ipo_date,
            StockMetrics.market_cap,
            StockMetrics.current_price,
            StockMetrics.range_52_week,
            StockMetrics.volume,
            StockMetrics.dollar_volume,
            StockMetrics.avg_vol_10d,
            StockMetrics.vol_vs_10d_avg,
            StockMetrics.ti65,
            StockMetrics.rsi,
            StockMetrics.rsi_mktcap,
            StockMetrics.dr_1,
            StockMetrics.dr_5,
            StockMetrics.dr_20,
            StockMetrics.dr_60,
            StockMetrics.dr_120,
            StockMetrics.atr20,
            StockMetrics.pe_t_minus_1,
            StockMetrics.pe_t,
            StockMetrics.pe_t_plus_1,
            StockMetrics.pe_t_plus_2,
            StockMetrics.ps_t_minus_1,
            StockMetrics.ps_t,
            StockMetrics.ps_t_plus_1,
            StockMetrics.ps_t_plus_2,
            StockMetrics.rev_growth_t_minus_1,
            StockMetrics.rev_growth_t,
            StockMetrics.rev_growth_t_plus_1,
            StockMetrics.rev_growth_t_plus_2,
            StockMetrics.eps_growth_t_minus_1,
            StockMetrics.eps_growth_t,
            StockMetrics.eps_growth_t_plus_1,
            StockMetrics.eps_growth_t_plus_2,
            StockMetrics.short_float,
            StockMetrics.short_ratio,
            StockMetrics.short_interest,
            StockMetrics.low_float,
            StockMetrics.float_shares,
            StockMetrics.outstanding_shares,
            StockMetrics.free_float,
            StockMetrics.updated_at,
            MainView.tags,
        ).join(
            StockMetrics, StockVolspikeGapper.ticker == StockMetrics.ticker
        ).outerjoin(
            MainView, MainView.ticker == StockVolspikeGapper.ticker
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
                'ti65': round(stock.ti65, 2) if stock.ti65 else None,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'dr_60': round(stock.dr_60, 2) if stock.dr_60 else None,
                'dr_120': round(stock.dr_120, 2) if stock.dr_120 else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                'pe_t_minus_1': round(stock.pe_t_minus_1, 2) if stock.pe_t_minus_1 else None,
                'pe_t': round(stock.pe_t, 2) if stock.pe_t else None,
                'pe_t_plus_1': round(stock.pe_t_plus_1, 2) if stock.pe_t_plus_1 else None,
                'pe_t_plus_2': round(stock.pe_t_plus_2, 2) if stock.pe_t_plus_2 else None,
                'ps_t_minus_1': round(stock.ps_t_minus_1, 2) if stock.ps_t_minus_1 else None,
                'ps_t': round(stock.ps_t, 2) if stock.ps_t else None,
                'ps_t_plus_1': round(stock.ps_t_plus_1, 2) if stock.ps_t_plus_1 else None,
                'ps_t_plus_2': round(stock.ps_t_plus_2, 2) if stock.ps_t_plus_2 else None,
                'rev_growth_t_minus_1': round(stock.rev_growth_t_minus_1, 2) if stock.rev_growth_t_minus_1 else None,
                'rev_growth_t': round(stock.rev_growth_t, 2) if stock.rev_growth_t else None,
                'rev_growth_t_plus_1': round(stock.rev_growth_t_plus_1, 2) if stock.rev_growth_t_plus_1 else None,
                'rev_growth_t_plus_2': round(stock.rev_growth_t_plus_2, 2) if stock.rev_growth_t_plus_2 else None,
                'eps_growth_t_minus_1': round(stock.eps_growth_t_minus_1, 2) if stock.eps_growth_t_minus_1 else None,
                'eps_growth_t': round(stock.eps_growth_t, 2) if stock.eps_growth_t else None,
                'eps_growth_t_plus_1': round(stock.eps_growth_t_plus_1, 2) if stock.eps_growth_t_plus_1 else None,
                'eps_growth_t_plus_2': round(stock.eps_growth_t_plus_2, 2) if stock.eps_growth_t_plus_2 else None,
                'short_float': round(stock.short_float, 2) if stock.short_float else None,
                'short_ratio': round(stock.short_ratio, 2) if stock.short_ratio else None,
                'short_interest': round(stock.short_interest, 2) if stock.short_interest else None,
                'low_float': stock.low_float,
                'float_shares': stock.float_shares,
                'outstanding_shares': stock.outstanding_shares,
                'free_float': round(stock.free_float, 2) if stock.free_float else None,
                'spike_day_count': stock.spike_day_count or 0,
                'avg_volume_spike': float(stock.avg_volume_spike) if stock.avg_volume_spike else None,
                'volume_spike_days': spike_dates,
                'gapper_day_count': stock.gapper_day_count or 0,
                'avg_return_gapper': float(stock.avg_return_gapper) if stock.avg_return_gapper else None,
                'gap_days': gap_dates,
                'last_event_date': stock.last_event_date.strftime('%Y-%m-%d') if stock.last_event_date else None,
                'last_event_type': stock.last_event_type,
                'last_event_magnitude': float(stock.last_event_magnitude) if stock.last_event_magnitude else None,
                'last_event_return': float(stock.last_event_return) if stock.last_event_return else None,
                'tags': stock.tags,
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
                'ti65': round(stock.ti65, 2) if stock.ti65 else None,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'dr_60': round(stock.dr_60, 2) if stock.dr_60 else None,
                'dr_120': round(stock.dr_120, 2) if stock.dr_120 else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                # P/E trajectory
                'pe_t_minus_1': round(stock.pe_t_minus_1, 2) if stock.pe_t_minus_1 else None,
                'pe_t': round(stock.pe_t, 2) if stock.pe_t else None,
                'pe_t_plus_1': round(stock.pe_t_plus_1, 2) if stock.pe_t_plus_1 else None,
                'pe_t_plus_2': round(stock.pe_t_plus_2, 2) if stock.pe_t_plus_2 else None,
                # P/S trajectory
                'ps_t_minus_1': round(stock.ps_t_minus_1, 2) if stock.ps_t_minus_1 else None,
                'ps_t': round(stock.ps_t, 2) if stock.ps_t else None,
                'ps_t_plus_1': round(stock.ps_t_plus_1, 2) if stock.ps_t_plus_1 else None,
                'ps_t_plus_2': round(stock.ps_t_plus_2, 2) if stock.ps_t_plus_2 else None,
                'rev_growth_t_minus_1': round(stock.rev_growth_t_minus_1, 2) if stock.rev_growth_t_minus_1 else None,
                'rev_growth_t': round(stock.rev_growth_t, 2) if stock.rev_growth_t else None,
                'rev_growth_t_plus_1': round(stock.rev_growth_t_plus_1, 2) if stock.rev_growth_t_plus_1 else None,
                'rev_growth_t_plus_2': round(stock.rev_growth_t_plus_2, 2) if stock.rev_growth_t_plus_2 else None,
                'eps_growth_t_minus_1': round(stock.eps_growth_t_minus_1, 2) if stock.eps_growth_t_minus_1 else None,
                'eps_growth_t': round(stock.eps_growth_t, 2) if stock.eps_growth_t else None,
                'eps_growth_t_plus_1': round(stock.eps_growth_t_plus_1, 2) if stock.eps_growth_t_plus_1 else None,
                'eps_growth_t_plus_2': round(stock.eps_growth_t_plus_2, 2) if stock.eps_growth_t_plus_2 else None,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'short_float': round(stock.short_float, 2) if stock.short_float else None,
                'short_ratio': round(stock.short_ratio, 2) if stock.short_ratio else None,
                'short_interest': round(stock.short_interest, 2) if stock.short_interest else None,
                'low_float': stock.low_float,
                # Float/Liquidity data
                'float_shares': stock.float_shares,
                'outstanding_shares': stock.outstanding_shares,
                'free_float': round(stock.free_float, 2) if stock.free_float else None,
                'spike_day_count': stock.spike_day_count or 0,
                'avg_volume_spike': round(stock.avg_volume_spike, 2) if stock.avg_volume_spike else None,
                'volume_spike_days': spike_dates,
                'gapper_day_count': stock.gapper_day_count or 0,
                'avg_return_gapper': round(stock.avg_return_gapper, 4) if stock.avg_return_gapper else None,
                'gap_days': gap_dates,
                'last_event_date': stock.last_event_date.strftime('%Y-%m-%d') if stock.last_event_date else None,
                'last_event_type': stock.last_event_type,
                'last_event_magnitude': float(stock.last_event_magnitude) if stock.last_event_magnitude else None,
                'last_event_return': float(stock.last_event_return) if stock.last_event_return else None,
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


@app.route('/api/MainView-ByTickers')
def get_main_view_by_tickers():
    """
    Get MainView data for a specific list of tickers.
    Tickers passed as comma-separated string via 'tickers' query param.
    """
    tickers_param = request.args.get('tickers', '').strip()
    if not tickers_param:
        return jsonify([])

    tickers = [t.strip().upper() for t in tickers_param.split(',') if t.strip()]
    if not tickers:
        return jsonify([])

    s = Session()
    try:
        stocks = s.query(MainView).filter(MainView.ticker.in_(tickers)).all()
        results = []
        for stock in stocks:
            spike_dates = [d for d in (stock.volume_spike_days or '').split(',') if d.strip()]
            gap_dates = [d for d in (stock.gap_days or '').split(',') if d.strip()]
            results.append({
                'ticker': stock.ticker,
                'company_name': stock.company_name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market_cap': stock.market_cap,
                'current_price': round(stock.current_price, 2) if stock.current_price else None,
                'volume': int(stock.volume) if stock.volume else None,
                'dollar_volume': round(stock.dollar_volume, 2) if stock.dollar_volume else None,
                'vol_vs_10d_avg': float(stock.vol_vs_10d_avg) if stock.vol_vs_10d_avg else None,
                'ti65': round(stock.ti65, 2) if stock.ti65 else None,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'dr_60': round(stock.dr_60, 2) if stock.dr_60 else None,
                'dr_120': round(stock.dr_120, 2) if stock.dr_120 else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'float_shares': stock.float_shares,
                'last_event_date': stock.last_event_date.strftime('%Y-%m-%d') if stock.last_event_date else None,
                'last_event_type': stock.last_event_type,
                'last_event_magnitude': float(stock.last_event_magnitude) if stock.last_event_magnitude else None,
                'volume_spike_days': spike_dates,
                'gap_days': gap_dates,
                'tags': stock.tags,
            })
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error getting main view by tickers: {str(e)}")
        return jsonify([])
    finally:
        s.close()


# ============================================================================
# High Sales Growth Endpoints (filter MainView by high_sales_growth tag)
# ============================================================================

def get_high_sales_growth_stocks(session, market_cap_category=None):
    """
    Get stocks from main_view table that have the high_sales_growth tag.
    """
    try:
        query = session.query(MainView).filter(MainView.tags.like('%high_sales_growth%'))

        # Apply market cap filter
        if market_cap_category:
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                query = query.filter(MainView.market_cap >= category['min'])
                if category['max'] is not None:
                    query = query.filter(MainView.market_cap < category['max'])

        # Order by revenue growth (average of t and t+1)
        query = query.order_by(desc(MainView.rev_growth_t_plus_1))

        stocks = query.all()

        results = []
        for stock in stocks:
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
                'ti65': round(stock.ti65, 2) if stock.ti65 else None,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'dr_60': round(stock.dr_60, 2) if stock.dr_60 else None,
                'dr_120': round(stock.dr_120, 2) if stock.dr_120 else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                'pe_t_minus_1': round(stock.pe_t_minus_1, 2) if stock.pe_t_minus_1 else None,
                'pe_t': round(stock.pe_t, 2) if stock.pe_t else None,
                'pe_t_plus_1': round(stock.pe_t_plus_1, 2) if stock.pe_t_plus_1 else None,
                'pe_t_plus_2': round(stock.pe_t_plus_2, 2) if stock.pe_t_plus_2 else None,
                'ps_t_minus_1': round(stock.ps_t_minus_1, 2) if stock.ps_t_minus_1 else None,
                'ps_t': round(stock.ps_t, 2) if stock.ps_t else None,
                'ps_t_plus_1': round(stock.ps_t_plus_1, 2) if stock.ps_t_plus_1 else None,
                'ps_t_plus_2': round(stock.ps_t_plus_2, 2) if stock.ps_t_plus_2 else None,
                'rev_growth_t_minus_1': round(stock.rev_growth_t_minus_1, 2) if stock.rev_growth_t_minus_1 else None,
                'rev_growth_t': round(stock.rev_growth_t, 2) if stock.rev_growth_t else None,
                'rev_growth_t_plus_1': round(stock.rev_growth_t_plus_1, 2) if stock.rev_growth_t_plus_1 else None,
                'rev_growth_t_plus_2': round(stock.rev_growth_t_plus_2, 2) if stock.rev_growth_t_plus_2 else None,
                'eps_growth_t_minus_1': round(stock.eps_growth_t_minus_1, 2) if stock.eps_growth_t_minus_1 else None,
                'eps_growth_t': round(stock.eps_growth_t, 2) if stock.eps_growth_t else None,
                'eps_growth_t_plus_1': round(stock.eps_growth_t_plus_1, 2) if stock.eps_growth_t_plus_1 else None,
                'eps_growth_t_plus_2': round(stock.eps_growth_t_plus_2, 2) if stock.eps_growth_t_plus_2 else None,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'short_float': round(stock.short_float, 2) if stock.short_float else None,
                'short_ratio': round(stock.short_ratio, 2) if stock.short_ratio else None,
                'short_interest': round(stock.short_interest, 2) if stock.short_interest else None,
                'low_float': stock.low_float,
                'float_shares': stock.float_shares,
                'outstanding_shares': stock.outstanding_shares,
                'free_float': round(stock.free_float, 2) if stock.free_float else None,
                'spike_day_count': stock.spike_day_count or 0,
                'avg_volume_spike': round(stock.avg_volume_spike, 2) if stock.avg_volume_spike else None,
                'volume_spike_days': spike_dates,
                'gapper_day_count': stock.gapper_day_count or 0,
                'avg_return_gapper': round(stock.avg_return_gapper, 4) if stock.avg_return_gapper else None,
                'gap_days': gap_dates,
                'last_event_date': stock.last_event_date.strftime('%Y-%m-%d') if stock.last_event_date else None,
                'last_event_type': stock.last_event_type,
                'last_event_magnitude': float(stock.last_event_magnitude) if stock.last_event_magnitude else None,
                'last_event_return': float(stock.last_event_return) if stock.last_event_return else None,
                'tags': stock.tags,
                'updated_at': stock.updated_at.strftime('%Y-%m-%d %H:%M:%S') if stock.updated_at else None
            })

        return results

    except Exception as e:
        logger.error(f"Error getting high_sales_growth stocks for {market_cap_category}: {str(e)}")
        return []


@app.route('/api/HighSalesGrowth-All')
def get_high_sales_growth_all():
    s = Session()
    try:
        return jsonify(get_high_sales_growth_stocks(s, market_cap_category=None))
    finally:
        s.close()

@app.route('/api/HighSalesGrowth-MicroCap')
def get_high_sales_growth_micro():
    s = Session()
    try:
        return jsonify(get_high_sales_growth_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/HighSalesGrowth-SmallCap')
def get_high_sales_growth_small():
    s = Session()
    try:
        return jsonify(get_high_sales_growth_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/HighSalesGrowth-MidCap')
def get_high_sales_growth_mid():
    s = Session()
    try:
        return jsonify(get_high_sales_growth_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/HighSalesGrowth-LargeCap')
def get_high_sales_growth_large():
    s = Session()
    try:
        return jsonify(get_high_sales_growth_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/HighSalesGrowth-MegaCap')
def get_high_sales_growth_mega():
    s = Session()
    try:
        return jsonify(get_high_sales_growth_stocks(s, market_cap_category='mega'))
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
# AI Stock Research (Perplexity API) Endpoints
# ============================================================

# Path to AI prompt templates
AI_PROMPT_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'ai_prompts', 'stock_research_prompt.txt')
CLAUDE_PROMPT_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'ai_prompts', 'claude_stock_research_prompt.txt')


def get_company_name_for_ticker(session, ticker):
    """Get company name for a ticker from the database."""
    ticker_obj = session.query(Ticker).filter(Ticker.ticker == ticker.upper()).first()
    if ticker_obj and ticker_obj.company_name:
        return ticker_obj.company_name
    return ticker.upper()


def load_ai_prompt_template():
    """Load the AI prompt template from file."""
    try:
        if os.path.exists(AI_PROMPT_FILE):
            with open(AI_PROMPT_FILE, 'r') as f:
                return f.read()
        else:
            logger.warning(f"AI prompt file not found at {AI_PROMPT_FILE}, using default prompt")
            return None
    except Exception as e:
        logger.error(f"Error loading AI prompt template: {str(e)}")
        return None


def load_claude_prompt_template():
    """Load the Claude AI prompt template from file."""
    try:
        if os.path.exists(CLAUDE_PROMPT_FILE):
            with open(CLAUDE_PROMPT_FILE, 'r') as f:
                return f.read()
        else:
            logger.warning(f"Claude prompt file not found at {CLAUDE_PROMPT_FILE}")
            return None
    except Exception as e:
        logger.error(f"Error loading Claude prompt template: {str(e)}")
        return None


def call_perplexity_api(prompt, model='sonar'):
    """
    Call the Perplexity API with the given prompt.
    
    Args:
        prompt: The prompt text to send
        model: 'sonar' or 'sonar-pro'
    
    Returns:
        Tuple of (response_text, error_message)
        - On success: (response_text, None)
        - On API credit failure: (None, 'INSUFFICIENT_CREDITS')
        - On other errors: (None, error_message)
    """
    import requests
    
    api_key = os.getenv('PERPLEXITY_API_KEY')
    if not api_key:
        return None, 'PERPLEXITY_API_KEY not configured'
    
    # Map model names to Perplexity API model identifiers
    model_map = {
        'sonar': 'sonar',
        'sonar-pro': 'sonar-pro'
    }
    api_model = model_map.get(model, 'sonar')
    
    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": api_model,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 20000,
        "temperature": 0.1,
        "return_citations": False
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        
        # Check for API credit issues
        if response.status_code == 402:
            return None, 'INSUFFICIENT_CREDITS'
        
        if response.status_code == 429:
            return None, 'RATE_LIMITED'
        
        if response.status_code != 200:
            error_detail = response.text[:500] if response.text else 'Unknown error'
            logger.error(f"Perplexity API error {response.status_code}: {error_detail}")
            return None, f'API error: {response.status_code}'
        
        data = response.json()
        
        if 'choices' in data and len(data['choices']) > 0:
            content = data['choices'][0].get('message', {}).get('content', '')
            return content, None
        else:
            return None, 'No response content from API'
            
    except requests.exceptions.Timeout:
        return None, 'API request timed out (120s)'
    except requests.exceptions.RequestException as e:
        logger.error(f"Perplexity API request error: {str(e)}")
        return None, f'Request error: {str(e)}'
    except Exception as e:
        logger.error(f"Unexpected error calling Perplexity API: {str(e)}")
        return None, f'Unexpected error: {str(e)}'


def call_claude_api_with_web_search(ticker: str, company_name: str, months: int = 12):
    """
    Call Claude API with web search to get stock news summary.
    
    Args:
        ticker: Stock ticker symbol
        company_name: Full company name
        months: How many months back to search
    
    Returns:
        Tuple of (response_text, error_message)
        - On success: (response_text, None)
        - On rate limit: (None, 'RATE_LIMITED')
        - On other errors: (None, error_message)
    """
    try:
        from anthropic import Anthropic
    except ImportError:
        return None, 'anthropic package not installed. Run: pip install anthropic'
    
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        return None, 'ANTHROPIC_API_KEY not configured'
    
    client = Anthropic(api_key=api_key)
    
    # Load prompt from file
    template = load_claude_prompt_template()
    if not template:
        return None, f'Claude prompt template not found at {CLAUDE_PROMPT_FILE}'
    
    # Replace placeholders
    prompt = template.replace('[company_name]', company_name)
    prompt = prompt.replace('[ticker]', ticker)
    prompt = prompt.replace('[months]', str(months))

    # Retry logic for rate limits
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=4096,
                system="You are a financial analyst. Use web search to find current, accurate information about stock-moving events. Be thorough and factual.",
                tools=[{
                    "type": "web_search_20250305",
                    "name": "web_search"
                }],
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Extract all text from response blocks
            text_parts = []
            for block in response.content:
                if hasattr(block, 'type') and block.type == "text":
                    text_parts.append(block.text)
            
            summary = "\n".join(text_parts)
            return summary, None
            
        except Exception as e:
            error_msg = str(e)
            
            # Rate limit - wait and retry
            if "429" in error_msg or "rate_limit" in error_msg:
                if attempt < max_retries - 1:
                    wait_time = 60 * (attempt + 1)
                    logger.info(f"Claude rate limit hit. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                return None, 'RATE_LIMITED'
            
            logger.error(f"Claude API error: {error_msg}")
            return None, f'API error: {error_msg}'
    
    return None, 'Max retries exceeded'


@app.route('/api/stock-notes/ai-research/<ticker>', methods=['POST'])
def generate_ai_stock_research(ticker):
    """
    Generate AI stock research notes using Perplexity API.
    Appends the generated notes to existing notes and saves to DB.
    
    Request body (optional):
    {
        "model": "sonar" or "sonar-pro" (default: "sonar"),
        "custom_prompt": "Optional custom prompt override"
    }
    
    Returns:
    {
        "ticker": "AAPL",
        "ai_notes": "Generated AI research notes...",
        "notes": "Full combined notes (existing + AI)",
        "updated_at": "2024-01-20T...",
        "model_used": "sonar",
        "message": "success"
    }
    
    Error responses:
    - 402: Insufficient API credits
    - 429: Rate limited
    - 500: Other errors
    """
    s = Session()
    try:
        ticker = ticker.upper()
        data = request.get_json() or {}
        
        model = data.get('model', 'sonar')
        custom_prompt = data.get('custom_prompt')
        
        # Validate model
        if model not in ['sonar', 'sonar-pro']:
            return jsonify({'error': 'model must be "sonar" or "sonar-pro"'}), 400
        
        # Get company name
        company_name = get_company_name_for_ticker(s, ticker)
        
        # Build prompt from template file
        if custom_prompt:
            prompt = custom_prompt
        else:
            template = load_ai_prompt_template()
            if not template:
                return jsonify({
                    'error': 'AI prompt template file not found. Please create the template at user_data/ai_prompts/stock_research_prompt.txt',
                    'error_type': 'MISSING_TEMPLATE'
                }), 500
            
            # Replace placeholders in template
            # Supports both [placeholder] and {placeholder} formats
            prompt = template.replace('[company_name]', company_name)
            prompt = prompt.replace('[ticker]', ticker)
        
        # Call Perplexity API
        logger.info(f"Calling Perplexity API for {ticker} with model {model}")
        ai_response, error = call_perplexity_api(prompt, model)
        
        if error:
            # Handle specific error types
            if error == 'INSUFFICIENT_CREDITS':
                return jsonify({
                    'error': 'Insufficient Perplexity API credits. Please add credits to your account.',
                    'error_type': 'INSUFFICIENT_CREDITS'
                }), 402
            elif error == 'RATE_LIMITED':
                return jsonify({
                    'error': 'Perplexity API rate limited. Please try again in a few minutes.',
                    'error_type': 'RATE_LIMITED'
                }), 429
            else:
                return jsonify({
                    'error': f'Failed to generate AI research: {error}',
                    'error_type': 'API_ERROR'
                }), 500
        
        # Format AI notes with timestamp
        from datetime import datetime
        import pytz
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        ai_notes_formatted = f"\n\n---\n\n## AI Research ({model}) - {timestamp}\n\n{ai_response}"
        
        # Get existing notes
        existing = s.query(StockNotes).filter(StockNotes.ticker == ticker).first()
        current_notes = existing.notes if existing and existing.notes else ''
        
        # Combine notes
        combined_notes = current_notes + ai_notes_formatted
        
        # Save to database
        if existing:
            existing.notes = combined_notes
        else:
            new_note = StockNotes(ticker=ticker, notes=combined_notes)
            s.add(new_note)
        
        s.commit()
        
        # Export all notes to file for persistence
        export_all_notes_to_file()
        
        # Get updated timestamp
        updated_note = s.query(StockNotes).filter(StockNotes.ticker == ticker).first()
        
        return jsonify({
            'ticker': ticker,
            'ai_notes': ai_response,
            'notes': combined_notes,
            'updated_at': updated_note.updated_at.isoformat() if updated_note and updated_note.updated_at else None,
            'model_used': model,
            'message': 'AI research notes generated and saved successfully'
        })
        
    except Exception as e:
        s.rollback()
        logger.error(f"Error generating AI research for {ticker}: {str(e)}")
        return jsonify({
            'error': f'Unexpected error: {str(e)}',
            'error_type': 'INTERNAL_ERROR'
        }), 500
    finally:
        s.close()


@app.route('/api/stock-notes/ai-research-claude/<ticker>', methods=['POST'])
def generate_ai_stock_research_claude(ticker):
    """
    Generate AI stock research notes using Claude API with web search.
    Appends the generated notes to existing notes and saves to DB.
    
    Request body (optional):
    {
        "months": 12  (default: 12 months of news)
    }
    
    Returns:
    {
        "ticker": "AAPL",
        "ai_notes": "Generated AI research notes...",
        "notes": "Full combined notes (existing + AI)",
        "updated_at": "2024-01-20T...",
        "model_used": "claude-web-search",
        "message": "success"
    }
    """
    s = Session()
    try:
        ticker = ticker.upper()
        data = request.get_json() or {}
        
        months = data.get('months', 12)
        
        # Get company name
        company_name = get_company_name_for_ticker(s, ticker)
        
        # Call Claude API with web search
        logger.info(f"Calling Claude API with web search for {ticker}")
        ai_response, error = call_claude_api_with_web_search(ticker, company_name, months)
        
        if error:
            if error == 'RATE_LIMITED':
                return jsonify({
                    'error': 'Claude API rate limited. Please try again in a few minutes.',
                    'error_type': 'RATE_LIMITED'
                }), 429
            else:
                return jsonify({
                    'error': f'Failed to generate AI research: {error}',
                    'error_type': 'API_ERROR'
                }), 500
        
        # Format AI notes with timestamp
        from datetime import datetime
        import pytz
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        ai_notes_formatted = f"\n\n---\n\n## AI Research (Claude Web Search) - {timestamp}\n\n{ai_response}"
        
        # Get existing notes
        existing = s.query(StockNotes).filter(StockNotes.ticker == ticker).first()
        current_notes = existing.notes if existing and existing.notes else ''
        
        # Combine notes
        combined_notes = current_notes + ai_notes_formatted
        
        # Save to database
        if existing:
            existing.notes = combined_notes
        else:
            new_note = StockNotes(ticker=ticker, notes=combined_notes)
            s.add(new_note)
        
        s.commit()
        
        # Export all notes to file for persistence
        export_all_notes_to_file()
        
        # Get updated timestamp
        updated_note = s.query(StockNotes).filter(StockNotes.ticker == ticker).first()
        
        return jsonify({
            'ticker': ticker,
            'ai_notes': ai_response,
            'notes': combined_notes,
            'updated_at': updated_note.updated_at.isoformat() if updated_note and updated_note.updated_at else None,
            'model_used': 'claude-web-search',
            'message': 'AI research notes generated and saved successfully'
        })
        
    except Exception as e:
        s.rollback()
        logger.error(f"Error generating Claude AI research for {ticker}: {str(e)}")
        return jsonify({
            'error': f'Unexpected error: {str(e)}',
            'error_type': 'INTERNAL_ERROR'
        }), 500
    finally:
        s.close()


@app.route('/api/stock-notes/ai-prompt', methods=['GET'])
def get_ai_prompt_template():
    """Get the current AI prompt template."""
    template = load_ai_prompt_template()
    if template:
        return jsonify({
            'prompt': template,
            'path': AI_PROMPT_FILE
        })
    return jsonify({
        'prompt': None,
        'error': 'Prompt template not found',
        'path': AI_PROMPT_FILE
    }), 404


@app.route('/api/stock-notes/ai-prompt', methods=['PUT'])
def update_ai_prompt_template():
    """Update the AI prompt template."""
    data = request.get_json()
    
    if not data or 'prompt' not in data:
        return jsonify({'error': 'prompt field is required'}), 400
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(AI_PROMPT_FILE), exist_ok=True)
        
        with open(AI_PROMPT_FILE, 'w') as f:
            f.write(data['prompt'])
        
        return jsonify({
            'message': 'Prompt template updated successfully',
            'path': AI_PROMPT_FILE
        })
    except Exception as e:
        logger.error(f"Error updating AI prompt template: {str(e)}")
        return jsonify({'error': str(e)}), 500


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


# ============================================================================
# Stock Detail Data Endpoints (for stock detail page)
# ============================================================================

@app.route('/api/earnings/<ticker>')
def get_earnings_data(ticker):
    """Get full earnings history for a specific ticker (last 12 quarters)."""
    s = Session()
    try:
        ticker = ticker.upper()
        
        earnings_data = s.query(Earnings).filter(
            Earnings.ticker == ticker
        ).order_by(desc(Earnings.date)).limit(12).all()
        
        results = []
        for row in earnings_data:
            # Calculate surprise if both actual and estimated exist
            eps_surprise = None
            eps_surprise_pct = None
            rev_surprise = None
            rev_surprise_pct = None
            
            if row.eps_actual is not None and row.eps_estimated is not None and row.eps_estimated != 0:
                eps_surprise = row.eps_actual - row.eps_estimated
                eps_surprise_pct = (eps_surprise / abs(row.eps_estimated)) * 100
            
            if row.revenue_actual is not None and row.revenue_estimated is not None and row.revenue_estimated != 0:
                rev_surprise = row.revenue_actual - row.revenue_estimated
                rev_surprise_pct = (rev_surprise / abs(row.revenue_estimated)) * 100
            
            results.append({
                'date': row.date.strftime('%Y-%m-%d') if row.date else None,
                'eps_actual': round(row.eps_actual, 2) if row.eps_actual is not None else None,
                'eps_estimated': round(row.eps_estimated, 2) if row.eps_estimated is not None else None,
                'eps_surprise': round(eps_surprise, 2) if eps_surprise is not None else None,
                'eps_surprise_pct': round(eps_surprise_pct, 1) if eps_surprise_pct is not None else None,
                'revenue_actual': row.revenue_actual,
                'revenue_estimated': row.revenue_estimated,
                'rev_surprise': rev_surprise,
                'rev_surprise_pct': round(rev_surprise_pct, 1) if rev_surprise_pct is not None else None
            })
        
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Error getting earnings data for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/analyst-estimates/<ticker>')
def get_analyst_estimates_data(ticker):
    """Get analyst estimates for a specific ticker (future years)."""
    s = Session()
    try:
        ticker = ticker.upper()
        
        # Get estimates - typically annual estimates for upcoming years
        estimates_data = s.query(AnalystEstimates).filter(
            AnalystEstimates.ticker == ticker
        ).order_by(AnalystEstimates.date.asc()).all()
        
        results = []
        for row in estimates_data:
            results.append({
                'date': row.date.strftime('%Y-%m-%d') if row.date else None,
                'revenue_avg': row.revenue_avg,
                'revenue_low': row.revenue_low,
                'revenue_high': row.revenue_high,
                'ebitda_avg': row.ebitda_avg,
                'ebit_avg': row.ebit_avg,
                'net_income_avg': row.net_income_avg,
                'eps_avg': round(row.eps_avg, 2) if row.eps_avg is not None else None,
                'eps_low': round(row.eps_low, 2) if row.eps_low is not None else None,
                'eps_high': round(row.eps_high, 2) if row.eps_high is not None else None,
                'num_analysts_revenue': row.num_analysts_revenue,
                'num_analysts_eps': row.num_analysts_eps
            })
        
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Error getting analyst estimates for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/ratios-ttm/<ticker>')
def get_ratios_ttm_data(ticker):
    """Get TTM ratios for a specific ticker."""
    s = Session()
    try:
        ticker = ticker.upper()
        
        ratios = s.query(RatiosTTM).filter(RatiosTTM.ticker == ticker).first()
        
        if not ratios:
            return jsonify({'error': 'Ratios not found for ticker'}), 404
        
        result = {
            # Profitability
            'gross_profit_margin': round(ratios.gross_profit_margin * 100, 1) if ratios.gross_profit_margin is not None else None,
            'operating_profit_margin': round(ratios.operating_profit_margin * 100, 1) if ratios.operating_profit_margin is not None else None,
            'net_profit_margin': round(ratios.net_profit_margin * 100, 1) if ratios.net_profit_margin is not None else None,
            # Valuation
            'pe_ratio': round(ratios.pe_ratio, 1) if ratios.pe_ratio is not None else None,
            'peg_ratio': round(ratios.peg_ratio, 2) if ratios.peg_ratio is not None else None,
            'price_to_book': round(ratios.price_to_book, 2) if ratios.price_to_book is not None else None,
            'price_to_sales': round(ratios.price_to_sales, 2) if ratios.price_to_sales is not None else None,
            'price_to_free_cash_flow': round(ratios.price_to_free_cash_flow, 1) if ratios.price_to_free_cash_flow is not None else None,
            # Liquidity
            'current_ratio': round(ratios.current_ratio, 2) if ratios.current_ratio is not None else None,
            'quick_ratio': round(ratios.quick_ratio, 2) if ratios.quick_ratio is not None else None,
            'cash_ratio': round(ratios.cash_ratio, 2) if ratios.cash_ratio is not None else None,
            # Leverage
            'debt_to_equity': round(ratios.debt_to_equity, 2) if ratios.debt_to_equity is not None else None,
            'debt_to_assets': round(ratios.debt_to_assets, 2) if ratios.debt_to_assets is not None else None,
            # Efficiency
            'asset_turnover': round(ratios.asset_turnover, 2) if ratios.asset_turnover is not None else None,
            'inventory_turnover': round(ratios.inventory_turnover, 2) if ratios.inventory_turnover is not None else None,
            'receivables_turnover': round(ratios.receivables_turnover, 2) if ratios.receivables_turnover is not None else None,
            # Returns
            'return_on_assets': round(ratios.return_on_assets * 100, 1) if ratios.return_on_assets is not None else None,
            'return_on_equity': round(ratios.return_on_equity * 100, 1) if ratios.return_on_equity is not None else None,
            'updated_at': ratios.updated_at.strftime('%Y-%m-%d %H:%M:%S') if ratios.updated_at else None
        }
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error getting ratios TTM for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/company-profile/<ticker>')
def get_company_profile_data(ticker):
    """Get company profile for a specific ticker."""
    s = Session()
    try:
        ticker = ticker.upper()
        
        profile = s.query(CompanyProfile).filter(CompanyProfile.ticker == ticker).first()
        
        if not profile:
            return jsonify({'error': 'Profile not found for ticker'}), 404
        
        result = {
            'ticker': profile.ticker,
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
            'vol_avg': profile.vol_avg,
            'last_div': round(profile.last_div, 2) if profile.last_div is not None else None,
            'range_52_week': profile.range_52_week,
            'is_adr': profile.is_adr,
            'updated_at': profile.updated_at.strftime('%Y-%m-%d %H:%M:%S') if profile.updated_at else None
        }
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error getting company profile for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


# ============================================================
# Stock News Endpoint (FMP + Yahoo RSS + Seeking Alpha RSS, merged)
# ============================================================

# A realistic UA helps with RSS feeds (esp. Seeking Alpha) that 403 default
# python-requests UAs.
_NEWS_UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
)


def _strip_html(text):
    """Crude HTML stripper for RSS description fields."""
    if not text:
        return ''
    import re
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _parse_rss_date(s):
    """Parse RFC 822 / ISO 8601 date strings; return ISO string or None."""
    if not s:
        return None
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(s)
        if dt is not None:
            return dt.isoformat()
    except Exception:
        pass
    try:
        from datetime import datetime as _dt
        return _dt.fromisoformat(s.replace('Z', '+00:00')).isoformat()
    except Exception:
        return None


def _fetch_fmp_news(ticker, limit, api_key):
    """Fetch news from Financial Modeling Prep."""
    if not api_key:
        return []
    try:
        import requests as req
        resp = req.get(
            'https://financialmodelingprep.com/stable/news/stock',
            params={'symbols': ticker, 'limit': limit, 'apikey': api_key},
            timeout=10,
        )
        resp.raise_for_status()
        articles = resp.json()
        if not isinstance(articles, list):
            return []
        results = []
        for a in articles:
            results.append({
                'title': a.get('title'),
                'url': a.get('url'),
                'published_date': a.get('publishedDate'),
                'site': a.get('site'),
                'text': a.get('text'),
                'image': a.get('image'),
                'symbol': a.get('symbol') or ticker,
                'source': 'FMP',
            })
        return results
    except Exception as e:
        logger.warning(f"FMP news fetch failed for {ticker}: {e}")
        return []


def _fetch_rss_items(url, source_name, ticker, limit):
    """Generic RSS fetcher for Yahoo + Seeking Alpha. Returns normalized items."""
    try:
        import requests as req
        import xml.etree.ElementTree as ET

        resp = req.get(
            url,
            headers={
                'User-Agent': _NEWS_UA,
                'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            },
            timeout=10,
        )
        resp.raise_for_status()
        # Some RSS endpoints return HTML on block; bail if not XML-ish.
        body = resp.content
        if not body or body.lstrip()[:1] not in (b'<',):
            return []

        try:
            root = ET.fromstring(body)
        except ET.ParseError as pe:
            logger.warning(f"{source_name} RSS parse error for {ticker}: {pe}")
            return []

        # Handle <rss><channel><item> (RSS 2.0) and <feed><entry> (Atom)
        channel = root.find('channel')
        items = channel.findall('item') if channel is not None else root.findall('{http://www.w3.org/2005/Atom}entry')

        results = []
        for item in items[:limit]:
            def _txt(tag):
                el = item.find(tag)
                if el is None:
                    el = item.find('{http://www.w3.org/2005/Atom}' + tag)
                return el.text.strip() if (el is not None and el.text) else None

            title = _txt('title')
            link = _txt('link')
            # Atom puts URL in href attribute
            if not link:
                link_el = item.find('{http://www.w3.org/2005/Atom}link')
                if link_el is not None:
                    link = link_el.get('href')
            pub = _txt('pubDate') or _txt('published') or _txt('updated')
            desc = _txt('description') or _txt('summary')

            # Try to get a thumbnail from <media:thumbnail> or <media:content>
            image = None
            for media_tag in (
                '{http://search.yahoo.com/mrss/}thumbnail',
                '{http://search.yahoo.com/mrss/}content',
            ):
                m = item.find(media_tag)
                if m is not None and m.get('url'):
                    image = m.get('url')
                    break

            if not title or not link:
                continue

            results.append({
                'title': title,
                'url': link,
                'published_date': _parse_rss_date(pub),
                'site': source_name,
                'text': _strip_html(desc),
                'image': image,
                'symbol': ticker,
                'source': source_name,
            })
        return results
    except Exception as e:
        logger.warning(f"{source_name} RSS fetch failed for {ticker}: {e}")
        return []


def _fetch_yahoo_news(ticker, limit):
    url = f'https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US'
    return _fetch_rss_items(url, 'Yahoo Finance', ticker, limit)


def _fetch_seekingalpha_news(ticker, limit):
    url = f'https://seekingalpha.com/api/sa/combined/{ticker}.xml'
    return _fetch_rss_items(url, 'Seeking Alpha', ticker, limit)


@app.route('/api/stock-news/<ticker>')
def get_stock_news(ticker):
    """Fetch latest news for a ticker, merged from FMP + Yahoo + Seeking Alpha."""
    ticker = ticker.upper()
    fmp_api_key = os.getenv('FMP_API_KEY')

    try:
        limit = request.args.get('limit', 20, type=int)
        # Per-source cap; we'll merge + dedupe + trim to `limit` after.
        per_source = max(limit, 20)

        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as ex:
            fut_fmp = ex.submit(_fetch_fmp_news, ticker, per_source, fmp_api_key)
            fut_yahoo = ex.submit(_fetch_yahoo_news, ticker, per_source)
            fut_sa = ex.submit(_fetch_seekingalpha_news, ticker, per_source)
            fmp_items = fut_fmp.result()
            yahoo_items = fut_yahoo.result()
            sa_items = fut_sa.result()

        # Merge + dedupe by URL (case-insensitive, ignore querystring fragments).
        seen = set()
        merged = []
        for batch in (sa_items, fmp_items, yahoo_items):  # SA first to keep their version on dedupe
            for art in batch:
                url = (art.get('url') or '').split('?')[0].strip().lower()
                if not url or url in seen:
                    continue
                seen.add(url)
                merged.append(art)

        # Sort newest-first by published_date (None goes last).
        def _sort_key(a):
            pd = a.get('published_date')
            return pd if pd else ''
        merged.sort(key=_sort_key, reverse=True)

        # Per-source counts (for UI badges/filters)
        counts = {
            'FMP': sum(1 for a in merged if a.get('source') == 'FMP'),
            'Yahoo Finance': sum(1 for a in merged if a.get('source') == 'Yahoo Finance'),
            'Seeking Alpha': sum(1 for a in merged if a.get('source') == 'Seeking Alpha'),
        }

        return jsonify({
            'ticker': ticker,
            'count': len(merged[:limit]),
            'total_fetched': len(merged),
            'source_counts': counts,
            'articles': merged[:limit],
        })

    except Exception as e:
        logger.error(f"Error fetching stock news for {ticker}: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ============================================================
# Abi General Notes API Endpoints (date-based personal notes)
# ============================================================

def export_all_abi_general_notes_to_file():
    """Export all abi general notes to JSON file for persistence across database resets."""
    s = Session()
    try:
        notes = s.query(AbiGeneralNotes).order_by(AbiGeneralNotes.note_date.desc(), AbiGeneralNotes.id.desc()).all()
        
        data = []
        for note in notes:
            data.append({
                'id': note.id,
                'note_date': note.note_date.strftime('%Y-%m-%d') if note.note_date else None,
                'title': note.title,
                'content': note.content,
                'tags': note.tags,
                'created_at': note.created_at.isoformat() if note.created_at else None,
                'updated_at': note.updated_at.isoformat() if note.updated_at else None
            })
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(ABI_GENERAL_NOTES_FILE), exist_ok=True)
        
        with open(ABI_GENERAL_NOTES_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Exported {len(data)} abi general notes to {ABI_GENERAL_NOTES_FILE}")
    except Exception as e:
        logger.error(f"Error exporting abi general notes: {str(e)}")
    finally:
        s.close()


@app.route('/api/abi-general-notes', methods=['GET'])
def get_abi_general_notes():
    """Get all abi general notes, optionally filtered by date range or search query."""
    s = Session()
    try:
        query = s.query(AbiGeneralNotes)
        
        # Optional date filtering
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        search = request.args.get('search')
        tag = request.args.get('tag')
        
        if start_date:
            query = query.filter(AbiGeneralNotes.note_date >= start_date)
        if end_date:
            query = query.filter(AbiGeneralNotes.note_date <= end_date)
        if search:
            search_term = f'%{search}%'
            query = query.filter(
                or_(
                    AbiGeneralNotes.title.ilike(search_term),
                    AbiGeneralNotes.content.ilike(search_term)
                )
            )
        if tag:
            query = query.filter(AbiGeneralNotes.tags.ilike(f'%{tag}%'))
        
        notes = query.order_by(AbiGeneralNotes.note_date.desc(), AbiGeneralNotes.id.desc()).all()
        
        results = []
        for note in notes:
            results.append({
                'id': note.id,
                'note_date': note.note_date.strftime('%Y-%m-%d') if note.note_date else None,
                'title': note.title,
                'content': note.content,
                'tags': note.tags,
                'created_at': note.created_at.isoformat() if note.created_at else None,
                'updated_at': note.updated_at.isoformat() if note.updated_at else None
            })
        
        return jsonify(results)
    finally:
        s.close()


@app.route('/api/abi-general-notes/<int:note_id>', methods=['GET'])
def get_abi_general_note(note_id):
    """Get a specific abi general note by ID."""
    s = Session()
    try:
        note = s.query(AbiGeneralNotes).filter(AbiGeneralNotes.id == note_id).first()
        
        if not note:
            return jsonify({'error': 'Note not found'}), 404
        
        return jsonify({
            'id': note.id,
            'note_date': note.note_date.strftime('%Y-%m-%d') if note.note_date else None,
            'title': note.title,
            'content': note.content,
            'tags': note.tags,
            'created_at': note.created_at.isoformat() if note.created_at else None,
            'updated_at': note.updated_at.isoformat() if note.updated_at else None
        })
    finally:
        s.close()


@app.route('/api/abi-general-notes', methods=['POST'])
def create_abi_general_note():
    """Create a new abi general note."""
    s = Session()
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'Request body is required'}), 400
        
        # note_date defaults to today if not provided
        note_date_str = data.get('note_date')
        if note_date_str:
            from datetime import datetime
            note_date = datetime.strptime(note_date_str, '%Y-%m-%d').date()
        else:
            note_date = date.today()
        
        new_note = AbiGeneralNotes(
            note_date=note_date,
            title=data.get('title'),
            content=data.get('content'),
            tags=data.get('tags')
        )
        
        s.add(new_note)
        s.commit()
        
        # Export all abi general notes to file for persistence
        export_all_abi_general_notes_to_file()
        
        return jsonify({
            'id': new_note.id,
            'note_date': new_note.note_date.strftime('%Y-%m-%d') if new_note.note_date else None,
            'title': new_note.title,
            'content': new_note.content,
            'tags': new_note.tags,
            'created_at': new_note.created_at.isoformat() if new_note.created_at else None,
            'updated_at': new_note.updated_at.isoformat() if new_note.updated_at else None,
            'message': 'Note created successfully'
        }), 201
    except Exception as e:
        s.rollback()
        logger.error(f"Error creating abi general note: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/abi-general-notes/<int:note_id>', methods=['PUT'])
def update_abi_general_note(note_id):
    """Update an existing abi general note."""
    s = Session()
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'Request body is required'}), 400
        
        note = s.query(AbiGeneralNotes).filter(AbiGeneralNotes.id == note_id).first()
        
        if not note:
            return jsonify({'error': 'Note not found'}), 404
        
        # Update fields if provided
        if 'note_date' in data:
            from datetime import datetime
            note.note_date = datetime.strptime(data['note_date'], '%Y-%m-%d').date()
        if 'title' in data:
            note.title = data['title']
        if 'content' in data:
            note.content = data['content']
        if 'tags' in data:
            note.tags = data['tags']
        
        s.commit()
        
        # Export all abi general notes to file for persistence
        export_all_abi_general_notes_to_file()
        
        return jsonify({
            'id': note.id,
            'note_date': note.note_date.strftime('%Y-%m-%d') if note.note_date else None,
            'title': note.title,
            'content': note.content,
            'tags': note.tags,
            'created_at': note.created_at.isoformat() if note.created_at else None,
            'updated_at': note.updated_at.isoformat() if note.updated_at else None,
            'message': 'Note updated successfully'
        })
    except Exception as e:
        s.rollback()
        logger.error(f"Error updating abi general note {note_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/abi-general-notes/<int:note_id>', methods=['DELETE'])
def delete_abi_general_note(note_id):
    """Delete an abi general note."""
    s = Session()
    try:
        note = s.query(AbiGeneralNotes).filter(AbiGeneralNotes.id == note_id).first()
        
        if not note:
            return jsonify({'error': 'Note not found'}), 404
        
        s.delete(note)
        s.commit()
        
        # Export all abi general notes to file for persistence
        export_all_abi_general_notes_to_file()
        
        return jsonify({'message': 'Note deleted successfully', 'id': note_id})
    except Exception as e:
        s.rollback()
        logger.error(f"Error deleting abi general note {note_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/abi-general-notes/tags', methods=['GET'])
def get_abi_general_notes_tags():
    """Get all unique tags used in abi general notes."""
    s = Session()
    try:
        notes = s.query(AbiGeneralNotes.tags).filter(AbiGeneralNotes.tags.isnot(None), AbiGeneralNotes.tags != '').all()
        
        # Extract unique tags
        all_tags = set()
        for (tags,) in notes:
            if tags:
                for tag in tags.split(','):
                    tag = tag.strip()
                    if tag:
                        all_tags.add(tag)
        
        return jsonify({'tags': sorted(list(all_tags))})
    finally:
        s.close()


# ============================================================================
# Sector/Index Performance Endpoint
# ============================================================================

# ETF metadata for display names and categorization
ETF_METADATA = {
    # Main Indices (Row 1 - will be shown in top section)
    'SPY': {'name': 'S&P 500', 'category': 'main'},
    'QQQ': {'name': 'Nasdaq 100', 'category': 'main'},
    'IWM': {'name': 'Russell 2000', 'category': 'main'},
    '^VIX': {'name': 'VIX', 'category': 'main'},
    # Commodities / Rates (Row 2)
    'GLD': {'name': 'Gold', 'category': 'commodity'},
    'SLV': {'name': 'Silver', 'category': 'commodity'},
    'USO': {'name': 'Oil', 'category': 'commodity'},
    'TLT': {'name': '20Y Treasury', 'category': 'commodity'},
    # Sector ETFs - Risk On
    'XLB': {'name': 'Materials', 'category': 'risk_on'},
    'XLC': {'name': 'Communication Services', 'category': 'risk_on'},
    'XLY': {'name': 'Consumer Discretionary', 'category': 'risk_on'},
    'XLE': {'name': 'Energy', 'category': 'risk_on'},
    'XLF': {'name': 'Financials', 'category': 'risk_on'},
    'XLV': {'name': 'Health Care', 'category': 'risk_on'},
    'XLI': {'name': 'Industrials', 'category': 'risk_on'},
    'XLK': {'name': 'Technology', 'category': 'risk_on'},
    # Sector ETFs - Risk Off
    'XLP': {'name': 'Consumer Staples', 'category': 'risk_off'},
    'XLRE': {'name': 'Real Estate', 'category': 'risk_off'},
    'XLU': {'name': 'Utilities', 'category': 'risk_off'},
    # Tech / Growth
    'IGM': {'name': 'Tech Expanded', 'category': 'tech'},
    'SOXX': {'name': 'Semiconductors', 'category': 'tech'},
    'IGV': {'name': 'Software', 'category': 'tech'},
    'ARTY': {'name': 'AI & Robotics', 'category': 'tech'},
    'BAI': {'name': 'AI Infrastructure', 'category': 'tech'},
    'IDGT': {'name': 'Digital Infrastructure', 'category': 'tech'},
    'ILIT': {'name': 'Lithium & Battery', 'category': 'tech'},
    # Healthcare
    'IBB': {'name': 'Biotech', 'category': 'healthcare'},
    'IHF': {'name': 'Healthcare Providers', 'category': 'healthcare'},
    'IHI': {'name': 'Medical Devices', 'category': 'healthcare'},
    'IHE': {'name': 'Pharmaceuticals', 'category': 'healthcare'},
    'IDNA': {'name': 'Genomics', 'category': 'healthcare'},
    # Energy / Materials
    'IEZ': {'name': 'Oil Equipment', 'category': 'energy'},
    'IEO': {'name': 'Oil Exploration', 'category': 'energy'},
    'FILL': {'name': 'Global Energy', 'category': 'energy'},
    'ICOP': {'name': 'Copper Miners', 'category': 'materials'},
    'RING': {'name': 'Gold Miners', 'category': 'materials'},
    'PICK': {'name': 'Metals & Mining', 'category': 'materials'},
    'SLVP': {'name': 'Silver Miners', 'category': 'materials'},
    'WOOD': {'name': 'Timber & Forestry', 'category': 'materials'},
    # Industrials / Defense
    'ITA': {'name': 'Aerospace & Defense', 'category': 'industrials'},
    'IYT': {'name': 'Transportation', 'category': 'industrials'},
    # Financials
    'IAI': {'name': 'Broker-Dealers', 'category': 'financials'},
    'IYG': {'name': 'Financial Services', 'category': 'financials'},
    'IAK': {'name': 'Insurance', 'category': 'financials'},
    'IAT': {'name': 'Regional Banks', 'category': 'financials'},
    # Real Estate
    'REM': {'name': 'Mortgage REITs', 'category': 'realestate'},
    'REZ': {'name': 'Residential REITs', 'category': 'realestate'},
    # Construction
    'ITB': {'name': 'Home Construction', 'category': 'construction'},
}


def get_sector_performance_data(connection):
    """
    Calculate 1D, 5D, 20D, 60D performance for all ETFs/indices from index_prices table.
    """
    try:
        query = """
        WITH trading_dates AS (
            SELECT DISTINCT date
            FROM index_prices
            ORDER BY date DESC
            LIMIT 61
        ),
        latest_date AS (
            SELECT MAX(date) as max_date FROM trading_dates
        ),
        date_1d_ago AS (
            SELECT date as d FROM trading_dates ORDER BY date DESC LIMIT 1 OFFSET 1
        ),
        date_5d_ago AS (
            SELECT date as d FROM trading_dates ORDER BY date DESC LIMIT 1 OFFSET 5
        ),
        date_20d_ago AS (
            SELECT date as d FROM trading_dates ORDER BY date DESC LIMIT 1 OFFSET 20
        ),
        date_60d_ago AS (
            SELECT date as d FROM trading_dates ORDER BY date DESC LIMIT 1 OFFSET 60
        )
        SELECT 
            ip_today.symbol,
            ip_today.close_price as current_price,
            ip_1d.close_price as price_1d_ago,
            ip_5d.close_price as price_5d_ago,
            ip_20d.close_price as price_20d_ago,
            ip_60d.close_price as price_60d_ago,
            CASE WHEN ip_1d.close_price > 0 THEN 
                ((ip_today.close_price - ip_1d.close_price) / ip_1d.close_price * 100) 
            END as dr_1,
            CASE WHEN ip_5d.close_price > 0 THEN 
                ((ip_today.close_price - ip_5d.close_price) / ip_5d.close_price * 100) 
            END as dr_5,
            CASE WHEN ip_20d.close_price > 0 THEN 
                ((ip_today.close_price - ip_20d.close_price) / ip_20d.close_price * 100) 
            END as dr_20,
            CASE WHEN ip_60d.close_price > 0 THEN 
                ((ip_today.close_price - ip_60d.close_price) / ip_60d.close_price * 100) 
            END as dr_60,
            ld.max_date as latest_date
        FROM index_prices ip_today
        CROSS JOIN latest_date ld
        LEFT JOIN index_prices ip_1d 
            ON ip_today.symbol = ip_1d.symbol 
            AND ip_1d.date = (SELECT d FROM date_1d_ago)
        LEFT JOIN index_prices ip_5d 
            ON ip_today.symbol = ip_5d.symbol 
            AND ip_5d.date = (SELECT d FROM date_5d_ago)
        LEFT JOIN index_prices ip_20d 
            ON ip_today.symbol = ip_20d.symbol 
            AND ip_20d.date = (SELECT d FROM date_20d_ago)
        LEFT JOIN index_prices ip_60d 
            ON ip_today.symbol = ip_60d.symbol 
            AND ip_60d.date = (SELECT d FROM date_60d_ago)
        WHERE ip_today.date = ld.max_date
        ORDER BY ip_today.symbol
        """
        
        result = connection.execute(text(query))
        rows = result.fetchall()
        
        main_indices = []
        sectors = []
        latest_date = None
        
        for row in rows:
            symbol = row[0]
            current_price = float(row[1]) if row[1] else None
            dr_1 = round(float(row[6]), 2) if row[6] is not None else None
            dr_5 = round(float(row[7]), 2) if row[7] is not None else None
            dr_20 = round(float(row[8]), 2) if row[8] is not None else None
            dr_60 = round(float(row[9]), 2) if row[9] is not None else None
            latest_date = row[10].strftime('%Y-%m-%d') if row[10] else None
            
            metadata = ETF_METADATA.get(symbol, {'name': symbol, 'category': 'other'})
            
            entry = {
                'symbol': symbol,
                'name': metadata['name'],
                'category': metadata['category'],
                'current_price': round(current_price, 2) if current_price else None,
                'dr_1': dr_1,
                'dr_5': dr_5,
                'dr_20': dr_20,
                'dr_60': dr_60,
            }
            
            if metadata['category'] == 'main':
                main_indices.append(entry)
            else:
                sectors.append(entry)
        
        # Sort sectors by 1D performance descending
        sectors.sort(key=lambda x: x['dr_1'] if x['dr_1'] is not None else -999, reverse=True)
        
        return {
            'latest_date': latest_date,
            'main_indices': main_indices,
            'sectors': sectors
        }
    
    except Exception as e:
        logger.error(f"Error getting sector performance data: {str(e)}")
        return {'main_indices': [], 'sectors': [], 'latest_date': None}


@app.route('/api/sector-performance')
def get_sector_performance():
    """Get performance data for all indices/ETFs."""
    s = Session()
    try:
        with s.get_bind().connect() as conn:
            return jsonify(get_sector_performance_data(conn))
    except Exception as e:
        logger.error(f"Error in sector performance endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


def get_homepage_data(connection):
    """
    Get homepage data: main indices, commodities, and sector ETFs with DMA calculations.
    """
    try:
        query = """
        WITH trading_dates AS (
            SELECT DISTINCT date
            FROM index_prices
            ORDER BY date DESC
            LIMIT 201
        ),
        latest_date AS (
            SELECT MAX(date) as max_date FROM trading_dates
        ),
        date_1d_ago AS (
            SELECT date as d FROM trading_dates ORDER BY date DESC LIMIT 1 OFFSET 1
        ),
        date_5d_ago AS (
            SELECT date as d FROM trading_dates ORDER BY date DESC LIMIT 1 OFFSET 5
        ),
        date_20d_ago AS (
            SELECT date as d FROM trading_dates ORDER BY date DESC LIMIT 1 OFFSET 20
        ),
        date_60d_ago AS (
            SELECT date as d FROM trading_dates ORDER BY date DESC LIMIT 1 OFFSET 60
        ),
        sma_50 AS (
            SELECT 
                ip.symbol,
                AVG(ip.close_price) as sma_50
            FROM index_prices ip
            WHERE ip.date IN (SELECT date FROM trading_dates ORDER BY date DESC LIMIT 50)
            GROUP BY ip.symbol
        ),
        sma_200 AS (
            SELECT 
                ip.symbol,
                AVG(ip.close_price) as sma_200
            FROM index_prices ip
            WHERE ip.date IN (SELECT date FROM trading_dates ORDER BY date DESC LIMIT 200)
            GROUP BY ip.symbol
        )
        SELECT 
            ip_today.symbol,
            ip_today.close_price as current_price,
            CASE WHEN ip_1d.close_price > 0 THEN 
                ((ip_today.close_price - ip_1d.close_price) / ip_1d.close_price * 100) 
            END as dr_1,
            CASE WHEN ip_5d.close_price > 0 THEN 
                ((ip_today.close_price - ip_5d.close_price) / ip_5d.close_price * 100) 
            END as dr_5,
            CASE WHEN ip_20d.close_price > 0 THEN 
                ((ip_today.close_price - ip_20d.close_price) / ip_20d.close_price * 100) 
            END as dr_20,
            CASE WHEN ip_60d.close_price > 0 THEN 
                ((ip_today.close_price - ip_60d.close_price) / ip_60d.close_price * 100) 
            END as dr_60,
            s50.sma_50,
            s200.sma_200,
            CASE WHEN s50.sma_50 > 0 THEN 
                ((ip_today.close_price - s50.sma_50) / s50.sma_50 * 100) 
            END as pct_from_50dma,
            CASE WHEN s200.sma_200 > 0 THEN 
                ((ip_today.close_price - s200.sma_200) / s200.sma_200 * 100) 
            END as pct_from_200dma,
            ld.max_date as latest_date
        FROM index_prices ip_today
        CROSS JOIN latest_date ld
        LEFT JOIN index_prices ip_1d 
            ON ip_today.symbol = ip_1d.symbol 
            AND ip_1d.date = (SELECT d FROM date_1d_ago)
        LEFT JOIN index_prices ip_5d 
            ON ip_today.symbol = ip_5d.symbol 
            AND ip_5d.date = (SELECT d FROM date_5d_ago)
        LEFT JOIN index_prices ip_20d 
            ON ip_today.symbol = ip_20d.symbol 
            AND ip_20d.date = (SELECT d FROM date_20d_ago)
        LEFT JOIN index_prices ip_60d 
            ON ip_today.symbol = ip_60d.symbol 
            AND ip_60d.date = (SELECT d FROM date_60d_ago)
        LEFT JOIN sma_50 s50 ON ip_today.symbol = s50.symbol
        LEFT JOIN sma_200 s200 ON ip_today.symbol = s200.symbol
        WHERE ip_today.date = ld.max_date
        ORDER BY ip_today.symbol
        """
        
        result = connection.execute(text(query))
        rows = result.fetchall()
        
        main_indices = []
        commodities = []
        risk_on_sectors = []
        risk_off_sectors = []
        latest_date = None
        
        for row in rows:
            symbol = row[0]
            current_price = float(row[1]) if row[1] else None
            dr_1 = round(float(row[2]), 2) if row[2] is not None else None
            dr_5 = round(float(row[3]), 2) if row[3] is not None else None
            dr_20 = round(float(row[4]), 2) if row[4] is not None else None
            dr_60 = round(float(row[5]), 2) if row[5] is not None else None
            sma_50 = round(float(row[6]), 2) if row[6] is not None else None
            sma_200 = round(float(row[7]), 2) if row[7] is not None else None
            pct_from_50dma = round(float(row[8]), 2) if row[8] is not None else None
            pct_from_200dma = round(float(row[9]), 2) if row[9] is not None else None
            latest_date = row[10].strftime('%Y-%m-%d') if row[10] else None
            
            metadata = ETF_METADATA.get(symbol, {'name': symbol, 'category': 'other'})
            
            entry = {
                'symbol': symbol,
                'name': metadata['name'],
                'category': metadata['category'],
                'current_price': round(current_price, 2) if current_price else None,
                'dr_1': dr_1,
                'dr_5': dr_5,
                'dr_20': dr_20,
                'dr_60': dr_60,
                'sma_50': sma_50,
                'sma_200': sma_200,
                'pct_from_50dma': pct_from_50dma,
                'pct_from_200dma': pct_from_200dma,
            }
            
            if metadata['category'] == 'main':
                main_indices.append(entry)
            elif metadata['category'] == 'commodity':
                commodities.append(entry)
            elif metadata['category'] == 'risk_on':
                risk_on_sectors.append(entry)
            elif metadata['category'] == 'risk_off':
                risk_off_sectors.append(entry)
        
        # Sort by symbol for consistent ordering
        main_indices_order = ['SPY', 'QQQ', 'IWM', '^VIX']
        main_indices.sort(key=lambda x: main_indices_order.index(x['symbol']) if x['symbol'] in main_indices_order else 99)
        
        commodities_order = ['GLD', 'SLV', 'USO', 'TLT']
        commodities.sort(key=lambda x: commodities_order.index(x['symbol']) if x['symbol'] in commodities_order else 99)
        
        # Sort sectors by 1D performance descending
        risk_on_sectors.sort(key=lambda x: x['dr_1'] if x['dr_1'] is not None else -999, reverse=True)
        risk_off_sectors.sort(key=lambda x: x['dr_1'] if x['dr_1'] is not None else -999, reverse=True)
        
        return {
            'latest_date': latest_date,
            'main_indices': main_indices,
            'commodities': commodities,
            'risk_on_sectors': risk_on_sectors,
            'risk_off_sectors': risk_off_sectors,
        }
    
    except Exception as e:
        logger.error(f"Error getting homepage data: {str(e)}")
        return {'main_indices': [], 'commodities': [], 'risk_on_sectors': [], 'risk_off_sectors': [], 'latest_date': None}


@app.route('/api/homepage')
def get_homepage():
    """Get homepage data: main indices, commodities, and sector ETFs with DMA calculations."""
    s = Session()
    try:
        with s.get_bind().connect() as conn:
            return jsonify(get_homepage_data(conn))
    except Exception as e:
        logger.error(f"Error in homepage endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/market-breadth')
def get_market_breadth():
    """Get market breadth data for the last year (for charting)."""
    s = Session()
    try:
        from datetime import timedelta
        
        # Get latest date
        latest_date = s.query(func.max(MarketBreadth.date)).scalar()
        if not latest_date:
            return jsonify({'error': 'No market breadth data available'}), 404
        
        # Get 1 year of data
        start_date = latest_date - timedelta(days=365)
        
        breadth_data = s.query(
            MarketBreadth.date,
            MarketBreadth.above_50dma,
            MarketBreadth.below_50dma,
            MarketBreadth.above_200dma,
            MarketBreadth.below_200dma,
            MarketBreadth.up_4pct,
            MarketBreadth.down_4pct,
            MarketBreadth.total_stocks
        ).filter(
            MarketBreadth.date >= start_date,
            MarketBreadth.date <= latest_date
        ).order_by(MarketBreadth.date.asc()).all()
        
        if not breadth_data:
            return jsonify({'error': 'No market breadth data found'}), 404
        
        results = []
        for row in breadth_data:
            total = row.total_stocks or 1
            results.append({
                'date': row.date.strftime('%Y-%m-%d'),
                'above_50dma': row.above_50dma,
                'below_50dma': row.below_50dma,
                'above_200dma': row.above_200dma,
                'below_200dma': row.below_200dma,
                'up_4pct': row.up_4pct,
                'down_4pct': row.down_4pct,
                'total_stocks': row.total_stocks,
                'pct_above_50dma': round((row.above_50dma / total) * 100, 1) if row.above_50dma else 0,
                'pct_above_200dma': round((row.above_200dma / total) * 100, 1) if row.above_200dma else 0,
            })
        
        # Get latest values for display
        latest = results[-1] if results else {}
        
        return jsonify({
            'latest_date': latest_date.strftime('%Y-%m-%d'),
            'latest': latest,
            'history': results
        })
    
    except Exception as e:
        logger.error(f"Error getting market breadth data: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/index-ohlc/<symbol>')
def get_index_ohlc_data(symbol):
    """Get OHLC data for a specific index/ETF from index_prices table (for charts)."""
    s = Session()
    try:
        symbol = symbol.upper()
        
        from datetime import timedelta
        
        # Get latest date from index_prices
        latest_date = s.query(func.max(IndexPrice.date)).scalar()
        if not latest_date:
            return jsonify({'error': 'No index price data available'}), 404
        
        # Fetch 800 calendar days to ensure 200 SMA has values for entire 1Y view
        # (365 days visible + 200 days for SMA calculation + buffer for weekends/holidays)
        start_date = latest_date - timedelta(days=800)
        
        # Get OHLC data
        ohlc_data = s.query(
            IndexPrice.date,
            IndexPrice.open_price,
            IndexPrice.high_price,
            IndexPrice.low_price,
            IndexPrice.close_price,
            IndexPrice.volume
        ).filter(
            IndexPrice.symbol == symbol,
            IndexPrice.date >= start_date,
            IndexPrice.date <= latest_date
        ).order_by(IndexPrice.date.asc()).all()
        
        if not ohlc_data:
            return jsonify({'error': f'No OHLC data found for {symbol}'}), 404
        
        results = []
        for row in ohlc_data:
            results.append({
                'time': row.date.strftime('%Y-%m-%d'),
                'open': round(row.open_price, 2) if row.open_price else None,
                'high': round(row.high_price, 2) if row.high_price else None,
                'low': round(row.low_price, 2) if row.low_price else None,
                'close': round(row.close_price, 2) if row.close_price else None,
                'volume': row.volume,
            })
        
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Error getting index OHLC data for {symbol}: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


# ============================================================================
# RS Screener Endpoints (multi-timeframe relative strength)
# ============================================================================

@app.route('/api/rs-screener')
@app.route('/api/rs-screener/<market_cap>')
def get_rs_screener(market_cap=None):
    """Get RS screener data, optionally filtered by market cap category."""
    s = Session()
    try:
        query = s.query(RsScreener).filter(
            RsScreener.market_cap.isnot(None),
            RsScreener.rs_2d_rank.isnot(None),
        )

        if market_cap and market_cap.lower() != 'all':
            category = MARKET_CAP_CATEGORIES.get(market_cap.lower())
            if category:
                query = query.filter(RsScreener.market_cap >= category['min'])
                if category['max'] is not None:
                    query = query.filter(RsScreener.market_cap < category['max'])

        # Apply liquidity filters using stock_metrics join
        avg_vol_min = GLOBAL_LIQUIDITY_FILTERS.get('avg_vol_10d_min')
        dollar_vol_min = GLOBAL_LIQUIDITY_FILTERS.get('dollar_volume_min')
        price_min = GLOBAL_LIQUIDITY_FILTERS.get('price_min')

        if avg_vol_min or dollar_vol_min or price_min:
            query = query.join(StockMetrics, RsScreener.ticker == StockMetrics.ticker)
            if avg_vol_min:
                query = query.filter(StockMetrics.avg_vol_10d >= avg_vol_min)
            if dollar_vol_min:
                query = query.filter(StockMetrics.dollar_volume >= dollar_vol_min)
            if price_min:
                query = query.filter(StockMetrics.current_price >= price_min)

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
                'rs_2d': float(stock.rs_2d) if stock.rs_2d is not None else None,
                'rs_5d': float(stock.rs_5d) if stock.rs_5d is not None else None,
                'rs_10d': float(stock.rs_10d) if stock.rs_10d is not None else None,
                'rs_20d': float(stock.rs_20d) if stock.rs_20d is not None else None,
                'rs_60d': float(stock.rs_60d) if stock.rs_60d is not None else None,
                'rs_2d_rank': stock.rs_2d_rank,
                'rs_5d_rank': stock.rs_5d_rank,
                'rs_10d_rank': stock.rs_10d_rank,
                'rs_20d_rank': stock.rs_20d_rank,
                'rs_60d_rank': stock.rs_60d_rank,
            })

        return jsonify(results)

    except Exception as e:
        logger.error(f"Error getting RS screener data: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


# ============================================================================
# Market Indicators (VIX, Treasury Yield)
# ============================================================================

@app.route('/api/vix-latest')
def get_vix_latest():
    """Get latest VIX close price from index_prices table."""
    s = Session()
    try:
        row = s.query(
            IndexPrice.date,
            IndexPrice.close_price,
            IndexPrice.open_price
        ).filter(
            IndexPrice.symbol == '^VIX'
        ).order_by(IndexPrice.date.desc()).first()

        if not row:
            return jsonify({'error': 'No VIX data available'}), 404

        prev = s.query(IndexPrice.close_price).filter(
            IndexPrice.symbol == '^VIX',
            IndexPrice.date < row.date
        ).order_by(IndexPrice.date.desc()).first()

        prev_close = prev.close_price if prev else None
        change = round(row.close_price - prev_close, 2) if prev_close else None
        change_pct = round((row.close_price - prev_close) / prev_close * 100, 2) if prev_close else None

        return jsonify({
            'symbol': 'VIX',
            'date': row.date.strftime('%Y-%m-%d'),
            'close': round(row.close_price, 2),
            'change': change,
            'change_pct': change_pct,
        })
    except Exception as e:
        logger.error(f"Error getting VIX latest: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        s.close()


@app.route('/api/treasury-10y')
def get_treasury_10y():
    """Scrape current 10-Year Treasury yield from CNBC."""
    import urllib.request
    import re

    try:
        url = 'https://www.cnbc.com/quotes/US10Y'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            html = resp.read().decode('utf-8', errors='ignore')

        match = re.search(r'"structured-data-module"[^>]*>.*?"price":\s*"([\d.]+)"', html, re.DOTALL)
        if not match:
            match = re.search(r'class="QuoteStrip-lastPrice[^"]*"[^>]*>([\d.]+)%?<', html)
        if not match:
            match = re.search(r'"last(?:Price)?"\s*:\s*"?([\d.]+)"?', html)

        if match:
            value = float(match.group(1))
            return jsonify({'symbol': 'US10Y', 'yield_pct': value})

        return jsonify({'error': 'Could not parse yield'}), 502
    except Exception as e:
        logger.error(f"Error scraping 10Y yield: {str(e)}")
        return jsonify({'error': str(e)}), 502


# ============================================================================
# Abi Ticker Notes Endpoints (JSON file-based per-ticker notes)
# ============================================================================
# Abi ticker notes are decoupled from watchlist / dislike membership. The daily
# screener still only consumes notes whose ticker is on the watchlist (see
# daily_screener stages s3/s4/s5).


def _load_abi_ticker_notes():
    """Load Abi ticker notes from JSON. Returns {TICKER: {notes, created_at, updated_at}}."""
    if os.path.exists(ABI_TICKER_NOTES_FILE):
        try:
            with open(ABI_TICKER_NOTES_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_abi_ticker_notes(data):
    """Save Abi ticker notes to JSON file."""
    os.makedirs(os.path.dirname(ABI_TICKER_NOTES_FILE), exist_ok=True)
    with open(ABI_TICKER_NOTES_FILE, 'w') as f:
        json.dump(data, f, indent=2)


@app.route('/api/abi-ticker-notes', methods=['GET'])
def get_abi_ticker_notes_all():
    """Get all Abi ticker notes: {TICKER: {notes, created_at, updated_at}, ...}."""
    return jsonify(_load_abi_ticker_notes())


@app.route('/api/abi-ticker-notes/<ticker>', methods=['GET'])
def get_abi_ticker_note(ticker):
    """Get Abi ticker notes for a single ticker."""
    ticker = ticker.upper()
    comments = _load_abi_ticker_notes()
    if ticker not in comments:
        return jsonify({'ticker': ticker, 'notes': '', 'has_ticker_note': False})
    return jsonify({
        'ticker': ticker,
        'notes': comments[ticker].get('notes', ''),
        'created_at': comments[ticker].get('created_at'),
        'updated_at': comments[ticker].get('updated_at'),
        'has_ticker_note': True,
    })


@app.route('/api/abi-ticker-notes/<ticker>', methods=['PUT'])
def upsert_abi_ticker_note(ticker):
    """Create or update Abi ticker notes for a ticker. Body: {notes}.

    Empty notes clears / removes the entry entirely (PUT '' is equivalent to DELETE).
    """
    ticker = ticker.upper()
    data = request.get_json() or {}
    if 'notes' not in data:
        return jsonify({'error': 'notes field is required'}), 400

    notes = (data.get('notes') or '').rstrip()
    comments = _load_abi_ticker_notes()
    now_iso = datetime.now(pytz.timezone("US/Eastern")).isoformat()

    if not notes:
        if ticker in comments:
            del comments[ticker]
            _save_abi_ticker_notes(comments)
        return jsonify({'ticker': ticker, 'notes': '', 'status': 'cleared'})

    if ticker in comments:
        comments[ticker]['notes'] = notes
        comments[ticker]['updated_at'] = now_iso
        comments[ticker].setdefault('created_at', now_iso)
        status = 'updated'
    else:
        comments[ticker] = {
            'notes': notes,
            'created_at': now_iso,
            'updated_at': now_iso,
        }
        status = 'created'

    _save_abi_ticker_notes(comments)
    return jsonify({
        'ticker': ticker,
        'notes': comments[ticker]['notes'],
        'created_at': comments[ticker]['created_at'],
        'updated_at': comments[ticker]['updated_at'],
        'status': status,
    })


@app.route('/api/abi-ticker-notes/<ticker>', methods=['DELETE'])
def delete_abi_ticker_note(ticker):
    """Delete Abi ticker notes for a ticker."""
    ticker = ticker.upper()
    comments = _load_abi_ticker_notes()
    if ticker in comments:
        del comments[ticker]
        _save_abi_ticker_notes(comments)
    return jsonify({'ticker': ticker, 'status': 'removed'})


@app.route('/api/abi-ticker-notes/batch-check', methods=['POST'])
def batch_check_abi_ticker_notes():
    """Return {TICKER: {notes, created_at, updated_at}, ...} for tickers that have notes."""
    data = request.get_json()
    if not data or 'tickers' not in data:
        return jsonify({'error': 'tickers field is required'}), 400

    comments = _load_abi_ticker_notes()
    result = {}
    for t in data['tickers']:
        t_upper = t.upper()
        if t_upper in comments:
            result[t_upper] = comments[t_upper]
    return jsonify(result)


# ============================================================================
# Abi Watchlist Endpoints (JSON file-based personal watchlist)
# ============================================================================
# The watchlist tracks list membership + star ratings. Per-ticker Abi ticker
# notes were moved to abi_ticker_notes.json (decoupled from membership). The
# response shape still includes a `notes` field, populated from that file, so
# existing UI code that reads `wl[ticker].notes` keeps working.

def _load_watchlist():
    """Load watchlist from JSON (membership, stars, timestamps). Ignores any
    embedded ``notes`` keys in the file — Abi ticker notes live only in
    abi_ticker_notes.json.
    """
    if os.path.exists(ABI_WATCHLIST_FILE):
        try:
            with open(ABI_WATCHLIST_FILE, 'r') as f:
                data = json.load(f)
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}
        for entry in data.values():
            if isinstance(entry, dict) and 'notes' in entry:
                del entry['notes']
        return data
    return {}


def _save_watchlist(data):
    """Save watchlist to JSON file."""
    os.makedirs(os.path.dirname(ABI_WATCHLIST_FILE), exist_ok=True)
    with open(ABI_WATCHLIST_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def _watchlist_with_comments():
    """Return watchlist data merged with Abi ticker notes: each entry has
    ``notes`` from abi_ticker_notes.json (or '').
    """
    wl = _load_watchlist()
    comments = _load_abi_ticker_notes()
    out = {}
    for tk, entry in wl.items():
        if not isinstance(entry, dict):
            continue
        merged = {k: v for k, v in entry.items() if k != 'notes'}
        merged['notes'] = (comments.get(tk) or {}).get('notes', '')
        out[tk] = merged
    return out


@app.route('/api/abi-watchlist', methods=['GET'])
def get_abi_watchlist():
    """Get all watchlist items: {TICKER: {notes, stars, added_at}, ...}.
    Notes are sourced from abi_ticker_notes.json (decoupled from membership)."""
    return jsonify(_watchlist_with_comments())


def _coerce_stars(value):
    """Validate and clamp a stars value to an int in [0, 3]. Returns None if invalid."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    if n < 0 or n > 3:
        return None
    return n


@app.route('/api/abi-watchlist', methods=['POST'])
def add_to_abi_watchlist():
    """Add a ticker to the watchlist with optional stars (0-3)."""
    data = request.get_json()
    if not data or 'ticker' not in data:
        return jsonify({'error': 'ticker field is required'}), 400

    ticker = data['ticker'].upper()
    stars = _coerce_stars(data.get('stars', 0)) or 0

    wl = _load_watchlist()
    wl[ticker] = {
        'stars': stars,
        'added_at': datetime.now(pytz.timezone("US/Eastern")).isoformat(),
    }
    _save_watchlist(wl)

    final_notes = (_load_abi_ticker_notes().get(ticker) or {}).get('notes', '')
    return jsonify({'ticker': ticker, 'notes': final_notes, 'stars': stars, 'status': 'added'})


@app.route('/api/abi-watchlist/<ticker>', methods=['PUT'])
def update_abi_watchlist(ticker):
    """Update stars (0-3) for a watchlist ticker."""
    ticker = ticker.upper()
    data = request.get_json() or {}

    if 'stars' not in data:
        return jsonify({'error': 'stars field is required'}), 400

    stars = _coerce_stars(data.get('stars'))
    if stars is None:
        return jsonify({'error': 'stars must be an integer between 0 and 3'}), 400

    wl = _load_watchlist()
    if ticker not in wl:
        return jsonify({'error': 'ticker not in watchlist'}), 404

    wl[ticker]['stars'] = stars
    _save_watchlist(wl)

    final_notes = (_load_abi_ticker_notes().get(ticker) or {}).get('notes', '')
    return jsonify({
        'ticker': ticker,
        'notes': final_notes,
        'stars': wl[ticker].get('stars', 0),
        'status': 'updated',
    })


@app.route('/api/abi-watchlist/<ticker>', methods=['DELETE'])
def remove_from_abi_watchlist(ticker):
    """Remove a ticker from the watchlist. Abi ticker notes for that ticker are unchanged."""
    ticker = ticker.upper()
    wl = _load_watchlist()
    if ticker in wl:
        del wl[ticker]
        _save_watchlist(wl)
    return jsonify({'ticker': ticker, 'status': 'removed'})


@app.route('/api/abi-watchlist/batch-check', methods=['POST'])
def batch_check_abi_watchlist():
    """Return watchlist data (Abi ticker notes joined under `notes`) for any
    of the given tickers that are on the watchlist. Tickers not on the
    watchlist are omitted from the response."""
    data = request.get_json()
    if not data or 'tickers' not in data:
        return jsonify({'error': 'tickers field is required'}), 400

    wl = _watchlist_with_comments()
    result = {}
    for t in data['tickers']:
        t_upper = t.upper()
        if t_upper in wl:
            result[t_upper] = wl[t_upper]
    return jsonify(result)


@app.route('/api/abi-watchlist/data', methods=['GET'])
def get_abi_watchlist_data():
    """Get watchlist tickers with full MainView data for the watchlist page."""
    wl = _load_watchlist()
    comments = _load_abi_ticker_notes()
    tickers = list(wl.keys())

    if not tickers:
        return jsonify([])

    s = Session()
    try:
        stocks = s.query(MainView).filter(MainView.ticker.in_(tickers)).all()

        results = []
        for stock in stocks:
            spike_dates = [d for d in (stock.volume_spike_days or '').split(',') if d.strip()]
            gap_dates = [d for d in (stock.gap_days or '').split(',') if d.strip()]

            item = {
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
                'ti65': round(stock.ti65, 2) if stock.ti65 else None,
                'dr_1': round(stock.dr_1, 2) if stock.dr_1 else None,
                'dr_5': round(stock.dr_5, 2) if stock.dr_5 else None,
                'dr_20': round(stock.dr_20, 2) if stock.dr_20 else None,
                'dr_60': round(stock.dr_60, 2) if stock.dr_60 else None,
                'dr_120': round(stock.dr_120, 2) if stock.dr_120 else None,
                'atr20': round(stock.atr20, 2) if stock.atr20 else None,
                'pe_t_minus_1': round(stock.pe_t_minus_1, 2) if stock.pe_t_minus_1 else None,
                'pe_t': round(stock.pe_t, 2) if stock.pe_t else None,
                'pe_t_plus_1': round(stock.pe_t_plus_1, 2) if stock.pe_t_plus_1 else None,
                'pe_t_plus_2': round(stock.pe_t_plus_2, 2) if stock.pe_t_plus_2 else None,
                'ps_t_minus_1': round(stock.ps_t_minus_1, 2) if stock.ps_t_minus_1 else None,
                'ps_t': round(stock.ps_t, 2) if stock.ps_t else None,
                'ps_t_plus_1': round(stock.ps_t_plus_1, 2) if stock.ps_t_plus_1 else None,
                'ps_t_plus_2': round(stock.ps_t_plus_2, 2) if stock.ps_t_plus_2 else None,
                'rev_growth_t_minus_1': round(stock.rev_growth_t_minus_1, 2) if stock.rev_growth_t_minus_1 else None,
                'rev_growth_t': round(stock.rev_growth_t, 2) if stock.rev_growth_t else None,
                'rev_growth_t_plus_1': round(stock.rev_growth_t_plus_1, 2) if stock.rev_growth_t_plus_1 else None,
                'rev_growth_t_plus_2': round(stock.rev_growth_t_plus_2, 2) if stock.rev_growth_t_plus_2 else None,
                'eps_growth_t_minus_1': round(stock.eps_growth_t_minus_1, 2) if stock.eps_growth_t_minus_1 else None,
                'eps_growth_t': round(stock.eps_growth_t, 2) if stock.eps_growth_t else None,
                'eps_growth_t_plus_1': round(stock.eps_growth_t_plus_1, 2) if stock.eps_growth_t_plus_1 else None,
                'eps_growth_t_plus_2': round(stock.eps_growth_t_plus_2, 2) if stock.eps_growth_t_plus_2 else None,
                'rsi': stock.rsi,
                'rsi_mktcap': stock.rsi_mktcap,
                'short_float': round(stock.short_float, 2) if stock.short_float else None,
                'short_ratio': round(stock.short_ratio, 2) if stock.short_ratio else None,
                'short_interest': round(stock.short_interest, 2) if stock.short_interest else None,
                'low_float': stock.low_float,
                'float_shares': stock.float_shares,
                'outstanding_shares': stock.outstanding_shares,
                'free_float': round(stock.free_float, 2) if stock.free_float else None,
                'spike_day_count': stock.spike_day_count or 0,
                'avg_volume_spike': round(stock.avg_volume_spike, 2) if stock.avg_volume_spike else None,
                'volume_spike_days': spike_dates,
                'gapper_day_count': stock.gapper_day_count or 0,
                'avg_return_gapper': round(stock.avg_return_gapper, 4) if stock.avg_return_gapper else None,
                'gap_days': gap_dates,
                'last_event_date': stock.last_event_date.strftime('%Y-%m-%d') if stock.last_event_date else None,
                'last_event_type': stock.last_event_type,
                'last_event_magnitude': float(stock.last_event_magnitude) if stock.last_event_magnitude else None,
                'last_event_return': float(stock.last_event_return) if stock.last_event_return else None,
                'tags': stock.tags,
                'updated_at': stock.updated_at.strftime('%Y-%m-%d %H:%M:%S') if stock.updated_at else None,
                'watchlist_notes': (comments.get(stock.ticker) or {}).get('notes', ''),
                'watchlist_added_at': wl.get(stock.ticker, {}).get('added_at', ''),
                'watchlist_stars': int(wl.get(stock.ticker, {}).get('stars', 0) or 0),
            }
            results.append(item)

        return jsonify(results)
    except Exception as e:
        logger.error(f"Error getting abi watchlist data: {str(e)}")
        return jsonify([])
    finally:
        s.close()


# ============================================================================
# Abi Dislikes Endpoints (JSON file-based thumbs-down list)
# ============================================================================
# This list is separate from the watchlist on purpose:
#   - Watchlist `stars: 0` means "on the watchlist but not yet rated" (neutral).
#   - Anything in this dislikes file is an explicit "do not surface this".
# The daily screener pipeline (Stage 1) reads this file as its veto source.
#
# Per-ticker notes (the "why is this blocked" rationale) live in
# abi_ticker_notes.json now; the dislikes file only tracks membership. The API
# response shape still includes a `notes` field populated from abi_ticker_notes.json.

def _load_dislikes():
    """Load the dislikes file. Ignores embedded ``notes`` keys — Abi ticker notes
    live only in abi_ticker_notes.json.
    """
    if os.path.exists(ABI_DISLIKES_FILE):
        try:
            with open(ABI_DISLIKES_FILE, 'r') as f:
                data = json.load(f)
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}
        for entry in data.values():
            if isinstance(entry, dict) and 'notes' in entry:
                del entry['notes']
        return data
    return {}


def _save_dislikes(data):
    os.makedirs(os.path.dirname(ABI_DISLIKES_FILE), exist_ok=True)
    with open(ABI_DISLIKES_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def _dislikes_with_comments():
    """Return dislikes data joined with Abi ticker notes: each entry will
    have a `notes` field populated from abi_ticker_notes.json (or '' if no
    Abi ticker note exists)."""
    dl = _load_dislikes()
    comments = _load_abi_ticker_notes()
    out = {}
    for tk, entry in dl.items():
        if not isinstance(entry, dict):
            continue
        merged = {k: v for k, v in entry.items() if k != 'notes'}
        merged['notes'] = (comments.get(tk) or {}).get('notes', '')
        out[tk] = merged
    return out


@app.route('/api/abi-dislikes', methods=['GET'])
def get_abi_dislikes():
    """Get all dislikes: {TICKER: {notes, added_at}, ...}.
    Notes are sourced from abi_ticker_notes.json (decoupled from membership)."""
    return jsonify(_dislikes_with_comments())


@app.route('/api/abi-dislikes', methods=['POST'])
def add_to_abi_dislikes():
    """Add a ticker to the dislikes list (membership only)."""
    data = request.get_json()
    if not data or 'ticker' not in data:
        return jsonify({'error': 'ticker field is required'}), 400

    ticker = data['ticker'].upper()

    dl = _load_dislikes()
    dl[ticker] = {
        'added_at': datetime.now(pytz.timezone("US/Eastern")).isoformat(),
    }
    _save_dislikes(dl)

    final_notes = (_load_abi_ticker_notes().get(ticker) or {}).get('notes', '')
    return jsonify({'ticker': ticker, 'notes': final_notes, 'status': 'added'})


@app.route('/api/abi-dislikes/<ticker>', methods=['DELETE'])
def remove_from_abi_dislikes(ticker):
    """Remove a ticker from the dislikes list. Abi ticker notes for that ticker are unchanged."""
    ticker = ticker.upper()
    dl = _load_dislikes()
    if ticker in dl:
        del dl[ticker]
        _save_dislikes(dl)
    return jsonify({'ticker': ticker, 'status': 'removed'})


@app.route('/api/abi-dislikes/batch-check', methods=['POST'])
def batch_check_abi_dislikes():
    """Return dislikes data (Abi ticker notes joined under `notes`) for any
    of the given tickers that are disliked."""
    data = request.get_json()
    if not data or 'tickers' not in data:
        return jsonify({'error': 'tickers field is required'}), 400

    dl = _dislikes_with_comments()
    result = {}
    for t in data['tickers']:
        t_upper = t.upper()
        if t_upper in dl:
            result[t_upper] = dl[t_upper]
    return jsonify(result)


# ============================================================================
# Technical Screener Endpoints
# ============================================================================
# These endpoints surface intraday/technical setups from the latest OHLC bar.
# Each criterion is exposed as a separate sub-endpoint so additional setups
# (e.g. inside bars, opening drives, etc.) can be added over time without
# changing the route surface for existing ones.
#
# Currently supported criteria:
#   - reversal: Biggest reversal from low-of-day to close (in % terms),
#               i.e. how far price recovered off the intraday low.
# ============================================================================


def _get_index_reversal_context(session, latest_date, symbols=('SPY', 'QQQ')):
    """Compute low-to-close reversal % on ``latest_date`` for the given index/ETF symbols.

    Returns a list of dicts (one per symbol) with the same shape used elsewhere
    on the page so the frontend can render them in a context panel.
    """
    context = []
    rows = session.query(
        IndexPrice.symbol,
        IndexPrice.open_price,
        IndexPrice.high_price,
        IndexPrice.low_price,
        IndexPrice.close_price,
    ).filter(
        IndexPrice.symbol.in_(list(symbols)),
        IndexPrice.date == latest_date,
    ).all()

    by_symbol = {r.symbol: r for r in rows}

    for sym in symbols:
        r = by_symbol.get(sym)
        if not r or r.low_price is None or r.close_price is None or r.open_price is None:
            context.append({
                'symbol': sym,
                'open': None,
                'high': None,
                'low': None,
                'close': None,
                'reversal_pct': None,
                'day_change_pct': None,
            })
            continue

        reversal_pct = ((r.close_price - r.low_price) / r.low_price * 100.0) if r.low_price else None
        day_change_pct = ((r.close_price - r.open_price) / r.open_price * 100.0) if r.open_price else None
        context.append({
            'symbol': sym,
            'open': round(r.open_price, 2),
            'high': round(r.high_price, 2) if r.high_price is not None else None,
            'low': round(r.low_price, 2),
            'close': round(r.close_price, 2),
            'reversal_pct': round(reversal_pct, 2) if reversal_pct is not None else None,
            'day_change_pct': round(day_change_pct, 2) if day_change_pct is not None else None,
        })

    return context


def get_technical_reversal_stocks(session, market_cap_category=None, limit=100):
    """Return stocks ranked by largest low-of-day to close reversal % on the
    latest trading day, joined with the full screener metrics set so the
    Top-Returns-style detail panel can render the same metric sections.

    Reversal % = (close - low) / low * 100
    """
    try:
        latest_date = get_latest_price_date(session)
        if not latest_date:
            return {'latest_date': None, 'context': [], 'stocks': []}

        reversal_expr = ((OHLC.close - OHLC.low) / OHLC.low * 100.0).label('reversal_pct')
        day_change_expr = ((OHLC.close - OHLC.open) / OHLC.open * 100.0).label('day_change_pct')

        query = session.query(
            StockMetrics.ticker,
            StockMetrics.company_name,
            StockMetrics.country,
            StockMetrics.sector,
            StockMetrics.industry,
            StockMetrics.ipo_date,
            StockMetrics.market_cap,
            StockMetrics.current_price,
            StockMetrics.range_52_week,
            StockMetrics.volume,
            StockMetrics.dollar_volume,
            StockMetrics.avg_vol_10d,
            StockMetrics.vol_vs_10d_avg,
            StockMetrics.ti65,
            StockMetrics.rsi,
            StockMetrics.rsi_mktcap,
            StockMetrics.dr_1,
            StockMetrics.dr_5,
            StockMetrics.dr_20,
            StockMetrics.dr_60,
            StockMetrics.dr_120,
            StockMetrics.atr20,
            StockMetrics.pe_t_minus_1,
            StockMetrics.pe_t,
            StockMetrics.pe_t_plus_1,
            StockMetrics.pe_t_plus_2,
            StockMetrics.ps_t_minus_1,
            StockMetrics.ps_t,
            StockMetrics.ps_t_plus_1,
            StockMetrics.ps_t_plus_2,
            StockMetrics.rev_growth_t_minus_1,
            StockMetrics.rev_growth_t,
            StockMetrics.rev_growth_t_plus_1,
            StockMetrics.rev_growth_t_plus_2,
            StockMetrics.eps_growth_t_minus_1,
            StockMetrics.eps_growth_t,
            StockMetrics.eps_growth_t_plus_1,
            StockMetrics.eps_growth_t_plus_2,
            StockMetrics.short_float,
            StockMetrics.short_ratio,
            StockMetrics.short_interest,
            StockMetrics.low_float,
            StockMetrics.float_shares,
            StockMetrics.outstanding_shares,
            StockMetrics.free_float,
            StockMetrics.updated_at,
            StockVolspikeGapper.last_event_date,
            StockVolspikeGapper.last_event_type,
            StockVolspikeGapper.last_event_magnitude,
            StockVolspikeGapper.last_event_return,
            MainView.tags,
            OHLC.open.label('day_open'),
            OHLC.high.label('day_high'),
            OHLC.low.label('day_low'),
            OHLC.close.label('day_close'),
            reversal_expr,
            day_change_expr,
        ).outerjoin(
            StockVolspikeGapper, StockVolspikeGapper.ticker == StockMetrics.ticker
        ).outerjoin(
            MainView, MainView.ticker == StockMetrics.ticker
        ).join(
            OHLC,
            (OHLC.ticker == StockMetrics.ticker) & (OHLC.date == latest_date)
        ).filter(
            OHLC.low.isnot(None),
            OHLC.low > 0,
            OHLC.close.isnot(None),
            StockMetrics.market_cap.isnot(None),
        )

        if market_cap_category:
            category = MARKET_CAP_CATEGORIES.get(market_cap_category)
            if category:
                query = query.filter(StockMetrics.market_cap >= category['min'])
                if category['max'] is not None:
                    query = query.filter(StockMetrics.market_cap < category['max'])

        query = apply_global_liquidity_filters(query)
        query = query.order_by(desc(reversal_expr)).limit(limit)

        results = []
        for r in query.all():
            results.append({
                'ticker': r.ticker,
                'company_name': r.company_name,
                'country': r.country,
                'sector': r.sector,
                'industry': r.industry,
                'ipo_date': r.ipo_date.strftime('%Y-%m-%d') if r.ipo_date else None,
                'market_cap': r.market_cap,
                'current_price': round(r.current_price, 2) if r.current_price else None,
                'range_52_week': r.range_52_week,
                'volume': int(r.volume) if r.volume else None,
                'dollar_volume': round(r.dollar_volume, 2) if r.dollar_volume else None,
                'avg_vol_10d': float(r.avg_vol_10d) if r.avg_vol_10d else None,
                'vol_vs_10d_avg': round(r.vol_vs_10d_avg, 2) if r.vol_vs_10d_avg else None,
                'ti65': round(r.ti65, 2) if r.ti65 else None,
                'rsi': r.rsi,
                'rsi_mktcap': r.rsi_mktcap,
                'dr_1': round(r.dr_1, 2) if r.dr_1 is not None else None,
                'dr_5': round(r.dr_5, 2) if r.dr_5 is not None else None,
                'dr_20': round(r.dr_20, 2) if r.dr_20 is not None else None,
                'dr_60': round(r.dr_60, 2) if r.dr_60 is not None else None,
                'dr_120': round(r.dr_120, 2) if r.dr_120 is not None else None,
                'atr20': round(r.atr20, 2) if r.atr20 else None,
                'pe_t_minus_1': round(r.pe_t_minus_1, 2) if r.pe_t_minus_1 else None,
                'pe_t': round(r.pe_t, 2) if r.pe_t else None,
                'pe_t_plus_1': round(r.pe_t_plus_1, 2) if r.pe_t_plus_1 else None,
                'pe_t_plus_2': round(r.pe_t_plus_2, 2) if r.pe_t_plus_2 else None,
                'ps_t_minus_1': round(r.ps_t_minus_1, 2) if r.ps_t_minus_1 else None,
                'ps_t': round(r.ps_t, 2) if r.ps_t else None,
                'ps_t_plus_1': round(r.ps_t_plus_1, 2) if r.ps_t_plus_1 else None,
                'ps_t_plus_2': round(r.ps_t_plus_2, 2) if r.ps_t_plus_2 else None,
                'rev_growth_t_minus_1': round(r.rev_growth_t_minus_1, 2) if r.rev_growth_t_minus_1 else None,
                'rev_growth_t': round(r.rev_growth_t, 2) if r.rev_growth_t else None,
                'rev_growth_t_plus_1': round(r.rev_growth_t_plus_1, 2) if r.rev_growth_t_plus_1 else None,
                'rev_growth_t_plus_2': round(r.rev_growth_t_plus_2, 2) if r.rev_growth_t_plus_2 else None,
                'eps_growth_t_minus_1': round(r.eps_growth_t_minus_1, 2) if r.eps_growth_t_minus_1 else None,
                'eps_growth_t': round(r.eps_growth_t, 2) if r.eps_growth_t else None,
                'eps_growth_t_plus_1': round(r.eps_growth_t_plus_1, 2) if r.eps_growth_t_plus_1 else None,
                'eps_growth_t_plus_2': round(r.eps_growth_t_plus_2, 2) if r.eps_growth_t_plus_2 else None,
                'short_float': round(r.short_float, 2) if r.short_float else None,
                'short_ratio': round(r.short_ratio, 2) if r.short_ratio else None,
                'short_interest': round(r.short_interest, 2) if r.short_interest else None,
                'low_float': r.low_float,
                'float_shares': r.float_shares,
                'outstanding_shares': r.outstanding_shares,
                'free_float': round(r.free_float, 2) if r.free_float else None,
                'last_event_date': r.last_event_date.strftime('%Y-%m-%d') if r.last_event_date else None,
                'last_event_type': r.last_event_type,
                'last_event_magnitude': float(r.last_event_magnitude) if r.last_event_magnitude else None,
                'last_event_return': float(r.last_event_return) if r.last_event_return else None,
                'tags': r.tags,
                'updated_at': r.updated_at.strftime('%Y-%m-%d %H:%M:%S') if r.updated_at else None,
                'day_open': round(r.day_open, 2) if r.day_open is not None else None,
                'day_high': round(r.day_high, 2) if r.day_high is not None else None,
                'day_low': round(r.day_low, 2) if r.day_low is not None else None,
                'day_close': round(r.day_close, 2) if r.day_close is not None else None,
                'reversal_pct': round(r.reversal_pct, 2) if r.reversal_pct is not None else None,
                'day_change_pct': round(r.day_change_pct, 2) if r.day_change_pct is not None else None,
            })

        context = _get_index_reversal_context(session, latest_date)

        return {
            'latest_date': latest_date.strftime('%Y-%m-%d'),
            'context': context,
            'stocks': results,
        }

    except Exception as e:
        logger.error(f"Error getting technical reversal stocks: {str(e)}")
        return {'latest_date': None, 'context': [], 'stocks': []}


@app.route('/api/TechnicalScreener-Reversal-All')
def get_technical_reversal_all():
    s = Session()
    try:
        return jsonify(get_technical_reversal_stocks(s, market_cap_category=None))
    finally:
        s.close()

@app.route('/api/TechnicalScreener-Reversal-MicroCap')
def get_technical_reversal_micro():
    s = Session()
    try:
        return jsonify(get_technical_reversal_stocks(s, market_cap_category='micro'))
    finally:
        s.close()

@app.route('/api/TechnicalScreener-Reversal-SmallCap')
def get_technical_reversal_small():
    s = Session()
    try:
        return jsonify(get_technical_reversal_stocks(s, market_cap_category='small'))
    finally:
        s.close()

@app.route('/api/TechnicalScreener-Reversal-MidCap')
def get_technical_reversal_mid():
    s = Session()
    try:
        return jsonify(get_technical_reversal_stocks(s, market_cap_category='mid'))
    finally:
        s.close()

@app.route('/api/TechnicalScreener-Reversal-LargeCap')
def get_technical_reversal_large():
    s = Session()
    try:
        return jsonify(get_technical_reversal_stocks(s, market_cap_category='large'))
    finally:
        s.close()

@app.route('/api/TechnicalScreener-Reversal-MegaCap')
def get_technical_reversal_mega():
    s = Session()
    try:
        return jsonify(get_technical_reversal_stocks(s, market_cap_category='mega'))
    finally:
        s.close()


# ============================================================================
# Daily Shortlist (Screening Agent) Endpoints
# ============================================================================

DAILY_SCREENER_OUTPUTS_DIR = os.path.join(
    os.path.dirname(__file__), '..', 'daily_screener', 'outputs'
)


def _compute_daily_shortlist_funnel(date: str):
    """Read per-stage artifacts under daily_screener/outputs/<date>/ and
    construct a compact funnel summary the UI can render. Works retroactively
    even if s6_audit didn't write a funnel block."""
    base = os.path.join(DAILY_SCREENER_OUTPUTS_DIR, date)

    def _safe_read(name):
        path = os.path.join(base, name)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception:
            return None

    universe = _safe_read('00_universe.json') or {}
    momentum = _safe_read('01_momentum.json') or {}
    news = _safe_read('03_news.json') or {}
    judged = _safe_read('04_judged.json') or {}
    summary = _safe_read('run_summary.json') or {}

    source_counts = universe.get('source_counts') or {}
    raw_total = int(universe.get('raw_total') or sum(int(v or 0) for v in source_counts.values()))

    disliked = int(universe.get('disliked_count') or 0)
    industry = int(universe.get('industry_dropped_count') or 0)
    universe_survivors = int(universe.get('total_universe') or 0)

    # Prefer fields written by s1; fall back to the (lossy) computation for
    # older artifacts that didn't capture them.
    unique_after_dedup = universe.get('unique_after_dedup')
    if unique_after_dedup is None:
        unique_after_dedup = universe_survivors + disliked + industry
    unique_after_dedup = int(unique_after_dedup)

    survivors_after_filters = universe.get('survivors_after_filters')
    if survivors_after_filters is None:
        survivors_after_filters = unique_after_dedup - disliked - industry
    survivors_after_filters = int(survivors_after_filters)

    truncated_count = int(universe.get('truncated_count') or 0)
    max_tickers_cap = universe.get('max_tickers_cap')

    momentum_survivors = int(momentum.get('survivor_count') or 0)
    momentum_dropped = int(momentum.get('drop_count') or 0)

    news_results = int(news.get('result_count') or 0)
    news_failures = int(news.get('failure_count') or 0)

    verdicts = judged.get('verdict_counts') or {}

    stages_meta = (summary.get('stages') or {}) if isinstance(summary, dict) else {}
    timings = {
        name: data.get('elapsed_s')
        for name, data in stages_meta.items()
        if isinstance(data, dict)
    } if stages_meta else None

    return {
        'source_counts': source_counts,
        'raw_total': raw_total,
        'unique_after_dedup': unique_after_dedup,
        'industry_dropped': industry,
        'disliked_dropped': disliked,
        'survivors_after_filters': survivors_after_filters,
        'max_tickers_cap': max_tickers_cap,
        'truncated_count': truncated_count,
        'universe_survivors': universe_survivors,
        'momentum_survivors': momentum_survivors,
        'momentum_dropped': momentum_dropped,
        'news_results': news_results,
        'news_failures': news_failures,
        'judged_pick': int(verdicts.get('PICK') or 0),
        'judged_watch': int(verdicts.get('WATCH') or 0),
        'judged_skip': int(verdicts.get('SKIP') or 0),
        'timings': timings,
    }


def _list_daily_shortlist_dates():
    """List YYYY-MM-DD folders under daily_screener/outputs that contain a 05_audit.json."""
    if not os.path.isdir(DAILY_SCREENER_OUTPUTS_DIR):
        return []
    entries = []
    for name in os.listdir(DAILY_SCREENER_OUTPUTS_DIR):
        full = os.path.join(DAILY_SCREENER_OUTPUTS_DIR, name)
        if not os.path.isdir(full):
            continue
        # Validate date-like name
        try:
            datetime.strptime(name, '%Y-%m-%d')
        except ValueError:
            continue
        audit_path = os.path.join(full, '05_audit.json')
        if os.path.exists(audit_path):
            try:
                mtime = os.path.getmtime(audit_path)
            except OSError:
                mtime = None
            entries.append({
                'date': name,
                'audit_mtime': mtime,
                'has_audit': True,
            })
        else:
            entries.append({'date': name, 'audit_mtime': None, 'has_audit': False})
    entries.sort(key=lambda e: e['date'], reverse=True)
    return entries


@app.route('/api/daily-shortlist/dates', methods=['GET'])
def daily_shortlist_dates():
    """List available daily shortlist dates."""
    return jsonify({'dates': _list_daily_shortlist_dates()})


@app.route('/api/daily-shortlist/<date>', methods=['GET'])
def daily_shortlist_for_date(date):
    """Return the audit artifact for a given date, enriched with watchlist /
    preference status so the UI can render in-line actions."""
    # Validate date format
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'invalid date (expected YYYY-MM-DD)'}), 400

    audit_path = os.path.join(DAILY_SCREENER_OUTPUTS_DIR, date, '05_audit.json')
    if not os.path.exists(audit_path):
        return jsonify({'error': 'no audit artifact for date', 'date': date}), 404

    try:
        with open(audit_path, 'r') as f:
            audit = json.load(f)
    except Exception as e:
        logger.error(f"failed to read audit {audit_path}: {e}")
        return jsonify({'error': f'failed to read audit: {e}'}), 500

    rows = audit.get('rows', []) or []
    tickers = sorted({(r.get('ticker') or '').upper() for r in rows if r.get('ticker')})

    feedback_for_date = _load_feedback().get(date, {}) or {}

    s = Session()
    try:
        wl = _load_watchlist()
        dl = _load_dislikes()
        comments = _load_abi_ticker_notes()
        prefs = {
            p.ticker: p.preference
            for p in s.query(StockPreference).filter(
                StockPreference.ticker.in_(tickers),
                StockPreference.preference.isnot(None),
            ).all()
        }
        # Pull MainView tags for these tickers (useful for the UI "tags" column).
        mvs = {
            m.ticker: m
            for m in s.query(MainView).filter(MainView.ticker.in_(tickers)).all()
        }

        enriched = []
        for r in rows:
            t = (r.get('ticker') or '').upper()
            wl_entry = wl.get(t) if t in wl else None
            dl_entry = dl.get(t) if t in dl else None
            comment_notes = (comments.get(t) or {}).get('notes', '')
            mv = mvs.get(t)
            enriched_row = dict(r)
            enriched_row['watchlist'] = (
                {
                    'in_watchlist': True,
                    # Abi ticker note text joined under `watchlist.notes` for the UI.
                    'notes': comment_notes,
                    'stars': int(wl_entry.get('stars', 0) or 0) if isinstance(wl_entry, dict) else 0,
                }
                if wl_entry is not None
                else {'in_watchlist': False, 'notes': comment_notes, 'stars': 0}
            )
            enriched_row['dislike'] = (
                {
                    'is_disliked': True,
                    'notes': comment_notes,
                }
                if dl_entry is not None
                else {'is_disliked': False, 'notes': comment_notes}
            )
            # Top-level `ticker_note`: the canonical Abi ticker note. The UI
            # should prefer this over `watchlist.notes` / `dislike.notes`.
            enriched_row['ticker_note'] = {
                'notes': comment_notes,
                'created_at': (comments.get(t) or {}).get('created_at'),
                'updated_at': (comments.get(t) or {}).get('updated_at'),
                'has_ticker_note': bool(comment_notes),
            }
            enriched_row['preference'] = prefs.get(t)
            enriched_row['feedback'] = feedback_for_date.get(t)
            if mv is not None:
                enriched_row.setdefault('tags', mv.tags)
                if enriched_row.get('current_price') is None and mv.current_price is not None:
                    enriched_row['current_price'] = round(mv.current_price, 2)
            enriched.append(enriched_row)
    finally:
        s.close()

    return jsonify({
        'date': audit.get('date', date),
        'total_universe': audit.get('total_universe'),
        'verdict_counts': audit.get('verdict_counts'),
        'source_counts': audit.get('source_counts'),
        'funnel': _compute_daily_shortlist_funnel(date),
        'rows': enriched,
    })


@app.route('/api/daily-shortlist/run', methods=['POST'])
def daily_shortlist_run():
    """Trigger a pipeline run as a background subprocess. Returns immediately
    with the date being processed. Optional JSON body:
      { "max_tickers": 5, "from_stage": 1, "force_refresh_themes": false }
    """
    import subprocess
    import sys as _sys

    data = request.get_json(silent=True) or {}
    args = [
        _sys.executable, '-m', 'daily_screener.run',
    ]
    if 'max_tickers' in data:
        args += ['--max-tickers', str(int(data['max_tickers']))]
    if 'from_stage' in data:
        args += ['--from-stage', str(int(data['from_stage']))]
    if data.get('force_refresh_themes'):
        args += ['--force-refresh-themes']
    if data.get('date'):
        args += ['--date', str(data['date'])]
    if data.get('verbose'):
        args += ['--verbose']

    cwd = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    try:
        proc = subprocess.Popen(
            args,
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as e:
        logger.error(f"failed to start daily_screener.run: {e}")
        return jsonify({'error': f'failed to start: {e}'}), 500

    return jsonify({
        'status': 'started',
        'pid': proc.pid,
        'cmd': args,
        'cwd': cwd,
    })


# ============================================================================
# Daily Shortlist Feedback (per-date, per-ticker corrections)
# ============================================================================
# The user clicks "Feedback" on a row in /daily-shortlist if they think the
# agent's verdict was wrong (e.g. PICK that should have been SKIP, or SKIP
# that should have been PICK). We capture both the agent's verdict at the
# time and what the user thinks it should have been, plus a free-text reason.
#
# Stage 5 of the daily_screener pipeline reads this file and feeds recent
# corrections into the judge prompt as calibration examples.

_VALID_VERDICTS = {'PICK', 'WATCH', 'SKIP'}


def _load_feedback():
    """Load the feedback file. Returns {date: {ticker: record}}."""
    if not os.path.exists(DAILY_SCREENER_FEEDBACK_FILE):
        return {}
    try:
        with open(DAILY_SCREENER_FEEDBACK_FILE, 'r') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error(f"failed to read feedback file: {e}")
        return {}


def _save_feedback(data):
    os.makedirs(os.path.dirname(DAILY_SCREENER_FEEDBACK_FILE), exist_ok=True)
    with open(DAILY_SCREENER_FEEDBACK_FILE, 'w') as f:
        json.dump(data, f, indent=2)


@app.route('/api/daily-shortlist/feedback/<date>', methods=['GET'])
def daily_shortlist_feedback_for_date(date):
    """Return all feedback entries for a given date as {ticker: record}."""
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'invalid date (expected YYYY-MM-DD)'}), 400
    data = _load_feedback()
    return jsonify(data.get(date, {}))


@app.route('/api/daily-shortlist/feedback', methods=['GET'])
def daily_shortlist_feedback_all():
    """Return all feedback entries flattened into a list, newest first.

    Optional ?limit=N to cap the list (handy for the judge prompt).
    """
    limit = request.args.get('limit', type=int)
    data = _load_feedback()
    flat = []
    for date_key in sorted(data.keys(), reverse=True):
        per_date = data[date_key]
        if not isinstance(per_date, dict):
            continue
        for ticker, rec in per_date.items():
            if not isinstance(rec, dict):
                continue
            flat.append({'date': date_key, 'ticker': ticker, **rec})
    if limit:
        flat = flat[:limit]
    return jsonify({'count': len(flat), 'feedback': flat})


@app.route('/api/daily-shortlist/feedback/<date>/<ticker>', methods=['PUT'])
def upsert_daily_shortlist_feedback(date, ticker):
    """Upsert a feedback record for (date, ticker).

    Request body:
      {
        "expected_verdict": "PICK|WATCH|SKIP",
        "reason": "free text",
        # Optional snapshot fields - capture the agent's state at the time so
        # we can show it later even if the audit artifact changes:
        "agent_verdict": "PICK|WATCH|SKIP",
        "agent_rationale": "...",
        "agent_composite_score": 76.5,
        "agent_news_materiality": 2,
        "agent_theme_fit": 3,
        "agent_matched_themes": [...]
      }
    """
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'invalid date (expected YYYY-MM-DD)'}), 400

    payload = request.get_json(silent=True) or {}

    expected = (payload.get('expected_verdict') or '').upper()
    if expected not in _VALID_VERDICTS:
        return jsonify({
            'error': f'expected_verdict must be one of {sorted(_VALID_VERDICTS)}'
        }), 400

    agent_verdict = (payload.get('agent_verdict') or '').upper() or None
    if agent_verdict is not None and agent_verdict not in _VALID_VERDICTS:
        return jsonify({
            'error': f'agent_verdict must be one of {sorted(_VALID_VERDICTS)} if provided'
        }), 400

    ticker_u = (ticker or '').upper()
    if not ticker_u:
        return jsonify({'error': 'ticker is required'}), 400

    data = _load_feedback()
    per_date = data.setdefault(date, {})
    existing = per_date.get(ticker_u) or {}
    now_iso = datetime.now(pytz.timezone('US/Eastern')).isoformat()

    record = {
        'expected_verdict': expected,
        'reason': (payload.get('reason') or '').strip(),
        'agent_verdict': agent_verdict or existing.get('agent_verdict'),
        'agent_rationale': payload.get('agent_rationale')
            if 'agent_rationale' in payload else existing.get('agent_rationale'),
        'agent_composite_score': payload.get('agent_composite_score')
            if 'agent_composite_score' in payload else existing.get('agent_composite_score'),
        'agent_news_materiality': payload.get('agent_news_materiality')
            if 'agent_news_materiality' in payload else existing.get('agent_news_materiality'),
        'agent_theme_fit': payload.get('agent_theme_fit')
            if 'agent_theme_fit' in payload else existing.get('agent_theme_fit'),
        'agent_matched_themes': payload.get('agent_matched_themes')
            if 'agent_matched_themes' in payload else existing.get('agent_matched_themes'),
        'created_at': existing.get('created_at') or now_iso,
        'updated_at': now_iso,
    }
    per_date[ticker_u] = record
    _save_feedback(data)
    return jsonify({'date': date, 'ticker': ticker_u, 'status': 'saved', **record})


@app.route('/api/daily-shortlist/feedback/<date>/<ticker>', methods=['DELETE'])
def delete_daily_shortlist_feedback(date, ticker):
    """Remove a feedback record for (date, ticker)."""
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'invalid date (expected YYYY-MM-DD)'}), 400

    ticker_u = (ticker or '').upper()
    data = _load_feedback()
    if date in data and ticker_u in data[date]:
        del data[date][ticker_u]
        if not data[date]:
            del data[date]
        _save_feedback(data)
        return jsonify({'date': date, 'ticker': ticker_u, 'status': 'removed'})
    return jsonify({'date': date, 'ticker': ticker_u, 'status': 'not_found'}), 404


# ============================================================================
# Daily Shortlist Themes (read the 02_theme_vector.json artifact)
# ============================================================================

@app.route('/api/daily-shortlist/themes/<date>', methods=['GET'])
def daily_shortlist_themes_for_date(date):
    """Return the merged theme vector for a date plus the per-source raw lists.

    Reads:
      - 02_theme_vector.json (merged, weighted vector — primary)
      - 02a_market_themes.json (hot market themes, source: hot_market)
      - 02b_user_themes.json (user-taste themes from watchlist, source: user_taste)
    """
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'invalid date (expected YYYY-MM-DD)'}), 400

    base = os.path.join(DAILY_SCREENER_OUTPUTS_DIR, date)
    if not os.path.isdir(base):
        return jsonify({'error': 'no theme artifacts for date', 'date': date}), 404

    def _read(name):
        path = os.path.join(base, name)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"failed to read {path}: {e}")
            return None

    merged = _read('02_theme_vector.json')
    market = _read('02a_market_themes.json')
    user = _read('02b_user_themes.json')

    if merged is None and market is None and user is None:
        return jsonify({'error': 'no theme artifacts for date', 'date': date}), 404

    return jsonify({
        'date': date,
        'merged': merged or {},
        'market': market or {},
        'user': user or {},
    })


@app.route('/api/daily-shortlist/themes/dates', methods=['GET'])
def daily_shortlist_theme_dates():
    """List dates that have a 02_theme_vector.json artifact."""
    if not os.path.isdir(DAILY_SCREENER_OUTPUTS_DIR):
        return jsonify({'dates': []})
    entries = []
    for name in os.listdir(DAILY_SCREENER_OUTPUTS_DIR):
        full = os.path.join(DAILY_SCREENER_OUTPUTS_DIR, name)
        if not os.path.isdir(full):
            continue
        try:
            datetime.strptime(name, '%Y-%m-%d')
        except ValueError:
            continue
        tv_path = os.path.join(full, '02_theme_vector.json')
        if os.path.exists(tv_path):
            try:
                mtime = os.path.getmtime(tv_path)
            except OSError:
                mtime = None
            entries.append({'date': name, 'mtime': mtime})
    entries.sort(key=lambda e: e['date'], reverse=True)
    return jsonify({'dates': entries})


# ============================================================
# Market Brief Endpoints
# ============================================================

MARKET_BRIEF_OUTPUTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'market_brief', 'outputs')


@app.route('/api/market-brief/dates', methods=['GET'])
def market_brief_dates():
    """List dates that have market brief outputs."""
    if not os.path.isdir(MARKET_BRIEF_OUTPUTS_DIR):
        return jsonify({'dates': []})
    entries = []
    for name in os.listdir(MARKET_BRIEF_OUTPUTS_DIR):
        full = os.path.join(MARKET_BRIEF_OUTPUTS_DIR, name)
        if not os.path.isdir(full):
            continue
        try:
            datetime.strptime(name, '%Y-%m-%d')
        except ValueError:
            continue
        brief_md = os.path.join(full, '02_brief.md')
        brief_json = os.path.join(full, '02_brief.json')
        if os.path.exists(brief_md) or os.path.exists(brief_json):
            try:
                mtime = os.path.getmtime(brief_json if os.path.exists(brief_json) else brief_md)
            except OSError:
                mtime = None
            entries.append({'date': name, 'mtime': mtime})
    entries.sort(key=lambda e: e['date'], reverse=True)
    return jsonify({'dates': entries})


@app.route('/api/market-brief/<date_str>', methods=['GET'])
def market_brief_for_date(date_str):
    """Get market brief content for a specific date."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400

    brief_dir = os.path.join(MARKET_BRIEF_OUTPUTS_DIR, date_str)
    if not os.path.isdir(brief_dir):
        return jsonify({'error': 'No brief found for this date'}), 404

    result = {'date': date_str}

    brief_md_path = os.path.join(brief_dir, '02_brief.md')
    if os.path.exists(brief_md_path):
        with open(brief_md_path, 'r', encoding='utf-8') as f:
            result['markdown'] = f.read()

    brief_json_path = os.path.join(brief_dir, '02_brief.json')
    if os.path.exists(brief_json_path):
        with open(brief_json_path, 'r', encoding='utf-8') as f:
            result['json'] = json.load(f)

    usage_path = os.path.join(brief_dir, 'usage.json')
    if os.path.exists(usage_path):
        with open(usage_path, 'r', encoding='utf-8') as f:
            result['usage'] = json.load(f)

    return jsonify(result)


@app.route('/api/market-brief/run', methods=['POST'])
def market_brief_run():
    """Trigger a new market brief run."""
    import subprocess
    import threading

    data = request.get_json() or {}
    asof = data.get('asof')

    cmd = ['python', '-m', 'market_brief.run']
    if asof:
        try:
            datetime.strptime(asof, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid asof date format. Use YYYY-MM-DD'}), 400
        cmd.extend(['--asof', asof])

    def run_brief():
        try:
            subprocess.run(cmd, cwd=os.path.dirname(os.path.dirname(__file__)), check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Market brief run failed: {e}")

    thread = threading.Thread(target=run_brief, daemon=True)
    thread.start()

    return jsonify({
        'status': 'started',
        'message': 'Market brief generation started in background',
        'asof': asof or 'today'
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
