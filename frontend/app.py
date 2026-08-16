"""Frontend Flask app - renders templates and proxies API calls to backend."""

from flask import Flask, render_template, jsonify, request, redirect, Response
import os
import json
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__,
    template_folder='templates',
    static_folder='static'
)

# Backend API base URL
BACKEND_URL = os.getenv('BACKEND_URL', 'http://localhost:5001')

# Determine if running in dev environment
IS_DEV = 'backend-dev' in BACKEND_URL

@app.context_processor
def inject_global_vars():
    """Inject global variables into all templates"""
    return {'is_dev': IS_DEV}

def make_backend_request(endpoint, method='GET', json_data=None):
    """Helper function to make requests to the backend API."""
    try:
        url = f"{BACKEND_URL}{endpoint}"
        
        if method == 'GET':
            response = requests.get(url)
        elif method == 'PUT':
            response = requests.put(url, json=json_data)
        elif method == 'POST':
            response = requests.post(url, json=json_data)
        elif method == 'DELETE':
            response = requests.delete(url, json=json_data)
        else:
            return None, 400
        
        if response.status_code >= 400:
            logger.error(f"Backend API error {response.status_code}: {response.text}")
            return None, response.status_code
        
        return response.json(), response.status_code
    except requests.RequestException as e:
        logger.error(f"Error connecting to backend API: {str(e)}")
        return None, 500
    except Exception as e:
        logger.error(f"Unexpected error in backend request: {str(e)}")
        return None, 500

@app.route('/')
def index():
    """Home page"""
    return render_template('index.html')

@app.route('/help')
def help_page():
    """Reference page - concise technical description of each page's business logic"""
    return render_template('help.html')


@app.route('/sector-performance')
def sector_performance_page():
    """Sector Performance page - Index/ETF performance across timeframes"""
    return render_template('sector_performance.html')


@app.route('/rs-screener')
def rs_screener_page():
    """RS Screener page - Multi-timeframe relative strength with adjustable weights"""
    return render_template('rs_screener.html')


@app.route('/top-performance')
def top_performance_page():
    """Top Performance page - Union of top stocks by 1D, 5D, 20D returns"""
    return render_template('top_performance.html')

@app.route('/all-stocks')
def all_stocks_page():
    """All Stocks page - main_view universe filtered only by sector/industry"""
    return render_template('all_stocks.html')

@app.route('/top-losers')
def top_losers_page():
    """Top Losers page - Union of bottom stocks by 1D, 5D, 20D returns"""
    return render_template('top_losers.html')

@app.route('/daily-review')
def daily_review_page():
    """Daily Review — latest-day VSG union top dr_1 by cap bucket"""
    return render_template('daily_review.html')

@app.route('/volspike-gapper')
def volspike_gapper_page():
    """Volume Spike & Gapper page - Stocks with unusual volume and gap activity"""
    return render_template('volspike_gapper.html')

@app.route('/volspike-gapper-weekly')
def volspike_gapper_weekly_page():
    """Same dataset as /volspike-gapper, grouped by week of event instead of day"""
    return render_template('volspike_gapper_weekly.html')

@app.route('/volspike-gapper-monthly')
def volspike_gapper_monthly_page():
    """Same dataset as /volspike-gapper, grouped by calendar month of event"""
    return render_template('volspike_gapper_monthly.html')

@app.route('/volspike-gapper-90d')
def volspike_gapper_90d_page():
    """VSG events with last_event_date in the last 90 calendar days"""
    return render_template('volspike_gapper_90d.html')

@app.route('/strong-stocks')
def strong_stocks_page():
    """Liquid universe ranked by mcap-adjusted TI65"""
    return render_template('strong_stocks.html')

@app.route('/top-returns-5-20')
def top_returns_5_20_page():
    """Union of top 30 adj dr_5 and top 30 adj dr_20"""
    return render_template('top_returns_5_20.html')

@app.route('/fast-rs')
def fast_rs_page():
    """Fast RS — frozen-weight RS score, mcap-adjusted"""
    return render_template('fast_rs.html')

@app.route('/weekly-review')
def weekly_review_page():
    """Combined weekly review queue (union of the 4 weekly screeners)."""
    return render_template('weekly_review.html')

@app.route('/main-view')
def main_view_page():
    """Main View page - Combined screener view with metrics, volspike/gapper, and tags"""
    return render_template('main_view.html')

@app.route('/main-view-hybrid')
def main_view_hybrid_page():
    """Standalone hybrid mobile layout preview — live data via existing frontend proxies"""
    return render_template('main_view_hybrid.html')

# Mobile-optimized page routes (separate templates; desktop routes unchanged)
@app.route('/m')
@app.route('/m/')
def m_index():
    return render_template('mobile/index.html')

@app.route('/m/main-view')
def m_main_view():
    return render_template('mobile/main_view.html')

@app.route('/m/volspike-gapper-weekly')
def m_volspike_gapper_weekly():
    return render_template('mobile/volspike_gapper_weekly.html')

@app.route('/m/all-stocks')
def m_all_stocks():
    return render_template('mobile/all_stocks.html')

@app.route('/m/rs-screener')
def m_rs_screener():
    return render_template('mobile/rs_screener.html')

@app.route('/m/technical-screener')
def m_technical_screener():
    return render_template('mobile/technical_screener.html')

@app.route('/m/high-sales-growth')
def m_high_sales_growth():
    return render_template('mobile/high_sales_growth.html')

@app.route('/m/daily-review')
def m_daily_review():
    return render_template('mobile/daily_review.html')

@app.route('/m/volspike-gapper')
def m_volspike_gapper():
    return render_template('mobile/volspike_gapper.html')

@app.route('/m/volspike-gapper-90d')
def m_volspike_gapper_90d():
    return render_template('mobile/volspike_gapper_90d.html')

@app.route('/m/strong-stocks')
def m_strong_stocks():
    return render_template('mobile/strong_stocks.html')

@app.route('/m/top-returns-5-20')
def m_top_returns_5_20():
    return render_template('mobile/top_returns_5_20.html')

@app.route('/m/fast-rs')
def m_fast_rs():
    return render_template('mobile/fast_rs.html')

@app.route('/m/top-performance')
def m_top_performance():
    return render_template('mobile/top_performance.html')

@app.route('/m/top-losers')
def m_top_losers():
    return render_template('mobile/top_losers.html')

@app.route('/m/abi-watchlist')
def m_abi_watchlist():
    return render_template('mobile/abi_watchlist.html')

@app.route('/m/abi-general-notes')
def m_abi_general_notes():
    return render_template('mobile/abi_general_notes.html')

@app.route('/m/context')
def m_context():
    return render_template('mobile/context.html')

@app.route('/m/context-2')
def m_context2():
    return render_template('mobile/context2.html')

@app.route('/m/market-brief')
def m_market_brief():
    return render_template('mobile/market_brief.html')

@app.route('/m/market-news')
def m_market_news():
    return render_template('mobile/market_news.html')

@app.route('/m/logs')
def m_logs():
    return render_template('mobile/logs.html')

@app.route('/m/stock/<ticker>')
def m_stock_detail(ticker):
    """Mobile stock detail page"""
    return render_template('mobile/stock.html', ticker=ticker.upper())

@app.route('/technical-screener')
def technical_screener_page():
    """Technical Screener page - intraday/technical setups (reversal, etc.)"""
    return render_template('technical_screener.html')

@app.route('/themes')
def themes_page():
    """Themes page - curated thematic watchlists shown side-by-side"""
    return render_template('themes.html')

@app.route('/etfs')
def etfs_page():
    """ETFs page - thematic / sub-sector ETF performance across timeframes"""
    return render_template('etfs.html')

# User data directory for themes
USER_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'user_data')
THEMES_FILE = os.path.join(USER_DATA_DIR, 'themes.json')
ETFS_FILE = os.path.join(USER_DATA_DIR, 'etfs.json')

@app.route('/api/frontend/etfs', methods=['GET'])
def api_get_etfs():
    """Get the curated ETF groups from user_data/etfs.json"""
    try:
        if os.path.exists(ETFS_FILE):
            with open(ETFS_FILE, 'r') as f:
                return jsonify(json.load(f)), 200
        return jsonify([]), 200
    except Exception as e:
        logger.error(f"Error reading etfs file: {e}")
        return jsonify({'error': 'Failed to read etfs'}), 500

@app.route('/api/frontend/etf-performance')
def api_etf_performance():
    """Proxy: ETF performance (1D/5D/20D/60D/120D) for a list of symbols"""
    symbols = request.args.get('symbols', '')
    data, status_code = make_backend_request(f'/api/etf-performance?symbols={symbols}')
    if data is None:
        return jsonify({'error': 'Failed to fetch ETF performance'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/themes', methods=['GET'])
def api_get_themes():
    """Get user-defined themes from user_data/themes.json"""
    try:
        if os.path.exists(THEMES_FILE):
            with open(THEMES_FILE, 'r') as f:
                themes = json.load(f)
            return jsonify(themes), 200
        else:
            return jsonify([]), 200
    except Exception as e:
        logger.error(f"Error reading themes file: {e}")
        return jsonify({'error': 'Failed to read themes'}), 500

@app.route('/api/frontend/themes', methods=['PUT'])
def api_save_themes():
    """Save themes to user_data/themes.json"""
    try:
        themes = request.get_json()
        os.makedirs(USER_DATA_DIR, exist_ok=True)
        with open(THEMES_FILE, 'w') as f:
            json.dump(themes, f, indent=2)
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logger.error(f"Error saving themes file: {e}")
        return jsonify({'error': 'Failed to save themes'}), 500

@app.route('/api/frontend/theme-proposals', methods=['GET'])
def api_get_theme_proposals():
    """Proxy: theme discovery proposals from backend."""
    data, status_code = make_backend_request('/api/theme-proposals')
    if data is None:
        return jsonify({'error': 'Failed to fetch theme proposals'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/theme-proposals/apply', methods=['POST'])
def api_apply_theme_proposals():
    """Proxy: apply approved theme proposals to themes.json."""
    data, status_code = make_backend_request(
        '/api/theme-proposals/apply',
        method='POST',
        json_data=request.get_json() or {},
    )
    if data is None:
        return jsonify({'error': 'Failed to apply theme proposals'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/all-stocks/by-tickers')
def api_all_stocks_by_tickers():
    """Proxy: AllStocks data filtered to a specific ticker list"""
    tickers = request.args.get('tickers', '')
    data, status_code = make_backend_request(f'/api/AllStocks-ByTickers?tickers={tickers}')
    if data is None:
        return jsonify({'error': 'Failed to fetch themes data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/main-view/by-tickers')
def api_main_view_by_tickers():
    """Proxy endpoint for MainView data filtered to a specific ticker list"""
    tickers = request.args.get('tickers', '')
    data, status_code = make_backend_request(f'/api/MainView-ByTickers?tickers={tickers}')
    if data is None:
        return jsonify({'error': 'Failed to fetch themes data'}), status_code
    return jsonify(data), status_code

@app.route('/stock/<ticker>')
def stock_detail(ticker):
    """Stock detail page"""
    return render_template('stock.html', ticker=ticker.upper())

@app.route('/api/frontend/stock/<ticker>')
def api_stock_detail(ticker):
    """Proxy endpoint for stock details from backend"""
    data, status_code = make_backend_request(f'/api/stock/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch stock details'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/ohlc/<ticker>')
def api_ohlc(ticker):
    """Proxy endpoint for OHLC data from backend"""
    data, status_code = make_backend_request(f'/api/ohlc/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch OHLC data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/earnings-eps/<ticker>')
def api_earnings_eps(ticker):
    """Proxy endpoint for earnings EPS data from backend"""
    data, status_code = make_backend_request(f'/api/earnings-eps/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch earnings EPS data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/volspike-events/<ticker>')
def api_volspike_events(ticker):
    """Proxy endpoint for volume spike and gap events from backend (for chart markers)"""
    data, status_code = make_backend_request(f'/api/volspike-events/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch volspike events'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/all-stocks')
def api_all_stocks():
    """Proxy: AllStocks — full liquid universe (stock_metrics LEFT JOIN volspike/gapper)"""
    data, status_code = make_backend_request('/api/AllStocks')
    if data is None:
        return jsonify({'error': 'Failed to fetch All Stocks data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/top-performance/<market_cap>')
def api_top_performance(market_cap):
    """Proxy endpoint for Top Performance (union of top stocks by 1D, 5D, 20D returns) from backend"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400
    
    data, status_code = make_backend_request(f'/api/TopPerformance-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Top Performance data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/top-losers/<market_cap>')
def api_top_losers(market_cap):
    """Proxy endpoint for Top Losers (union of bottom stocks by 1D, 5D, 20D returns) from backend"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400
    
    data, status_code = make_backend_request(f'/api/BottomPerformance-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Top Losers data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/daily-review/<market_cap>')
def api_daily_review(market_cap):
    """Proxy: Daily Review (latest-day VSG ∪ top dr_1 by cap bucket)"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400

    data, status_code = make_backend_request(f'/api/DailyReview-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Daily Review data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/volspike-gapper/<market_cap>')
def api_volspike_gapper(market_cap):
    """Proxy endpoint for Volume Spike & Gapper data from backend"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400
    
    data, status_code = make_backend_request(f'/api/VolspikeGapper-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Volume Spike & Gapper data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/volspike-gapper-90d/<market_cap>')
def api_volspike_gapper_90d(market_cap):
    """Proxy: VSG rows with last_event_date in the last 90 calendar days"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400

    data, status_code = make_backend_request(
        f'/api/VolspikeGapper-{endpoint_cap}?lookback_days=90'
    )
    if data is None:
        return jsonify({'error': 'Failed to fetch Volume Spike & Gapper 90d data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/strong-stocks/<market_cap>')
def api_strong_stocks(market_cap):
    """Proxy: liquid TI65 universe with mcap-adjusted TI65"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400

    data, status_code = make_backend_request(f'/api/StrongStocks-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Strong Stocks data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/strong-stocks-setup')
def api_strong_stocks_setup():
    """Proxy: MA-consolidation metrics for liquid TI65 names"""
    data, status_code = make_backend_request('/api/StrongStocks-Setup')
    if data is None:
        return jsonify({'error': 'Failed to fetch Strong Stocks setup data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/top-returns-5-20/<market_cap>')
def api_top_returns_5_20(market_cap):
    """Proxy: union of top 30 adj dr_5 and top 30 adj dr_20"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400

    data, status_code = make_backend_request(f'/api/TopReturns520-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Top 5D/20D data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/weekly-review')
def api_weekly_review():
    data, status_code = make_backend_request('/api/WeeklyReview')
    if data is None:
        return jsonify({'error': 'Failed to fetch weekly review'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/weekly-review-config')
def api_weekly_review_config():
    data, status_code = make_backend_request('/api/WeeklyReview-Config')
    if data is None:
        return jsonify({'error': 'Failed to fetch weekly review config'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/fast-rs/<market_cap>')
def api_fast_rs(market_cap):
    """Proxy: liquid Fast RS universe with mcap-adjusted RS score"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400

    data, status_code = make_backend_request(f'/api/FastRs-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Fast RS data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/volspike-gapper-setup')
def api_volspike_gapper_setup():
    """Proxy endpoint for MA-consolidation metrics keyed by ticker"""
    data, status_code = make_backend_request('/api/VolspikeGapper-Setup')
    if data is None:
        return jsonify({'error': 'Failed to fetch Volume Spike & Gapper setup data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/main-view/<market_cap>')
def api_main_view(market_cap):
    """Proxy endpoint for Main View data from backend"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400
    
    data, status_code = make_backend_request(f'/api/MainView-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Main View data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/technical-screener/<criterion>/<market_cap>')
def api_technical_screener(criterion, market_cap):
    """Proxy endpoint for Technical Screener data from backend.

    Supported criteria:
      - reversal: biggest low-to-close reversal % for the latest trading day
    """
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    criterion_map = {
        'reversal': 'Reversal',
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    endpoint_criterion = criterion_map.get(criterion.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400
    if not endpoint_criterion:
        return jsonify({'error': 'Invalid technical screener criterion'}), 400

    data, status_code = make_backend_request(f'/api/TechnicalScreener-{endpoint_criterion}-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Technical Screener data'}), status_code
    return jsonify(data), status_code

@app.route('/context')
def context_page():
    """Context page - Market context with QQQ/SPY charts and DMA analysis"""
    return render_template('context.html')

@app.route('/context-2')
def context2_page():
    """Context 2 page - Research links, commentary, and calendar events"""
    return render_template('context2.html')

@app.route('/high-sales-growth')
def high_sales_growth_page():
    """High Sales Growth page - Stocks with high revenue growth"""
    return render_template('high_sales_growth.html')

@app.route('/api/frontend/high-sales-growth/<market_cap>')
def api_high_sales_growth(market_cap):
    """Proxy endpoint for High Sales Growth data from backend"""
    cap_map = {
        'all': 'All',
        'micro': 'MicroCap',
        'small': 'SmallCap',
        'mid': 'MidCap',
        'large': 'LargeCap',
        'mega': 'MegaCap'
    }
    endpoint_cap = cap_map.get(market_cap.lower())
    if not endpoint_cap:
        return jsonify({'error': 'Invalid market cap category'}), 400
    
    data, status_code = make_backend_request(f'/api/HighSalesGrowth-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch High Sales Growth data'}), status_code
    return jsonify(data), status_code

# Stock Notes + AI Research proxy endpoints removed: the underlying
# DB-backed store has been deprecated. Per-ticker notes are now served by
# the file-only abi_ticker_notes endpoints. The AI research integration
# (Perplexity / Claude) was removed along with stock_notes; if you want it
# back, reintroduce it as a feature that writes into abi_ticker_notes.json.

# Stock Preferences endpoints removed: superseded by file-only
# abi_watchlist (favorites) and abi_dislikes (dislikes) endpoints elsewhere
# in this file. The UI in stock.html / main_view.html no longer renders the
# favorite/dislike controls that called these.

# ============================================================
# Abi General Notes Endpoints
# ============================================================

@app.route('/abi-notes')
def abi_notes_legacy_redirect():
    """Old URL; permanent redirect to Abi General Notes."""
    return redirect('/abi-general-notes', code=301)

@app.route('/abi-general-notes')
def abi_general_notes_page():
    """Abi General Notes page - personal date-based notes"""
    return render_template('abi_general_notes.html')

@app.route('/api/frontend/abi-general-notes', methods=['GET'])
def api_get_abi_general_notes():
    """Proxy endpoint to get all abi general notes"""
    # Pass through query parameters
    params = request.args.to_dict()
    query_string = '&'.join(f'{k}={v}' for k, v in params.items())
    endpoint = f'/api/abi-general-notes?{query_string}' if query_string else '/api/abi-general-notes'
    data, status_code = make_backend_request(endpoint)
    if data is None:
        return jsonify({'error': 'Failed to fetch abi general notes'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-general-notes', methods=['POST'])
def api_create_abi_general_note():
    """Proxy endpoint to create a new abi general note"""
    json_data = request.get_json()
    data, status_code = make_backend_request('/api/abi-general-notes', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to create abi general note'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-general-notes/<int:note_id>', methods=['GET'])
def api_get_abi_general_note(note_id):
    """Proxy endpoint to get a specific abi general note"""
    data, status_code = make_backend_request(f'/api/abi-general-notes/{note_id}')
    if data is None:
        return jsonify({'error': 'Failed to fetch abi general note'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-general-notes/<int:note_id>', methods=['PUT'])
def api_update_abi_general_note(note_id):
    """Proxy endpoint to update an abi general note"""
    json_data = request.get_json()
    data, status_code = make_backend_request(f'/api/abi-general-notes/{note_id}', method='PUT', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to update abi general note'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-general-notes/<int:note_id>', methods=['DELETE'])
def api_delete_abi_general_note(note_id):
    """Proxy endpoint to delete an abi general note"""
    data, status_code = make_backend_request(f'/api/abi-general-notes/{note_id}', method='DELETE', json_data={})
    if data is None:
        return jsonify({'error': 'Failed to delete abi general note'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-general-notes/tags', methods=['GET'])
def api_get_abi_general_notes_tags():
    """Proxy endpoint to get all unique tags"""
    data, status_code = make_backend_request('/api/abi-general-notes/tags')
    if data is None:
        return jsonify({'error': 'Failed to fetch abi general notes tags'}), status_code
    return jsonify(data), status_code

# ============================================================
# Stock Detail Data Endpoints
# ============================================================

@app.route('/api/frontend/earnings/<ticker>')
def api_earnings(ticker):
    """Proxy endpoint for full earnings history from backend"""
    data, status_code = make_backend_request(f'/api/earnings/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch earnings data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/analyst-estimates/<ticker>')
def api_analyst_estimates(ticker):
    """Proxy endpoint for analyst estimates from backend"""
    data, status_code = make_backend_request(f'/api/analyst-estimates/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch analyst estimates'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/ratios-ttm/<ticker>')
def api_ratios_ttm(ticker):
    """Proxy endpoint for TTM ratios from backend"""
    data, status_code = make_backend_request(f'/api/ratios-ttm/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch ratios data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/company-profile/<ticker>')
def api_company_profile(ticker):
    """Proxy endpoint for company profile from backend"""
    data, status_code = make_backend_request(f'/api/company-profile/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch company profile'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/stock-news/<ticker>')
def api_stock_news(ticker):
    """Proxy endpoint for merged stock news (FMP + Yahoo + Seeking Alpha) via backend"""
    limit = request.args.get('limit', 40, type=int)
    data, status_code = make_backend_request(f'/api/stock-news/{ticker}?limit={limit}')
    if data is None:
        return jsonify({'error': 'Failed to fetch stock news'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/benzinga-news/<ticker>', methods=['GET'])
def api_benzinga_news_get(ticker):
    """Proxy: cached Benzinga news from database."""
    limit = request.args.get('limit', 40, type=int)
    data, status_code = make_backend_request(f'/api/benzinga-news/{ticker}?limit={limit}')
    if data is None:
        return jsonify({'error': 'Failed to load Benzinga news'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/benzinga-news/<ticker>', methods=['POST'])
def api_benzinga_news_refresh(ticker):
    """Proxy: refresh Benzinga news from API and store in database."""
    limit = request.args.get('limit', 40, type=int)
    data, status_code = make_backend_request(
        f'/api/benzinga-news/{ticker}?limit={limit}',
        method='POST',
    )
    if data is None:
        return jsonify({'error': 'Failed to refresh Benzinga news'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/benzinga-news/market', methods=['GET'])
def api_benzinga_market_news():
    """Proxy: market-wide Benzinga news from database cache."""
    limit = request.args.get('limit', 200, type=int)
    channel = request.args.get('channel', '')
    params = f'limit={limit}'
    if channel:
        params += f'&channel={channel}'
    data, status_code = make_backend_request(f'/api/benzinga-news/market?{params}')
    if data is None:
        return jsonify({'error': 'Failed to load market Benzinga news'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/benzinga-news/market', methods=['POST'])
def api_benzinga_market_news_refresh():
    """Proxy: fetch fresh market Benzinga news from API and upsert to DB."""
    limit = request.args.get('limit', 200, type=int)
    api_limit = request.args.get('api_limit', 100, type=int)
    data, status_code = make_backend_request(
        f'/api/benzinga-news/market?limit={limit}&api_limit={api_limit}',
        method='POST',
    )
    if data is None:
        return jsonify({'error': 'Failed to refresh Benzinga news from API'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/market-news/fmp', methods=['GET'])
def api_market_fmp_news():
    """Proxy: general market news from FMP."""
    limit = request.args.get('limit', 100, type=int)
    data, status_code = make_backend_request(f'/api/market-news/fmp?limit={limit}')
    if data is None:
        return jsonify({'error': 'Failed to load FMP market news'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/market-news/seeking-alpha', methods=['GET'])
def api_market_seeking_alpha_news():
    """Proxy: general market news from Seeking Alpha."""
    limit = request.args.get('limit', 100, type=int)
    data, status_code = make_backend_request(f'/api/market-news/seeking-alpha?limit={limit}')
    if data is None:
        return jsonify({'error': 'Failed to load Seeking Alpha market news'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/latest-date')
def api_latest_date():
    """Proxy endpoint for latest OHLC date from backend"""
    data, status_code = make_backend_request('/api/latest_date')
    if data is None:
        return jsonify({'error': 'Failed to fetch latest date'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/sector-performance')
def api_sector_performance():
    """Proxy endpoint for sector/index performance from backend"""
    data, status_code = make_backend_request('/api/sector-performance')
    if data is None:
        return jsonify({'error': 'Failed to fetch sector performance data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/homepage')
def api_homepage():
    """Proxy endpoint for homepage data (indices, commodities, sectors with DMA)"""
    data, status_code = make_backend_request('/api/homepage')
    if data is None:
        return jsonify({'error': 'Failed to fetch homepage data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/market-breadth')
def api_market_breadth():
    """Proxy endpoint for market breadth data (1 year history for charting)"""
    data, status_code = make_backend_request('/api/market-breadth')
    if data is None:
        return jsonify({'error': 'Failed to fetch market breadth data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/index-ohlc/<symbol>')
def api_index_ohlc(symbol):
    """Proxy endpoint for index/ETF OHLC data from backend"""
    data, status_code = make_backend_request(f'/api/index-ohlc/{symbol}')
    if data is None:
        return jsonify({'error': 'Failed to fetch index OHLC data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/vix-latest')
def api_vix_latest():
    """Proxy endpoint for latest VIX value from backend"""
    data, status_code = make_backend_request('/api/vix-latest')
    if data is None:
        return jsonify({'error': 'Failed to fetch VIX data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/treasury-10y')
def api_treasury_10y():
    """Proxy endpoint for 10-Year Treasury yield from backend"""
    data, status_code = make_backend_request('/api/treasury-10y')
    if data is None:
        return jsonify({'error': 'Failed to fetch Treasury yield'}), status_code
    return jsonify(data), status_code

# ============================================================
# Abi Watchlist Endpoints
# ============================================================

@app.route('/abi-watchlist')
def abi_watchlist_page():
    """Abi Watchlist page - personal watchlist with Main View layout"""
    return render_template('abi_watchlist.html')

@app.route('/abi-trades')
def abi_trades_page():
    """Buy/short trade candidates."""
    return render_template('abi_trades.html')

@app.route('/api/frontend/abi-trades', methods=['GET'])
def api_get_abi_trades():
    data, status_code = make_backend_request('/api/abi-trades')
    if data is None:
        return jsonify({'error': 'Failed to fetch trades'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-trades', methods=['POST'])
def api_add_abi_trade():
    json_data = request.get_json()
    data, status_code = make_backend_request('/api/abi-trades', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to add trade'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-trades/<ticker>', methods=['DELETE'])
def api_delete_abi_trade(ticker):
    data, status_code = make_backend_request(f'/api/abi-trades/{ticker}', method='DELETE', json_data={})
    if data is None:
        return jsonify({'error': 'Failed to remove trade'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-trades/data')
def api_abi_trades_data():
    data, status_code = make_backend_request('/api/abi-trades/data')
    if data is None:
        return jsonify({'error': 'Failed to fetch trades data'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-passes', methods=['POST'])
def api_add_abi_pass():
    json_data = request.get_json()
    data, status_code = make_backend_request('/api/abi-passes', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to pass ticker'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-watchlist', methods=['GET'])
def api_get_abi_watchlist():
    """Proxy endpoint to get all watchlist items"""
    data, status_code = make_backend_request('/api/abi-watchlist')
    if data is None:
        return jsonify({'error': 'Failed to fetch watchlist'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-watchlist', methods=['POST'])
def api_add_to_abi_watchlist():
    """Proxy endpoint to add a ticker to the watchlist"""
    json_data = request.get_json()
    data, status_code = make_backend_request('/api/abi-watchlist', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to add to watchlist'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-watchlist/<ticker>', methods=['PUT'])
def api_update_abi_watchlist(ticker):
    """Proxy endpoint to update watchlist notes"""
    json_data = request.get_json()
    data, status_code = make_backend_request(f'/api/abi-watchlist/{ticker}', method='PUT', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to update watchlist'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-watchlist/<ticker>', methods=['DELETE'])
def api_delete_from_abi_watchlist(ticker):
    """Proxy endpoint to remove a ticker from the watchlist"""
    data, status_code = make_backend_request(f'/api/abi-watchlist/{ticker}', method='DELETE', json_data={})
    if data is None:
        return jsonify({'error': 'Failed to remove from watchlist'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-watchlist/batch-check', methods=['POST'])
def api_batch_check_abi_watchlist():
    """Proxy endpoint to check which tickers are in the watchlist"""
    json_data = request.get_json()
    data, status_code = make_backend_request('/api/abi-watchlist/batch-check', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to check watchlist'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-watchlist/data')
def api_abi_watchlist_data():
    """Proxy endpoint for watchlist data with MainView metrics"""
    data, status_code = make_backend_request('/api/abi-watchlist/data')
    if data is None:
        return jsonify({'error': 'Failed to fetch watchlist data'}), status_code
    return jsonify(data), status_code

# ============================================================
# Abi Dislikes Endpoints (parallel to watchlist, thumbs-down list with notes)
# ============================================================
# Replaces the legacy DB-backed `preference='dislike'`. Dislike entries get
# filtered out of the daily screener pipeline and are surfaced as "you blocked
# this and here's why" anywhere they would otherwise appear.

@app.route('/abi-dislikes')
def abi_dislikes_page():
    """Abi Dislikes page - thumbs-down list with notes."""
    return render_template('abi_dislikes.html')

@app.route('/api/frontend/abi-dislikes', methods=['GET'])
def api_get_abi_dislikes():
    data, status_code = make_backend_request('/api/abi-dislikes')
    if data is None:
        return jsonify({'error': 'Failed to fetch dislikes'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-dislikes', methods=['POST'])
def api_add_to_abi_dislikes():
    json_data = request.get_json()
    data, status_code = make_backend_request('/api/abi-dislikes', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to add to dislikes'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-dislikes/<ticker>', methods=['DELETE'])
def api_delete_from_abi_dislikes(ticker):
    data, status_code = make_backend_request(f'/api/abi-dislikes/{ticker}', method='DELETE', json_data={})
    if data is None:
        return jsonify({'error': 'Failed to remove from dislikes'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-dislikes/batch-check', methods=['POST'])
def api_batch_check_abi_dislikes():
    json_data = request.get_json()
    data, status_code = make_backend_request('/api/abi-dislikes/batch-check', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to check dislikes'}), status_code
    return jsonify(data), status_code

# ============================================================
# Abi Ticker Notes Endpoints (per-ticker notes, decoupled from
# watchlist/dislike membership)
# ============================================================
# Abi ticker notes can exist for any ticker. The daily screener only consumes
# notes for tickers on the watchlist.

@app.route('/api/frontend/abi-ticker-notes', methods=['GET'])
def api_get_abi_ticker_notes_all():
    """Proxy endpoint to get all Abi ticker notes."""
    data, status_code = make_backend_request('/api/abi-ticker-notes')
    if data is None:
        return jsonify({'error': 'Failed to fetch Abi ticker notes'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-ticker-notes/<ticker>', methods=['GET'])
def api_get_abi_ticker_note(ticker):
    """Proxy endpoint to get Abi ticker notes for a single ticker."""
    data, status_code = make_backend_request(f'/api/abi-ticker-notes/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch Abi ticker notes'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-ticker-notes/<ticker>', methods=['PUT'])
def api_upsert_abi_ticker_note(ticker):
    """Proxy endpoint to create or update Abi ticker notes for a ticker."""
    json_data = request.get_json()
    data, status_code = make_backend_request(
        f'/api/abi-ticker-notes/{ticker}', method='PUT', json_data=json_data
    )
    if data is None:
        return jsonify({'error': 'Failed to save Abi ticker notes'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-ticker-notes/<ticker>', methods=['DELETE'])
def api_delete_abi_ticker_note(ticker):
    """Proxy endpoint to delete Abi ticker notes for a ticker."""
    data, status_code = make_backend_request(
        f'/api/abi-ticker-notes/{ticker}', method='DELETE', json_data={}
    )
    if data is None:
        return jsonify({'error': 'Failed to delete Abi ticker notes'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/abi-ticker-notes/batch-check', methods=['POST'])
def api_batch_check_abi_ticker_notes():
    """Proxy endpoint to fetch Abi ticker notes for a list of tickers."""
    json_data = request.get_json()
    data, status_code = make_backend_request(
        '/api/abi-ticker-notes/batch-check', method='POST', json_data=json_data
    )
    if data is None:
        return jsonify({'error': 'Failed to fetch Abi ticker notes'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/abi-chart-notes/<ticker>', methods=['GET'])
def api_get_abi_chart_notes(ticker):
    data, status_code = make_backend_request(f'/api/abi-chart-notes/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch chart notes'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/abi-chart-notes/<ticker>', methods=['PUT'])
def api_upsert_abi_chart_notes(ticker):
    json_data = request.get_json()
    data, status_code = make_backend_request(
        f'/api/abi-chart-notes/{ticker}', method='PUT', json_data=json_data
    )
    if data is None:
        return jsonify({'error': 'Failed to save chart notes'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/abi-chart-notes/<ticker>', methods=['DELETE'])
def api_delete_abi_chart_notes(ticker):
    data, status_code = make_backend_request(
        f'/api/abi-chart-notes/{ticker}', method='DELETE', json_data={}
    )
    if data is None:
        return jsonify({'error': 'Failed to delete chart notes'}), status_code
    return jsonify(data), status_code

# ============================================================
# Daily Shortlist Endpoints
# ============================================================

@app.route('/daily-shortlist')
def daily_shortlist_page():
    """Daily Shortlist page - screened candidates with Picks/Watch/Rejected tabs"""
    return render_template('daily_shortlist.html')

@app.route('/api/frontend/daily-shortlist/dates')
def api_daily_shortlist_dates():
    """Proxy: list of dates with daily shortlist artifacts"""
    data, status_code = make_backend_request('/api/daily-shortlist/dates')
    if data is None:
        return jsonify({'error': 'Failed to fetch dates'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/daily-shortlist/<date>')
def api_daily_shortlist_for_date(date):
    """Proxy: full audit artifact for a specific date"""
    data, status_code = make_backend_request(f'/api/daily-shortlist/{date}')
    if data is None:
        return jsonify({'error': 'Failed to fetch daily shortlist'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/daily-shortlist/run', methods=['POST'])
def api_daily_shortlist_run():
    """Proxy: kick off a daily shortlist pipeline run"""
    json_data = request.get_json() or {}
    data, status_code = make_backend_request(
        '/api/daily-shortlist/run', method='POST', json_data=json_data
    )
    if data is None:
        return jsonify({'error': 'Failed to start run'}), status_code
    return jsonify(data), status_code

# ----- Daily Shortlist Feedback proxies -----

@app.route('/api/frontend/daily-shortlist/feedback/<date>', methods=['GET'])
def api_daily_shortlist_feedback_for_date(date):
    """Proxy: feedback for a given date as {ticker: record}"""
    data, status_code = make_backend_request(f'/api/daily-shortlist/feedback/{date}')
    if data is None:
        return jsonify({'error': 'Failed to fetch feedback'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/daily-shortlist/feedback/<date>/<ticker>', methods=['PUT'])
def api_daily_shortlist_feedback_upsert(date, ticker):
    """Proxy: upsert feedback for (date, ticker)"""
    json_data = request.get_json() or {}
    data, status_code = make_backend_request(
        f'/api/daily-shortlist/feedback/{date}/{ticker}',
        method='PUT', json_data=json_data,
    )
    if data is None:
        return jsonify({'error': 'Failed to save feedback'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/daily-shortlist/feedback/<date>/<ticker>', methods=['DELETE'])
def api_daily_shortlist_feedback_delete(date, ticker):
    """Proxy: delete a feedback entry for (date, ticker)"""
    data, status_code = make_backend_request(
        f'/api/daily-shortlist/feedback/{date}/{ticker}',
        method='DELETE', json_data={},
    )
    if data is None:
        return jsonify({'error': 'Failed to delete feedback'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/daily-shortlist/feedback-all', methods=['GET'])
def api_daily_shortlist_feedback_all():
    """Proxy: all feedback entries, flattened. Optional ?limit=N."""
    qs = request.query_string.decode() if request.query_string else ''
    endpoint = '/api/daily-shortlist/feedback' + (('?' + qs) if qs else '')
    data, status_code = make_backend_request(endpoint)
    if data is None:
        return jsonify({'error': 'Failed to fetch all feedback'}), status_code
    return jsonify(data), status_code

# ----- Daily Themes (visualization) -----

@app.route('/daily-themes')
def daily_themes_page():
    """Daily Themes page - visualize the theme vector for any pipeline run"""
    return render_template('daily_themes.html')

@app.route('/api/frontend/daily-shortlist/themes/dates', methods=['GET'])
def api_daily_themes_dates():
    """Proxy: list of dates that have a theme vector artifact"""
    data, status_code = make_backend_request('/api/daily-shortlist/themes/dates')
    if data is None:
        return jsonify({'error': 'Failed to fetch theme dates'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/daily-shortlist/themes/<date>', methods=['GET'])
def api_daily_themes_for_date(date):
    """Proxy: theme vector + per-source raw lists for a given date"""
    data, status_code = make_backend_request(f'/api/daily-shortlist/themes/{date}')
    if data is None:
        return jsonify({'error': 'Failed to fetch themes'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/rs-screener/<market_cap>')
def api_rs_screener(market_cap):
    """Proxy endpoint for RS Screener data from backend"""
    data, status_code = make_backend_request(f'/api/rs-screener/{market_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch RS screener data'}), status_code
    return jsonify(data), status_code

# ============================================================
# Logs Endpoints
# ============================================================

@app.route('/logs')
def logs_page():
    """Logs viewer page"""
    return render_template('logs.html')

@app.route('/api/frontend/logs')
def api_list_logs():
    """List available log files"""
    from pathlib import Path
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    et = ZoneInfo('America/New_York')

    def _est_edt_abbr(dt_et):
        return 'EDT' if dt_et.dst() else 'EST'

    # Look for logs in the project root's logs directory
    # In Docker, the project is mounted at /app
    logs_dir = Path('/app/logs')
    if not logs_dir.exists():
        logs_dir = Path(__file__).parent.parent / 'logs'
    
    log_files = []
    if logs_dir.exists():
        for log_file in sorted(logs_dir.glob('*.log'), key=lambda x: x.stat().st_mtime, reverse=True):
            stat = log_file.stat()
            mt_et = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).astimezone(et)
            log_files.append({
                'name': log_file.name,
                'size': stat.st_size,
                'modified': stat.st_mtime,
                'modified_str': (
                    f'{mt_et.year}-{mt_et.month:02d}-{mt_et.day:02d} '
                    f'{mt_et.hour:02d}:{mt_et.minute:02d}:{mt_et.second:02d} '
                    f'{_est_edt_abbr(mt_et)}'
                ),
            })
    
    return jsonify({'logs': log_files})

@app.route('/api/frontend/logs/<filename>')
def api_get_log(filename):
    """Get contents of a specific log file"""
    from pathlib import Path
    import re
    
    # Validate filename to prevent path traversal
    if not re.match(r'^[\w\-\.]+\.log$', filename):
        return jsonify({'error': 'Invalid filename'}), 400
    
    # Look for logs in the project root's logs directory
    logs_dir = Path('/app/logs')
    if not logs_dir.exists():
        logs_dir = Path(__file__).parent.parent / 'logs'
    
    log_path = logs_dir / filename
    
    if not log_path.exists():
        return jsonify({'error': 'Log file not found'}), 404
    
    try:
        # Read last N lines or full file if small
        max_lines = request.args.get('lines', type=int, default=None)
        
        with open(log_path, 'r') as f:
            if max_lines:
                # Read last N lines efficiently
                from collections import deque
                lines = deque(f, maxlen=max_lines)
                content = ''.join(lines)
            else:
                content = f.read()
        
        return jsonify({
            'filename': filename,
            'content': content,
            'size': log_path.stat().st_size
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================
# Market News Page
# ============================================================

@app.route('/market-news')
def market_news_page():
    """Market News page - aggregated news from multiple sources"""
    return render_template('market_news.html')

# ============================================================
# Market Brief Endpoints
# ============================================================

@app.route('/market-brief/history')
def market_brief_history_page():
    """Market Brief History — browse past AI-generated market briefs."""
    return render_template('market_brief_history.html')

@app.route('/market-brief')
def market_brief_page():
    """Market Brief page - daily AI-generated market summary"""
    return render_template('market_brief.html')

@app.route('/api/frontend/market-brief/dates', methods=['GET'])
def api_market_brief_dates():
    """Proxy endpoint to list market brief dates"""
    data, status_code = make_backend_request('/api/market-brief/dates')
    if data is None:
        return jsonify({'error': 'Failed to fetch market brief dates'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/market-brief/<date_str>', methods=['GET'])
def api_market_brief_for_date(date_str):
    """Proxy endpoint to get market brief for a specific date"""
    data, status_code = make_backend_request(f'/api/market-brief/{date_str}')
    if data is None:
        return jsonify({'error': 'Failed to fetch market brief'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/market-brief/<date_str>/costs', methods=['GET'])
def api_market_brief_costs(date_str):
    """Proxy endpoint for live run cost polling."""
    data, status_code = make_backend_request(f'/api/market-brief/{date_str}/costs')
    if data is None:
        return jsonify({'error': 'Failed to fetch run costs'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/market-brief/<date_str>/pdf', methods=['GET'])
def api_market_brief_pdf(date_str):
    """Proxy PDF download from backend."""
    try:
        url = f"{BACKEND_URL}/api/market-brief/{date_str}/pdf"
        response = requests.get(url, timeout=180)
        if response.status_code >= 400:
            try:
                err = response.json()
                msg = err.get('error', response.text)
            except ValueError:
                msg = response.text or 'PDF export failed'
            return jsonify({'error': msg}), response.status_code
        headers = {}
        cd = response.headers.get('Content-Disposition')
        if cd:
            headers['Content-Disposition'] = cd
        return Response(response.content, mimetype='application/pdf', headers=headers)
    except requests.RequestException as e:
        logger.error('PDF proxy error: %s', e)
        return jsonify({'error': 'Failed to export PDF'}), 500

@app.route('/api/frontend/market-brief/generate', methods=['POST'])
def api_market_brief_generate():
    """Proxy endpoint to run Steps 3–4 pipeline on existing source data."""
    json_data = request.get_json() or {}
    data, status_code = make_backend_request(
        '/api/market-brief/generate', method='POST', json_data=json_data
    )
    if data is None:
        return jsonify({'error': 'Failed to start market brief pipeline'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/market-brief/run', methods=['POST'])
def api_market_brief_run():
    """Proxy endpoint to trigger market brief generation"""
    json_data = request.get_json() or {}
    data, status_code = make_backend_request('/api/market-brief/run', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to start market brief run'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/market-brief-losers/generate', methods=['POST'])
def api_market_brief_losers_generate():
    """Proxy endpoint to start R1D losers brief pipeline."""
    json_data = request.get_json() or {}
    data, status_code = make_backend_request(
        '/api/market-brief-losers/generate', method='POST', json_data=json_data
    )
    if data is None:
        return jsonify({'error': 'Failed to start losers brief pipeline'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/market-brief-losers/<date_str>/costs', methods=['GET'])
def api_market_brief_losers_costs(date_str):
    """Proxy endpoint for losers brief run progress."""
    data, status_code = make_backend_request(f'/api/market-brief-losers/{date_str}/costs')
    if data is None:
        return jsonify({'error': 'Failed to fetch losers brief run status'}), status_code
    return jsonify(data), status_code

@app.route('/api/frontend/auto-commit', methods=['POST'])
def api_auto_commit():
    """Proxy to backend: run auto_commit.sh for user_data backup."""
    json_data = request.get_json() or {}
    try:
        url = f'{BACKEND_URL}/api/auto-commit'
        r = requests.post(url, json=json_data, timeout=45)
        try:
            payload = r.json()
        except ValueError:
            payload = {
                'status': 'error',
                'message': 'Unexpected response from backend',
                'error': (r.text or '')[:500],
            }
        return jsonify(payload), r.status_code
    except requests.Timeout:
        logger.error('Auto-commit proxy: backend request timed out')
        return jsonify({'status': 'error', 'message': 'Backend timed out'}), 504
    except requests.RequestException as e:
        logger.error('Auto-commit proxy: %s', e)
        return jsonify({'status': 'error', 'message': str(e)}), 502

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
