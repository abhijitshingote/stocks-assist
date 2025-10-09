from flask import Flask, render_template, request, jsonify, redirect, url_for
from datetime import datetime, timedelta
import os
import logging
import pytz
import requests
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(
    __name__,
    static_folder='static',
    template_folder='templates'
)

# Backend API base URL
BACKEND_URL = os.getenv('BACKEND_URL', 'http://localhost:5001')

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

def make_backend_request(endpoint, method='GET', data=None, params=None):
    """Helper function to make requests to the backend API"""
    try:
        url = f"{BACKEND_URL}{endpoint}"
        headers = {'Content-Type': 'application/json'}

        if method == 'GET':
            response = requests.get(url, params=params, headers=headers)
        elif method == 'POST':
            response = requests.post(url, json=data, headers=headers)
        elif method == 'PUT':
            response = requests.put(url, json=data, headers=headers)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        if response.status_code >= 400:
            logger.error(f"Backend API error {response.status_code}: {response.text}")
            return None

        if response.headers.get('content-type', '').startswith('application/json'):
            return response.json()
        else:
            return response.text

    except requests.RequestException as e:
        logger.error(f"Error connecting to backend API: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in backend request: {str(e)}")
        return None

# Template rendering routes

@app.route('/')
def index():
    """Home page"""
    return render_template('index.html')

# ---------------------------------------------
# Dashboard with upcoming market-moving events
# ---------------------------------------------
BACKUP_DIR = Path(os.getenv("USER_DATA_DIR", "user_data"))
BACKUP_DIR.mkdir(exist_ok=True)

def load_cached_market_events_html():
    """Load cached upcoming market-moving events HTML from disk."""
    try:
        MARKET_EVENTS_CACHE_FILE = BACKUP_DIR / "market_events.html"
        if MARKET_EVENTS_CACHE_FILE.exists():
            return MARKET_EVENTS_CACHE_FILE.read_text(encoding='utf-8')
        return "<p>No cached events found. Run the refresh script to populate them.</p>"
    except Exception as e:
        logger.error(f"Error reading cached market events: {e}")
        return "<p>Unable to load cached events.</p>"

@app.route('/dashboard')
def dashboard():
    """Dashboard with upcoming market-moving events"""
    events_html = load_cached_market_events_html()
    # Derive last updated string from cache mtime
    try:
        MARKET_EVENTS_CACHE_FILE = BACKUP_DIR / "market_events.html"
        if MARKET_EVENTS_CACHE_FILE.exists():
            mtime = MARKET_EVENTS_CACHE_FILE.stat().st_mtime
            eastern = pytz.timezone('US/Eastern')
            last_updated = datetime.fromtimestamp(mtime, eastern).strftime('%Y-%m-%d %I:%M %p %Z')
        else:
            last_updated = None
    except Exception:
        last_updated = None

    return render_template('dashboard.html', market_events_html=events_html, events_last_updated=last_updated)

@app.route('/Return1D')
def return_1d():
    """1-day return scanner page"""
    return render_template('return_1d.html')

@app.route('/Return5D')
def return_5d():
    """5-day return scanner page"""
    return render_template('return_5d.html')

@app.route('/Return20D')
def return_20d():
    """20-day return scanner page"""
    return render_template('return_20d.html')

@app.route('/Return60D')
def return_60d():
    """60-day return scanner page"""
    return render_template('return_60d.html')

@app.route('/Return120D')
def return_120d():
    """120-day return scanner page"""
    return render_template('return_120d.html')

@app.route('/Gapper')
def gapper():
    """Gap scanner page"""
    return render_template('gapper.html')

@app.route('/Volume')
def volume():
    """Volume scanner page"""
    return render_template('volume.html')

@app.route('/AllReturns')
def all_returns():
    """All returns scanner page"""
    return render_template('all_returns.html')

@app.route('/AbiShortList')
def abi_shortlist():
    """ABI shortlist page"""
    return render_template('abi_shortlist.html')

@app.route('/AbiBlackList')
def abi_blacklist():
    """ABI blacklist page"""
    return render_template('abi_blacklist.html')

@app.route('/RSI')
def rsi_table():
    """RSI table page"""
    return render_template('rsi_table.html')

@app.route('/SeaChange')
def sea_change():
    """Sea change scanner page"""
    return render_template('sea_change.html')

@app.route('/ticker/<ticker>')
def ticker_detail(ticker):
    """Ticker detail page with company info and candlestick chart"""
    return render_template('ticker_detail.html', ticker=ticker.upper())

@app.route('/stock-research')
def stock_research():
    """Display stock research results from HTML template"""
    return render_template('stock_research_results.html')

# AJAX endpoints for frontend data fetching

@app.route('/api/frontend/comments/<ticker>')
def get_comments_frontend(ticker):
    """Get comments for frontend via backend API"""
    comments = make_backend_request(f'/api/comments/{ticker}')
    if comments is None:
        return jsonify({'error': 'Failed to fetch comments'}), 500
    return jsonify(comments)

@app.route('/api/frontend/comments/<ticker>', methods=['POST'])
def add_comment_frontend(ticker):
    """Add comment via backend API"""
    data = request.get_json()
    result = make_backend_request(f'/api/comments/{ticker}', method='POST', data=data)
    if result is None:
        return jsonify({'error': 'Failed to add comment'}), 500
    return jsonify(result)

@app.route('/api/frontend/ai-comments/<ticker>')
def get_pending_ai_comments_frontend(ticker):
    """Get pending AI comments via backend API"""
    comments = make_backend_request(f'/api/ai-comments/{ticker}')
    if comments is None:
        return jsonify({'error': 'Failed to fetch AI comments'}), 500
    return jsonify(comments)

@app.route('/api/frontend/ai-comments/<ticker>', methods=['POST'])
def add_ai_comment_frontend(ticker):
    """Add AI comment via backend API"""
    data = request.get_json()
    result = make_backend_request(f'/api/ai-comments/{ticker}', method='POST', data=data)
    if result is None:
        return jsonify({'error': 'Failed to add AI comment'}), 500
    return jsonify(result)

@app.route('/api/frontend/ai-comments/<int:comment_id>/review', methods=['POST'])
def review_ai_comment_frontend(comment_id):
    """Review AI comment via backend API"""
    data = request.get_json()
    result = make_backend_request(f'/api/ai-comments/{comment_id}/review', method='POST', data=data)
    if result is None:
        return jsonify({'error': 'Failed to review comment'}), 500
    return jsonify(result)

@app.route('/api/frontend/latest_date')
def get_latest_date_frontend():
    """Get latest date via backend API"""
    result = make_backend_request('/api/latest_date')
    if result is None:
        return jsonify({'error': 'Failed to fetch latest date'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return1D-Above200M')
def get_return_1d_above_200m_frontend():
    """Get 1D returns above 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return1D-Above200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 1D returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return1D-Below200M')
def get_return_1d_below_200m_frontend():
    """Get 1D returns below 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return1D-Below200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 1D returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return5D-Above200M')
def get_return_5d_above_200m_frontend():
    """Get 5D returns above 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return5D-Above200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 5D returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return5D-Below200M')
def get_return_5d_below_200m_frontend():
    """Get 5D returns below 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return5D-Below200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 5D returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return20D-Above200M')
def get_return_20d_above_200m_frontend():
    """Get 20D returns above 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return20D-Above200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 20D returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return20D-Below200M')
def get_return_20d_below_200m_frontend():
    """Get 20D returns below 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return20D-Below200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 20D returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return60D-Above200M')
def get_return_60d_above_200m_frontend():
    """Get 60D returns above 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return60D-Above200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 60D returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return60D-Below200M')
def get_return_60d_below_200m_frontend():
    """Get 60D returns below 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return60D-Below200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 60D returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return120D-Above200M')
def get_return_120d_above_200m_frontend():
    """Get 120D returns above 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return120D-Above200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 120D returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return120D-Below200M')
def get_return_120d_below_200m_frontend():
    """Get 120D returns below 200M via backend API"""
    rewind_date = get_rewind_date()
    params = {'rewind_date': rewind_date.strftime('%Y-%m-%d')} if rewind_date else None
    result = make_backend_request('/api/Return120D-Below200M', params=params)
    if result is None:
        return jsonify({'error': 'Failed to fetch 120D returns'}), 500
    return jsonify(result)

# New market cap category endpoints
@app.route('/api/frontend/Return1D-MicroCap')
def get_return_1d_micro_frontend():
    """Get 1D returns for Micro Cap via backend API"""
    result = make_backend_request('/api/Return1D-MicroCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return1D-SmallCap')
def get_return_1d_small_frontend():
    """Get 1D returns for Small Cap via backend API"""
    result = make_backend_request('/api/Return1D-SmallCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return1D-MidCap')
def get_return_1d_mid_frontend():
    """Get 1D returns for Mid Cap via backend API"""
    result = make_backend_request('/api/Return1D-MidCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return1D-LargeCap')
def get_return_1d_large_frontend():
    """Get 1D returns for Large Cap via backend API"""
    result = make_backend_request('/api/Return1D-LargeCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return1D-MegaCap')
def get_return_1d_mega_frontend():
    """Get 1D returns for Mega Cap via backend API"""
    result = make_backend_request('/api/Return1D-MegaCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return5D-MicroCap')
def get_return_5d_micro_frontend():
    """Get 5D returns for Micro Cap via backend API"""
    result = make_backend_request('/api/Return5D-MicroCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return5D-SmallCap')
def get_return_5d_small_frontend():
    """Get 5D returns for Small Cap via backend API"""
    result = make_backend_request('/api/Return5D-SmallCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return5D-MidCap')
def get_return_5d_mid_frontend():
    """Get 5D returns for Mid Cap via backend API"""
    result = make_backend_request('/api/Return5D-MidCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return5D-LargeCap')
def get_return_5d_large_frontend():
    """Get 5D returns for Large Cap via backend API"""
    result = make_backend_request('/api/Return5D-LargeCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return5D-MegaCap')
def get_return_5d_mega_frontend():
    """Get 5D returns for Mega Cap via backend API"""
    result = make_backend_request('/api/Return5D-MegaCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return20D-MicroCap')
def get_return_20d_micro_frontend():
    """Get 20D returns for Micro Cap via backend API"""
    result = make_backend_request('/api/Return20D-MicroCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return20D-SmallCap')
def get_return_20d_small_frontend():
    """Get 20D returns for Small Cap via backend API"""
    result = make_backend_request('/api/Return20D-SmallCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return20D-MidCap')
def get_return_20d_mid_frontend():
    """Get 20D returns for Mid Cap via backend API"""
    result = make_backend_request('/api/Return20D-MidCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return20D-LargeCap')
def get_return_20d_large_frontend():
    """Get 20D returns for Large Cap via backend API"""
    result = make_backend_request('/api/Return20D-LargeCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return20D-MegaCap')
def get_return_20d_mega_frontend():
    """Get 20D returns for Mega Cap via backend API"""
    result = make_backend_request('/api/Return20D-MegaCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return60D-MicroCap')
def get_return_60d_micro_frontend():
    """Get 60D returns for Micro Cap via backend API"""
    result = make_backend_request('/api/Return60D-MicroCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return60D-SmallCap')
def get_return_60d_small_frontend():
    """Get 60D returns for Small Cap via backend API"""
    result = make_backend_request('/api/Return60D-SmallCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return60D-MidCap')
def get_return_60d_mid_frontend():
    """Get 60D returns for Mid Cap via backend API"""
    result = make_backend_request('/api/Return60D-MidCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return60D-LargeCap')
def get_return_60d_large_frontend():
    """Get 60D returns for Large Cap via backend API"""
    result = make_backend_request('/api/Return60D-LargeCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return60D-MegaCap')
def get_return_60d_mega_frontend():
    """Get 60D returns for Mega Cap via backend API"""
    result = make_backend_request('/api/Return60D-MegaCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return120D-MicroCap')
def get_return_120d_micro_frontend():
    """Get 120D returns for Micro Cap via backend API"""
    result = make_backend_request('/api/Return120D-MicroCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return120D-SmallCap')
def get_return_120d_small_frontend():
    """Get 120D returns for Small Cap via backend API"""
    result = make_backend_request('/api/Return120D-SmallCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return120D-MidCap')
def get_return_120d_mid_frontend():
    """Get 120D returns for Mid Cap via backend API"""
    result = make_backend_request('/api/Return120D-MidCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return120D-LargeCap')
def get_return_120d_large_frontend():
    """Get 120D returns for Large Cap via backend API"""
    result = make_backend_request('/api/Return120D-LargeCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Return120D-MegaCap')
def get_return_120d_mega_frontend():
    """Get 120D returns for Mega Cap via backend API"""
    result = make_backend_request('/api/Return120D-MegaCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch data'}), 500
    return jsonify(result)

@app.route('/api/frontend/Gapper-MicroCap')
def get_gapper_micro_frontend():
    """Get gapper stocks micro cap via backend API"""
    result = make_backend_request('/api/Gapper-MicroCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch gapper stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/Gapper-SmallCap')
def get_gapper_small_frontend():
    """Get gapper stocks small cap via backend API"""
    result = make_backend_request('/api/Gapper-SmallCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch gapper stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/Gapper-MidCap')
def get_gapper_mid_frontend():
    """Get gapper stocks mid cap via backend API"""
    result = make_backend_request('/api/Gapper-MidCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch gapper stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/Gapper-LargeCap')
def get_gapper_large_frontend():
    """Get gapper stocks large cap via backend API"""
    result = make_backend_request('/api/Gapper-LargeCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch gapper stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/Gapper-MegaCap')
def get_gapper_mega_frontend():
    """Get gapper stocks mega cap via backend API"""
    result = make_backend_request('/api/Gapper-MegaCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch gapper stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/Volume-MicroCap')
def get_volume_micro_frontend():
    """Get volume stocks micro cap via backend API"""
    result = make_backend_request('/api/Volume-MicroCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch volume stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/Volume-SmallCap')
def get_volume_small_frontend():
    """Get volume stocks small cap via backend API"""
    result = make_backend_request('/api/Volume-SmallCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch volume stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/Volume-MidCap')
def get_volume_mid_frontend():
    """Get volume stocks mid cap via backend API"""
    result = make_backend_request('/api/Volume-MidCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch volume stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/Volume-LargeCap')
def get_volume_large_frontend():
    """Get volume stocks large cap via backend API"""
    result = make_backend_request('/api/Volume-LargeCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch volume stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/Volume-MegaCap')
def get_volume_mega_frontend():
    """Get volume stocks mega cap via backend API"""
    result = make_backend_request('/api/Volume-MegaCap')
    if result is None:
        return jsonify({'error': 'Failed to fetch volume stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/SeaChange-Above200M')
def get_sea_change_above_200m_frontend():
    """Get sea change stocks above 200M via backend API"""
    result = make_backend_request('/api/SeaChange-Above200M')
    if result is None:
        return jsonify({'error': 'Failed to fetch sea change stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/SeaChange-Below200M')
def get_sea_change_below_200m_frontend():
    """Get sea change stocks below 200M via backend API"""
    result = make_backend_request('/api/SeaChange-Below200M')
    if result is None:
        return jsonify({'error': 'Failed to fetch sea change stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/AllReturns-Above200M')
def get_all_returns_above_200m_frontend():
    """Get all returns above 200M via backend API"""
    result = make_backend_request('/api/AllReturns-Above200M')
    if result is None:
        return jsonify({'error': 'Failed to fetch all returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/AllReturns-Below200M')
def get_all_returns_below_200m_frontend():
    """Get all returns below 200M via backend API"""
    result = make_backend_request('/api/AllReturns-Below200M')
    if result is None:
        return jsonify({'error': 'Failed to fetch all returns'}), 500
    return jsonify(result)

@app.route('/api/frontend/stocks')
def get_stocks_frontend():
    """Get all stocks via backend API"""
    result = make_backend_request('/api/stocks')
    if result is None:
        return jsonify({'error': 'Failed to fetch stocks'}), 500
    return jsonify(result)

@app.route('/api/frontend/sectors')
def get_sectors_frontend():
    """Get sectors via backend API"""
    result = make_backend_request('/api/sectors')
    if result is None:
        return jsonify({'error': 'Failed to fetch sectors'}), 500
    return jsonify(result)

@app.route('/api/frontend/industries')
def get_industries_frontend():
    """Get industries via backend API"""
    result = make_backend_request('/api/industries')
    if result is None:
        return jsonify({'error': 'Failed to fetch industries'}), 500
    return jsonify(result)

@app.route('/api/frontend/exchanges')
def get_exchanges_frontend():
    """Get exchanges via backend API"""
    result = make_backend_request('/api/exchanges')
    if result is None:
        return jsonify({'error': 'Failed to fetch exchanges'}), 500
    return jsonify(result)

@app.route('/api/frontend/stock/<ticker>')
def get_stock_details_frontend(ticker):
    """Get stock details via backend API"""
    result = make_backend_request(f'/api/stock/{ticker}')
    if result is None:
        return jsonify({'error': 'Failed to fetch stock details'}), 500
    return jsonify(result)

@app.route('/api/frontend/stock/<ticker>/prices')
def get_stock_prices_frontend(ticker):
    """Get stock prices via backend API"""
    result = make_backend_request(f'/api/stock/{ticker}/prices')
    if result is None:
        return jsonify({'error': 'Failed to fetch stock prices'}), 500
    return jsonify(result)

@app.route('/api/frontend/stock/<ticker>/earnings')
def get_stock_earnings_frontend(ticker):
    """Get stock earnings via backend API"""
    result = make_backend_request(f'/api/stock/{ticker}/earnings')
    if result is None:
        return jsonify({'error': 'Failed to fetch stock earnings'}), 500
    return jsonify(result)

@app.route('/api/frontend/market_cap_distribution')
def get_market_cap_distribution_frontend():
    """Get market cap distribution via backend API"""
    result = make_backend_request('/api/market_cap_distribution')
    if result is None:
        return jsonify({'error': 'Failed to fetch market cap distribution'}), 500
    return jsonify(result)

@app.route('/api/frontend/perplexity', methods=['POST'])
def get_perplexity_analysis_frontend():
    """Get Perplexity analysis via backend API"""
    data = request.get_json()
    result = make_backend_request('/api/perplexity', method='POST', data=data)
    if result is None:
        return jsonify({'error': 'Failed to get Perplexity analysis'}), 500
    return jsonify(result)

@app.route('/api/frontend/comments/<int:comment_id>', methods=['DELETE'])
def delete_comment_frontend(comment_id):
    """Delete comment via backend API"""
    result = make_backend_request(f'/api/comments/{comment_id}', method='DELETE')
    if result is None:
        return jsonify({'error': 'Failed to delete comment'}), 500
    return jsonify(result)

@app.route('/api/frontend/comments/<int:comment_id>', methods=['PUT'])
def edit_comment_frontend(comment_id):
    """Edit comment via backend API"""
    data = request.get_json()
    result = make_backend_request(f'/api/comments/{comment_id}', method='PUT', data=data)
    if result is None:
        return jsonify({'error': 'Failed to edit comment'}), 500
    return jsonify(result)

@app.route('/api/frontend/finviz-news/<ticker>')
def get_finviz_news_frontend(ticker):
    """Get Finviz news via backend API"""
    result = make_backend_request(f'/api/finviz-news/{ticker}')
    if result is None:
        return jsonify({'error': 'Failed to fetch Finviz news'}), 500
    return jsonify(result)

@app.route('/api/frontend/flags/<ticker>', methods=['GET'])
def get_flags_frontend(ticker):
    """Get flags via backend API"""
    result = make_backend_request(f'/api/flags/{ticker}')
    if result is None:
        return jsonify({'error': 'Failed to fetch flags'}), 500
    return jsonify(result)

@app.route('/api/frontend/flags', methods=['POST'])
def update_flags_frontend():
    """Update flags via backend API"""
    data = request.get_json()
    result = make_backend_request('/api/flags', method='POST', data=data)
    if result is None:
        return jsonify({'error': 'Failed to update flags'}), 500
    return jsonify(result)

@app.route('/api/frontend/flags/batch', methods=['POST'])
def update_flags_batch_frontend():
    """Update flags batch via backend API"""
    data = request.get_json()
    result = make_backend_request('/api/flags/batch', method='POST', data=data)
    if result is None:
        return jsonify({'error': 'Failed to update flags batch'}), 500
    return jsonify(result)

@app.route('/api/frontend/shortlist')
def api_shortlist_frontend():
    """Get shortlist via backend API"""
    result = make_backend_request('/api/shortlist')
    if result is None:
        return jsonify({'error': 'Failed to fetch shortlist'}), 500
    return jsonify(result)

@app.route('/api/frontend/blacklist')
def api_blacklist_frontend():
    """Get blacklist via backend API"""
    result = make_backend_request('/api/blacklist')
    if result is None:
        return jsonify({'error': 'Failed to fetch blacklist'}), 500
    return jsonify(result)

@app.route('/api/frontend/concise-notes/<ticker>')
def get_concise_note_frontend(ticker):
    """Get concise note via backend API"""
    result = make_backend_request(f'/api/concise-notes/{ticker}')
    if result is None:
        return jsonify({'error': 'Failed to fetch concise note'}), 500
    return jsonify(result)

@app.route('/api/frontend/concise-notes/<ticker>', methods=['PUT'])
def update_concise_note_frontend(ticker):
    """Update concise note via backend API"""
    data = request.get_json()
    result = make_backend_request(f'/api/concise-notes/{ticker}', method='PUT', data=data)
    if result is None:
        return jsonify({'error': 'Failed to update concise note'}), 500
    return jsonify(result)

@app.route('/api/frontend/rsi')
def api_rsi_frontend():
    """Get RSI data via backend API"""
    result = make_backend_request('/api/rsi')
    if result is None:
        return jsonify({'error': 'Failed to fetch RSI data'}), 500
    return jsonify(result)

@app.route('/Indices')
def indices_page():
    """Indices page showing SPX, QQQ, IWM with metrics and charts"""
    return render_template('indices.html', active_page='indices')


@app.route('/api/frontend/indices', methods=['GET'])
def get_indices_proxy():
    """Proxy endpoint for indices data"""
    result = make_backend_request('/api/indices')
    if result is None:
        return jsonify({'error': 'Failed to fetch indices data'}), 500
    return jsonify(result)


@app.route('/api/frontend/indices/<symbol>/prices', methods=['GET'])
def get_index_prices_proxy(symbol):
    """Proxy endpoint for index prices"""
    days = request.args.get('days', '365')
    result = make_backend_request(f'/api/indices/{symbol}/prices?days={days}')
    if result is None:
        return jsonify({'error': f'Failed to fetch prices for {symbol}'}), 500
    return jsonify(result)


@app.route('/api/frontend/economic-calendar', methods=['GET'])
def get_economic_calendar_proxy():
    """Proxy endpoint for economic calendar data"""
    # Pass through refresh parameter if present
    refresh = request.args.get('refresh', 'false')
    result = make_backend_request(f'/api/economic-calendar?refresh={refresh}')
    if result is None:
        return jsonify({'error': 'Failed to fetch economic calendar data'}), 500
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
