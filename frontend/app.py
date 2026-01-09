"""Frontend Flask app - renders templates and proxies API calls to backend."""

from flask import Flask, render_template, jsonify, request
import os
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


@app.route('/rsi')
def rsi_page():
    """RSI by Market Cap page"""
    return render_template('rsi.html')


@app.route('/sector-rsi')
def sector_rsi_page():
    """Sector RSI page"""
    return render_template('sector_rsi.html')


@app.route('/rsi-mktcap')
def rsi_mktcap_page():
    """RSI Market Cap page - RSI percentile within market cap bucket"""
    return render_template('rsi_mktcap.html')


@app.route('/rsi-momentum')
def rsi_momentum_page():
    """RSI Momentum page - RSI MktCap change over 5 trading days"""
    return render_template('rsi_momentum.html')


@app.route('/rsi-spx')
def rsi_spx_page():
    """RSI S&P 500 page - RSI percentile within S&P 500 universe"""
    return render_template('rsi_spx.html')


@app.route('/rsi-ndx')
def rsi_ndx_page():
    """RSI NASDAQ 100 page - RSI percentile within NASDAQ 100 universe"""
    return render_template('rsi_ndx.html')


@app.route('/rsi-dji')
def rsi_dji_page():
    """RSI Dow Jones page - RSI percentile within Dow Jones universe"""
    return render_template('rsi_dji.html')


@app.route('/top-performance')
def top_performance_page():
    """Top Performance page - Union of top stocks by 1D, 5D, 20D returns"""
    return render_template('top_performance.html')


@app.route('/volspike-gapper')
def volspike_gapper_page():
    """Volume Spike & Gapper page - Stocks with unusual volume and gap activity"""
    return render_template('volspike_gapper.html')


@app.route('/main-view')
def main_view_page():
    """Main View page - Combined screener view with metrics, volspike/gapper, and tags"""
    return render_template('main_view.html')


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


@app.route('/api/frontend/rsi/<market_cap>')
def api_rsi_by_market_cap(market_cap):
    """Proxy endpoint for RSI by market cap from backend"""
    # Map the frontend parameter to backend endpoint
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
    
    data, status_code = make_backend_request(f'/api/RSI-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch RSI data'}), status_code
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


@app.route('/api/frontend/sector-rsi')
def api_sector_rsi():
    """Proxy endpoint for sector RSI from backend"""
    data, status_code = make_backend_request('/api/sector-rsi')
    if data is None:
        return jsonify({'error': 'Failed to fetch sector RSI data'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/rsi-mktcap/<market_cap>')
def api_rsi_mktcap(market_cap):
    """Proxy endpoint for RSI Market Cap from backend"""
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
    
    data, status_code = make_backend_request(f'/api/RSIMktCap-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch RSI MktCap data'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/rsi-momentum/<market_cap>')
def api_rsi_momentum(market_cap):
    """Proxy endpoint for RSI Momentum (5-day RSI MktCap change) from backend"""
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
    
    data, status_code = make_backend_request(f'/api/RSIMomentum-{endpoint_cap}')
    if data is None:
        return jsonify({'error': 'Failed to fetch RSI Momentum data'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/rsi-index/<index_type>')
@app.route('/api/frontend/rsi-index/<index_type>/<market_cap>')
def api_rsi_index(index_type, market_cap=None):
    """Proxy endpoint for RSI Index (SPX, NDX, DJI) from backend with optional market cap filter"""
    index_map = {
        'spx': 'SPX',
        'ndx': 'NDX',
        'dji': 'DJI'
    }
    endpoint_index = index_map.get(index_type.lower())
    if not endpoint_index:
        return jsonify({'error': 'Invalid index type'}), 400
    
    # Build endpoint with optional market cap filter
    if market_cap:
        cap_map = {
            'micro': 'micro',
            'small': 'small',
            'mid': 'mid',
            'large': 'large',
            'mega': 'mega'
        }
        endpoint_cap = cap_map.get(market_cap.lower())
        if not endpoint_cap:
            return jsonify({'error': 'Invalid market cap category'}), 400
        endpoint = f'/api/RSI-{endpoint_index}/{endpoint_cap}'
    else:
        endpoint = f'/api/RSI-{endpoint_index}'
    
    data, status_code = make_backend_request(endpoint)
    if data is None:
        return jsonify({'error': 'Failed to fetch RSI Index data'}), status_code
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


# ============================================================
# Stock Notes Endpoints
# ============================================================

@app.route('/api/frontend/stock-notes/<ticker>', methods=['GET'])
def api_get_stock_notes(ticker):
    """Proxy endpoint to get notes for a specific stock"""
    data, status_code = make_backend_request(f'/api/stock-notes/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch stock notes'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/stock-notes/<ticker>', methods=['PUT'])
def api_update_stock_notes(ticker):
    """Proxy endpoint to create or update notes for a specific stock"""
    json_data = request.get_json()
    data, status_code = make_backend_request(f'/api/stock-notes/{ticker}', method='PUT', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to update stock notes'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/stock-notes/batch', methods=['POST'])
def api_get_stock_notes_batch():
    """Proxy endpoint to get notes for multiple tickers at once"""
    json_data = request.get_json()
    data, status_code = make_backend_request('/api/stock-notes/batch', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to fetch stock notes batch'}), status_code
    return jsonify(data), status_code


# ============================================================
# Stock Preferences Endpoints
# ============================================================

@app.route('/api/frontend/stock-preferences/<ticker>', methods=['GET'])
def api_get_stock_preference(ticker):
    """Proxy endpoint to get preference for a specific stock"""
    data, status_code = make_backend_request(f'/api/stock-preferences/{ticker}')
    if data is None:
        return jsonify({'error': 'Failed to fetch stock preference'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/stock-preferences/<ticker>', methods=['PUT'])
def api_update_stock_preference(ticker):
    """Proxy endpoint to create or update preference for a specific stock"""
    json_data = request.get_json()
    data, status_code = make_backend_request(f'/api/stock-preferences/{ticker}', method='PUT', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to update stock preference'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/stock-preferences/batch', methods=['POST'])
def api_get_stock_preferences_batch():
    """Proxy endpoint to get preferences for multiple tickers at once"""
    json_data = request.get_json()
    data, status_code = make_backend_request('/api/stock-preferences/batch', method='POST', json_data=json_data)
    if data is None:
        return jsonify({'error': 'Failed to fetch stock preferences batch'}), status_code
    return jsonify(data), status_code


@app.route('/api/frontend/stock-preferences/list/<preference_type>')
def api_list_stock_preferences(preference_type):
    """Proxy endpoint to list all stocks with a specific preference"""
    data, status_code = make_backend_request(f'/api/stock-preferences/list/{preference_type}')
    if data is None:
        return jsonify({'error': 'Failed to fetch stock preferences list'}), status_code
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


@app.route('/api/frontend/latest-date')
def api_latest_date():
    """Proxy endpoint for latest OHLC date from backend"""
    data, status_code = make_backend_request('/api/latest_date')
    if data is None:
        return jsonify({'error': 'Failed to fetch latest date'}), status_code
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
    import glob
    from pathlib import Path
    
    # Look for logs in the project root's logs directory
    # In Docker, the project is mounted at /app
    logs_dir = Path('/app/logs')
    if not logs_dir.exists():
        logs_dir = Path(__file__).parent.parent / 'logs'
    
    log_files = []
    if logs_dir.exists():
        for log_file in sorted(logs_dir.glob('*.log'), key=lambda x: x.stat().st_mtime, reverse=True):
            stat = log_file.stat()
            log_files.append({
                'name': log_file.name,
                'size': stat.st_size,
                'modified': stat.st_mtime,
                'modified_str': __import__('datetime').datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
