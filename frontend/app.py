"""Frontend Flask app - renders templates and proxies API calls to backend."""

from flask import Flask, render_template, jsonify
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


def make_backend_request(endpoint):
    """Helper function to make requests to the backend API."""
    try:
        url = f"{BACKEND_URL}{endpoint}"
        response = requests.get(url)
        
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
