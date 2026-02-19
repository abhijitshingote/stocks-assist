"""
Extract OHLC data from Financial Modeling Prep API.

Usage:
    python extract_data_from_api.py
    
Requires FMP_API_KEY environment variable.
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ============== Configuration ==============
SYMBOLS = ['SPY', 'QQQ', 'IWM']
START_DATE = '2024-01-01'
END_DATE = '2026-01-28'
OUTPUT_FILE = 'price_data.csv'

# ============== Constants ==============
BASE_URL = 'https://financialmodelingprep.com/stable'


def get_api_key() -> str:
    """Get FMP API key from environment."""
    load_dotenv()
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        raise ValueError("FMP_API_KEY not found in environment variables.")
    return api_key


def fetch_historical_prices(api_key: str, symbol: str, 
                            start_date: str, end_date: str) -> list:
    """Fetch historical OHLC data for a symbol from FMP API."""
    url = f'{BASE_URL}/historical-price-eod/full'
    params = {
        'symbol': symbol,
        'apikey': api_key,
        'from': start_date,
        'to': end_date
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.ok:
            data = response.json()
            return data if isinstance(data, list) else []
        else:
            print(f"Error fetching {symbol}: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return []


def process_price_data(symbol: str, raw_data: list) -> pd.DataFrame:
    """Process raw API response into DataFrame."""
    if not raw_data:
        return pd.DataFrame()
    
    records = []
    for item in raw_data:
        records.append({
            'symbol': symbol,
            'date': item.get('date'),
            'open_price': item.get('open'),
            'high_price': item.get('high'),
            'low_price': item.get('low'),
            'close_price': item.get('close'),
            'volume': item.get('volume')
        })
    
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    return df


def extract_data(symbols: list, start_date: str, end_date: str, 
                 output_file: str) -> pd.DataFrame:
    """Extract data for all symbols and save to CSV."""
    api_key = get_api_key()
    
    all_data = []
    
    for symbol in symbols:
        print(f"Fetching {symbol}...")
        raw_data = fetch_historical_prices(api_key, symbol, start_date, end_date)
        df = process_price_data(symbol, raw_data)
        
        if not df.empty:
            all_data.append(df)
            print(f"  Retrieved {len(df)} records")
        else:
            print(f"  No data retrieved")
    
    if not all_data:
        print("No data retrieved for any symbol")
        return pd.DataFrame()
    
    # Combine and sort
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values(['symbol', 'date'])
    
    # Save to CSV
    output_path = os.path.join(os.path.dirname(__file__), output_file)
    combined_df.to_csv(output_path, index=False)
    print(f"\nSaved {len(combined_df)} total records to {output_path}")
    
    return combined_df


def main():
    print("=" * 50)
    print("Extracting OHLC data from FMP API")
    print("=" * 50)
    print(f"Symbols: {SYMBOLS}")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print(f"Output file: {OUTPUT_FILE}")
    print("-" * 50)
    
    extract_data(SYMBOLS, START_DATE, END_DATE, OUTPUT_FILE)
    
    print("=" * 50)
    print("Done!")


if __name__ == '__main__':
    main()
