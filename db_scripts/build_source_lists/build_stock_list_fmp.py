import requests
import csv
import os
import time
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv('FMP_API_KEY')
if not API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables. Please add it to your .env file.")

# Ensure data directory exists
DATA_DIR = 'db_scripts/data'
os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_FILE = os.path.join(DATA_DIR, 'stock_list.csv')
TEMP_FILE = os.path.join(DATA_DIR, 'stock_list_temp.csv')
BASE_URL = 'https://financialmodelingprep.com/api/v3'

def get_all_symbols():
    url = f'{BASE_URL}/stock/list?apikey={API_KEY}'
    print(f"Fetching symbols from: {url}")
    response = requests.get(url)
    print(f"Symbols API status: {response.status_code}")
    
    if response.ok:
        try:
            data = response.json()
            print(f"Successfully loaded {len(data)} symbols")
            return data
        except Exception as e:
            print(f"Error parsing symbols JSON: {e}")
            return []
    else:
        print(f"Failed to fetch symbols: {response.status_code}")
        return []

def get_company_profiles_batch(symbols):
    """
    Fetch detailed company profiles for multiple symbols at once
    """
    symbol_str = ','.join(symbols)
    url = f'{BASE_URL}/profile/{symbol_str}?apikey={API_KEY}'
    response = requests.get(url)
    
    if response.ok:
        try:
            data = response.json()
            if data:
                print(f"✅ Batch success: {len(data)} profiles returned for {len(symbols)} symbols")
                return data
            else:
                print(f"⚠️ Empty response for batch: {symbol_str}")
                return []
        except Exception as e:
            print(f"❌ Error parsing batch JSON for [{symbol_str}]: {e}")
            return []
    else:
        print(f"❌ Failed to fetch batch profiles for [{symbol_str}]: Status {response.status_code}")
        return []

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fetch stock profiles from Financial Modeling Prep API')
    parser.add_argument('--batch-size', type=int, default=1000, 
                        help='Number of symbols to process per batch (default: 1000)')
    args = parser.parse_args()
    
    batch_size = args.batch_size
    print(f"Using batch size: {batch_size}")
    print("=" * 60)
    
    all_symbols = get_all_symbols()
    if not all_symbols:
        print("Failed to load symbols.")
        return

    # Filter for US exchanges and stocks only
    us_exchanges = {"NASDAQ", "NYSE", "AMEX"}
    filtered_symbols = []
    
    for stock in all_symbols:
        exchange = stock.get("exchangeShortName")
        stock_type = stock.get("type")
        
        if exchange in us_exchanges and stock_type == "stock":
            filtered_symbols.append(stock.get("symbol"))
    
    print(f"Filtered {len(filtered_symbols)} stocks from {len(all_symbols)} total symbols")
    print(f"Exchanges: {us_exchanges}")
    print(f"Type: stock")

    # Fetch detailed profiles for filtered stocks using batch requests
    print(f"\nFetching detailed profiles for {len(filtered_symbols)} stocks using batch requests...")
    
    csv_writer = None
    csv_file = None
    profiles_written = 0
    
    try:
        # Process symbols in batches
        for i in range(0, len(filtered_symbols), batch_size):
            batch = filtered_symbols[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(filtered_symbols) + batch_size - 1) // batch_size
            
            print(f"\nProcessing batch {batch_num}/{total_batches}: {batch}")
            
            profiles = get_company_profiles_batch(batch)
            
            for profile in profiles:
                if profile and profile.get('isActivelyTrading') == True:
                    # Initialize CSV writer with headers on first successful profile
                    if csv_writer is None:
                        csv_file = open(TEMP_FILE, 'w', newline='', encoding='utf-8')
                        fieldnames = profile.keys()
                        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        csv_writer.writeheader()
                        print(f"Created CSV file with headers: {list(fieldnames)}")
                    
                    # Write the profile immediately
                    csv_writer.writerow(profile)
                    profiles_written += 1
                    symbol = profile.get('symbol', 'Unknown')
                    print(f"✓ {symbol} written to CSV (Total: {profiles_written})")
            
            # Add delay between batches to respect API limits
            if i + batch_size < len(filtered_symbols):  # Don't sleep after last batch
                print(f"Waiting 1.2 seconds before next batch...")
                time.sleep(1.2)
    
    finally:
        # Always close the file
        if csv_file:
            csv_file.close()
    
    # Replace main file only if temp file has data
    if profiles_written > 0:
        os.rename(TEMP_FILE, OUTPUT_FILE)
        print(f"✅ Updated: {profiles_written} profiles saved to {OUTPUT_FILE}")
    else:
        if os.path.exists(TEMP_FILE):
            os.remove(TEMP_FILE)
        print(f"❌ Not updated: No valid data found")

if __name__ == '__main__':
    main()