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

OUTPUT_FILE = 'stock_list.csv'
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
            print(f"Raw response: {response.text}")
            return []
    else:
        print(f"Failed to fetch symbols: {response.status_code}")
        print(f"Error response: {response.text}")
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

    # Filter for US exchanges, stocks only, and price > $1
    us_exchanges = {"NASDAQ", "NYSE", "AMEX"}
    filtered_symbols = []
    
    for stock in all_symbols:
        exchange = stock.get("exchangeShortName")
        stock_type = stock.get("type")
        price = stock.get("price")
        
        # Check if price is valid and greater than $1
        try:
            price_value = float(price) if price is not None else 0
        except (ValueError, TypeError):
            price_value = 0
        
        if exchange in us_exchanges and stock_type == "stock" and price_value > 1.0:
            filtered_symbols.append(stock.get("symbol"))
    
    print(f"Filtered {len(filtered_symbols)} stocks from {len(all_symbols)} total symbols")
    print(f"Exchanges: {us_exchanges}")
    print(f"Type: stock")
    print(f"Price: > $1.00")

    # Fetch detailed profiles for filtered stocks using batch requests
    print(f"\nFetching detailed profiles for {len(filtered_symbols)} stocks using batch requests...")
    
    BATCH_SIZE = batch_size  # Use the batch size from command line argument
    csv_writer = None
    csv_file = None
    profiles_written = 0
    
    try:
        # Process symbols in batches
        for i in range(0, len(filtered_symbols), BATCH_SIZE):
            batch = filtered_symbols[i:i+BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(filtered_symbols) + BATCH_SIZE - 1) // BATCH_SIZE
            
            print(f"\nProcessing batch {batch_num}/{total_batches}: {batch}")
            
            profiles = get_company_profiles_batch(batch)
            
            for profile in profiles:
                if profile:
                    # Initialize CSV writer with headers on first successful profile
                    if csv_writer is None:
                        csv_file = open(OUTPUT_FILE, 'w', newline='', encoding='utf-8')
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
            if i + BATCH_SIZE < len(filtered_symbols):  # Don't sleep after last batch
                print(f"Waiting 1.2 seconds before next batch...")
                time.sleep(1.2)
    
    finally:
        # Always close the file
        if csv_file:
            csv_file.close()
    
    print(f"✅ Done. {profiles_written} detailed profiles saved to {OUTPUT_FILE}")

if __name__ == '__main__':
    main()