import os
import logging
from datetime import datetime, timedelta
from polygon import RESTClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Stock
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError
from sic_mapper import get_industry_sector
import time
from sqlalchemy.sql import text
import requests
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Polygon.io client
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY not found in environment variables")

client = RESTClient(POLYGON_API_KEY)

# Initialize database
def init_db():
    """Initialize database connection and create tables"""
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment variables")
    
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def fetch_all_tickers():
    """
    Fetch all available tickers from Polygon.io reference API
    Returns a list of active stock tickers
    """
    logger.info("Fetching all available tickers from Polygon.io")
    
    try:
        # Use the reference API to get all stock tickers
        tickers_response = client.list_tickers(
            market='stocks',
            active=True,
            limit=1000  # Max limit per request
        )
        
        all_tickers = []
        
        # Process first page
        if hasattr(tickers_response, 'results') and tickers_response.results:
            all_tickers.extend([ticker.ticker for ticker in tickers_response.results])
            logger.info(f"Fetched {len(tickers_response.results)} tickers from first page")
        
        # Handle pagination if there are more results
        next_url = getattr(tickers_response, 'next_url', None)
        page_count = 1
        
        while next_url and page_count < 20:  # Limit to 20 pages (20,000 tickers max)
            try:
                # Extract the cursor from next_url for pagination
                import urllib.parse as urlparse
                parsed_url = urlparse.urlparse(next_url)
                query_params = urlparse.parse_qs(parsed_url.query)
                cursor = query_params.get('cursor', [None])[0]
                
                if cursor:
                    next_response = client.list_tickers(
                        market='stocks',
                        active=True,
                        limit=1000,
                        cursor=cursor
                    )
                    
                    if hasattr(next_response, 'results') and next_response.results:
                        all_tickers.extend([ticker.ticker for ticker in next_response.results])
                        logger.info(f"Fetched {len(next_response.results)} tickers from page {page_count + 1}")
                        next_url = getattr(next_response, 'next_url', None)
                        page_count += 1
                    else:
                        break
                else:
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching page {page_count + 1}: {str(e)}")
                break
        
        logger.info(f"Total tickers fetched: {len(all_tickers)}")
        return all_tickers
        
    except Exception as e:
        logger.error(f"Error fetching tickers: {str(e)}")
        # Fallback to a predefined list if API fails
        return [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'WMT',
            'PG', 'MA', 'UNH', 'HD', 'BAC', 'XOM', 'DIS', 'NFLX', 'ADBE', 'CRM',
            'PYPL', 'INTC', 'CMCSA', 'PEP', 'CSCO', 'ABT', 'KO', 'TMO', 'AVGO', 'QCOM'
        ]

def fetch_market_snapshot_bulk():
    """
    Fetch market snapshot data for all tickers using Polygon's bulk snapshot endpoint
    This is much more efficient than individual ticker requests
    """
    logger.info("Fetching market snapshot data using bulk endpoint")
    
    try:
        # Use the full market snapshot endpoint
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        params = {
            'apiKey': POLYGON_API_KEY,
            'include_otc': 'false'  # Exclude OTC securities
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('status') == 'OK' and 'tickers' in data:
            logger.info(f"Successfully fetched snapshot data for {len(data['tickers'])} tickers")
            return data['tickers']
        else:
            logger.error(f"API response error: {data.get('status', 'Unknown error')}")
            return []
            
    except Exception as e:
        logger.error(f"Error fetching market snapshot: {str(e)}")
        return []

def fetch_ticker_snapshots_batch(tickers, batch_size=100):
    """
    Fetch snapshot data for specific tickers in batches using the universal snapshot endpoint
    """
    logger.info(f"Fetching snapshot data for {len(tickers)} tickers in batches of {batch_size}")
    
    all_snapshots = []
    
    # Process tickers in batches
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        ticker_list = ','.join(batch)
        
        try:
            url = f"https://api.polygon.io/v3/snapshot"
            params = {
                'ticker.any_of': ticker_list,
                'apiKey': POLYGON_API_KEY
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'OK' and 'results' in data:
                all_snapshots.extend(data['results'])
                logger.info(f"Fetched snapshot data for batch {i//batch_size + 1}: {len(data['results'])} tickers")
            else:
                logger.warning(f"No data for batch {i//batch_size + 1}: {data.get('status', 'Unknown error')}")
            
            # Rate limiting - be respectful to the API
            time.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Error fetching batch {i//batch_size + 1}: {str(e)}")
            continue
    
    logger.info(f"Total snapshots fetched: {len(all_snapshots)}")
    return all_snapshots

def process_snapshot_data(session, snapshots):
    """
    Process snapshot data and store in database
    """
    logger.info(f"Processing {len(snapshots)} snapshot records")
    
    processed_count = 0
    error_count = 0
    
    for snapshot in snapshots:
        try:
            ticker = snapshot.get('ticker')
            if not ticker:
                continue
            
            # Extract price data from snapshot
            day_data = snapshot.get('day', {})
            prev_day_data = snapshot.get('prevDay', {})
            
            if not day_data and not prev_day_data:
                logger.warning(f"No price data available for {ticker}")
                continue
            
            # Use day data if available, otherwise use previous day data
            price_data = day_data if day_data else prev_day_data
            
            stock_data = {
                'ticker': ticker,
                'date': datetime.now().date(),
                'open_price': price_data.get('o'),
                'high_price': price_data.get('h'),
                'low_price': price_data.get('l'),
                'close_price': price_data.get('c'),
                'volume': price_data.get('v'),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # Check if stock already exists
            existing_stock = session.query(Stock).filter_by(ticker=ticker).first()
            
            if existing_stock:
                # Update existing stock with new price data
                for key, value in stock_data.items():
                    if key != 'ticker' and value is not None:
                        setattr(existing_stock, key, value)
            else:
                # Create new stock entry with price data
                new_stock = Stock(**stock_data)
                session.add(new_stock)
            
            processed_count += 1
            
            # Commit in batches to improve performance
            if processed_count % 100 == 0:
                session.commit()
                logger.info(f"Processed {processed_count} stocks so far...")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Error processing snapshot for {snapshot.get('ticker', 'unknown')}: {str(e)}")
            session.rollback()
    
    # Final commit
    try:
        session.commit()
        logger.info(f"Successfully processed {processed_count} stocks, {error_count} errors")
    except Exception as e:
        session.rollback()
        logger.error(f"Error in final commit: {str(e)}")

def update_stock_details_batch(session, batch_size=50):
    """
    Update stock details in batches for better efficiency
    """
    logger.info("Starting batch update of stock details")
    
    try:
        # Get all stocks that don't have complete information
        stocks_to_update = session.query(Stock).filter(
            (Stock.company_name == None) | 
            (Stock.sector == None) | 
            (Stock.industry == None) |
            (Stock.market_cap == None)
        ).limit(1000).all()  # Limit to prevent overwhelming the API
        
        logger.info(f"Found {len(stocks_to_update)} stocks that need detail updates")
        
        for i in range(0, len(stocks_to_update), batch_size):
            batch = stocks_to_update[i:i + batch_size]
            
            for stock in batch:
                try:
                    # Get ticker details from API
                    ticker_details = client.get_ticker_details(stock.ticker)
                    
                    if ticker_details:
                        sector, industry = get_industry_sector(getattr(ticker_details, 'sic_code', None))
                        
                        # Update stock with additional details
                        stock.company_name = getattr(ticker_details, 'name', stock.company_name)
                        stock.sector = sector or stock.sector
                        stock.industry = industry or stock.industry
                        stock.market_cap = getattr(ticker_details, 'market_cap', stock.market_cap)
                        stock.shares_outstanding = getattr(ticker_details, 'shares_outstanding', stock.shares_outstanding)
                        stock.eps = getattr(ticker_details, 'eps', stock.eps)
                        stock.pe_ratio = getattr(ticker_details, 'pe_ratio', stock.pe_ratio)
                        stock.updated_at = datetime.now()
                        
                        logger.info(f"Updated stock details for {stock.ticker}")
                    
                    else:
                        logger.warning(f"No ticker details found for {stock.ticker}")
                        
                except Exception as e:
                    logger.error(f"Error updating stock details for {stock.ticker}: {str(e)}")
                
                # Rate limiting
                time.sleep(0.1)
            
            # Commit batch
            try:
                session.commit()
                logger.info(f"Committed batch {i//batch_size + 1} of stock detail updates")
            except Exception as e:
                session.rollback()
                logger.error(f"Error committing batch {i//batch_size + 1}: {str(e)}")
            
            # Pause between batches
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"Error in update_stock_details_batch: {str(e)}")

def main():
    """Main function to process all tickers efficiently"""
    session = init_db()
    
    try:
        logger.info("Starting efficient bulk data fetching process")
        
        # Option 1: Use full market snapshot (most efficient for getting all market data)
        logger.info("Attempting to fetch full market snapshot...")
        snapshots = fetch_market_snapshot_bulk()
        
        if snapshots:
            process_snapshot_data(session, snapshots)
        else:
            # Option 2: Fallback to batch processing of specific tickers
            logger.info("Full market snapshot failed, falling back to batch processing...")
            tickers = fetch_all_tickers()
            
            if tickers:
                # Process in smaller batches to respect API limits
                snapshots = fetch_ticker_snapshots_batch(tickers, batch_size=50)
                if snapshots:
                    process_snapshot_data(session, snapshots)
        
        # Step 2: Update stock details for stocks missing information
        update_stock_details_batch(session, batch_size=25)
        
        logger.info("Bulk data fetching and stock updates completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    main() 