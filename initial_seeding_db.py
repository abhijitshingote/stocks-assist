from polygon import RESTClient
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, tuple_
from sqlalchemy.orm import sessionmaker
from models import Stock, Price
import logging
import time
from sic_mapper import get_industry_sector
import urllib.parse
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def get_eastern_datetime():
    """Get current datetime in Eastern Time (handles EST/EDT automatically)"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)

def get_polygon_client():
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise ValueError("POLYGON_API_KEY environment variable is not set")
    return RESTClient(api_key)

def get_bulk_tickers(client, batch_size=1000):
    """Get tickers in bulk using the reference/tickers endpoint"""
    all_tickers = []
    next_page_token = None
    
    while True:
        try:
            # Get tickers in bulk
            tickers = client.list_tickers(
                market="stocks",
                active=True,
                limit=batch_size
            )
            
            if not tickers:
                break
                
            all_tickers.extend(tickers)
            
            # Check if there are more pages
            if not hasattr(tickers, 'next_page_token'):
                break
                
            # Get next page using the next_url
            next_url = tickers.next_url
            if not next_url:
                break
                
            # Extract the page token from the next_url
            parsed_url = urllib.parse.urlparse(next_url)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            next_page_token = query_params.get('page_token', [None])[0]
            
            if not next_page_token:
                break
                
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error fetching tickers: {str(e)}")
            break
    
    return all_tickers

def get_bulk_snapshots(client, tickers):
    """Get company details for multiple tickers"""
    company_details = {}
    try:
        # Get company details for each ticker with faster rate limiting
        for ticker in tickers:
            try:
                # Get company details
                details = client.get_ticker_details(ticker.ticker)
                if details:
                    company_details[ticker.ticker] = details
                time.sleep(0.1)  # Reduced rate limiting from 0.2 to 0.1
            except Exception as e:
                logger.error(f"Error fetching details for {ticker.ticker}: {str(e)}")
                continue
        return company_details
    except Exception as e:
        logger.error(f"Error in bulk company details: {str(e)}")
        return None

def get_bulk_aggregates(client, tickers, start_date, end_date):
    """Get daily aggregates for multiple tickers"""
    all_aggregates = {}
    
    # Process tickers individually with faster rate limiting
    for ticker in tickers:
        try:
            # Get daily aggregates for the ticker
            aggs = client.get_aggs(
                ticker.ticker,
                multiplier=1,
                timespan="day",
                from_=start_date,
                to=end_date
            )
            
            if aggs:
                all_aggregates[ticker.ticker] = aggs
            
            time.sleep(0.1)  # Reduced rate limiting from 0.2 to 0.1
            
        except Exception as e:
            logger.error(f"Error fetching aggregates for {ticker.ticker}: {str(e)}")
            continue
    
    return all_aggregates

def process_bulk_data(session, tickers, company_details, aggregates):
    """Process bulk data and create/update records"""
    # Process company details in bulk
    stock_updates = []
    for ticker, details in company_details.items():
        try:
            # Get industry and sector
            sic_code = getattr(details, 'sic_code', None)
            sector, industry = get_industry_sector(sic_code)
            
            # Prepare stock data for bulk upsert
            stock_data = {
                'ticker': ticker,
                'company_name': getattr(details, 'name', None),
                'sector': sector,
                'industry': industry,
                'subsector': getattr(details, 'subsector', None),
                'market_cap': getattr(details, 'market_cap', None),
                'shares_outstanding': getattr(details, 'share_class_shares_outstanding', None),
                'eps': getattr(details, 'weighted_shares_outstanding', None),
                'pe_ratio': getattr(details, 'pe_ratio', None),
                'updated_at': get_eastern_datetime()
            }
            stock_updates.append(stock_data)
            
        except Exception as e:
            logger.error(f"Error preparing company details for {ticker}: {str(e)}")
            continue
    
    # Bulk upsert stock data
    if stock_updates:
        try:
            # Check if we need to use merge (for existing records) or bulk insert (for new records)
            existing_tickers = set()
            if stock_updates:
                # Get existing ticker symbols to determine which need merging vs inserting
                ticker_symbols = [stock['ticker'] for stock in stock_updates]
                existing_records = session.query(Stock.ticker).filter(Stock.ticker.in_(ticker_symbols)).all()
                existing_tickers = {row[0] for row in existing_records}
            
            # Separate new records from updates
            new_records = [stock for stock in stock_updates if stock['ticker'] not in existing_tickers]
            update_records = [stock for stock in stock_updates if stock['ticker'] in existing_tickers]
            
            # Bulk insert new records
            if new_records:
                session.bulk_insert_mappings(Stock, new_records)
                logger.info(f"Bulk inserted {len(new_records)} new company records")
            
            # Merge existing records
            if update_records:
                for stock_data in update_records:
                    session.merge(Stock(**stock_data))
                logger.info(f"Updated {len(update_records)} existing company records")
            
            session.commit()
        except Exception as e:
            logger.error(f"Error in bulk stock update: {str(e)}")
            session.rollback()
    
    # Process aggregates for price data in bulk
    price_updates = []
    for ticker, aggs in aggregates.items():
        try:
            # Prepare price data for bulk upsert
            for agg in aggs:
                price_date = datetime.fromtimestamp(agg.timestamp / 1000).date()
                
                price_data = {
                    'ticker': ticker,
                    'date': price_date,
                    'open_price': agg.open,
                    'high_price': agg.high,
                    'low_price': agg.low,
                    'close_price': agg.close,
                    'volume': agg.volume,
                    'updated_at': get_eastern_datetime()
                }
                price_updates.append(price_data)
            
        except Exception as e:
            logger.error(f"Error preparing aggregates for {ticker}: {str(e)}")
            continue
    
    # Process price data in smaller chunks to avoid PostgreSQL stack depth limits
    if price_updates:
        try:
            logger.info(f"Processing {len(price_updates)} price records in chunks...")
            
            # Process price data in chunks to avoid database stack overflow
            chunk_size = 5000  # Process 5000 price records at a time
            for i in range(0, len(price_updates), chunk_size):
                chunk = price_updates[i:i + chunk_size]
                logger.info(f"Processing price chunk {i//chunk_size + 1} of {(len(price_updates) + chunk_size - 1)//chunk_size} ({len(chunk)} records)")
                
                # Check existing price records for this chunk
                price_keys = [(price['ticker'], price['date']) for price in chunk]
                existing_records = session.query(Price.ticker, Price.date).filter(
                    tuple_(Price.ticker, Price.date).in_(price_keys)
                ).all() if price_keys else []
                existing_prices = {(row[0], row[1]) for row in existing_records}
                
                # Separate new records from updates for this chunk
                new_prices = [price for price in chunk 
                             if (price['ticker'], price['date']) not in existing_prices]
                update_prices = [price for price in chunk 
                                if (price['ticker'], price['date']) in existing_prices]
                
                # Bulk insert new price records
                if new_prices:
                    session.bulk_insert_mappings(Price, new_prices)
                    logger.info(f"  - Inserted {len(new_prices)} new price records")
                
                # Merge existing price records in smaller sub-chunks
                if update_prices:
                    for j, price_data in enumerate(update_prices):
                        session.merge(Price(**price_data))
                        # Commit every 1000 updates to avoid memory issues
                        if (j + 1) % 1000 == 0:
                            session.commit()
                    logger.info(f"  - Updated {len(update_prices)} existing price records")
                
                # Commit this chunk
                session.commit()
                
        except Exception as e:
            logger.error(f"Error in price update: {str(e)}")
            session.rollback()

def seed_database():
    # Record start time
    start_time = time.time()
    print("Starting database seeding process...")
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        client = get_polygon_client()
        
        # Get all tickers
        logger.info("Fetching tickers...")
        tickers = get_bulk_tickers(client)
        if not tickers:
            logger.error("No tickers found")
            return
        
        # Process tickers in batches optimized for API limits and database performance
        batch_size = 150  # Balanced batch size - not too large to avoid DB issues
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} of {(len(tickers) + batch_size - 1)//batch_size} ({len(batch)} tickers)")
            
            # Get date range for historical data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            
            # Get company details
            company_details = get_bulk_snapshots(client, batch)
            if not company_details:
                logger.warning(f"No company details found for batch {i//batch_size + 1}")
                continue
            
            # Get bulk aggregates
            aggregates = get_bulk_aggregates(client, batch, start_date, end_date)
            if not aggregates:
                logger.warning(f"No aggregates found for batch {i//batch_size + 1}")
                continue
            
            # Process the bulk data
            process_bulk_data(session, batch, company_details, aggregates)
            
            # Clear variables to free memory
            del company_details
            del aggregates
            
            # Reduced sleep time between batches
            time.sleep(0.5)  # Reduced from 1 second to 0.5 seconds
        
        logger.info("Database seeding completed successfully")
        
        # Calculate and print total time taken
        end_time = time.time()
        total_minutes = (end_time - start_time) / 60
        print(f"Time taken to complete - {total_minutes:.2f} minutes")
        
    except Exception as e:
        logger.error(f"Error during database seeding: {str(e)}")
        session.rollback()
        
        # Calculate and print time taken even if there was an error
        end_time = time.time()
        total_minutes = (end_time - start_time) / 60
        print(f"Process failed after {total_minutes:.2f} minutes")
    finally:
        session.close()

if __name__ == "__main__":
    seed_database() 