from polygon import RESTClient
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Stock, Price
import logging
import time
from sic_mapper import get_industry_sector
import urllib.parse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

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
        # Get company details for each ticker
        for ticker in tickers:
            try:
                # Get company details
                details = client.get_ticker_details(ticker.ticker)
                if details:
                    company_details[ticker.ticker] = details
                time.sleep(0.2)  # Rate limiting
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
    
    # Process tickers in smaller batches to avoid API limits
    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            # Get aggregates for each ticker in the batch
            for ticker in batch:
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
                    
                    time.sleep(0.2)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error fetching aggregates for {ticker.ticker}: {str(e)}")
                    continue
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            continue
    
    return all_aggregates

def process_bulk_data(session, tickers, company_details, aggregates):
    """Process bulk data and create/update records"""
    # Process company details
    for ticker, details in company_details.items():
        try:
            # Get industry and sector
            sic_code = getattr(details, 'sic_code', None)
            sector, industry = get_industry_sector(sic_code)
            
            # Create or update ticker record (only company info, no price data)
            stock = session.query(Stock).filter_by(ticker=ticker).first()
            if not stock:
                stock = Stock(ticker=ticker)
            
            stock.company_name = getattr(details, 'name', None)
            stock.sector = sector
            stock.industry = industry
            stock.subsector = getattr(details, 'subsector', None)
            stock.market_cap = getattr(details, 'market_cap', None)
            stock.shares_outstanding = getattr(details, 'share_class_shares_outstanding', None)
            stock.eps = getattr(details, 'weighted_shares_outstanding', None)
            stock.pe_ratio = getattr(details, 'pe_ratio', None)
            stock.updated_at = datetime.now()
            
            session.merge(stock)
            session.commit()
            
        except Exception as e:
            logger.error(f"Error processing company details for {ticker}: {str(e)}")
            session.rollback()
            continue
    
    # Process aggregates for price data (separate from ticker/company data)
    for ticker, aggs in aggregates.items():
        try:
            # Insert daily price data
            for agg in aggs:
                price_date = datetime.fromtimestamp(agg.timestamp / 1000).date()
                
                # Check if price record exists
                existing_price = session.query(Price).filter_by(
                    ticker=ticker,
                    date=price_date
                ).first()
                
                if existing_price:
                    # Update existing price record
                    existing_price.open_price = agg.open
                    existing_price.high_price = agg.high
                    existing_price.low_price = agg.low
                    existing_price.close_price = agg.close
                    existing_price.volume = agg.volume
                    existing_price.updated_at = datetime.now()
                else:
                    # Create new price record
                    price = Price(
                        ticker=ticker,
                        date=price_date,
                        open_price=agg.open,
                        high_price=agg.high,
                        low_price=agg.low,
                        close_price=agg.close,
                        volume=agg.volume
                    )
                    session.add(price)
            
            # Commit price records for this ticker
            session.commit()
            
        except Exception as e:
            logger.error(f"Error processing aggregates for {ticker}: {str(e)}")
            session.rollback()
            continue

def seed_database():
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
        
        # Process tickers in batches
        batch_size = 100
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} of {(len(tickers) + batch_size - 1)//batch_size}")
            
            # Get date range for historical data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            
            # Get company details
            company_details = get_bulk_snapshots(client, batch)
            if not company_details:
                continue
            
            # Get bulk aggregates
            aggregates = get_bulk_aggregates(client, batch, start_date, end_date)
            if not aggregates:
                continue
            
            # Process the bulk data
            process_bulk_data(session, batch, company_details, aggregates)
            
            # Rate limiting between batches
            time.sleep(1)
        
        logger.info("Database seeding completed successfully")
        
    except Exception as e:
        logger.error(f"Error during database seeding: {str(e)}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    seed_database() 