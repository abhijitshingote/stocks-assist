from polygon import RESTClient
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, tuple_
from sqlalchemy.orm import sessionmaker

# Add backend directory to Python path to import models
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import Stock, Price
import logging
import time
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

def get_bulk_aggregates(client, symbols, start_date, end_date):
    """Get daily aggregates for multiple symbols"""
    all_aggregates = {}

    for symbol in symbols:
        try:
            # Get daily aggregates for the symbol
            aggs = client.get_aggs(
                symbol,
                multiplier=1,
                timespan="day",
                from_=start_date,
                to=end_date
            )

            if aggs:
                all_aggregates[symbol] = aggs

            time.sleep(0.1)  # Rate limiting

        except Exception as e:
            logger.error(f"Error fetching aggregates for {symbol}: {str(e)}")
            continue

    return all_aggregates

def process_price_data(session, aggregates):
    """Process price data and bulk insert"""
    price_updates = []

    for symbol, aggs in aggregates.items():
        try:
            for agg in aggs:
                price_date = datetime.fromtimestamp(agg.timestamp / 1000).date()

                price_data = {
                    'ticker': symbol,  # Using ticker field as requested
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
            logger.error(f"Error preparing aggregates for {symbol}: {str(e)}")
            continue

    # Process price data in chunks
    if price_updates:
        try:
            logger.info(f"Processing {len(price_updates)} price records in chunks...")

            chunk_size = 5000
            for i in range(0, len(price_updates), chunk_size):
                chunk = price_updates[i:i + chunk_size]
                logger.info(f"Processing price chunk {i//chunk_size + 1} of {(len(price_updates) + chunk_size - 1)//chunk_size} ({len(chunk)} records)")

                # Check existing price records for this chunk
                price_keys = [(price['ticker'], price['date']) for price in chunk]
                existing_records = session.query(Price.ticker, Price.date).filter(
                    tuple_(Price.ticker, Price.date).in_(price_keys)
                ).all() if price_keys else []
                existing_prices = {(row[0], row[1]) for row in existing_records}

                # Separate new records from updates
                new_prices = [price for price in chunk
                             if (price['ticker'], price['date']) not in existing_prices]
                update_prices = [price for price in chunk
                                if (price['ticker'], price['date']) in existing_prices]

                # Bulk insert new price records
                if new_prices:
                    session.bulk_insert_mappings(Price, new_prices)
                    logger.info(f"  - Inserted {len(new_prices)} new price records")

                # Update existing price records
                if update_prices:
                    for price_data in update_prices:
                        session.merge(Price(**price_data))
                    logger.info(f"  - Updated {len(update_prices)} existing price records")

                session.commit()

        except Exception as e:
            logger.error(f"Error in price update: {str(e)}")
            session.rollback()

def main():
    """Main function to seed price data from Polygon"""
    import argparse

    parser = argparse.ArgumentParser(description='Seed price data from Polygon API')
    parser.add_argument('--limit', type=int, help='Limit number of stocks to process for price data')
    args = parser.parse_args()

    print("Starting price data seeding from Polygon...")

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get all tickers from stock table
        all_stocks = session.query(Stock.ticker).all()
        symbols = [stock[0] for stock in all_stocks]

        # Apply limit if specified
        if args.limit:
            symbols = symbols[:args.limit]
            logger.info(f"Limited to {len(symbols)} stocks for price data (--limit {args.limit})")
        else:
            logger.info(f"Found {len(symbols)} stocks for price data")

        if not symbols:
            logger.warning("No stocks found in database")
            return

        # Fetch price data from Polygon
        client = get_polygon_client()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)  # 1 year of data

        logger.info(f"Fetching price data from {start_date.date()} to {end_date.date()}")

        # Process symbols in batches
        batch_size = 150
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            logger.info(f"Processing price batch {i//batch_size + 1} of {(len(symbols) + batch_size - 1)//batch_size} ({len(batch)} symbols)")

            # Get price data for this batch
            aggregates = get_bulk_aggregates(client, batch, start_date, end_date)
            if aggregates:
                process_price_data(session, aggregates)

            # Clear memory
            del aggregates

        # Final statistics
        total_prices = session.query(Price).count()
        logger.info(f"✅ Price seeding completed. Total price records: {total_prices}")

    except Exception as e:
        logger.error(f"Error in price seeding process: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == '__main__':
    main()
