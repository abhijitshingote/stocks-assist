from datetime import datetime, timedelta, date
import os
import sys
import csv
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

# Add backend directory to Python path to import models
sys.path.append('/app/backend')
from models import Stock
import logging
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def get_eastern_datetime():
    """Get current datetime in Eastern Time (handles EST/EDT automatically)"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)

def upsert_stock_batch(session, stock_batch):
    """Upsert a batch of stocks using PostgreSQL ON CONFLICT"""
    if not stock_batch:
        return 0, 0

    try:
        # Deduplicate within batch (keep last occurrence of each ticker)
        ticker_map = {}
        for stock in stock_batch:
            ticker_map[stock['ticker']] = stock
        deduplicated_batch = list(ticker_map.values())

        # Count existing records before upsert
        tickers = [stock['ticker'] for stock in deduplicated_batch]
        existing_count = session.query(Stock).filter(Stock.ticker.in_(tickers)).count()

        # Use PostgreSQL's INSERT ... ON CONFLICT for true upsert
        for stock_data in deduplicated_batch:
            stmt = insert(Stock).values(**stock_data)

            # Define which columns to update on conflict
            update_dict = {key: stmt.excluded[key] for key in stock_data.keys() if key != 'ticker'}

            stmt = stmt.on_conflict_do_update(
                index_elements=['ticker'],
                set_=update_dict
            )

            session.execute(stmt)

        session.commit()

        # Calculate new vs updated counts
        new_count = len(deduplicated_batch) - existing_count
        updated_count = existing_count

        return max(0, new_count), updated_count

    except Exception as e:
        logger.error(f"Error in upsert batch: {str(e)}")
        session.rollback()
        return 0, 0

def load_stocks_from_csv(session, csv_file='stock_list.csv'):
    """Load all stock data from FMP CSV into ticker table"""
    if not os.path.exists(csv_file):
        logger.error(f"CSV file {csv_file} not found")
        return 0

    logger.info(f"Loading stocks from {csv_file} into ticker table...")

    stocks_loaded = 0
    stocks_updated = 0
    batch_size = 1000
    stock_batch = []

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                # Parse date fields
                ipo_date = None
                if row.get('ipoDate'):
                    try:
                        ipo_date = datetime.strptime(row['ipoDate'], '%Y-%m-%d').date()
                    except:
                        pass

                # Parse boolean fields
                def parse_bool(value):
                    if isinstance(value, str):
                        return value.lower() in ('true', '1', 'yes')
                    return bool(value) if value is not None else False

                # Parse numeric fields
                def parse_float(value):
                    try:
                        return float(value) if value and value != '' else None
                    except:
                        return None

                def parse_int(value):
                    try:
                        return int(float(value)) if value and value != '' else None
                    except:
                        return None

                stock_data = {
                    'ticker': row.get('symbol', '').strip().upper(),
                    'price': parse_float(row.get('price')),
                    'beta': parse_float(row.get('beta')),
                    'vol_avg': parse_int(row.get('volAvg')),
                    'market_cap': parse_float(row.get('mktCap')),
                    'last_div': parse_float(row.get('lastDiv')),
                    'range': row.get('range', '').strip() or None,
                    'changes': parse_float(row.get('changes')),
                    'company_name': row.get('companyName', '').strip() or None,
                    'currency': row.get('currency', '').strip() or None,
                    'cik': row.get('cik', '').strip() or None,
                    'isin': row.get('isin', '').strip() or None,
                    'cusip': row.get('cusip', '').strip() or None,
                    'exchange': row.get('exchange', '').strip() or None,
                    'exchange_short_name': row.get('exchangeShortName', '').strip() or None,
                    'industry': row.get('industry', '').strip() or None,
                    'website': row.get('website', '').strip() or None,
                    'description': row.get('description', '').strip() or None,
                    'ceo': row.get('ceo', '').strip() or None,
                    'sector': row.get('sector', '').strip() or None,
                    'country': row.get('country', '').strip() or None,
                    'full_time_employees': parse_int(row.get('fullTimeEmployees')),
                    'phone': row.get('phone', '').strip() or None,
                    'address': row.get('address', '').strip() or None,
                    'city': row.get('city', '').strip() or None,
                    'state': row.get('state', '').strip() or None,
                    'zip': row.get('zip', '').strip() or None,
                    'dcf_diff': parse_float(row.get('dcfDiff')),
                    'dcf': parse_float(row.get('dcf')),
                    'image': row.get('image', '').strip() or None,
                    'ipo_date': ipo_date,
                    'default_image': parse_bool(row.get('defaultImage')),
                    'is_etf': parse_bool(row.get('isEtf')),
                    'is_actively_trading': parse_bool(row.get('isActivelyTrading')),
                    'is_adr': parse_bool(row.get('isAdr')),
                    'is_fund': parse_bool(row.get('isFund')),
                    'created_at': get_eastern_datetime(),
                    'updated_at': get_eastern_datetime()
                }

                if stock_data['ticker']:  # Only add if ticker exists
                    stock_batch.append(stock_data)
                    stocks_loaded += 1

                # Process batch when full
                if len(stock_batch) >= batch_size:
                    new_count, update_count = upsert_stock_batch(session, stock_batch)
                    stocks_loaded += new_count
                    stocks_updated += update_count
                    logger.info(f"Processed batch: {new_count} new, {update_count} updated (Total: {stocks_loaded} new, {stocks_updated} updated)")
                    stock_batch = []

            except Exception as e:
                logger.error(f"Error processing row {row.get('symbol', 'Unknown')}: {str(e)}")
                continue

    # Process remaining stocks
    if stock_batch:
        new_count, update_count = upsert_stock_batch(session, stock_batch)
        stocks_loaded += new_count
        stocks_updated += update_count
        logger.info(f"Processed final batch: {new_count} new, {update_count} updated")

    logger.info(f"Successfully processed {stocks_loaded} new stocks and {stocks_updated} updated stocks from CSV")
    return stocks_loaded + stocks_updated

def main():
    """Main function to seed stock table from CSV"""
    print("Starting stock table seeding from CSV...")

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        stocks_processed = load_stocks_from_csv(session)
        if stocks_processed == 0:
            logger.error("No stocks loaded from CSV.")
            return

        total_stocks = session.query(Stock).count()
        logger.info(f"✅ Stock seeding completed. Total stocks in database: {total_stocks}")

    except Exception as e:
        logger.error(f"Error in stock seeding process: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == '__main__':
    main()
