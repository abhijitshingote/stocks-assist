from polygon import RESTClient
from datetime import datetime, timedelta, date
import os
import csv
from dotenv import load_dotenv
from sqlalchemy import create_engine, tuple_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from models import Stock, Price, ShortList, BlackList, Comment
import logging
import time
import pytz
import json
from pathlib import Path

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

def restore_user_lists(session, symbols_set, backup_dir='user_data'):
    """Restore shortlist, blacklist, and comments data from JSON backups after reset."""
    backup_path = Path(backup_dir)

    restored_shortlist = restored_blacklist = restored_comments = 0

    # --- Shortlist ---
    shortlist_file = backup_path / 'shortlist_backup.json'
    if shortlist_file.exists():
        try:
            data = json.loads(shortlist_file.read_text())
            for item in data:
                if item['ticker'] in symbols_set:
                    session.merge(ShortList(ticker=item['ticker'], notes=item.get('notes')))
                    restored_shortlist += 1
            session.commit()
            # Keep backup file; exporting will overwrite with up-to-date data
            logger.info(f"Restored {restored_shortlist} shortlist tickers from backup")
        except Exception as e:
            session.rollback()
            logger.error(f"Error restoring shortlist backup: {e}")

    # --- Blacklist ---
    blacklist_file = backup_path / 'blacklist_backup.json'
    if blacklist_file.exists():
        try:
            data = json.loads(blacklist_file.read_text())
            for item in data:
                if item['ticker'] in symbols_set:
                    session.merge(BlackList(ticker=item['ticker'], notes=item.get('notes')))
                    restored_blacklist += 1
            session.commit()
            # Keep backup file; exporting will overwrite with up-to-date data
            logger.info(f"Restored {restored_blacklist} blacklist tickers from backup")
        except Exception as e:
            session.rollback()
            logger.error(f"Error restoring blacklist backup: {e}")

    # --- Comments ---
    comments_file = backup_path / 'comments_backup.json'
    if comments_file.exists():
        try:
            data = json.loads(comments_file.read_text())
            for item in data:
                if item['ticker'] not in symbols_set:
                    continue  # skip if ticker not present in seeded data

                # Prepare datetime parsing helper
                def parse_ts(ts):
                    from datetime import datetime  # local import to avoid top clutter
                    return datetime.fromisoformat(ts) if ts else None

                comment_obj = Comment(
                    ticker=item['ticker'],
                    comment_text=item['comment_text'],
                    comment_type=item.get('comment_type', 'user'),
                    status=item.get('status', 'active'),
                    ai_source=item.get('ai_source'),
                    reviewed_by=item.get('reviewed_by'),
                    reviewed_at=parse_ts(item.get('reviewed_at')),
                    created_at=parse_ts(item.get('created_at')),
                    updated_at=parse_ts(item.get('updated_at'))
                )

                # Use merge for upsert-like behaviour to avoid duplicates
                session.add(comment_obj)
                restored_comments += 1
            session.commit()
            # Keep backup file; exporting will overwrite with up-to-date data
            logger.info(f"Restored {restored_comments} comments from backup")
        except Exception as e:
            session.rollback()
            logger.error(f"Error restoring comments backup: {e}")

def seed_database():
    """Main seeding function"""
    start_time = time.time()
    print("Starting new CSV-based database seeding process...")
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Step 1: Load all stocks from CSV
        stocks_loaded = load_stocks_from_csv(session)
        if stocks_loaded == 0:
            logger.error("No stocks loaded from CSV. Exiting.")
            return
        
        # Step 2: Get all tickers for price data
        all_stocks = session.query(Stock.ticker).all()
        symbols = [stock[0] for stock in all_stocks]
        logger.info(f"Found {len(symbols)} stocks for price data")
        
        if not symbols:
            logger.warning("No actively trading US stocks found")
            return
        
        symbols_set = set(symbols)

        # Step 3: Fetch price data from Polygon
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
        total_stocks = session.query(Stock).count()
        total_prices = session.query(Price).count()
        
        # Restore user lists now that ticker table is populated
        restore_user_lists(session, symbols_set)

        elapsed_time = time.time() - start_time
        logger.info(f"✅ Seeding completed in {int(elapsed_time // 3600)}h {int((elapsed_time % 3600) // 60)}m")
        logger.info(f"📊 Final counts: {total_stocks:,} stocks, {total_prices:,} price records")
        
    except Exception as e:
        logger.error(f"Error in seeding process: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == '__main__':
    seed_database() 