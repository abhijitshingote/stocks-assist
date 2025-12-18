#!/usr/bin/env python3
"""
Daily Price Update Script (FMP API)
Fetches today's stock price data using FMP batch quote API and updates the OHLC table.
Overwrites existing data for today's date if it exists.
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import requests
import time
import pytz
import argparse

# Add backend and db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Base, Ticker, OHLC
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'daily_price_update'
logger = get_logger(SCRIPT_NAME)

# Load environment variables
load_dotenv()

# FMP API configuration
FMP_API_KEY = os.getenv('FMP_API_KEY')
if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables")

BASE_URL = 'https://financialmodelingprep.com/stable'


def get_eastern_date():
    """Get current date in Eastern Time (handles EST/EDT automatically)"""
    eastern = pytz.timezone('US/Eastern')
    eastern_now = datetime.now(eastern)
    return eastern_now.date()


def get_eastern_datetime():
    """Get current datetime in Eastern Time (handles EST/EDT automatically)"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)


def init_db():
    """Initialize database connection"""
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment variables")
    
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session(), engine


def get_existing_tickers(session):
    """Get all tickers that exist in the Ticker table"""
    try:
        tickers = session.query(Ticker.ticker).filter(
            Ticker.is_actively_trading == True
        ).all()
        ticker_list = [t[0] for t in tickers]
        logger.info(f"Found {len(ticker_list)} actively trading tickers in database")
        return ticker_list
    except Exception as e:
        logger.error(f"Error fetching existing tickers: {str(e)}")
        return []


def parse_float(value):
    """Parse float value safely"""
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def parse_int(value):
    """Parse integer value safely"""
    try:
        return int(float(value)) if value is not None else None
    except (ValueError, TypeError):
        return None


# format_duration is imported from db_scripts.logger


def fetch_eod_prices(tickers, target_date, delay=0.12, progress_interval=100):
    """
    Fetch EOD price data one ticker at a time using FMP historical-price-eod/full endpoint.
    
    Args:
        tickers: List of ticker symbols
        target_date: The date to fetch prices for
        delay: Delay between API calls in seconds
        progress_interval: How often to print progress (every N tickers)
    
    Returns:
        List of price data dictionaries
    """
    total = len(tickers)
    date_str = target_date.strftime('%Y-%m-%d')
    
    # Estimate time
    estimated_seconds = total * delay
    logger.info(f"Fetching EOD prices for {total} tickers (date: {date_str})")
    logger.info(f"Estimated time: {format_duration(estimated_seconds)} (at {delay}s/ticker)")
    logger.info("-" * 60)
    
    all_prices = []
    success_count = 0
    error_count = 0
    start_time = time.time()
    
    for i, ticker in enumerate(tickers):
        try:
            url = f"{BASE_URL}/historical-price-eod/full"
            params = {
                'apikey': FMP_API_KEY,
                'symbol': ticker,
                'from': date_str,
                'to': date_str
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle list or dict response
            historical = data if isinstance(data, list) else data.get('historical', [])
            for item in historical:
                all_prices.append({
                    'symbol': ticker,
                    **item
                })
            
            if historical:
                success_count += 1
            
            # Rate limiting
            time.sleep(delay)
            
        except Exception as e:
            error_count += 1
            # Only log errors for non-404s (missing data is expected for some tickers)
            if '404' not in str(e) and '429' not in str(e):
                logger.debug(f"Error fetching {ticker}: {str(e)}")
            continue
        
        # Progress reporting
        if (i + 1) % progress_interval == 0 or (i + 1) == total:
            elapsed = time.time() - start_time
            pct = ((i + 1) / total) * 100
            avg_time = elapsed / (i + 1)
            remaining = avg_time * (total - i - 1)
            
            logger.info(
                f"Progress: {i+1:,}/{total:,} ({pct:.1f}%) | "
                f"Found: {len(all_prices):,} | "
                f"Elapsed: {format_duration(elapsed)} | "
                f"ETA: {format_duration(remaining)}"
            )
    
    # Final summary
    total_time = time.time() - start_time
    logger.info("-" * 60)
    logger.info(f"Fetch complete: {len(all_prices):,} prices from {success_count:,} tickers")
    logger.info(f"Errors/Missing: {error_count:,} | Total time: {format_duration(total_time)}")
    
    return all_prices


def process_quote_data(quotes, existing_tickers_set):
    """
    Process FMP quote data and prepare for database insertion.
    
    FMP quote format:
    {
        "symbol": "AAPL",
        "name": "Apple Inc.",
        "price": 195.89,
        "changesPercentage": 1.5,
        "change": 2.89,
        "dayLow": 193.05,
        "dayHigh": 196.40,
        "yearHigh": 199.62,
        "yearLow": 164.08,
        "marketCap": 3000000000000,
        "priceAvg50": 185.50,
        "priceAvg200": 180.25,
        "volume": 58000000,
        "avgVolume": 55000000,
        "exchange": "NASDAQ",
        "open": 194.00,
        "previousClose": 193.00,
        "timestamp": 1699999999
    }
    """
    price_records = []
    processed_count = 0
    today = get_eastern_date()
    now = get_eastern_datetime()
    
    logger.info(f"Processing quote data for {len(quotes)} tickers...")
    
    for quote in quotes:
        try:
            ticker = quote.get('symbol')
            if not ticker or ticker not in existing_tickers_set:
                continue
            
            # Extract price data
            open_price = parse_float(quote.get('open'))
            high_price = parse_float(quote.get('dayHigh'))
            low_price = parse_float(quote.get('dayLow'))
            close_price = parse_float(quote.get('price'))  # Current price as close
            volume = parse_int(quote.get('volume'))
            
            # Skip if missing essential data
            if close_price is None:
                continue
            
            # Determine the trading date from timestamp if available
            trading_date = today
            if 'timestamp' in quote and quote['timestamp']:
                try:
                    timestamp = quote['timestamp']
                    dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
                    eastern = pytz.timezone('US/Eastern')
                    eastern_dt = dt.astimezone(eastern)
                    trading_date = eastern_dt.date()
                except (ValueError, TypeError, OSError):
                    trading_date = today
            
            price_record = {
                'ticker': ticker,
                'date': trading_date,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            }
            
            price_records.append(price_record)
            processed_count += 1
                
        except Exception as e:
            logger.error(f"Error processing quote for {quote.get('symbol', 'unknown')}: {str(e)}")
            continue
    
    logger.info(f"Processed {processed_count} valid price records from quote data")
    return price_records


def process_eod_data(eod_prices, existing_tickers_set):
    """
    Process FMP historical/EOD price data and prepare for database insertion.
    """
    price_records = []
    processed_count = 0
    
    logger.info(f"Processing EOD data for {len(eod_prices)} records...")
    
    for item in eod_prices:
        try:
            ticker = item.get('symbol')
            if not ticker or ticker not in existing_tickers_set:
                continue
            
            # Parse date
            date_str = item.get('date')
            if not date_str:
                continue
            trading_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            price_record = {
                'ticker': ticker,
                'date': trading_date,
                'open': parse_float(item.get('open')),
                'high': parse_float(item.get('high')),
                'low': parse_float(item.get('low')),
                'close': parse_float(item.get('close')),
                'volume': parse_int(item.get('volume'))
            }
            
            # Only add if we have at least a close price
            if price_record['close'] is not None:
                price_records.append(price_record)
                processed_count += 1
                
        except Exception as e:
            logger.error(f"Error processing EOD for {item.get('symbol', 'unknown')}: {str(e)}")
            continue
    
    logger.info(f"Processed {processed_count} valid price records from EOD data")
    return price_records


def bulk_upsert_prices(session, engine, price_records):
    """
    Bulk upsert price records using PostgreSQL's ON CONFLICT functionality.
    This will automatically overwrite existing records for the same ticker and date.
    """
    if not price_records:
        logger.warning("No price records to upsert")
        return 0
    
    logger.info(f"Starting bulk upsert of {len(price_records)} price records...")
    
    try:
        # Use PostgreSQL's INSERT ... ON CONFLICT for efficient upserts
        stmt = insert(OHLC).values(price_records)
        
        # Define what to do on conflict (use the constraint name)
        stmt = stmt.on_conflict_do_update(
            constraint='uq_ohlc',
            set_=dict(
                open=stmt.excluded.open,
                high=stmt.excluded.high,
                low=stmt.excluded.low,
                close=stmt.excluded.close,
                volume=stmt.excluded.volume
            )
        )
        
        # Execute the bulk upsert
        session.execute(stmt)
        session.commit()
        
        logger.info(f"Successfully upserted {len(price_records)} price records")
        return len(price_records)
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error in bulk upsert: {str(e)}")
        return 0


def main():
    """Main function to fetch today's price data and update the database"""
    parser = argparse.ArgumentParser(description='Daily price update from FMP API')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD). Defaults to today.')
    parser.add_argument('--delay', type=float, default=0.12,
                        help='Delay between API calls in seconds (default: 0.12)')
    parser.add_argument('--progress', type=int, default=100,
                        help='Print progress every N tickers (default: 100)')
    args = parser.parse_args()
    
    overall_start = time.time()
    logger.info("=" * 60)
    logger.info("=== Starting Daily Price Update (FMP) ===")
    logger.info("=" * 60)
    
    session, engine = init_db()
    
    try:
        # Step 1: Get existing tickers from our database
        existing_tickers = get_existing_tickers(session)
        if not existing_tickers:
            logger.error("No existing tickers found in database. Run initial data fetch first.")
            return
        
        existing_tickers_set = set(existing_tickers)
        
        # Determine target date
        if args.date:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        else:
            target_date = get_eastern_date()
        
        logger.info(f"Target date: {target_date}")
        
        # Step 2: Fetch price data (single ticker at a time)
        eod_data = fetch_eod_prices(
            existing_tickers, 
            target_date, 
            delay=args.delay,
            progress_interval=args.progress
        )
        
        # Step 3: Process and upsert the price data
        total_time = time.time() - overall_start
        if eod_data:
            price_records = process_eod_data(eod_data, existing_tickers_set)
            if price_records:
                upserted_count = bulk_upsert_prices(session, engine, price_records)
                logger.info(f"Updated {upserted_count:,} records for {target_date}")
                write_summary(SCRIPT_NAME, 'SUCCESS', f'Updated prices for {target_date}', upserted_count, duration_seconds=total_time)
            else:
                logger.warning("No valid price records after processing")
                write_summary(SCRIPT_NAME, 'WARNING', 'No valid price records after processing', duration_seconds=total_time)
        else:
            logger.warning("No price data fetched")
            write_summary(SCRIPT_NAME, 'WARNING', 'No price data fetched', duration_seconds=total_time)
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        session.rollback()
        total_time = time.time() - overall_start
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=total_time)
    finally:
        session.close()
        total_time = time.time() - overall_start
        logger.info("=" * 60)
        logger.info(f"=== Daily Price Update Completed in {format_duration(total_time)} ===")
        logger.info("=" * 60)
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    main()
