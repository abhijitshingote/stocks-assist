"""
Seed Index and IndexPrice tables with data from Yahoo Finance

This script:
1. Creates/updates index metadata in the 'index' table
2. Fetches historical price data from Yahoo Finance 
3. Populates the 'index_price' table with daily price data

Indices/ETFs included: SPX (S&P 500), QQQ (Nasdaq 100), IWM (Russell 2000)
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import yfinance as yf
import logging
import pytz
import time

# Add backend directory to Python path to import models
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
from models import Index, IndexPrice, get_eastern_datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Define indices to track
INDICES = {
    'SPX': {
        'yahoo_symbol': '^GSPC',
        'name': 'S&P 500',
        'description': 'Standard & Poor\'s 500 Index - Large cap U.S. equity index',
        'index_type': 'index'
    },
    'QQQ': {
        'yahoo_symbol': 'QQQ',
        'name': 'Invesco QQQ Trust',
        'description': 'Tracks the Nasdaq-100 Index - Large cap U.S. tech/growth equity ETF',
        'index_type': 'etf'
    },
    'IWM': {
        'yahoo_symbol': 'IWM',
        'name': 'iShares Russell 2000 ETF',
        'description': 'Tracks the Russell 2000 Index - Small cap U.S. equity ETF',
        'index_type': 'etf'
    }
}


def upsert_index(session, symbol, index_data):
    """Upsert index metadata into the index table"""
    try:
        stmt = insert(Index).values(
            symbol=symbol,
            yahoo_symbol=index_data['yahoo_symbol'],
            name=index_data['name'],
            description=index_data['description'],
            index_type=index_data['index_type'],
            is_active=True,
            created_at=get_eastern_datetime(),
            updated_at=get_eastern_datetime()
        )

        # Update all fields except symbol on conflict
        update_dict = {
            'yahoo_symbol': stmt.excluded.yahoo_symbol,
            'name': stmt.excluded.name,
            'description': stmt.excluded.description,
            'index_type': stmt.excluded.index_type,
            'is_active': stmt.excluded.is_active,
            'updated_at': stmt.excluded.updated_at
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol'],
            set_=update_dict
        )

        session.execute(stmt)
        session.commit()
        logger.info(f"✓ Upserted index metadata for {symbol}")
        return True

    except Exception as e:
        logger.error(f"Error upserting index {symbol}: {str(e)}")
        session.rollback()
        return False


def download_index_history(yahoo_symbol, period='1y', max_retries=3):
    """Download historical price data from Yahoo Finance with retry logic"""
    
    # Use yfinance.download() directly - matching working VIX example pattern
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = attempt * 2
                logger.info(f"Retry {attempt + 1}/{max_retries} after {wait_time}s delay...")
                time.sleep(wait_time)
            
            logger.info(f"Downloading {period} of data for {yahoo_symbol}...")
            
            # Convert period to start/end dates
            end_date = datetime.now()
            if period == '1y':
                start_date = end_date - timedelta(days=365)
            elif period == '2y':
                start_date = end_date - timedelta(days=730)
            elif period == '5y':
                start_date = end_date - timedelta(days=1825)
            elif period == 'max':
                start_date = datetime(1970, 1, 1)
            elif period.endswith('d'):
                days = int(period[:-1])
                start_date = end_date - timedelta(days=days)
            elif period.endswith('mo'):
                months = int(period[:-2])
                start_date = end_date - timedelta(days=months*30)
            else:
                start_date = end_date - timedelta(days=365)
            
            # Use yfinance.download() - same pattern as working VIX example
            df = yf.download(
                yahoo_symbol,
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval="1d",
                progress=False,
                auto_adjust=False,
                actions=False,
            )
            
            if df is None or df.empty:
                logger.warning(f"No data returned for {yahoo_symbol} on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    continue
                return pd.DataFrame()

            # Handle possible MultiIndex columns (yfinance sometimes returns per-ticker level)
            if isinstance(df.columns, pd.MultiIndex):
                try:
                    df = df.xs(yahoo_symbol, axis=1, level=1)
                except Exception:
                    # Fallback: take top-level names
                    df.columns = df.columns.get_level_values(0)
            
            # Ensure strictly daily bars (one row per date)
            df = df.sort_index()
            # Normalize to date (strip any time component) and de-duplicate
            df["_date"] = pd.to_datetime(df.index).date
            df = (
                df.reset_index(drop=True)
                .groupby("_date", as_index=False)
                .last()
                .rename(columns={"_date": "date"})
            )

            # Coerce numeric columns
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Normalize column names to match our schema
            df = df.rename(columns={
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
                'Volume': 'volume'
            })

            # Keep only the columns we need
            available_cols = [col for col in ['date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume'] if col in df.columns]
            df = df[available_cols]
            
            logger.info(f"✓ Successfully downloaded {len(df)} rows for {yahoo_symbol}")
            return df

        except Exception as e:
            logger.error(f"Error downloading data for {yahoo_symbol} (attempt {attempt + 1}): {str(e)}")
            if attempt < max_retries - 1:
                continue
            else:
                logger.error(f"Failed to download {yahoo_symbol} after {max_retries} attempts")
                return pd.DataFrame()


def upsert_index_prices(session, symbol, df):
    """Upsert index price data into the index_price table"""
    if df.empty:
        logger.warning(f"No price data to insert for {symbol}")
        return 0

    try:
        inserted_count = 0
        updated_count = 0
        
        for _, row in df.iterrows():
            stmt = insert(IndexPrice).values(
                symbol=symbol,
                date=row['date'],
                open_price=float(row['open_price']) if pd.notna(row['open_price']) else None,
                high_price=float(row['high_price']) if pd.notna(row['high_price']) else None,
                low_price=float(row['low_price']) if pd.notna(row['low_price']) else None,
                close_price=float(row['close_price']) if pd.notna(row['close_price']) else None,
                volume=float(row['volume']) if pd.notna(row['volume']) else None,
                created_at=get_eastern_datetime(),
                updated_at=get_eastern_datetime()
            )

            # Check if record exists before upserting
            existing = session.query(IndexPrice).filter(
                IndexPrice.symbol == symbol,
                IndexPrice.date == row['date']
            ).first()

            if existing:
                updated_count += 1
            else:
                inserted_count += 1

            # Update all fields except symbol and date on conflict
            update_dict = {
                'open_price': stmt.excluded.open_price,
                'high_price': stmt.excluded.high_price,
                'low_price': stmt.excluded.low_price,
                'close_price': stmt.excluded.close_price,
                'volume': stmt.excluded.volume,
                'updated_at': stmt.excluded.updated_at
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol', 'date'],
                set_=update_dict
            )

            session.execute(stmt)

        session.commit()
        logger.info(f"✓ Processed {len(df)} price records for {symbol} ({inserted_count} new, {updated_count} updated)")
        return len(df)

    except Exception as e:
        logger.error(f"Error upserting price data for {symbol}: {str(e)}")
        session.rollback()
        return 0


def main():
    """Main function to seed index data from Yahoo Finance"""
    import argparse

    parser = argparse.ArgumentParser(description='Seed index/ETF data from Yahoo Finance')
    parser.add_argument('--period', type=str, default='1y', 
                       help='Period of historical data to fetch (e.g., 1y, 2y, 5y, max)')
    parser.add_argument('--symbols', type=str, nargs='+',
                       help='Specific symbols to update (default: all defined indices)')
    args = parser.parse_args()

    print(f"Starting index data seeding from Yahoo Finance (period: {args.period})...")

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Determine which indices to process
        symbols_to_process = args.symbols if args.symbols else INDICES.keys()
        
        total_prices = 0
        for symbol in symbols_to_process:
            if symbol not in INDICES:
                logger.warning(f"Unknown symbol {symbol}, skipping...")
                continue

            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {symbol} ({INDICES[symbol]['name']})")
            logger.info(f"{'='*60}")

            # Step 1: Upsert index metadata
            if not upsert_index(session, symbol, INDICES[symbol]):
                logger.error(f"Failed to upsert index metadata for {symbol}, skipping price data")
                continue

            # Step 2: Download price history
            yahoo_symbol = INDICES[symbol]['yahoo_symbol']
            df = download_index_history(yahoo_symbol, period=args.period)

            # Step 3: Upsert price data
            if not df.empty:
                count = upsert_index_prices(session, symbol, df)
                total_prices += count

        # Final statistics
        total_indices = session.query(Index).count()
        total_index_prices = session.query(IndexPrice).count()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"✅ Index seeding completed successfully!")
        logger.info(f"   Total indices in database: {total_indices}")
        logger.info(f"   Total index price records: {total_index_prices}")
        logger.info(f"   New/updated prices this run: {total_prices}")
        logger.info(f"{'='*60}")

    except Exception as e:
        logger.error(f"Error in index seeding process: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    main()

