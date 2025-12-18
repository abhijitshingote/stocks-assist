"""
Seed index constituents from Wikipedia.

PURPOSE: Populate which stocks belong to which index.
TABLES UPDATED:
  - indices: Index definitions (DJI, SPX, NDX)
  - index_components: Mapping of index → ticker

This script fetches index constituents from Wikipedia:
  - https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
  - https://en.wikipedia.org/wiki/Nasdaq-100
  - https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average

Run AFTER seed_tickers_from_fmp.py (needs tickers table populated).
"""

from datetime import datetime
import os
import sys
import time
import requests
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import argparse
import pytz

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from models import Ticker, IndexComponents
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

# Script name for logging
SCRIPT_NAME = 'seed_index_constituents_fmp'
logger = get_logger(SCRIPT_NAME)

load_dotenv()

# User-Agent header to avoid 403 errors from Wikipedia
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Index definitions with their Wikipedia URLs and parsing info
INDEX_DEFINITIONS = {
    'DJI': {
        'name': 'Dow Jones Industrial Average',
        'url': 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average',
    },
    'SPX': {
        'name': 'S&P 500',
        'url': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
    },
    'NDX': {
        'name': 'NASDAQ 100',
        'url': 'https://en.wikipedia.org/wiki/Nasdaq-100',
    }
}


def get_eastern_now():
    """Get current datetime in US/Eastern timezone"""
    return datetime.now(pytz.timezone("US/Eastern"))


def find_symbol_column(df):
    """Find the symbol/ticker column in a dataframe"""
    # Flatten multi-level columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [' '.join(str(c) for c in col).strip() for col in df.columns.values]
    
    # Look for symbol/ticker column
    for col in df.columns:
        col_str = str(col).lower()
        if 'symbol' in col_str or 'ticker' in col_str:
            return col
    return None


def extract_symbols_from_table(df):
    """Extract stock symbols from a dataframe, handling various formats"""
    symbol_col = find_symbol_column(df)
    
    if symbol_col is not None:
        symbols = df[symbol_col].astype(str).tolist()
    else:
        # If no symbol column found, try to find a column with stock-like values
        # Stock symbols are usually 1-5 uppercase letters
        for col in df.columns:
            values = df[col].astype(str).tolist()
            # Check if values look like stock symbols
            symbol_like = [v for v in values if v.replace('.', '').replace('-', '').isalpha() 
                          and 1 <= len(v) <= 6 and v.upper() == v and v != 'NAN']
            if len(symbol_like) > len(values) * 0.5:  # More than 50% look like symbols
                symbols = values
                break
        else:
            return None
    
    # Clean symbols
    cleaned = []
    for sym in symbols:
        if pd.isna(sym) or sym.upper() == 'NAN' or not sym.strip():
            continue
        sym = sym.strip().replace('.', '-')
        # Basic validation: 1-6 chars, mostly letters
        if 1 <= len(sym) <= 6:
            cleaned.append(sym.upper())
    
    return cleaned if cleaned else None


def fetch_dji_constituents():
    """Fetch Dow Jones constituents from Wikipedia"""
    url = INDEX_DEFINITIONS['DJI']['url']
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
        
        # Try each table to find one with stock symbols
        for i, df in enumerate(tables):
            if len(df) >= 25 and len(df) <= 35:  # DJI has 30 stocks
                symbols = extract_symbols_from_table(df)
                if symbols and len(symbols) >= 25:
                    logger.info(f"  Found DJI symbols in table {i}")
                    return symbols
        
        logger.error("Could not find Dow Jones table")
        return None
    except Exception as e:
        logger.error(f"Error fetching DJI: {e}")
        return None


def fetch_spx_constituents():
    """Fetch S&P 500 constituents from Wikipedia"""
    url = INDEX_DEFINITIONS['SPX']['url']
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
        
        # First table is the S&P 500 constituents
        if tables:
            df = tables[0]
            symbols = extract_symbols_from_table(df)
            if symbols and len(symbols) >= 400:
                return symbols
        
        logger.error("Could not find S&P 500 table")
        return None
    except Exception as e:
        logger.error(f"Error fetching SPX: {e}")
        return None


def fetch_ndx_constituents():
    """Fetch NASDAQ 100 constituents from Wikipedia"""
    url = INDEX_DEFINITIONS['NDX']['url']
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
        
        # Find table with ~100 stocks
        for i, df in enumerate(tables):
            if len(df) >= 95 and len(df) <= 110:
                symbols = extract_symbols_from_table(df)
                if symbols and len(symbols) >= 95:
                    logger.info(f"  Found NDX symbols in table {i}")
                    return symbols
        
        logger.error("Could not find NASDAQ 100 table")
        return None
    except Exception as e:
        logger.error(f"Error fetching NDX: {e}")
        return None


def fetch_index_constituents(index_symbol):
    """Fetch constituents for a given index"""
    if index_symbol == 'DJI':
        return fetch_dji_constituents()
    elif index_symbol == 'SPX':
        return fetch_spx_constituents()
    elif index_symbol == 'NDX':
        return fetch_ndx_constituents()
    return None


def upsert_index(session, symbol, name):
    """Upsert an index definition - only symbol and name columns"""
    try:
        # First check if it exists
        result = session.execute(
            text("SELECT symbol FROM indices WHERE symbol = :symbol"),
            {'symbol': symbol}
        ).fetchone()
        
        if result:
            # Update existing
            session.execute(
                text("UPDATE indices SET name = :name WHERE symbol = :symbol"),
                {'symbol': symbol, 'name': name}
            )
        else:
            # Insert new - only use symbol and name
            session.execute(
                text("INSERT INTO indices (symbol, name) VALUES (:symbol, :name)"),
                {'symbol': symbol, 'name': name}
            )
        session.commit()
        return True
    except Exception as e:
        logger.error(f"Error upserting index {symbol}: {e}")
        session.rollback()
        return False


def upsert_index_components(session, index_symbol, tickers):
    """Upsert index components, only for tickers that exist in tickers table"""
    if not tickers:
        return 0, 0
    
    # Get existing tickers in our database
    existing_tickers = set(
        t[0] for t in session.query(Ticker.ticker).filter(
            Ticker.ticker.in_(tickers)
        ).all()
    )
    
    added = 0
    skipped = 0
    
    for ticker in tickers:
        if ticker not in existing_tickers:
            skipped += 1
            continue
        
        try:
            stmt = insert(IndexComponents).values(
                index_symbol=index_symbol,
                ticker=ticker
            )
            stmt = stmt.on_conflict_do_nothing(constraint='uq_index_component')
            result = session.execute(stmt)
            if result.rowcount > 0:
                added += 1
        except Exception as e:
            logger.error(f"Error adding {ticker} to {index_symbol}: {e}")
    
    session.commit()
    return added, skipped


def update_sync_metadata(session, key):
    """Update sync metadata using raw SQL"""
    try:
        result = session.execute(
            text("SELECT key FROM sync_metadata WHERE key = :key"),
            {'key': key}
        ).fetchone()
        
        if result:
            session.execute(
                text("UPDATE sync_metadata SET last_synced_at = :ts WHERE key = :key"),
                {'key': key, 'ts': datetime.utcnow()}
            )
        else:
            session.execute(
                text("INSERT INTO sync_metadata (key, last_synced_at) VALUES (:key, :ts)"),
                {'key': key, 'ts': datetime.utcnow()}
            )
        session.commit()
    except Exception as e:
        logger.warning(f"Could not update sync metadata: {e}")
        session.rollback()


def seed_index(session, index_symbol):
    """Seed a single index and its constituents"""
    if index_symbol not in INDEX_DEFINITIONS:
        logger.error(f"Unknown index: {index_symbol}")
        return False
    
    index_def = INDEX_DEFINITIONS[index_symbol]
    logger.info(f"Processing {index_symbol} ({index_def['name']})...")
    
    # Fetch constituents from Wikipedia
    components = fetch_index_constituents(index_symbol)
    
    if not components:
        logger.error(f"  Failed to fetch constituents for {index_symbol}")
        return False
    
    logger.info(f"  Fetched {len(components)} constituents from Wikipedia")
    
    # Upsert index
    if not upsert_index(session, index_symbol, index_def['name']):
        return False
    
    # Upsert components
    added, skipped = upsert_index_components(session, index_symbol, components)
    logger.info(f"  Added: {added}, Skipped (not in tickers table): {skipped}")
    
    return True


# format_duration is imported from db_scripts.logger


def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Seed index constituents from Wikipedia')
    parser.add_argument('--index', type=str, default=None,
                        help='Specific index to seed (DJI, SPX, NDX). Default: all')
    parser.add_argument('--delay', type=float, default=1.0,
                        help='Delay between requests (default: 1.0s)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Index Constituents Seeding from Wikipedia")
    logger.info("=" * 60)

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        indices_to_process = [args.index] if args.index else list(INDEX_DEFINITIONS.keys())
        
        for index_symbol in indices_to_process:
            seed_index(session, index_symbol)
            time.sleep(args.delay)
            print()
        
        # Update sync metadata
        update_sync_metadata(session, 'index_constituents_last_sync')
        
        logger.info("=" * 60)
        logger.info("Summary:")
        
        # Use raw SQL for counts
        total_indices = session.execute(text("SELECT COUNT(*) FROM indices")).scalar()
        total_components = session.execute(text("SELECT COUNT(*) FROM index_components")).scalar()
        logger.info(f"  Total indices: {total_indices}")
        logger.info(f"  Total index components: {total_components}")
        
        # Show breakdown per index
        result = session.execute(text("""
            SELECT i.symbol, COUNT(ic.ticker) as cnt
            FROM indices i
            LEFT JOIN index_components ic ON i.symbol = ic.index_symbol
            GROUP BY i.symbol
            HAVING COUNT(ic.ticker) > 0
        """))
        for row in result:
            logger.info(f"    {row[0]}: {row[1]} components")
        
        elapsed = time.time() - start_time
        logger.info(f"  Time taken: {format_duration(elapsed)}")
        logger.info("=" * 60)
        logger.info("✅ Index constituents seeding completed!")
        write_summary(SCRIPT_NAME, 'SUCCESS', f'{total_indices} indices, {total_components} components', total_components, duration_seconds=elapsed)

    except Exception as e:
        logger.error(f"Error: {e}")
        session.rollback()
        elapsed = time.time() - start_time
        write_summary(SCRIPT_NAME, 'FAILED', str(e), duration_seconds=elapsed)
        raise
    finally:
        session.close()
        flush_logger(SCRIPT_NAME)


if __name__ == '__main__':
    main()
