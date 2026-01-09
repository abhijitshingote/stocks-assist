"""
Shared utilities for FMP API scripts.

Provides:
- RateLimiter: Token bucket rate limiter for API calls (300/min)
- bulk_upsert: Efficient bulk upsert using raw SQL
- fetch_with_rate_limit: API fetch wrapper with rate limiting
"""

import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from sqlalchemy import text


class RateLimiter:
    """
    Token bucket rate limiter for API calls.
    Allows bursting while maintaining average rate of calls_per_minute.
    Thread-safe for use with concurrent workers.
    """
    def __init__(self, calls_per_minute=290):
        self.calls_per_minute = calls_per_minute
        self.lock = threading.Lock()
        self.call_times = deque(maxlen=calls_per_minute)
    
    def acquire(self):
        """Wait until we can make an API call without exceeding rate limit."""
        with self.lock:
            now = time.time()
            
            # Remove timestamps older than 60 seconds
            while self.call_times and now - self.call_times[0] > 60:
                self.call_times.popleft()
            
            # If we've made max calls in the last minute, wait
            if len(self.call_times) >= self.calls_per_minute:
                sleep_time = 60 - (now - self.call_times[0]) + 0.05
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    while self.call_times and now - self.call_times[0] > 60:
                        self.call_times.popleft()
            
            self.call_times.append(now)


def fetch_with_rate_limit(url, params, rate_limiter=None, timeout=30):
    """
    Fetch data from URL with rate limiting.
    
    Args:
        url: API endpoint URL
        params: Request parameters
        rate_limiter: RateLimiter instance (optional)
        timeout: Request timeout in seconds
    
    Returns:
        Response JSON or None on error
    """
    if rate_limiter:
        rate_limiter.acquire()
    
    try:
        response = requests.get(url, params=params, timeout=timeout)
        if response.ok:
            return response.json()
        return None
    except Exception:
        return None


def bulk_upsert(engine, table_name, records, conflict_constraint, 
                conflict_columns, update_columns, chunk_size=5000):
    """
    Efficient bulk upsert using raw SQL with ON CONFLICT.
    
    Args:
        engine: SQLAlchemy engine
        table_name: Target table name
        records: List of dicts with column data
        conflict_constraint: Name of unique constraint for ON CONFLICT
        conflict_columns: Columns that form the unique key (for building set clause)
        update_columns: Columns to update on conflict
        chunk_size: Number of records per INSERT statement
    
    Returns:
        Number of records processed
    """
    if not records:
        return 0
    
    # Deduplicate records by conflict columns (keep first occurrence - API returns latest first)
    # This prevents "ON CONFLICT DO UPDATE command cannot affect row a second time" error
    seen = {}
    for rec in records:
        key = tuple(rec.get(col) for col in conflict_columns)
        if key not in seen:
            seen[key] = rec
    records = list(seen.values())
    
    # Get all column names from first record
    columns = list(records[0].keys())
    
    with engine.connect() as conn:
        try:
            total_processed = 0
            
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                
                # Build VALUES clause with parameters
                values_list = []
                params = {}
                
                for idx, rec in enumerate(chunk):
                    placeholders = []
                    for col in columns:
                        param_name = f'{col}_{idx}'
                        placeholders.append(f':{param_name}')
                        params[param_name] = rec.get(col)
                    values_list.append(f"({', '.join(placeholders)})")
                
                # Build UPDATE SET clause
                set_clause = ', '.join([
                    f"{col} = EXCLUDED.{col}" 
                    for col in update_columns
                ])
                
                sql = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES {', '.join(values_list)}
                    ON CONFLICT ON CONSTRAINT {conflict_constraint}
                    DO UPDATE SET {set_clause}
                """
                
                conn.execute(text(sql), params)
                total_processed += len(chunk)
            
            conn.commit()
            return total_processed
            
        except Exception as e:
            conn.rollback()
            raise e


def bulk_upsert_simple(engine, table_name, records, primary_key, 
                       update_columns, chunk_size=5000):
    """
    Efficient bulk upsert using raw SQL with ON CONFLICT on primary key.
    
    Args:
        engine: SQLAlchemy engine
        table_name: Target table name
        records: List of dicts with column data
        primary_key: Primary key column name (for ON CONFLICT)
        update_columns: Columns to update on conflict
        chunk_size: Number of records per INSERT statement
    
    Returns:
        Number of records processed
    """
    if not records:
        return 0
    
    # Deduplicate records by primary key (keep first occurrence - API returns latest first)
    seen = {}
    for rec in records:
        key = rec.get(primary_key)
        if key not in seen:
            seen[key] = rec
    records = list(seen.values())
    
    columns = list(records[0].keys())
    
    with engine.connect() as conn:
        try:
            total_processed = 0
            
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                
                values_list = []
                params = {}
                
                for idx, rec in enumerate(chunk):
                    placeholders = []
                    for col in columns:
                        param_name = f'{col}_{idx}'
                        placeholders.append(f':{param_name}')
                        params[param_name] = rec.get(col)
                    values_list.append(f"({', '.join(placeholders)})")
                
                set_clause = ', '.join([
                    f"{col} = EXCLUDED.{col}" 
                    for col in update_columns
                ])
                
                sql = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES {', '.join(values_list)}
                    ON CONFLICT ({primary_key})
                    DO UPDATE SET {set_clause}
                """
                
                conn.execute(text(sql), params)
                total_processed += len(chunk)
            
            conn.commit()
            return total_processed
            
        except Exception as e:
            conn.rollback()
            raise e


def process_tickers_concurrent(tickers, fetch_fn, process_fn, 
                               rate_limiter=None, max_workers=10,
                               logger=None):
    """
    Process tickers concurrently with rate limiting.
    
    Args:
        tickers: List of ticker symbols
        fetch_fn: Function(ticker) -> data from API
        process_fn: Function(ticker, data) -> list of records
        rate_limiter: RateLimiter instance
        max_workers: Number of concurrent threads
        logger: Logger instance for progress updates
    
    Returns:
        Tuple of (all_records, success_count, failed_count)
    """
    all_records = []
    success_count = 0
    failed_count = 0
    processed = 0
    
    def worker(ticker):
        if rate_limiter:
            rate_limiter.acquire()
        data = fetch_fn(ticker)
        if data:
            records = process_fn(ticker, data)
            return (ticker, records, True)
        return (ticker, [], False)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, t): t for t in tickers}
        
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                _, records, success = future.result()
                if success:
                    all_records.extend(records)
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                if logger:
                    logger.error(f"Error processing {ticker}: {e}")
            
            processed += 1
            if logger and processed % 100 == 0:
                logger.info(f"Progress: {processed}/{len(tickers)} | Records: {len(all_records)}")
    
    return all_records, success_count, failed_count

