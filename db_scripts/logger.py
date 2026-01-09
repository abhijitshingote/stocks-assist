#!/usr/bin/env python3
"""
Centralized Logging Utility for Data Scripts

Provides consistent logging across all data update and initialization scripts.
Logs are written to:
- logs_detailed.txt - Full verbose output (newest entries at top)
- logs_summary.txt - 1-2 line summary per script run (newest entries at top)

Usage:
    from db_scripts.logger import get_logger, write_summary

    logger = get_logger('my_script_name')
    logger.info("Starting process...")
    
    # At the end of your script:
    write_summary('my_script_name', 'SUCCESS', 'Updated 500 stocks')
"""

import logging
import os
import time
from datetime import datetime
import pytz
from pathlib import Path


def get_test_ticker_limit():
    """
    Get the test ticker limit from environment variable.
    Returns None if not set or in normal mode.
    
    Usage in scripts:
        from db_scripts.logger import get_test_ticker_limit
        test_limit = get_test_ticker_limit()
        if test_limit:
            logger.info(f"🧪 TEST MODE: Limiting to {test_limit} tickers")
            tickers = tickers[:test_limit]
    """
    limit = os.getenv('TEST_TICKER_LIMIT')
    if limit:
        try:
            return int(limit)
        except ValueError:
            return None
    return None

# Get the project root directory (parent of db_scripts)
PROJECT_ROOT = Path(__file__).parent.parent
LOGS_DETAILED_PATH = PROJECT_ROOT / 'logs_detailed.txt'
LOGS_SUMMARY_PATH = PROJECT_ROOT / 'logs_summary.txt'


def get_eastern_datetime():
    """Get current datetime in Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)


def prepend_to_file(filepath, content):
    """
    Prepend content to a file (newest entries at top).
    Creates the file if it doesn't exist.
    """
    filepath = Path(filepath)
    
    # Read existing content
    existing_content = ""
    if filepath.exists():
        existing_content = filepath.read_text()
    
    # Write new content at top
    with open(filepath, 'w') as f:
        f.write(content)
        if existing_content:
            f.write(existing_content)


class PrependFileHandler(logging.Handler):
    """
    A logging handler that prepends log entries to a file.
    Collects all log entries during the script run, then prepends them all at once.
    """
    
    def __init__(self, filepath, script_name):
        super().__init__()
        self.filepath = Path(filepath)
        self.script_name = script_name
        self.log_entries = []
        
    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_entries.append(msg)
        except Exception:
            self.handleError(record)
    
    def flush_to_file(self):
        """Write all collected log entries to the file (prepended)"""
        if not self.log_entries:
            return
            
        # Create header for this log session
        timestamp = get_eastern_datetime().strftime('%Y-%m-%d %H:%M:%S %Z')
        header = f"\n{'='*80}\n[{self.script_name}] Session started at {timestamp}\n{'='*80}\n"
        footer = f"\n{'='*80}\n[{self.script_name}] Session ended\n{'='*80}\n\n"
        
        # Combine all entries
        content = header + '\n'.join(self.log_entries) + footer
        
        # Prepend to file
        prepend_to_file(self.filepath, content)
        self.log_entries = []


# Global registry to track handlers for cleanup
_active_handlers = {}


def get_logger(script_name):
    """
    Get a configured logger for a script.
    
    Args:
        script_name: Name of the script (used in log headers)
    
    Returns:
        A configured logger instance
    """
    # Create unique logger name
    logger_name = f"stocks_assist.{script_name}"
    logger = logging.getLogger(logger_name)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # Detailed log file handler (prepending)
    file_handler = PrependFileHandler(LOGS_DETAILED_PATH, script_name)
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    # Store handler reference for cleanup
    _active_handlers[logger_name] = file_handler
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def flush_logger(script_name):
    """
    Flush the logger's file handler to write logs to file.
    Call this at the end of your script.
    """
    logger_name = f"stocks_assist.{script_name}"
    if logger_name in _active_handlers:
        _active_handlers[logger_name].flush_to_file()


def write_summary(script_name, status, message, records_affected=None, duration_seconds=None):
    """
    Write a summary line to the summary log file.

    Args:
        script_name: Name of the script
        status: Status string (e.g., 'SUCCESS', 'FAILED', 'WARNING')
        message: Brief summary message
        records_affected: Optional count of records affected
        duration_seconds: Optional duration in seconds

    Example output:
        2025-12-18 10:30:15 | daily_price_update | SUCCESS | Updated 500 stocks | Duration: 45.2s
    """
    timestamp = get_eastern_datetime().strftime('%Y-%m-%d %H:%M:%S')

    parts = [timestamp, script_name, status, message]

    if records_affected is not None:
        parts.append(f"Records: {records_affected}")

    if duration_seconds is not None:
        # Format duration nicely
        if duration_seconds < 1:
            duration_str = f"{duration_seconds:.1f}ms"
        elif duration_seconds < 60:
            duration_str = f"{duration_seconds:.1f}s"
        else:
            minutes = int(duration_seconds // 60)
            seconds = duration_seconds % 60
            duration_str = f"{minutes}m {seconds:.1f}s"
        parts.append(f"Duration: {duration_str}")

    summary_line = " | ".join(parts) + "\n"
    
    prepend_to_file(LOGS_SUMMARY_PATH, summary_line)


def log_script_start(logger, script_name, description=None):
    """Helper to log script start with consistent formatting"""
    logger.info("=" * 60)
    logger.info(f"=== Starting {script_name} ===")
    if description:
        logger.info(f"Description: {description}")
    logger.info(f"Started at: {get_eastern_datetime().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("=" * 60)


def log_script_end(logger, script_name, duration_seconds=None, success=True):
    """Helper to log script end with consistent formatting"""
    logger.info("=" * 60)
    status = "Completed" if success else "Failed"
    if duration_seconds is not None:
        duration_str = format_duration(duration_seconds)
        logger.info(f"=== {script_name} {status} in {duration_str} ===")
    else:
        logger.info(f"=== {script_name} {status} ===")
    logger.info("=" * 60)


def format_duration(seconds):
    """Format duration in hours, minutes, seconds"""
    hrs = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hrs > 0:
        return f"{hrs}h {mins}m {secs}s"
    elif mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def progress_bar(current, total, width=40, prefix="Progress:", suffix=""):
    """
    Create a progress bar string that can be printed inline.

    Args:
        current: Current progress value
        total: Total value to reach
        width: Width of the progress bar
        prefix: Text before the progress bar
        suffix: Text after the progress bar

    Returns:
        String representation of progress bar
    """
    if total == 0:
        return f"{prefix} [{('█' * width)}] 100% {suffix}"

    percentage = int((current / total) * 100)
    filled_width = int(width * current / total)
    bar = "█" * filled_width + "░" * (width - filled_width)

    return f"{prefix} [{bar}] {percentage}% ({current}/{total}) {suffix}"


class ProgressTracker:
    """
    Track progress and display updates with a progress bar.
    Updates the display less frequently to avoid spam.
    
    Shows both terminal progress (via print) and logs progress at regular intervals.
    """

    def __init__(self, total, logger, update_interval=50, log_interval=None, prefix="Progress:"):
        """
        Args:
            total: Total number of items to process
            logger: Logger instance
            update_interval: How often to update terminal display (items)
            log_interval: How often to log to file (items). Defaults to 10% of total or every 200 items
            prefix: Prefix for progress bar
        """
        self.total = total
        self.logger = logger
        self.update_interval = update_interval
        # Log every 10% or every 200 items, whichever is smaller
        self.log_interval = log_interval or min(max(total // 10, 1), 200)
        self.prefix = prefix
        self.last_update = 0
        self.last_log = 0
        self.start_time = time.time()
        self.db_writes = 0  # Track DB write batches

    def update(self, current, suffix=""):
        """Update progress display if enough items have been processed"""
        if current - self.last_update >= self.update_interval or current == self.total:
            elapsed = time.time() - self.start_time
            rate = current / elapsed if elapsed > 0 else 0
            eta = (self.total - current) / rate if rate > 0 else 0

            eta_str = f" | ETA: {format_duration(eta)}" if eta > 0 else ""
            progress_str = progress_bar(current, self.total, prefix=self.prefix, suffix=f"{suffix}{eta_str}")

            # Use \r to overwrite the current line
            print(f"\r{progress_str}", end="", flush=True)

            self.last_update = current

            # Log to file at regular intervals
            if current - self.last_log >= self.log_interval or current == self.total:
                pct = int((current / self.total) * 100) if self.total > 0 else 100
                self.logger.info(f"📡 API Progress: {current}/{self.total} ({pct}%) {suffix} | ETA: {format_duration(eta) if eta > 0 else 'done'}")
                self.last_log = current

    def log_db_write(self, records_count, batch_num=None):
        """Log a database write operation"""
        self.db_writes += 1
        batch_info = f"batch {batch_num}" if batch_num else f"batch {self.db_writes}"
        self.logger.info(f"💾 DB Write: {records_count:,} records ({batch_info})")

    def finish(self, suffix=""):
        """Finalize the progress display"""
        print()  # New line after progress bar
        elapsed = time.time() - self.start_time
        self.logger.info(f"✅ Completed in {format_duration(elapsed)} {suffix}")


def estimate_processing_time(script_name, item_count, processing_rate_per_minute=None):
    """
    Estimate processing time based on historical data or defaults.

    Args:
        script_name: Name of the script for rate lookup
        item_count: Number of items to process
        processing_rate_per_minute: Override rate (items per minute)

    Returns:
        Estimated time in minutes
    """
    # Historical processing rates (items per minute) based on observed data
    rates = {
        'seed_earnings_from_fmp': 45,  # ~45 tickers per minute based on logs
        'seed_analyst_estimates_from_fmp': 50,  # ~50 tickers per minute
        'seed_ohlc_from_fmp': 35,  # ~35 tickers per minute
        'seed_profiles_from_fmp': 60,  # ~60 tickers per minute
        'seed_ratios_from_fmp': 70,  # ~70 tickers per minute
    }

    rate = processing_rate_per_minute or rates.get(script_name, 30)  # Default 30 items/minute

    estimated_minutes = item_count / rate

    # Add some buffer for variability
    estimated_minutes *= 1.2

    return estimated_minutes

