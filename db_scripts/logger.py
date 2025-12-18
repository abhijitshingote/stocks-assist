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
from datetime import datetime
import pytz
from pathlib import Path

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

