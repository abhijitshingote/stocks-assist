#!/usr/bin/env python3
"""
Trigger Scanner Script

Run triggers based on config to identify stock candidates.
Usage: python db_scripts/update_data/run_triggers.py [--date YYYY-MM-DD]
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import yaml
import json

# Add project root to path to import models
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.models import Ticker, OHLC
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration
import time
from sqlalchemy import func, and_, desc, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from datetime import date as date_type
from dotenv import load_dotenv
import pytz

load_dotenv()

# Script name for logging
SCRIPT_NAME = 'run_triggers'
logger = get_logger(SCRIPT_NAME)


def init_db():
    """Initialize database connection"""
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment variables")
    
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()


def load_config(config_path='db_scripts/data/triggers_config.yaml'):
    """Load triggers configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_latest_price_date(session):
    """Get the most recent date in OHLC table"""
    result = session.query(func.max(OHLC.date)).scalar()
    return result


def calculate_return(session, ticker, trading_days_ago, target_date):
    """Calculate return percentage for a stock over specified trading days"""
    # Query enough data to ensure we get the required number of trading days
    # Look back up to 2x the trading days to account for weekends/holidays
    lookback_calendar_days = trading_days_ago * 2 + 10
    start_date = target_date - timedelta(days=lookback_calendar_days)
    
    prices = session.query(OHLC).filter(
        OHLC.ticker == ticker,
        OHLC.date >= start_date,
        OHLC.date <= target_date
    ).order_by(OHLC.date.desc()).all()
    
    # Need at least trading_days_ago + 1 days of data
    if len(prices) < trading_days_ago + 1:
        return None
    
    current_price = prices[0].close
    # Get the price from exactly trading_days_ago trading days ago
    old_price = prices[trading_days_ago].close
    
    if old_price and old_price > 0 and current_price:
        return ((current_price - old_price) / old_price) * 100
    return None


def calculate_volume_change(session, ticker, target_date):
    """Calculate volume change vs average of last 20 trading days"""
    # Query enough data to get 21 trading days (current + 20 for average)
    lookback_calendar_days = 35  # ~21 trading days
    start_date = target_date - timedelta(days=lookback_calendar_days)
    
    prices = session.query(OHLC).filter(
        OHLC.ticker == ticker,
        OHLC.date >= start_date,
        OHLC.date <= target_date
    ).order_by(OHLC.date.desc()).all()
    
    # Need at least 11 trading days (current + 10 for reasonable average)
    if len(prices) < 11:
        return None
    
    current_volume = prices[0].volume
    if not current_volume:
        return None
    
    # Calculate average volume from the next 20 trading days (excluding current)
    avg_volumes = [p.volume for p in prices[1:21] if p.volume]
    if not avg_volumes or len(avg_volumes) < 10:
        return None
    
    avg_volume = sum(avg_volumes) / len(avg_volumes)
    if avg_volume > 0:
        return (current_volume / avg_volume) * 100
    return None


def calculate_moving_average(session, ticker, target_date, trading_days):
    """Calculate simple moving average for specified trading days"""
    # Query enough calendar days to ensure we get the required trading days
    # Approximately 1.5x for weekends/holidays
    lookback_calendar_days = int(trading_days * 1.5 + 10)
    start_date = target_date - timedelta(days=lookback_calendar_days)
    
    prices = session.query(OHLC).filter(
        OHLC.ticker == ticker,
        OHLC.date >= start_date,
        OHLC.date <= target_date
    ).order_by(OHLC.date.desc()).all()
    
    # Need at least the specified number of trading days
    if len(prices) < trading_days:
        return None
    
    # Get exactly 'trading_days' number of the most recent prices
    close_prices = [p.close for p in prices[:trading_days] if p.close]
    
    if len(close_prices) < trading_days:
        return None
    
    return sum(close_prices) / len(close_prices)


def calculate_distance_from_ma(current_price, ma_value):
    """Calculate percentage distance from moving average"""
    if not current_price or not ma_value or ma_value == 0:
        return None
    return ((current_price - ma_value) / ma_value) * 100


def calculate_days_above_50ma(session, ticker, target_date, lookback_trading_days=30):
    """
    Calculate how many days in the recent period the stock was above its 50 DMA.
    Returns a dict with:
    - days_above: number of days above 50 DMA
    - days_below: number of days below 50 DMA  
    - percent_above: percentage of days above
    - max_distance_below: worst distance below (negative number)
    """
    # Get enough data for lookback period plus 50 DMA calculation
    lookback_calendar_days = int((lookback_trading_days + 50) * 1.5 + 10)
    start_date = target_date - timedelta(days=lookback_calendar_days)
    
    # Get all prices in descending order (most recent first)
    prices = session.query(OHLC).filter(
        OHLC.ticker == ticker,
        OHLC.date >= start_date,
        OHLC.date <= target_date
    ).order_by(OHLC.date.desc()).all()
    
    if len(prices) < lookback_trading_days + 50:
        return None
    
    # For each of the last lookback_trading_days, calculate if it was above/below 50 DMA
    days_above = 0
    days_below = 0
    max_distance_below = 0  # Most negative value
    
    for i in range(lookback_trading_days):
        if i + 50 >= len(prices):
            break
            
        current_price = prices[i].close
        if not current_price:
            continue
        
        # Calculate 50 DMA for this day (using the 50 days before it)
        ma_prices = [p.close for p in prices[i:i+50] if p.close]
        if len(ma_prices) < 50:
            continue
            
        ma_50 = sum(ma_prices) / len(ma_prices)
        distance = ((current_price - ma_50) / ma_50) * 100
        
        if distance >= 0:
            days_above += 1
        else:
            days_below += 1
            if distance < max_distance_below:
                max_distance_below = distance
    
    total_days = days_above + days_below
    if total_days == 0:
        return None
    
    return {
        'days_above': days_above,
        'days_below': days_below,
        'percent_above': (days_above / total_days) * 100,
        'max_distance_below': max_distance_below if days_below > 0 else 0
    }


def get_stock_metrics(session, ticker_obj, target_date):
    """Calculate all metrics for a stock"""
    metrics = {
        'ticker': ticker_obj.ticker,
        'market_cap': ticker_obj.market_cap,
        'price': ticker_obj.price,
        'sector': ticker_obj.sector,
        'industry': ticker_obj.industry,
    }
    
    # Get latest price data from OHLC
    latest_price = session.query(OHLC).filter(
        OHLC.ticker == ticker_obj.ticker,
        OHLC.date == target_date
    ).first()
    
    if latest_price:
        metrics['volume'] = latest_price.volume
        metrics['close_price'] = latest_price.close
    
    # Calculate returns for different periods
    metrics['return_1d'] = calculate_return(session, ticker_obj.ticker, 1, target_date)
    metrics['return_5d'] = calculate_return(session, ticker_obj.ticker, 5, target_date)
    metrics['return_20d'] = calculate_return(session, ticker_obj.ticker, 20, target_date)
    metrics['return_60d'] = calculate_return(session, ticker_obj.ticker, 60, target_date)
    metrics['return_120d'] = calculate_return(session, ticker_obj.ticker, 120, target_date)
    
    # Calculate volume change
    metrics['volume_change'] = calculate_volume_change(session, ticker_obj.ticker, target_date)
    
    # Calculate moving averages
    metrics['ma_50'] = calculate_moving_average(session, ticker_obj.ticker, target_date, 50)
    metrics['ma_200'] = calculate_moving_average(session, ticker_obj.ticker, target_date, 200)
    
    # Calculate distance from 50 DMA
    if latest_price and metrics['ma_50']:
        metrics['distance_from_50ma'] = calculate_distance_from_ma(latest_price.close, metrics['ma_50'])
    else:
        metrics['distance_from_50ma'] = None
    
    # Calculate how many days stock has been above 50 DMA
    days_above_data = calculate_days_above_50ma(session, ticker_obj.ticker, target_date, lookback_trading_days=30)
    if days_above_data:
        metrics['days_above_50ma'] = days_above_data['days_above']
        metrics['days_below_50ma'] = days_above_data['days_below']
        metrics['percent_above_50ma'] = days_above_data['percent_above']
        metrics['max_distance_below_50ma'] = days_above_data['max_distance_below']
    else:
        metrics['days_above_50ma'] = None
        metrics['days_below_50ma'] = None
        metrics['percent_above_50ma'] = None
        metrics['max_distance_below_50ma'] = None
    
    return metrics


def evaluate_condition(metrics, condition):
    """Evaluate a single condition against stock metrics"""
    field = condition['field']
    operator = condition['operator']
    value = condition['value']
    
    if field not in metrics or metrics[field] is None:
        return False
    
    metric_value = metrics[field]
    
    if operator == '>':
        return metric_value > value
    elif operator == '<':
        return metric_value < value
    elif operator == '>=':
        return metric_value >= value
    elif operator == '<=':
        return metric_value <= value
    elif operator == '==':
        return metric_value == value
    elif operator == '!=':
        return metric_value != value
    
    return False


def apply_global_filters(session, config):
    """Build query with global filters applied"""
    query = session.query(Ticker).filter(Ticker.is_actively_trading == True)
    
    global_filters = config.get('global_filters', {})
    
    # Exclude sectors
    exclude_sectors = global_filters.get('exclude_sectors', [])
    if exclude_sectors:
        query = query.filter(~Ticker.sector.in_(exclude_sectors))
    
    # Exclude industries
    exclude_industries = global_filters.get('exclude_industries', [])
    if exclude_industries:
        query = query.filter(~Ticker.industry.in_(exclude_industries))
    
    # Min price
    min_price = global_filters.get('min_price')
    if min_price:
        query = query.filter(Ticker.price >= min_price)
    
    return query


def run_triggers(target_date=None, config_path='db_scripts/data/triggers_config.yaml'):
    """Run all triggers and print results (no TriggerEvent storage)"""
    start_time = time.time()
    session = init_db()
    
    # Load configuration
    config = load_config(config_path)
    
    # Determine target date
    if target_date is None:
        target_date = get_latest_price_date(session)
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    if not target_date:
        logger.error("Error: No price data found in database")
        return
    
    logger.info(f"Running triggers for date: {target_date}")
    logger.info(f"Found {len(config['triggers'])} triggers to evaluate")
    
    # Get all stocks with global filters applied
    stocks_query = apply_global_filters(session, config)
    stocks = stocks_query.all()
    
    logger.info(f"Evaluating {len(stocks)} stocks after global filters")
    
    # Track results
    results = {trigger['name']: [] for trigger in config['triggers']}
    results_with_metrics = {trigger['name']: [] for trigger in config['triggers']}
    
    # Evaluate each stock against each trigger
    for i, ticker_obj in enumerate(stocks):
        if (i + 1) % 500 == 0:
            logger.info(f"  Progress: {i + 1}/{len(stocks)} stocks evaluated...")
        
        metrics = get_stock_metrics(session, ticker_obj, target_date)
        
        # Apply min volume filter if volume was calculated
        min_volume = config.get('global_filters', {}).get('min_volume')
        if min_volume and metrics.get('volume'):
            if metrics['volume'] < min_volume:
                continue
        
        # Check each trigger
        for trigger in config['triggers']:
            trigger_name = trigger['name']
            conditions = trigger['conditions']
            
            # All conditions must be met
            all_conditions_met = all(
                evaluate_condition(metrics, condition) 
                for condition in conditions
            )
            
            if all_conditions_met:
                results[trigger_name].append(ticker_obj.ticker)
                
                # Store metadata for detailed output
                trigger_metadata = {
                    'ticker': ticker_obj.ticker,
                    'return_1d': metrics.get('return_1d'),
                    'return_5d': metrics.get('return_5d'),
                    'return_20d': metrics.get('return_20d'),
                    'return_60d': metrics.get('return_60d'),
                    'volume_change': metrics.get('volume_change'),
                    'market_cap': metrics.get('market_cap'),
                    'price': metrics.get('close_price'),
                    'ma_50': metrics.get('ma_50'),
                    'ma_200': metrics.get('ma_200'),
                    'distance_from_50ma': metrics.get('distance_from_50ma'),
                    'days_above_50ma': metrics.get('days_above_50ma'),
                    'percent_above_50ma': metrics.get('percent_above_50ma'),
                    'max_distance_below_50ma': metrics.get('max_distance_below_50ma'),
                }
                results_with_metrics[trigger_name].append(trigger_metadata)
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("TRIGGER RESULTS SUMMARY")
    logger.info("="*60)
    
    total_matches = 0
    for trigger_name, tickers in results.items():
        count = len(tickers)
        total_matches += count
        logger.info(f"\n{trigger_name}: {count} stocks")
        if count > 0 and count <= 10:
            logger.info(f"  Tickers: {', '.join(tickers)}")
        elif count > 10:
            logger.info(f"  Top 10: {', '.join(tickers[:10])}")
    
    elapsed = time.time() - start_time
    logger.info("\n" + "="*60)
    logger.info(f"Total matches across all triggers: {total_matches}")
    logger.info(f"Results computed for date: {target_date}")
    logger.info(f"Time taken: {format_duration(elapsed)}")
    logger.info("="*60)
    
    write_summary(SCRIPT_NAME, 'SUCCESS', f'{len(config["triggers"])} triggers evaluated', total_matches, duration_seconds=elapsed)
    
    session.close()
    flush_logger(SCRIPT_NAME)
    return results, results_with_metrics


def main():
    parser = argparse.ArgumentParser(description='Run stock triggers based on config')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD). Defaults to latest price date.')
    parser.add_argument('--config', type=str, default='db_scripts/data/triggers_config.yaml', help='Path to config file')
    parser.add_argument('--output', type=str, help='Output JSON file for detailed results')
    
    args = parser.parse_args()
    
    results, detailed_results = run_triggers(target_date=args.date, config_path=args.config)
    
    # Optionally save detailed results to JSON
    if args.output and results:
        output_data = {
            'date': args.date or str(datetime.now().date()),
            'triggers': detailed_results
        }
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        logger.info(f"\nDetailed results saved to: {args.output}")


if __name__ == '__main__':
    main()
