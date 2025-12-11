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

from backend.models import Stock, Price, TriggerEvent, init_db
from sqlalchemy import func, and_, desc
from datetime import date as date_type

def load_config(config_path='db_scripts/data/triggers_config.yaml'):
    """Load triggers configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_latest_price_date(session):
    """Get the most recent date in price table"""
    result = session.query(func.max(Price.date)).scalar()
    return result

def calculate_return(session, ticker, trading_days_ago, target_date):
    """Calculate return percentage for a stock over specified trading days"""
    # Query enough data to ensure we get the required number of trading days
    # Look back up to 2x the trading days to account for weekends/holidays
    lookback_calendar_days = trading_days_ago * 2 + 10
    start_date = target_date - timedelta(days=lookback_calendar_days)
    
    prices = session.query(Price).filter(
        Price.ticker == ticker,
        Price.date >= start_date,
        Price.date <= target_date
    ).order_by(Price.date.desc()).all()
    
    # Need at least trading_days_ago + 1 days of data
    if len(prices) < trading_days_ago + 1:
        return None
    
    current_price = prices[0].close_price
    # Get the price from exactly trading_days_ago trading days ago
    old_price = prices[trading_days_ago].close_price
    
    if old_price and old_price > 0 and current_price:
        return ((current_price - old_price) / old_price) * 100
    return None

def calculate_volume_change(session, ticker, target_date):
    """Calculate volume change vs average of last 20 trading days"""
    # Query enough data to get 21 trading days (current + 20 for average)
    lookback_calendar_days = 35  # ~21 trading days
    start_date = target_date - timedelta(days=lookback_calendar_days)
    
    prices = session.query(Price).filter(
        Price.ticker == ticker,
        Price.date >= start_date,
        Price.date <= target_date
    ).order_by(Price.date.desc()).all()
    
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
    
    prices = session.query(Price).filter(
        Price.ticker == ticker,
        Price.date >= start_date,
        Price.date <= target_date
    ).order_by(Price.date.desc()).all()
    
    # Need at least the specified number of trading days
    if len(prices) < trading_days:
        return None
    
    # Get exactly 'trading_days' number of the most recent prices
    close_prices = [p.close_price for p in prices[:trading_days] if p.close_price]
    
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
    prices = session.query(Price).filter(
        Price.ticker == ticker,
        Price.date >= start_date,
        Price.date <= target_date
    ).order_by(Price.date.desc()).all()
    
    if len(prices) < lookback_trading_days + 50:
        return None
    
    # For each of the last lookback_trading_days, calculate if it was above/below 50 DMA
    days_above = 0
    days_below = 0
    max_distance_below = 0  # Most negative value
    
    for i in range(lookback_trading_days):
        if i + 50 >= len(prices):
            break
            
        current_price = prices[i].close_price
        if not current_price:
            continue
        
        # Calculate 50 DMA for this day (using the 50 days before it)
        ma_prices = [p.close_price for p in prices[i:i+50] if p.close_price]
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

def get_stock_metrics(session, stock, target_date):
    """Calculate all metrics for a stock"""
    metrics = {
        'ticker': stock.ticker,
        'market_cap': stock.market_cap,
        'price': stock.price,
        'sector': stock.sector,
        'industry': stock.industry,
    }
    
    # Get latest price data
    latest_price = session.query(Price).filter(
        Price.ticker == stock.ticker,
        Price.date == target_date
    ).first()
    
    if latest_price:
        metrics['volume'] = latest_price.volume
        metrics['close_price'] = latest_price.close_price
    
    # Calculate returns for different periods
    metrics['return_1d'] = calculate_return(session, stock.ticker, 1, target_date)
    metrics['return_5d'] = calculate_return(session, stock.ticker, 5, target_date)
    metrics['return_20d'] = calculate_return(session, stock.ticker, 20, target_date)
    metrics['return_60d'] = calculate_return(session, stock.ticker, 60, target_date)
    metrics['return_120d'] = calculate_return(session, stock.ticker, 120, target_date)
    
    # Calculate volume change
    metrics['volume_change'] = calculate_volume_change(session, stock.ticker, target_date)
    
    # Calculate moving averages
    metrics['ma_50'] = calculate_moving_average(session, stock.ticker, target_date, 50)
    metrics['ma_200'] = calculate_moving_average(session, stock.ticker, target_date, 200)
    
    # Calculate distance from 50 DMA
    if latest_price and metrics['ma_50']:
        metrics['distance_from_50ma'] = calculate_distance_from_ma(latest_price.close_price, metrics['ma_50'])
    else:
        metrics['distance_from_50ma'] = None
    
    # Calculate how many days stock has been above 50 DMA
    days_above_data = calculate_days_above_50ma(session, stock.ticker, target_date, lookback_trading_days=30)
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
    query = session.query(Stock).filter(Stock.is_actively_trading == True)
    
    global_filters = config.get('global_filters', {})
    
    # Exclude sectors
    exclude_sectors = global_filters.get('exclude_sectors', [])
    if exclude_sectors:
        query = query.filter(~Stock.sector.in_(exclude_sectors))
    
    # Exclude industries
    exclude_industries = global_filters.get('exclude_industries', [])
    if exclude_industries:
        query = query.filter(~Stock.industry.in_(exclude_industries))
    
    # Min price
    min_price = global_filters.get('min_price')
    if min_price:
        query = query.filter(Stock.price >= min_price)
    
    return query

def run_triggers(target_date=None, config_path='db_scripts/data/triggers_config.yaml'):
    """Run all triggers and store results"""
    session = init_db()
    
    # Load configuration
    config = load_config(config_path)
    
    # Determine target date
    if target_date is None:
        target_date = get_latest_price_date(session)
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    if not target_date:
        print("Error: No price data found in database")
        return
    
    print(f"Running triggers for date: {target_date}")
    print(f"Found {len(config['triggers'])} triggers to evaluate")
    
    # Delete existing trigger events for this date (for clean overwrite)
    deleted_count = session.query(TriggerEvent).filter(
        TriggerEvent.trigger_date == target_date
    ).delete()
    session.commit()
    
    if deleted_count > 0:
        print(f"Deleted {deleted_count} existing trigger events for {target_date}")
    
    # Get all stocks with global filters applied
    stocks_query = apply_global_filters(session, config)
    stocks = stocks_query.all()
    
    print(f"Evaluating {len(stocks)} stocks after global filters")
    
    # Track results
    results = {trigger['name']: [] for trigger in config['triggers']}
    
    # Evaluate each stock against each trigger
    for stock in stocks:
        metrics = get_stock_metrics(session, stock, target_date)
        
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
                results[trigger_name].append(stock.ticker)
                
                # Store in database
                trigger_metadata = {
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
                
                # Determine primary trigger value (use most relevant metric)
                trigger_value = None
                if 'return_5d' in str(conditions):
                    trigger_value = metrics.get('return_5d')
                elif 'return_1d' in str(conditions):
                    trigger_value = metrics.get('return_1d')
                elif 'return_20d' in str(conditions):
                    trigger_value = metrics.get('return_20d')
                
                # Add new trigger event (we already deleted old ones for this date)
                trigger_event = TriggerEvent(
                    ticker=stock.ticker,
                    trigger_name=trigger_name,
                    trigger_date=target_date,
                    trigger_value=trigger_value,
                    trigger_metadata=json.dumps(trigger_metadata)
                )
                session.add(trigger_event)
    
    # Commit all changes
    session.commit()
    
    # Print summary
    print("\n" + "="*60)
    print("TRIGGER RESULTS SUMMARY")
    print("="*60)
    
    total_matches = 0
    for trigger_name, tickers in results.items():
        count = len(tickers)
        total_matches += count
        print(f"\n{trigger_name}: {count} stocks")
        if count > 0 and count <= 10:
            print(f"  Tickers: {', '.join(tickers)}")
        elif count > 10:
            print(f"  Top 10: {', '.join(tickers[:10])}")
    
    print("\n" + "="*60)
    print(f"Total matches across all triggers: {total_matches}")
    print(f"Results saved to database for date: {target_date}")
    print("="*60)
    
    session.close()
    return results

def main():
    parser = argparse.ArgumentParser(description='Run stock triggers based on config')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD). Defaults to latest price date.')
    parser.add_argument('--config', type=str, default='db_scripts/data/triggers_config.yaml', help='Path to config file')
    
    args = parser.parse_args()
    
    run_triggers(target_date=args.date, config_path=args.config)

if __name__ == '__main__':
    main()

