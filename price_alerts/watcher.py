"""Poll live prices for tickers with chart levels and push ntfy alerts when near a level.

Source of alerts: user_data/abi_chart_levels.json ({TICKER: {levels: [...]}}).
Dedup state:      user_data/price_alerts_state.json ({"TICKER|level": {"date", "price", "fired_at"}}).

Usage:
  python -m price_alerts.watcher              # loop forever
  python -m price_alerts.watcher --once       # single pass
  python -m price_alerts.watcher --once --force   # ignore market hours
  python -m price_alerts.watcher --test-notify    # send a test push
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, time as dtime

import pytz
import requests
import yfinance as yf

ET = pytz.timezone('America/New_York')
USER_DATA_DIR = os.environ.get('USER_DATA_DIR', os.path.join(os.path.dirname(__file__), '..', 'user_data'))
LEVELS_FILE = os.path.join(USER_DATA_DIR, 'abi_chart_levels.json')
STATE_FILE = os.path.join(USER_DATA_DIR, 'price_alerts_state.json')

NTFY_SERVER = os.environ.get('NTFY_SERVER', 'https://ntfy.sh').rstrip('/')
NTFY_TOPIC = os.environ.get('NTFY_TOPIC', '')
POLL_SECONDS = int(os.environ.get('ALERT_POLL_SECONDS', '300'))
NEAR_PCT = float(os.environ.get('ALERT_NEAR_PCT', '0.5'))
CHART_BASE_URL = os.environ.get('ALERT_CHART_BASE_URL', '').rstrip('/')

MARKET_OPEN = dtime(9, 30)
MARKET_CLOSE = dtime(16, 0)

log = logging.getLogger('price_alerts')


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_json(path, data):
    tmp = f'{path}.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def load_levels():
    out = {}
    for ticker, entry in load_json(LEVELS_FILE).items():
        levels = entry.get('levels') if isinstance(entry, dict) else None
        if levels:
            out[ticker.upper()] = [float(x) for x in levels]
    return out


def market_open(now):
    return now.weekday() < 5 and MARKET_OPEN <= now.time() < MARKET_CLOSE


def fetch_prices(tickers, today):
    """Last 1m close per ticker; drops tickers whose last bar isn't from today (holidays, halts)."""
    df = yf.download(
        tickers, period='1d', interval='1m', group_by='ticker',
        progress=False, threads=True, auto_adjust=False,
    )
    prices = {}
    if df is None or df.empty:
        return prices
    for t in tickers:
        try:
            closes = (df[t]['Close'] if t in df.columns.get_level_values(0) else df['Close']).dropna()
        except KeyError:
            continue
        if closes.empty:
            continue
        ts = closes.index[-1]
        ts = ts.tz_convert(ET) if ts.tzinfo else ET.localize(ts)
        if ts.date() != today:
            continue
        prices[t] = float(closes.iloc[-1])
    return prices


def notify(title, message, ticker=None, tags='chart_with_upwards_trend'):
    if not NTFY_TOPIC:
        log.warning('NTFY_TOPIC not set; skipping push: %s | %s', title, message)
        return
    headers = {'Title': title, 'Tags': tags, 'Priority': 'high'}
    if ticker and CHART_BASE_URL:
        headers['Click'] = f'{CHART_BASE_URL}/stock/{ticker}'
    resp = requests.post(f'{NTFY_SERVER}/{NTFY_TOPIC}', data=message.encode(), headers=headers, timeout=10)
    resp.raise_for_status()


def run_once(force=False):
    now = datetime.now(ET)
    if not force and not market_open(now):
        log.info('market closed; skip')
        return
    levels = load_levels()
    if not levels:
        log.info('no chart levels')
        return
    today = now.date()
    prices = fetch_prices(sorted(levels), today)
    missing = sorted(set(levels) - set(prices))
    if missing:
        log.warning('no price for %s', ','.join(missing))

    state = load_json(STATE_FILE)
    today_iso = today.isoformat()
    state = {k: v for k, v in state.items() if v.get('date') == today_iso}
    fired = 0
    for ticker, price in prices.items():
        for level in levels[ticker]:
            dist_pct = (price - level) / level * 100
            if abs(dist_pct) > NEAR_PCT:
                continue
            key = f'{ticker}|{level}'
            if key in state:
                continue
            side = 'above' if dist_pct >= 0 else 'below'
            msg = f'{ticker} {price:.2f} is {abs(dist_pct):.2f}% {side} level {level:g}'
            log.info('ALERT %s', msg)
            notify(f'{ticker} near {level:g}', msg, ticker=ticker)
            state[key] = {'date': today_iso, 'price': price, 'fired_at': now.isoformat()}
            fired += 1
    save_json(STATE_FILE, state)
    log.info('checked %d tickers, fired %d', len(prices), fired)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--force', action='store_true', help='ignore market hours')
    parser.add_argument('--test-notify', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if args.test_notify:
        notify('stocks-assist test', 'Price alerts are wired up.', tags='white_check_mark')
        log.info('test push sent to %s/%s', NTFY_SERVER, NTFY_TOPIC)
        return
    if args.once:
        run_once(force=args.force)
        return
    log.info('watching every %ds, near=%.2f%%, topic=%s', POLL_SECONDS, NEAR_PCT, NTFY_TOPIC or '(unset)')
    while True:
        try:
            run_once(force=args.force)
        except Exception:
            log.exception('poll failed')
        time.sleep(POLL_SECONDS)


if __name__ == '__main__':
    main()
