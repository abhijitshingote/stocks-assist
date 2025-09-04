#!/usr/bin/env python3
"""
Earnings Dates Fetcher using Yahoo Finance Web Scraping

This script fetches upcoming and recent earnings dates for stocks by scraping Yahoo Finance web pages
and populates the earnings table in the database.

Usage:
    python get_earnings_dates.py --ticker AAPL                    # Single ticker
    python get_earnings_dates.py --file tickers.txt               # File with tickers (one per line)
    python get_earnings_dates.py --csv tickers.csv                # CSV file with comma-separated tickers
    python get_earnings_dates.py --seachange                      # Process seachangestocks.csv file
    python get_earnings_dates.py --missing                        # Only tickers with no earnings data
    python get_earnings_dates.py --limit 50                       # Limit number of tickers to process
    python get_earnings_dates.py --ticker AAPL --debug            # Debug mode for detailed output
    python get_earnings_dates.py --seachange --debug              # Process SeaChange stocks with debug output
    python get_earnings_dates.py --missing --limit 100 --debug    # Process first 100 missing tickers with debug
    python get_earnings_dates.py --backup                         # Create backup of all earnings data to JSON
    python get_earnings_dates.py --stats                          # Show earnings coverage statistics

Note: Uses web scraping with Playwright - no API key required!
Earnings data is automatically backed up after each run to user_data/earnings_backup.json
The --missing option intelligently targets tickers without earnings data to avoid redundant processing.
Tickers are processed in descending market cap order (largest companies first) for optimal prioritization.
"""

import os
import sys
import argparse
import requests
import json
from datetime import datetime, timedelta, date
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import logging
import time
import random
from bs4 import BeautifulSoup
import re
from urllib.parse import urlencode
from playwright.sync_api import sync_playwright

# Add current directory to path to import models
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from models import Stock, Earnings, init_db

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EarningsDatesFetcher:
    def __init__(self, debug=False, conservative_mode=False):
        self.debug = debug
        self.conservative_mode = conservative_mode
        
        # Set logging level based on debug mode
        if self.debug:
            logging.getLogger().setLevel(logging.DEBUG)
        
        logger.info("Using Yahoo Finance Web Scraping - No API key required!")
        logger.info(f"Debug mode: {'enabled' if debug else 'disabled'}")
        
        # Initialize database
        database_url = os.getenv('DATABASE_URL', 'postgresql://localhost:5432/stocks_db')
        logger.info(f"Connecting to database: {database_url}")
        
        try:
            self.engine = create_engine(database_url)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            logger.info("Database connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
        
        # Enhanced rate limiting to avoid blocking
        if self.conservative_mode:
            self.base_delay = 10.0  # Very conservative delay for heavily rate-limited periods
            self.max_delay = 300.0  # Up to 5 minutes max delay
            self.max_retries = 8    # More retries for conservative mode
            logger.info("🐌 Conservative mode enabled - using very slow rate limiting")
        else:
            self.base_delay = 3.0  # Base delay between requests (further increased)
            self.max_delay = 60.0  # Maximum delay for exponential backoff
            self.max_retries = 5   # Maximum number of retries for failed requests
        self.last_request_time = 0
        
        # Configure session with realistic headers
        self.session_requests = requests.Session()
        self.session_requests.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
    
    def test_api_connectivity(self):
        """Test if we can reach Yahoo Finance with web scraping"""
        logger.info("Testing Yahoo Finance web scraping connectivity...")
        
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    delay = min(self.base_delay * (2 ** attempt) + random.uniform(0, 1), self.max_delay)
                    logger.info(f"Retry attempt {attempt + 1}, waiting {delay:.1f} seconds...")
                    time.sleep(delay)
                
                # Test with a simple page request
                test_url = "https://finance.yahoo.com/quote/AAPL"
                response = self.session_requests.get(test_url, timeout=30)
                response.raise_for_status()
                
                if "Apple Inc." in response.text or "AAPL" in response.text:
                    logger.info(f"📝 Yahoo Finance Response: Successfully accessed AAPL page")
                    logger.info("✅ Yahoo Finance web scraping connectivity test passed")
                    return True
                else:
                    logger.error("❌ Web scraping test failed - unexpected page content")
                    if attempt == self.max_retries - 1:
                        return False
                
            except Exception as e:
                error_msg = str(e).lower()
                if '429' in error_msg or 'too many requests' in error_msg or 'blocked' in error_msg:
                    logger.warning(f"Rate limited/blocked on attempt {attempt + 1}: {str(e)}")
                    if attempt == self.max_retries - 1:
                        logger.error("❌ Yahoo Finance connectivity test failed after all retries due to rate limiting/blocking")
                        logger.error("💡 Try running with --conservative mode or wait longer between runs")
                        return False
                else:
                    logger.error(f"❌ Yahoo Finance connectivity test failed: {str(e)}")
                    if attempt == self.max_retries - 1:
                        return False
        
        return False
    
    def _rate_limit_with_jitter(self):
        """Enforce rate limiting with jitter to avoid synchronization"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # Add random jitter to avoid synchronized requests
        delay = self.base_delay + random.uniform(0, 2.0)
        
        if time_since_last < delay:
            sleep_time = delay - time_since_last
            if self.debug:
                logger.info(f"Rate limiting: sleeping for {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get_earnings_data(self, ticker):
        """Get earnings data from Yahoo Finance using Playwright"""
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    # Exponential backoff with jitter
                    delay = min(self.base_delay * (2 ** attempt) + random.uniform(0, 3), self.max_delay)
                    logger.info(f"Retry attempt {attempt + 1} for {ticker}, waiting {delay:.1f} seconds...")
                    time.sleep(delay)
                else:
                    self._rate_limit_with_jitter()
                
                if self.debug:
                    logger.info(f"Scraping Yahoo Finance earnings for {ticker} using Playwright (attempt {attempt + 1})...")
                
                logger.info(f"Loading Yahoo Finance earnings page for {ticker}...")
                
                # Use Playwright to get earnings data
                earnings_data = self._scrape_with_playwright(ticker)
                
                logger.info(f"Successfully found {len(earnings_data)} earnings records for {ticker}")
                return earnings_data
                    
            except Exception as e:
                error_msg = str(e).lower()
                if 'timeout' in error_msg or 'blocked' in error_msg:
                    logger.warning(f"Playwright timeout/blocked for {ticker} on attempt {attempt + 1}: {str(e)}")
                    if attempt == self.max_retries - 1:
                        logger.error(f"Failed to get earnings data for {ticker} after {self.max_retries} attempts")
                        return []
                else:
                    logger.error(f"Error getting earnings data for {ticker} on attempt {attempt + 1}: {str(e)}")
                    if attempt == self.max_retries - 1:
                        return []
        
        return []
    
    def _scrape_with_playwright(self, ticker):
        """Use Playwright to scrape Yahoo Finance earnings data - using working method"""
        earnings_data = []
        
        try:
            with sync_playwright() as p:
                # Launch browser with additional options for better compatibility
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--disable-web-security',
                        '--disable-extensions',
                        '--no-zygote',
                        '--single-process'
                    ]
                )
                
                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                page = context.new_page()
                
                # Set longer timeout and add retries
                url = f"https://finance.yahoo.com/calendar/earnings?symbol={ticker}"
                if self.debug:
                    logger.debug(f"Navigating to: {url}")
                
                try:
                    # Navigate with extended timeout
                    page.goto(url, timeout=60000, wait_until='domcontentloaded')
                    
                    # Wait a bit for dynamic content to load
                    time.sleep(2)
                    
                    # Check if we have the expected content
                    try:
                        page.wait_for_selector("table", timeout=10000)
                        if self.debug:
                            logger.debug("Table found, extracting earnings dates...")
                        
                        rows = page.locator("table tbody tr")
                        count = rows.count()
                        if self.debug:
                            logger.debug(f"Found {count} rows in table")
                        
                        # Extract data from each row - simplified approach like working script
                        for i in range(count):
                            try:
                                date_text = rows.nth(i).locator("td").nth(2).inner_text()
                                if date_text and date_text.strip():
                                    # Parse the date string to extract meaningful earnings info
                                    earnings_entry = self._parse_earnings_date_string(date_text.strip(), ticker)
                                    if earnings_entry:
                                        earnings_data.append(earnings_entry)
                                        
                            except Exception as e:
                                if self.debug:
                                    logger.debug(f"Error extracting date from row {i}: {e}")
                                continue
                        
                        browser.close()
                        return earnings_data
                        
                    except Exception as e:
                        if self.debug:
                            logger.debug(f"Error waiting for table or extracting data: {e}")
                        # Try to get page content for debugging
                        content = page.content()
                        if "blocked" in content.lower() or "captcha" in content.lower():
                            logger.warning("Detected blocking or CAPTCHA - Yahoo Finance may be blocking automated requests")
                        elif "error" in content.lower():
                            logger.warning("Page shows error - the symbol might not exist or have earnings data")
                        else:
                            logger.warning("Page loaded but table not found - structure may have changed")
                        
                        browser.close()
                        return []
                        
                except Exception as e:
                    logger.warning(f"Error navigating to page: {e}")
                    browser.close()
                    return []
                    
        except Exception as e:
            logger.error(f"Error initializing Playwright: {e}")
            logger.error("This might indicate that Playwright browsers are not installed.")
            logger.error("Try running: playwright install chromium")
            return []
    
    def _parse_earnings_date_string(self, date_string, ticker):
        """Parse earnings date string from Yahoo Finance table"""
        try:
            # Examples of date strings from Yahoo Finance:
            # "October 23, 2025 at 4 PM EDT"
            # "January 30, 2025 at 8 AM EST"
            
            # Extract date part and time part
            parts = date_string.split(' at ')
            if len(parts) != 2:
                if self.debug:
                    logger.debug(f"Could not parse date string format: {date_string}")
                return None
                
            date_part = parts[0].strip()
            time_part = parts[1].strip()
            
            # Parse the date
            try:
                earnings_date = datetime.strptime(date_part, '%B %d, %Y').date()
            except ValueError:
                try:
                    earnings_date = datetime.strptime(date_part, '%b %d, %Y').date()
                except ValueError:
                    if self.debug:
                        logger.debug(f"Could not parse date: {date_part}")
                    return None
            
            # Parse timing (AMC/BMO)
            announcement_time = 'AMC'  # Default to after market close
            if 'AM' in time_part:
                announcement_time = 'BMO'  # Before market open
            elif 'PM' in time_part:
                announcement_time = 'AMC'  # After market close
            
            # Determine quarter from date
            month = earnings_date.month
            year = earnings_date.year
            if month in [1, 2, 3]:
                quarter = f"Q4 {year - 1}"  # Q4 earnings usually announced early next year
            elif month in [4, 5, 6]:
                quarter = f"Q1 {year}"
            elif month in [7, 8, 9]:
                quarter = f"Q2 {year}"
            else:  # month in [10, 11, 12]
                quarter = f"Q3 {year}"
            
            return {
                'type': 'calendar',
                'earnings_date': earnings_date,
                'eps_estimate': None,  # Not available in simple scraping
                'announcement_time': announcement_time,
                'quarter': quarter,
                'is_upcoming': earnings_date >= date.today()
            }
        
        except Exception as e:
            if self.debug:
                logger.debug(f"Error parsing earnings date string '{date_string}': {e}")
            return None
    
    def _parse_earnings_from_page(self, soup, ticker, url):
        """Parse earnings data from a Yahoo Finance page"""
        earnings_data = []
        
        try:
            # Method 1: Look for earnings calendar table
            earnings_data.extend(self._parse_earnings_calendar_table(soup, ticker))
            
            # Method 2: Look for analyst estimates section
            earnings_data.extend(self._parse_analyst_estimates(soup, ticker))
            
            # Method 3: Look for JSON data in script tags
            earnings_data.extend(self._parse_json_data(soup, ticker))
            
        except Exception as e:
            if self.debug:
                logger.warning(f"Error parsing page {url}: {str(e)}")
        
        return earnings_data
    
    def _parse_earnings_calendar_table(self, soup, ticker):
        """Parse earnings from calendar table format"""
        earnings_data = []
        
        try:
            page_text = soup.get_text()
            if self.debug:
                logger.debug(f"Page title: {soup.find('title').get_text() if soup.find('title') else 'No title found'}")
                logger.debug(f"Page contains 'Oops': {'Oops' in page_text}")
                logger.debug(f"Page contains ticker '{ticker}': {ticker.upper() in page_text.upper()}")
            
            # Check if page shows error message
            if 'Oops, something went wrong' in page_text:
                if self.debug:
                    logger.debug("Page returned 'Oops, something went wrong' - no earnings data available")
                return earnings_data
            
            # Look for tables with earnings information
            tables = soup.find_all('table')
            if self.debug:
                logger.debug(f"Found {len(tables)} tables on the page")
            
            for i, table in enumerate(tables):
                rows = table.find_all('tr')
                if self.debug:
                    logger.debug(f"Table {i} has {len(rows)} rows")
                
                for j, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'])
                    
                    if self.debug and j < 3:  # Show first 3 rows for debugging
                        row_text = row.get_text().strip()
                        logger.debug(f"Table {i}, Row {j}: {len(cells)} cells - '{row_text[:100]}...'")
                    
                    # Look for rows with our specific ticker
                    if len(cells) >= 4:
                        # Check if this row contains our ticker
                        row_text = row.get_text()
                        if ticker.upper() in row_text:
                            if self.debug:
                                logger.debug(f"Found ticker {ticker} in row: {row_text[:200]}")
                            try:
                                # Extract data from the structured table - flexible parsing
                                cell_texts = [cell.get_text().strip() for cell in cells]
                                
                                if self.debug:
                                    logger.debug(f"Cell contents: {cell_texts}")
                                
                                # Try to find the symbol, event, timing, and EPS in any position
                                symbol_cell = ""
                                event_cell = ""  
                                timing_cell = ""
                                eps_cell = ""
                                
                                for cell_text in cell_texts:
                                    if ticker.upper() in cell_text.upper():
                                        symbol_cell = cell_text
                                    elif 'Q' in cell_text and ('2024' in cell_text or '2025' in cell_text):
                                        event_cell = cell_text
                                    elif cell_text in ['AMC', 'BMO', 'DMT']:
                                        timing_cell = cell_text
                                    elif '$' in cell_text or (cell_text.replace('.', '').replace('-', '').isdigit() and cell_text != '-'):
                                        eps_cell = cell_text
                                
                                if symbol_cell.upper() == ticker.upper():
                                    # Extract quarter and year from event description
                                    quarter_match = re.search(r'Q(\d)\s*(\d{4})', event_cell)
                                    if quarter_match:
                                        quarter_num = int(quarter_match.group(1))
                                        year = int(quarter_match.group(2))
                                        
                                        # Estimate earnings date based on quarter
                                        earnings_date = self._estimate_earnings_date(quarter_num, year)
                                        
                                        if earnings_date:
                                            # Parse EPS estimate
                                            eps_estimate = None
                                            if eps_cell and eps_cell != '-':
                                                try:
                                                    eps_estimate = float(eps_cell.replace('$', ''))
                                                except ValueError:
                                                    pass
                                            
                                            # Parse timing (AMC/BMO)
                                            announcement_time = 'AMC'  # Default
                                            if 'BMO' in timing_cell:
                                                announcement_time = 'BMO'
                                            elif 'AMC' in timing_cell:
                                                announcement_time = 'AMC'
                                            
                                            earnings_data.append({
                                                'type': 'calendar',
                                                'earnings_date': earnings_date,
                                                'eps_estimate': eps_estimate,
                                                'announcement_time': announcement_time,
                                                'quarter': f"Q{quarter_num} {year}",
                                                'is_upcoming': earnings_date >= date.today()
                                            })
                                            
                                            if self.debug:
                                                logger.info(f"Found earnings entry: Q{quarter_num} {year} on {earnings_date}, EPS: {eps_estimate}")
                                
                            except Exception as parse_error:
                                if self.debug:
                                    logger.debug(f"Error parsing calendar table row: {parse_error}")
                                continue
                                
        except Exception as e:
            if self.debug:
                logger.debug(f"Error parsing earnings calendar table: {e}")
        
        return earnings_data
    
    def _estimate_earnings_date(self, quarter, year):
        """Estimate earnings announcement date based on quarter"""
        try:
            # Typical earnings announcement timing
            # Q1 earnings usually announced in April/May
            # Q2 earnings usually announced in July/August  
            # Q3 earnings usually announced in October/November
            # Q4 earnings usually announced in January/February (next year)
            
            quarter_months = {
                1: (4, year),      # Q1 -> April
                2: (7, year),      # Q2 -> July  
                3: (10, year),     # Q3 -> October
                4: (1, year + 1)   # Q4 -> January next year
            }
            
            month, actual_year = quarter_months.get(quarter, (1, year))
            
            # Use middle of the month as estimate
            return date(actual_year, month, 15)
            
        except Exception:
            return None
    
    def _parse_analyst_estimates(self, soup, ticker):
        """Parse earnings from analyst estimates section"""
        earnings_data = []
        
        try:
            # Look for analyst estimates sections
            estimate_sections = soup.find_all(['div', 'section'], 
                                            class_=re.compile(r'.*estimate.*|.*earning.*|.*forecast.*', re.I))
            
            for section in estimate_sections:
                text = section.get_text()
                
                # Look for EPS estimates with dates
                eps_matches = re.finditer(r'(\d{4}|\w+\s+\d{4}).*?\$?(-?\d+\.?\d*)', text)
                
                for match in eps_matches:
                    try:
                        period_str = match.group(1)
                        eps_value = float(match.group(2))
                        
                        # Try to infer date from period
                        earnings_date = self._infer_date_from_period(period_str)
                        
                        if earnings_date:
                            earnings_data.append({
                                'type': 'estimate',
                                'earnings_date': earnings_date,
                                'eps_estimate': eps_value,
                                'is_upcoming': earnings_date >= date.today()
                            })
                            
                    except Exception as parse_error:
                        if self.debug:
                            logger.debug(f"Error parsing estimate: {parse_error}")
                        continue
                        
        except Exception as e:
            if self.debug:
                logger.debug(f"Error parsing analyst estimates: {e}")
        
        return earnings_data
    
    def _parse_json_data(self, soup, ticker):
        """Parse earnings from JSON data in script tags"""
        earnings_data = []
        
        try:
            # Look for script tags containing JSON data
            scripts = soup.find_all('script')
            
            for script in scripts:
                if script.string:
                    try:
                        # Look for earnings-related JSON data
                        if 'earnings' in script.string.lower() or 'calendar' in script.string.lower():
                            # Try to extract JSON objects
                            json_matches = re.finditer(r'\{[^{}]*earnings[^{}]*\}', script.string, re.I)
                            
                            for json_match in json_matches:
                                try:
                                    json_str = json_match.group(0)
                                    data = json.loads(json_str)
                                    
                                    # Process JSON earnings data
                                    json_earnings = self._process_json_earnings(data, ticker)
                                    earnings_data.extend(json_earnings)
                                    
                                except (json.JSONDecodeError, KeyError):
                                    continue
                                    
                    except Exception as script_error:
                        if self.debug:
                            logger.debug(f"Error parsing script: {script_error}")
                        continue
                        
        except Exception as e:
            if self.debug:
                logger.debug(f"Error parsing JSON data: {e}")
        
            return earnings_data
    
    def _process_json_earnings(self, data, ticker):
        """Process earnings data from JSON"""
        earnings_data = []
        
        try:
            # Handle different JSON structures
            if isinstance(data, dict):
                # Look for earnings date
                for key, value in data.items():
                    if 'date' in key.lower() and isinstance(value, str):
                        try:
                            earnings_date = self._parse_date(value)
                            if earnings_date:
                                eps_estimate = data.get('epsEstimate') or data.get('eps') or data.get('estimate')
                                if eps_estimate and isinstance(eps_estimate, (int, float)):
                                    earnings_data.append({
                                        'type': 'json',
                                        'earnings_date': earnings_date,
                                        'eps_estimate': float(eps_estimate),
                                        'is_upcoming': earnings_date >= date.today()
                                    })
                        except:
                            continue
                
        except Exception as e:
            if self.debug:
                logger.debug(f"Error processing JSON earnings: {e}")
        
        return earnings_data
    
    def _parse_date(self, date_str):
        """Parse various date formats"""
        if not date_str:
            return None
            
        try:
            # Try different date formats
            formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%m-%d-%Y',
                '%d/%m/%Y',
                '%B %d, %Y',
                '%b %d, %Y',
                '%Y/%m/%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str.strip(), fmt).date()
                except ValueError:
                    continue
                    
        except Exception:
            pass
        
        return None
    
    def _infer_date_from_period(self, period_str):
        """Infer earnings date from period like 'Q1 2024' or '2024'"""
        try:
            current_year = datetime.now().year
            
            # Look for quarter patterns
            quarter_match = re.search(r'Q(\d)\s*(\d{4})', period_str)
            if quarter_match:
                quarter = int(quarter_match.group(1))
                year = int(quarter_match.group(2))
                
                # Estimate earnings date based on quarter
                quarter_months = {1: 4, 2: 7, 3: 10, 4: 1}  # Typical earnings months
                month = quarter_months.get(quarter, 1)
                if quarter == 4:  # Q4 earnings typically in next year
                    year += 1
                
                # Use middle of the month as estimate
                return date(year, month, 15)
            
            # Look for year only
            year_match = re.search(r'(\d{4})', period_str)
            if year_match:
                year = int(year_match.group(1))
                # Use end of year as default
                return date(year, 12, 31)
                
        except Exception:
            pass
        
        return None
    
    def _deduplicate_earnings(self, earnings_data):
        """Remove duplicate earnings entries based on date"""
        seen_dates = set()
        unique_earnings = []
        
        for earning in earnings_data:
            date_key = earning.get('earnings_date')
            if date_key and date_key not in seen_dates:
                seen_dates.add(date_key)
                unique_earnings.append(earning)
        
        return unique_earnings
    
    def parse_earnings_data(self, ticker, earnings_data):
        """Parse Yahoo Finance earnings data"""
        if not earnings_data:
            return []
        
        earnings_entries = []
        
        for earning in earnings_data:
            try:
                # Web scraped format - earnings_date is already a date object
                earnings_date = earning.get('earnings_date')
                
                if not earnings_date:
                    continue
                
                # Handle different date formats from web scraping
                if isinstance(earnings_date, date):
                    # Already a date object from scraping
                    pass
                elif hasattr(earnings_date, 'date'):
                    earnings_date = earnings_date.date()
                elif isinstance(earnings_date, str):
                    earnings_date = datetime.strptime(earnings_date, '%Y-%m-%d').date()
                elif hasattr(earnings_date, 'strftime'):
                    earnings_date = earnings_date.date()
                
                # Get EPS data
                eps_actual = earning.get('eps_actual')
                eps_estimate = earning.get('eps_estimate')
                
                # Get revenue data
                revenue_actual = earning.get('revenue_actual')
                revenue_estimate = earning.get('revenue_estimate')
                
                # Get timing from scraped data or default
                timing = earning.get('announcement_time', 'AMC')
                
                # Get quarter from scraped data or determine from date
                quarter = earning.get('quarter')
                if not quarter:
                    month = earnings_date.month
                    year = earnings_date.year
                    quarter_num = (month - 1) // 3 + 1
                    quarter = f"Q{quarter_num} {year}"
                
                # Determine if confirmed based on type or date
                is_confirmed = not earning.get('is_upcoming', False)
                
                # Extract fiscal year from quarter or date
                fiscal_year = earnings_date.year
                if quarter:
                    year_match = re.search(r'(\d{4})', quarter)
                    if year_match:
                        fiscal_year = int(year_match.group(1))
                
                # Create earnings entry
                entry = {
                    'ticker': ticker.upper(),
                    'earnings_date': earnings_date,
                    'announcement_type': 'earnings',
                    'is_confirmed': is_confirmed,
                    'quarter': quarter,
                    'fiscal_year': fiscal_year,
                    'announcement_time': timing,
                    'estimated_eps': float(eps_estimate) if eps_estimate and str(eps_estimate).lower() not in ['none', 'nan', ''] else None,
                    'actual_eps': float(eps_actual) if eps_actual and str(eps_actual).lower() not in ['none', 'nan', ''] else None,
                    'revenue_estimate': float(revenue_estimate) if revenue_estimate and str(revenue_estimate).lower() not in ['none', 'nan', ''] else None,
                    'actual_revenue': float(revenue_actual) if revenue_actual and str(revenue_actual).lower() not in ['none', 'nan', ''] else None,
                    'source': 'Yahoo Finance Web Scraping',
                    'notes': f"Yahoo Finance earnings calendar scraped on {datetime.now().strftime('%Y-%m-%d')}"
                }
                
                earnings_entries.append(entry)
                
                if self.debug:
                    logger.info(f"Created earnings entry for {ticker} - {quarter} on {earnings_date}")
                
            except Exception as e:
                logger.warning(f"Error parsing earnings data for {ticker}: {str(e)}")
                if self.debug:
                    logger.debug(f"Earnings data: {earning}")
                continue
        
        return earnings_entries
    

    
    def get_earnings_for_ticker(self, ticker):
        """Fetch earnings information for a single ticker using Yahoo Finance"""
        logger.info(f"Fetching Yahoo Finance earnings data for {ticker}")
        
        # Get earnings data from Yahoo Finance
        earnings_data = self.get_earnings_data(ticker)
        if not earnings_data:
            logger.error(f"No earnings data found for {ticker}")
            return []
        
        if self.debug:
            logger.info(f"Raw earnings data for {ticker}: {len(earnings_data)} records found")
        
        # Parse the earnings data to extract earnings information
        earnings_entries = self.parse_earnings_data(ticker, earnings_data)
        
        if self.debug:
            logger.info(f"Parsed {len(earnings_entries)} earnings entries for {ticker}")
        
        return earnings_entries
    
    def save_earnings_to_db(self, earnings_entries):
        """Save earnings entries to the database"""
        saved_count = 0
        
        for entry in earnings_entries:
            try:
                # Check if this entry already exists
                existing = self.session.query(Earnings).filter_by(
                    ticker=entry['ticker'],
                    earnings_date=entry['earnings_date'],
                    announcement_type=entry.get('announcement_type', 'earnings')
                ).first()
                
                if existing:
                    # Update existing entry
                    for key, value in entry.items():
                        if hasattr(existing, key) and value is not None:
                            setattr(existing, key, value)
                    if self.debug:
                        logger.info(f"Updated existing earnings entry for {entry['ticker']} on {entry['earnings_date']}")
                else:
                    # Create new entry
                    new_earning = Earnings(**entry)
                    self.session.add(new_earning)
                    if self.debug:
                        logger.info(f"Added new earnings entry for {entry['ticker']} on {entry['earnings_date']}")
                
                saved_count += 1
                
            except IntegrityError as e:
                logger.warning(f"Integrity error saving earnings for {entry['ticker']}: {str(e)}")
                self.session.rollback()
            except Exception as e:
                logger.error(f"Error saving earnings entry for {entry['ticker']}: {str(e)}")
                self.session.rollback()
        
        try:
            self.session.commit()
            logger.info(f"Successfully saved {saved_count} earnings entries to database")
            
            # Create backup after saving new earnings data
            try:
                self._export_earnings_backup()
                if self.debug:
                    logger.debug("Created earnings backup after saving to database")
            except Exception as backup_error:
                logger.warning(f"Failed to create earnings backup: {backup_error}")
                
        except Exception as e:
            logger.error(f"Error committing earnings to database: {str(e)}")
            self.session.rollback()
        
        return saved_count
    
    def process_ticker(self, ticker):
        """Process a single ticker: fetch and save earnings data"""
        try:
            logger.info(f"Starting to process ticker: {ticker}")
            earnings_entries = self.get_earnings_for_ticker(ticker)
            logger.info(f"Finished getting earnings data for {ticker}, found {len(earnings_entries) if earnings_entries else 0} entries")
            
            if earnings_entries:
                saved_count = self.save_earnings_to_db(earnings_entries)
                logger.info(f"Saved {saved_count} earnings entries for {ticker}")
                return saved_count
            else:
                logger.warning(f"No earnings data extracted for {ticker}")
                return 0
        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {str(e)}")
            return 0
    
    def process_multiple_tickers(self, tickers):
        """Process multiple tickers with progress tracking and rate limiting"""
        total = len(tickers)
        processed = 0
        total_saved = 0
        
        logger.info(f"Processing {total} tickers for earnings data")
        logger.info(f"Rate limiting: {self.base_delay}s base delay with jitter to avoid 429 errors")
        
        for i, ticker in enumerate(tickers, 1):
            logger.info(f"Processing {ticker} ({i}/{total})")
            
            try:
                saved_count = self.process_ticker(ticker)
                total_saved += saved_count
                processed += 1
                
                # Add extra delay between tickers when processing in batch to be more respectful
                if i < total:  # Don't delay after the last ticker
                    batch_delay = self.base_delay + random.uniform(0.5, 1.5)
                    if self.debug:
                        logger.info(f"Inter-ticker delay: {batch_delay:.1f} seconds")
                    time.sleep(batch_delay)
                
                if i % 10 == 0:  # Progress update every 10 tickers
                    logger.info(f"Progress: {i}/{total} tickers processed, {total_saved} earnings entries saved")
                    
            except KeyboardInterrupt:
                logger.info("Process interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error processing {ticker}: {str(e)}")
        
        logger.info(f"Completed: {processed}/{total} tickers processed, {total_saved} total earnings entries saved")
        return processed, total_saved
    
    def get_tickers_from_db(self, limit=None):
        """Get list of tickers from the database"""
        query = self.session.query(Stock.ticker).filter(Stock.is_actively_trading == True)
        if limit:
            query = query.limit(limit)
        return [row.ticker for row in query.all()]
    
    def get_tickers_missing_earnings(self, limit=None):
        """Get list of tickers that have no earnings data, ordered by market cap descending"""
        try:
            # Get tickers that already have earnings data
            tickers_with_earnings = set(self.session.query(Earnings.ticker).distinct().all())
            tickers_with_earnings = {ticker[0] for ticker in tickers_with_earnings}
            
            # Get all actively trading tickers without earnings data, ordered by market cap descending
            missing_tickers_query = self.session.query(Stock.ticker, Stock.market_cap).filter(
                Stock.is_actively_trading == True,
                ~Stock.ticker.in_(tickers_with_earnings)
            ).order_by(Stock.market_cap.desc().nulls_last())
            
            if limit:
                missing_tickers_query = missing_tickers_query.limit(limit)
            
            missing_tickers_data = missing_tickers_query.all()
            missing_tickers = [ticker[0] for ticker in missing_tickers_data]
            
            # Get total counts for reporting
            total_active_tickers = self.session.query(Stock).filter(Stock.is_actively_trading == True).count()
            
            logger.info(f"Found {len(missing_tickers)} tickers missing earnings data out of {total_active_tickers} total actively trading tickers")
            logger.info("📊 Processing tickers in descending market cap order (largest companies first)")
            
            if self.debug:
                logger.debug(f"Tickers with earnings: {len(tickers_with_earnings)}")
                if missing_tickers_data:
                    # Show top 5 tickers with their market caps
                    top_tickers = missing_tickers_data[:5]
                    logger.debug("Top missing tickers by market cap:")
                    for ticker, market_cap in top_tickers:
                        cap_str = f"${market_cap/1e9:.1f}B" if market_cap and market_cap >= 1e9 else f"${market_cap/1e6:.0f}M" if market_cap and market_cap >= 1e6 else f"${market_cap:,.0f}" if market_cap else "N/A"
                        logger.debug(f"  • {ticker}: {cap_str}")
            
            return missing_tickers
            
        except Exception as e:
            logger.error(f"Error finding tickers missing earnings: {e}")
            return []
    
    def get_tickers_from_csv(self, csv_file):
        """Get list of tickers from CSV file"""
        try:
            with open(csv_file, 'r') as f:
                content = f.read()
                # Split by comma and clean up whitespace
                tickers = [ticker.strip().upper() for ticker in content.split(',') if ticker.strip()]
                logger.info(f"Loaded {len(tickers)} tickers from {csv_file}")
                return tickers
        except Exception as e:
            logger.error(f"Error reading CSV file {csv_file}: {e}")
            return []
    
    def show_earnings_coverage_stats(self):
        """Show statistics about earnings data coverage"""
        try:
            # Get counts
            total_stocks = self.session.query(Stock).filter(Stock.is_actively_trading == True).count()
            tickers_with_earnings = self.session.query(Earnings.ticker).distinct().count()
            total_earnings_records = self.session.query(Earnings).count()
            
            missing_count = total_stocks - tickers_with_earnings
            coverage_pct = (tickers_with_earnings / total_stocks * 100) if total_stocks > 0 else 0
            
            logger.info("📊 Earnings Data Coverage Statistics:")
            logger.info(f"   • Total actively trading stocks: {total_stocks:,}")
            logger.info(f"   • Stocks with earnings data: {tickers_with_earnings:,}")
            logger.info(f"   • Stocks missing earnings data: {missing_count:,}")
            logger.info(f"   • Coverage percentage: {coverage_pct:.1f}%")
            logger.info(f"   • Total earnings records: {total_earnings_records:,}")
            
            if missing_count > 0:
                avg_records_per_stock = total_earnings_records // tickers_with_earnings if tickers_with_earnings > 0 else 0
                logger.info(f"   • Average records per stock: {avg_records_per_stock}")
                logger.info(f"   • Missing tickers will be processed by market cap (largest first)")
            
            return {
                'total_stocks': total_stocks,
                'tickers_with_earnings': tickers_with_earnings,
                'missing_count': missing_count,
                'coverage_pct': coverage_pct,
                'total_earnings_records': total_earnings_records
            }
            
        except Exception as e:
            logger.error(f"Error getting earnings coverage stats: {e}")
            return None
    
    def _export_earnings_backup(self):
        """Create backup of earnings data to JSON file"""
        try:
            # Create backup directory if it doesn't exist
            backup_dir = Path('user_data')
            backup_dir.mkdir(exist_ok=True)
            
            # Get all earnings records
            earnings_rows = self.session.query(Earnings).all()
            earnings_payload = []
            
            for e in earnings_rows:
                earnings_payload.append({
                    'ticker': e.ticker,
                    'earnings_date': e.earnings_date.isoformat() if e.earnings_date else None,
                    'announcement_type': e.announcement_type,
                    'is_confirmed': e.is_confirmed,
                    'quarter': e.quarter,
                    'fiscal_year': e.fiscal_year,
                    'announcement_time': e.announcement_time,
                    'estimated_eps': e.estimated_eps,
                    'actual_eps': e.actual_eps,
                    'revenue_estimate': e.revenue_estimate,
                    'actual_revenue': e.actual_revenue,
                    'source': e.source,
                    'notes': e.notes,
                    'created_at': e.created_at.isoformat() if e.created_at else None,
                    'updated_at': e.updated_at.isoformat() if e.updated_at else None
                })
            
            # Write to backup file
            backup_file = backup_dir / 'earnings_backup.json'
            with backup_file.open('w', encoding='utf-8') as f:
                json.dump(earnings_payload, f, indent=2)
            
            logger.info(f"Created earnings backup with {len(earnings_payload)} records at {backup_file}")
            
        except Exception as e:
            logger.error(f"Error creating earnings backup: {e}")
            raise
    
    def close(self):
        """Clean up database connection"""
        self.session.close()

def main():
    parser = argparse.ArgumentParser(description='Fetch earnings dates from Yahoo Finance Web Scraping')
    parser.add_argument('--ticker', type=str, help='Single ticker to process')
    parser.add_argument('--file', type=str, help='File containing tickers (one per line)')
    parser.add_argument('--csv', type=str, help='CSV file containing comma-separated tickers')
    parser.add_argument('--seachange', action='store_true', help='Process tickers from seachangestocks.csv')
    parser.add_argument('--missing', action='store_true', help='Process only tickers that have no earnings data')
    parser.add_argument('--limit', type=int, help='Limit number of tickers to process')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    parser.add_argument('--conservative', action='store_true', help='Enable conservative mode with very slow rate limiting for heavily rate-limited periods')
    parser.add_argument('--backup', action='store_true', help='Create backup of all earnings data to JSON file')
    parser.add_argument('--stats', action='store_true', help='Show earnings data coverage statistics and exit')
    
    args = parser.parse_args()
    
    # Handle backup-only operation
    if args.backup:
        logger.info("Creating earnings backup...")
        fetcher = EarningsDatesFetcher(debug=args.debug, conservative_mode=args.conservative)
        try:
            fetcher._export_earnings_backup()
            logger.info("✅ Earnings backup completed successfully")
            return 0
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")
            return 1
        finally:
            fetcher.close()
    
    # Handle stats-only operation
    if args.stats:
        logger.info("Showing earnings data coverage statistics...")
        fetcher = EarningsDatesFetcher(debug=args.debug, conservative_mode=args.conservative)
        try:
            stats = fetcher.show_earnings_coverage_stats()
            if stats:
                logger.info("✅ Statistics displayed successfully")
                return 0
            else:
                logger.error("❌ Failed to retrieve statistics")
                return 1
        except Exception as e:
            logger.error(f"❌ Stats failed: {e}")
            return 1
        finally:
            fetcher.close()
    
    # Validate arguments for regular processing
    if not any([args.ticker, args.file, args.csv, args.seachange, args.missing]):
        parser.error("Must specify either --ticker, --file, --csv, --seachange, --missing, --backup, or --stats")
    
    fetcher = EarningsDatesFetcher(debug=args.debug, conservative_mode=args.conservative)
    
    # Test API connectivity before processing
    if not fetcher.test_api_connectivity():
        logger.error("Yahoo Finance connectivity test failed. Please check your internet connection.")
        return 1
    
    try:
        if args.ticker:
            # Process single ticker
            logger.info(f"Processing single ticker: {args.ticker}")
            saved_count = fetcher.process_ticker(args.ticker.upper())
            logger.info(f"Completed: {saved_count} earnings entries saved for {args.ticker}")
            
        elif args.file:
            # Process tickers from file (one per line)
            if not os.path.exists(args.file):
                logger.error(f"File not found: {args.file}")
                return 1
            
            with open(args.file, 'r') as f:
                tickers = [line.strip().upper() for line in f if line.strip()]
            
            if args.limit:
                tickers = tickers[:args.limit]
            
            fetcher.process_multiple_tickers(tickers)
            
        elif args.csv:
            # Process tickers from CSV file (comma-separated)
            if not os.path.exists(args.csv):
                logger.error(f"CSV file not found: {args.csv}")
                return 1
            
            tickers = fetcher.get_tickers_from_csv(args.csv)
            if not tickers:
                logger.error("No tickers found in CSV file")
                return 1
            
            if args.limit:
                tickers = tickers[:args.limit]
            
            fetcher.process_multiple_tickers(tickers)
            
        elif args.seachange:
            # Process tickers from seachangestocks.csv
            csv_file = 'seachangestocks.csv'
            if not os.path.exists(csv_file):
                logger.error(f"seachangestocks.csv not found in current directory")
                return 1
            
            logger.info("Processing SeaChange stocks from seachangestocks.csv")
            tickers = fetcher.get_tickers_from_csv(csv_file)
            if not tickers:
                logger.error("No tickers found in seachangestocks.csv")
                return 1
            
            if args.limit:
                tickers = tickers[:args.limit]
                
            fetcher.process_multiple_tickers(tickers)
            
        elif args.missing:
            # Process only tickers that have no earnings data
            logger.info("Processing tickers missing earnings data...")
            
            # Show stats first
            fetcher.show_earnings_coverage_stats()
            
            tickers = fetcher.get_tickers_missing_earnings(limit=args.limit)
            if not tickers:
                logger.info("✅ All actively trading tickers already have earnings data!")
                return 0
            
            fetcher.process_multiple_tickers(tickers)
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1
    finally:
        fetcher.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 