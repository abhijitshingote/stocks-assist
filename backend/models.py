import os
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, Boolean, ForeignKey, UniqueConstraint, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import pytz

Base = declarative_base()

# ------------------------------------------------------------
# 1. Ticker (from Company Screener - basic info, bulk updates)
# Source: /stable/company-screener
# ------------------------------------------------------------
class Ticker(Base):
    __tablename__ = "tickers"

    ticker = Column(String(20), primary_key=True)
    company_name = Column(String(255))
    exchange = Column(String(100))
    exchange_short_name = Column(String(20))
    sector = Column(String(100))
    industry = Column(String(100))
    country = Column(String(50))
    
    # Screener metrics (updated frequently)
    price = Column(Float)
    market_cap = Column(BigInteger)
    beta = Column(Float)
    volume = Column(BigInteger)
    last_annual_dividend = Column(Float)
    
    # Flags
    is_etf = Column(Boolean, default=False)
    is_fund = Column(Boolean, default=False)
    is_actively_trading = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")))
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")), 
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))


# ------------------------------------------------------------
# 2. CompanyProfile (from Profile endpoint - detailed info)
# Source: /stable/profile?symbol=AAPL
# ------------------------------------------------------------
class CompanyProfile(Base):
    __tablename__ = "company_profiles"

    ticker = Column(String(20), ForeignKey("tickers.ticker"), primary_key=True)
    
    # Identifiers
    cik = Column(String(20))
    isin = Column(String(20))
    cusip = Column(String(20))
    
    # Company details
    description = Column(Text)
    ceo = Column(String(255))
    website = Column(String(255))
    
    # Location
    phone = Column(String(50))
    address = Column(String(255))
    city = Column(String(100))
    state = Column(String(100))
    zip = Column(String(20))
    
    # Other profile data
    full_time_employees = Column(Integer)
    ipo_date = Column(Date)
    image = Column(String(255))
    currency = Column(String(20))
    
    # Market data (from profile endpoint)
    vol_avg = Column(BigInteger)
    last_div = Column(Float)
    price = Column(Float)
    range_52_week = Column(String(50))
    change = Column(Float)
    change_percentage = Column(Float)
    
    # Additional flags
    is_adr = Column(Boolean, default=False)
    default_image = Column(Boolean, default=False)

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 3. OHLC (Daily price history)
# ------------------------------------------------------------
class OHLC(Base):
    __tablename__ = "ohlc"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(20), ForeignKey("tickers.ticker"))
    date = Column(Date)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)

    __table_args__ = (UniqueConstraint("ticker", "date", name="uq_ohlc"),)

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 4. Index table (S&P 500, NASDAQ 100, etc.)
# ------------------------------------------------------------
class Index(Base):
    __tablename__ = "indices"

    symbol = Column(String(20), primary_key=True)
    name = Column(String(255))
    description = Column(Text)
    index_type = Column(String(20))  # 'index', 'etf', etc.
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")))
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))


# ------------------------------------------------------------
# 4b. IndexPrice table (Daily price history for indices/ETFs)
# ------------------------------------------------------------
class IndexPrice(Base):
    __tablename__ = "index_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20))
    date = Column(Date)
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    volume = Column(BigInteger)
    created_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")))
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))

    __table_args__ = (UniqueConstraint("symbol", "date", name="uq_index_price"),)


# ------------------------------------------------------------
# 5. IndexComponents (mapping index → tickers)
# ------------------------------------------------------------
class IndexComponents(Base):
    __tablename__ = "index_components"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_symbol = Column(String(20), ForeignKey("indices.symbol"))
    ticker = Column(String(20), ForeignKey("tickers.ticker"))

    __table_args__ = (UniqueConstraint("index_symbol", "ticker", name="uq_index_component"),)

    index_rel = relationship("Index")
    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 6. RatiosTTM (from /stable/ratios-ttm)
# ------------------------------------------------------------
class RatiosTTM(Base):
    __tablename__ = "ratios_ttm"

    ticker = Column(String(20), ForeignKey("tickers.ticker"), primary_key=True)
    
    # Profitability
    gross_profit_margin = Column(Float)
    operating_profit_margin = Column(Float)
    net_profit_margin = Column(Float)
    
    # Valuation
    pe_ratio = Column(Float)
    peg_ratio = Column(Float)
    price_to_book = Column(Float)
    price_to_sales = Column(Float)
    price_to_free_cash_flow = Column(Float)
    
    # Liquidity
    current_ratio = Column(Float)
    quick_ratio = Column(Float)
    cash_ratio = Column(Float)
    
    # Leverage
    debt_to_equity = Column(Float)
    debt_to_assets = Column(Float)
    
    # Efficiency
    asset_turnover = Column(Float)
    inventory_turnover = Column(Float)
    receivables_turnover = Column(Float)
    
    # Returns
    return_on_assets = Column(Float)
    return_on_equity = Column(Float)
    
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 7. AnalystEstimates (from /stable/analyst-estimates)
# ------------------------------------------------------------
class AnalystEstimates(Base):
    __tablename__ = "analyst_estimates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(20), ForeignKey("tickers.ticker"))
    date = Column(Date)
    
    revenue_avg = Column(BigInteger)
    revenue_low = Column(BigInteger)
    revenue_high = Column(BigInteger)
    ebitda_avg = Column(BigInteger)
    ebit_avg = Column(BigInteger)
    net_income_avg = Column(BigInteger)
    eps_avg = Column(Float)
    eps_low = Column(Float)
    eps_high = Column(Float)
    num_analysts_revenue = Column(Integer)
    num_analysts_eps = Column(Integer)

    __table_args__ = (UniqueConstraint("ticker", "date", name="uq_analyst_est"),)

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 8. Earnings (from /stable/earnings)
# ------------------------------------------------------------
class Earnings(Base):
    __tablename__ = "earnings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(20), ForeignKey("tickers.ticker"))
    date = Column(Date)
    
    eps_actual = Column(Float)
    eps_estimated = Column(Float)
    revenue_actual = Column(BigInteger)
    revenue_estimated = Column(BigInteger)

    __table_args__ = (UniqueConstraint("ticker", "date", name="uq_earnings"),)

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 9. SyncMetadata (ETL tracking)
# ------------------------------------------------------------
class SyncMetadata(Base):
    __tablename__ = "sync_metadata"

    key = Column(String(100), primary_key=True)
    last_synced_at = Column(DateTime)


# ------------------------------------------------------------
# 10. StockMetrics (pre-computed screener/dashboard metrics)
# ------------------------------------------------------------
class StockMetrics(Base):
    __tablename__ = "stock_metrics"

    ticker = Column(String(20), ForeignKey("tickers.ticker"), primary_key=True)
    company_name = Column(String(255))
    country = Column(String(50))
    sector = Column(String(100))
    industry = Column(String(100))
    ipo_date = Column(Date)
    market_cap = Column(BigInteger)
    current_price = Column(Float)
    range_52_week = Column(String(50))
    volume = Column(BigInteger)
    dollar_volume = Column(Float)
    avg_vol_10d = Column(Float)
    vol_vs_10d_avg = Column(Float)
    
    # Price change percentages
    dr_1 = Column(Float)   # 1-day return
    dr_5 = Column(Float)   # 5-day return
    dr_20 = Column(Float)  # 20-day return
    dr_60 = Column(Float)  # 60-day return
    dr_120 = Column(Float) # 120-day return
    
    # Volatility
    atr20 = Column(Float)  # 20-day Average True Range (as %)
    
    # P/E Ratio trajectory (by fiscal year, using analyst estimates)
    pe_t_minus_1 = Column(Float)  # P/E for year t-1
    pe_t = Column(Float)          # P/E for current year t
    pe_t_plus_1 = Column(Float)   # P/E for year t+1
    pe_t_plus_2 = Column(Float)   # P/E for year t+2
    
    # P/S Ratio trajectory (by fiscal year, using analyst estimates)
    ps_t_minus_1 = Column(Float)  # P/S for year t-1
    ps_t = Column(Float)          # P/S for current year t
    ps_t_plus_1 = Column(Float)   # P/S for year t+1
    ps_t_plus_2 = Column(Float)   # P/S for year t+2
    
    # Growth metrics (YoY from analyst estimates, in %)

    # YoY Revenue Growth by year (from analyst estimates)
    rev_growth_t_minus_1 = Column(Float)  # Revenue growth t-1 year
    rev_growth_t = Column(Float)          # Revenue growth current year
    rev_growth_t_plus_1 = Column(Float)   # Revenue growth t+1 year
    rev_growth_t_plus_2 = Column(Float)   # Revenue growth t+2 year

    # YoY EPS Growth by year (from analyst estimates)
    eps_growth_t_minus_1 = Column(Float)  # EPS growth t-1 year
    eps_growth_t = Column(Float)          # EPS growth current year
    eps_growth_t_plus_1 = Column(Float)   # EPS growth t+1 year
    eps_growth_t_plus_2 = Column(Float)   # EPS growth t+2 year
    
    # Relative strength (percentile rank 1-100)
    rsi = Column(Integer)
    rsi_mktcap = Column(Integer)  # RSI percentile within market cap bucket
    
    # Short interest (nullable for future use)
    short_float = Column(Float)
    short_ratio = Column(Float)
    short_interest = Column(Float)
    low_float = Column(Boolean)
    
    # Float/Liquidity data (from shares_float table)
    float_shares = Column(BigInteger)         # Number of shares available for trading
    outstanding_shares = Column(BigInteger)   # Total shares outstanding
    free_float = Column(Float)                # Percentage of shares available for trading
    
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 11. HistoricalRSI (daily RSI time series for all stocks)
# ------------------------------------------------------------
class HistoricalRSI(Base):
    __tablename__ = "historical_rsi"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(20), ForeignKey("tickers.ticker"))
    date = Column(Date)
    
    # RSI percentile rank (1-100) across all stocks for this date
    rsi_global = Column(Integer)
    
    # RSI percentile rank (1-100) within market cap bucket for this date
    rsi_mktcap = Column(Integer)
    
    # Daily Moving Averages
    dma_50 = Column(Float)   # 50-day simple moving average
    dma_200 = Column(Float)  # 200-day simple moving average

    __table_args__ = (UniqueConstraint("ticker", "date", name="uq_historical_rsi"),)

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 12. RsiIndices (RSI rankings within index universes: SPX, NDX, DJI)
# ------------------------------------------------------------
class RsiIndices(Base):
    __tablename__ = "rsi_indices"

    ticker = Column(String(20), ForeignKey("tickers.ticker"), primary_key=True)
    
    # Index membership flags
    is_spx = Column(Boolean, default=False)  # S&P 500
    is_ndx = Column(Boolean, default=False)  # NASDAQ 100
    is_dji = Column(Boolean, default=False)  # Dow Jones Industrial Average
    
    # RSI percentile rank (1-100) within each index universe
    rsi_spx = Column(Integer)   # RSI percentile within S&P 500
    rsi_ndx = Column(Integer)   # RSI percentile within NASDAQ 100
    rsi_dji = Column(Integer)   # RSI percentile within Dow Jones
    
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 13. StockVolspikeGapper (volume spike and gapper detection)
# ------------------------------------------------------------
class StockVolspikeGapper(Base):
    __tablename__ = "stock_volspike_gapper"

    ticker = Column(String(20), ForeignKey("tickers.ticker"), primary_key=True)
    
    # Volume spike metrics (last 365 days)
    spike_day_count = Column(Integer)        # Number of volume spike days
    avg_volume_spike = Column(Float)         # Average volume ratio on spike days
    volume_spike_days = Column(Text)         # Array of spike dates (stored as comma-separated string)
    
    # Gapper metrics (last 365 days)
    gapper_day_count = Column(Integer)       # Number of gap-up days
    avg_return_gapper = Column(Float)        # Average return on gap days
    gap_days = Column(Text)                  # Array of gap dates (stored as comma-separated string)
    
    # Event tracking for sorting
    last_event_date = Column(Date)           # Most recent spike or gap date
    last_event_type = Column(String(20))     # 'volume_spike' or 'gapper'
    
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 14. MainView (pre-computed screener view combining metrics, volspike/gapper, and tags)
# ------------------------------------------------------------
class MainView(Base):
    __tablename__ = "main_view"

    ticker = Column(String(20), ForeignKey("tickers.ticker"), primary_key=True)
    
    # From stock_metrics
    company_name = Column(String(255))
    country = Column(String(50))
    sector = Column(String(100))
    industry = Column(String(100))
    ipo_date = Column(Date)
    market_cap = Column(BigInteger)
    current_price = Column(Float)
    range_52_week = Column(String(50))
    volume = Column(BigInteger)
    dollar_volume = Column(Float)
    avg_vol_10d = Column(Float)
    vol_vs_10d_avg = Column(Float)
    
    # Price returns
    dr_1 = Column(Float)
    dr_5 = Column(Float)
    dr_20 = Column(Float)
    dr_60 = Column(Float)
    dr_120 = Column(Float)
    
    # Volatility
    atr20 = Column(Float)
    
    # P/E Ratio trajectory (by fiscal year)
    pe_t_minus_1 = Column(Float)
    pe_t = Column(Float)
    pe_t_plus_1 = Column(Float)
    pe_t_plus_2 = Column(Float)
    
    # P/S Ratio trajectory (by fiscal year)
    ps_t_minus_1 = Column(Float)
    ps_t = Column(Float)
    ps_t_plus_1 = Column(Float)
    ps_t_plus_2 = Column(Float)
    
    # Growth metrics
    rev_growth_t_minus_1 = Column(Float)
    rev_growth_t = Column(Float)
    rev_growth_t_plus_1 = Column(Float)
    rev_growth_t_plus_2 = Column(Float)
    eps_growth_t_minus_1 = Column(Float)
    eps_growth_t = Column(Float)
    eps_growth_t_plus_1 = Column(Float)
    eps_growth_t_plus_2 = Column(Float)
    
    # RSI
    rsi = Column(Integer)
    rsi_mktcap = Column(Integer)
    
    # Short interest
    short_float = Column(Float)
    short_ratio = Column(Float)
    short_interest = Column(Float)
    low_float = Column(Boolean)
    
    # Float/Liquidity data (from shares_float table)
    float_shares = Column(BigInteger)         # Number of shares available for trading
    outstanding_shares = Column(BigInteger)   # Total shares outstanding
    free_float = Column(Float)                # Percentage of shares available for trading
    
    # From stock_volspike_gapper
    spike_day_count = Column(Integer)
    avg_volume_spike = Column(Float)
    volume_spike_days = Column(Text)
    gapper_day_count = Column(Integer)
    avg_return_gapper = Column(Float)
    gap_days = Column(Text)
    last_event_date = Column(Date)
    last_event_type = Column(String(20))
    
    # Computed tags
    tags = Column(Text)
    
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 15. StockNotes (user-entered long-form notes/comments per stock)
# ------------------------------------------------------------
class StockNotes(Base):
    __tablename__ = "stock_notes"

    ticker = Column(String(20), ForeignKey("tickers.ticker"), primary_key=True)
    
    # Long-form notes content (markdown supported)
    notes = Column(Text)
    
    created_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")))
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 16. StockPreference (user favorite/dislike status per stock)
# ------------------------------------------------------------
class StockPreference(Base):
    __tablename__ = "stock_preferences"

    ticker = Column(String(20), ForeignKey("tickers.ticker"), primary_key=True)
    
    # Preference status: 'favorite', 'dislike', or None (neutral)
    preference = Column(String(20))
    
    created_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")))
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 17. SharesFloat (from /stable/shares-float)
# Source: https://financialmodelingprep.com/stable/shares-float
# ------------------------------------------------------------
class SharesFloat(Base):
    __tablename__ = "shares_float"

    ticker = Column(String(20), ForeignKey("tickers.ticker"), primary_key=True)
    
    # Float data
    float_shares = Column(BigInteger)         # Number of shares available for trading
    outstanding_shares = Column(BigInteger)   # Total shares outstanding
    free_float = Column(Float)                # Percentage of shares available for trading
    
    # Data timestamp from API
    date = Column(Date)
    
    created_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")))
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))

    ticker_rel = relationship("Ticker")


# ------------------------------------------------------------
# 18. AbiNotes (user-entered free-form notes by date)
# ------------------------------------------------------------
class AbiNotes(Base):
    __tablename__ = "abi_notes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Date the note is associated with (not necessarily when created)
    note_date = Column(Date, nullable=False)
    
    # Optional title/summary for the note
    title = Column(String(255))
    
    # Long-form notes content (markdown supported)
    content = Column(Text)
    
    # Optional tags for categorization (comma-separated)
    tags = Column(String(500))
    
    created_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")))
    updated_at = Column(DateTime, default=lambda: datetime.now(pytz.timezone("US/Eastern")),
                       onupdate=lambda: datetime.now(pytz.timezone("US/Eastern")))
