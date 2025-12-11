import os
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, Boolean, create_engine, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import pytz

Base = declarative_base()

def get_eastern_datetime():
    """Get current datetime in Eastern Time (handles EST/EDT automatically)"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)

class Stock(Base):
    __tablename__ = 'ticker'

    id = Column(Integer, primary_key=True)
    ticker = Column(String, unique=True, nullable=False)  # Primary identifier
    price = Column(Float)
    beta = Column(Float)
    vol_avg = Column(Integer)  # volAvg
    market_cap = Column(Float)  # mktCap
    last_div = Column(Float)  # lastDiv
    range = Column(String)
    changes = Column(Float)
    company_name = Column(String)  # companyName
    currency = Column(String)
    cik = Column(String)
    isin = Column(String)
    cusip = Column(String)
    exchange = Column(String)
    exchange_short_name = Column(String)  # exchangeShortName
    industry = Column(String)
    website = Column(String)
    description = Column(Text)
    ceo = Column(String)
    sector = Column(String)
    country = Column(String)
    full_time_employees = Column(Integer)  # fullTimeEmployees
    phone = Column(String)
    address = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    dcf_diff = Column(Float)  # dcfDiff
    dcf = Column(Float)
    image = Column(String)
    ipo_date = Column(Date)  # ipoDate
    default_image = Column(Boolean)  # defaultImage
    is_etf = Column(Boolean)  # isEtf
    is_actively_trading = Column(Boolean)  # isActivelyTrading
    is_adr = Column(Boolean)  # isAdr
    is_fund = Column(Boolean)  # isFund
    created_at = Column(DateTime, default=get_eastern_datetime)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)

    def __repr__(self):
        return f"<Stock(ticker='{self.ticker}', company_name='{self.company_name}', sector='{self.sector}', industry='{self.industry}')>"

class Price(Base):
    __tablename__ = 'price'
    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    volume = Column(Float)
    last_traded_timestamp = Column(DateTime)  # When the last trade occurred for this price
    created_at = Column(DateTime, default=get_eastern_datetime)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    
    # Add unique constraint for ticker + date combination to enable upserts
    __table_args__ = (UniqueConstraint('ticker', 'date', name='_ticker_date_uc'),)

class Comment(Base):
    __tablename__ = 'comment'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey('ticker.ticker'), nullable=False)
    comment_text = Column(Text, nullable=False)
    comment_type = Column(String, nullable=False, default='user')  # 'user' or 'ai'
    status = Column(String, nullable=False, default='active')  # 'active', 'pending', 'approved', 'rejected'
    ai_source = Column(String)  # which AI model/service generated this (for AI comments)
    reviewed_by = Column(String)  # who reviewed this AI comment (optional)
    reviewed_at = Column(DateTime)  # when was this reviewed
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    
    def __repr__(self):
        return f"<Comment(ticker='{self.ticker}', type='{self.comment_type}', status='{self.status}', created_at='{self.created_at}')>"

class ShortList(Base):
    __tablename__ = 'shortlist'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey('ticker.ticker'), nullable=False)
    shortlisted_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    notes = Column(Text)  # Optional notes about why this stock was shortlisted
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    
    # Ensure each ticker can only be shortlisted once
    __table_args__ = (UniqueConstraint('ticker', name='_ticker_shortlist_uc'),)
    
    def __repr__(self):
        return f"<ShortList(ticker='{self.ticker}', shortlisted_at='{self.shortlisted_at}')>"

class BlackList(Base):
    __tablename__ = 'blacklist'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey('ticker.ticker'), nullable=False)
    blacklisted_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    notes = Column(Text)  # Optional notes about why this stock was blacklisted
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    
    # Ensure each ticker can only be blacklisted once
    __table_args__ = (UniqueConstraint('ticker', name='_ticker_blacklist_uc'),)
    
    def __repr__(self):
        return f"<BlackList(ticker='{self.ticker}', blacklisted_at='{self.blacklisted_at}')>"

class Reviewed(Base):
    __tablename__ = 'reviewed'

    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey('ticker.ticker'), nullable=False)
    reviewed_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)

    __table_args__ = (UniqueConstraint('ticker', name='_ticker_reviewed_uc'),)

    def __repr__(self):
        return f"<Reviewed(ticker='{self.ticker}', reviewed_at='{self.reviewed_at}')>"

class Flags(Base):
    __tablename__ = 'flags'
    ticker = Column(String, primary_key=True)
    is_reviewed = Column(Boolean, default=False, nullable=False)
    is_shortlisted = Column(Boolean, default=False, nullable=False)
    is_blacklisted = Column(Boolean, default=False, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime, nullable=False)

    def __repr__(self):
        return f"<Flags(ticker='{self.ticker}', reviewed={self.is_reviewed}, shortlisted={self.is_shortlisted}, blacklisted={self.is_blacklisted})>"

class ConciseNote(Base):
    __tablename__ = 'concise_notes'
    ticker = Column(String, primary_key=True)
    note = Column(String, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime, nullable=False)

    def __repr__(self):
        return f"<ConciseNote(ticker='{self.ticker}', note='{self.note[:20]}...')>"

class Earnings(Base):
    __tablename__ = 'earnings'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey('ticker.ticker'), nullable=False)
    earnings_date = Column(Date, nullable=False)
    announcement_type = Column(String, nullable=False, default='earnings')  # 'earnings', 'pre-announcement', 'guidance'  
    estimated_eps = Column(Float)  # Analyst consensus estimate
    actual_eps = Column(Float)     # Actual reported EPS (null until announced)
    revenue_estimate = Column(Float)  # Revenue estimate in millions
    actual_revenue = Column(Float)    # Actual revenue in millions (null until announced)
    is_confirmed = Column(Boolean, default=False, nullable=False)  # Whether date is confirmed vs estimated
    quarter = Column(String)  # e.g., 'Q1 2024', 'Q4 2023'
    fiscal_year = Column(Integer)  # Fiscal year
    announcement_time = Column(String)  # 'BMO' (before market open), 'AMC' (after market close), 'DMT' (during market hours)
    source = Column(String)  # Source of the earnings date info (e.g., 'Perplexity', 'SEC', 'Company PR')
    notes = Column(Text)  # Additional notes about the earnings
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    
    # Ensure uniqueness of ticker + earnings_date + announcement_type
    __table_args__ = (UniqueConstraint('ticker', 'earnings_date', 'announcement_type', name='_ticker_date_type_uc'),)
    
    def __repr__(self):
        return f"<Earnings(ticker='{self.ticker}', date='{self.earnings_date}', type='{self.announcement_type}', quarter='{self.quarter}')>"

class StockRSI(Base):
    __tablename__ = 'stock_rsi'

    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey('ticker.ticker'), nullable=False, unique=True)
    rsi_spx_1week = Column(Float)
    rsi_spx_1month = Column(Float)
    rsi_spx_3month = Column(Float)
    rsi_qqq_1week = Column(Float)
    rsi_qqq_1month = Column(Float)
    rsi_qqq_3month = Column(Float)
    rsi_spx_1week_percentile = Column(Float)
    rsi_spx_1month_percentile = Column(Float)
    rsi_spx_3month_percentile = Column(Float)
    rsi_qqq_1week_percentile = Column(Float)
    rsi_qqq_1month_percentile = Column(Float)
    rsi_qqq_3month_percentile = Column(Float)
    rsi_spx_correction = Column(Float)
    rsi_spx_correction_percentile = Column(Float)
    last_updated = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)

    def __repr__(self):
        return f"<StockRSI(ticker='{self.ticker}', spx_1w={self.rsi_spx_1week}, qqq_1w={self.rsi_qqq_1week})>"

class Index(Base):
    __tablename__ = 'index'

    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)  # e.g., 'SPX', 'QQQ', 'IWM'
    yahoo_symbol = Column(String, nullable=False)  # Yahoo Finance symbol (e.g., '^GSPC' for SPX)
    name = Column(String, nullable=False)  # Full name (e.g., 'S&P 500')
    description = Column(Text)  # Description of the index/ETF
    index_type = Column(String)  # 'index' or 'etf'
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)

    def __repr__(self):
        return f"<Index(symbol='{self.symbol}', name='{self.name}', type='{self.index_type}')>"

class IndexPrice(Base):
    __tablename__ = 'index_price'

    id = Column(Integer, primary_key=True)
    symbol = Column(String, ForeignKey('index.symbol'), nullable=False)  # References Index.symbol
    date = Column(Date, nullable=False)
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    volume = Column(Float)
    created_at = Column(DateTime, default=get_eastern_datetime)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    
    # Add unique constraint for symbol + date combination to enable upserts
    __table_args__ = (UniqueConstraint('symbol', 'date', name='_index_symbol_date_uc'),)

class TriggerEvent(Base):
    __tablename__ = 'trigger_event'

    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey('ticker.ticker'), nullable=False)
    trigger_name = Column(String, nullable=False)  # Name of the trigger that fired
    trigger_date = Column(Date, nullable=False)  # Date when trigger was run
    trigger_value = Column(Float)  # Optional value that caused trigger (e.g., return %, volume change)
    trigger_metadata = Column(Text)  # JSON string with additional trigger-specific data
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    
    # Index for fast querying by date
    __table_args__ = (
        UniqueConstraint('ticker', 'trigger_name', 'trigger_date', name='_trigger_event_uc'),
    )
    
    def __repr__(self):
        return f"<TriggerEvent(ticker='{self.ticker}', trigger='{self.trigger_name}', date='{self.trigger_date}')>"

def init_db():
    database_url = os.getenv('DATABASE_URL', 'postgresql://localhost:5432/stocks_db')
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session() 