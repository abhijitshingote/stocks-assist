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

def init_db():
    database_url = os.getenv('DATABASE_URL', 'postgresql://localhost:5432/stocks_db')
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session() 