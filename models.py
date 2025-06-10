import os
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, create_engine, ForeignKey, UniqueConstraint
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
    ticker = Column(String, unique=True, nullable=False)
    company_name = Column(String)
    sector = Column(String)
    subsector = Column(String)
    industry = Column(String)
    market_cap = Column(Float)
    shares_outstanding = Column(Integer)
    eps = Column(Float)
    pe_ratio = Column(Float)
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

def init_db():
    database_url = os.getenv('DATABASE_URL', 'postgresql://localhost:5432/stocks_db')
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session() 