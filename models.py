import os
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

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
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

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
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

def init_db():
    database_url = os.getenv('DATABASE_URL', 'postgresql://localhost:5432/stocks_db')
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session() 