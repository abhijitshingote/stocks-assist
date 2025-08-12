from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Date,
    DateTime,
    Text,
    Boolean,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base
from datetime import datetime
import pytz


Base = declarative_base()


def get_eastern_datetime():
    eastern = pytz.timezone("US/Eastern")
    return datetime.now(eastern)


class Stock(Base):
    __tablename__ = "ticker"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, unique=True, nullable=False)
    price = Column(Float)
    beta = Column(Float)
    vol_avg = Column(Integer)
    market_cap = Column(Float)
    last_div = Column(Float)
    range = Column(String)
    changes = Column(Float)
    company_name = Column(String)
    currency = Column(String)
    cik = Column(String)
    isin = Column(String)
    cusip = Column(String)
    exchange = Column(String)
    exchange_short_name = Column(String)
    industry = Column(String)
    website = Column(String)
    description = Column(Text)
    ceo = Column(String)
    sector = Column(String)
    country = Column(String)
    full_time_employees = Column(Integer)
    phone = Column(String)
    address = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    dcf_diff = Column(Float)
    dcf = Column(Float)
    image = Column(String)
    ipo_date = Column(Date)
    default_image = Column(Boolean)
    is_etf = Column(Boolean)
    is_actively_trading = Column(Boolean)
    is_adr = Column(Boolean)
    is_fund = Column(Boolean)
    created_at = Column(DateTime, default=get_eastern_datetime)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)


class Price(Base):
    __tablename__ = "price"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    volume = Column(Float)
    last_traded_timestamp = Column(DateTime)
    created_at = Column(DateTime, default=get_eastern_datetime)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    __table_args__ = (UniqueConstraint("ticker", "date", name="_ticker_date_uc"),)


class Comment(Base):
    __tablename__ = "comment"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey("ticker.ticker"), nullable=False)
    comment_text = Column(Text, nullable=False)
    comment_type = Column(String, nullable=False, default="user")
    status = Column(String, nullable=False, default="active")
    ai_source = Column(String)
    reviewed_by = Column(String)
    reviewed_at = Column(DateTime)
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)


class ShortList(Base):
    __tablename__ = "shortlist"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey("ticker.ticker"), nullable=False)
    shortlisted_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    notes = Column(Text)
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    __table_args__ = (UniqueConstraint("ticker", name="_ticker_shortlist_uc"),)


class BlackList(Base):
    __tablename__ = "blacklist"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey("ticker.ticker"), nullable=False)
    blacklisted_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    notes = Column(Text)
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    __table_args__ = (UniqueConstraint("ticker", name="_ticker_blacklist_uc"),)


class Reviewed(Base):
    __tablename__ = "reviewed"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey("ticker.ticker"), nullable=False)
    reviewed_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    __table_args__ = (UniqueConstraint("ticker", name="_ticker_reviewed_uc"),)


class Flags(Base):
    __tablename__ = "flags"

    ticker = Column(String, primary_key=True)
    is_reviewed = Column(Boolean, default=False, nullable=False)
    is_shortlisted = Column(Boolean, default=False, nullable=False)
    is_blacklisted = Column(Boolean, default=False, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime, nullable=False)


class ConciseNote(Base):
    __tablename__ = "concise_notes"

    ticker = Column(String, primary_key=True)
    note = Column(String, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime, nullable=False)


class Earnings(Base):
    __tablename__ = "earnings"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, ForeignKey("ticker.ticker"), nullable=False)
    earnings_date = Column(Date, nullable=False)
    announcement_type = Column(String, nullable=False, default="earnings")
    estimated_eps = Column(Float)
    actual_eps = Column(Float)
    revenue_estimate = Column(Float)
    actual_revenue = Column(Float)
    is_confirmed = Column(Boolean, default=False, nullable=False)
    quarter = Column(String)
    fiscal_year = Column(Integer)
    announcement_time = Column(String)
    source = Column(String)
    notes = Column(Text)
    created_at = Column(DateTime, default=get_eastern_datetime, nullable=False)
    updated_at = Column(DateTime, default=get_eastern_datetime, onupdate=get_eastern_datetime)
    __table_args__ = (
        UniqueConstraint("ticker", "earnings_date", "announcement_type", name="_ticker_date_type_uc"),
    )

