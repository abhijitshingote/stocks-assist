from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from ..config.settings import Settings


connect_args = {}
if Settings.DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

engine = create_engine(Settings.DATABASE_URL, pool_pre_ping=True, connect_args=connect_args)
SessionLocal = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))

