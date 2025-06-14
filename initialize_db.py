from sqlalchemy import create_engine, text
import os
import sys
from dotenv import load_dotenv

load_dotenv()

def initialize_database(reset=False):
    """
    Initialize the database with proper table structure.
    
    Args:
        reset (bool): If True, drop existing tables and recreate them.
                     If False, create tables only if they don't exist.
    """
    # Get database URL from environment variable
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    # Create engine
    engine = create_engine(database_url)

    # Create tables
    with engine.connect() as connection:
        if reset:
            print("Resetting database - dropping existing tables...")
            # Drop all tables if they exist (including both old and new table names)
            connection.execute(text("DROP TABLE IF EXISTS shortlist CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS comment CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS stocks CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS ticker CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS price CASCADE;"))
            print("Old tables dropped.")
        
        # Create ticker table (renamed from stocks, only company info)
        table_creation_clause = "CREATE TABLE" if reset else "CREATE TABLE IF NOT EXISTS"
        connection.execute(text(f"""
            {table_creation_clause} ticker (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) UNIQUE NOT NULL,
                company_name VARCHAR(255),
                sector VARCHAR(100),
                subsector VARCHAR(100),
                industry VARCHAR(100),
                market_cap FLOAT,
                shares_outstanding INTEGER,
                eps FLOAT,
                pe_ratio FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Create price table
        connection.execute(text(f"""
            {table_creation_clause} price (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume FLOAT,
                last_traded_timestamp TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT _ticker_date_uc UNIQUE (ticker, date)
            )
        """))
        
        # Create comment table
        connection.execute(text(f"""
            {table_creation_clause} comment (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                comment_text TEXT NOT NULL,
                comment_type VARCHAR(20) NOT NULL DEFAULT 'user',
                status VARCHAR(20) NOT NULL DEFAULT 'active',
                ai_source VARCHAR(50),
                reviewed_by VARCHAR(100),
                reviewed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES ticker(ticker) ON DELETE CASCADE
            )
        """))
        
        # Create shortlist table
        connection.execute(text(f"""
            {table_creation_clause} shortlist (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                shortlisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES ticker(ticker) ON DELETE CASCADE,
                CONSTRAINT _ticker_shortlist_uc UNIQUE (ticker)
            )
        """))
        
        # Create indexes for better query performance
        index_creation_clause = "CREATE INDEX" if reset else "CREATE INDEX IF NOT EXISTS"
        connection.execute(text(f"""
            {index_creation_clause} idx_ticker_ticker ON ticker(ticker);
            {index_creation_clause} idx_ticker_sector ON ticker(sector);
            {index_creation_clause} idx_ticker_industry ON ticker(industry);
            {index_creation_clause} idx_ticker_market_cap ON ticker(market_cap);
            {index_creation_clause} idx_price_ticker ON price(ticker);
            {index_creation_clause} idx_price_date ON price(date);
            {index_creation_clause} idx_price_last_traded_timestamp ON price(last_traded_timestamp);
            {index_creation_clause} idx_comment_ticker ON comment(ticker);
            {index_creation_clause} idx_comment_created_at ON comment(created_at);
            {index_creation_clause} idx_comment_type ON comment(comment_type);
            {index_creation_clause} idx_comment_status ON comment(status);
            {index_creation_clause} idx_shortlist_ticker ON shortlist(ticker);
            {index_creation_clause} idx_shortlist_shortlisted_at ON shortlist(shortlisted_at);
        """))
        
        connection.commit()
        
        if reset:
            print("Database reset completed successfully!")
            print("- All existing tables dropped")
            print("- New 'ticker' table created (company info only)")
            print("- Price table recreated (daily price data)")
            print("- Comment table created (user/AI comments with review workflow)")
            print("- Shortlist table created (Abi's stock shortlist)")
        else:
            print("Database initialization completed successfully!")
            print("- 'ticker' table ready (company info only)")
            print("- 'price' table ready (daily price data)")
            print("- 'comment' table ready (user/AI comments with review workflow)")
            print("- 'shortlist' table ready (Abi's stock shortlist)")

if __name__ == "__main__":
    # Check for reset flag
    reset_mode = "--reset" in sys.argv
    
    if reset_mode:
        confirm = input("Are you sure you want to reset the database? This will drop all existing data. (y/N): ")
        if confirm.lower() != 'y':
            print("Database reset cancelled.")
            sys.exit(0)
    
    initialize_database(reset=reset_mode) 