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

            connection.execute(text("DROP TABLE IF EXISTS concise_notes CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS flags CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS comment CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS stock_rsi CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS index_price CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS index CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS ticker CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS price CASCADE;"))
            print("Old tables dropped.")
        
        # Create ticker table with comprehensive FMP fields
        table_creation_clause = "CREATE TABLE" if reset else "CREATE TABLE IF NOT EXISTS"
        connection.execute(text(f"""
            {table_creation_clause} ticker (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) UNIQUE NOT NULL,
                price FLOAT,
                beta FLOAT,
                vol_avg INTEGER,
                market_cap FLOAT,
                last_div FLOAT,
                range VARCHAR(50),
                changes FLOAT,
                company_name VARCHAR(255),
                currency VARCHAR(10),
                cik VARCHAR(20),
                isin VARCHAR(20),
                cusip VARCHAR(20),
                exchange VARCHAR(100),
                exchange_short_name VARCHAR(20),
                industry VARCHAR(100),
                website VARCHAR(255),
                description TEXT,
                ceo VARCHAR(100),
                sector VARCHAR(100),
                country VARCHAR(10),
                full_time_employees INTEGER,
                phone VARCHAR(50),
                address VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(10),
                zip VARCHAR(20),
                dcf_diff FLOAT,
                dcf FLOAT,
                image VARCHAR(255),
                ipo_date DATE,
                default_image BOOLEAN,
                is_etf BOOLEAN,
                is_actively_trading BOOLEAN,
                is_adr BOOLEAN,
                is_fund BOOLEAN,
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
        
        # Create flags table (reviewed/shortlisted/blacklisted booleans)
        connection.execute(text(f"""
            {table_creation_clause} flags (
                ticker VARCHAR(10) PRIMARY KEY,
                is_reviewed BOOLEAN NOT NULL DEFAULT FALSE,
                is_shortlisted BOOLEAN NOT NULL DEFAULT FALSE,
                is_blacklisted BOOLEAN NOT NULL DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            )
        """))

        # Create stock_rsi table for relative strength metrics vs SPX/QQQ
        connection.execute(text(f"""
            {table_creation_clause} stock_rsi (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL UNIQUE,
                rsi_spx_1week FLOAT,
                rsi_spx_1month FLOAT,
                rsi_spx_3month FLOAT,
                rsi_qqq_1week FLOAT,
                rsi_qqq_1month FLOAT,
                rsi_qqq_3month FLOAT,
                rsi_spx_1week_percentile FLOAT,
                rsi_spx_1month_percentile FLOAT,
                rsi_spx_3month_percentile FLOAT,
                rsi_qqq_1week_percentile FLOAT,
                rsi_qqq_1month_percentile FLOAT,
                rsi_qqq_3month_percentile FLOAT,
                rsi_spx_correction FLOAT,
                rsi_spx_correction_percentile FLOAT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                FOREIGN KEY (ticker) REFERENCES ticker(ticker) ON DELETE CASCADE
            )
        """))

        # Concise notes table
        connection.execute(text(f"""
            {table_creation_clause} concise_notes (
                ticker VARCHAR(10) PRIMARY KEY,
                note TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            )
        """))

        # Create index table for tracking indices and ETFs
        connection.execute(text(f"""
            {table_creation_clause} index (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) UNIQUE NOT NULL,
                yahoo_symbol VARCHAR(20) NOT NULL,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                index_type VARCHAR(20),
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # Create index_price table for historical index/ETF prices
        connection.execute(text(f"""
            {table_creation_clause} index_price (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT _index_symbol_date_uc UNIQUE (symbol, date),
                FOREIGN KEY (symbol) REFERENCES index(symbol) ON DELETE CASCADE
            )
        """))
        
        # Create indexes for better query performance
        index_creation_clause = "CREATE INDEX" if reset else "CREATE INDEX IF NOT EXISTS"
        connection.execute(text(f"""
            {index_creation_clause} idx_ticker_ticker ON ticker(ticker);
            {index_creation_clause} idx_ticker_sector ON ticker(sector);
            {index_creation_clause} idx_ticker_industry ON ticker(industry);
            {index_creation_clause} idx_ticker_market_cap ON ticker(market_cap);
            {index_creation_clause} idx_ticker_exchange_short_name ON ticker(exchange_short_name);
            {index_creation_clause} idx_ticker_is_actively_trading ON ticker(is_actively_trading);
            {index_creation_clause} idx_ticker_price ON ticker(price);
            {index_creation_clause} idx_price_ticker ON price(ticker);
            {index_creation_clause} idx_price_date ON price(date);
            {index_creation_clause} idx_price_last_traded_timestamp ON price(last_traded_timestamp);
            {index_creation_clause} idx_comment_ticker ON comment(ticker);
            {index_creation_clause} idx_comment_created_at ON comment(created_at);
            {index_creation_clause} idx_comment_type ON comment(comment_type);
            {index_creation_clause} idx_comment_status ON comment(status);
            {index_creation_clause} idx_flags_reviewed ON flags(is_reviewed);
            {index_creation_clause} idx_flags_shortlisted ON flags(is_shortlisted);
            {index_creation_clause} idx_flags_blacklisted ON flags(is_blacklisted);
            {index_creation_clause} idx_notes_ticker ON concise_notes(ticker);
            {index_creation_clause} idx_stock_rsi_ticker ON stock_rsi(ticker);
            {index_creation_clause} idx_index_symbol ON index(symbol);
            {index_creation_clause} idx_index_type ON index(index_type);
            {index_creation_clause} idx_index_price_symbol ON index_price(symbol);
            {index_creation_clause} idx_index_price_date ON index_price(date);
        """))
        
        connection.commit()
        
        if reset:
            print("Database reset completed successfully!")
            print("- All existing tables dropped")
            print("- New 'ticker' table created (comprehensive FMP company data)")
            print("- Price table created")
            print("- Comment table created (user/AI comments with review workflow)")
            print("- Flags table created (reviewed/shortlisted/blacklisted booleans)")
            print("- Concise notes table created")
            print("- Index table created (market indices/ETFs metadata)")
            print("- Index_price table created (historical index/ETF prices)")
        else:
            print("Database initialization completed successfully!")
            print("- 'ticker' table ready (comprehensive FMP company data)")
            print("- 'price' table ready (daily price data)")
            print("- 'comment' table ready (user/AI comments with review workflow)")
            print("- 'flags' table ready (reviewed/shortlisted/blacklisted booleans)")
            print("- 'concise notes' table ready")
            print("- 'index' table ready (market indices/ETFs metadata)")
            print("- 'index_price' table ready (historical index/ETF prices)")

if __name__ == "__main__":
    # Check for reset flag
    reset_mode = "--reset" in sys.argv
    
    initialize_database(reset=reset_mode) 