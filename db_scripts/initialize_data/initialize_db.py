from sqlalchemy import create_engine, text
import os
import sys
import time
from dotenv import load_dotenv

# Add db_scripts to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from db_scripts.logger import get_logger, write_summary, flush_logger, format_duration

load_dotenv()

# Script name for logging
SCRIPT_NAME = 'initialize_db'
logger = get_logger(SCRIPT_NAME)

def initialize_database(reset=False):
    """
    Initialize the database with proper table structure based on backend/models.py.
    
    Tables:
    1. tickers - Basic info from Company Screener (bulk updates)
    2. company_profiles - Detailed info from Profile endpoint
    3. ohlc - Daily price history
    4. indices - Index definitions
    5. index_prices - Index/ETF price history
    6. index_components - Index to ticker mapping
    7. ratios_ttm - Financial ratios TTM
    8. analyst_estimates - Analyst estimates
    9. earnings - Earnings data
    10. sync_metadata - ETL tracking
    11. stock_metrics - Pre-computed screener metrics
    12. historical_rsi - Daily RSI time series
    13. rsi_indices - RSI rankings within index universes (SPX, NDX, DJI)
    14. stock_volspike_gapper - Volume spike and gapper detection
    15. main_view - Combined screener view with metrics, volspike/gapper, and tags
    16. stock_notes - User notes for stocks
    17. stock_preferences - User favorite/dislike status for stocks
    
    Args:
        reset (bool): If True, drop existing tables and recreate them.
    """
    start_time = time.time()
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    engine = create_engine(database_url)

    with engine.connect() as connection:
        if reset:
            logger.info("Resetting database - dropping existing tables...")
            # Drop in reverse dependency order
            connection.execute(text("DROP TABLE IF EXISTS main_view CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS stock_notes CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS stock_preferences CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS stock_volspike_gapper CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS rsi_indices CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS historical_rsi CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS stock_metrics CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS sync_metadata CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS earnings CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS analyst_estimates CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS ratios_ttm CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS index_components CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS index_prices CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS indices CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS ohlc CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS company_profiles CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS tickers CASCADE;"))
            logger.info("Old tables dropped.")
        
        table_clause = "CREATE TABLE" if reset else "CREATE TABLE IF NOT EXISTS"
        
        # 1. tickers - from Company Screener (basic info, bulk updates)
        connection.execute(text(f"""
            {table_clause} tickers (
                ticker VARCHAR(20) PRIMARY KEY,
                company_name VARCHAR(255),
                exchange VARCHAR(100),
                exchange_short_name VARCHAR(20),
                sector VARCHAR(100),
                industry VARCHAR(100),
                country VARCHAR(50),
                price FLOAT,
                market_cap BIGINT,
                beta FLOAT,
                volume BIGINT,
                last_annual_dividend FLOAT,
                is_etf BOOLEAN DEFAULT FALSE,
                is_fund BOOLEAN DEFAULT FALSE,
                is_actively_trading BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # 2. company_profiles - from Profile endpoint (detailed info)
        connection.execute(text(f"""
            {table_clause} company_profiles (
                ticker VARCHAR(20) PRIMARY KEY,
                cik VARCHAR(20),
                isin VARCHAR(20),
                cusip VARCHAR(20),
                description TEXT,
                ceo VARCHAR(255),
                website VARCHAR(255),
                phone VARCHAR(50),
                address VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(100),
                zip VARCHAR(20),
                full_time_employees INTEGER,
                ipo_date DATE,
                image VARCHAR(255),
                currency VARCHAR(20),
                vol_avg BIGINT,
                last_div FLOAT,
                price FLOAT,
                range_52_week VARCHAR(50),
                change FLOAT,
                change_percentage FLOAT,
                is_adr BOOLEAN DEFAULT FALSE,
                default_image BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 3. ohlc - Daily price history
        connection.execute(text(f"""
            {table_clause} ohlc (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20),
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                CONSTRAINT uq_ohlc UNIQUE (ticker, date),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 4. indices - Index definitions
        connection.execute(text(f"""
            {table_clause} indices (
                symbol VARCHAR(20) PRIMARY KEY,
                name VARCHAR(255),
                description TEXT,
                index_type VARCHAR(20),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # 4b. index_prices - Daily price history for indices/ETFs
        connection.execute(text(f"""
            {table_clause} index_prices (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                date DATE,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_index_price UNIQUE (symbol, date)
            )
        """))
        
        # 5. index_components - Index to ticker mapping
        connection.execute(text(f"""
            {table_clause} index_components (
                id SERIAL PRIMARY KEY,
                index_symbol VARCHAR(20),
                ticker VARCHAR(20),
                CONSTRAINT uq_index_component UNIQUE (index_symbol, ticker),
                FOREIGN KEY (index_symbol) REFERENCES indices(symbol) ON DELETE CASCADE,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 6. ratios_ttm - from /stable/ratios-ttm
        connection.execute(text(f"""
            {table_clause} ratios_ttm (
                ticker VARCHAR(20) PRIMARY KEY,
                gross_profit_margin FLOAT,
                operating_profit_margin FLOAT,
                net_profit_margin FLOAT,
                pe_ratio FLOAT,
                peg_ratio FLOAT,
                price_to_book FLOAT,
                price_to_sales FLOAT,
                price_to_free_cash_flow FLOAT,
                current_ratio FLOAT,
                quick_ratio FLOAT,
                cash_ratio FLOAT,
                debt_to_equity FLOAT,
                debt_to_assets FLOAT,
                asset_turnover FLOAT,
                inventory_turnover FLOAT,
                receivables_turnover FLOAT,
                return_on_assets FLOAT,
                return_on_equity FLOAT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 7. analyst_estimates - from /stable/analyst-estimates
        connection.execute(text(f"""
            {table_clause} analyst_estimates (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20),
                date DATE,
                revenue_avg BIGINT,
                revenue_low BIGINT,
                revenue_high BIGINT,
                ebitda_avg BIGINT,
                ebit_avg BIGINT,
                net_income_avg BIGINT,
                eps_avg FLOAT,
                eps_low FLOAT,
                eps_high FLOAT,
                num_analysts_revenue INTEGER,
                num_analysts_eps INTEGER,
                CONSTRAINT uq_analyst_est UNIQUE (ticker, date),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 8. earnings - from /stable/earnings
        connection.execute(text(f"""
            {table_clause} earnings (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20),
                date DATE,
                eps_actual FLOAT,
                eps_estimated FLOAT,
                revenue_actual BIGINT,
                revenue_estimated BIGINT,
                CONSTRAINT uq_earnings UNIQUE (ticker, date),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 9. sync_metadata - ETL tracking
        connection.execute(text(f"""
            {table_clause} sync_metadata (
                key VARCHAR(100) PRIMARY KEY,
                last_synced_at TIMESTAMP
            )
        """))
        
        # 10. stock_metrics - Pre-computed stock metrics for screener/dashboard
        connection.execute(text(f"""
            {table_clause} stock_metrics (
                ticker VARCHAR(20) PRIMARY KEY,
                company_name VARCHAR(255),
                country VARCHAR(50),
                sector VARCHAR(100),
                industry VARCHAR(100),
                ipo_date DATE,
                market_cap BIGINT,
                current_price FLOAT,
                range_52_week VARCHAR(50),
                volume BIGINT,
                dollar_volume FLOAT,
                avg_vol_10d FLOAT,
                vol_vs_10d_avg FLOAT,
                dr_1 FLOAT,
                dr_5 FLOAT,
                dr_20 FLOAT,
                dr_60 FLOAT,
                dr_120 FLOAT,
                atr20 FLOAT,
                pe_t_minus_1 FLOAT,
                pe_t FLOAT,
                pe_t_plus_1 FLOAT,
                pe_t_plus_2 FLOAT,
                ps_t_minus_1 FLOAT,
                ps_t FLOAT,
                ps_t_plus_1 FLOAT,
                ps_t_plus_2 FLOAT,
                rev_growth_t_minus_1 FLOAT,
                rev_growth_t FLOAT,
                rev_growth_t_plus_1 FLOAT,
                rev_growth_t_plus_2 FLOAT,
                eps_growth_t_minus_1 FLOAT,
                eps_growth_t FLOAT,
                eps_growth_t_plus_1 FLOAT,
                eps_growth_t_plus_2 FLOAT,
                rsi INTEGER,
                rsi_mktcap INTEGER,
                short_float FLOAT,
                short_ratio FLOAT,
                short_interest FLOAT,
                low_float BOOLEAN,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 11. historical_rsi - Daily RSI time series for all stocks
        connection.execute(text(f"""
            {table_clause} historical_rsi (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20),
                date DATE,
                rsi_global INTEGER,
                rsi_mktcap INTEGER,
                dma_50 FLOAT,
                dma_200 FLOAT,
                CONSTRAINT uq_historical_rsi UNIQUE (ticker, date),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 12. rsi_indices - RSI rankings within index universes (SPX, NDX, DJI)
        connection.execute(text(f"""
            {table_clause} rsi_indices (
                ticker VARCHAR(20) PRIMARY KEY,
                is_spx BOOLEAN DEFAULT FALSE,
                is_ndx BOOLEAN DEFAULT FALSE,
                is_dji BOOLEAN DEFAULT FALSE,
                rsi_spx INTEGER,
                rsi_ndx INTEGER,
                rsi_dji INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 13. stock_volspike_gapper - Volume spike and gapper detection
        connection.execute(text(f"""
            {table_clause} stock_volspike_gapper (
                ticker VARCHAR(20) PRIMARY KEY,
                spike_day_count INTEGER,
                avg_volume_spike FLOAT,
                volume_spike_days TEXT,
                gapper_day_count INTEGER,
                avg_return_gapper FLOAT,
                gap_days TEXT,
                last_event_date DATE,
                last_event_type VARCHAR(20),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))
        
        # 14. main_view - Combined screener view with metrics, volspike/gapper, and tags
        connection.execute(text(f"""
            {table_clause} main_view (
                ticker VARCHAR(20) PRIMARY KEY,
                company_name VARCHAR(255),
                country VARCHAR(50),
                sector VARCHAR(100),
                industry VARCHAR(100),
                ipo_date DATE,
                market_cap BIGINT,
                current_price FLOAT,
                range_52_week VARCHAR(50),
                volume BIGINT,
                dollar_volume FLOAT,
                avg_vol_10d FLOAT,
                vol_vs_10d_avg FLOAT,
                dr_1 FLOAT,
                dr_5 FLOAT,
                dr_20 FLOAT,
                dr_60 FLOAT,
                dr_120 FLOAT,
                atr20 FLOAT,
                pe_t_minus_1 FLOAT,
                pe_t FLOAT,
                pe_t_plus_1 FLOAT,
                pe_t_plus_2 FLOAT,
                ps_t_minus_1 FLOAT,
                ps_t FLOAT,
                ps_t_plus_1 FLOAT,
                ps_t_plus_2 FLOAT,
                rev_growth_t_minus_1 FLOAT,
                rev_growth_t FLOAT,
                rev_growth_t_plus_1 FLOAT,
                rev_growth_t_plus_2 FLOAT,
                eps_growth_t_minus_1 FLOAT,
                eps_growth_t FLOAT,
                eps_growth_t_plus_1 FLOAT,
                eps_growth_t_plus_2 FLOAT,
                rsi INTEGER,
                rsi_mktcap INTEGER,
                short_float FLOAT,
                short_ratio FLOAT,
                short_interest FLOAT,
                low_float BOOLEAN,
                spike_day_count INTEGER,
                avg_volume_spike FLOAT,
                volume_spike_days TEXT,
                gapper_day_count INTEGER,
                avg_return_gapper FLOAT,
                gap_days TEXT,
                last_event_date DATE,
                last_event_type VARCHAR(20),
                tags TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            )
        """))

        # 16. stock_notes - User notes for stocks
        connection.execute(text(f"""
            {table_clause} stock_notes (
                ticker VARCHAR(20) PRIMARY KEY REFERENCES tickers(ticker),
                notes TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))

        # 17. stock_preferences - User favorite/dislike status for stocks
        connection.execute(text(f"""
            {table_clause} stock_preferences (
                ticker VARCHAR(20) PRIMARY KEY REFERENCES tickers(ticker),
                preference VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))

        # Create indexes for better query performance
        idx_clause = "CREATE INDEX" if reset else "CREATE INDEX IF NOT EXISTS"
        connection.execute(text(f"""
            {idx_clause} idx_tickers_exchange ON tickers(exchange_short_name);
            {idx_clause} idx_tickers_sector ON tickers(sector);
            {idx_clause} idx_tickers_industry ON tickers(industry);
            {idx_clause} idx_tickers_country ON tickers(country);
            {idx_clause} idx_tickers_market_cap ON tickers(market_cap);
            {idx_clause} idx_tickers_actively_trading ON tickers(is_actively_trading);
            {idx_clause} idx_ohlc_ticker ON ohlc(ticker);
            {idx_clause} idx_ohlc_date ON ohlc(date);
            {idx_clause} idx_analyst_est_ticker ON analyst_estimates(ticker);
            {idx_clause} idx_analyst_est_date ON analyst_estimates(date);
            {idx_clause} idx_earnings_ticker ON earnings(ticker);
            {idx_clause} idx_earnings_date ON earnings(date);
            {idx_clause} idx_index_prices_symbol ON index_prices(symbol);
            {idx_clause} idx_index_prices_date ON index_prices(date);
            {idx_clause} idx_stock_metrics_sector ON stock_metrics(sector);
            {idx_clause} idx_stock_metrics_industry ON stock_metrics(industry);
            {idx_clause} idx_stock_metrics_market_cap ON stock_metrics(market_cap);
            {idx_clause} idx_stock_metrics_dr_20 ON stock_metrics(dr_20);
            {idx_clause} idx_stock_metrics_rsi ON stock_metrics(rsi);
            {idx_clause} idx_historical_rsi_ticker ON historical_rsi(ticker);
            {idx_clause} idx_historical_rsi_date ON historical_rsi(date);
            {idx_clause} idx_historical_rsi_ticker_date ON historical_rsi(ticker, date);
            {idx_clause} idx_rsi_indices_is_spx ON rsi_indices(is_spx);
            {idx_clause} idx_rsi_indices_is_ndx ON rsi_indices(is_ndx);
            {idx_clause} idx_rsi_indices_is_dji ON rsi_indices(is_dji);
            {idx_clause} idx_volspike_gapper_spike_count ON stock_volspike_gapper(spike_day_count);
            {idx_clause} idx_volspike_gapper_gapper_count ON stock_volspike_gapper(gapper_day_count);
            {idx_clause} idx_volspike_gapper_last_event_date ON stock_volspike_gapper(last_event_date DESC);
            {idx_clause} idx_main_view_market_cap ON main_view(market_cap);
            {idx_clause} idx_main_view_sector ON main_view(sector);
            {idx_clause} idx_main_view_industry ON main_view(industry);
            {idx_clause} idx_main_view_dr_5 ON main_view(dr_5);
            {idx_clause} idx_main_view_dr_20 ON main_view(dr_20);
            {idx_clause} idx_main_view_dr_60 ON main_view(dr_60);
            {idx_clause} idx_main_view_dr_120 ON main_view(dr_120);
        """))
        
        connection.commit()
        
        elapsed = time.time() - start_time
        if reset:
            logger.info("Database reset completed successfully!")
            write_summary(SCRIPT_NAME, 'SUCCESS', 'Database reset completed - all tables recreated', duration_seconds=elapsed)
        else:
            logger.info("Database initialization completed successfully!")
            write_summary(SCRIPT_NAME, 'SUCCESS', 'Database initialized - tables created if not exists', duration_seconds=elapsed)
        
        logger.info("Tables:")
        logger.info("  - tickers (from Company Screener)")
        logger.info("  - company_profiles (from Profile endpoint)")
        logger.info("  - ohlc (price history)")
        logger.info("  - indices (index definitions)")
        logger.info("  - index_prices (index/ETF price history)")
        logger.info("  - index_components (index → ticker mapping)")
        logger.info("  - ratios_ttm (from Ratios TTM endpoint)")
        logger.info("  - analyst_estimates (from Analyst Estimates endpoint)")
        logger.info("  - earnings (from Earnings endpoint)")
        logger.info("  - sync_metadata (ETL tracking)")
        logger.info("  - stock_metrics (pre-computed screener metrics)")
        logger.info("  - historical_rsi (daily RSI time series)")
        logger.info("  - rsi_indices (RSI within index universes: SPX, NDX, DJI)")
        logger.info("  - stock_volspike_gapper (volume spike and gapper detection)")
        logger.info("  - main_view (combined screener view with tags)")
        logger.info("  - stock_notes (user notes for stocks)")
        logger.info("  - stock_preferences (user favorite/dislike status for stocks)")
        
        flush_logger(SCRIPT_NAME)


if __name__ == "__main__":
    reset_mode = "--reset" in sys.argv
    initialize_database(reset=reset_mode)
