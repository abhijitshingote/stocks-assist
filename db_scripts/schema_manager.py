"""
Schema Manager for Zero-Downtime Database Updates

This module provides utilities for blue-green deployment of database schemas.
Instead of dropping tables and having downtime during seeding, we:
1. Create a 'staging' schema
2. Seed all data into staging
3. Atomically swap staging -> public (live)
4. Drop the old schema

Usage:
    from schema_manager import SchemaManager
    
    sm = SchemaManager()
    sm.prepare_staging()           # Creates fresh staging schema
    # ... run all seeding scripts with schema='staging' ...
    sm.swap_schemas()              # Atomic swap: staging -> public
    sm.cleanup_old()               # Drop old schema
"""

from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()


class SchemaManager:
    """
    Manages PostgreSQL schemas for zero-downtime deployments.
    
    Schema lifecycle:
    - 'public' (or 'live'): Always serves the app
    - 'staging': Where new data is seeded
    - 'old_data': Temporary holding during swap
    """
    
    LIVE_SCHEMA = 'public'
    STAGING_SCHEMA = 'staging'
    OLD_SCHEMA = 'old_data'
    
    def __init__(self, database_url=None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is not set")
        self.engine = create_engine(self.database_url)
    
    def prepare_staging(self):
        """
        Create a fresh staging schema, dropping if it exists.
        This is where all seeding will happen.
        """
        with self.engine.connect() as conn:
            # Drop staging if it exists (from a failed previous run)
            conn.execute(text(f"DROP SCHEMA IF EXISTS {self.STAGING_SCHEMA} CASCADE"))
            
            # Drop old_data if it exists (from a failed previous swap)
            conn.execute(text(f"DROP SCHEMA IF EXISTS {self.OLD_SCHEMA} CASCADE"))
            
            # Create fresh staging schema
            conn.execute(text(f"CREATE SCHEMA {self.STAGING_SCHEMA}"))
            
            conn.commit()
            print(f"✓ Created fresh '{self.STAGING_SCHEMA}' schema")
    
    def swap_schemas(self, auto_cleanup=True):
        """
        Atomically swap staging schema to become the live public schema.
        
        The swap happens in a single transaction:
        1. Rename 'public' -> 'old_data' 
        2. Rename 'staging' -> 'public'
        3. Drop 'old_data' (if auto_cleanup=True)
        
        If anything fails, the transaction rolls back and public remains unchanged.
        
        Args:
            auto_cleanup (bool): If True (default), automatically drop old schema after swap
        """
        with self.engine.connect() as conn:
            # Check that staging schema exists and has tables
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = '{self.STAGING_SCHEMA}'
            """))
            table_count = result.scalar()
            
            if table_count == 0:
                raise ValueError(f"Staging schema '{self.STAGING_SCHEMA}' has no tables. Aborting swap.")
            
            print(f"✓ Staging schema has {table_count} tables, proceeding with swap...")
            
            # Perform atomic swap
            conn.execute(text(f"""
                -- Rename public to old_data (backup)
                ALTER SCHEMA {self.LIVE_SCHEMA} RENAME TO {self.OLD_SCHEMA};
                
                -- Rename staging to public (make it live)
                ALTER SCHEMA {self.STAGING_SCHEMA} RENAME TO {self.LIVE_SCHEMA};
            """))
            conn.commit()
            
            print(f"✓ Schemas swapped: '{self.STAGING_SCHEMA}' is now live as '{self.LIVE_SCHEMA}'")
            
            # Auto-cleanup old schema for memory efficiency
            if auto_cleanup:
                conn.execute(text(f"DROP SCHEMA IF EXISTS {self.OLD_SCHEMA} CASCADE"))
                conn.commit()
                print(f"✓ Dropped old schema '{self.OLD_SCHEMA}' (auto-cleanup)")
    
    def cleanup_old(self):
        """
        Drop the old schema after successful swap.
        Call this after verifying the app works with new data.
        """
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {self.OLD_SCHEMA} CASCADE"))
            conn.commit()
            print(f"✓ Dropped old schema '{self.OLD_SCHEMA}'")
    
    def rollback_swap(self):
        """
        Emergency rollback: restore old_data as public if swap went wrong.
        Only works if cleanup_old() hasn't been called yet.
        """
        with self.engine.connect() as conn:
            # Check if old_data exists
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM information_schema.schemata 
                WHERE schema_name = '{self.OLD_SCHEMA}'
            """))
            if result.scalar() == 0:
                raise ValueError(f"Cannot rollback: '{self.OLD_SCHEMA}' schema doesn't exist")
            
            conn.execute(text(f"""
                -- Move current public to staging (the bad data)
                ALTER SCHEMA {self.LIVE_SCHEMA} RENAME TO {self.STAGING_SCHEMA};
                
                -- Restore old_data as public
                ALTER SCHEMA {self.OLD_SCHEMA} RENAME TO {self.LIVE_SCHEMA};
            """))
            conn.commit()
            
            print(f"✓ Rolled back: restored '{self.OLD_SCHEMA}' as '{self.LIVE_SCHEMA}'")
    
    def get_schema_info(self):
        """Get information about existing schemas and their table counts."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    n.nspname as schema_name,
                    COUNT(c.relname) as table_count
                FROM pg_namespace n
                LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relkind = 'r'
                WHERE n.nspname IN ('public', 'staging', 'old_data')
                GROUP BY n.nspname
                ORDER BY n.nspname
            """))
            
            schemas = {}
            for row in result:
                schemas[row[0]] = row[1]
            
            return schemas
    
    def set_search_path(self, schema):
        """
        Helper to create a connection with a specific search_path.
        Useful for running seeding scripts against staging schema.
        """
        # Modify the database URL to set search_path
        if '?' in self.database_url:
            return f"{self.database_url}&options=-csearch_path%3D{schema}"
        else:
            return f"{self.database_url}?options=-csearch_path%3D{schema}"

    # Tables sourced from FMP API that benefit from pre-population.
    # Seed scripts have incremental logic, so copying existing data means
    # they only fetch the delta instead of re-downloading everything.
    FMP_DATA_TABLES = [
        'tickers',
        'ohlc',
        'earnings',
        'analyst_estimates',
        'company_profiles',
        'ratios_ttm',
        'shares_float',
        'index_prices',
        'indices',
        'index_components',
    ]

    def prepopulate_staging(self):
        """
        Copy FMP-sourced data from public to staging so seed scripts
        only need to fetch incremental updates from the API.
        Must be called AFTER prepare_staging() and initialize_db --schema staging.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = '{self.STAGING_SCHEMA}'
            """))
            if result.scalar() == 0:
                raise ValueError("Staging schema has no tables. Run initialize_db --schema staging first.")

            total_rows = 0
            for table in self.FMP_DATA_TABLES:
                try:
                    public_exists = conn.execute(text(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = '{self.LIVE_SCHEMA}' AND table_name = :tbl
                        )
                    """), {'tbl': table}).scalar()

                    staging_exists = conn.execute(text(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = '{self.STAGING_SCHEMA}' AND table_name = :tbl
                        )
                    """), {'tbl': table}).scalar()

                    if not public_exists or not staging_exists:
                        print(f"  ⏭ {table}: skipped (table missing in one schema)")
                        continue

                    conn.execute(text(
                        f"INSERT INTO {self.STAGING_SCHEMA}.{table} "
                        f"SELECT * FROM {self.LIVE_SCHEMA}.{table}"
                    ))
                    row_count = conn.execute(text(
                        f"SELECT COUNT(*) FROM {self.STAGING_SCHEMA}.{table}"
                    )).scalar()
                    total_rows += row_count
                    print(f"  ✓ {table}: {row_count:,} rows copied")
                except Exception as e:
                    print(f"  ⚠ {table}: failed ({e})")
                    conn.rollback()
                    continue

            conn.commit()
            print(f"✓ Pre-populated staging with {total_rows:,} total rows from public")

            # Reset sequences so auto-generated IDs don't collide with copied data
            reset_count = 0
            result = conn.execute(text(f"""
                SELECT seq.relname AS sequence_name,
                       tab.relname AS table_name,
                       col.attname AS column_name
                FROM pg_class seq
                JOIN pg_namespace ns ON ns.oid = seq.relnamespace
                JOIN pg_depend d ON d.objid = seq.oid
                JOIN pg_class tab ON tab.oid = d.refobjid
                JOIN pg_attribute col ON col.attrelid = tab.oid AND col.attnum = d.refobjsubid
                WHERE ns.nspname = '{self.STAGING_SCHEMA}'
                  AND seq.relkind = 'S'
            """))
            for row in result:
                seq_name, table_name, col_name = row
                conn.execute(text(
                    f"SELECT setval('{self.STAGING_SCHEMA}.{seq_name}', "
                    f"COALESCE((SELECT MAX({col_name}) FROM {self.STAGING_SCHEMA}.{table_name}), 0) + 1, false)"
                ))
                reset_count += 1
            conn.commit()
            if reset_count:
                print(f"✓ Reset {reset_count} sequence(s) in staging")


def get_staging_database_url():
    """
    Get a DATABASE_URL that targets the staging schema.
    Use this in seeding scripts to write to staging instead of public.
    """
    sm = SchemaManager()
    return sm.set_search_path(SchemaManager.STAGING_SCHEMA)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Manage database schemas for zero-downtime updates')
    parser.add_argument('action', choices=['prepare', 'swap', 'cleanup', 'rollback', 'info', 'prepopulate'],
                        help='Action to perform')
    args = parser.parse_args()
    
    sm = SchemaManager()
    
    if args.action == 'prepare':
        sm.prepare_staging()
    elif args.action == 'swap':
        sm.swap_schemas()
    elif args.action == 'cleanup':
        sm.cleanup_old()
    elif args.action == 'rollback':
        sm.rollback_swap()
    elif args.action == 'prepopulate':
        sm.prepopulate_staging()
    elif args.action == 'info':
        info = sm.get_schema_info()
        print("Schema Information:")
        for schema, count in info.items():
            print(f"  {schema}: {count} tables")

