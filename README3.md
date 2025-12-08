How does this app work

1. We have a backend which copies external sb_scripts folder into container, intializes a db along with schema and then db_scripts that run on this long living container that also populate the db
2. Scripts
    - db_scripts/build_source_lists
        - build_earnings_dates.py - not in use(was built to visualize if stock moves were earnings related)
        - build_index_lists_fmp.py - not sure if in use. But is needed (check if this can still be fetched from fmp reliably)
        - build_stock_list_fmp.py - fmp is disallowing this on the free tier but is extremely crucial. using an old static list for now
    - db_scripts/initialize_data
        - initialize_db.py - THIS is the one we need to inventory properly so its easier to switch data providers
        - seed_1_year_price_table_from_polygon.py - see which fields are being fetched
        - seed_comments_flags_from_backup.py - not sure if still in use but doesnt use data api , maybe just perplexity
        - seed_index_prices_polygon.py - see which fields are being fetched
        - seed_earnings_from_scraped_earnings_backup.py  - not sure if still in use but doesnt use data api
        - seed_stock_table_from_masterstocklistcsv.py - BROKEN, needs new data provider, review fields
        