## General - Things to Do
-- the categories should be user settable , market cap industry sector, threshold ,
other filters
- IPO screen  
- dashboard page - like a doctors office, top movers, upcoming calendar, news,upcoming ipo, moving averages, SPX and NASDAQ performance on every page  
--RSI analysis
--- vol % of float or total outstanding

Triggers
- near 50d
- R1D, ,,, R20D - Rank
- Volume
- Gap
- High ATR
- Low Float
- Theme

## FMP things to do
modify seed index prices to work with fmp
migrate carefully screening logic to new app.py and drop all the fluff
what should be run in update_data
dekete tmp?
```bash
docker-compose exec -T db pg_dump -U postgres -Fc stocks_db > stocks_db_backup.dump

cat stocks_db_backup.dump |  docker-compose exec -T db pg_restore -U postgres -d stocks_db --clean --if-exists
```
#seed_profiles has IPO date, market cap
#seed_ratios has pe ratio ttm, 
# we are missing outstanding shares so we dont have to run 1 ticker at a time company profile to get mcap
# punted on income statment since ttm is higher paid tier and quaterly or non ttm is confusing
## First Run
```bash
docker-compose exec  backend bash -c "
  python db_scripts/initialize_data/initialize_db.py --reset && \
  python db_scripts/initialize_data/seed_tickers_from_fmp.py && \
python db_scripts/initialize_data/seed_earnings_from_fmp.py && \
python db_scripts/initialize_data/seed_analyst_estimates_from_fmp.py && \
  python db_scripts/initialize_data/seed_index_prices_fmp.py && \
    python db_scripts/initialize_data/seed_index_constituents_fmp.py && \
  python db_scripts/initialize_data/seed_ohlc_from_fmp.py && \
  python db_scripts/initialize_data/seed_profiles_from_fmp.py && \
 python db_scripts/initialize_data/seed_ratios_from_fmp.py && \ 
 python db_scripts/update_data/stock_metrics_update.py && \
  python db_scripts/update_data/historical_rsi_update.py && \
   python db_scripts/update_data/rsi_indices_update.py 
"

docker-compose exec backend bash -c "
  python db_scripts/initialize_data/initialize_db.py --reset && \
  python db_scripts/initialize_data/seed_tickers_from_fmp.py && \
  python db_scripts/initialize_data/seed_earnings_from_fmp.py && \
  python db_scripts/initialize_data/seed_analyst_estimates_from_fmp.py && \
  python db_scripts/initialize_data/seed_index_prices_fmp.py && \
  python db_scripts/initialize_data/seed_index_constituents_fmp.py && \
  python db_scripts/initialize_data/seed_ohlc_from_fmp.py && \
  python db_scripts/initialize_data/seed_profiles_from_fmp.py && \
  python db_scripts/initialize_data/seed_ratios_from_fmp.py && \
  python db_scripts/update_data/stock_metrics_update.py && \
  python db_scripts/update_data/historical_rsi_update.py && \
  python db_scripts/update_data/rsi_indices_update.py
" && \
docker-compose exec -T db pg_dump -U postgres -Fc stocks_db > stocks_db_backup.dump
```

```bash - update
docker-compose exec backend bash -c "
  python db_scripts/update_data/daily_price_update.py  && \
 python db_scripts/update_data/daily_indices_update.py  && \
 python db_scripts/update_data/stock_metrics_update.py && \
  python db_scripts/update_data/historical_rsi_update.py && \
   python db_scripts/update_data/rsi_indices_update.py
"
```
```bash - test full load
docker-compose exec backend bash -c "
  python db_scripts/initialize_data/initialize_db.py --reset && \
  python db_scripts/initialize_data/seed_tickers_from_fmp.py --limit 10 && \
python db_scripts/initialize_data/seed_earnings_from_fmp.py --limit 10 && \
python db_scripts/initialize_data/seed_analyst_estimates_from_fmp.py --limit 10 && \
  python db_scripts/initialize_data/seed_index_prices_fmp.py && \
    python db_scripts/initialize_data/seed_index_constituents_fmp.py && \
  python db_scripts/initialize_data/seed_ohlc_from_fmp.py  --limit 10 && \
  python db_scripts/initialize_data/seed_profiles_from_fmp.py --limit 10 && \
 python db_scripts/initialize_data/seed_ratios_from_fmp.py --limit 10 && \
 python db_scripts/update_data/stock_metrics_update.py && \
  python db_scripts/update_data/historical_rsi_update.py && \
   python db_scripts/update_data/rsi_indices_update.py
"
```
```bash - test update
docker-compose exec backend bash -c "
  python db_scripts/update_data/daily_price_update.py --progress 10 && \
  python db_scripts/update_data/daily_indices_update.py --days 1 && \
  python db_scripts/update_data/stock_metrics_update.py && \
  python db_scripts/update_data/historical_rsi_update.py && \
  python db_scripts/update_data/rsi_indices_update.py
"
```bash
docker-compose build --no-cache

docker-compose down -v && docker-compose up -d && docker-compose exec backend bash

python db_scripts/initialize_data/initialize_db.py --reset
python db_scripts/initialize_data/seed_tickers_from_fmp.py 
python db_scripts/initialize_data/seed_1_year_price_table_from_polygon.py
python db_scripts/initialize_data/seed_index_prices_polygon.py
python db_scripts/update_data/daily_price_update.py
python db_scripts/update_data/daily_indices_update.py
crontab -l | sed '/^# \*/s/^# //' | crontab -
# python get_earnings_dates.py
python get_earnings_dates.py --missing --limit 100 --debug

docker exec stocks-assist-backend-1 python db_scripts/update_data/run_triggers.py
```




Search the web to identify the most recent and significant fundamental drivers behind  CRDO Credo Technologies 

Focus only on developments that could materially impact the company’s business operations, growth outlook, or competitive positioning. This includes:
	•	Company-specific news: press releases, partnerships, executive changes, earnings, product launches
	•	Sector-wide developments: regulation, macro policy changes, geopolitical drivers
	•	Government/political events: executive actions, defense/military policy shifts, procurement changes, high-level public statements that mention or affect the company or its industry
	•	Viral moments: news segments, interviews (e.g., Fox, CNBC), social media posts (Twitter/X, Reddit, YouTube) that went viral and specifically influenced investor sentiment

Use both traditional media (e.g., Bloomberg, Reuters, CNBC, PR Newswire) and non-traditional sources (e.g., Reddit threads, X/Twitter influencers, government websites, YouTube commentary).

Exclude purely technical trading signals, chart patterns, or sentiment-based moves unless they were directly triggered by a material event.

Summarize the findings in order of significance and explain how each could impact the company. Include citations/links where appropriate.


AI Software
AI DataCenter
AI Power 
AI Semiconductors
Rate Earth Materials
Drones & Defense
AirTaxi and Related
GLP-1
Bitcoin Treasury
SOlana Treasury
Ethereum Related
