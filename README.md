# Stocks Assist

**Note:**  
- OTC stocks are excluded

---

## Things to Do
-- the categories should be user settable , market cap industry sector, threshold ,
other filters
-- FMP has deprecated endpoitn, need to fix build_stock_list_fmp.py
- IPO screen  
- dashboard page - like a doctors office, top movers, upcoming calendar, news,upcoming ipo, moving averages, SPX and NASDAQ performance on every page  
--RSI analysis
--- vol % of float or total outstanding

## First Run

```bash
docker-compose build --no-cache

docker-compose down -v && docker-compose up -d && docker-compose exec backend bash

python db_scripts/initialize_data/initialize_db.py --reset
python db_scripts/initialize_data/seed_stock_table_from_masterstocklistcsv.py 
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
