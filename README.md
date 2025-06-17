# Stocks Assist
Note -
-- OTC stocks are excluded

Things to do
## Done--  Other screens gaps and vol multipliers (above and below 200M)
## Done-- A Daily Refresh Process
## Done -- Call LLM Search to see the latest outlook - websearch (Need to research pricing and quality of best available LLM with web search tool)
## Done -- explore a single page that encapsulates all scans
## Done-- fix industry sector
## Done-- switch to fmp so float is available
## Done -- Speed up the initialize DB process
-- Refactor
-- Document for portability
-- Deploy
-- IPO screen
-- QA logic of the screens
## Done --Make the citations clickable on AI comments
## Done -- abi shortlist
## Done -- update daily_price_update to account new db structure if needed
## Done--add stock_list.csv to for daily cadence only if it contains data
## Done -- initial sedding db  needs to upsert ticker table unless --reset flag is supplied which will drop and recreate tables
## Done-- Blacklist page
-- Inconsistent NavBar


First run

1.docker-compose build --no-cache
2. docker-compose up
3.docker-compose exec web bash
4. python build_stock_list.py
4.python initialize_db.py --reset
5. python initial_seeding_db.py.py 
6. python daily_price_update.py

docker-compose down -v && \
docker-compose build --no-cache && \
docker-compose up -d && \
docker-compose exec web bash -c "\
  python initialize_db.py --reset && \
  python initial_seeding_db.py && \
  python daily_price_update.py"

investopedia.com
Need to fix fetch AI comments prompt

Should we add volume filter to returns page to screen out noise
