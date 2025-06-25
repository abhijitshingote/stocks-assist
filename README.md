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
## Done-- QA logic of the screens
## Done --Make the citations clickable on AI comments
## Done -- abi shortlist
## Done -- update daily_price_update to account new db structure if needed
## Done--add stock_list.csv to for daily cadence only if it contains data
## Done -- initial sedding db  needs to upsert ticker table unless --reset flag is supplied which will drop and recreate tables
## Done-- Blacklist page
## Done-- Inconsistent NavBar
## Done-- Need a way to persist shortlist, blacklist data during db drops
## Done-- Chat button on navbar
-- Need to fix fetch AI comments prompt -investopedia.com--
-- Should we add volume filter to returns page to screen out noise
-- fix sortable
-- Add SPX and NASDAQ performance on every page


First run

docker-compose build --no-cache 

docker-compose down -v && docker-compose up -d && docker-compose exec web bash 
python initialize_db.py --reset && python initial_seeding_db.py && python daily_price_update.py


