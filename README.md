# Stocks Assist


A stock market data visualization and analysis application that fetches and stores stock data from Polygon.io.

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the root directory with the following variables:
```
POLYGON_API_KEY=your_api_key_here
DATABASE_URL=postgresql://username:password@localhost:5432/stocks_db
```

4. Set up PostgreSQL database:
```bash
createdb stocks_db
```

5. Run the data fetcher:
```bash
python data_fetcher.py
```

6. Run the Flask application:
```bash
python app.py
```

## Components

1. `data_fetcher.py`: Fetches stock data from Polygon.io and stores it in PostgreSQL
2. `app.py`: Flask application for visualizing stock data
3. `models.py`: Database models
4. `config.py`: Configuration settings

## Data Collection

The data fetcher runs daily and collects the following information for each stock:
- Ticker
- Company name
- Sector
- Subsector
- Industry
- Open, High, Low, Close prices
- Volume
- Market Cap
- Shares Outstanding
- EPS (if available)
- P/E Ratio (if available) 


Things to do
## Done--  Other screens gaps and vol multipliers (above and below 200M)
## Done-- A Daily Refresh Process
## Done -- Call LLM Search to see the latest outlook - websearch (Need to research pricing and quality of best available LLM with web search tool)
## Done -- explore a single page that encapsulates all scans
-- fix industry sector
-- switch to fmp so float is available
## Done -- Speed up the initialize DB process
-- Refactor
-- Document for portability
-- Deploy
-- IPO screen
-- QA logic of the screens
## Done --Make the citations clickable on AI comments
## Done -- abi shortlist
## Done -- update daily_price_update to account new db structure if needed
--add stock_list.csv to for daily cadence only if it contains data
## Done -- initial sedding db  needs to upsert ticker table unless --reset flag is supplied which will drop and recreate tables


First run

1.docker-compose build --no-cache
2. docker-compose up
3.docker-compose execweb bash
4. python build_stock_list.py
4.python initialize_db.py --reset
5. python initial_seeding_db.py.py 