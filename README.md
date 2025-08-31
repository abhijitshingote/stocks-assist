# Stocks Assist

**Note:**  
- OTC stocks are excluded

---

## Things to Do


- IPO screen  
- dashboard page - like a doctors office, top movers, upcoming calendar, news,upcoming ipo, moving averages, SPX and NASDAQ performance on every page  

---

## First Run

```bash
docker-compose build --no-cache

docker-compose down -v && docker-compose up -d && docker-compose exec web bash

python initialize_db.py --reset
python initial_seeding_db.py
python daily_price_update.py
crontab -l | sed '2 s/^#//' | crontab -
# python get_earnings_dates.py
python get_earnings_dates.py --missing --limit 100 --debug
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
