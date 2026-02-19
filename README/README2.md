We should think about switching from Polygon to FMP or even something else. This is why we need an inventory of fields we actually get from the API, Yahoo or other sources. So we need a csv which shows where they are coming from API/Yahoo, script fetching them, db schema lcoation

Yahoo finance is blocking docker scraper, so may need API
market cap is broken (Just scrape shares outstanding so market cap is dynamic)
Need forward PE ( Get expected EPS)

Future Enhacement - Estimate Change(so store expected EPS history to see change)

Sort Order is fucked up on the 5 day , 20 day , etc ... it should be sorted by 5D Return, 20D Return , etc

ideas
triggers 
What is the universe (SPX500, Nasdaq - 100)
(R1W,R1M, R3M, R6M) - Need a function to determine VCP, hugging 10,20EMA
Need a very robust function determine previous highs and lows(get chatgpt to help)
near 50d, near support line, previous high low
gapper
scrape shares outstanding (market cap) , eps estimates
from yahoo

 prompt = f"""Why did {entity} stock move up?
    Check for the most impactful catalysts, including:
	•	Earnings-related: EPS, revenue, and guidance with reported vs. expected numbers, noting whether it was a beat/miss or raise/lower
	•	Analyst actions: rating changes and price-target moves (old → new)
	•	Sector or industry news: regulatory actions, peer earnings, supply-chain updates
	•	Macro drivers: CPI, jobs, Fed commentary, geopolitical headlines
	•	Market behavior: short interest changes, options flow, meme-stock activity

Provide a succinct 3–4 sentence summary focused only on the actual drivers of the move."""