# TODO
1. Adjust market-cap weighting/booster for mega-caps (e.g., Meta/Apple) on market-cap-normalized screener views so they rank higher.
1. Q Accelerating Rev adn Profits
1. Need all stock market news feed to be curated , remove political garbage, ai generated technical clickbaits like if you invested $1000 in XYZ but not just limited to that specific style, and summarize news. .. how to fetch that entire universe from api... first ask agent to pull down maybe enough data to analyzethe different tags represent what kind of news.. once we learn the analysis we apply the curation filters for all-stock-market-news-summary. source from benzinga for now - http://localhost/m/market-news. i think i would like a weekend version(saturday -monday ) and a daily version (last 24-48 hours). first just focus on analysis and tell me what you find
1. imp readiness should have a higher weight for strength & especially revgrowth(t+1 or t+2)
2. Combined weekly view - /volspike-gapper-90d, /strong-stocks, /top-returns-5-20, /fast-rs with identifier for where it originated from the 4 , just so we dont review the same stock multiple times. But in order to do this , these existing lists are too long and need some pruning - maybe ask AI.
a. vol spike is all events in 90d - no pruning makes sense, its a weekend review so its fine
b. Strong Stocks, adj ti65 already normalizes for market cap, so maybe a flat ti65 > x only. get AI to do some analysis for different cutoffs before deciding
c. top5d/20d - seems fine
d. Fast RS - is adjusted for market cap so just needs a cutoff point. get ai to do analysis for different cutoff point impacts before deciding.
3. Sweep frontend logic; push what can to backend
4. Mobile layout inherits from `_screener_shell.html` + `screener-layout.css` (screeners) and `_shell.html` + `pages.css` (utility), same idea as desktop `COMPONENTS.md`.
*new idea* what if we did classifier to only pick importnt articles
what if we buxcketedvthe artickles into 10 classes to help
then what if we identified important significant events but maintained hitory
then what if we visualized that in a tier 1 , 2, etc importance
like tier 1 hangs for longer like an overlay as you can scroll back and forth so you always have this perennial context, antidote for recency bias
and what if we wrapped this in a beautiful ui and sold it *
