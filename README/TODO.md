# TODO
1. imp readiness should have a higher weight for strength
2. Combined weekly view - /volspike-gapper-90d, /strong-stocks, /top-returns-5-20, /fast-rs with identifier for where it originated from the 4 , just so we dont review the same stock multiple times. But in order to do this , these existing lists are too long and need some pruning - maybe ask AI.
a. vol spike is all events in 90d - no pruning makes sense, its a weekend review so its fine
b. Strong Stocks, adj ti65 already normalizes for market cap, so maybe a flat ti65 > x only. get AI to do some analysis for different cutoffs before deciding
c. top5d/20d - seems fine
d. Fast RS - is adjusted for market cap so just needs a cutoff point. get ai to do analysis for different cutoff point impacts before deciding.
3. Sweep frontend logic; push what can to backend
4. Mobile layout needs to use a central place to inherit from kind of like we did for the desktop view in frontend/COMPONENTS.md
