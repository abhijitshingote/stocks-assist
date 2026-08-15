# TODO
# imp readiness should have a higher weight for strength
1. Top Returns (W) — week grouper - containing top returns in that week (note that is distinct from current Top Returns combo of daily/weekly/monthly)
2. Change existing Top Returns to daily grouper only (same pattern as Vol Spike / Gapper). ie. top returns for that day only
3. Main View (W) — combo of Vol Spike (W) + Top Returns (W) so the same stock isn’t reviewed twice
4. Sweep frontend logic; push what can to backend
5. Mobile layout needs to use a central place to inherit from kind of like we did for the desktop view in frontend/COMPONENTS.md
6. ~~Add next earnings date on chart~~
7. ~~Stock detail page shouldnt have to scroll to see chart. replicate layout like screener pages~~

I think we need to pivot our thinking to a daily review and weekly review
by the way when i reference today or daily, it really means the latest trading day for which we have data
Before implementing anything , ask the agent to layout an approach and agree on it

~~Daily Review
1. Vol spike and gappers that happened on the latest available day
2. dr_1 - mega - top 8, large - top 10, mid - top 10 , small - top 5
Combine into 1 list and invisible rank by adjusted dr_1 (dr_1 should be a reflection of how strong the move is for that category - but dont do this everyday just do some analysis to figure this out and come up with some constants)
Show vol spike and gapper stats like on vol spike and gapper page ,when they are available
give me a toggle for this adjusted dr_1 vs a flat dr_1 for the sort
Page layout should look  like vol spike and gapper page~~

~~Weekly Review
New page: Vol Spike & Gapper (90d)

Universe: all stock_volspike_gapper events with last_event_date in the last 90 days.

Sort: event-day return (dr_1 of that event date), mcap-normalized the same way Daily Review does:

    adjusted = event_return / (clip(mcap, $200M, $100B) / $100B)^-0.134

No date/recency in the sort. Volume ratio is not the sort key — only how large the event-day move was, scaled so a mega-cap +5% ranks above a small-cap +8% of equivalent rarity.

Sort toggle: Adj (above) vs readiness (monthly setupParts, as-is).~~

~~New page: Strong Stocks.

Universe: liquid names with a current TI65 (avg_close_7d / avg_close_65d).

Do not reuse Daily Review’s -0.134. That exponent is p90(positive 1d) vs mcap. TI65 is a 7d/65d MA ratio — same qualitative idea (small caps print larger numbers), different curve.

One-shot analysis, freeze constants (same method as Daily Review, new fit):

Fit p90(TI65 − 1) vs log(mcap) on the liquid universe.
Power law + clip if the data still flatten at mega. Do not assume $200M/$100B/-0.134.
Sort:

adjusted_ti65 = (ti65 − 1) / scale(mcap)
Scale the excess, not the raw ratio. TI65 = 1.0 is flat at every mcap.

Rank by adjusted_ti65 only. No date/recency. A mega-cap TI65 of 1.2 should outrank a small-cap 1.4 of equivalent rarity.

Before implementing: show the fit (p90 vs mcap table + proposed scale(mcap)) and agree on constants.
Sort toggle: Adj ti65 vs raw ti65 vs readiness (monthly setupParts, as-is).
Biotech exclude toggle like Vol Spike 90d.~~



~~top returns - r5d+r20d
a new page with top 30 5d return and top 30 20d return
adjust for market cap like in TODO.md for other newer screeners.
Sort toggle - adj 5d return or 5d return~~


~~fast rs - weekly review
we want a new fast RS page similar to the fast RS section of the current slow/fast RS page. However, on the new page the RS should be calculated adjusted for market cap like we do on the above screeners

please give me a sort toggle - by adjusted RS or readiness like the above screeners~~
