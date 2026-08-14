# TODO

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