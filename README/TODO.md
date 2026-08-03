# High Priority

2. This should fold into db_scripts/initialize for table creation & /update_data for refresh. also rename script appropriately. Should run as part of init-zd as well as update-benzinga market news page
3. Not sure what to do with RSI feature - not being used
4. deepse ek
5. remove garbage or one line articles from benzinga
6. only 100 articles is too little .but can the prompt handle more
7. In a seperate call, also get a read on losers to see if tide is turning


# More Complex
1. ~~Need Mobile first alternate pages , totally seperate from current~~ ✅ Done — separate /m/* mobile site.
2. ~~Desktop web app should use reusable components?~~ ✅ Done — see `frontend/COMPONENTS.md` for the component catalog.
   - Shared CSS: `tokens.css`, `components.css`, `screener.css`, `card-grid.css`
   - Shared shell: `templates/desktop/_screener_shell.html`
   - Shared engine: `static/js/desktop/screener-app.js`
   - RSI pages merged: `templates/rsi_index.html` (replaces 3 near-identical files)
   - 3 breakpoint tiers: Phone ≤640px, Tablet 641–1024px, Desktop ≥1025px
3. Sweep for logic in frontend and see if it can be delegated to backend
4. Potential deprecate  
| Path               | Notes                                        |
| ------------------ | -------------------------------------------- |
| `screening_agent/` | Legacy experiment; not used by production UI |
| `weekly_brief/`    | Empty placeholder                            |
| `tests/`           | pytest                                       |
| `archive/`         | Old compose variants                         |


## POSSIBLE BUG - market brief

So general  be not equal to ticker, right or is that too much noise ?
should hgeneral be last pririty when assembling prompt
