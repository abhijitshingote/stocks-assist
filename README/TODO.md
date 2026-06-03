# High Priority
1. Market Brief & Daily Screener should be endpoints to be consistent with architecture
2. This should fold into db_scripts/initialize for table creation & /update_data for refresh. also rename script appropriately. Should run as part of init-zd as well as update
3. Not sure what to do with RSI feature - not being used
4. When Running Market bried - article ids should be saved as an artefact to help with reproduction of brief or experimentation with other LLM providers
5. remove garbage or one line articles from benzinga
6. only 100 articles is too little .but can the prompt handle more


# More Complex
1. Need Mobile first alternate pages , totally seperate from current
2. Desktop web app should use resuable components? some framework that allows us to do this?
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
