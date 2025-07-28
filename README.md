# Stocks Assist

**Note:**  
- OTC stocks are excluded

---

## Things to Do

### Done
- Other screens: gaps and volume multipliers (above and below 200M)  
- A daily refresh process  
- Call LLM search to see the latest outlook – websearch  
  *(Need to research pricing and quality of best available LLM with web search tool)*  
- Explore a single page that encapsulates all scans  
- Fix industry sector  
- Switch to FMP so float is available  
- Speed up the initialize DB process  
- QA logic of the screens  
- Make the citations clickable on AI comments  
- Abi shortlist  
- Update `daily_price_update` to account for new DB structure if needed  
- Add `stock_list.csv` for daily cadence only if it contains data  
- Initial seeding DB: needs to upsert ticker table unless `--reset` flag is supplied  
- Blacklist page  
- Inconsistent NavBar  
- Need a way to persist shortlist and blacklist data during DB drops  
- Chat button on navbar  

### Pending
- Refactor  
- Document for portability  
- Deploy  
- IPO screen  
- Fix fetch AI comments prompt – `investopedia.com`  
- Consider adding volume filter to returns page to screen out noise  
- Fix sortable  
- Add SPX and NASDAQ performance on every page  

---

## First Run

```bash
docker-compose build --no-cache

docker-compose down -v && docker-compose up -d && docker-compose exec web bash

python initialize_db.py --reset
python initial_seeding_db.py
python daily_price_update.py
```

---

## After Initial Seeding

```bash
crontab -l | sed '2 s/^#//' | crontab -
```
