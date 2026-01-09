## General - Things to Do
-- the categories should be user settable , market cap industry sector, threshold ,
other filters
- IPO screen  
- dashboard page - like a doctors office, top movers, upcoming calendar, news,upcoming ipo, moving averages, SPX and NASDAQ performance on every page  
--RSI too volatile - smooth it out 
--- vol % of float or total outstanding

Triggers
- near 50d
- R1D, ,,, R20D - Rank
- Volume
- Gap
- High ATR
- Low Float
- Theme
- Sales Growth
- IPO Date
- Short Data
- add last and next earnings date and maybe eps to column set

Did R1D + vol spike,or gapper, has a high ATR, float, did it beat and raise, does it have a high RSI or experience a high change in RSI

```bash
docker-compose exec -T db pg_dump -U postgres -Fc stocks_db > stocks_db_backup.dump

cat stocks_db_backup_2025-12-31.dump |  docker-compose exec -T db pg_restore -U postgres -d stocks_db --clean --if-exists
```
#seed_profiles has IPO date, market cap
#seed_ratios has pe ratio ttm, 
# we are missing outstanding shares so we dont have to run 1 ticker at a time company profile to get mcap
# punted on income statment since ttm is higher paid tier and quaterly or non ttm is confusing
## First Run
```bash
docker-compose exec backend bash -c "
  python db_scripts/initialize_data/initialize_db.py --reset && \
  python db_scripts/initialize_data/seed_tickers_from_fmp.py && \
  python db_scripts/initialize_data/seed_earnings_from_fmp.py && \
  python db_scripts/initialize_data/seed_analyst_estimates_from_fmp.py && \
  python db_scripts/initialize_data/seed_index_prices_fmp.py && \
  python db_scripts/initialize_data/seed_index_constituents_fmp.py && \
  python db_scripts/initialize_data/seed_ohlc_from_fmp.py && \
  python db_scripts/initialize_data/seed_profiles_from_fmp.py && \
  python db_scripts/initialize_data/seed_ratios_from_fmp.py && \
  python db_scripts/initialize_data/seed_shares_float_from_fmp.py && \
  python db_scripts/update_data/stock_metrics_update.py && \
  python db_scripts/update_data/historical_rsi_update.py && \
  python db_scripts/update_data/rsi_indices_update.py && \
  python db_scripts/update_data/volspike_gapper_update.py && \
  python db_scripts/update_data/main_view_update.py && \
  python db_scripts/initialize_data/seed_stock_notes.py && \
  python db_scripts/initialize_data/seed_stock_preferences.py
" && \
docker-compose exec -T db pg_dump -U postgres -Fc stocks_db > stocks_db_backup_$(date +%Y-%m-%d).dump
```

```bash - update
docker-compose exec backend bash -c "
  python db_scripts/update_data/daily_price_update.py  && \
 python db_scripts/update_data/daily_indices_update.py  && \
 python db_scripts/update_data/stock_metrics_update.py && \
  python db_scripts/update_data/historical_rsi_update.py && \
   python db_scripts/update_data/rsi_indices_update.py && \
    python db_scripts/update_data/volspike_gapper_update.py && \
  python db_scripts/update_data/main_view_update.py
"
```
```bash - test full load
docker-compose exec backend bash -c "
  python db_scripts/initialize_data/initialize_db.py --reset && \
  python db_scripts/initialize_data/seed_tickers_from_fmp.py --limit 10 && \
python db_scripts/initialize_data/seed_earnings_from_fmp.py --limit 10 && \
python db_scripts/initialize_data/seed_analyst_estimates_from_fmp.py --limit 10 && \
  python db_scripts/initialize_data/seed_index_prices_fmp.py && \
    python db_scripts/initialize_data/seed_index_constituents_fmp.py && \
  python db_scripts/initialize_data/seed_ohlc_from_fmp.py  --limit 10 && \
  python db_scripts/initialize_data/seed_profiles_from_fmp.py --limit 10 && \
 python db_scripts/initialize_data/seed_ratios_from_fmp.py --limit 10 && \
 python db_scripts/update_data/stock_metrics_update.py && \
  python db_scripts/update_data/historical_rsi_update.py && \
   python db_scripts/update_data/rsi_indices_update.py && \
   python db_scripts/update_data/volspike_gapper_update.py
"
```
```bash - test update
docker-compose exec backend bash -c "
  python db_scripts/update_data/daily_price_update.py --progress 10 && \
  python db_scripts/update_data/daily_indices_update.py --days 1 && \
  python db_scripts/update_data/stock_metrics_update.py && \
  python db_scripts/update_data/historical_rsi_update.py && \
  python db_scripts/update_data/rsi_indices_update.py && \
  python db_scripts/update_data/volspike_gapper_update.py
"
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


######### CLOUD DEPLOY #############
ssh -i ~/Downloads/ssh-key-2026-01-07.key ubuntu@150.136.244.255
# install git
sudo apt update && sudo apt install --no-install-recommends git -y
sudo apt install --no-install-recommends nano -y
sudo apt install --no-install-recommends cron -y
sudo apt install --no-install-recommends docker.io -y
sudo systemctl enable --now docker
sudo usermod -aG docker ubuntu
newgrp docker
git clone https://github.com/abhijitshingote/stocks-assist.git
git checkout fmp_env
git pull origin fmp_env
cd stocks-assist
# Copy env file
chmod +x .env
./get_compute_instance_ready.sh
./docker/generate-ssl.sh
sudo chmod -R 777 logs
./manage-env.sh prod start
./manage-env.sh prod init --test=5
# Remember to add 443,80 to inbound ip
# run detached
ssh -i ~/Downloads/ssh-key-2026-01-07.key ubuntu@150.136.244.255 "nohup bash -c 'cd stocks-assist && ./manage-env.sh prod init' >/dev/null 2>&1 &"