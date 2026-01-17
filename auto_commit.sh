#!/bin/bash
cd /home/ubuntu/stocks-assist
git add user_data/stock_notes.json user_data/stock_preferences.json user_data/abi_notes.json
git commit -m "Auto-update stock notes, preferences, and abi notes - $(date)" --allow-empty
git push origin main
