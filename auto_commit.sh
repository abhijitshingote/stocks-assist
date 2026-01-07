#!/bin/bash
cd /home/ubuntu/stocks-assist
git add user_data/stock_notes.json user_data/stock_preferences.json
git commit -m "Auto-update stock notes and preferences - $(date)" --allow-empty
git push origin main
