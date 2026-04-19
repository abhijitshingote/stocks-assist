#!/bin/bash
cd /home/ubuntu/stocks-assist
git add user_data/
git commit -m "Auto-backup user_data - $(date)" --allow-empty
git push origin main
