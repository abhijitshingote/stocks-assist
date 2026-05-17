#!/bin/bash
cd /home/ubuntu/stocks-assist
git add user_data/
git commit -m "Auto-backup user_data - $(date)" 
git push --force origin 
