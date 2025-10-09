#!/bin/bash

# Create log directory
mkdir -p /var/log

# Create a proper crontab file for root user (no environment variables in the crontab file)
cat > /tmp/root-crontab << EOF
# # Run daily_price_update.py every 30 minutes on weekdays from 9:00 AM to 3:59 PM EST
# * 9-20 * * 1-5 cd /app && DATABASE_URL=postgresql://postgres:postgres@db:5432/stocks_db POLYGON_API_KEY=${POLYGON_API_KEY} FMP_API_KEY=${FMP_API_KEY} /usr/local/bin/python db_scripts/update_date/daily_price_update.py >> /var/log/cron.log 2>&1

# # Run daily_indices_update.py every 30 minutes on weekdays from 9:00 AM to 3:59 PM EST
# * 9-20 * * 1-5 cd /app && DATABASE_URL=postgresql://postgres:postgres@db:5432/stocks_db POLYGON_API_KEY=${POLYGON_API_KEY} FMP_API_KEY=${FMP_API_KEY} /usr/local/bin/python db_scripts/update_date/daily_indices_update.py >> /var/log/cron.log 2>&1

# Run build_stock_list_fmp.py at 8:00 PM EST on weekdays
0 20 * * 1-5 cd /app && DATABASE_URL=postgresql://postgres:postgres@db:5432/stocks_db POLYGON_API_KEY=${POLYGON_API_KEY} FMP_API_KEY=${FMP_API_KEY} /usr/local/bin/python build_stock_list_fmp.py >> /var/log/cron.log 2>&1
EOF

# Install the crontab for root user
crontab /tmp/root-crontab

# Start cron service
service cron start

# Verify cron is running
echo "Cron service status:"
service cron status

# Show installed cron jobs
echo "Installed cron jobs:"
crontab -l

# Create initial log file
touch /var/log/cron.log
echo "$(date): Cron jobs installed and service started" >> /var/log/cron.log

# Start the Flask application
python backend/app.py
