#!/bin/bash

# Create cron job file with environment variables
cat > /etc/cron.d/price-update << EOF
# Run daily_price_update.py every 2 minutes on weekdays from 9:00 AM to 3:59 PM EST
DATABASE_URL=postgresql://postgres:postgres@db:5432/stocks_db
POLYGON_API_KEY=${POLYGON_API_KEY}
TZ=America/New_York
*/30 9-15 * * 1-5 cd /app && /usr/local/bin/python daily_price_update.py >> /var/log/cron.log 2>&1
0 16 * * 1-5 cd /app && /usr/local/bin/python daily_price_update.py >> /var/log/cron.log 2>&1
EOF

# Set proper permissions for cron job
chmod 0644 /etc/cron.d/price-update

# Apply cron job
crontab /etc/cron.d/price-update

# Start cron service
service cron start

# Start the Flask application
python app.py
