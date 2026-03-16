#!/bin/bash

mkdir -p /var/log

# Container cron: run_triggers every 2 hours
cat > /tmp/root-crontab << 'EOF'
0 */2 * * * cd /app && python db_scripts/update_data/run_triggers.py >> /var/log/cron.log 2>&1
EOF

crontab /tmp/root-crontab
service cron start

echo "Installed cron jobs:"
crontab -l

touch /var/log/cron.log
echo "$(date): Cron jobs installed and service started" >> /var/log/cron.log

python backend/app.py
