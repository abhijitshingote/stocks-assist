#!/bin/bash

mkdir -p /var/log

# Git setup for auto_commit.sh
git config --global safe.directory /app
git config --global user.email "auto@stocks-assist"
git config --global user.name "Auto Commit"

# Use env vars for HTTPS auth (GIT_PUSH_TOKEN, GITHUB_TOKEN, or GIT_TOKEN from .env)
export GIT_ASKPASS=/app/docker/git_askpass_env.sh
export GIT_TERMINAL_PROMPT=0

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
