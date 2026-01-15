#!/bin/bash

# =============================================================================
# Compute Instance Setup Script for stocks-assist
# Run this script on a fresh Ubuntu/WSL2 instance to prepare the environment
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="/home/ubuntu/stocks-assist"
ENV_FILE="$SCRIPT_DIR/.env"

echo "=================================================="
echo "  stocks-assist Compute Instance Setup"
echo "=================================================="
echo ""

# -----------------------------------------------------------------------------
# 1. Install legacy docker-compose binary (self-contained, no Python)
# -----------------------------------------------------------------------------
echo "[1/4] Installing docker-compose binary..."

# Ensure curl is available
if ! command -v curl &> /dev/null; then
    echo "  Installing curl..."
    sudo apt update
    sudo apt install -y curl
fi

if command -v docker-compose &> /dev/null; then
    echo "  ✓ docker-compose already installed: $(docker-compose --version)"
else
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    echo "  ✓ docker-compose installed: $(docker-compose --version)"
fi

# -----------------------------------------------------------------------------
# 2. Configure iptables to allow TCP port 80
# -----------------------------------------------------------------------------
echo ""
echo "[2/4] Configuring iptables for port 80..."

# Check if rule already exists
if sudo iptables -C INPUT -p tcp --dport 80 -j ACCEPT 2>/dev/null; then
    echo "  ✓ iptables rule for port 80 already exists"
else
    # Insert rule to allow TCP port 80 before the final REJECT
    sudo iptables -I INPUT 4 -p tcp --dport 80 -j ACCEPT
    echo "  ✓ iptables rule for port 80 added"
fi

# Install iptables-persistent to save rules across reboots
if dpkg -l | grep -q iptables-persistent; then
    echo "  ✓ iptables-persistent already installed"
else
    echo "  Installing iptables-persistent..."
    sudo apt update
    sudo DEBIAN_FRONTEND=noninteractive apt install -y iptables-persistent
fi

sudo netfilter-persistent save 2>/dev/null || true
echo "  ✓ iptables rules saved"

# -----------------------------------------------------------------------------
# 3. Configure git credentials from .env
# -----------------------------------------------------------------------------
echo ""
echo "[3/4] Configuring git credentials from .env..."

if [ -f "$ENV_FILE" ]; then
    # Source the .env file
    set -a
    source "$ENV_FILE"
    set +a
    
    # Configure git user info
    if [ -n "$GIT_USER_NAME" ]; then
        git config --global user.name "$GIT_USER_NAME"
        echo "  ✓ Git user.name set to: $GIT_USER_NAME"
    else
        echo "  ⚠ GIT_USER_NAME not found in .env"
    fi
    
    if [ -n "$GIT_USER_EMAIL" ]; then
        git config --global user.email "$GIT_USER_EMAIL"
        echo "  ✓ Git user.email set to: $GIT_USER_EMAIL"
    else
        echo "  ⚠ GIT_USER_EMAIL not found in .env"
    fi
    
    # Configure credential helper to store credentials
    git config --global credential.helper store
    echo "  ✓ Git credential helper set to 'store'"
    
    # If GITHUB_TOKEN is provided, set up credentials for github.com
    if [ -n "$GITHUB_TOKEN" ] && [ -n "$GIT_USER_NAME" ]; then
        # Create/update credentials file
        CREDENTIALS_FILE="$HOME/.git-credentials"
        # Remove existing github.com entry if present
        if [ -f "$CREDENTIALS_FILE" ]; then
            grep -v "github.com" "$CREDENTIALS_FILE" > "$CREDENTIALS_FILE.tmp" 2>/dev/null || true
            mv "$CREDENTIALS_FILE.tmp" "$CREDENTIALS_FILE"
        fi
        # Add new github.com credentials
        echo "https://${GIT_USER_NAME}:${GITHUB_TOKEN}@github.com" >> "$CREDENTIALS_FILE"
        chmod 600 "$CREDENTIALS_FILE"
        echo "  ✓ GitHub credentials configured for auto_commit.sh"
    else
        echo "  ⚠ GITHUB_TOKEN not found in .env - git push may require manual authentication"
    fi
else
    echo "  ⚠ .env file not found at $ENV_FILE"
    echo "  Please create .env with the following variables:"
    echo "    GIT_USER_NAME=your-github-username"
    echo "    GIT_USER_EMAIL=your-email@example.com"
    echo "    GITHUB_TOKEN=your-personal-access-token"
fi

# -----------------------------------------------------------------------------
# 4. Set up cron jobs for scheduled updates
# -----------------------------------------------------------------------------
echo ""
echo "[4/4] Setting up cron jobs..."

# Cron schedule:
# - 10:00 AM UTC daily: ./manage-env.sh prod update
# - 4:10 PM UTC daily (16:10): ./manage-env.sh prod update
# - 11:15 PM UTC daily (23:15): ./manage-env.sh prod stop && start && init

CRON_10AM="0 10 * * * cd $PROJECT_DIR && ./manage-env.sh prod update >> $PROJECT_DIR/logs/cron_update.log 2>&1"
CRON_410PM="10 16 * * * cd $PROJECT_DIR && ./manage-env.sh prod update >> $PROJECT_DIR/logs/cron_update.log 2>&1"
CRON_AUTOCOMMIT="59 23 * * * $PROJECT_DIR/auto_commit.sh >> $PROJECT_DIR/logs/cron_autocommit.log 2>&1"
CRON_RESTART="15 23 * * * cd $PROJECT_DIR && ./manage-env.sh prod stop && ./manage-env.sh prod start && ./manage-env.sh prod init >> $PROJECT_DIR/logs/cron_restart.log 2>&1"

# Get existing crontab (or empty if none)
CURRENT_CRONTAB=$(crontab -l 2>/dev/null || true)

# Function to add cron job if it doesn't exist
add_cron_if_missing() {
    local job="$1"
    local description="$2"
    if echo "$CURRENT_CRONTAB" | grep -Fq "manage-env.sh prod update" && [[ "$job" == *"manage-env.sh prod update"* ]]; then
        # Check exact match for this specific time
        if echo "$CURRENT_CRONTAB" | grep -Fq "$job"; then
            echo "  ✓ Cron job already exists: $description"
        else
            CURRENT_CRONTAB="$CURRENT_CRONTAB"$'\n'"$job"
            echo "  + Adding cron job: $description"
        fi
    elif echo "$CURRENT_CRONTAB" | grep -Fq "$job"; then
        echo "  ✓ Cron job already exists: $description"
    else
        CURRENT_CRONTAB="$CURRENT_CRONTAB"$'\n'"$job"
        echo "  + Adding cron job: $description"
    fi
}

add_cron_if_missing "$CRON_10AM" "prod update at 10:00 AM UTC"
add_cron_if_missing "$CRON_410PM" "prod update at 4:10 PM UTC"
add_cron_if_missing "$CRON_AUTOCOMMIT" "auto commit at 11:59 PM UTC"
add_cron_if_missing "$CRON_RESTART" "prod stop/start/init at 11:15 PM UTC"

# Remove empty lines and install updated crontab
echo "$CURRENT_CRONTAB" | grep -v '^$' | crontab -

echo ""
echo "Current crontab:"
crontab -l

echo ""
echo "=================================================="
echo "  ✅ Compute instance setup complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "  1. Ensure .env file exists with required variables"
echo "  2. Run: cd $PROJECT_DIR && ./manage-env.sh prod start"
echo ""

