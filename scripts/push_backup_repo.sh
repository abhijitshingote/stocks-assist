#!/usr/bin/env bash
# Copy the newest backups/app dump into the backup repo clone and push it (off-box copy).
#
# Called by backup_app.sh when backups/data-repo/.git exists (cron + nav Save both go through it).
# Auth: GITHUB_TOKEN (or GIT_PUSH_TOKEN / GIT_TOKEN) from .env via docker/git_askpass_env.sh.
# Stores plain SQL (app.sql) so day-to-day diffs stay small.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

REPO="${APP_BACKUP_REPO_DIR:-backups/data-repo}"
LATEST="$(ls -1t backups/app/app_*.sql.gz | head -1)"
export GIT_ASKPASS="${GIT_ASKPASS:-$PWD/docker/git_askpass_env.sh}"
export GIT_TERMINAL_PROMPT=0

# pg_dump 17 adds a random \restrict token per dump; drop it so unchanged data makes no commit.
gunzip -c "$LATEST" | grep -v -E '^\\(un)?restrict ' > "$REPO/app.sql"
cd "$REPO"
git config --global --add safe.directory "$PWD" 2>/dev/null || true
git add app.sql
if git diff --cached --quiet; then
  echo "Backup repo: no changes."
else
  git -c user.name="stocks-assist backup" -c user.email="backup@stocks-assist" \
    commit -q -m "app schema backup $(basename "$LATEST" .sql.gz)"
  git push -q origin HEAD
  echo "Backup repo: pushed $(basename "$LATEST")."
fi
