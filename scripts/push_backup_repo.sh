#!/usr/bin/env bash
# Copy the newest backups/app dump into a git clone and push it (off-box copy).
#
# Runs on the host (uses the host's git credentials), after backup_app.sh:
#   APP_BACKUP_REPO_DIR=~/stocks-assist-data ./scripts/push_backup_repo.sh
#
# Stores plain SQL (app.sql) so day-to-day diffs stay small.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

REPO="${APP_BACKUP_REPO_DIR:-}"
if [[ -z "$REPO" || ! -d "$REPO/.git" ]]; then
  echo "Backup repo: not set up (${REPO:-APP_BACKUP_REPO_DIR unset}); skipped."
  exit 0
fi
LATEST="$(ls -1t backups/app/app_*.sql.gz | head -1)"

gunzip -c "$LATEST" > "$REPO/app.sql"
cd "$REPO"
git add app.sql
if git diff --cached --quiet; then
  echo "Backup repo: no changes."
else
  git commit -q -m "app schema backup $(basename "$LATEST" .sql.gz)"
  git push -q origin HEAD
  echo "Backup repo: pushed $(basename "$LATEST")."
fi
