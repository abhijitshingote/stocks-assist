#!/usr/bin/env bash
# Dump the Postgres ``app`` schema (user data + published pipeline outputs) to backups/app/.
#
# Run inside the backend container (repo mounted at /app):
#   docker compose exec -T backend ./scripts/backup_app.sh
#
# Writes backups/app/app_<UTC timestamp>.sql.gz (keeps the newest APP_BACKUP_KEEP, default 60).
# If APP_BACKUP_REPO_DIR points at a git clone, also writes app.sql there, commits and pushes
# (plain SQL so day-to-day diffs stay small). Restore: scripts/restore_app.sh.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

DATABASE_URL="${DATABASE_URL:-postgresql://postgres:postgres@db:5432/stocks_db}"
OUT_DIR="backups/app"
KEEP="${APP_BACKUP_KEEP:-60}"
STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"

mkdir -p "$OUT_DIR"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

pg_dump --dbname="$DATABASE_URL" --schema=app --no-owner --no-privileges --format=plain > "$TMP"
gzip -c "$TMP" > "$OUT_DIR/app_${STAMP}.sql.gz"
echo "Wrote $OUT_DIR/app_${STAMP}.sql.gz ($(wc -c < "$OUT_DIR/app_${STAMP}.sql.gz") bytes)"

ls -1t "$OUT_DIR"/app_*.sql.gz | tail -n +"$((KEEP + 1))" | xargs -r rm -f

if [[ -n "${APP_BACKUP_REPO_DIR:-}" ]]; then
  cp "$TMP" "$APP_BACKUP_REPO_DIR/app.sql"
  cd "$APP_BACKUP_REPO_DIR"
  git add app.sql
  if git diff --cached --quiet; then
    echo "Backup repo: no changes."
  else
    git commit -q -m "app schema backup ${STAMP}"
    git push -q
    echo "Backup repo: pushed ${STAMP}."
  fi
fi
