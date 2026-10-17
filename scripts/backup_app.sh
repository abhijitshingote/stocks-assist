#!/usr/bin/env bash
# Dump the Postgres ``app`` schema (user data + published pipeline outputs) to backups/app/.
#
# Run inside the backend container (repo mounted at /app):
#   docker compose exec -T backend ./scripts/backup_app.sh
#
# Writes backups/app/app_<UTC timestamp>.sql.gz (keeps the newest APP_BACKUP_KEEP, default 60).
# Off-box copy: if backups/data-repo is a git clone, scripts/push_backup_repo.sh commits + pushes
# the dump there. Restore: scripts/restore_app.sh.
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

if [[ -d "${APP_BACKUP_REPO_DIR:-backups/data-repo}/.git" ]]; then
  ./scripts/push_backup_repo.sh
fi
