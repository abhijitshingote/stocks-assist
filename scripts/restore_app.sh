#!/usr/bin/env bash
# Replace the Postgres ``app`` schema with a dump from scripts/backup_app.sh.
#
#   docker compose exec -T backend ./scripts/restore_app.sh                       # newest backups/app/*.sql.gz
#   docker compose exec -T backend ./scripts/restore_app.sh backups/app/app_<ts>.sql.gz
#   docker compose exec -T backend ./scripts/restore_app.sh /path/to/app.sql
#
# Drops schema ``app`` and reloads it in one transaction; ``public`` (price data) is untouched.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

DATABASE_URL="${DATABASE_URL:-postgresql://postgres:postgres@db:5432/stocks_db}"
DUMP="${1:-$(ls -1t backups/app/app_*.sql.gz 2>/dev/null | head -n 1)}"
if [[ -z "$DUMP" || ! -f "$DUMP" ]]; then
  echo "No dump found (arg or backups/app/app_*.sql.gz)." >&2
  exit 1
fi

if [[ "$DUMP" == *.gz ]]; then READ=(gzip -dc "$DUMP"); else READ=(cat "$DUMP"); fi

echo "Restoring app schema from $DUMP"
# pg_dump 17 (backend image) emits SET transaction_timeout, which Postgres 14 rejects.
{
  echo "DROP SCHEMA IF EXISTS app CASCADE;"
  "${READ[@]}" | grep -v '^SET transaction_timeout'
} | psql --dbname="$DATABASE_URL" --quiet --single-transaction -v ON_ERROR_STOP=1 > /dev/null

for t in $(psql --dbname="$DATABASE_URL" -At -c \
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'app' ORDER BY 1"); do
  echo "app.$t $(psql --dbname="$DATABASE_URL" -At -c "SELECT count(*) FROM app.$t")"
done
