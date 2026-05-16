#!/usr/bin/env bash
# Push user_data/ to a dedicated branch with --force (no merges).
# Snapshots user_data before checkout churn; restores it afterward.
# Each backup commit is orphan + contains only user_data/. Remote main is untouched.
#
# Env:
#   REPO                     Repo root (default: parent of this script)
#   USER_DATA_BACKUP_BRANCH  Remote branch (default: user-data-backup)
#   USER_DATA_WORK_BRANCH    Local scratch branch (default: _cron-user-data-backup)
#
# Cron:
#   0 6,18 * * * .../backup_user_data_github.sh >>/var/log/user-data-backup.log 2>&1
#
# Restore:
#   git fetch origin user-data-backup && git checkout origin/user-data-backup -- user_data/

set -euo pipefail

REPO="${REPO:-$(cd "$(dirname "$0")/.." && pwd)}"
BACKUP_BRANCH="${USER_DATA_BACKUP_BRANCH:-user-data-backup}"
WORK_BRANCH="${USER_DATA_WORK_BRANCH:-_cron-user-data-backup}"

TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
USER_DATA_DIR="$REPO/user_data"

cd "$REPO"

if [[ ! -d "$USER_DATA_DIR" ]]; then
  echo "error: missing $USER_DATA_DIR" >&2
  exit 1
fi

short="$(git symbolic-ref -q --short HEAD 2>/dev/null || true)"
if [[ "$short" == "$WORK_BRANCH" ]]; then
  echo "error: HEAD is $WORK_BRANCH (stuck from a prior run). Checkout main or a feature branch, then retry." >&2
  exit 1
fi

# Branch ref or detached SHA — restored verbatim after backup.
RESTORE="$(git symbolic-ref -q HEAD 2>/dev/null || git rev-parse HEAD)"

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

cp -a "$USER_DATA_DIR"/. "$TMP/"

git branch -D "$WORK_BRANCH" 2>/dev/null || true
git checkout --orphan "$WORK_BRANCH"

git rm -rf . >/dev/null 2>&1 || true
mkdir -p "$USER_DATA_DIR"
cp -a "$TMP"/. "$USER_DATA_DIR/"

git add -f user_data/
if git diff --cached --quiet; then
  echo "[backup] nothing to commit under user_data/; skip push."
  git checkout -f "$RESTORE"
  cp -a "$TMP"/. "$USER_DATA_DIR/"
  git branch -D "$WORK_BRANCH"
  exit 0
fi

git commit -m "user_data backup $TS"
git push origin "$WORK_BRANCH:$BACKUP_BRANCH" --force

git checkout -f "$RESTORE"
cp -a "$TMP"/. "$USER_DATA_DIR/"

git branch -D "$WORK_BRANCH"

echo "[backup] pushed origin/$BACKUP_BRANCH at $TS"
