#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

git add user_data/

if git diff --cached --quiet; then
  echo "No changes in user_data to commit."
  exit 0
fi

git commit -m "Auto-backup user_data - $(date)"
git push --force origin
