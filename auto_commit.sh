#!/usr/bin/env bash
set -euo pipefail

# Repo root is the directory containing this script (works locally and in Docker /app).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

if ! command -v git >/dev/null 2>&1; then
  echo "git is not installed or not on PATH" >&2
  exit 127
fi

git add user_data/

if git diff --cached --quiet; then
  echo "No changes in user_data to commit."
  exit 0
fi

git commit -m "Auto-backup user_data - $(date)"
git push --force origin
