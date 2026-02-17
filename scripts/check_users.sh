#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

DB_HOST="${DB_HOST:-localhost}"
DB_USER="${DB_USER:-aoe2user}"
DB_NAME="${DB_NAME:-aoe2db}"

echo "🧮 Postgres User Summary"
echo "------------------------"

postgres_count=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -tAc "SELECT COUNT(*) FROM users;")
echo "📊 Postgres users: $postgres_count"

if [ "$postgres_count" -gt 0 ]; then
  echo "👤 User details:"
  psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -P pager=off -c \
    "SELECT uid, email, in_game_name, CASE WHEN is_admin THEN 'admin' ELSE 'user' END AS role FROM users ORDER BY created_at DESC LIMIT 100;" \
    | sed '1d;$d' | sed 's/^/   - /'
else
  echo "   No Postgres users found."
fi
