#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

if [[ -f .env.dbs ]]; then
  # shellcheck disable=SC1091
  source .env.dbs
fi

DB_HOST="${DB_HOST:-localhost}"
DB_USER="${DB_USER:-aoe2user}"
DB_NAME="${DB_NAME:-aoe2db}"
VPS_SSH="${VPS_SSH:-root@157.180.114.124}"

echo "🐘 Local Postgres Users"
echo "-----------------------"

local_count=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -tAc "SELECT COUNT(*) FROM users;")
echo "📊 Local DB users: $local_count"

if [ "$local_count" -gt 0 ]; then
  psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -P pager=off -c \
    "SELECT uid, email, in_game_name, CASE WHEN is_admin THEN 'admin' ELSE 'user' END AS role, created_at FROM users ORDER BY created_at DESC LIMIT 100;" \
    | sed '1d;$d' | sed 's/^/   - /'
else
  echo "   No users in local Postgres."
fi
echo ""

echo "☁️ VPS Postgres Users (via SSH)"
echo "-------------------------------"

if [[ -z "${PGPASSWORD:-}" ]]; then
  echo "⚠️ PGPASSWORD is not set; skipping VPS query."
  exit 0
fi

ssh "$VPS_SSH" PGPASSWORD="$PGPASSWORD" DB_USER="$DB_USER" DB_NAME="$DB_NAME" bash <<'EOF'
COUNT=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -tAc "SELECT COUNT(*) FROM users;")
echo "📊 VPS DB users: $COUNT"

if [ "$COUNT" -gt 0 ]; then
  psql -h localhost -U "$DB_USER" -d "$DB_NAME" -P pager=off -c \
    "SELECT uid, email, in_game_name, is_admin, created_at FROM users ORDER BY created_at DESC LIMIT 100;" \
      | sed '1d;$d' | sed 's/^/   - /'
else
  echo "   No users in VPS Postgres."
fi
EOF
