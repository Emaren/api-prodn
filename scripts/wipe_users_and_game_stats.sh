#!/bin/bash
set -e

echo "🔍 Checking user counts BEFORE wipe..."
./scripts/check_all_users.sh
echo

read -p "⚠️ Are you sure you want to DELETE ALL users from Postgres? (y/n): " confirm
[[ $confirm == [yY] ]] || exit 1

echo "🧹 Truncating PostgreSQL users table (cascades to game_stats)..."
psql -U aoe2user -d aoe2db -h localhost -c "TRUNCATE TABLE users RESTART IDENTITY CASCADE;"

# Only try connecting if optional env var is present
if [[ -n "$RENDER_DB_HOST" && -n "$RENDER_DB_USER" && -n "$RENDER_DB_NAME" ]]; then
  PGPASSWORD="$RENDER_DB_PASSWORD" psql -U "$RENDER_DB_USER" -d "$RENDER_DB_NAME" -h "$RENDER_DB_HOST" -c "
    SELECT email, ingame_name, is_verified FROM users ORDER BY created_at DESC LIMIT 20;
  " || echo "❌ Could not connect to Render Postgres."
else
  echo "   ⚠️ Skipping Render DB check — missing credentials or RENDER_DB_HOST unset."
fi

echo
echo "🔁 Checking user counts AFTER wipe..."
./scripts/check_all_users.sh

echo
echo "✅ All users deleted from Postgres."
