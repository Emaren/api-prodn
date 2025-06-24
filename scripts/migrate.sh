#!/bin/bash
set -e
source .env.dbs

target="$1"
export PYTHONPATH=$(pwd)  # Ensures `db/` is importable

case "$target" in
  render)
    echo "🔼 Applying migration to RENDER..."
    alembic -x "db_url=$RENDER_DB_URI" upgrade head
    echo "🔁 Re-syncing schema and data to RENDER..."
    ./scripts/sync_schema.sh
    ./scripts/sync_data.sh
    ;;
  local)
    echo "🔼 Applying migration to LOCAL..."
    alembic -x "db_url=$LOCAL_DB_URI" upgrade head
    echo "🔁 Re-syncing schema and data to LOCAL..."
    ./scripts/sync_schema.sh
    ./scripts/sync_data.sh
    ;;
  *)
    echo "❌ Usage: ./scripts/migrate.sh [local|render]"
    exit 1
    ;;
esac
