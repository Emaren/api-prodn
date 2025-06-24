#!/bin/bash
set -e

DB_NAME="aoe2db"
DB_USER="aoe2user"
BACKUP_FILE="aoe2db_backup_$(date +%Y%m%d_%H%M%S).sql"

echo "📦 Backing up current DB to $BACKUP_FILE..."
sudo -u postgres pg_dump -U postgres "$DB_NAME" > "$BACKUP_FILE"
echo "✅ Backup complete."

echo "⚠️ Truncating all tables..."
sudo -u postgres psql -d "$DB_NAME" -c "TRUNCATE TABLE users, game_stats RESTART IDENTITY CASCADE;"
echo "✅ Tables truncated and sequences reset."

echo "🎯 Schema now clean and ready for production launch."
