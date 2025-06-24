#!/bin/bash
set -e

DB_NAME="aoe2db"
DB_USER="aoe2user"
DB_PASS="aoe2pass"  # adjust if needed
BACKUP_FILE="aoe2db_backup_$(date +%Y%m%d_%H%M%S).sql"

echo "📦 Backing up current DB to $BACKUP_FILE..."
sudo -u postgres pg_dump -U postgres "$DB_NAME" > "$BACKUP_FILE"
echo "✅ Backup complete."

echo "🔥 Dropping DB and user..."
sudo -u postgres psql -c "DROP DATABASE IF EXISTS $DB_NAME;"
sudo -u postgres psql -c "DROP ROLE IF EXISTS $DB_USER;"
echo "✅ Dropped."

echo "🔧 Recreating DB and user..."
sudo -u postgres psql -c "CREATE ROLE $DB_USER WITH LOGIN PASSWORD '$DB_PASS';"
sudo -u postgres psql -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;"
echo "✅ Created."

echo "📐 Applying base schema via Alembic..."
alembic downgrade base
alembic upgrade head
echo "✅ Alembic migrations applied."

echo "🎯 Database fully reset and schema is fresh."
