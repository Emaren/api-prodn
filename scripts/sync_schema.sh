#!/bin/bash
set -e
source .env.dbs

echo "📤 Dumping schema from LOCAL..."
pg_dump -s --clean -U aoe2user -h localhost -d aoe2db > db/schema.sql

echo "🐳 Syncing schema to DOCKER..."
docker cp db/schema.sql aoe2-postgres:/schema.sql
docker exec -it aoe2-postgres psql -U aoe2user -d aoe2db -f /schema.sql

echo "🌐 Syncing schema to RENDER..."
psql "$RENDER_DB_URI" < db/schema.sql

echo "✅ Schema synced across all environments."
