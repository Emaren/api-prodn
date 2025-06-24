#!/bin/bash
set -e
source .env.dbs

echo "📤 Dumping data from LOCAL..."
pg_dump -U aoe2user -h localhost -d aoe2db -a --data-only > db/data.sql

echo "🐳 Syncing data to DOCKER..."
docker cp db/data.sql aoe2-postgres:/data.sql
docker exec -it aoe2-postgres psql -U aoe2user -d aoe2db -f /data.sql

echo "🌐 Syncing data to RENDER..."
psql "$RENDER_DB_URI" < db/data.sql

echo "✅ Data synced across all environments."
