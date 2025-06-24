#!/bin/bash

# ───────────────────────────────────────────────
# 🌍 Detect Environment
# ───────────────────────────────────────────────
if [[ "$(hostname)" == "wolo" ]]; then
  # VPS
  ENV="vps"
  PROJECT_DIR="/var/www/aoe2hdbets-api/aoe2hd-parsing"
else
  # Local Mac
  ENV="local"
  PROJECT_DIR="$HOME/projects/aoe2hd-parsing"
fi

cd "$PROJECT_DIR" || { echo "❌ Project directory not found: $PROJECT_DIR"; exit 1; }
source .env.dbs

# ───────────────────────────────────────────────
# 🔥 Firebase / Firestore Auth
# ───────────────────────────────────────────────
firebase auth:export users.json --format=json > /dev/null
firebase_count=$(jq '.users | length' users.json)

echo "🔥 Firebase Auth Users"
echo "----------------------"
echo "📥 Firebase users:  $firebase_count"
if [ "$firebase_count" -gt 0 ]; then
  jq -r '.users[] | "\(.uid) \(.email) \(.displayName // "-") \(.createdAt // "-")"' users.json
else
  echo "   No Firebase users found."
fi
echo ""

# ───────────────────────────────────────────────
# 🐘 Local Postgres
# ───────────────────────────────────────────────
echo "🐘 Local Postgres Users"
echo "-----------------------"

local_count=$(PGPASSWORD=$PGPASSWORD psql -h localhost -U aoe2user -d aoe2db -tAc "SELECT COUNT(*) FROM users;")
echo "📊 Local DB users: $local_count"

if [ "$local_count" -gt 0 ]; then
  PGPASSWORD=$PGPASSWORD psql -h localhost -U aoe2user -d aoe2db -P pager=off -c \
    "SELECT email, in_game_name, CASE WHEN is_admin THEN '✅ admin' ELSE '❌' END AS role FROM users;" \
    | sed '1d;$d' | sed 's/^/   - /'
else
  echo "   No users in local Postgres."
fi
echo ""

# ───────────────────────────────────────────────
# ☁️ VPS Postgres (via SSH)
# ───────────────────────────────────────────────
echo "☁️ VPS Postgres Users (via SSH)"
echo "-------------------------------"

PGPASS=$(echo "$LOCAL_DB_URI" | sed -E 's|.*://[^:]+:([^@]+)@.*|\1|')

ssh root@157.180.114.124 PGPASSWORD="$PGPASS" bash <<'EOF'
COUNT=$(psql -h localhost -U aoe2user -d aoe2db -tAc "SELECT COUNT(*) FROM users;")
echo "📊 VPS DB users: $COUNT"

if [ "$COUNT" -gt 0 ]; then
  psql -h localhost -U aoe2user -d aoe2db -P pager=off -c \
    "SELECT uid, email, in_game_name, is_admin, created_at FROM users;" \
      | sed '1d;$d' | sed 's/^/   - /'
else
  echo "   No users in VPS Postgres."
fi
EOF

