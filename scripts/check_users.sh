#!/bin/bash
cd /var/www/aoe2hdbets-api/aoe2hd-parsing || exit 1

# Export Firebase users to JSON
firebase auth:export users.json --format=json > /dev/null

echo "🧮 User Count Summary"
echo "----------------------"

# Firebase user count and emails
firebase_count=$(jq '.users | length' users.json)
echo "📥 Firebase users:  $firebase_count"

if [ "$firebase_count" -gt 0 ]; then
  echo "📧 Firebase emails:"
  jq -r '.users[].email' users.json | sed 's/^/   - /'
else
  echo "   No Firebase users found."
fi

echo ""

# Postgres user count and details
postgres_count=$(psql -h localhost -U aoe2user -d aoe2db -tAc "SELECT COUNT(*) FROM users;")
echo "📊 Postgres users: $postgres_count"

if [ "$postgres_count" -gt 0 ]; then
  echo "👤 Postgres user details:"
  psql -h localhost -U aoe2user -d aoe2db -P pager=off -c \
    "SELECT email, in_game_name, CASE WHEN is_admin THEN '✅ admin' ELSE '❌' END AS role FROM users;" \
    | sed '1d;$d' | sed 's/^/   - /'
else
  echo "   No Postgres users found."
fi
