#!/bin/bash

# 🧠 Always absolute paths to be safe
BACKEND_DIR="$HOME/projects/AoE2HDBets/api-prodn"
FRONTEND_DIR="$HOME/projects/AoE2HDBets/app-prodn"

# 1. Git push frontend
echo "🧼 Pushing frontend..."
cd "$FRONTEND_DIR" || exit 1
git add .
git commit -m "🚀 Frontend prod deploy" || echo "✅ No changes to commit in frontend."
git push origin main

# 2. Git push backend
echo "🧼 Pushing backend..."
cd "$BACKEND_DIR" || exit 1
git add .
git commit -m "🚀 Backend prod deploy" || echo "✅ No changes to commit in backend."
git push origin main

# Inject production secrets and generate .env.production
export FRONTEND_URL="https://aoe2-betting.vercel.app"
export ENABLE_REALTIME=true
export SHOW_DEBUG_UI=false
export DATABASE_URL="postgresql+asyncpg://aoe2db_user:your-password@your-db-host:5432/aoe2db"
export ADMIN_TOKEN="your_secure_admin_token"
export FASTAPI_URL="https://aoe2hdbets.com/api/parse_replay"
export API_TARGETS="https://aoe2hdbets.com/api/parse_replay"

bash scripts/generate_env.sh

# 3. Alembic DB migration
echo "🛠️ Applying Alembic migrations to Render database..."
export ENV=production
set -a
source "$BACKEND_DIR/.env.production"
set +a
export PYTHONPATH="$BACKEND_DIR"
alembic upgrade head
echo "✅ Alembic migrations applied successfully!"

echo "🎉 All systems go. Full prod deploy complete."
