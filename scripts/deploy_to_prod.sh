#!/bin/bash

cd "$(dirname "$0")"

# Load production env vars manually into the shell
export ENV=production
set -a
source .env.production
set +a

export PYTHONPATH=$(pwd)

echo "🚀 Applying Alembic migrations to production database..."

alembic upgrade head

echo "✅ Production schema updated successfully!"
