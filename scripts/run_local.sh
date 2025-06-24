#!/bin/bash

# Load environment variables (default to .env if not provided)
ENV_FILE="${ENV_FILE:-.env}"
export $(grep -v '^#' "$ENV_FILE" | xargs)

echo "🚀 Launching FastAPI locally at http://localhost:8002 ..."
uvicorn app:app --reload --host 0.0.0.0 --port 8002
