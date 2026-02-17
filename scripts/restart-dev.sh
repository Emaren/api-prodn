#!/bin/bash

SESSION="aoe2-dev"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
APP_DIR="$(cd "$API_DIR/../app-prodn" && pwd)"

# Restart frontend (pane 0) and keep pane alive
tmux respawn-pane -t $SESSION:0.0 -k \
  "clear && cd '$APP_DIR' && exec npm run dev"

# Restart backend (pane 1) and keep pane alive
tmux respawn-pane -t $SESSION:0.1 -k \
  "clear && cd '$API_DIR' && ENV=development AUTO_CREATE_TABLES=true exec uvicorn app:app --reload --host 0.0.0.0 --port 8002"

tmux display-message "Dev session restarted"
