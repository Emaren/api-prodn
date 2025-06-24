#!/bin/bash

SESSION="aoe2-dev"

# Restart frontend (pane 0) and keep pane alive
tmux respawn-pane -t $SESSION:0.0 -k \
  "clear && cd ~/projects/aoe2hd-frontend && exec npm run dev"

# Restart backend (pane 1) and keep pane alive
tmux respawn-pane -t $SESSION:0.1 -k \
  "clear && cd ~/projects/aoe2hd-parsing && ENV=development exec uvicorn app:app --reload --host 0.0.0.0 --port 8002"

tmux display-message "🔄 Dev session restarted"
