#!/bin/bash

SESSION="aoe2-dev"

# Kill any existing session
tmux kill-session -t $SESSION 2>/dev/null

# Start new session with frontend on the left
tmux new-session -d -s $SESSION -n dev "cd ~/projects/aoe2hd-frontend && npm run dev"

# Split horizontally: backend on the right
tmux split-window -h -t $SESSION:0 "cd ~/projects/aoe2hd-parsing && ENV=development uvicorn app:app --reload --host 0.0.0.0 --port 8002"

# Rename panes
tmux select-pane -t $SESSION:0.0 -T "Frontend"
tmux select-pane -t $SESSION:0.1 -T "Backend"

# Bind 'r' inside this tmux session to restart frontend/backend visibly
# ✅ CORRECTED (new path in scripts/)
tmux send-keys -t $SESSION:0 "tmux bind r run-shell '~/projects/aoe2hd-parsing/scripts/restart-dev.sh' \; display-message '🔄 Dev session restarted'" C-m

# Attach session
tmux attach -t $SESSION
