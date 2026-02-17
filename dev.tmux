#!/bin/bash

SESSION="aoe2dev"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_DIR="$SCRIPT_DIR"
APP_DIR="$(cd "$API_DIR/../app-prodn" && pwd)"

# 💀 Kill any existing tmux session cleanly
tmux kill-session -t $SESSION 2>/dev/null

# 💀 Kill any leftover uvicorn processes cleanly
pkill -f "uvicorn.*8002" 2>/dev/null

# ----------------------------
# Step 1: Create the main window ("dev") with Backend and Watcher side-by-side
# ----------------------------
tmux new-session -d -s $SESSION -n dev "cd '$API_DIR' && uvicorn app:app --reload --host localhost --port 8002"
tmux split-window -h -t $SESSION:dev "cd '$API_DIR' && python watch_replays.py"
tmux select-layout -t $SESSION:dev even-horizontal

# ----------------------------
# Step 2: Create a separate watcher window running the frontend
# ----------------------------
tmux new-window -t $SESSION -n watcher "cd '$APP_DIR' && npm run dev"

# ----------------------------
# Step 3: Join the watcher pane into the main window as a full-width bottom pane
# ----------------------------
tmux join-pane -v -s $SESSION:watcher.0 -t $SESSION:dev.0

# Kill the now empty watcher window
tmux kill-window -t $SESSION:watcher

# ----------------------------
# Step 4: Force a clean tiled layout (leave your perfect window sizing)
# ----------------------------
tmux select-layout -t $SESSION:dev tiled

# Focus on the backend pane (pane 0) for convenience
tmux select-pane -t $SESSION:dev.0

# Attach to the session
tmux attach-session -t $SESSION
