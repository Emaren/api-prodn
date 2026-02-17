#!/bin/bash
# scripts/run-dev.sh

SESSION_NAME="aoe2-dev"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
APP_DIR="$(cd "$API_DIR/../app-prodn" && pwd)"

tmux has-session -t $SESSION_NAME 2>/dev/null

if [ $? != 0 ]; then
  tmux new-session -d -s $SESSION_NAME

  # Backend pane
  tmux send-keys -t $SESSION_NAME "cd '$API_DIR' && pkill -f 'uvicorn.*8002' || true && ENV=development AUTO_CREATE_TABLES=true uvicorn app:app --reload --host 0.0.0.0 --port 8002" C-m

  # Frontend pane
  tmux split-window -h -t $SESSION_NAME
  tmux send-keys -t $SESSION_NAME "cd '$APP_DIR' && rm -rf .next && npm run dev" C-m

  # Focus on left pane (backend)
  tmux select-pane -t $SESSION_NAME:.0
fi

tmux attach-session -t $SESSION_NAME
