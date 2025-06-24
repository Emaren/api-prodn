#!/bin/bash
# scripts/run-dev.sh

SESSION_NAME="aoe2-dev"

tmux has-session -t $SESSION_NAME 2>/dev/null

if [ $? != 0 ]; then
  tmux new-session -d -s $SESSION_NAME

  # Backend pane
  tmux send-keys -t $SESSION_NAME 'cd ~/projects/aoe2hd-parsing && pkill -f "uvicorn" || true && make dev' C-m

  # Frontend pane
  tmux split-window -h -t $SESSION_NAME
  tmux send-keys -t $SESSION_NAME 'cd ~/projects/aoe2hd-frontend && rm -rf .next && npm run dev' C-m

  # Focus on left pane (backend)
  tmux select-pane -t $SESSION_NAME:.0
fi

tmux attach-session -t $SESSION_NAME
