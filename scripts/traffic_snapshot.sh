#!/bin/bash

LOG_FILE="${AOE2_TRAFFIC_LOG_PATH:-${TRAFFIC_LOG_PATH:-/var/log/nginx/aoe2hdbets.access.log}}"
FALLBACK_LOG_FILE="/var/log/nginx/access.log"

if [ ! -f "$LOG_FILE" ] && [ -f "$FALLBACK_LOG_FILE" ]; then
  LOG_FILE="$FALLBACK_LOG_FILE"
fi

tail -n 20 "$LOG_FILE"
