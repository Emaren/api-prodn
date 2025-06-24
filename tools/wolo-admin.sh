#!/bin/bash

# Location: /var/www/api-prod/tools/wolo-admin.sh

set -e

TOOLS_DIR="$(dirname "$0")"

print_menu() {
  echo "=============================="
  echo "  Wolo Admin Panel"
  echo "=============================="
  echo "1) Chain Status (wolo-status)"
  echo "2) Address Inspector"
  echo "3) Node Sync Check"
  echo "4) API Healthcheck"
  echo "5) Port Check"
  echo "0) Exit"
  echo "------------------------------"
}

check_sync() {
  for PORT in 1317 1327 1337; do
    echo "Checking sync status on port $PORT"
    curl -s http://localhost:$PORT/cosmos/base/tendermint/v1beta1/syncing | jq
    echo "------------------------------"
  done
}

api_healthcheck() {
  echo "Pinging API Gateway..."
  curl -I https://api.aoe2hdbets.com | grep HTTP
}

port_check() {
  echo "Active ports:"
  sudo lsof -i -P -n | grep LISTEN
}

while true; do
  print_menu
  read -p "Select option: " opt

  case $opt in
    1)
      bash "$TOOLS_DIR/wolo-status.sh"
      ;;
    2)
      read -p "Enter address: " addr
      python3 "$TOOLS_DIR/address_inspector.py" "$addr"
      ;;
    3)
      check_sync
      ;;
    4)
      api_healthcheck
      ;;
    5)
      port_check
      ;;
    0)
      echo "Goodbye."
      exit 0
      ;;
    *)
      echo "Invalid option."
      ;;
  esac
done
