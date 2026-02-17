#!/bin/bash

set -e

SCRIPTS_DIR="$(dirname "$0")"

print_menu() {
  echo "=============================="
  echo "  Wolo Postgres Admin"
  echo "=============================="
  echo "1) Check All Users"
  echo "2) Check Users"
  echo "3) Sync Data"
  echo "4) Sync Schema"
  echo "5) Migrate DB"
  echo "6) Reset DB for Launch"
  echo "7) Wipe for Launch"
  echo "8) Wipe Users and Game Stats"
  echo "0) Exit"
  echo "------------------------------"
}

while true; do
  print_menu
  read -p "Select option: " opt

  case $opt in
    1)
      bash "$SCRIPTS_DIR/check_all_users.sh"
      ;;
    2)
      bash "$SCRIPTS_DIR/check_users.sh"
      ;;
    3)
      bash "$SCRIPTS_DIR/sync_data.sh"
      ;;
    4)
      bash "$SCRIPTS_DIR/sync_schema.sh"
      ;;
    5)
      bash "$SCRIPTS_DIR/migrate.sh"
      ;;
    6)
      bash "$SCRIPTS_DIR/reset_db_for_launch.sh"
      ;;
    7)
      bash "$SCRIPTS_DIR/wipe_for_launch.sh"
      ;;
    8)
      bash "$SCRIPTS_DIR/wipe_users_and_game_stats.sh"
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
