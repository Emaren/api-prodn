#!/bin/bash

LOG_FILE="/var/log/nginx/access.log"
tail -n 20 "$LOG_FILE"
