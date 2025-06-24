#!/bin/bash

cd /var/www/api-prod

pm2 delete api-prod

pm2 start "uvicorn app:app --host 0.0.0.0 --port 8002" \
  --name api-prod \
  --env GOOGLE_APPLICATION_CREDENTIALS=/var/www/api-prod/secrets/serviceAccountKey.json
