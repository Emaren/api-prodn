#!/bin/bash

cat scripts/.env.template | envsubst > .env.production

echo "✅ .env.production generated from template:"
cat .env.production
