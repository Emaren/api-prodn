#!/usr/bin/env bash
# go to the repo root (where app.py lives)
cd "$(dirname "$0")"

# skip activation, call uvicorn with the venv's Python directly
exec ./venv/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8003
