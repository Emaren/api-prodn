# api-prodn

Production FastAPI backend for AoE2HDBets.

## Responsibilities

- Replay ingestion and parsing
- `game_stats` persistence in Postgres
- User/admin endpoints
- Traffic diagnostics endpoint

## Key replay routes

- `POST /api/replay/upload`:
  - Accepts multipart replay file upload
  - Parses server-side
  - Stores final replay row in `game_stats`
- `POST /api/parse_replay`:
  - JSON replay ingestion path (compatible with helper scripts)
  - Supports `x-api-key` when `INTERNAL_API_KEY` is configured

## Local development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload --host 0.0.0.0 --port 8002
```

## Environment

Required:

- `DATABASE_URL` (`postgresql+asyncpg://...`)

Optional/common:

- `INTERNAL_API_KEY`
- `MAX_REPLAY_UPLOAD_BYTES`
- `CHAIN_ID`
- `ALLOWED_ORIGINS`

## Helper scripts

- `watch_replays.py` watches local replay folders and triggers parsing uploads.
- `parse_replay.py` parses replay files and sends JSON to configured API targets.

## Deployment model

- Local MBP -> push `main` -> VPS pull `main` -> restart service.
