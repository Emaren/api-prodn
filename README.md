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
  - Supports `x-api-key` when `INTERNAL_API_KEY` is configured
- `POST /api/parse_replay`:
  - JSON replay ingestion path (compatible with helper scripts)
  - Supports `x-api-key` when `INTERNAL_API_KEY` is configured
- `GET /api/traffic`:
  - Traffic diagnostics data
  - Requires admin bearer token (`ADMIN_TOKEN`)

## Local development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
uvicorn app:app --reload --host 0.0.0.0 --port 8002
```

## Environment

Start from `.env.production.example` and create your local `.env.production`.

Required:

- `DATABASE_URL` (`postgresql+asyncpg://...`)
- `ADMIN_TOKEN` (required for `/api/admin/*` routes)

Optional/common:

- `INTERNAL_API_KEY`
- `MAX_REPLAY_UPLOAD_BYTES`
- `CHAIN_ID`
- `ALLOWED_ORIGINS`
- `TRAFFIC_STATE_DIR` (default: `runtime/` in repo root)
- `AOE2_API_BASE_URL` (used by `parse_replay.py` for non-local targets; default `https://api-prodn.aoe2hdbets.com`)
- `LOG_REQUESTS=true` to enable request-line logging (disabled by default in production)
- `ALLOW_UNVERIFIED_BEARER_IDENTITY=true` only for legacy compatibility; keep disabled in production
- `AUTO_CREATE_TABLES=true` (local-only convenience; default is disabled to avoid schema drift)

## Migrations

Apply before restarting backend on VPS/prod:

```bash
alembic upgrade head
```

## Helper scripts

- `watch_replays.py` watches local replay folders and triggers parsing uploads.
- `parse_replay.py` parses replay files and sends JSON to configured API targets.

## Deployment model

- Local MBP -> push `main` -> VPS pull `main` -> restart service.

## Admin bootstrap

Use the helper to inspect/promote admin users:

```bash
python scripts/set_admin.py --list
python scripts/set_admin.py --email you@example.com
```

Other selectors:

```bash
python scripts/set_admin.py --uid <uid>
python scripts/set_admin.py --name "<in-game-name>"
python scripts/set_admin.py --latest
python scripts/set_admin.py --email you@example.com --unset
```
