# api-prodn

Production FastAPI backend for AoE2HDBets.

## Canonical docs

- [TESTING.md](/Users/tonyblum/projects/AoE2HDBets/api-prodn/TESTING.md)

## Responsibilities

- replay ingestion and parsing
- `game_stats` persistence in Postgres
- canonical recent-match recency for public feeds via `played_at` (`played_on` → filename/file-mtime derived → `created_at` → `timestamp`)
- live/non-final replay handling for watcher uploads
- user/admin endpoints
- traffic diagnostics endpoint

## Key replay routes

### `POST /api/replay/upload`

- accepts multipart replay file upload
- parses server-side
- stores replay state in `game_stats`
- supports live/non-final replay iterations
- supports final replay upload after file settlement
- supports `x-api-key` when `INTERNAL_API_KEY` is configured

### `POST /api/parse_replay`

- JSON replay ingestion path (compatible with helper scripts)
- supports `x-api-key` when `INTERNAL_API_KEY` is configured

### `GET /api/traffic`

- traffic diagnostics data
- requires admin bearer token (`ADMIN_TOKEN`)
- uses a short in-process response cache (`TRAFFIC_RESPONSE_CACHE_SECONDS`, default 20s) to avoid rebuilding the full nginx tail + geo summary on every poll
- reads the dedicated AoE2 nginx log first (`/var/log/nginx/aoe2hdbets.access.log`) and falls back to the shared nginx access log only if needed

### `GET /api/game_stats`

- returns final replay rows for public match surfaces
- orders recent matches by canonical `played_at`, not mutable parse/update bookkeeping timestamps
- payload includes `played_at`, `played_on`, `derived_played_on`, `created_at`, and `timestamp`

## Local development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
uvicorn app:app --reload --host 127.0.0.1 --port 3330
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
- `AOE2_TRAFFIC_LOG_PATH` (preferred override for the AoE2 local traffic diagnostics reader; defaults to `/var/log/nginx/aoe2hdbets.access.log` with fallback to `/var/log/nginx/access.log`)
- `TRAFFIC_LOG_PATH` (legacy-compatible fallback override for the same AoE2 diagnostics reader)
- `TRAFFIC_RESPONSE_CACHE_SECONDS` (default: `20`; set `0` to disable the in-process `/api/traffic` response cache)
- `AOE2_API_BASE_URL` (used by `parse_replay.py` for non-local targets; default `https://api-prodn.aoe2hdbets.com`)
- `LOG_REQUESTS=true` to enable request-line logging (disabled by default in production)
- `ENABLE_TRACE_LOGS=true` to emit replay `.trace` files and `trace.index` while debugging replay behavior
- `ALLOW_UNVERIFIED_BEARER_IDENTITY=true` only for legacy compatibility; keep disabled in production
- `AUTO_CREATE_TABLES=true` (local-only convenience; default is disabled to avoid schema drift)

## Trace logging notes

When `ENABLE_TRACE_LOGS=true`, the backend may emit local runtime artifacts such as:

- `*.trace`
- `trace.index`

These are useful while building and debugging replay behavior. They are not deployment assets and can be deleted safely when you want a clean working tree.

## Migrations

Apply before restarting backend on VPS/prod:

```bash
alembic upgrade head
```

## Helper scripts

- `watch_replays.py` watches local replay folders and triggers parsing uploads
- `parse_replay.py` parses replay files and sends JSON to configured API targets

## Deployment model

Local MBP -> push `main` -> VPS pull `main` -> restart service

## Production runtime truth

- VPS repo path: `/var/www/AoE2HDBets/api-prodn`
- service: `aoe2hdbets-api.service`
- production bind: `127.0.0.1:3330`
- production entrypoint: `uvicorn app:app`

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

## Current known edges

- replay/live/final behavior is much healthier than earlier, but still worth documenting carefully as it evolves
- exact postgame achievement-table extraction is still not part of the replay pipeline
- local trace output is expected while building if trace logging is enabled
- `tests/test_fast.py` now skips replay fixtures that are absent from `tests/recs/`; restore DE/HD fixtures if you want that suite to become a hard gate again
