# api-prodn

## Replay completeness and finality contract

Watcher responses expose `raw_replay_archived`, `artifact_accepted`, `parse_completed`, `final_submission_received`, `result_resolved`, `result_trusted`, `stats_eligible`, and `betting_eligible` as separate facts. The legacy `final_accepted` and `should_settle` fields remain settlement signals, not artifact-upload signals. Parse-completeness classes are `live_header`, `live_roster`, `final_result_only`, `final_unparsed_proof`, and `final_unsafe`.

`final_recorded`, `final_recorded_duplicate`, and `final_recorded_refreshed` mean that final replay bytes were archived and the parsed game/result candidate was stored, but the result is not authorized for automatic settlement. Watchers should count these as archived/parsed and routed for correction, not as failed uploads and not as trusted finals. Only `trusted_final*` or `reviewed_match*` with `should_settle=true` permits the settlement path.

Trusted finality—not HTTP success—allows settlement. Disconnect/desync evidence, parser failure, watcher interruption, and silent disappearance are distinct. Unsafe winners never become betting eligible. Missing postgame/achievement values remain absent rather than becoming zeroes.

`utils/replay_team_contract.py` is the canonical replay-player boundary. It normalizes alternate parser names once and preserves replay-observed name, Steam ID, civilization, color, position, explicit team ID (including valid team `0`), player number, winner flag, score, rating snapshot, EAPM, and achievements when present. It never infers team membership from array order. Team games resolve only with exactly two complete equal-size explicit teams. Because HD can flip winner/completion flags after the first teammate resignation, resignation proof resolves a team result only when exactly one full explicit team resigned; the opposing explicit team is then the derived winner. No fully resigned team, both teams resigned, partial resignation, or conflicting flags remain review-only unless independent postgame/scoreboard proof resolves them. `winning_team_id`, `winning_player_keys`, result provenance, confidence, and evidence are stored in `key_events.result_resolution` alongside `key_events.team_resolution`. The legacy scalar `winner` field never establishes team settlement truth.

`utils/replay_engine.py` is the additive Parser Engine Room boundary. One artifact hash plus parser implementation/version, schema version, pass version, and options produces one deterministic idempotency key. Schema/pass v2 returns a candidate-only envelope containing provenance-bearing observations, map/terrain and explicitly initial-header-only object evidence, the immutable parsed action packet stream, deterministic canonical packet identities and multiplicities, research/age-up commands, market/tribute commands, a raw resignation-packet lane, and an earliest-event-per-player semantic resignation lane. Per-player raw-packet and exact-identity-normalized action/EAPM diagnostics are separate; the normalized lane is explicitly experimental and never claimed as validated gameplay truth. `candidate.semantic_sha256` fingerprints that evidence and parser identity; it is deliberately not a checksum of serialized or compressed output bytes. A storage worker must hash the exact stored object separately as its candidate-output hash. Reprocessing never promotes or deletes truth by itself. Candidate output contains a bounded receipt carrying identity, hashes, coverage, and safe summaries, but the production upload route remains projection-only and does not copy that receipt or the full evidence stream into hot `game_stats` JSONB. Workers persist complete candidate output on the mounted replay volume and normalized run/observation tables before any separate promotion decision.

Parser failures are grouped by privacy-safe deterministic signatures (`stage`, category, exception class, normalized message fingerprint). Paths, upload IDs, byte offsets, and other unstable numbers are removed before grouping. A successful header or model fallback remains a recovered candidate and records the primary failure; it does not become stronger result evidence.

Live/final iterations retain their own canonical player evidence in `game_stats`. The app-side session merger prefers complete identity/team fields, keeps earlier complete assignments when a later iteration is incomplete, and blocks conflicting assignments. Multiple watcher orderings are therefore harmless; conflicting team evidence is not. For a team final, `betting_eligible` additionally requires high-confidence teams and a coherent winning team.

Identity precedence is platform match ID, watcher/session identity, normalized filename plus watcher session, then hash/fallback metadata. Parse attempts remain audit evidence while public views collapse iterations and duplicate watcher uploads into one canonical match.

Fixture work belongs under `tests/`; private user replays are not committed. The six HD 5.8 golden files (the five supplied fixtures plus Jim's verified 4v4 regression replay) are pinned by filename, SHA-256, byte size, and expected evidence in `tests/fixtures/hd5_8_golden_manifest.json`. Set `AOE2_HD_GOLDEN_DIR` to the protected corpus directory to run byte-level golden tests. Coverage should include normal/resignation/disconnect finals, incomplete/live files, team games, repeated/multi-watcher iterations, corrupt/unsupported input, missing scoreboard, late finals, and batch duplicates.

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
- falls back to explicit `header_only_summary_fallback` metadata if `mgz.summary` cannot decode a replay but the header remains readable; this preserves watcher proof and player identity without inventing winner/score/economy truth
- falls back to `watcher_final_unparsed` for watcher final uploads that cannot be decoded even at header level; this stores the upload proof, uploader player, replay hash, and filename time while leaving match outcome/economy unknown

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
- `AOE2_API_BASE_URL` (used by `parse_replay.py` for non-local targets; default `https://api-prodn.aoe2war.com`)
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
- `scripts/parse_replay_candidate.py` parses one local artifact directly into the
  deterministic, candidate-only Parser Engine contract without HTTP or database
  writes. Complete action/chat evidence is emitted to canonical JSON; the result
  never promotes itself into effective game truth.

Parse an ordinary replay to stdout:

```bash
python scripts/parse_replay_candidate.py \
  "/path/to/MP Replay v5.8 @2026.07.06 182842.aoe2record"
```

Archive objects may be content-addressed. Supply the original replay name so
filename-derived legacy metadata remains available, verify the immutable digest,
and write the output atomically with private permissions:

```bash
python scripts/parse_replay_candidate.py \
  /mnt/HC_Volume_105319120/aoe2-parser-engine/golden-fixtures/<sha256>.aoe2record \
  --source-name "MP Replay v5.8 @2026.07.06 182842.aoe2record" \
  --expected-sha256 <sha256> \
  --output /mnt/HC_Volume_105319120/aoe2-parser-engine/jobs/manual/candidates/<sha256>.json
```

Use `--receipt-only` only for the compact hot-database receipt. A failed parse
still emits structured JSON and exits with status `2`, allowing a worker to
catalog stable failure signatures without scraping stderr.

Historical manifests run through
`scripts/run_replay_engine_room_job.py`; its plan mode is zero-write and its
candidate mode is bounded, resumable, mounted-volume-backed, and never changes
public truth. After a run, `scripts/report_replay_engine_room_job.py` verifies
the stored candidate bytes again and writes the private per-game reconciliation
equation. See `docs/REPLAY_ENGINE_ROOM_WORKER.md` for the production runbook.

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
- header-only replay fallback rows are useful proof breadcrumbs, not authoritative postgame result rows
- `watcher_final_unparsed` rows are proof rows only; they should be upgraded by a later parse/re-upload, not treated as authoritative result rows
- exact postgame achievement-table extraction is still not part of the replay pipeline
- local trace output is expected while building if trace logging is enabled
- `tests/test_fast.py` now skips replay fixtures that are absent from `tests/recs/`; restore DE/HD fixtures if you want that suite to become a hard gate again
