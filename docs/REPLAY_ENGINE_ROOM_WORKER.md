# Replay Engine Room worker

This worker replays a frozen CSV manifest through the deterministic HD parser
without changing current game truth. It is a private evidence lane: no
`game_stats`, public aggregate, market, bet, or settlement row is written.

## Safety contract

- `plan`/`--dry-run` reads the CSV and every replay byte, validates the
  content-addressed archive path, byte size, SHA-256, row count, extensions,
  duplicates, and IDs, and performs zero filesystem or database writes.
- `candidate` repeats that full reconciliation before opening the write rail.
- The only inserts are immutable rows in `replay_artifacts`,
  `replay_submissions`, `replay_parse_runs`, `replay_observations`,
  `replay_reprocess_jobs`, and `replay_reprocess_job_events`.
- A manifest `game_stats_id` is read back before any insert. Its `replay_hash`
  and submitter UID must match the manifest. The submitter user is derived from
  that linked row, so a Jim manifest produces Jim-owned submission receipts
  even when the CSV has no `submitter_uid` column.
- Submission receipts are cohort-independent: legacy parse attempt when
  available, otherwise linked `game_stats`, otherwise artifact + submitter.
  Overlapping Jim/full-vault jobs therefore reuse and validate the same
  immutable receipt. A repeated artifact/parser identity is verified and
  counted as `skipped`; it is not parsed or inserted again.
- The complete observation catalog remains in the compressed candidate object.
  Postgres stores only material observations: non-`absent` rows plus
  result-critical conflict/inference rows even when their value is null. The
  parse run's `observation_count` is the full emitted count; metrics and final
  job output separately report emitted, persisted, and catalog-only counts.
- Concurrency is deliberately fixed at one. PostgreSQL advisory locks prevent
  two jobs from appending the same job/artifact identity simultaneously.
- Progress is a gapless append-only event stream. An interrupted job appends a
  `paused` event when possible and resumes from the recorded
  `artifact_completed` cursors.
- `--max-artifacts-this-run N` is an invocation-only canary bound. When rows
  remain after the Nth newly accounted row, the worker appends exactly one
  `paused` event carrying the checkpoint cursor and exits `75`. The bound is
  not part of immutable job identity; rerunning the command continues with a
  fresh per-invocation budget.
- Every completion balances exactly:
  `processed = succeeded + failed + skipped = manifest rows`.

## Candidate object format

The complete candidate envelope, including full action/chat evidence, is
canonical JSON compressed with gzip level 9 and `mtime=0`. The worker writes a
mode `0600` temp file, fsyncs it, and atomically hard-links it into its immutable
location:

```text
/mnt/HC_Volume_105319120/aoe2-parser-engine/jobs/
  <job-sha256>/candidates/<artifact[0:2]>/<artifact[2:4]>/<run-sha256>.json.gz
```

`replay_parse_runs.candidate_output_hash` is the SHA-256 of the exact compressed
bytes stored there, and `candidate_output_byte_size` is the compressed size.
The parser's semantic `candidate.semantic_sha256` is a different concept and is
preserved in `replay_parse_runs.metrics.candidate_semantic_sha256`.

On every idempotent resume, an existing object must still:

- have mode `0600`;
- match the DB compressed byte size and exact compressed-byte SHA-256;
- decompress to canonical JSON;
- recompress to the same deterministic gzip bytes;
- name the same artifact, parser run, and semantic hash.

The candidate jobs root must resolve below `/mnt` **and** its nearest existing
parent must be on a different filesystem device than `/`; a missing HC volume
cannot silently turn `/mnt/...` into root-disk output. The worker defaults to
preserving 5 GiB of free volume space and pauses before crossing that reserve.
The minimum configurable reserve is 1 GiB.

Candidate volume space and database/WAL space are separate gates. Before each
artifact/submission transaction and again before a material-observation batch,
the worker checks the filesystem containing `--database-storage-path` (default
`/`). It preserves 3 GiB there by default; `--database-root-reserve-gib` may be
changed but never below 1 GiB. On a VPS where PGDATA is on another filesystem,
point `--database-storage-path` at PGDATA or another existing path on that same
filesystem.

## Commands

Run the no-write gate first:

```bash
cd /var/www/AoE2HDBets/api-prodn
source .venv/bin/activate
python scripts/run_replay_engine_room_job.py \
  --mode plan \
  --manifest /mnt/HC_Volume_105319120/aoe2-parser-engine/reports/<frozen-manifest>.csv \
  --archive-root /mnt/HC_Volume_105319120/aoe2-replay-archive
```

`--dry-run` forces this exact zero-write behavior even if `--mode candidate` is
also supplied.

After the web migration containing the Replay Engine Room tables is deployed,
run the candidate pass with the production DB URL already loaded in the service
environment:

```bash
python scripts/run_replay_engine_room_job.py \
  --mode candidate \
  --manifest /mnt/HC_Volume_105319120/aoe2-parser-engine/reports/<frozen-manifest>.csv \
  --archive-root /mnt/HC_Volume_105319120/aoe2-replay-archive \
  --jobs-root /mnt/HC_Volume_105319120/aoe2-parser-engine/jobs \
  --requested-by-uid <admin-uid> \
  --batch-size 25 \
  --max-artifacts-this-run 25 \
  --database-storage-path / \
  --database-root-reserve-gib 3 \
  --concurrency 1
```

This first invocation is the production canary: it accounts for 25 rows, then
exits `75` with one resumable pause when more rows remain. Measure Postgres
table/index/WAL growth and mounted-volume growth, then rerun the identical
command. It resumes after the recorded cursor and processes the next 25. Remove
or increase `--max-artifacts-this-run` only after the canary measurements are
acceptable; changing this invocation bound does not create a new job.

The manifest hash, archive root, parser identity, parser options, and batch size
form the immutable job identity. A completed job returns its already-recorded
final accounting without inserting another event.

Exit codes:

- `0`: plan passed, or candidate job completed with no parser failures;
- `2`: reconciliation/contract/configuration failure;
- `4`: candidate job completed, but one or more artifacts produced structured
  failed parse runs;
- `75`: safely paused (invocation bound, lock contention, candidate-volume
  reserve, or database/WAL reserve); rerun the same command after checking the
  stated condition.

Do not pass `--no-hd-early-exit-rules` for the frozen baseline unless the intent
is to create a new parser-pass identity. That switch is explicit because parser
options are part of immutable run identity.

## Reconciliation report

After a bounded or completed candidate run, build the private evidence equation
from the verified candidate objects and current effective result ledger:

```bash
python scripts/report_replay_engine_room_job.py \
  --job-id <job-id> \
  --report-root /mnt/HC_Volume_105319120/aoe2-parser-engine/reports \
  --label jim-2025-candidate-reconciliation
```

The reporter opens the database read-only, re-verifies every exact compressed
candidate hash/size/semantic link, and writes mode `0600` JSON plus CSV. It
separates trusted direct results, coherent parser results, private review,
unsupported artifacts, current-result matches, improvements, conflicts, and
human verdicts that must remain authoritative. It also totals action and
observation coverage by provenance. Reporting never promotes a candidate.

## Verification

```bash
python -m py_compile \
  utils/replay_engine_room_worker.py \
  utils/replay_engine_room_reporting.py \
  scripts/run_replay_engine_room_job.py \
  scripts/report_replay_engine_room_job.py
ruff check \
  utils/replay_engine_room_worker.py \
  utils/replay_engine_room_reporting.py \
  scripts/run_replay_engine_room_job.py \
  scripts/report_replay_engine_room_job.py \
  tests/test_replay_engine_room_worker.py \
  tests/test_replay_engine_room_reporting.py
pytest -q \
  tests/test_replay_engine_room_worker.py \
  tests/test_replay_engine_room_reporting.py
```

The production candidate command is intentionally not part of a normal API
service start. Run it only after the read-only plan is clean, the migration is
present, the archive is mounted, and the mounted-volume free-space reserve is
confirmed.
