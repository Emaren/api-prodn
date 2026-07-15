#!/usr/bin/env python3
"""Plan or run a private, candidate-only Replay Engine Room manifest job."""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import sys
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
API_ROOT = SCRIPT_DIR.parent
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from utils.replay_engine_room_worker import (  # noqa: E402
    DEFAULT_DATABASE_MIN_FREE_BYTES,
    DEFAULT_JOBS_ROOT,
    DEFAULT_MIN_FREE_BYTES,
    CandidateObjectError,
    ReconciliationError,
    WorkerPaused,
    build_job_spec,
    reconcile_frozen_manifest,
    run_candidate_job,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=("plan", "candidate"),
        default="plan",
        help="plan is strictly read-only; candidate appends private Engine Room facts",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="force read-only plan mode even if --mode candidate was supplied",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="required only for candidate mode; never printed",
    )
    parser.add_argument(
        "--jobs-root",
        type=Path,
        default=Path(os.getenv("REPLAY_ENGINE_JOBS_DIR", str(DEFAULT_JOBS_ROOT))),
    )
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument(
        "--max-artifacts-this-run",
        type=int,
        help="pause resumably after this many newly accounted manifest rows",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="reserved for future use; production-safe default and maximum are both 1",
    )
    parser.add_argument(
        "--min-free-gib",
        type=float,
        default=DEFAULT_MIN_FREE_BYTES / 1024**3,
        help="pause before the mounted volume falls below this reserve (minimum 1)",
    )
    parser.add_argument(
        "--database-root-reserve-gib",
        type=float,
        default=DEFAULT_DATABASE_MIN_FREE_BYTES / 1024**3,
        help="minimum free space for the Postgres/WAL filesystem (minimum 1)",
    )
    parser.add_argument(
        "--database-storage-path",
        type=Path,
        default=Path(os.getenv("REPLAY_ENGINE_DATABASE_STORAGE_PATH", "/")),
        help="existing path on the filesystem hosting Postgres/WAL (default /)",
    )
    parser.add_argument(
        "--submitter-uid",
        help="optional assertion; DB-linked game_stats UID remains authoritative",
    )
    parser.add_argument("--requested-by-uid")
    parser.add_argument("--worker-key")
    parser.add_argument(
        "--no-hd-early-exit-rules",
        action="store_true",
        help="changes parser pass identity; keep the default for the frozen HD pass",
    )
    return parser.parse_args()


def print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def main() -> int:
    args = parse_args()
    if args.concurrency != 1:
        raise SystemExit("--concurrency is currently bounded to exactly 1")
    if not math.isfinite(args.min_free_gib) or args.min_free_gib < 1:
        raise SystemExit("--min-free-gib cannot be lower than 1")
    if (
        not math.isfinite(args.database_root_reserve_gib)
        or args.database_root_reserve_gib < 1
    ):
        raise SystemExit("--database-root-reserve-gib cannot be lower than 1")
    if (
        args.max_artifacts_this_run is not None
        and args.max_artifacts_this_run < 1
    ):
        raise SystemExit("--max-artifacts-this-run must be a positive integer")

    try:
        report = reconcile_frozen_manifest(args.manifest, args.archive_root)
    except ReconciliationError as error:
        print_json(
            {
                "mode": "plan",
                "writes_performed": False,
                "ok": False,
                "error": str(error),
            }
        )
        return 2

    effective_mode = "plan" if args.dry_run else args.mode
    if effective_mode == "plan" or not report.ok:
        print_json(report.summary())
        return 0 if report.ok else 2

    if not args.database_url:
        print_json(
            {
                "mode": "candidate",
                "ok": False,
                "error": "DATABASE_URL or --database-url is required",
            }
        )
        return 2

    try:
        spec = build_job_spec(
            report,
            apply_hd_early_exit_rules=not args.no_hd_early_exit_rules,
            batch_size=min(args.batch_size, report.manifest_rows),
        )
        result = run_candidate_job(
            report,
            spec,
            database_url=args.database_url,
            jobs_root=args.jobs_root,
            submitter_uid_override=args.submitter_uid,
            requested_by_uid=args.requested_by_uid,
            worker_key=args.worker_key,
            min_free_bytes=int(args.min_free_gib * 1024**3),
            database_storage_path=args.database_storage_path,
            database_min_free_bytes=int(
                args.database_root_reserve_gib * 1024**3
            ),
            max_artifacts_this_run=args.max_artifacts_this_run,
            apply_hd_early_exit_rules=not args.no_hd_early_exit_rules,
        )
    except WorkerPaused as error:
        print_json(
            {
                "mode": "candidate",
                "ok": False,
                "paused": True,
                "resume_safe": True,
                "error": str(error),
            }
        )
        return 75
    except (ReconciliationError, CandidateObjectError, ValueError) as error:
        print_json(
            {
                "mode": "candidate",
                "ok": False,
                "resume_safe": False,
                "error": str(error),
            }
        )
        return 2
    except RuntimeError as error:
        print_json(
            {
                "mode": "candidate",
                "ok": False,
                "resume_safe": True,
                "error": str(error),
            }
        )
        return 1

    result["ok"] = result["accounting"]["failed"] == 0
    print_json(result)
    return 0 if result["ok"] else 4


if __name__ == "__main__":
    sys.exit(main())
