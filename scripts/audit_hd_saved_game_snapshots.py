#!/usr/bin/env python3
"""Read-only structural audit for archived AoE2 HD saved-game snapshots."""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
from pathlib import Path
import sys
import zlib

from construct import GreedyBytes, Int32ul, Struct, Tell
from mgz import compressed_header


SCRIPT_DIR = Path(__file__).resolve().parent
API_ROOT = SCRIPT_DIR.parent
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from utils.replay_parser import (  # noqa: E402
    _extract_header_player_rows,
    _patch_mgz_hd_type9_game_type,
)


DEFAULT_ARCHIVE_ROOT = Path("/mnt/HC_Volume_105319120/aoe2-replay-archive")
MAX_DECOMPRESSED_BYTES = 64 * 1024 * 1024
SAVED_GAME_SNAPSHOT = Struct(
    *compressed_header.subcons[:11],
    "save_snapshot_word" / Int32ul,
    *compressed_header.subcons[11:-1],
    "snapshot_offset" / Tell,
    "snapshot_tail" / GreedyBytes,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def bounded_raw_deflate(payload: bytes) -> bytes:
    inflater = zlib.decompressobj(wbits=-15)
    decompressed = inflater.decompress(payload, MAX_DECOMPRESSED_BYTES + 1)
    if len(decompressed) > MAX_DECOMPRESSED_BYTES or inflater.unconsumed_tail:
        raise ValueError("saved-game snapshot exceeds decompression limit")
    decompressed += inflater.flush()
    if len(decompressed) > MAX_DECOMPRESSED_BYTES:
        raise ValueError("saved-game snapshot exceeds decompression limit")
    if not inflater.eof or inflater.unused_data:
        raise ValueError(
            "saved-game snapshot is not one exact raw-deflate stream: "
            f"eof={inflater.eof} unused={len(inflater.unused_data)}"
        )
    return decompressed


def main() -> int:
    args = parse_args()
    if args.start < 0 or args.limit < 1:
        raise SystemExit("--start must be non-negative and --limit must be positive")

    _patch_mgz_hd_type9_game_type()
    with args.manifest.expanduser().open(newline="", encoding="utf-8") as handle:
        source_rows = [
            row
            for row in csv.DictReader(handle)
            if str(row.get("extension") or "").casefold() == ".aoe2mpgame"
        ]
    rows = source_rows[args.start : args.start + args.limit]

    successes = []
    failures = []
    for row in rows:
        try:
            archive_path = (
                args.archive_root.expanduser() / str(row["archive_relative_path"])
            )
            decompressed = bounded_raw_deflate(archive_path.read_bytes())
            parsed = SAVED_GAME_SNAPSHOT.parse(decompressed)
            tail = bytes(parsed.snapshot_tail or b"")
            if tail:
                raise ValueError(f"snapshot parser left {len(tail)} trailing bytes")
            players = _extract_header_player_rows(parsed)
            names = [str(player.get("name") or "").strip() for player in players]
            if (
                not players
                or any(not name for name in names)
                or len({name.casefold() for name in names}) != len(names)
            ):
                raise ValueError(f"snapshot roster is not unique and complete: {names}")
            successes.append(
                {
                    "game_stats_id": int(row["game_stats_id"]),
                    "decompressed_bytes": len(decompressed),
                    "player_count": len(players),
                    "restore_time_ms": int(parsed.initial.restore_time),
                    "version": str(parsed.version),
                    "game_type": str(parsed.lobby.game_type),
                }
            )
        except Exception as error:
            failures.append(
                {
                    "game_stats_id": int(row["game_stats_id"]),
                    "artifact_sha256": row["replay_hash"],
                    "original_filename": row["original_filename"],
                    "error": f"{type(error).__name__}: {error}"[:500],
                }
            )

    print(
        json.dumps(
            {
                "read_only": True,
                "candidate_only": True,
                "manifest_saved_game_rows": len(source_rows),
                "start": args.start,
                "rows": len(rows),
                "ok": len(successes),
                "failed": len(failures),
                "failures": failures,
                "decompressed_bytes": sum(
                    row["decompressed_bytes"] for row in successes
                ),
                "player_counts": dict(
                    sorted(Counter(row["player_count"] for row in successes).items())
                ),
                "restore_time_ms": {
                    "min": min(
                        (row["restore_time_ms"] for row in successes), default=None
                    ),
                    "max": max(
                        (row["restore_time_ms"] for row in successes), default=None
                    ),
                },
                "versions": dict(Counter(row["version"] for row in successes)),
                "game_types": dict(
                    Counter(row["game_type"] for row in successes)
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
