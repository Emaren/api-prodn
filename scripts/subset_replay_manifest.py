#!/usr/bin/env python3
"""Create an immutable, byte-addressed subset of a frozen replay CSV manifest."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sha256", action="append", default=[])
    parser.add_argument("--sha256-file", type=Path)
    return parser.parse_args()


def requested_hashes(args: argparse.Namespace) -> set[str]:
    values = list(args.sha256)
    if args.sha256_file:
        values.extend(args.sha256_file.read_text(encoding="utf-8").splitlines())
    cleaned = {value.strip().casefold() for value in values if value.strip()}
    if any(not SHA256_RE.fullmatch(value) for value in cleaned):
        raise ValueError("every requested artifact key must be a lowercase SHA-256")
    if not cleaned:
        raise ValueError("at least one artifact SHA-256 is required")
    return cleaned


def main() -> int:
    args = parse_args()
    source = args.manifest.expanduser().resolve()
    destination = args.output.expanduser().resolve()
    if not source.is_file():
        raise SystemExit("source manifest does not exist")
    if destination.exists():
        raise SystemExit("subset manifest already exists; choose a new immutable path")
    selected_hashes = requested_hashes(args)

    with source.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "replay_hash" not in reader.fieldnames:
            raise SystemExit("source manifest has no replay_hash column")
        fieldnames = list(reader.fieldnames)
        rows = [row for row in reader if row.get("replay_hash") in selected_hashes]

    found = {row["replay_hash"] for row in rows}
    missing = selected_hashes - found
    if missing:
        raise SystemExit(f"{len(missing)} requested artifacts are absent from the source manifest")
    if len(rows) != len(selected_hashes):
        raise SystemExit("source manifest contains duplicate rows for a requested artifact")

    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            os.fchmod(handle.fileno(), 0o600)
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise

    payload = destination.read_bytes()
    print(json.dumps({
        "source_manifest": source.name,
        "output": str(destination),
        "rows": len(rows),
        "byte_size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "mode": oct(destination.stat().st_mode & 0o777),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
