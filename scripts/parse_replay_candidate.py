#!/usr/bin/env python3
"""Parse one local replay artifact into canonical candidate-only JSON."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import string
import sys
import tempfile
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
API_ROOT = SCRIPT_DIR.parent
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from utils.replay_engine import (  # noqa: E402
    build_candidate_envelope,
    canonical_candidate_json,
    compact_candidate_receipt,
    normalize_failure_signature,
)
from utils.replay_parser import parse_replay_candidate_bytes  # noqa: E402


def _sha256_argument(value: str) -> str:
    normalized = value.strip().casefold()
    if len(normalized) != 64 or any(character not in string.hexdigits for character in normalized):
        raise argparse.ArgumentTypeError("expected a 64-character SHA-256 digest")
    return normalized


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path, help="local .aoe2record/.mgz artifact")
    parser.add_argument(
        "--source-name",
        help=(
            "original replay filename used for filename-derived metadata; useful "
            "when the archive object is named by SHA-256"
        ),
    )
    parser.add_argument(
        "--expected-sha256",
        type=_sha256_argument,
        help="reject the artifact before parsing when its content digest differs",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="output file, written atomically with mode 0600; '-' writes stdout",
    )
    parser.add_argument(
        "--receipt-only",
        action="store_true",
        help="emit the compact hot-database receipt instead of the complete candidate",
    )
    parser.add_argument(
        "--no-hd-early-exit-rules",
        action="store_true",
        help="disable the legacy HD early-exit compatibility option",
    )
    return parser.parse_args(argv)


def _failed_candidate(
    replay_path: str,
    *,
    file_bytes: bytes | None,
    error: BaseException,
    stage: str,
    parse_mode: str,
    apply_hd_early_exit_rules: bool,
) -> dict[str, Any]:
    return build_candidate_envelope(
        replay_path=replay_path,
        file_bytes=file_bytes,
        projection=None,
        evidence=None,
        apply_hd_early_exit_rules=apply_hd_early_exit_rules,
        parse_mode=parse_mode,
        failure=normalize_failure_signature(error, stage=stage),
    )


def parse_local_artifact(args: argparse.Namespace) -> dict[str, Any]:
    artifact_path = args.artifact.expanduser()
    replay_path = args.source_name or str(artifact_path)
    apply_hd_early_exit_rules = not args.no_hd_early_exit_rules

    try:
        file_bytes = artifact_path.read_bytes()
    except OSError as error:
        return _failed_candidate(
            replay_path,
            file_bytes=None,
            error=error,
            stage="artifact_read",
            parse_mode="artifact_io_failed",
            apply_hd_early_exit_rules=apply_hd_early_exit_rules,
        )

    actual_sha256 = hashlib.sha256(file_bytes).hexdigest()
    if args.expected_sha256 and actual_sha256 != args.expected_sha256:
        return _failed_candidate(
            replay_path,
            file_bytes=file_bytes,
            error=ValueError("artifact sha256 does not match expected digest"),
            stage="artifact_integrity",
            parse_mode="artifact_integrity_failed",
            apply_hd_early_exit_rules=apply_hd_early_exit_rules,
        )

    return parse_replay_candidate_bytes(
        replay_path,
        file_bytes,
        apply_hd_early_exit_rules=apply_hd_early_exit_rules,
    )


def _write_output(output: str, payload: str) -> None:
    rendered = payload + "\n"
    if output == "-":
        sys.stdout.write(rendered)
        return

    destination = Path(output).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="\n") as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    candidate = parse_local_artifact(args)
    output: dict[str, Any] = (
        compact_candidate_receipt(candidate) if args.receipt_only else candidate
    )
    _write_output(args.output, canonical_candidate_json(output))
    return 2 if candidate.get("run", {}).get("status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
