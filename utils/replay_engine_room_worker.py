"""Production-safe, candidate-only replay Engine Room worker primitives.

The worker has two deliberately separate phases:

* reconciliation reads and hashes every manifest artifact without touching the
  database or filesystem state;
* execution appends private Engine Room facts and immutable candidate objects.

It never updates ``game_stats`` or any public/market table.  Candidate files are
canonical JSON compressed with a zero gzip timestamp and addressed by the hash
of the exact compressed bytes stored on disk.
"""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
from typing import Any, Callable, Iterator, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from utils.replay_engine import (
    build_candidate_envelope,
    canonical_candidate_json,
    normalize_failure_signature,
    parser_identity,
)
from utils.replay_parser import parse_replay_candidate_bytes


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_EXTENSION_RE = re.compile(r"^\.[a-z0-9]{1,31}$")
DEFAULT_JOBS_ROOT = Path(
    "/mnt/HC_Volume_105319120/aoe2-parser-engine/jobs"
)
DEFAULT_MIN_FREE_BYTES = 5 * 1024**3
DEFAULT_DATABASE_MIN_FREE_BYTES = 3 * 1024**3
MAX_MANIFEST_ROWS = 100_000
REQUIRED_ENGINE_ROOM_TABLES = {
    "replay_artifacts",
    "replay_submissions",
    "replay_parse_runs",
    "replay_observations",
    "replay_reprocess_jobs",
    "replay_reprocess_job_events",
}
TERMINAL_JOB_EVENTS = {"completed", "failed", "cancelled"}
API_ROOT = Path(__file__).resolve().parents[1]
LOCAL_CANDIDATE_CLI = API_ROOT / "scripts" / "parse_replay_candidate.py"


class ReconciliationError(ValueError):
    """The frozen manifest and archive do not describe identical bytes."""


class CandidateObjectError(RuntimeError):
    """An immutable candidate object is missing, mutable, or corrupt."""


class WorkerPaused(RuntimeError):
    """Execution stopped safely and may be resumed with the same command."""


class InvocationLimitPaused(WorkerPaused):
    """The invocation bound paused a job after its event was already appended."""

    event_already_recorded = True


@dataclass(frozen=True)
class ManifestRow:
    ordinal: int
    cursor: str
    logical_key_hash: str
    game_stats_id: int | None
    legacy_parse_attempt_id: int | None
    replay_hash: str
    original_filename: str
    extension: str | None
    archive_relative_path: str
    archive_path: Path
    byte_size: int
    submitter_uid: str | None


@dataclass(frozen=True)
class ReconciliationIssue:
    ordinal: int | None
    code: str
    detail: str


@dataclass
class ReconciliationReport:
    manifest_path: str
    manifest_sha256: str
    archive_root: str
    manifest_rows: int
    unique_artifacts: int
    duplicate_artifact_rows: int
    total_bytes: int
    extension_counts: dict[str, int]
    rows: list[ManifestRow] = field(repr=False)
    errors: list[ReconciliationIssue] = field(default_factory=list)
    warnings: list[ReconciliationIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def summary(self) -> dict[str, Any]:
        return {
            "mode": "plan",
            "writes_performed": False,
            "ok": self.ok,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "archive_root": self.archive_root,
            "manifest_rows": self.manifest_rows,
            "unique_artifacts": self.unique_artifacts,
            "duplicate_artifact_rows": self.duplicate_artifact_rows,
            "total_bytes": self.total_bytes,
            "extension_counts": self.extension_counts,
            "errors": [asdict(issue) for issue in self.errors],
            "warnings": [asdict(issue) for issue in self.warnings],
        }


@dataclass(frozen=True)
class CandidateObject:
    path: Path
    storage_key: str
    compressed_sha256: str
    compressed_byte_size: int
    canonical_json_sha256: str
    semantic_sha256: str | None
    reused: bool


@dataclass(frozen=True)
class ManifestReference:
    game_stats_id: int | None
    legacy_parse_attempt_id: int | None
    submitter_user_id: int | None
    submitter_uid: str | None


@dataclass(frozen=True)
class JobSpec:
    job_identity_hash: str
    idempotency_key: str
    scope: dict[str, Any]
    scope_hash: str
    parser: dict[str, Any]
    parser_config_hash: str
    batch_size: int
    max_artifacts: int


@dataclass(frozen=True)
class JobAccounting:
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0

    def advanced(self, outcome: str) -> "JobAccounting":
        if outcome not in {"succeeded", "failed", "skipped"}:
            raise ValueError(f"invalid artifact outcome: {outcome}")
        return JobAccounting(
            processed=self.processed + 1,
            succeeded=self.succeeded + (outcome == "succeeded"),
            failed=self.failed + (outcome == "failed"),
            skipped=self.skipped + (outcome == "skipped"),
        )

    def validate(self, expected: int | None = None) -> None:
        if self.processed != self.succeeded + self.failed + self.skipped:
            raise RuntimeError("job accounting equation is not balanced")
        if expected is not None and self.processed != expected:
            raise RuntimeError(
                f"job accounting is incomplete: {self.processed} != {expected}"
            )


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def resolve_submitter_uid_assertion(
    *,
    game_stats_uid: str | None,
    legacy_attempt_uid: str | None,
    manifest_uid: str | None,
    override_uid: str | None,
    manifest_ordinal: int,
) -> str | None:
    """Resolve one submitter UID while treating every supplied value as an assertion."""

    candidates = [
        str(value).strip()
        for value in (
            game_stats_uid,
            legacy_attempt_uid,
            manifest_uid,
            override_uid,
        )
        if value and str(value).strip()
    ]
    if not candidates:
        return None
    derived_uid = candidates[0]
    if any(candidate != derived_uid for candidate in candidates[1:]):
        raise ReconciliationError(
            f"manifest row {manifest_ordinal} submitter UID assertions disagree"
        )
    return derived_uid


def submission_receipt_identity(
    *,
    row: ManifestRow,
    reference: ManifestReference,
) -> tuple[str, str, dict[str, Any]]:
    """Return a cohort-independent immutable submission receipt identity."""

    if reference.legacy_parse_attempt_id is not None:
        identity_basis: dict[str, Any] = {
            "legacy_parse_attempt_id": reference.legacy_parse_attempt_id,
        }
        identity_kind = "legacy_parse_attempt"
    elif reference.game_stats_id is not None:
        identity_basis = {"game_stats_id": reference.game_stats_id}
        identity_kind = "game_stats"
    else:
        identity_basis = {
            "artifact_sha256": row.replay_hash,
            "submitter_uid": reference.submitter_uid,
        }
        identity_kind = "artifact_submitter"
    identity_material = {
        "version": 1,
        "source": "engine_room_manifest",
        "identity_kind": identity_kind,
        **identity_basis,
    }
    idempotency_key = stable_hash(identity_material)
    client_submission_id = f"engine-room:{idempotency_key}"
    transport_metadata = {
        "candidate_only": True,
        "affects_public_aggregates": False,
        "receipt_identity_version": 1,
        "receipt_identity_kind": identity_kind,
        "game_stats_id": reference.game_stats_id,
        "game_stats_linkage": (
            "validated_replay_hash" if reference.game_stats_id is not None else None
        ),
        "legacy_parse_attempt_id": reference.legacy_parse_attempt_id,
        "artifact_sha256": row.replay_hash,
        "submitter_uid": reference.submitter_uid,
    }
    return idempotency_key, client_submission_id, transport_metadata


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _positive_int(value: Any, *, allow_empty: bool = True) -> int | None:
    normalized = str(value or "").strip()
    if not normalized and allow_empty:
        return None
    try:
        parsed = int(normalized)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _manifest_bool(value: Any) -> bool | None:
    normalized = str(value or "").strip().casefold()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    return None


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def reconcile_frozen_manifest(
    manifest_path: Path,
    archive_root: Path,
    *,
    max_rows: int = MAX_MANIFEST_ROWS,
) -> ReconciliationReport:
    """Fully hash a frozen CSV manifest and its archive with zero writes."""

    manifest_path = manifest_path.expanduser().resolve()
    archive_root = archive_root.expanduser().resolve()
    if not manifest_path.is_file():
        raise ReconciliationError(f"manifest does not exist: {manifest_path}")
    if not archive_root.is_dir():
        raise ReconciliationError(f"archive root does not exist: {archive_root}")

    manifest_sha256 = file_sha256(manifest_path)
    errors: list[ReconciliationIssue] = []
    warnings: list[ReconciliationIssue] = []
    rows: list[ManifestRow] = []
    extensions: Counter[str] = Counter()
    artifact_hashes: Counter[str] = Counter()
    logical_hashes: Counter[str] = Counter()

    with manifest_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        if len(fieldnames) != len(set(fieldnames)):
            raise ReconciliationError("manifest contains duplicate column names")
        columns = set(fieldnames)
        required = {"replay_hash", "archive_relative_path"}
        missing_columns = sorted(required - columns)
        if missing_columns:
            raise ReconciliationError(
                "manifest is missing required columns: "
                + ", ".join(missing_columns)
            )
        raw_rows = list(reader)

    if not raw_rows:
        raise ReconciliationError("manifest contains no artifact rows")
    if len(raw_rows) > max_rows:
        raise ReconciliationError(
            f"manifest has {len(raw_rows)} rows; maximum is {max_rows}"
        )

    for ordinal, raw in enumerate(raw_rows, start=1):
        replay_hash = str(raw.get("replay_hash") or "").strip().casefold()
        cursor = f"{ordinal:06d}:{replay_hash[:16] or 'invalid'}"

        def error(code: str, detail: str) -> None:
            errors.append(ReconciliationIssue(ordinal, code, detail))

        if not SHA256_RE.fullmatch(replay_hash):
            error("invalid_replay_hash", "replay_hash must be 64 lowercase hex chars")
            continue

        archive_present_raw = raw.get("archive_present")
        if archive_present_raw not in (None, ""):
            archive_present = _manifest_bool(archive_present_raw)
            if archive_present is not True:
                error("archive_not_present", "archive_present is not true")

        relative_text = str(raw.get("archive_relative_path") or "").strip()
        relative_path = Path(relative_text)
        if not relative_text:
            error("missing_archive_path", "archive_relative_path is empty")
            continue
        if relative_path.is_absolute() or ".." in relative_path.parts:
            error("unsafe_archive_path", "archive path must be relative and traversal-free")
            continue
        archive_path = (archive_root / relative_path).resolve()
        if not _is_within(archive_path, archive_root):
            error("archive_path_escape", "resolved archive path escapes archive root")
            continue
        if not archive_path.is_file():
            error("archive_file_missing", "archive path is not a regular file")
            continue
        if len(str(archive_path)) > 1000:
            error("archive_path_too_long", "archive storage key exceeds 1000 chars")

        suffix = archive_path.suffix.casefold()
        if archive_path.name.casefold() != f"{replay_hash}{suffix}":
            error(
                "archive_locator_mismatch",
                "archive filename is not the replay hash plus its extension",
            )
        relative_parts = relative_path.parts
        if (
            len(relative_parts) != 3
            or relative_parts[0].casefold() != replay_hash[:2]
            or relative_parts[1].casefold() != replay_hash[2:4]
        ):
            error(
                "archive_layout_mismatch",
                "archive path must use <sha[0:2]>/<sha[2:4]>/<sha><ext>",
            )

        stat_size = archive_path.stat().st_size
        if stat_size <= 0:
            error("empty_archive_file", "archive byte size must be positive")
        declared_size_raw = str(raw.get("archive_bytes") or "").strip()
        if declared_size_raw:
            declared_size = _positive_int(declared_size_raw, allow_empty=False)
            if declared_size is None:
                error("invalid_archive_bytes", "archive_bytes must be a positive integer")
            elif declared_size != stat_size:
                error(
                    "archive_size_mismatch",
                    f"manifest bytes {declared_size} do not equal file bytes {stat_size}",
                )

        actual_hash = file_sha256(archive_path)
        if actual_hash != replay_hash:
            error(
                "archive_hash_mismatch",
                f"file content hash is {actual_hash}",
            )

        game_stats_raw = str(raw.get("game_stats_id") or "").strip()
        game_stats_id = _positive_int(game_stats_raw)
        if game_stats_raw and game_stats_id is None:
            error("invalid_game_stats_id", "game_stats_id must be a positive integer")

        legacy_raw = str(raw.get("legacy_parse_attempt_id") or "").strip()
        legacy_id = _positive_int(legacy_raw)
        if legacy_raw and legacy_id is None:
            error(
                "invalid_legacy_parse_attempt_id",
                "legacy_parse_attempt_id must be a positive integer",
            )

        original_filename = str(raw.get("original_filename") or "").strip()
        if len(original_filename) > 255:
            error("filename_too_long", "original_filename exceeds 255 characters")
        submitter_uid = str(raw.get("submitter_uid") or "").strip() or None
        if submitter_uid and len(submitter_uid) > 100:
            error("submitter_uid_too_long", "submitter_uid exceeds 100 characters")

        declared_extension = str(raw.get("extension") or "").strip().casefold()
        if declared_extension and suffix and declared_extension != suffix:
            error(
                "archive_extension_mismatch",
                "manifest extension differs from content-addressed archive path",
            )
        extension = declared_extension or Path(original_filename).suffix.casefold() or suffix
        if extension and not SAFE_EXTENSION_RE.fullmatch(extension):
            error("invalid_extension", "extension must be a short dot-prefixed token")
            extension = None

        logical_key = str(raw.get("logical_replay_key") or "").strip()
        logical_key_hash = stable_hash(
            {"logical_replay_key": logical_key}
            if logical_key
            else {"manifest_ordinal": ordinal}
        )
        artifact_hashes[replay_hash] += 1
        logical_hashes[logical_key_hash] += 1
        extensions[extension or "<none>"] += 1
        rows.append(
            ManifestRow(
                ordinal=ordinal,
                cursor=cursor,
                logical_key_hash=logical_key_hash,
                game_stats_id=game_stats_id,
                legacy_parse_attempt_id=legacy_id,
                replay_hash=replay_hash,
                original_filename=original_filename,
                extension=extension,
                archive_relative_path=relative_text,
                archive_path=archive_path,
                byte_size=stat_size,
                submitter_uid=submitter_uid,
            )
        )

    duplicate_rows = sum(count - 1 for count in artifact_hashes.values() if count > 1)
    if duplicate_rows:
        warnings.append(
            ReconciliationIssue(
                None,
                "duplicate_artifact_rows",
                f"{duplicate_rows} manifest rows reuse an artifact hash and will be accounted as skipped after its first run",
            )
        )
    duplicate_logical = sum(count - 1 for count in logical_hashes.values() if count > 1)
    if duplicate_logical:
        warnings.append(
            ReconciliationIssue(
                None,
                "duplicate_logical_rows",
                f"{duplicate_logical} manifest rows reuse a logical key",
            )
        )

    return ReconciliationReport(
        manifest_path=str(manifest_path),
        manifest_sha256=manifest_sha256,
        archive_root=str(archive_root),
        manifest_rows=len(raw_rows),
        unique_artifacts=len(artifact_hashes),
        duplicate_artifact_rows=duplicate_rows,
        total_bytes=sum(row.byte_size for row in rows),
        extension_counts=dict(sorted(extensions.items())),
        rows=rows,
        errors=errors,
        warnings=warnings,
    )


def build_job_spec(
    report: ReconciliationReport,
    *,
    apply_hd_early_exit_rules: bool,
    batch_size: int,
    parser_identity_override: Mapping[str, Any] | None = None,
) -> JobSpec:
    if not report.ok:
        raise ReconciliationError("cannot build a job from a failed reconciliation")
    if batch_size < 1 or batch_size > min(500, report.manifest_rows):
        raise ValueError("batch_size must be between 1 and min(500, manifest rows)")
    identity = dict(parser_identity_override) if parser_identity_override else parser_identity(
        apply_hd_early_exit_rules=apply_hd_early_exit_rules
    )
    required_identity = {
        "implementation",
        "implementation_version",
        "schema_version",
        "pass_name",
        "pass_version",
        "options",
    }
    if not required_identity.issubset(identity):
        raise ValueError("parser identity override is incomplete")
    if not isinstance(identity.get("options"), Mapping):
        raise ValueError("parser identity options must be an object")
    if identity["options"].get("apply_hd_early_exit_rules") is not bool(
        apply_hd_early_exit_rules
    ):
        raise ValueError("parser identity options differ from the requested pass")
    parser_config_hash = stable_hash(identity.get("options") or {})
    scope = {
        "version": 1,
        "kind": "frozen_csv_manifest",
        "manifest_sha256": report.manifest_sha256,
        "manifest_filename": Path(report.manifest_path).name,
        "manifest_rows": report.manifest_rows,
        "unique_artifacts": report.unique_artifacts,
        "archive_root": report.archive_root,
    }
    scope_hash = stable_hash(scope)
    job_identity_hash = stable_hash(
        {
            "scope_hash": scope_hash,
            "parser": identity,
            "batch_size": batch_size,
            "max_attempts_per_artifact": 1,
            "candidate_only": True,
            "affects_public_aggregates": False,
        }
    )
    return JobSpec(
        job_identity_hash=job_identity_hash,
        idempotency_key=f"replay-engine-room:{job_identity_hash}",
        scope=scope,
        scope_hash=scope_hash,
        parser=identity,
        parser_config_hash=parser_config_hash,
        batch_size=batch_size,
        max_artifacts=report.manifest_rows,
    )


def _external_parser_environment() -> dict[str, str]:
    """Return the deliberately tiny environment inherited by parser subprocesses."""
    environment = {
        "PYTHONPATH": str(API_ROOT),
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUNBUFFERED": "1",
    }
    if os.environ.get("PATH"):
        environment["PATH"] = os.environ["PATH"]
    return environment


def validate_external_parser_python(value: Path) -> Path:
    interpreter = value.expanduser().resolve()
    if not interpreter.is_file() or not os.access(interpreter, os.X_OK):
        raise ValueError("external parser Python must be an executable file")
    if not LOCAL_CANDIDATE_CLI.is_file():
        raise ValueError("local candidate parser CLI is missing")
    return interpreter


def load_external_parser_identity(
    python_executable: Path,
    *,
    apply_hd_early_exit_rules: bool,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    """Probe an isolated parser runtime without inheriting application secrets."""
    interpreter = validate_external_parser_python(python_executable)
    if timeout_seconds < 1 or timeout_seconds > 300:
        raise ValueError("external parser identity timeout must be between 1 and 300 seconds")
    expression = (
        "import json; from utils.replay_engine import parser_identity; "
        "print(json.dumps(parser_identity(apply_hd_early_exit_rules="
        f"{bool(apply_hd_early_exit_rules)!r}), sort_keys=True))"
    )
    try:
        completed = subprocess.run(
            [str(interpreter), "-c", expression],
            cwd=API_ROOT,
            env=_external_parser_environment(),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise ValueError("external parser identity probe timed out") from error
    if completed.returncode != 0:
        raise ValueError("external parser identity probe failed")
    try:
        identity = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise ValueError("external parser identity probe returned invalid JSON") from error
    if not isinstance(identity, dict):
        raise ValueError("external parser identity probe returned no identity object")
    return identity


def external_candidate_parser(
    python_executable: Path,
    *,
    timeout_seconds: int = 900,
) -> Callable[[str, bytes, bool], dict[str, Any]]:
    """Build a worker callable backed by one explicitly isolated parser runtime."""
    interpreter = validate_external_parser_python(python_executable)
    if timeout_seconds < 10 or timeout_seconds > 3600:
        raise ValueError("external parser timeout must be between 10 and 3600 seconds")

    def parse(
        replay_path: str,
        file_bytes: bytes,
        apply_hd_early_exit_rules: bool,
    ) -> dict[str, Any]:
        artifact_path = Path(replay_path).expanduser().resolve()
        expected_sha256 = hashlib.sha256(file_bytes).hexdigest()
        command = [
            str(interpreter),
            str(LOCAL_CANDIDATE_CLI),
            str(artifact_path),
            "--expected-sha256",
            expected_sha256,
            "--source-name",
            str(artifact_path),
        ]
        if not apply_hd_early_exit_rules:
            command.append("--no-hd-early-exit-rules")
        try:
            completed = subprocess.run(
                command,
                cwd=API_ROOT,
                env=_external_parser_environment(),
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError("external candidate parser timed out") from error
        # Exit 2 is the candidate CLI's structured parse-failure result. Its
        # stdout still carries a valid immutable failed candidate envelope.
        if completed.returncode not in {0, 2}:
            raise RuntimeError("external candidate parser process failed")
        try:
            candidate = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise RuntimeError("external candidate parser returned invalid JSON") from error
        if not isinstance(candidate, dict):
            raise RuntimeError("external candidate parser returned no candidate object")
        return candidate

    return parse


def _deterministic_gzip_bytes(canonical: bytes) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        compresslevel=9,
        fileobj=buffer,
        mtime=0,
    ) as handle:
        handle.write(canonical)
    return buffer.getvalue()


def deterministic_gzip(candidate: Mapping[str, Any]) -> tuple[bytes, bytes]:
    canonical = canonical_candidate_json(dict(candidate)).encode("utf-8")
    compressed = _deterministic_gzip_bytes(canonical)
    return canonical, compressed


def candidate_object_path(
    jobs_root: Path,
    job_identity_hash: str,
    artifact_sha256: str,
    run_idempotency_key: str,
) -> Path:
    if not all(
        SHA256_RE.fullmatch(value)
        for value in (job_identity_hash, artifact_sha256, run_idempotency_key)
    ):
        raise ValueError("candidate path components must be lowercase SHA-256 hashes")
    path = (
        jobs_root
        / job_identity_hash
        / "candidates"
        / artifact_sha256[:2]
        / artifact_sha256[2:4]
        / f"{run_idempotency_key}.json.gz"
    )
    if len(str(path)) > 1000:
        raise ValueError("candidate output storage key exceeds 1000 characters")
    return path


def validate_jobs_root(jobs_root: Path) -> Path:
    resolved = jobs_root.expanduser().resolve()
    mounted_root = Path("/mnt").resolve()
    if not _is_within(resolved, mounted_root) or resolved == mounted_root:
        raise ValueError(
            "candidate jobs root must resolve beneath /mnt; root-disk output is forbidden"
        )
    existing_parent = _nearest_existing_parent(resolved)
    if existing_parent.stat().st_dev == Path("/").stat().st_dev:
        raise ValueError(
            "candidate jobs root is not backed by a separate mounted filesystem"
        )
    return resolved


def _nearest_existing_parent(path: Path) -> Path:
    current = path
    while not current.exists():
        if current.parent == current:
            raise CandidateObjectError(f"no existing parent for {path}")
        current = current.parent
    return current


def require_storage_reserve(
    path: Path,
    *,
    min_free_bytes: int,
    pending_bytes: int = 0,
) -> None:
    free = shutil.disk_usage(_nearest_existing_parent(path)).free
    required = min_free_bytes + max(0, pending_bytes)
    if free < required:
        raise WorkerPaused(
            f"mounted replay volume has {free} free bytes; {required} required"
        )


def require_database_storage_reserve(
    storage_path: Path,
    *,
    min_free_bytes: int,
) -> None:
    """Protect the filesystem hosting Postgres/WAL before write-heavy appends."""

    if min_free_bytes < 1024**3:
        raise ValueError("database storage reserve cannot be lower than 1 GiB")
    resolved = storage_path.expanduser().resolve()
    if not resolved.exists():
        raise ValueError(f"database storage reserve path does not exist: {resolved}")
    free = shutil.disk_usage(resolved).free
    if free < min_free_bytes:
        raise WorkerPaused(
            f"database storage filesystem has {free} free bytes; "
            f"{min_free_bytes} required"
        )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_private_directory_tree(path: Path, private_root: Path | None) -> None:
    if private_root is None:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.chmod(0o700)
        return
    private_root = private_root.resolve()
    path = path.resolve()
    if not _is_within(path, private_root):
        raise CandidateObjectError("candidate path escapes its private jobs root")
    private_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    private_root.chmod(0o700)
    current = private_root
    for part in path.relative_to(private_root).parts:
        current = current / part
        current.chmod(0o700)


def verify_candidate_object(
    path: Path,
    *,
    expected_compressed_sha256: str,
    expected_compressed_byte_size: int,
    expected_artifact_sha256: str | None = None,
    expected_run_idempotency_key: str | None = None,
    expected_semantic_sha256: str | None = None,
) -> dict[str, Any]:
    """Verify exact stored bytes, canonical payload, contract linkage, and mode."""

    if not path.is_file():
        raise CandidateObjectError(f"candidate object is missing: {path}")
    mode = path.stat().st_mode & 0o777
    if mode != 0o600:
        raise CandidateObjectError(
            f"candidate object mode is {oct(mode)}, expected 0o600: {path}"
        )
    compressed = path.read_bytes()
    if len(compressed) != expected_compressed_byte_size:
        raise CandidateObjectError("candidate compressed byte size does not match")
    compressed_hash = hashlib.sha256(compressed).hexdigest()
    if compressed_hash != expected_compressed_sha256:
        raise CandidateObjectError("candidate compressed byte hash does not match")
    try:
        canonical = gzip.decompress(compressed)
        envelope = json.loads(canonical)
    except (gzip.BadGzipFile, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CandidateObjectError("candidate object is not valid gzip JSON") from error
    if canonical_json_bytes(envelope) != canonical:
        raise CandidateObjectError("candidate JSON is not canonical")
    if _deterministic_gzip_bytes(canonical) != compressed:
        raise CandidateObjectError("candidate gzip bytes are not deterministic")

    artifact = envelope.get("artifact") if isinstance(envelope, dict) else {}
    run = envelope.get("run") if isinstance(envelope, dict) else {}
    candidate = envelope.get("candidate") if isinstance(envelope, dict) else {}
    if expected_artifact_sha256 and artifact.get("sha256") != expected_artifact_sha256:
        raise CandidateObjectError("candidate artifact hash does not match")
    if (
        expected_run_idempotency_key
        and run.get("idempotency_key") != expected_run_idempotency_key
    ):
        raise CandidateObjectError("candidate run identity does not match")
    if (
        expected_semantic_sha256 is not None
        and candidate.get("semantic_sha256") != expected_semantic_sha256
    ):
        raise CandidateObjectError("candidate semantic hash does not match")
    return envelope


def store_candidate_object(
    path: Path,
    candidate: Mapping[str, Any],
    *,
    min_free_bytes: int,
    private_root: Path | None = None,
) -> CandidateObject:
    """Link a complete temp object into place; never replace an existing object."""

    canonical, compressed = deterministic_gzip(candidate)
    compressed_hash = hashlib.sha256(compressed).hexdigest()
    canonical_hash = hashlib.sha256(canonical).hexdigest()
    candidate_state = candidate.get("candidate")
    semantic_hash = (
        candidate_state.get("semantic_sha256")
        if isinstance(candidate_state, Mapping)
        else None
    )
    if path.exists():
        verify_candidate_object(
            path,
            expected_compressed_sha256=compressed_hash,
            expected_compressed_byte_size=len(compressed),
            expected_artifact_sha256=(candidate.get("artifact") or {}).get("sha256"),
            expected_run_idempotency_key=(candidate.get("run") or {}).get(
                "idempotency_key"
            ),
            expected_semantic_sha256=semantic_hash,
        )
        return CandidateObject(
            path=path,
            storage_key=str(path),
            compressed_sha256=compressed_hash,
            compressed_byte_size=len(compressed),
            canonical_json_sha256=canonical_hash,
            semantic_sha256=semantic_hash,
            reused=True,
        )

    require_storage_reserve(
        path,
        min_free_bytes=min_free_bytes,
        pending_bytes=len(compressed),
    )
    _ensure_private_directory_tree(path.parent, private_root)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=".candidate-",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            os.chmod(temporary_path, 0o600)
            handle.write(compressed)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, path)
            _fsync_directory(path.parent)
        except FileExistsError:
            pass
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    verify_candidate_object(
        path,
        expected_compressed_sha256=compressed_hash,
        expected_compressed_byte_size=len(compressed),
        expected_artifact_sha256=(candidate.get("artifact") or {}).get("sha256"),
        expected_run_idempotency_key=(candidate.get("run") or {}).get(
            "idempotency_key"
        ),
        expected_semantic_sha256=semantic_hash,
    )
    return CandidateObject(
        path=path,
        storage_key=str(path),
        compressed_sha256=compressed_hash,
        compressed_byte_size=len(compressed),
        canonical_json_sha256=canonical_hash,
        semantic_sha256=semantic_hash,
        reused=False,
    )


def normalize_observations(
    observations: Sequence[Mapping[str, Any]],
    *,
    run_idempotency_key: str,
) -> list[dict[str, Any]]:
    """Normalize only material DB observations; full catalog stays in output.

    Explicit ``absent`` catalog entries are valuable provenance, but persisting
    hundreds of null rows per replay would waste the small Postgres volume. A
    result-critical conflict/inference remains material even when its value is
    null so operator review never loses the reason a result needs attention.
    """

    normalized: list[dict[str, Any]] = []
    for ordinal, observation in enumerate(observations, start=1):
        field_path = str(observation.get("field") or "").strip()
        if not field_path or len(field_path) > 255:
            raise ValueError("observation field path is missing or exceeds 255 chars")
        subject = observation.get("subject") or {"type": "game"}
        value = observation.get("value")
        provenance_class = str(
            observation.get("provenance_class") or "unspecified"
        )
        conflict_state = str(observation.get("conflict_state") or "none")
        result_critical = (
            field_path.startswith("result.")
            or field_path.startswith("teams.")
            or field_path in {"player.team_id", "player.winner", "game.completed"}
        )
        preserve_review_signal = result_critical and (
            provenance_class == "inferred_review_only"
            or conflict_state != "none"
        )
        if provenance_class == "absent" and not preserve_review_signal:
            continue
        complete_hash = stable_hash(observation)
        root_kind = re.sub(r"[^a-z0-9_]+", "_", field_path.split(".", 1)[0].casefold())
        observation_kind = (root_kind or "field")[:40]
        observation_key = (
            f"{observation_kind}:{ordinal:05d}:{complete_hash}"
        )
        value_hash = stable_hash({"subject": subject, "value": value})
        exact = bool(observation.get("exact"))
        if not exact or provenance_class == "inferred_review_only":
            confidence_bps = None
        elif provenance_class == "derived_coherent":
            confidence_bps = 9000
        else:
            confidence_bps = 10000
        provenance = {
            "subject": subject,
            "class": provenance_class,
            "evidence_source": observation.get("evidence_source"),
            "exact": exact,
            "conflict_state": conflict_state,
            "candidate_observation_sha256": complete_hash,
            "candidate_ordinal": ordinal,
        }
        normalized.append(
            {
                "idempotency_key": stable_hash(
                    {
                        "run_idempotency_key": run_idempotency_key,
                        "observation_key": observation_key,
                    }
                ),
                "observation_key": observation_key,
                "observation_kind": observation_kind,
                "field_path": field_path,
                "value": value,
                "value_hash": value_hash,
                "confidence_bps": confidence_bps,
                "provenance": provenance,
            }
        )
    return normalized


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def normalize_database_url(value: str) -> str:
    return value.replace("postgresql+asyncpg://", "postgresql://", 1)


def _database_failure_signature(value: Any) -> str | None:
    if not value:
        return None
    signature = str(value)
    if len(signature) <= 128:
        return signature
    return f"candidate_failure:{hashlib.sha256(signature.encode('utf-8')).hexdigest()}"


class EngineRoomRepository:
    """Narrow psycopg repository restricted to private Engine Room appends."""

    def __init__(self, database_url: str):
        self.connection = psycopg.connect(
            normalize_database_url(database_url),
            autocommit=True,
            row_factory=dict_row,
        )

    def close(self) -> None:
        self.connection.close()

    def verify_schema(self) -> None:
        rows = self.connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = ANY(%s)
            """,
            (list(sorted(REQUIRED_ENGINE_ROOM_TABLES)),),
        ).fetchall()
        present = {row["table_name"] for row in rows}
        missing = sorted(REQUIRED_ENGINE_ROOM_TABLES - present)
        if missing:
            raise RuntimeError(
                "Replay Engine Room migration is not deployed; missing tables: "
                + ", ".join(missing)
            )

    @contextmanager
    def session_lock(self, key: str, *, wait: bool = False) -> Iterator[None]:
        function = "pg_advisory_lock" if wait else "pg_try_advisory_lock"
        row = self.connection.execute(
            f"SELECT {function}(hashtextextended(%s, 0)) AS acquired",
            (key,),
        ).fetchone()
        if not wait and not bool(row and row["acquired"]):
            raise WorkerPaused(f"another worker holds lock {stable_hash(key)[:12]}")
        try:
            yield
        finally:
            self.connection.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                (key,),
            )

    def resolve_manifest_references(
        self,
        rows: Sequence[ManifestRow],
        *,
        submitter_uid_override: str | None,
    ) -> dict[str, ManifestReference]:
        """Validate DB links before any Engine Room row or object is created."""

        game_ids = sorted(
            {row.game_stats_id for row in rows if row.game_stats_id is not None}
        )
        attempt_ids = sorted(
            {
                row.legacy_parse_attempt_id
                for row in rows
                if row.legacy_parse_attempt_id is not None
            }
        )
        game_rows: dict[int, Mapping[str, Any]] = {}
        if game_ids:
            found = self.connection.execute(
                """
                SELECT id, user_uid, replay_hash
                FROM game_stats
                WHERE id = ANY(%s)
                """,
                (game_ids,),
            ).fetchall()
            game_rows = {int(row["id"]): row for row in found}
            missing = sorted(set(game_ids) - set(game_rows))
            if missing:
                raise ReconciliationError(
                    f"manifest references missing game_stats ids: {missing[:20]}"
                )

        attempt_rows: dict[int, Mapping[str, Any]] = {}
        if attempt_ids:
            found = self.connection.execute(
                """
                SELECT id, user_uid, replay_hash, game_stats_id
                FROM replay_parse_attempts
                WHERE id = ANY(%s)
                """,
                (attempt_ids,),
            ).fetchall()
            attempt_rows = {int(row["id"]): row for row in found}
            missing = sorted(set(attempt_ids) - set(attempt_rows))
            if missing:
                raise ReconciliationError(
                    "manifest references missing replay_parse_attempt ids: "
                    f"{missing[:20]}"
                )

        uid_candidates = {
            uid
            for row in rows
            for uid in (row.submitter_uid, submitter_uid_override)
            if uid
        }
        uid_candidates.update(
            str(game["user_uid"])
            for game in game_rows.values()
            if game.get("user_uid")
        )
        uid_candidates.update(
            str(attempt["user_uid"])
            for attempt in attempt_rows.values()
            if attempt.get("user_uid")
        )
        user_rows: dict[str, int] = {}
        if uid_candidates:
            found = self.connection.execute(
                "SELECT id, uid FROM users WHERE uid = ANY(%s)",
                (sorted(uid_candidates),),
            ).fetchall()
            user_rows = {str(row["uid"]): int(row["id"]) for row in found}

        result: dict[str, ManifestReference] = {}
        for row in rows:
            game = game_rows.get(row.game_stats_id or -1)
            attempt = attempt_rows.get(row.legacy_parse_attempt_id or -1)
            game_uid = str(game.get("user_uid") or "") if game else ""
            attempt_uid = str(attempt.get("user_uid") or "") if attempt else ""
            declared_uid = row.submitter_uid

            if game:
                database_hash = str(game.get("replay_hash") or "").casefold()
                if database_hash != row.replay_hash:
                    raise ReconciliationError(
                        f"manifest row {row.ordinal} replay hash does not match game_stats"
                    )
            if attempt:
                attempt_hash = str(attempt.get("replay_hash") or "").casefold()
                if attempt_hash != row.replay_hash:
                    raise ReconciliationError(
                        f"manifest row {row.ordinal} replay hash does not match legacy attempt"
                    )
                attempt_game_id = attempt.get("game_stats_id")
                if (
                    row.game_stats_id is not None
                    and attempt_game_id is not None
                    and int(attempt_game_id) != row.game_stats_id
                ):
                    raise ReconciliationError(
                        f"manifest row {row.ordinal} attempt/game_stats link differs"
                    )

            derived_uid = resolve_submitter_uid_assertion(
                game_stats_uid=game_uid or None,
                legacy_attempt_uid=attempt_uid or None,
                manifest_uid=declared_uid,
                override_uid=submitter_uid_override,
                manifest_ordinal=row.ordinal,
            )
            if derived_uid and derived_uid not in user_rows:
                raise ReconciliationError(
                    f"manifest row {row.ordinal} submitter UID has no users row"
                )
            result[row.cursor] = ManifestReference(
                game_stats_id=row.game_stats_id,
                legacy_parse_attempt_id=row.legacy_parse_attempt_id,
                submitter_user_id=user_rows.get(derived_uid) if derived_uid else None,
                submitter_uid=derived_uid or None,
            )
        return result

    def resolve_requester(self, uid: str | None) -> int | None:
        if not uid:
            return None
        row = self.connection.execute(
            "SELECT id FROM users WHERE uid = %s",
            (uid,),
        ).fetchone()
        if not row:
            raise ReconciliationError("requested-by UID has no users row")
        return int(row["id"])

    def create_or_load_job(
        self,
        spec: JobSpec,
        *,
        requested_by_user_id: int | None,
    ) -> dict[str, Any]:
        created = False
        with self.connection.transaction():
            row = self.connection.execute(
                """
                INSERT INTO replay_reprocess_jobs (
                    requested_by_user_id, idempotency_key, job_identity_hash,
                    scope_kind, scope, scope_hash, parser_name, parser_version,
                    pass_name, pass_version, parser_config_hash, batch_size,
                    max_artifacts, max_attempts_per_artifact, dry_run,
                    candidate_only, affects_public_aggregates
                ) VALUES (
                    %s, %s, %s, 'frozen_csv_manifest', %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, 1, FALSE, TRUE, FALSE
                )
                ON CONFLICT (job_identity_hash) DO NOTHING
                RETURNING *
                """,
                (
                    requested_by_user_id,
                    spec.idempotency_key,
                    spec.job_identity_hash,
                    Jsonb(spec.scope),
                    spec.scope_hash,
                    spec.parser["implementation"],
                    spec.parser["implementation_version"],
                    spec.parser["pass_name"],
                    spec.parser["pass_version"],
                    spec.parser_config_hash,
                    spec.batch_size,
                    spec.max_artifacts,
                ),
            ).fetchone()
            if row:
                created = True
            else:
                row = self.connection.execute(
                    """
                    SELECT * FROM replay_reprocess_jobs
                    WHERE job_identity_hash = %s
                    """,
                    (spec.job_identity_hash,),
                ).fetchone()
            if not row:
                raise RuntimeError("failed to create or reload replay job")
            immutable_expected = {
                "idempotency_key": spec.idempotency_key,
                "scope_hash": spec.scope_hash,
                "parser_name": spec.parser["implementation"],
                "parser_version": spec.parser["implementation_version"],
                "pass_name": spec.parser["pass_name"],
                "pass_version": spec.parser["pass_version"],
                "parser_config_hash": spec.parser_config_hash,
                "batch_size": spec.batch_size,
                "max_artifacts": spec.max_artifacts,
                "dry_run": False,
                "candidate_only": True,
                "affects_public_aggregates": False,
            }
            for key, expected in immutable_expected.items():
                if row[key] != expected:
                    raise RuntimeError(
                        f"existing replay job differs at immutable field {key}"
                    )
            if created:
                self._append_event_in_transaction(
                    job_id=int(row["id"]),
                    event_type="queued",
                    accounting=JobAccounting(),
                    detail={
                        "manifest_sha256": spec.scope["manifest_sha256"],
                        "manifest_rows": spec.max_artifacts,
                        "candidate_only": True,
                        "affects_public_aggregates": False,
                    },
                )
        return dict(row)

    def job_state(
        self, job_id: int
    ) -> tuple[dict[str, Any], JobAccounting, set[str]]:
        latest = self.connection.execute(
            """
            SELECT * FROM replay_reprocess_job_events
            WHERE job_id = %s
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (job_id,),
        ).fetchone()
        if not latest:
            raise RuntimeError("replay job has no queued event")
        cursor_rows = self.connection.execute(
            """
            SELECT detail->>'manifest_cursor' AS cursor
            FROM replay_reprocess_job_events
            WHERE job_id = %s AND event_type = 'artifact_completed'
            ORDER BY sequence
            """,
            (job_id,),
        ).fetchall()
        cursors = {str(row["cursor"]) for row in cursor_rows if row.get("cursor")}
        accounting = JobAccounting(
            processed=int(latest["processed_count"]),
            succeeded=int(latest["succeeded_count"]),
            failed=int(latest["failed_count"]),
            skipped=int(latest["skipped_count"]),
        )
        accounting.validate()
        if len(cursors) != accounting.processed:
            raise RuntimeError(
                "artifact_completed cursor count does not match job accounting"
            )
        return dict(latest), accounting, cursors

    def job_observation_accounting(self, job_id: int) -> dict[str, int]:
        row = self.connection.execute(
            """
            SELECT
              COALESCE(SUM((detail->>'emitted_observation_count')::integer), 0)
                AS emitted,
              COALESCE(SUM((detail->>'persisted_observation_count')::integer), 0)
                AS persisted
            FROM replay_reprocess_job_events
            WHERE job_id = %s AND event_type = 'artifact_completed'
            """,
            (job_id,),
        ).fetchone()
        emitted = int(row["emitted"] if row else 0)
        persisted = int(row["persisted"] if row else 0)
        return {
            "emitted": emitted,
            "persisted": persisted,
            "catalog_only": emitted - persisted,
        }

    def append_event(
        self,
        *,
        job_id: int,
        event_type: str,
        accounting: JobAccounting,
        detail: Mapping[str, Any] | None = None,
        artifact_id: int | None = None,
        parse_run_id: int | None = None,
        worker_key: str | None = None,
        checkpoint_cursor: str | None = None,
        attempt_number: int = 0,
    ) -> dict[str, Any]:
        with self.connection.transaction():
            return self._append_event_in_transaction(
                job_id=job_id,
                event_type=event_type,
                accounting=accounting,
                detail=detail,
                artifact_id=artifact_id,
                parse_run_id=parse_run_id,
                worker_key=worker_key,
                checkpoint_cursor=checkpoint_cursor,
                attempt_number=attempt_number,
            )

    def _append_event_in_transaction(
        self,
        *,
        job_id: int,
        event_type: str,
        accounting: JobAccounting,
        detail: Mapping[str, Any] | None = None,
        artifact_id: int | None = None,
        parse_run_id: int | None = None,
        worker_key: str | None = None,
        checkpoint_cursor: str | None = None,
        attempt_number: int = 0,
    ) -> dict[str, Any]:
        latest = self.connection.execute(
            """
            SELECT * FROM replay_reprocess_job_events
            WHERE job_id = %s
            ORDER BY sequence DESC
            LIMIT 1
            FOR UPDATE
            """,
            (job_id,),
        ).fetchone()
        sequence = int(latest["sequence"]) + 1 if latest else 0
        manifest_cursor = str((detail or {}).get("manifest_cursor") or "")
        if event_type == "artifact_completed" and manifest_cursor:
            idempotency_key = stable_hash(
                {
                    "job_id": job_id,
                    "event_type": event_type,
                    "manifest_cursor": manifest_cursor,
                }
            )
            existing = self.connection.execute(
                """
                SELECT * FROM replay_reprocess_job_events
                WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            ).fetchone()
            if existing:
                return dict(existing)
        else:
            idempotency_key = stable_hash(
                {
                    "job_id": job_id,
                    "sequence": sequence,
                    "event_type": event_type,
                    "worker_key": worker_key,
                    "checkpoint_cursor": checkpoint_cursor,
                }
            )
        row = self.connection.execute(
            """
            INSERT INTO replay_reprocess_job_events (
                job_id, artifact_id, parse_run_id, idempotency_key, sequence,
                event_type, worker_key, checkpoint_cursor, attempt_number,
                processed_count, succeeded_count, failed_count, skipped_count,
                detail
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            RETURNING *
            """,
            (
                job_id,
                artifact_id,
                parse_run_id,
                idempotency_key,
                sequence,
                event_type,
                worker_key,
                checkpoint_cursor,
                attempt_number,
                accounting.processed,
                accounting.succeeded,
                accounting.failed,
                accounting.skipped,
                Jsonb(dict(detail or {})),
            ),
        ).fetchone()
        if not row:
            raise RuntimeError("failed to append replay job event")
        return dict(row)

    def ensure_artifact_and_submission(
        self,
        *,
        row: ManifestRow,
        reference: ManifestReference,
        job_spec: JobSpec,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        artifact_metadata = {
            "source": "frozen_csv_manifest",
            "manifest_sha256": job_spec.scope["manifest_sha256"],
            "manifest_cursor": row.cursor,
            "logical_key_hash": row.logical_key_hash,
            "archive_relative_path": row.archive_relative_path,
        }
        (
            submission_key,
            client_submission_id,
            transport_metadata,
        ) = submission_receipt_identity(
            row=row,
            reference=reference,
        )
        with self.connection.transaction():
            artifact_created = False
            artifact = self.connection.execute(
                """
                INSERT INTO replay_artifacts (
                    sha256, byte_size, storage_provider, storage_key,
                    original_extension, media_type, archive_metadata
                ) VALUES (%s, %s, 'filesystem', %s, %s, %s, %s)
                ON CONFLICT (sha256) DO NOTHING
                RETURNING *
                """,
                (
                    row.replay_hash,
                    row.byte_size,
                    str(row.archive_path),
                    row.extension,
                    "application/vnd.age2.replay",
                    Jsonb(artifact_metadata),
                ),
            ).fetchone()
            if artifact:
                artifact_created = True
            else:
                artifact = self.connection.execute(
                    "SELECT * FROM replay_artifacts WHERE sha256 = %s",
                    (row.replay_hash,),
                ).fetchone()
            if not artifact:
                raise RuntimeError("failed to create or reload replay artifact")
            if int(artifact["byte_size"]) != row.byte_size:
                raise CandidateObjectError(
                    "existing replay artifact byte size differs from manifest"
                )
            if artifact["storage_provider"] != "filesystem":
                raise CandidateObjectError(
                    "worker cannot verify a non-filesystem replay artifact"
                )
            if not artifact_created:
                stored_artifact = Path(str(artifact["storage_key"])).resolve()
                if not stored_artifact.is_file():
                    raise CandidateObjectError(
                        "existing immutable replay artifact is missing"
                    )
                if stored_artifact.stat().st_size != row.byte_size:
                    raise CandidateObjectError(
                        "existing immutable replay artifact size differs"
                    )
                if file_sha256(stored_artifact) != row.replay_hash:
                    raise CandidateObjectError(
                        "existing immutable replay artifact hash differs"
                    )

            submission = self.connection.execute(
                """
                INSERT INTO replay_submissions (
                    artifact_id, submitted_by_user_id, legacy_parse_attempt_id,
                    idempotency_key, submitter_uid_snapshot, source,
                    original_filename, client_submission_id, transport_metadata
                ) VALUES (%s, %s, %s, %s, %s, 'engine_room_manifest', %s, %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING *
                """,
                (
                    int(artifact["id"]),
                    reference.submitter_user_id,
                    reference.legacy_parse_attempt_id,
                    submission_key,
                    reference.submitter_uid,
                    row.original_filename or None,
                    client_submission_id,
                    Jsonb(transport_metadata),
                ),
            ).fetchone()
            if not submission:
                submission = self.connection.execute(
                    "SELECT * FROM replay_submissions WHERE idempotency_key = %s",
                    (submission_key,),
                ).fetchone()
            if not submission:
                raise RuntimeError("failed to create or reload replay submission")
            if int(submission["artifact_id"]) != int(artifact["id"]):
                raise RuntimeError("existing replay submission points to another artifact")
            immutable_submission = {
                "submitted_by_user_id": reference.submitter_user_id,
                "legacy_parse_attempt_id": reference.legacy_parse_attempt_id,
                "submitter_uid_snapshot": reference.submitter_uid,
                "source": "engine_room_manifest",
                "client_submission_id": client_submission_id,
                "original_filename": row.original_filename or None,
                "transport_metadata": transport_metadata,
            }
            for key, expected in immutable_submission.items():
                if submission.get(key) != expected:
                    raise RuntimeError(
                        f"existing replay submission differs at immutable field {key}"
                    )
        return dict(artifact), dict(submission)

    def find_parse_run(
        self,
        *,
        artifact_id: int,
        run_identity_hash: str,
    ) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT * FROM replay_parse_runs
            WHERE artifact_id = %s AND run_identity_hash = %s
            """,
            (artifact_id, run_identity_hash),
        ).fetchone()
        return dict(row) if row else None

    def insert_run_observations_and_event(
        self,
        *,
        job_id: int,
        artifact: Mapping[str, Any],
        submission: Mapping[str, Any],
        manifest_row: ManifestRow,
        reference: ManifestReference,
        job_spec: JobSpec,
        candidate: Mapping[str, Any],
        candidate_object: CandidateObject,
        observations: Sequence[Mapping[str, Any]],
        accounting: JobAccounting,
        started_at: datetime,
        completed_at: datetime,
    ) -> dict[str, Any]:
        parser = candidate["parser"]
        run = candidate["run"]
        run_status = str(run["status"])
        database_status = "failed" if run_status == "failed" else "completed"
        failure = run.get("failure") if isinstance(run.get("failure"), Mapping) else None
        failure_signature = _database_failure_signature(
            failure.get("signature") if failure else None
        )
        if database_status == "failed" and not failure_signature:
            failure_signature = "candidate_parse:failed:missing_signature"
        action_count = int((candidate.get("actions") or {}).get("count") or 0)
        emitted_observation_count = len(candidate.get("observations") or [])
        persisted_observation_count = len(observations)
        metrics = {
            "contract_version": candidate.get("contract_version"),
            "candidate_run_status": run_status,
            "parse_mode": run.get("parse_mode"),
            "candidate_semantic_sha256": candidate_object.semantic_sha256,
            "candidate_output_hash_scope": "exact_deterministic_gzip_bytes",
            "candidate_canonical_json_sha256": candidate_object.canonical_json_sha256,
            "candidate_compression": {
                "format": "gzip",
                "compresslevel": 9,
                "mtime": 0,
            },
            "manifest_sha256": job_spec.scope["manifest_sha256"],
            "manifest_cursor": manifest_row.cursor,
            "job_identity_hash": job_spec.job_identity_hash,
            "candidate_object_reused": candidate_object.reused,
            "emitted_observation_count": emitted_observation_count,
            "persisted_observation_count": persisted_observation_count,
            "omitted_catalog_observation_count": (
                emitted_observation_count - persisted_observation_count
            ),
        }
        with self.connection.transaction():
            parse_run = self.connection.execute(
                """
                INSERT INTO replay_parse_runs (
                    artifact_id, submission_id, legacy_parse_attempt_id,
                    game_stats_id, idempotency_key, run_identity_hash,
                    parser_name, parser_version, parser_build, pass_name,
                    pass_version, schema_version, input_hash,
                    parser_config_hash, status, candidate_output_hash,
                    candidate_output_storage_provider,
                    candidate_output_storage_key, candidate_output_byte_size,
                    observation_count, action_count, failure_signature,
                    failure_detail, metrics, candidate_only,
                    affects_public_aggregates, started_at, completed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s,
                    %s, %s, %s, %s, %s, %s, 'filesystem', %s, %s,
                    %s, %s, %s, %s, %s, TRUE, FALSE, %s, %s
                )
                RETURNING *
                """,
                (
                    int(artifact["id"]),
                    int(submission["id"]),
                    reference.legacy_parse_attempt_id,
                    reference.game_stats_id,
                    run["idempotency_key"],
                    run["idempotency_key"],
                    parser["implementation"],
                    parser["implementation_version"],
                    parser["pass_name"],
                    parser["pass_version"],
                    parser["schema_version"],
                    manifest_row.replay_hash,
                    job_spec.parser_config_hash,
                    database_status,
                    candidate_object.compressed_sha256,
                    candidate_object.storage_key,
                    candidate_object.compressed_byte_size,
                    emitted_observation_count,
                    action_count,
                    failure_signature,
                    canonical_json_bytes(failure).decode("utf-8") if failure else None,
                    Jsonb(metrics),
                    started_at,
                    completed_at,
                ),
            ).fetchone()
            if not parse_run:
                raise RuntimeError("failed to insert replay parse run")
            for observation in observations:
                self.connection.execute(
                    """
                    INSERT INTO replay_observations (
                        parse_run_id, idempotency_key, observation_key,
                        observation_kind, field_path, value, value_hash,
                        confidence_bps, provenance, candidate_only,
                        affects_public_aggregates
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, FALSE)
                    """,
                    (
                        int(parse_run["id"]),
                        observation["idempotency_key"],
                        observation["observation_key"],
                        observation["observation_kind"],
                        observation["field_path"],
                        Jsonb(observation["value"]),
                        observation["value_hash"],
                        observation["confidence_bps"],
                        Jsonb(observation["provenance"]),
                    ),
                )
            outcome = "failed" if database_status == "failed" else "succeeded"
            self._append_event_in_transaction(
                job_id=job_id,
                event_type="artifact_completed",
                accounting=accounting,
                artifact_id=int(artifact["id"]),
                parse_run_id=int(parse_run["id"]),
                attempt_number=1,
                detail={
                    "manifest_cursor": manifest_row.cursor,
                    "outcome": outcome,
                    "run_status": database_status,
                    "candidate_output_sha256": candidate_object.compressed_sha256,
                    "candidate_output_byte_size": candidate_object.compressed_byte_size,
                    "candidate_semantic_sha256": candidate_object.semantic_sha256,
                    "emitted_observation_count": emitted_observation_count,
                    "persisted_observation_count": persisted_observation_count,
                    "failure_signature": failure_signature,
                },
            )
        return dict(parse_run)


    def append_existing_run_event(
        self,
        *,
        job_id: int,
        artifact_id: int,
        parse_run: Mapping[str, Any],
        manifest_row: ManifestRow,
        accounting: JobAccounting,
    ) -> None:
        metrics = parse_run.get("metrics")
        metrics = metrics if isinstance(metrics, Mapping) else {}
        self.append_event(
            job_id=job_id,
            event_type="artifact_completed",
            accounting=accounting,
            artifact_id=artifact_id,
            parse_run_id=int(parse_run["id"]),
            attempt_number=1,
            detail={
                "manifest_cursor": manifest_row.cursor,
                "outcome": "skipped",
                "reason": "immutable_parser_identity_already_recorded",
                "existing_run_status": parse_run["status"],
                "candidate_output_sha256": parse_run["candidate_output_hash"],
                "emitted_observation_count": int(
                    parse_run.get("observation_count") or 0
                ),
                "persisted_observation_count": int(
                    metrics.get(
                        "persisted_observation_count",
                        0,
                    )
                ),
            },
        )


def enforce_invocation_artifact_limit(
    repository: Any,
    *,
    job_id: int,
    accounting: JobAccounting,
    worker_key: str,
    checkpoint_cursor: str,
    newly_accounted: int,
    remaining_manifest_rows: int,
    max_artifacts_this_run: int | None,
) -> None:
    """Append one resumable pause at an invocation boundary, then stop."""

    if max_artifacts_this_run is None:
        return
    if max_artifacts_this_run < 1:
        raise ValueError("max_artifacts_this_run must be a positive integer")
    if newly_accounted < max_artifacts_this_run or remaining_manifest_rows <= 0:
        return
    repository.append_event(
        job_id=job_id,
        event_type="paused",
        accounting=accounting,
        worker_key=worker_key,
        checkpoint_cursor=checkpoint_cursor,
        detail={
            "reason": "invocation_artifact_limit",
            "resume_safe": True,
            "checkpoint_cursor": checkpoint_cursor,
            "max_artifacts_this_run": max_artifacts_this_run,
            "newly_accounted_this_run": newly_accounted,
            "remaining_manifest_rows": remaining_manifest_rows,
        },
    )
    raise InvocationLimitPaused(
        f"invocation artifact limit {max_artifacts_this_run} reached; "
        f"{remaining_manifest_rows} manifest rows remain"
    )


def validate_candidate_envelope(
    candidate: Mapping[str, Any],
    *,
    manifest_row: ManifestRow,
    job_spec: JobSpec,
) -> None:
    artifact = candidate.get("artifact")
    parser = candidate.get("parser")
    run = candidate.get("run")
    state = candidate.get("candidate")
    observations = candidate.get("observations")
    if not all(isinstance(value, Mapping) for value in (artifact, parser, run, state)):
        raise RuntimeError("parser candidate is missing contract objects")
    if artifact.get("sha256") != manifest_row.replay_hash:
        raise RuntimeError("parser candidate artifact hash differs from manifest")
    if int(artifact.get("byte_size") or 0) != manifest_row.byte_size:
        raise RuntimeError("parser candidate artifact byte size differs from manifest")
    for key in (
        "implementation",
        "implementation_version",
        "schema_version",
        "pass_name",
        "pass_version",
        "options",
    ):
        if parser.get(key) != job_spec.parser.get(key):
            raise RuntimeError(f"parser candidate identity differs at {key}")
    run_key = run.get("idempotency_key")
    if not isinstance(run_key, str) or not SHA256_RE.fullmatch(run_key):
        raise RuntimeError("parser candidate run idempotency key is invalid")
    if run.get("status") not in {"succeeded", "recovered", "failed"}:
        raise RuntimeError("parser candidate run status is invalid")
    if state.get("promotion_status") != "candidate_only":
        raise RuntimeError("parser candidate is not candidate-only")
    if state.get("changes_effective_truth") is not False:
        raise RuntimeError("parser candidate attempts to change effective truth")
    semantic_hash = state.get("semantic_sha256")
    if semantic_hash is not None and not SHA256_RE.fullmatch(str(semantic_hash)):
        raise RuntimeError("parser candidate semantic hash is invalid")
    if not isinstance(observations, list):
        raise RuntimeError("parser candidate observations must be a list")


def _verify_existing_parse_run(parse_run: Mapping[str, Any]) -> dict[str, Any]:
    required = (
        parse_run.get("candidate_output_hash"),
        parse_run.get("candidate_output_storage_provider"),
        parse_run.get("candidate_output_storage_key"),
        parse_run.get("candidate_output_byte_size"),
    )
    if any(value is None for value in required):
        raise CandidateObjectError(
            "existing parse run has no complete immutable candidate locator"
        )
    if parse_run["candidate_output_storage_provider"] != "filesystem":
        raise CandidateObjectError("worker cannot verify non-filesystem candidate output")
    path = Path(str(parse_run["candidate_output_storage_key"])).resolve()
    if not _is_within(path, Path("/mnt").resolve()):
        raise CandidateObjectError("existing candidate output is not on mounted storage")
    metrics = parse_run.get("metrics")
    semantic_hash = (
        metrics.get("candidate_semantic_sha256")
        if isinstance(metrics, Mapping)
        else None
    )
    return verify_candidate_object(
        path,
        expected_compressed_sha256=str(parse_run["candidate_output_hash"]),
        expected_compressed_byte_size=int(parse_run["candidate_output_byte_size"]),
        expected_artifact_sha256=str(parse_run["input_hash"]),
        expected_run_idempotency_key=str(parse_run["idempotency_key"]),
        expected_semantic_sha256=semantic_hash,
    )


def _worker_failure_candidate(
    row: ManifestRow,
    file_bytes: bytes,
    *,
    apply_hd_early_exit_rules: bool,
    error: BaseException,
) -> dict[str, Any]:
    return build_candidate_envelope(
        replay_path=str(row.archive_path),
        file_bytes=file_bytes,
        projection=None,
        evidence=None,
        apply_hd_early_exit_rules=apply_hd_early_exit_rules,
        parse_mode="worker_parser_exception",
        failure=normalize_failure_signature(error, stage="candidate_worker"),
    )


def default_worker_key() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"[:128]


def run_candidate_job(
    report: ReconciliationReport,
    spec: JobSpec,
    *,
    database_url: str,
    jobs_root: Path = DEFAULT_JOBS_ROOT,
    submitter_uid_override: str | None = None,
    requested_by_uid: str | None = None,
    worker_key: str | None = None,
    min_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
    database_storage_path: Path = Path("/"),
    database_min_free_bytes: int = DEFAULT_DATABASE_MIN_FREE_BYTES,
    max_artifacts_this_run: int | None = None,
    apply_hd_early_exit_rules: bool = True,
    parser_callable: Callable[[str, bytes, bool], dict[str, Any]] = (
        parse_replay_candidate_bytes
    ),
) -> dict[str, Any]:
    """Execute or resume one bounded, single-worker, candidate-only job."""

    if not report.ok:
        raise ReconciliationError("candidate execution requires a clean plan")
    if report.manifest_rows != len(report.rows):
        raise ReconciliationError(
            "candidate execution requires every manifest row to reconcile"
        )
    jobs_root = validate_jobs_root(jobs_root)
    if min_free_bytes < 1024**3:
        raise ValueError("minimum storage reserve cannot be lower than 1 GiB")
    if database_min_free_bytes < 1024**3:
        raise ValueError("database storage reserve cannot be lower than 1 GiB")
    if max_artifacts_this_run is not None and max_artifacts_this_run < 1:
        raise ValueError("max_artifacts_this_run must be a positive integer")
    database_storage_path = database_storage_path.expanduser().resolve()
    if not database_storage_path.exists():
        raise ValueError(
            f"database storage reserve path does not exist: {database_storage_path}"
        )
    worker_key = (worker_key or default_worker_key())[:128]
    repository = EngineRoomRepository(database_url)
    job_id: int | None = None
    accounting = JobAccounting()
    last_cursor: str | None = None
    newly_accounted_this_run = 0
    try:
        repository.verify_schema()
        references = repository.resolve_manifest_references(
            report.rows,
            submitter_uid_override=submitter_uid_override,
        )
        requester_id = repository.resolve_requester(requested_by_uid)
        require_storage_reserve(jobs_root, min_free_bytes=min_free_bytes)

        with repository.session_lock(
            f"replay-engine-room-job:{spec.job_identity_hash}"
        ):
            job = repository.create_or_load_job(
                spec,
                requested_by_user_id=requester_id,
            )
            job_id = int(job["id"])
            latest, accounting, processed_cursors = repository.job_state(job_id)
            if latest["event_type"] in TERMINAL_JOB_EVENTS:
                if latest["event_type"] != "completed":
                    raise RuntimeError(
                        f"replay job is terminal with state {latest['event_type']}"
                    )
                accounting.validate(expected=report.manifest_rows)
                observation_accounting = repository.job_observation_accounting(job_id)
                return {
                    "mode": "candidate",
                    "job_id": job_id,
                    "job_identity_hash": spec.job_identity_hash,
                    "resumed": True,
                    "already_complete": True,
                    "accounting": asdict(accounting),
                    "observation_accounting": observation_accounting,
                    "equation": (
                        f"{accounting.processed} = {accounting.succeeded} + "
                        f"{accounting.failed} + {accounting.skipped}"
                    ),
                }

            repository.append_event(
                job_id=job_id,
                event_type="leased",
                accounting=accounting,
                worker_key=worker_key,
                detail={"worker_key": worker_key, "resume": accounting.processed > 0},
            )

            remaining = [
                row for row in report.rows if row.cursor not in processed_cursors
            ]
            for offset, row in enumerate(remaining):
                if offset % spec.batch_size == 0:
                    repository.append_event(
                        job_id=job_id,
                        event_type="batch_started",
                        accounting=accounting,
                        worker_key=worker_key,
                        detail={
                            "batch_first_cursor": row.cursor,
                            "batch_size_limit": spec.batch_size,
                        },
                    )
                last_cursor = row.cursor
                with repository.session_lock(
                    f"replay-engine-room-artifact:{row.replay_hash}"
                ):
                    file_bytes = row.archive_path.read_bytes()
                    if len(file_bytes) != row.byte_size:
                        raise WorkerPaused(
                            f"archive size changed after plan at cursor {row.cursor}"
                        )
                    if hashlib.sha256(file_bytes).hexdigest() != row.replay_hash:
                        raise WorkerPaused(
                            f"archive hash changed after plan at cursor {row.cursor}"
                        )
                    require_database_storage_reserve(
                        database_storage_path,
                        min_free_bytes=database_min_free_bytes,
                    )
                    artifact, submission = repository.ensure_artifact_and_submission(
                        row=row,
                        reference=references[row.cursor],
                        job_spec=spec,
                    )
                    run_identity_hash = stable_hash(
                        {
                            "artifact_sha256": row.replay_hash,
                            "parser": spec.parser,
                        }
                    )
                    existing_run = repository.find_parse_run(
                        artifact_id=int(artifact["id"]),
                        run_identity_hash=run_identity_hash,
                    )
                    if existing_run:
                        _verify_existing_parse_run(existing_run)
                        accounting = accounting.advanced("skipped")
                        repository.append_existing_run_event(
                            job_id=job_id,
                            artifact_id=int(artifact["id"]),
                            parse_run=existing_run,
                            manifest_row=row,
                            accounting=accounting,
                        )
                    else:
                        started_at = _utc_now_naive()
                        try:
                            candidate = parser_callable(
                                str(row.archive_path),
                                file_bytes,
                                apply_hd_early_exit_rules,
                            )
                        except Exception as error:
                            candidate = _worker_failure_candidate(
                                row,
                                file_bytes,
                                apply_hd_early_exit_rules=(
                                    apply_hd_early_exit_rules
                                ),
                                error=error,
                            )
                        validate_candidate_envelope(
                            candidate,
                            manifest_row=row,
                            job_spec=spec,
                        )
                        run_key = str(candidate["run"]["idempotency_key"])
                        if run_key != run_identity_hash:
                            raise RuntimeError(
                                "candidate run key differs from worker identity hash"
                            )
                        output_path = candidate_object_path(
                            jobs_root,
                            spec.job_identity_hash,
                            row.replay_hash,
                            run_key,
                        )
                        candidate_object = store_candidate_object(
                            output_path,
                            candidate,
                            min_free_bytes=min_free_bytes,
                            private_root=jobs_root,
                        )
                        observations = normalize_observations(
                            candidate["observations"],
                            run_idempotency_key=run_key,
                        )
                        require_database_storage_reserve(
                            database_storage_path,
                            min_free_bytes=database_min_free_bytes,
                        )
                        outcome = (
                            "failed"
                            if candidate["run"]["status"] == "failed"
                            else "succeeded"
                        )
                        accounting = accounting.advanced(outcome)
                        repository.insert_run_observations_and_event(
                            job_id=job_id,
                            artifact=artifact,
                            submission=submission,
                            manifest_row=row,
                            reference=references[row.cursor],
                            job_spec=spec,
                            candidate=candidate,
                            candidate_object=candidate_object,
                            observations=observations,
                            accounting=accounting,
                            started_at=started_at,
                            completed_at=_utc_now_naive(),
                        )

                newly_accounted_this_run += 1
                remaining_after_row = len(remaining) - offset - 1
                enforce_invocation_artifact_limit(
                    repository,
                    job_id=job_id,
                    accounting=accounting,
                    worker_key=worker_key,
                    checkpoint_cursor=row.cursor,
                    newly_accounted=newly_accounted_this_run,
                    remaining_manifest_rows=remaining_after_row,
                    max_artifacts_this_run=max_artifacts_this_run,
                )
                is_batch_end = (
                    (offset + 1) % spec.batch_size == 0
                    or offset == len(remaining) - 1
                )
                if is_batch_end:
                    repository.append_event(
                        job_id=job_id,
                        event_type="checkpointed",
                        accounting=accounting,
                        worker_key=worker_key,
                        checkpoint_cursor=row.cursor,
                        detail={
                            "checkpoint_cursor": row.cursor,
                            "processed_count": accounting.processed,
                        },
                    )

            accounting.validate(expected=report.manifest_rows)
            observation_accounting = repository.job_observation_accounting(job_id)
            repository.append_event(
                job_id=job_id,
                event_type="completed",
                accounting=accounting,
                worker_key=worker_key,
                checkpoint_cursor=last_cursor,
                detail={
                    "manifest_sha256": report.manifest_sha256,
                    "manifest_rows": report.manifest_rows,
                    "unique_artifacts": report.unique_artifacts,
                    "processed": accounting.processed,
                    "succeeded": accounting.succeeded,
                    "failed": accounting.failed,
                    "skipped": accounting.skipped,
                    "equation_balanced": True,
                    "observation_accounting": observation_accounting,
                    "candidate_only": True,
                    "affects_public_aggregates": False,
                },
            )
            return {
                "mode": "candidate",
                "job_id": job_id,
                "job_identity_hash": spec.job_identity_hash,
                "resumed": bool(processed_cursors),
                "already_complete": False,
                "accounting": asdict(accounting),
                "observation_accounting": observation_accounting,
                "equation": (
                    f"{accounting.processed} = {accounting.succeeded} + "
                    f"{accounting.failed} + {accounting.skipped}"
                ),
                "candidate_only": True,
                "affects_public_aggregates": False,
                "jobs_root": str(jobs_root),
            }
    except BaseException as error:
        if job_id is not None and not getattr(
            error,
            "event_already_recorded",
            False,
        ):
            try:
                latest, current, _ = repository.job_state(job_id)
                if latest["event_type"] not in TERMINAL_JOB_EVENTS:
                    repository.append_event(
                        job_id=job_id,
                        event_type="paused",
                        accounting=current,
                        worker_key=worker_key,
                        checkpoint_cursor=last_cursor,
                        detail={
                            "failure": normalize_failure_signature(
                                error,
                                stage="engine_room_worker",
                            ),
                            "resume_safe": True,
                            "last_cursor": last_cursor,
                        },
                    )
            except Exception:
                pass
        raise
    finally:
        repository.close()
