from __future__ import annotations

import csv
from contextlib import contextmanager
from dataclasses import replace
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from types import SimpleNamespace

import pytest

import utils.replay_engine_room_worker as worker_module
from utils.replay_engine import build_candidate_envelope, parser_identity
from utils.replay_engine_room_worker import (
    CandidateObjectError,
    JobAccounting,
    InvocationLimitPaused,
    ManifestReference,
    ReconciliationError,
    build_job_spec,
    candidate_object_path,
    deterministic_gzip,
    enforce_invocation_artifact_limit,
    external_candidate_parser,
    load_external_parser_identity,
    normalize_observations,
    reconcile_frozen_manifest,
    require_database_storage_reserve,
    resolve_submitter_uid_assertion,
    stable_hash,
    store_candidate_object,
    submission_receipt_identity,
    validate_external_parser_python,
    validate_jobs_root,
    verify_candidate_object,
)


def _archive_file(root: Path, payload: bytes, suffix: str = ".aoe2record") -> Path:
    digest = hashlib.sha256(payload).hexdigest()
    path = root / digest[:2] / digest[2:4] / f"{digest}{suffix}"
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    return path


def _manifest(path: Path, archive_root: Path, archive_path: Path) -> None:
    replay_hash = archive_path.name.split(".", 1)[0]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "logical_replay_key",
                "game_stats_id",
                "replay_hash",
                "original_filename",
                "extension",
                "archive_present",
                "archive_relative_path",
                "archive_bytes",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "logical_replay_key": "replay:fixture",
                "game_stats_id": "42",
                "replay_hash": replay_hash,
                "original_filename": "fixture.aoe2record",
                "extension": ".aoe2record",
                "archive_present": "true",
                "archive_relative_path": str(archive_path.relative_to(archive_root)),
                "archive_bytes": archive_path.stat().st_size,
            }
        )


def _candidate(payload: bytes, replay_path: Path) -> dict:
    projection = {
        "game_version": "Version.HD",
        "game_type": "RM",
        "duration": 100,
        "completed": True,
        "players": [],
        "key_events": {},
        "team_resolution": {
            "result_trusted": False,
            "winning_team_id": None,
            "winning_player_keys": [],
        },
    }
    return build_candidate_envelope(
        replay_path=str(replay_path),
        file_bytes=payload,
        projection=projection,
        evidence={},
        apply_hd_early_exit_rules=True,
        parse_mode="test",
    )


def test_external_parser_identity_is_part_of_job_identity(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_path = _archive_file(archive_root, b"external parser identity")
    manifest_path = tmp_path / "manifest.csv"
    _manifest(manifest_path, archive_root, archive_path)
    report = reconcile_frozen_manifest(manifest_path, archive_root)
    identity = load_external_parser_identity(
        Path(sys.executable),
        apply_hd_early_exit_rules=True,
    )
    default_spec = build_job_spec(
        report,
        apply_hd_early_exit_rules=True,
        batch_size=1,
    )
    external_spec = build_job_spec(
        report,
        apply_hd_early_exit_rules=True,
        batch_size=1,
        parser_identity_override=identity,
    )

    assert external_spec.parser == identity
    repeated_external_spec = build_job_spec(
        report,
        apply_hd_early_exit_rules=True,
        batch_size=1,
        parser_identity_override=identity,
    )
    assert external_spec.job_identity_hash == repeated_external_spec.job_identity_hash
    if identity != default_spec.parser:
        assert external_spec.job_identity_hash != default_spec.job_identity_hash

    changed_identity = dict(identity)
    changed_identity["implementation_version"] = "compatibility-fixture"
    changed_spec = build_job_spec(
        report,
        apply_hd_early_exit_rules=True,
        batch_size=1,
        parser_identity_override=changed_identity,
    )
    assert changed_spec.job_identity_hash != default_spec.job_identity_hash


def test_external_parser_keeps_explicit_venv_symlink_path(tmp_path: Path) -> None:
    interpreter = tmp_path / "compat-venv-python"
    interpreter.symlink_to(sys.executable)

    assert validate_external_parser_python(interpreter) == interpreter.absolute()


def test_external_candidate_parser_returns_structured_failed_candidate(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "broken.aoe2record"
    payload = b"not a replay"
    artifact.write_bytes(payload)
    parser = external_candidate_parser(Path(sys.executable), timeout_seconds=30)

    candidate = parser(str(artifact), payload, True)

    assert candidate["artifact"]["sha256"] == hashlib.sha256(payload).hexdigest()
    assert candidate["run"]["status"] == "failed"
    assert candidate["candidate"]["changes_effective_truth"] is False


def test_plan_reconciles_every_byte_without_creating_output(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_path = _archive_file(archive_root, b"immutable replay bytes")
    manifest = tmp_path / "manifest.csv"
    _manifest(manifest, archive_root, archive_path)
    untouched = set(tmp_path.rglob("*"))

    report = reconcile_frozen_manifest(manifest, archive_root)

    assert report.ok
    assert report.manifest_rows == 1
    assert report.unique_artifacts == 1
    assert report.total_bytes == len(b"immutable replay bytes")
    assert report.rows[0].replay_hash == hashlib.sha256(
        b"immutable replay bytes"
    ).hexdigest()
    assert set(tmp_path.rglob("*")) == untouched
    assert report.summary()["writes_performed"] is False


def test_plan_reports_hash_and_path_integrity_failures(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_path = _archive_file(archive_root, b"first")
    manifest = tmp_path / "manifest.csv"
    _manifest(manifest, archive_root, archive_path)
    archive_path.write_bytes(b"changed")

    report = reconcile_frozen_manifest(manifest, archive_root)

    assert not report.ok
    assert {issue.code for issue in report.errors} >= {
        "archive_size_mismatch",
        "archive_hash_mismatch",
    }


def test_plan_rejects_archive_path_traversal(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    outside = tmp_path / "outside.aoe2record"
    outside.write_bytes(b"outside")
    digest = hashlib.sha256(b"outside").hexdigest()
    manifest = tmp_path / "manifest.csv"
    with manifest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["replay_hash", "archive_relative_path"],
        )
        writer.writeheader()
        writer.writerow(
            {"replay_hash": digest, "archive_relative_path": "../outside.aoe2record"}
        )

    report = reconcile_frozen_manifest(manifest, archive_root)

    assert not report.ok
    assert "unsafe_archive_path" in {issue.code for issue in report.errors}


def test_candidate_gzip_is_deterministic_and_exact_byte_addressed(
    tmp_path: Path,
) -> None:
    payload = b"candidate replay"
    replay_path = tmp_path / "candidate.aoe2record"
    replay_path.write_bytes(payload)
    candidate = _candidate(payload, replay_path)
    assert candidate["run"]["idempotency_key"] == stable_hash(
        {
            "artifact_sha256": candidate["artifact"]["sha256"],
            "parser": candidate["parser"],
        }
    )
    canonical_one, compressed_one = deterministic_gzip(candidate)
    canonical_two, compressed_two = deterministic_gzip(candidate)

    assert canonical_one == canonical_two
    assert compressed_one == compressed_two
    assert compressed_one[4:8] == b"\x00\x00\x00\x00"
    assert compressed_one[9] == 255
    assert gzip.decompress(compressed_one) == canonical_one

    path = tmp_path / "jobs" / "candidate.json.gz"
    stored = store_candidate_object(path, candidate, min_free_bytes=0)
    assert stored.compressed_sha256 == hashlib.sha256(compressed_one).hexdigest()
    assert stored.compressed_byte_size == len(compressed_one)
    assert path.stat().st_mode & 0o777 == 0o600
    reused = store_candidate_object(path, candidate, min_free_bytes=0)
    assert reused.reused is True
    assert reused.compressed_sha256 == stored.compressed_sha256


def test_existing_candidate_object_tamper_is_rejected(tmp_path: Path) -> None:
    payload = b"candidate replay"
    replay_path = tmp_path / "candidate.aoe2record"
    replay_path.write_bytes(payload)
    candidate = _candidate(payload, replay_path)
    path = tmp_path / "candidate.json.gz"
    stored = store_candidate_object(path, candidate, min_free_bytes=0)
    path.write_bytes(path.read_bytes() + b"tampered")
    path.chmod(0o600)

    with pytest.raises(CandidateObjectError):
        verify_candidate_object(
            path,
            expected_compressed_sha256=stored.compressed_sha256,
            expected_compressed_byte_size=stored.compressed_byte_size,
        )


def test_job_identity_and_candidate_path_are_stable(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_path = _archive_file(archive_root, b"stable")
    manifest = tmp_path / "manifest.csv"
    _manifest(manifest, archive_root, archive_path)
    report = reconcile_frozen_manifest(manifest, archive_root)
    first = build_job_spec(
        report,
        apply_hd_early_exit_rules=True,
        batch_size=1,
    )
    second = build_job_spec(
        report,
        apply_hd_early_exit_rules=True,
        batch_size=1,
    )
    run_key = stable_hash({"fixture": 1})

    assert first == second
    path = candidate_object_path(
        Path("/mnt/replay/jobs"),
        first.job_identity_hash,
        report.rows[0].replay_hash,
        run_key,
    )
    assert str(path).endswith(f"/{run_key}.json.gz")


def test_cross_language_worker_identity_golden_vectors() -> None:
    parser = parser_identity(apply_hd_early_exit_rules=True)
    artifact_sha256 = "a" * 64
    run_identity_hash = stable_hash(
        {"artifact_sha256": artifact_sha256, "parser": parser}
    )
    scope = {
        "version": 1,
        "kind": "frozen_csv_manifest",
        "manifest_sha256": "b" * 64,
        "manifest_filename": "fixture.csv",
        "manifest_rows": 5,
        "unique_artifacts": 5,
        "archive_root": "/mnt/archive",
    }
    scope_hash = stable_hash(scope)
    job_identity_hash = stable_hash(
        {
            "scope_hash": scope_hash,
            "parser": parser,
            "batch_size": 5,
            "max_attempts_per_artifact": 1,
            "candidate_only": True,
            "affects_public_aggregates": False,
        }
    )

    assert run_identity_hash == (
        "4f2b7f1d1b911119f90907722452d13aaaa10b532a65bdebefc71ac9ad0044c7"
    )
    assert scope_hash == (
        "9841a68aadb2f0d1ab9b4af7d72a238811d5db20a66e963bde3d0828902a4a68"
    )
    assert job_identity_hash == (
        "c67b2f2f85a3e01ca36dec892812273ce6a7caeebc9d1e0e6e4e947c6e1c242b"
    )


def test_observations_keep_subject_provenance_and_unique_keys() -> None:
    run_key = "a" * 64
    observations = [
        {
            "field": "player.winner",
            "value": True,
            "subject": {"type": "player", "player_number": 1},
            "provenance_class": "derived_coherent",
            "evidence_source": "fixture",
            "exact": True,
            "conflict_state": "none",
        },
        {
            "field": "player.winner",
            "value": None,
            "subject": {"type": "player", "player_number": 2},
            "provenance_class": "inferred_review_only",
            "evidence_source": "fixture",
            "exact": False,
            "conflict_state": "none",
        },
        {
            "field": "player.postgame.economy.food_collected",
            "value": None,
            "subject": {"type": "player", "player_number": 1},
            "provenance_class": "absent",
            "evidence_source": "fixture",
            "exact": True,
            "conflict_state": "none",
        },
        {
            "field": "result.winning_team_id",
            "value": None,
            "subject": {"type": "game"},
            "provenance_class": "absent",
            "evidence_source": "fixture",
            "exact": False,
            "conflict_state": "conflict",
        },
    ]

    normalized = normalize_observations(
        observations,
        run_idempotency_key=run_key,
    )

    assert len(normalized) == 3
    assert len({row["observation_key"] for row in normalized}) == 3
    assert normalized[0]["confidence_bps"] == 9000
    assert normalized[1]["confidence_bps"] is None
    assert normalized[0]["provenance"]["subject"]["player_number"] == 1
    assert normalized[2]["field_path"] == "result.winning_team_id"


def test_accounting_must_balance_exactly() -> None:
    accounting = JobAccounting().advanced("succeeded").advanced("failed")
    accounting = accounting.advanced("skipped")
    accounting.validate(expected=3)
    assert accounting.processed == (
        accounting.succeeded + accounting.failed + accounting.skipped
    )


def test_jobs_root_rejects_root_disk_fallback() -> None:
    if Path("/mnt").exists() and Path("/mnt").stat().st_dev != Path("/").stat().st_dev:
        pytest.skip("/mnt is a real mount on this test host")
    with pytest.raises(ValueError, match="separate mounted filesystem"):
        validate_jobs_root(Path("/mnt/not-a-mounted-replay-volume/jobs"))


def test_submission_receipt_is_stable_across_overlapping_cohort_jobs(
    tmp_path: Path,
) -> None:
    archive_root = tmp_path / "archive"
    archive_path = _archive_file(archive_root, b"overlap")
    manifest = tmp_path / "manifest.csv"
    _manifest(manifest, archive_root, archive_path)
    row = reconcile_frozen_manifest(manifest, archive_root).rows[0]
    later_full_vault_row = replace(
        row,
        ordinal=2907,
        cursor=f"002907:{row.replay_hash[:16]}",
        logical_key_hash="f" * 64,
    )
    reference = ManifestReference(
        game_stats_id=42,
        legacy_parse_attempt_id=None,
        submitter_user_id=7,
        submitter_uid="u_jim",
    )

    first = submission_receipt_identity(row=row, reference=reference)
    overlapping = submission_receipt_identity(
        row=later_full_vault_row,
        reference=reference,
    )

    assert first == overlapping
    assert first[1] == f"engine-room:{first[0]}"
    assert "manifest_cursor" not in first[2]
    assert "manifest_sha256" not in first[2]


def test_submitter_uid_override_is_always_an_assertion() -> None:
    assert (
        resolve_submitter_uid_assertion(
            game_stats_uid="u_jim",
            legacy_attempt_uid=None,
            manifest_uid="u_jim",
            override_uid="u_jim",
            manifest_ordinal=1,
        )
        == "u_jim"
    )
    with pytest.raises(ReconciliationError, match="assertions disagree"):
        resolve_submitter_uid_assertion(
            game_stats_uid="u_jim",
            legacy_attempt_uid=None,
            manifest_uid="u_jim",
            override_uid="u_emaren",
            manifest_ordinal=1,
        )


def test_invocation_bound_appends_one_pause_and_resumes_from_new_budget() -> None:
    class EventRecorder:
        def __init__(self) -> None:
            self.events: list[dict] = []

        def append_event(self, **kwargs: object) -> None:
            self.events.append(dict(kwargs))

    recorder = EventRecorder()
    accounting = JobAccounting(processed=2, succeeded=2)
    with pytest.raises(InvocationLimitPaused) as paused:
        enforce_invocation_artifact_limit(
            recorder,
            job_id=9,
            accounting=accounting,
            worker_key="worker",
            checkpoint_cursor="000002:abcdef",
            newly_accounted=2,
            remaining_manifest_rows=3,
            max_artifacts_this_run=2,
        )

    assert len(recorder.events) == 1
    assert paused.value.event_already_recorded is True
    assert recorder.events[0]["event_type"] == "paused"
    assert recorder.events[0]["checkpoint_cursor"] == "000002:abcdef"
    assert recorder.events[0]["detail"]["reason"] == "invocation_artifact_limit"

    # A resumed invocation starts a fresh invocation budget. Its final row does
    # not add another pause because no manifest work remains.
    enforce_invocation_artifact_limit(
        recorder,
        job_id=9,
        accounting=JobAccounting(processed=5, succeeded=5),
        worker_key="worker-2",
        checkpoint_cursor="000005:abcdef",
        newly_accounted=3,
        remaining_manifest_rows=0,
        max_artifacts_this_run=2,
    )
    assert len(recorder.events) == 1


def test_database_root_reserve_pauses_then_allows_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gib = 1024**3
    monkeypatch.setattr(
        worker_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=2 * gib),
    )
    with pytest.raises(worker_module.WorkerPaused, match="database storage"):
        require_database_storage_reserve(tmp_path, min_free_bytes=3 * gib)

    monkeypatch.setattr(
        worker_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=4 * gib),
    )
    require_database_storage_reserve(tmp_path, min_free_bytes=3 * gib)


def test_candidate_invocation_pause_resumes_without_duplicate_pause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_root = tmp_path / "archive"
    manifest = tmp_path / "manifest.csv"
    archive_paths = [
        _archive_file(archive_root, f"replay-{index}".encode())
        for index in range(1, 4)
    ]
    with manifest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "logical_replay_key",
                "game_stats_id",
                "replay_hash",
                "original_filename",
                "extension",
                "archive_present",
                "archive_relative_path",
                "archive_bytes",
            ],
        )
        writer.writeheader()
        for index, archive_path in enumerate(archive_paths, start=1):
            writer.writerow(
                {
                    "logical_replay_key": f"replay:{index}",
                    "game_stats_id": index,
                    "replay_hash": archive_path.stem,
                    "original_filename": f"fixture-{index}.aoe2record",
                    "extension": ".aoe2record",
                    "archive_present": "true",
                    "archive_relative_path": str(
                        archive_path.relative_to(archive_root)
                    ),
                    "archive_bytes": archive_path.stat().st_size,
                }
            )
    report = reconcile_frozen_manifest(manifest, archive_root)
    spec = build_job_spec(
        report,
        apply_hd_early_exit_rules=True,
        batch_size=2,
    )

    class FakeRepository:
        def __init__(self) -> None:
            self.events: list[dict] = []

        def close(self) -> None:
            return None

        def verify_schema(self) -> None:
            return None

        def resolve_manifest_references(self, rows, **_kwargs):
            return {
                row.cursor: ManifestReference(
                    game_stats_id=row.game_stats_id,
                    legacy_parse_attempt_id=None,
                    submitter_user_id=1,
                    submitter_uid="u_jim",
                )
                for row in rows
            }

        def resolve_requester(self, _uid):
            return None

        @contextmanager
        def session_lock(self, _key):
            yield

        def create_or_load_job(self, _spec, **_kwargs):
            if not self.events:
                self.append_event(
                    job_id=1,
                    event_type="queued",
                    accounting=JobAccounting(),
                    detail={},
                )
            return {"id": 1}

        def append_event(
            self,
            *,
            event_type,
            accounting,
            detail=None,
            **kwargs,
        ):
            event = {
                "sequence": len(self.events),
                "event_type": event_type,
                "processed_count": accounting.processed,
                "succeeded_count": accounting.succeeded,
                "failed_count": accounting.failed,
                "skipped_count": accounting.skipped,
                "detail": dict(detail or {}),
                **kwargs,
            }
            self.events.append(event)
            return event

        def job_state(self, _job_id):
            latest = self.events[-1]
            accounting = JobAccounting(
                processed=latest["processed_count"],
                succeeded=latest["succeeded_count"],
                failed=latest["failed_count"],
                skipped=latest["skipped_count"],
            )
            cursors = {
                event["detail"]["manifest_cursor"]
                for event in self.events
                if event["event_type"] == "artifact_completed"
            }
            return latest, accounting, cursors

        def ensure_artifact_and_submission(self, *, row, **_kwargs):
            return {"id": row.ordinal}, {"id": row.ordinal}

        def find_parse_run(self, **_kwargs):
            return None

        def insert_run_observations_and_event(
            self,
            *,
            job_id,
            artifact,
            manifest_row,
            accounting,
            candidate,
            **_kwargs,
        ):
            self.append_event(
                job_id=job_id,
                event_type="artifact_completed",
                accounting=accounting,
                artifact_id=artifact["id"],
                parse_run_id=manifest_row.ordinal,
                attempt_number=1,
                detail={
                    "manifest_cursor": manifest_row.cursor,
                    "outcome": "succeeded",
                    "emitted_observation_count": len(candidate["observations"]),
                    "persisted_observation_count": 0,
                },
            )
            return {"id": manifest_row.ordinal}

        def job_observation_accounting(self, _job_id):
            return {"emitted": 0, "persisted": 0, "catalog_only": 0}

    repository = FakeRepository()
    monkeypatch.setattr(
        worker_module,
        "EngineRoomRepository",
        lambda _database_url: repository,
    )
    monkeypatch.setattr(
        worker_module,
        "validate_jobs_root",
        lambda path: path.resolve(),
    )
    monkeypatch.setattr(
        worker_module,
        "require_storage_reserve",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        worker_module,
        "require_database_storage_reserve",
        lambda *_args, **_kwargs: None,
    )

    def fixture_parser(path: str, payload: bytes, _early_exit: bool) -> dict:
        return _candidate(payload, Path(path))

    run_kwargs = {
        "database_url": "postgresql://unused",
        "jobs_root": tmp_path / "jobs",
        "database_storage_path": tmp_path,
        "min_free_bytes": 1024**3,
        "database_min_free_bytes": 1024**3,
        "max_artifacts_this_run": 2,
        "parser_callable": fixture_parser,
    }
    with pytest.raises(InvocationLimitPaused):
        worker_module.run_candidate_job(report, spec, **run_kwargs)

    assert len(
        [event for event in repository.events if event["event_type"] == "paused"]
    ) == 1
    assert not any(
        event["event_type"] == "checkpointed" for event in repository.events
    )

    result = worker_module.run_candidate_job(report, spec, **run_kwargs)

    assert result["accounting"]["processed"] == 3
    assert result["already_complete"] is False
    assert repository.events[-1]["event_type"] == "completed"
    assert len(
        [event for event in repository.events if event["event_type"] == "paused"]
    ) == 1


def test_worker_sql_only_inserts_private_engine_room_tables() -> None:
    source = (
        Path(__file__).parents[1] / "utils" / "replay_engine_room_worker.py"
    ).read_text(encoding="utf-8")
    targets = {
        match.casefold()
        for match in re.findall(r"INSERT\s+INTO\s+([a-z_]+)", source, re.I)
    }
    assert targets == {
        "replay_artifacts",
        "replay_submissions",
        "replay_parse_runs",
        "replay_observations",
        "replay_reprocess_jobs",
        "replay_reprocess_job_events",
    }
    assert not re.search(r"(?:UPDATE|DELETE\s+FROM)\s+game_stats\b", source, re.I)


def test_cli_plan_runs_from_outside_repo_and_remains_read_only(
    tmp_path: Path,
) -> None:
    archive_root = tmp_path / "archive"
    archive_path = _archive_file(archive_root, b"cli plan")
    manifest = tmp_path / "manifest.csv"
    _manifest(manifest, archive_root, archive_path)
    before = {path: path.stat().st_mtime_ns for path in tmp_path.rglob("*")}
    script = (
        Path(__file__).parents[1] / "scripts" / "run_replay_engine_room_job.py"
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--mode",
            "plan",
            "--manifest",
            str(manifest),
            "--archive-root",
            str(archive_root),
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    after = {path: path.stat().st_mtime_ns for path in tmp_path.rglob("*")}
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["writes_performed"] is False
    assert after == before

    invalid_bound = subprocess.run(
        [
            sys.executable,
            str(script),
            "--mode",
            "plan",
            "--manifest",
            str(manifest),
            "--archive-root",
            str(archive_root),
            "--max-artifacts-this-run",
            "0",
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )
    assert invalid_bound.returncode != 0
    assert "positive integer" in invalid_bound.stderr
