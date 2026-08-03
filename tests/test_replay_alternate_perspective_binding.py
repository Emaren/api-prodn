from __future__ import annotations

from pathlib import Path
import inspect

import pytest

from utils.replay_engine_room_worker import (
    ManifestReference,
    ManifestRow,
    ReconciliationError,
    EngineRoomRepository,
    resolve_game_stats_artifact_linkage,
    submission_receipt_identity,
)


def test_canonical_artifact_linkage_uses_game_owner_assertion() -> None:
    linkage, assert_game_owner = resolve_game_stats_artifact_linkage(
        game_stats_id=18245,
        game_replay_hash="a" * 64,
        game_is_final=True,
        manifest_replay_hash="a" * 64,
        legacy_parse_attempt_id=None,
        legacy_attempt_game_stats_id=None,
        legacy_attempt_replay_hash=None,
        legacy_attempt_status=None,
    )

    assert linkage == "validated_replay_hash"
    assert assert_game_owner is True


@pytest.mark.parametrize(
    "status",
    [
        "duplicate_reviewed_match",
        "reviewed_match_artifact_advanced",
        "reviewed_match_refreshed",
    ],
)
def test_reviewed_alternate_artifact_uses_attempt_submitter(
    status: str,
) -> None:
    linkage, assert_game_owner = resolve_game_stats_artifact_linkage(
        game_stats_id=18245,
        game_replay_hash="a" * 64,
        game_is_final=True,
        manifest_replay_hash="b" * 64,
        legacy_parse_attempt_id=42351,
        legacy_attempt_game_stats_id=18245,
        legacy_attempt_replay_hash="b" * 64,
        legacy_attempt_status=status,
    )

    assert linkage == "validated_reviewed_match_alternate_artifact"
    assert assert_game_owner is False


@pytest.mark.parametrize(
    (
        "game_is_final",
        "attempt_id",
        "attempt_game_id",
        "attempt_hash",
        "attempt_status",
    ),
    [
        (False, 42351, 18245, "b" * 64, "duplicate_reviewed_match"),
        (True, None, 18245, "b" * 64, "duplicate_reviewed_match"),
        (True, 42351, 99999, "b" * 64, "duplicate_reviewed_match"),
        (True, 42351, 18245, "c" * 64, "duplicate_reviewed_match"),
        (True, 42351, 18245, "b" * 64, "parse_failed"),
    ],
)
def test_unproven_alternate_artifact_fails_closed(
    game_is_final: bool,
    attempt_id: int | None,
    attempt_game_id: int | None,
    attempt_hash: str,
    attempt_status: str,
) -> None:
    with pytest.raises(
        ReconciliationError,
        match="validated reviewed-match alternate artifact receipt",
    ):
        resolve_game_stats_artifact_linkage(
            game_stats_id=18245,
            game_replay_hash="a" * 64,
            game_is_final=game_is_final,
            manifest_replay_hash="b" * 64,
            legacy_parse_attempt_id=attempt_id,
            legacy_attempt_game_stats_id=attempt_game_id,
            legacy_attempt_replay_hash=attempt_hash,
            legacy_attempt_status=attempt_status,
        )


def test_alternate_receipt_preserves_canonical_hash_snapshot() -> None:
    row = ManifestRow(
        ordinal=1,
        cursor="000001:" + "b" * 16,
        logical_key_hash="c" * 64,
        game_stats_id=18245,
        legacy_parse_attempt_id=42351,
        replay_hash="b" * 64,
        original_filename="julio.aoe2record",
        extension=".aoe2record",
        archive_relative_path="bb/bb/" + "b" * 64 + ".aoe2record",
        archive_path=Path("/private/archive/julio.aoe2record"),
        byte_size=535093,
        submitter_uid="u_julio",
    )

    reference = ManifestReference(
        game_stats_id=18245,
        legacy_parse_attempt_id=42351,
        submitter_user_id=65,
        submitter_uid="u_julio",
        game_stats_linkage=(
            "validated_reviewed_match_alternate_artifact"
        ),
        game_stats_replay_hash_snapshot="a" * 64,
    )

    _, _, metadata = submission_receipt_identity(
        row=row,
        reference=reference,
    )

    assert metadata["game_stats_linkage"] == (
        "validated_reviewed_match_alternate_artifact"
    )
    assert metadata["game_stats_replay_hash_snapshot"] == "a" * 64
    assert metadata["artifact_sha256"] == "b" * 64
    assert metadata["candidate_only"] is True
    assert metadata["affects_public_aggregates"] is False


def test_repository_reference_resolution_uses_linkage_policy() -> None:
    source = inspect.getsource(
        EngineRoomRepository.resolve_manifest_references
    )

    assert "resolve_game_stats_artifact_linkage" in source
    assert "game_uid_assertion = None" in source
    assert "legacy_attempt_status" in source
