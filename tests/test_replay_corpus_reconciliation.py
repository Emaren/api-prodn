from pathlib import Path

from utils.replay_corpus_reconciliation import (
    ArchiveObject,
    archive_profile,
    build_full_artifact_manifest_rows,
    classify_current_result,
    freeze_logical_cohort,
    logical_replay_key,
    parse_archive_object,
    verify_archive_content_hashes,
)


JIM_UID = "u_0df73bdbb64646c19e4a9bfd225b3285"


def game_row(game_id: int, **overrides):
    row = {
        "id": game_id,
        "user_uid": JIM_UID,
        "replay_file": f"Replay-{game_id}.aoe2record",
        "original_filename": f"Replay-{game_id}.aoe2record",
        "replay_hash": f"{game_id:064x}",
        "is_final": True,
        "winner": "Jim",
        "players": [{"name": "Jim"}, {"name": "Opponent"}],
        "key_events": {},
        "parse_source": "watcher_final",
        "parse_reason": "recorded_resignation_final",
        "created_at": f"2026-07-14T00:00:{game_id:02d}+00:00",
        "played_on": None,
        "timestamp": None,
    }
    row.update(overrides)
    return row


def test_freeze_logical_cohort_keeps_better_later_projection_without_double_counting():
    partial = game_row(
        10,
        replay_file="same.aoe2record",
        winner="Jim",
        parse_reason="superseded_by_later_upload",
    )
    complete = game_row(
        11,
        replay_file="same.aoe2record",
        replay_hash="f" * 64,
    )
    after_anchor = game_row(13)

    cohort, duplicates = freeze_logical_cohort(
        [partial, complete, after_anchor],
        user_uid=JIM_UID,
        anchor_game_id=12,
    )

    assert [row["id"] for row in cohort] == [11]
    assert list(duplicates) == ["replay:same.aoe2record"]


def test_logical_replay_key_matches_profile_filename_grain():
    assert logical_replay_key({"id": 5, "replay_file": " Battle.AOE2RECORD "}) == (
        "replay:battle.aoe2record"
    )


def test_classify_current_result_requires_complete_team_winner():
    row = game_row(
        20,
        players=[
            {"name": "A", "team_id": 0},
            {"name": "B", "team_id": 0},
            {"name": "C", "team_id": 1},
            {"name": "D", "team_id": 1},
        ],
        winner="A",
        key_events={
            "team_resolution": {
                "status": "resolved",
                "teams": [
                    {"team_id": 0, "players": ["A", "B"]},
                    {"team_id": 1, "players": ["C", "D"]},
                ],
                "winning_team_id": None,
            }
        },
    )

    assert classify_current_result(row, manually_verified=False) == (
        "team_result_revalidation_required"
    )
    row["key_events"]["team_resolution"]["winning_team_id"] = 0
    assert classify_current_result(row, manually_verified=False) == (
        "resolved_coherent_team"
    )


def test_archive_parser_validates_hash_layout(tmp_path: Path):
    replay_hash = "ab" + "cd" + "0" * 60
    good = tmp_path / "ab" / "cd" / f"{replay_hash}.aoe2record"
    good.parent.mkdir(parents=True)
    good.write_bytes(b"replay")

    parsed = parse_archive_object(tmp_path, good)
    assert parsed is not None
    assert parsed.layout_valid is True
    assert archive_profile([parsed], [])["unique_hashes"] == 1
    assert verify_archive_content_hashes(tmp_path, [parsed]) == [
        {
            "relative_path": f"ab/cd/{replay_hash}.aoe2record",
            "expected_sha256": replay_hash,
            "actual_sha256": "ac203c9843b5bd8c883e07039ff82820c94422010be6108bb82403ca25376a22",
        }
    ]


def test_full_vault_manifest_uses_artifact_grain_and_best_provenance_links():
    replay_hash = "a" * 64
    other_hash = "b" * 64
    objects = [
        ArchiveObject(
            sha256=other_hash,
            suffix=".aoe2mpgame",
            relative_path=f"bb/bb/{other_hash}.aoe2mpgame",
            byte_size=222,
            layout_valid=True,
        ),
        ArchiveObject(
            sha256=replay_hash,
            suffix=".aoe2record",
            relative_path=f"aa/aa/{replay_hash}.aoe2record",
            byte_size=111,
            layout_valid=True,
        ),
    ]
    live = game_row(30, replay_hash=replay_hash, is_final=False)
    final = game_row(
        31,
        replay_hash=replay_hash,
        original_filename="Jim-final.aoe2record",
    )
    attempts = [
        {
            "id": 80,
            "replay_hash": replay_hash,
            "game_stats_id": 30,
            "user_uid": "u_other",
            "original_filename": "early.aoe2record",
            "created_at": "2026-07-14T00:00:00+00:00",
        },
        {
            "id": 81,
            "replay_hash": replay_hash,
            "game_stats_id": 31,
            "user_uid": JIM_UID,
            "original_filename": "Jim-final.aoe2record",
            "created_at": "2026-07-14T00:01:00+00:00",
        },
    ]

    rows = build_full_artifact_manifest_rows(objects, [live, final], attempts)

    assert [row["replay_hash"] for row in rows] == [replay_hash, other_hash]
    assert rows[0]["game_stats_id"] == 31
    assert rows[0]["legacy_parse_attempt_id"] == 81
    assert rows[0]["submitter_uid"] == JIM_UID
    assert rows[0]["logical_replay_key"] == f"artifact:{replay_hash}"
    assert rows[1]["game_stats_id"] == ""
    assert rows[1]["legacy_parse_attempt_id"] == ""
