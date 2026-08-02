import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from db.models.game_stats import GameStats


def test_public_played_at_prefers_absolute_watcher_file_mtime():
    game = GameStats(
        replay_file="legacy-save.aoe2record",
        replay_hash="hash-a",
        played_on=datetime(2024, 5, 1, 12, 0, 0),
        timestamp=datetime(2026, 1, 1, 12, 0, 0),
        created_at=datetime(2025, 1, 1, 12, 0, 0),
        key_events={"watcher_upload": {"file_mtime_ms": 1714564800000}},
    )

    assert game.public_played_at() == datetime(2024, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert game.public_played_at_details() == (
        datetime(2024, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
        "watcher_file_mtime",
        True,
    )


def test_public_played_at_prefers_explicit_played_on_without_watcher_mtime():
    game = GameStats(
        replay_file="legacy-save.aoe2record",
        replay_hash="hash-b",
        played_on=datetime(2024, 5, 1, 12, 0, 0),
        timestamp=datetime(2026, 1, 1, 12, 0, 0),
        created_at=datetime(2025, 1, 1, 12, 0, 0),
    )

    assert game.public_played_at() == datetime(2024, 5, 1, 12, 0, 0)


def test_public_played_at_uses_filename_stamp_before_row_bookkeeping():
    game = GameStats(
        replay_file="fallback.aoe2record",
        replay_hash="hash-c",
        original_filename="ranked_20231224_081530.aoe2record",
        played_on=None,
        timestamp=datetime(2026, 1, 1, 12, 0, 0),
        created_at=datetime(2025, 1, 1, 12, 0, 0),
    )

    assert game.public_played_at() == datetime(2023, 12, 24, 8, 15, 30)


def test_to_dict_marks_absolute_times_and_serializes_utc_with_z():
    game = GameStats(
        replay_file="proof.aoe2record",
        replay_hash="hash-d",
        created_at=datetime(2026, 8, 2, 5, 21, 48),
        timestamp=datetime(2026, 8, 2, 5, 21, 49),
        played_on=datetime(2026, 8, 1, 22, 37, 20),
        key_events={"watcher_upload": {"file_mtime_ms": 1785648141218}},
    )

    payload = game.to_dict()

    assert payload["created_at"].endswith("Z")
    assert payload["timestamp"].endswith("Z")
    assert payload["played_on"] == "2026-08-01T22:37:20"
    assert payload["played_at"].endswith("Z")
    assert payload["played_at_source"] == "watcher_file_mtime"
    assert payload["played_at_is_absolute"] is True
    assert payload["watcher_file_mtime"] == payload["played_at"]
