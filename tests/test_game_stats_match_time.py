import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from db.models.game_stats import GameStats


def test_public_played_at_prefers_explicit_played_on():
    game = GameStats(
        replay_file="legacy-save.aoe2record",
        replay_hash="hash-a",
        played_on=datetime(2024, 5, 1, 12, 0, 0),
        timestamp=datetime(2026, 1, 1, 12, 0, 0),
        created_at=datetime(2025, 1, 1, 12, 0, 0),
    )

    assert game.public_played_at() == datetime(2024, 5, 1, 12, 0, 0)


def test_public_played_at_uses_filename_stamp_before_row_bookkeeping():
    game = GameStats(
        replay_file="fallback.aoe2record",
        replay_hash="hash-b",
        original_filename="ranked_20231224_081530.aoe2record",
        played_on=None,
        timestamp=datetime(2026, 1, 1, 12, 0, 0),
        created_at=datetime(2025, 1, 1, 12, 0, 0),
    )

    assert game.public_played_at() == datetime(2023, 12, 24, 8, 15, 30)
