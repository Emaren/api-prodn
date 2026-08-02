from datetime import datetime, timedelta, timezone

from app import _public_match_sort_key, _public_sort_datetime


class GameStub:
    def __init__(self, game_id, played_at, timestamp=None, created_at=None):
        self.id = game_id
        self._played_at = played_at
        self.timestamp = timestamp
        self.created_at = created_at

    def public_played_at(self):
        return self._played_at


def test_public_sort_datetime_preserves_legacy_naive_wall_time():
    value = datetime(2026, 8, 2, 5, 16, 0)
    assert _public_sort_datetime(value) == value
    assert _public_sort_datetime(value).tzinfo is None


def test_public_sort_datetime_normalizes_absolute_instants_to_utc_naive():
    mountain = timezone(timedelta(hours=-6))
    value = datetime(2026, 8, 1, 23, 17, 15, tzinfo=mountain)
    assert _public_sort_datetime(value) == datetime(2026, 8, 2, 5, 17, 15)
    assert _public_sort_datetime(value).tzinfo is None


def test_public_match_sort_key_handles_mixed_naive_and_aware_values():
    aware = GameStub(
        2,
        datetime(2026, 8, 2, 5, 17, 15, tzinfo=timezone.utc),
        timestamp=datetime(2026, 8, 2, 5, 17, 41),
    )
    legacy_naive = GameStub(
        1,
        datetime(2026, 8, 2, 5, 16, 0),
        timestamp=datetime(2026, 8, 2, 5, 16, 30),
    )
    ordered = sorted([legacy_naive, aware], key=_public_match_sort_key, reverse=True)
    assert [game.id for game in ordered] == [2, 1]


def test_public_match_sort_key_handles_mixed_parsed_at_values():
    first = GameStub(
        1,
        datetime(2026, 8, 2, 5, 17, 15, tzinfo=timezone.utc),
        timestamp=datetime(2026, 8, 2, 5, 17, 40),
    )
    second = GameStub(
        2,
        datetime(2026, 8, 2, 5, 17, 15, tzinfo=timezone.utc),
        timestamp=datetime(2026, 8, 2, 5, 17, 41, tzinfo=timezone.utc),
    )
    ordered = sorted([first, second], key=_public_match_sort_key, reverse=True)
    assert [game.id for game in ordered] == [2, 1]
