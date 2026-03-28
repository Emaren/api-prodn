import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.backfill_hd_duration_units import should_normalize_duration


def build_row(**overrides):
    defaults = {
        "game_version": "Version.HD",
        "parse_source": "file_upload",
        "duration": 6007,
        "game_duration": 6007,
        "key_events": {},
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_should_not_re_normalize_already_normalized_seconds():
    row = build_row(duration=6007, game_duration=6007, key_events={"duration_source": "mgz_summary_ms_normalized"})
    assert should_normalize_duration(row) is False


def test_should_require_confident_millisecond_values_from_legacy_rows():
    assert should_normalize_duration(build_row(duration=6007, game_duration=6007)) is False
    assert should_normalize_duration(build_row(duration=600_7872, game_duration=600_7872)) is True
    assert should_normalize_duration(build_row(duration=27, game_duration=27, key_events={"raw_duration_ms": 26_460})) is True
