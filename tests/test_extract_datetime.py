import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.extract_datetime import extract_datetime_from_filename


def test_extract_datetime_supports_classic_watcher_filename():
    parsed = extract_datetime_from_filename("emaren @2026.04.02 193455.aoe2record")

    assert parsed == datetime(2026, 4, 2, 19, 34, 55)


def test_extract_datetime_supports_compact_filename_stamps():
    parsed = extract_datetime_from_filename("replay_20260402_193455.aoe2record")

    assert parsed == datetime(2026, 4, 2, 19, 34, 55)


def test_extract_datetime_falls_back_to_file_mtime_for_local_paths(tmp_path):
    replay_path = tmp_path / "legacy-save.aoe2record"
    replay_path.write_bytes(b"stub")

    expected = datetime(2023, 7, 9, 15, 22, 11, tzinfo=timezone.utc).timestamp()
    os.utime(replay_path, (expected, expected))

    parsed = extract_datetime_from_filename(str(replay_path))

    assert parsed == datetime.fromtimestamp(expected, tz=timezone.utc).replace(tzinfo=None)
