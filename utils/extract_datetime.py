import os
import re
from datetime import datetime, timezone

_FILENAME_DATETIME_PATTERNS = (
    re.compile(
        r"@?(?P<year>\d{4})[._-](?P<month>\d{2})[._-](?P<day>\d{2})[ T_-]?"
        r"(?P<hour>\d{2})[:._-]?(?P<minute>\d{2})[:._-]?(?P<second>\d{2})"
    ),
    re.compile(
        r"(?<!\d)(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})[ T_-]?"
        r"(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})(?!\d)"
    ),
)


def _extract_datetime_from_text(value):
    if not value:
        return None

    for pattern in _FILENAME_DATETIME_PATTERNS:
        match = pattern.search(value)
        if not match:
            continue

        try:
            return datetime(
                int(match.group("year")),
                int(match.group("month")),
                int(match.group("day")),
                int(match.group("hour")),
                int(match.group("minute")),
                int(match.group("second")),
            )
        except ValueError:
            continue

    return None


def extract_datetime_from_filename(fname):
    parsed = _extract_datetime_from_text(os.path.basename(fname or ""))
    if parsed is not None:
        return parsed

    if fname and os.path.isfile(fname):
        try:
            return datetime.fromtimestamp(
                os.path.getmtime(fname),
                tz=timezone.utc,
            ).replace(tzinfo=None)
        except OSError:
            return None

    return None
