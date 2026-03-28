import asyncio
import math
import os
import sys
from pathlib import Path

from sqlalchemy import select

sys.path.append(str(Path(__file__).resolve().parents[1]))

from db.db import async_session  # noqa: E402
from db.models import GameStats  # noqa: E402


EARLY_EXIT_PARSE_REASON = "hd_early_exit_under_60s"
MIN_CONFIDENT_MILLISECONDS_DURATION = 60_000


def normalize_duration_seconds(raw_value):
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
        return None

    numeric = float(raw_value)
    if numeric <= 0:
        return None

    return max(1, int(math.ceil(numeric / 1000.0)))


def should_normalize_duration(row):
    if str(row.game_version or "").strip() != "Version.HD":
        return False

    if row.parse_source not in {"file_upload", "json_parse"}:
        return False

    key_events = dict(row.key_events or {})
    duration_source = str(key_events.get("duration_source") or "").strip()
    if duration_source == "mgz_summary_ms_normalized":
        return False

    raw_duration = key_events.get("raw_duration_ms")
    if isinstance(raw_duration, (int, float)) and raw_duration >= 480:
        return True

    duration_value = row.duration or row.game_duration or 0
    return isinstance(duration_value, int) and duration_value >= MIN_CONFIDENT_MILLISECONDS_DURATION


def mark_early_exit_if_needed(row, normalized_seconds):
    key_events = dict(row.key_events or {})
    event_types = list(row.event_types or [])
    resigned_numbers = key_events.get("resigned_player_numbers") or []
    rated = bool(key_events.get("rated"))
    has_resign = "resign" in event_types or bool(resigned_numbers)

    if not rated or normalized_seconds >= 60 or not (has_resign or row.disconnect_detected):
        row.key_events = key_events
        return False

    players = []
    for player in list(row.players or []):
        updated = dict(player)
        updated["winner"] = None
        players.append(updated)

    if row.winner and row.winner != "Unknown":
        key_events["suppressed_winner"] = row.winner

    key_events["completed"] = False
    key_events["early_exit_under_60s"] = True
    key_events["no_rated_result"] = True
    key_events["early_exit_seconds"] = normalized_seconds

    row.players = players
    row.winner = "Unknown"
    row.disconnect_detected = True
    row.parse_reason = EARLY_EXIT_PARSE_REASON
    row.key_events = key_events
    return True


async def main():
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"

    async with async_session() as session:
        result = await session.execute(select(GameStats).order_by(GameStats.id.asc()))
        rows = list(result.scalars())

        normalized = 0
        early_exit = 0

        for row in rows:
            if not should_normalize_duration(row):
                continue

            raw_duration = row.duration or row.game_duration or 0
            normalized_seconds = normalize_duration_seconds(raw_duration)
            if not normalized_seconds:
                continue

            key_events = dict(row.key_events or {})
            key_events["raw_duration_ms"] = int(raw_duration)
            key_events["duration_source"] = "mgz_summary_ms_normalized"
            row.key_events = key_events
            row.duration = normalized_seconds
            row.game_duration = normalized_seconds
            normalized += 1

            if mark_early_exit_if_needed(row, normalized_seconds):
                early_exit += 1

        if dry_run:
            await session.rollback()
            print(f"dry-run normalized={normalized} early_exit={early_exit}")
            return

        await session.commit()
        print(f"normalized={normalized} early_exit={early_exit}")


if __name__ == "__main__":
    asyncio.run(main())
