"""Small process-local cache for the public replay list.

The cache is intentionally owned outside ``app.py`` so replay ingestion can
invalidate every limit variant immediately after durable final truth changes.
HTTP ``no-store`` headers cannot invalidate an application-memory cache.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any


_CACHE_LOCK = threading.Lock()
_GAME_STATS_CACHE_MAX_VARIANTS = 12
_GAME_STATS_CACHE: OrderedDict[
    str,
    tuple[float, list[dict[str, Any]]],
] = OrderedDict()
_GAME_STATS_CACHE_GENERATION = 0


@dataclass(frozen=True)
class GameStatsCacheLookup:
    """One atomic cache read and the generation that authorized a refill."""

    payload: list[dict[str, Any]] | None
    generation: int


def read_game_stats_cache_generation(
    cache_key: str,
) -> GameStatsCacheLookup:
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _GAME_STATS_CACHE.get(cache_key)
        if cached and cached[0] <= now:
            _GAME_STATS_CACHE.pop(cache_key, None)
            cached = None
        elif cached:
            _GAME_STATS_CACHE.move_to_end(cache_key)

        return GameStatsCacheLookup(
            payload=cached[1] if cached else None,
            generation=_GAME_STATS_CACHE_GENERATION,
        )


def read_game_stats_cache(cache_key: str) -> list[dict[str, Any]] | None:
    return read_game_stats_cache_generation(cache_key).payload


def write_game_stats_cache(
    cache_key: str,
    payload: list[dict[str, Any]],
    ttl_seconds: float,
    *,
    expected_generation: int | None = None,
) -> bool:
    expires_at = time.monotonic() + max(0.0, ttl_seconds)
    with _CACHE_LOCK:
        if (
            expected_generation is not None
            and expected_generation != _GAME_STATS_CACHE_GENERATION
        ):
            return False

        _GAME_STATS_CACHE[cache_key] = (expires_at, payload)
        _GAME_STATS_CACHE.move_to_end(cache_key)

        now = time.monotonic()
        for key, value in list(_GAME_STATS_CACHE.items()):
            if value[0] <= now:
                _GAME_STATS_CACHE.pop(key, None)

        while len(_GAME_STATS_CACHE) > _GAME_STATS_CACHE_MAX_VARIANTS:
            _GAME_STATS_CACHE.popitem(last=False)

        return True


def invalidate_game_stats_cache() -> int:
    """Advance truth generation, drop variants, and return the number dropped."""

    global _GAME_STATS_CACHE_GENERATION
    with _CACHE_LOCK:
        invalidated = len(_GAME_STATS_CACHE)
        _GAME_STATS_CACHE_GENERATION += 1
        _GAME_STATS_CACHE.clear()
        return invalidated
