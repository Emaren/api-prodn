"""Notify the web-owned post-ingest coordinator after direct API commits.

Watcher clients historically uploaded directly to FastAPI. The AoE2WAR web
application owns result adjudication, identity projection, tournament proof,
and market reconciliation, so direct live/final uploads must hand the exact
durable receipt across that boundary. The call runs as a FastAPI background
task and is idempotent on the web side.
"""

from __future__ import annotations

import json
import logging
import os
import time
from urllib import error, request


logger = logging.getLogger("aoe2hdbets.replay_post_ingest")


def _bounded_integer_env(
    name: str,
    fallback: int,
    minimum: int,
    maximum: int,
) -> int:
    try:
        parsed = int(os.getenv(name, "").strip())
    except (TypeError, ValueError):
        return fallback
    return min(maximum, max(minimum, parsed))


def _bounded_float_env(
    name: str,
    fallback: float,
    minimum: float,
    maximum: float,
) -> float:
    try:
        parsed = float(os.getenv(name, "").strip())
    except (TypeError, ValueError):
        return fallback
    return min(maximum, max(minimum, parsed))


def _retryable_status(status: int) -> bool:
    return status in {408, 425, 429} or status >= 500


def notify_replay_post_ingest(payload: dict) -> bool:
    internal_key = os.getenv("INTERNAL_API_KEY", "").strip()
    if not internal_key:
        logger.error(
            "Replay post-ingest bridge skipped: INTERNAL_API_KEY is not configured"
        )
        return False

    endpoint = os.getenv(
        "AOE2_WEB_REPLAY_POST_INGEST_URL",
        "http://127.0.0.1:3030/api/replay/post-ingest",
    ).strip()
    if not endpoint:
        logger.error("Replay post-ingest bridge skipped: endpoint is empty")
        return False

    max_attempts = _bounded_integer_env(
        "AOE2_WEB_REPLAY_POST_INGEST_MAX_ATTEMPTS",
        3,
        1,
        4,
    )
    timeout_seconds = _bounded_float_env(
        "AOE2_WEB_REPLAY_POST_INGEST_TIMEOUT_SECONDS",
        5.0,
        1.0,
        10.0,
    )
    retry_base_seconds = _bounded_float_env(
        "AOE2_WEB_REPLAY_POST_INGEST_RETRY_BASE_SECONDS",
        0.25,
        0.0,
        2.0,
    )

    body = json.dumps({"source": "api_direct", "receipt": payload}).encode("utf-8")
    bridge_request = request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "content-type": "application/json",
            "x-api-key": internal_key,
        },
    )

    for attempt in range(1, max_attempts + 1):
        retry_detail: str | None = None
        try:
            with request.urlopen(
                bridge_request,
                timeout=timeout_seconds,
            ) as response:
                status = int(response.status)
                response.read()
                if 200 <= status < 300:
                    logger.info(
                        "Replay post-ingest bridge completed game_id=%s finality=%s attempt=%s",
                        payload.get("game_id"),
                        payload.get("finality_status"),
                        attempt,
                    )
                    return True

                if not _retryable_status(status):
                    logger.error(
                        "Replay post-ingest bridge returned permanent status=%s game_id=%s",
                        status,
                        payload.get("game_id"),
                    )
                    return False
                retry_detail = f"status={status}"
        except error.HTTPError as bridge_error:
            status = int(bridge_error.code)
            if not _retryable_status(status):
                logger.error(
                    "Replay post-ingest bridge returned permanent status=%s game_id=%s",
                    status,
                    payload.get("game_id"),
                )
                return False
            retry_detail = f"status={status} error={bridge_error}"
        except (error.URLError, TimeoutError, OSError) as bridge_error:
            retry_detail = str(bridge_error)

        if attempt >= max_attempts:
            logger.error(
                "Replay post-ingest bridge exhausted attempts=%s game_id=%s: %s",
                max_attempts,
                payload.get("game_id"),
                retry_detail,
            )
            return False

        retry_delay = min(
            2.0,
            retry_base_seconds * (2 ** (attempt - 1)),
        )
        logger.warning(
            "Replay post-ingest bridge retrying attempt=%s/%s game_id=%s delay=%.3fs: %s",
            attempt + 1,
            max_attempts,
            payload.get("game_id"),
            retry_delay,
            retry_detail,
        )
        if retry_delay > 0:
            time.sleep(retry_delay)

    return False
