import json
import sys
from pathlib import Path
from urllib import error

sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.game_stats_public_cache import (
    invalidate_game_stats_cache,
    read_game_stats_cache,
    read_game_stats_cache_generation,
    write_game_stats_cache,
)
from utils import replay_post_ingest_bridge


def test_final_ingest_invalidation_clears_every_limit_variant():
    invalidate_game_stats_cache()
    write_game_stats_cache("limit:128", [{"id": 1}], 90)
    write_game_stats_cache("limit:160", [{"id": 2}], 90)

    assert read_game_stats_cache("limit:128") == [{"id": 1}]
    assert read_game_stats_cache("limit:160") == [{"id": 2}]
    assert invalidate_game_stats_cache() == 2
    assert read_game_stats_cache("limit:128") is None
    assert read_game_stats_cache("limit:160") is None


def test_invalidation_rejects_an_inflight_stale_generation_refill():
    invalidate_game_stats_cache()
    lookup = read_game_stats_cache_generation("limit:128")
    assert lookup.payload is None

    # A final commit can invalidate while the cache-miss DB query is in flight.
    # Advancing the generation matters even when there was no stored entry yet.
    assert invalidate_game_stats_cache() == 0
    assert write_game_stats_cache(
        "limit:128",
        [{"id": "stale"}],
        90,
        expected_generation=lookup.generation,
    ) is False
    assert read_game_stats_cache("limit:128") is None

    fresh_lookup = read_game_stats_cache_generation("limit:128")
    assert write_game_stats_cache(
        "limit:128",
        [{"id": "fresh"}],
        90,
        expected_generation=fresh_lookup.generation,
    ) is True
    assert read_game_stats_cache("limit:128") == [{"id": "fresh"}]


def test_game_stats_route_conditionally_refills_the_generation_it_read():
    app_source = Path("app.py").read_text()
    assert "read_game_stats_cache_generation(cache_key)" in app_source
    assert "expected_generation=cache_lookup.generation" in app_source
    assert "if not cache_written" in app_source


def test_game_stats_cache_enforces_a_deterministic_twelve_variant_lru():
    invalidate_game_stats_cache()
    for limit in range(1, 13):
        write_game_stats_cache(f"limit:{limit}", [{"id": limit}], 90)

    # Refresh limit:1, making limit:2 the least-recently-used variant.
    assert read_game_stats_cache("limit:1") == [{"id": 1}]
    write_game_stats_cache("limit:13", [{"id": 13}], 90)

    assert read_game_stats_cache("limit:2") is None
    assert read_game_stats_cache("limit:1") == [{"id": 1}]
    assert read_game_stats_cache("limit:13") == [{"id": 13}]
    assert invalidate_game_stats_cache() == 12


def test_direct_final_bridge_posts_exact_receipt(monkeypatch):
    observed = {}

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"ok":true}'

    def fake_urlopen(bridge_request, timeout):
        observed["url"] = bridge_request.full_url
        observed["key"] = bridge_request.headers["X-api-key"]
        observed["body"] = json.loads(bridge_request.data.decode("utf-8"))
        observed["timeout"] = timeout
        return Response()

    monkeypatch.setenv("INTERNAL_API_KEY", "test-internal-key")
    monkeypatch.setenv(
        "AOE2_WEB_REPLAY_POST_INGEST_URL",
        "http://127.0.0.1:3030/api/replay/post-ingest",
    )
    monkeypatch.setenv("AOE2_WEB_REPLAY_POST_INGEST_TIMEOUT_SECONDS", "7")
    monkeypatch.setattr(replay_post_ingest_bridge.request, "urlopen", fake_urlopen)

    receipt = {
        "game_id": 22262,
        "is_final": True,
        "effective_is_final": True,
        "finality_status": "final_recorded",
    }
    assert replay_post_ingest_bridge.notify_replay_post_ingest(receipt) is True

    assert observed == {
        "url": "http://127.0.0.1:3030/api/replay/post-ingest",
        "key": "test-internal-key",
        "body": {"source": "api_direct", "receipt": receipt},
        "timeout": 7.0,
    }


def test_direct_final_bridge_retries_transient_failures_then_succeeds(monkeypatch):
    attempts = []
    delays = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"ok":true}'

    def fake_urlopen(_bridge_request, timeout):
        attempts.append(timeout)
        if len(attempts) < 3:
            raise error.URLError("web coordinator unavailable")
        return Response()

    monkeypatch.setenv("INTERNAL_API_KEY", "test-internal-key")
    monkeypatch.setenv("AOE2_WEB_REPLAY_POST_INGEST_MAX_ATTEMPTS", "3")
    monkeypatch.setenv("AOE2_WEB_REPLAY_POST_INGEST_TIMEOUT_SECONDS", "2")
    monkeypatch.setenv("AOE2_WEB_REPLAY_POST_INGEST_RETRY_BASE_SECONDS", "0.1")
    monkeypatch.setattr(replay_post_ingest_bridge.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(replay_post_ingest_bridge.time, "sleep", delays.append)

    assert replay_post_ingest_bridge.notify_replay_post_ingest({"game_id": 7}) is True
    assert attempts == [2.0, 2.0, 2.0]
    assert delays == [0.1, 0.2]


def test_direct_final_bridge_does_not_retry_permanent_http_failure(monkeypatch):
    attempts = []

    def fake_urlopen(bridge_request, timeout):
        attempts.append(timeout)
        raise error.HTTPError(
            bridge_request.full_url,
            401,
            "unauthorized",
            hdrs=None,
            fp=None,
        )

    monkeypatch.setenv("INTERNAL_API_KEY", "test-internal-key")
    monkeypatch.setenv("AOE2_WEB_REPLAY_POST_INGEST_MAX_ATTEMPTS", "4")
    monkeypatch.setattr(replay_post_ingest_bridge.request, "urlopen", fake_urlopen)

    assert replay_post_ingest_bridge.notify_replay_post_ingest({"game_id": 8}) is False
    assert attempts == [5.0]


def test_web_proxy_upload_is_marked_as_post_ingest_owner():
    replay_source = Path("routes/replay_routes_async.py").read_text()
    assert 'alias="x-post-ingest-owner"' in replay_source
    assert '!= "web_proxy"' in replay_source
    assert "invalidate_game_stats_cache()" in replay_source
    assert "background_tasks.add_task(" in replay_source
