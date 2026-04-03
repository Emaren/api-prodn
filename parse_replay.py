import os
import sys
import json
import logging
import argparse
import asyncio
from datetime import datetime
import requests

# Local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.replay_parser import parse_replay_full, hash_replay_file
from config import load_config, get_api_targets
from utils.extract_datetime import extract_datetime_from_filename

# ───────────────────────────────────────────────
# 🔧 Setup
# ───────────────────────────────────────────────
config = load_config()
api_targets = get_api_targets() or config.get("api_targets", ["local"])

LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", config.get("logging_level", "DEBUG")).upper()
logging.basicConfig(level=getattr(logging, LOGGING_LEVEL, logging.DEBUG))

PRODUCTION_API_BASE = (
    os.environ.get("AOE2_API_BASE_URL")
    or config.get("aoe2_api_base_url")
    or "https://api-prodn.aoe2hdbets.com"
).rstrip("/")

ENDPOINTS = {
    "local": "http://localhost:8002/api/parse_replay",
    "render": f"{PRODUCTION_API_BASE}/api/parse_replay",
    "production": f"{PRODUCTION_API_BASE}/api/parse_replay",
}

# ───────────────────────────────────────────────
# 🧠 Core
# ───────────────────────────────────────────────
async def parse_and_send(
    replay_path: str,
    force: bool = False,
    parse_iteration: int = 1,
    is_final: bool = True,
    parse_source: str | None = None,
    parse_reason: str | None = None,
    original_filename: str | None = None,
):
    if not os.path.exists(replay_path):
        logging.error(f"❌ Replay not found: {replay_path}")
        return False

    logging.info(f"📄 Parsing replay: {replay_path}")
    parsed = await parse_replay_full(replay_path)
    if not parsed:
        logging.warning(f"⚠️ Failed to parse: {replay_path}")
        return False

    replay_name = original_filename or os.path.basename(replay_path)
    played_on = extract_datetime_from_filename(replay_name) or extract_datetime_from_filename(
        replay_path
    )
    parsed["played_on"] = played_on.isoformat() if played_on else None
    parsed["replay_file"] = replay_path
    parsed["original_filename"] = replay_name
    parsed["parse_iteration"] = parse_iteration
    parsed["is_final"] = is_final
    parsed["parse_source"] = parse_source or ("watcher_final" if is_final else "watcher_live")
    parsed["parse_reason"] = parse_reason or (
        "watcher_final_submission" if is_final else "watcher_live_iteration"
    )
    parsed["replay_hash"] = await hash_replay_file(replay_path)
    parsed["game_duration"] = parsed.get("duration") or parsed.get("header", {}).get("duration") or None

    # Optional local dump
    try:
        with open(replay_path + ".json", "w") as f:
            json.dump(parsed, f, indent=2)
    except Exception as e:
        logging.warning(f"❌ Could not save .json: {e}")

    sent_successfully = False

    for target in api_targets:
        url = ENDPOINTS.get(target) or target
        full_url = url
        if force:
            full_url += "?force=true"
        elif is_final:
            full_url += "?mode=final"

        try:
            logging.info(f"📤 Sending to [{target}] → {parsed['replay_file']}")
            headers = {}
            user_uid = (
                os.environ.get("WATCHER_USER_UID")
                or os.environ.get("USER_UID")
                or config.get("watcher_user_uid")
                or "system"
            )
            headers["x-user-uid"] = user_uid

            api_key = (
                os.environ.get("INTERNAL_API_KEY")
                or config.get("internal_api_key")
                or os.environ.get("API_INTERNAL_KEY")
            )
            if api_key:
                headers["x-api-key"] = api_key

            token = (
                os.environ.get("API_AUTH_TOKEN")
                or config.get("api_auth_token")
                or os.environ.get("AUTH_TOKEN")
                or config.get("auth_token")
            )
            if token:
                headers["Authorization"] = f"Bearer {token}"

            response = requests.post(full_url, json=parsed, headers=headers, timeout=60)

            if response.ok:
                sent_successfully = True
                logging.info(f"✅ [{target}] Response: {response.status_code} - {response.text}")
            elif response.status_code in {404, 405} and is_final:
                fallback_url = full_url.replace("/api/parse_replay", "/api/replay/upload")
                logging.warning(
                    f"🔁 [{target}] JSON route unavailable; retrying final upload as file → {fallback_url}"
                )
                upload_headers = {"x-user-uid": user_uid}
                if api_key:
                    upload_headers["x-api-key"] = api_key
                with open(replay_path, "rb") as replay_handle:
                    fallback = requests.post(
                        fallback_url,
                        files={"file": (os.path.basename(replay_path), replay_handle)},
                        headers=upload_headers,
                        timeout=90,
                    )
                if fallback.ok:
                    sent_successfully = True
                    logging.info(
                        f"✅ [{target}] File-upload fallback succeeded: {fallback.status_code} - {fallback.text}"
                    )
                else:
                    logging.error(
                        f"❌ [{target}] File-upload fallback failed: {fallback.status_code} - {fallback.text}"
                    )
            else:
                logging.error(f"❌ [{target}] Error: {response.status_code} - {response.text}")
        except Exception as exc:
            logging.error(f"❌ [{target}] API failed: {exc}")

    return sent_successfully

# ───────────────────────────────────────────────
# 🧪 Entrypoint
# ───────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="Parse and upload AoE2 replay.")
    parser.add_argument("replay_path", nargs="?", help="Path to .aoe2record or .mgz replay")
    parser.add_argument("--force", action="store_true", help="Force re-upload even if marked as final")
    args = parser.parse_args()

    if args.replay_path:
        await parse_and_send(args.replay_path, force=args.force)
    else:
        logging.info("🔍 Scanning for replays from config...")
        SAVEGAME_DIRS = config.get("replay_directories") or []
        for path in SAVEGAME_DIRS:
            if not os.path.exists(path):
                logging.warning(f"⚠️ Missing: {path}")
                continue

            files = [
                f
                for f in os.listdir(path)
                if f.endswith((".aoe2record", ".aoe2mpgame", ".mgz", ".mgx", ".mgl"))
            ]
            files.sort(
                key=lambda f: extract_datetime_from_filename(os.path.join(path, f))
                or datetime.min,
                reverse=True,
            )

            for fname in files:
                full_path = os.path.join(path, fname)
                await parse_and_send(full_path, force=args.force)

if __name__ == "__main__":
    asyncio.run(main())
