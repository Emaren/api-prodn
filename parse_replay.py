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

ENDPOINTS = {
    "local": "http://localhost:8002/api/parse_replay",
    "render": "https://aoe2hd-parser-api.onrender.com/api/parse_replay"
}

# ───────────────────────────────────────────────
# 🧠 Core
# ───────────────────────────────────────────────
async def parse_and_send(replay_path: str, force: bool = False, parse_iteration: int = 1, is_final: bool = True):
    if not os.path.exists(replay_path):
        logging.error(f"❌ Replay not found: {replay_path}")
        return

    logging.info(f"📄 Parsing replay: {replay_path}")
    parsed = await parse_replay_full(replay_path)
    if not parsed:
        logging.warning(f"⚠️ Failed to parse: {replay_path}")
        return

    parsed["played_on"] = extract_datetime_from_filename(os.path.basename(replay_path)).isoformat() if extract_datetime_from_filename(os.path.basename(replay_path)) else None
    parsed["replay_file"] = replay_path
    parsed["parse_iteration"] = parse_iteration
    parsed["is_final"] = is_final
    parsed["replay_hash"] = await hash_replay_file(replay_path)
    parsed["game_duration"] = parsed.get("duration") or parsed.get("header", {}).get("duration") or None

    # Optional local dump
    try:
        with open(replay_path + ".json", "w") as f:
            json.dump(parsed, f, indent=2)
    except Exception as e:
        logging.warning(f"❌ Could not save .json: {e}")

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
            token = os.environ.get("FIREBASE_AUTH_TOKEN") or config.get("firebase_auth_token")
            if token:
                headers["Authorization"] = f"Bearer {token}"

            response = requests.post(full_url, json=parsed, headers=headers)

            if response.ok:
                logging.info(f"✅ [{target}] Response: {response.status_code} - {response.text}")
            else:
                logging.error(f"❌ [{target}] Error: {response.status_code} - {response.text}")
        except Exception as exc:
            logging.error(f"❌ [{target}] API failed: {exc}")

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

            files = [f for f in os.listdir(path) if f.endswith(".aoe2record") or f.endswith(".mgz")]
            files.sort(key=lambda f: extract_datetime_from_filename(f) or datetime.min, reverse=True)

            for fname in files:
                full_path = os.path.join(path, fname)
                await parse_and_send(full_path, force=args.force)

if __name__ == "__main__":
    asyncio.run(main())
