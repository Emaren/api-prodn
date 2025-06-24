import os
import sys
import logging
import json
from datetime import timedelta

# Import the real HD parser
from mgz_hd.parser_hd import parse_hd_replay as real_parse_hd

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')


def format_duration(seconds: int) -> str:
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {secs}s" if hours else f"{minutes}m {secs}s"


def normalize_parsed_hd_data(raw, filepath):
    # Ensure all expected fields are present and mapped correctly
    players = raw.get("players", [])
    civs = raw.get("civs", [])
    duration = raw.get("duration", 0)
    version = raw.get("version", "Unknown")
    map_name = raw.get("map", "Unknown")
    winner = raw.get("winner", "Unknown")

    # Merge civs into players if needed
    for i, player in enumerate(players):
        if isinstance(player, dict):
            player.setdefault("civilization", civs[i] if i < len(civs) else "Unknown")
            player.setdefault("winner", player.get("name") == winner)
            player.setdefault("team", "Unknown")
            player.setdefault("score", None)
            player.setdefault("apm", 0)
            player.setdefault("military_score", 0)
            player.setdefault("economy_score", 0)
            player.setdefault("technology_score", 0)
            player.setdefault("society_score", 0)
            player.setdefault("units_killed", 0)
            player.setdefault("buildings_destroyed", 0)
            player.setdefault("resources_gathered", 0)
            player.setdefault("fastest_castle_age", 0)
            player.setdefault("fastest_imperial_age", 0)
            player.setdefault("relics_collected", 0)

    return {
        "replay_file": filepath,
        "game_version": version,
        "duration": duration,
        "game_duration": format_duration(duration),
        "map_name": map_name,
        "map_size": "Unknown",
        "game_type": "HD Replay",
        "players": players,
        "winner": winner,
        "unique_event_types": [],
        "key_events": []
    }


def parse_hd_replay(filepath):
    if not os.path.exists(filepath):
        logging.error(f"🚫 HD parser error: Filepath not found: {filepath}")
        return {}

    logging.info("🎮 Parsing AoE2 HD replay: %s", filepath)

    try:
        raw_data = real_parse_hd(filepath)
        if raw_data is None:
            logging.error("❌ HD parser returned None.")
            return {}

        parsed_data = normalize_parsed_hd_data(raw_data, filepath)

        outpath = filepath + ".json"
        with open(outpath, "w") as f:
            json.dump(parsed_data, f, indent=2)
        logging.info("✅ Dumped parsed HD replay to %s", outpath)

        return parsed_data

    except Exception as e:
        logging.error(f"❌ Failed to parse HD replay: {e}")
        return {}


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python parse_hd.py <path_to_aoe2record>")
        sys.exit(1)

    replay_path = sys.argv[1]
    if not os.path.exists(replay_path):
        logging.error("File not found: %s", replay_path)
        sys.exit(1)

    parsed = parse_hd_replay(replay_path)

    # Optional: POST to API
    # import requests
    # response = requests.post("http://localhost:8002/api/parse_replay", json=parsed)
    # logging.info("📤 API Response: %s - %s", response.status_code, response.text)
