import json
import logging
import requests
from parse_replay import parse_replay

# Configure logging (reuse similar settings as in parse_replay.py)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# In client.py
from config import load_config

config = load_config()
API_ENDPOINT = config.get("api_endpoint", "http://your-betting-app.example.com/api/replays")


def send_stats_to_backend(stats):
    """
    Send the parsed replay stats to the betting app's backend.
    """
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(API_ENDPOINT, json=stats, headers=headers, timeout=10)
        response.raise_for_status()
        logging.info("Successfully sent stats to backend.")
        return True
    except requests.RequestException as e:
        logging.error(f"Error sending stats: {e}")
        return False

def process_replay(replay_path):
    """
    Process a replay file: parse it and send its data to the backend.
    """
    logging.info(f"Processing replay: {replay_path}")
    print(f"📂 About to parse: {replay_path}")
    stats = parse_replay(replay_path)
    if stats is None:
        logging.error("Parsing failed, skipping backend submission.")
        return False
    return send_stats_to_backend(stats)

if __name__ == '__main__':
    # For testing, you can supply a replay file path via command line, for example:
    import sys
    if len(sys.argv) > 1:
        replay_file = sys.argv[1]
        process_replay(replay_file)
    else:
        logging.info("No replay file provided for testing.")
