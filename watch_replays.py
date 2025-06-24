import os
import time
import logging
import threading
import platform
import hashlib
import asyncio
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler

from config import load_config, get_api_targets
from parse_replay import parse_and_send
from utils.replay_parser import parse_replay_full
from utils.extract_datetime import extract_datetime_from_filename

# ───────────────────────────────────────────────
# 🔧 Config
# ───────────────────────────────────────────────
config = load_config()
REPLAY_DIRS = config.get("replay_directories") or []
USE_POLLING = config.get("use_polling", True)
POLL_INTERVAL = config.get("polling_interval", 1)
PARSE_INTERVAL = config.get("parse_interval", 15)
STABLE_TIME = config.get("stable_time_seconds", 60)
MIN_SIZE = 1

logging.basicConfig(
    level=os.getenv("LOGGING_LEVEL", config.get("logging_level", "DEBUG")).upper(),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

LOCK = threading.Lock()
ACTIVE = {}

# ───────────────────────────────────────────────
# 🔁 Helpers
# ───────────────────────────────────────────────
def sha1_of_file(path):
    try:
        with open(path, 'rb') as f:
            return hashlib.sha1(f.read()).hexdigest()
    except Exception as e:
        logging.error(f"❌ SHA1 failed for {path}: {e}")
        return None

def summarize_parse(path):
    try:
        parsed = asyncio.run(parse_replay_full(path))
        if not parsed:
            logging.warning("⚠️ Could not summarize parse (empty)")
            return
        map_name = parsed.get("map", {}).get("name", "Unknown")
        winner = parsed.get("winner", "Unknown")
        players = parsed.get("players", [])
        player_names = ", ".join(p.get("name", "?") for p in players)

        logging.debug(f"🧠 Parsed map: {map_name}")
        logging.debug(f"🧠 Winner: {winner}")
        logging.debug(f"🧠 Players: {player_names}")
    except Exception as e:
        logging.warning(f"❌ Failed to summarize parse: {e}")

def parse(path, iteration, is_final=False):
    try:
        if os.path.getsize(path) < MIN_SIZE:
            logging.debug(f"⏳ Skipping tiny file: {path}")
            return
        asyncio.run(parse_and_send(path, force=False, parse_iteration=iteration, is_final=is_final))
        if is_final:
            summarize_parse(path)
    except Exception as e:
        logging.error(f"❌ Parse failed: {e}", exc_info=True)

def wait_for_stability(path, delay=STABLE_TIME, poll=3):
    last_size, stable = -1, 0
    while True:
        try:
            size = os.path.getsize(path)
        except FileNotFoundError:
            logging.warning(f"🛑 File gone: {path}")
            return False
        if size == last_size:
            stable += poll
        else:
            last_size = size
            stable = 0
        if stable >= delay:
            return True
        time.sleep(poll)

def watch_replay(path):
    logging.info(f"🎬 Watching: {path}")
    if not wait_for_stability(path):
        logging.warning(f"⚠️ Never stabilized: {path}")
        return

    last_hash, last_time = None, 0
    iteration, stable_count = 0, 0
    max_stable = 4
    cooldown = 120

    while True:
        if not os.path.exists(path):
            logging.info(f"🗑️ Replay removed: {path}")
            return

        now = time.time()
        h = sha1_of_file(path)

        if h and h != last_hash and (now - last_time >= cooldown):
            last_hash, last_time = h, now
            iteration += 1
            stable_count = 0
            logging.debug(f"🚀 Parsing iter {iteration}: {path}")
            parse(path, iteration, is_final=False)
        else:
            stable_count += 1
            logging.debug(f"⏸ Waiting... {stable_count}/{max_stable}")

        if stable_count >= max_stable:
            logging.info(f"🏁 Final parse for: {path}")
            parse(path, iteration + 1, is_final=True)
            break

        time.sleep(PARSE_INTERVAL)

# ───────────────────────────────────────────────
# 👀 Watcher Handler
# ───────────────────────────────────────────────
class Handler(FileSystemEventHandler):
    def handle(self, path):
        if not path.endswith((".aoe2record", ".aoe2mpgame", ".mgz")) or "Out of Sync" in path:
            return
        with LOCK:
            if path not in ACTIVE:
                logging.info(f"🆕 Replay: {path}")
                t = threading.Thread(target=watch_replay, args=(path,), daemon=True)
                ACTIVE[path] = t
                t.start()

    def on_created(self, e):
        if not e.is_directory:
            self.handle(e.src_path)

    def on_modified(self, e):
        if not e.is_directory:
            self.handle(e.src_path)

# ───────────────────────────────────────────────
# 📁 Default Search Paths
# ───────────────────────────────────────────────
def default_dirs():
    system = platform.system()
    home = os.path.expanduser("~")
    paths = ["/replays"]
    if system == "Windows":
        u = os.environ.get("USERPROFILE", "")
        paths += [os.path.join(u, p) for p in [
            "Documents/My Games/Age of Empires 2 HD/SaveGame",
            "Documents/My Games/Age of Empires 2 DE/SaveGame"
        ]]
    elif system == "Darwin":
        paths += [
            "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame",
            "/Users/tonyblum/Documents/My Games/Age of Empires 2 DE/SaveGame"
        ]
    else:
        paths += [os.path.join(home, p) for p in [
            ".wine/drive_c/Program Files (x86)/Microsoft Games/Age of Empires II HD/SaveGame",
            "Documents/My Games/Age of Empires 2 HD/SaveGame"
        ]]
    return [d for d in paths if os.path.isdir(d)]

# ───────────────────────────────────────────────
# 🚀 Entrypoint
# ───────────────────────────────────────────────
if __name__ == "__main__":
    dirs = REPLAY_DIRS or default_dirs()
    observer = PollingObserver() if USE_POLLING else Observer()

    for d in dirs:
        if os.path.exists(d):
            logging.info(f"👀 Watching dir: {d}")
            observer.schedule(Handler(), d, recursive=False)
        else:
            logging.warning(f"⚠️ Missing dir: {d}")

    observer.start()
    try:
        while True:
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logging.info("🛑 Exiting...")
        observer.stop()
    observer.join()
