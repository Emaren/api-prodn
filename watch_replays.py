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

from config import load_config
from parse_replay import parse_and_send
from utils.replay_parser import parse_replay_full

# ───────────────────────────────────────────────
# 🔧 Config
# ───────────────────────────────────────────────
config = load_config()
REPLAY_DIRS = config.get("replay_directories") or []
USE_POLLING = config.get("use_polling", True)
POLL_INTERVAL = config.get("polling_interval", 1)
PARSE_INTERVAL = config.get("parse_interval", 15)
STABLE_TIME = config.get("stable_time_seconds", 60)
INITIAL_LIVE_DELAY = config.get("initial_live_delay_seconds", 3)
LIVE_PARSE_COOLDOWN = config.get("live_parse_cooldown_seconds", max(PARSE_INTERVAL * 3, 45))
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
            return False
        sent = asyncio.run(parse_and_send(path, force=False, parse_iteration=iteration, is_final=is_final))
        if sent and is_final:
            summarize_parse(path)
        return sent
    except Exception as e:
        logging.error(f"❌ Parse failed: {e}", exc_info=True)
        return False

def wait_for_first_bytes(path, timeout=30, poll=1):
    started_at = time.time()
    while time.time() - started_at <= timeout:
        if not os.path.exists(path):
            logging.warning(f"🛑 File gone before first parse: {path}")
            return False

        try:
            if os.path.getsize(path) >= MIN_SIZE:
                return True
        except FileNotFoundError:
            logging.warning(f"🛑 File gone before first parse: {path}")
            return False

        time.sleep(poll)

    logging.warning(f"⚠️ Replay never reached minimum size: {path}")
    return False

def watch_replay(path):
    logging.info(f"🎬 Watching: {path}")
    try:
        if not wait_for_first_bytes(path):
            return

        if INITIAL_LIVE_DELAY > 0:
            time.sleep(INITIAL_LIVE_DELAY)

        last_hash = None
        last_parse_at = 0.0
        last_change_at = time.time()
        iteration = 0

        while True:
            if not os.path.exists(path):
                logging.info(f"🗑️ Replay removed: {path}")
                return

            now = time.time()
            replay_hash = sha1_of_file(path)

            if replay_hash and replay_hash != last_hash:
                last_hash = replay_hash
                last_change_at = now

                if iteration == 0 or now - last_parse_at >= LIVE_PARSE_COOLDOWN:
                    next_iteration = iteration + 1
                    logging.debug(f"🚀 Live parse iter {next_iteration}: {path}")
                    if parse(path, next_iteration, is_final=False):
                        iteration = next_iteration
                        last_parse_at = now
                    else:
                        logging.debug(f"⚠️ Live parse attempt did not store yet: {path}")
                else:
                    cooldown_remaining = max(0, LIVE_PARSE_COOLDOWN - (now - last_parse_at))
                    logging.debug(
                        f"⏳ Replay still changing, waiting {cooldown_remaining:.0f}s before next live parse: {path}"
                    )
            elif iteration > 0 and now - last_change_at >= STABLE_TIME:
                logging.info(f"🏁 Final parse for: {path}")
                parse(path, iteration + 1, is_final=True)
                break
            else:
                idle_for = max(0, now - last_change_at)
                logging.debug(f"⏸ Waiting for more replay bytes ({idle_for:.0f}s idle): {path}")

            time.sleep(PARSE_INTERVAL)
    finally:
        with LOCK:
            ACTIVE.pop(path, None)

# ───────────────────────────────────────────────
# 👀 Watcher Handler
# ───────────────────────────────────────────────
class Handler(FileSystemEventHandler):
    def handle(self, path):
        if not path.endswith((".aoe2record", ".aoe2mpgame", ".mgz", ".mgx", ".mgl")) or "Out of Sync" in path:
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
            os.path.join(
                home,
                "Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame",
            ),
            os.path.join(home, "Documents/My Games/Age of Empires 2 DE/SaveGame"),
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
