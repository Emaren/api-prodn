import json
import os
import logging
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_loaded = False
logger = logging.getLogger("aoe2hdbets.config")

def _resolve_env(default: str = "development") -> str:
    current = os.getenv("ENV")
    if current:
        return current
    render_flag = str(os.getenv("RENDER", "")).strip().lower()
    if render_flag and render_flag not in {"0", "false", "no"}:
        return "production"
    return default

# ✅ 0. Prefer explicit dotenv path when provided by process manager.
dotenv_override = os.getenv("DOTENV_CONFIG_PATH")
if dotenv_override:
    dotenv_path = os.path.abspath(os.path.expanduser(dotenv_override))
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
        env_loaded = True
        logger.info("Loaded dotenv from DOTENV_CONFIG_PATH: %s", dotenv_path)
    else:
        logger.warning("DOTENV_CONFIG_PATH not found: %s", dotenv_path)

# Get ENV after optional override load
ENV = _resolve_env()

# ✅ 1. Prefer .env.override only in development
override_path = os.path.join(BASE_DIR, ".env.override")
if ENV != "production" and os.path.exists(override_path):
    load_dotenv(dotenv_path=override_path)
    env_loaded = True
    logger.info("Loaded override from .env.override (dev only)")

# ✅ 2. Load env-specific file (production/dev/etc)
if not env_loaded:
    env_file = (
        ".env.production" if ENV == "production"
        else ".env.dev" if ENV == "dev"
        else ".env.fastdev" if ENV == "fastdev"
        else ".env"
    )
    env_path = os.path.join(BASE_DIR, env_file)
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        env_loaded = True
        logger.info("Loaded environment: %s from %s", ENV, env_file)
    else:
        logger.warning("No env file found for %s. Proceeding with defaults.", ENV)

# Re-resolve ENV in case file load changed it.
ENV = _resolve_env(default=ENV)

# ✅ 3. Load .env.local last, but only in dev mode
local_path = os.path.join(BASE_DIR, ".env.local")
if ENV == "development" and os.path.exists(local_path):
    load_dotenv(dotenv_path=local_path, override=True)
    logger.info("Loaded .env.local (final override layer for dev)")

# --- Exports ---
def get_fastapi_api_url():
    return os.getenv("FASTAPI_API_URL", "http://localhost:8002/api/parse_replay")

def get_api_targets():
    val = os.getenv("API_TARGETS")
    if val:
        return [x.strip() for x in val.split(",")]
    return []

def load_config():
    config_path = os.path.join(BASE_DIR, "config.json")
    if not os.path.exists(config_path):
        raise RuntimeError(f"❌ Configuration file not found at {config_path}")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"❌ JSON error in config.json: {e}")
    except Exception as e:
        raise RuntimeError(f"❌ Failed to load config.json: {e}")

logger.info("Config initialized for ENV=%s", ENV)
