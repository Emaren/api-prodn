import os
import re
import json
import hashlib
import subprocess
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from db.db import get_db
from db.models import User
from routes.admin_routes_async import verify_admin_token

router = APIRouter()

LOG_PATH = "/var/log/nginx/access.log"
BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPT_DIR = BASE_DIR / "scripts"  # legacy location
STATE_DIR = Path(os.getenv("TRAFFIC_STATE_DIR", str(BASE_DIR / "runtime")))

IP_COUNT_FILE = os.getenv("IP_COUNT_FILE", str(STATE_DIR / "ip_visit_counts.json"))
IP_TIMESTAMP_FILE = os.getenv("IP_TIMESTAMP_FILE", str(STATE_DIR / "ip_timestamps.json"))
IP_COUNTRY_FILE = os.getenv("IP_COUNTRY_FILE", str(STATE_DIR / "ip_country.json"))
SEEN_LINE_HASHES_FILE = os.getenv("SEEN_LINE_HASHES_FILE", str(STATE_DIR / "seen_log_line_hashes.json"))

LEGACY_IP_COUNT_FILE = str(SCRIPT_DIR / "ip_visit_counts.json")
LEGACY_IP_TIMESTAMP_FILE = str(SCRIPT_DIR / "ip_timestamps.json")
LEGACY_IP_COUNTRY_FILE = str(SCRIPT_DIR / "ip_country.json")
LEGACY_SEEN_LINE_HASHES_FILE = str(SCRIPT_DIR / "seen_log_line_hashes.json")

def load_json(path, fallback_path=None):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except:
            pass
    if fallback_path and os.path.exists(fallback_path):
        try:
            with open(fallback_path) as f:
                return json.load(f)
        except:
            pass
    return {}

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)

def get_country(ip):
    try:
        out = subprocess.check_output(["geoiplookup", ip]).decode()
        return out.strip().split(":")[-1].split(",")[-1].strip()
    except:
        return "??"

@router.get("/api/traffic")
async def get_traffic_stats(
    authorization: str = Header(default=None),
    x_admin_token: str = Header(default=None, alias="X-Admin-Token"),
    db: AsyncSession = Depends(get_db),
):
    verify_admin_token(authorization, x_admin_token)
    try:
        # User totals and profile quality checks from Postgres only.
        result = await db.execute(select(User.uid, User.email, User.in_game_name))
        users = result.fetchall()
        postgres_total = len(users)
        missing_email_uids = sorted([uid for uid, email, _ in users if uid and not email])
        missing_name_uids = sorted([uid for uid, _, in_game_name in users if uid and not in_game_name])
        profile_gap_uids = sorted(set(missing_email_uids) | set(missing_name_uids))

        # Load persistent visit data
        ip_counts = load_json(IP_COUNT_FILE, LEGACY_IP_COUNT_FILE)
        ip_timestamps = load_json(IP_TIMESTAMP_FILE, LEGACY_IP_TIMESTAMP_FILE)
        ip_countries = load_json(IP_COUNTRY_FILE, LEGACY_IP_COUNTRY_FILE)
        seen_hashes = load_json(SEEN_LINE_HASHES_FILE, LEGACY_SEEN_LINE_HASHES_FILE)
        if not isinstance(seen_hashes, list):
            seen_hashes = []
        seen_set = set(seen_hashes)
        new_hashes = []

        recent_entries = []
        ip_categories = defaultdict(set)
        now = datetime.utcnow()
        day_ago = now - timedelta(hours=24)

        if os.path.exists(LOG_PATH):
            try:
                lines = subprocess.check_output(["tail", "-n", "1000", LOG_PATH]).decode().splitlines()
            except Exception:
                lines = []

            for line in lines:
                ip_match = re.match(r"(\d+\.\d+\.\d+\.\d+)", line)
                ua_match = re.search(r'"([^"]*)"$', line)
                if not ip_match or not ua_match:
                    continue

                ip = ip_match.group(1)
                ua = ua_match.group(1).lower()
                line_hash = hashlib.sha1(line.encode("utf-8", errors="ignore")).hexdigest()

                if line_hash not in seen_set:
                    seen_set.add(line_hash)
                    new_hashes.append(line_hash)

                    if ip not in ip_counts:
                        ip_counts[ip] = 0
                    ip_counts[ip] += 1

                    if ip not in ip_timestamps:
                        ip_timestamps[ip] = []
                    ip_timestamps[ip].append(now.isoformat())

                    if ip not in ip_countries:
                        ip_countries[ip] = get_country(ip)

                if any(bot in ua for bot in ["bot", "crawl", "spider", "censys", "zgrab"]):
                    ip_categories["bot"].add(ip)
                elif any(term in ua for term in ["curl", "wget", "python", "scrapy", "attack"]):
                    ip_categories["suspicious"].add(ip)
                elif any(term in ua for term in ["mozilla", "chrome", "safari", "firefox", "edge"]):
                    ip_categories["real"].add(ip)
                else:
                    ip_categories["unknown"].add(ip)

                recent_entries.append(line)

        # Filter 24h real users
        real_24h_ips = {
            ip for ip in ip_categories["real"]
            if any(datetime.fromisoformat(ts) > day_ago for ts in ip_timestamps.get(ip, []))
        }

        repeat_visitors = len([ip for ip, count in ip_counts.items() if count > 1])
        top_repeat_ips = sorted(ip_counts.items(), key=lambda x: x[1], reverse=True)[:10]

        # Top countries from real IPs only
        real_countries = [ip_countries[ip] for ip in ip_categories["real"]]
        top_countries = Counter(real_countries).most_common(10)

        # Save
        save_json(IP_COUNT_FILE, ip_counts)
        save_json(IP_TIMESTAMP_FILE, ip_timestamps)
        save_json(IP_COUNTRY_FILE, ip_countries)
        # Bound memory/disk growth for seen log lines.
        trimmed_hashes = (seen_hashes + new_hashes)[-10000:]
        save_json(SEEN_LINE_HASHES_FILE, trimmed_hashes)

        return {
            "postgres_total": postgres_total,
            "profile_gap_count": len(profile_gap_uids),
            "profile_gap_uids": profile_gap_uids,
            "missing_email_count": len(missing_email_uids),
            "missing_name_count": len(missing_name_uids),
            "traffic_log": "\n".join(recent_entries[-20:]),
            "summary": {
                "real_24h": len(real_24h_ips),
                "repeat": repeat_visitors,
                "repeat_visitors": repeat_visitors,
                "bot": len(ip_categories["bot"]),
                "suspicious": len(ip_categories["suspicious"]),
                "unknown": len(ip_categories["unknown"]),
                "total_all_time_ips": len(ip_counts),
                "top_repeat_ips": top_repeat_ips,
                "top_countries": top_countries
            }
        }

    except Exception as e:
        return {"error": str(e)}
