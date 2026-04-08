import os
import re
import json
import hashlib
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, urlsplit

from fastapi.responses import JSONResponse
from fastapi import APIRouter, Depends, Header
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.db import get_db
from db.models import User
from routes.admin_routes_async import verify_admin_token

router = APIRouter()

DEFAULT_DEDICATED_LOG_PATH = "/var/log/nginx/aoe2hdbets.access.log"
DEFAULT_SHARED_LOG_PATH = "/var/log/nginx/access.log"
BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPT_DIR = BASE_DIR / "scripts"
STATE_DIR = Path(os.getenv("TRAFFIC_STATE_DIR", str(BASE_DIR / "runtime")))

TAIL_LINES = int(os.getenv("TRAFFIC_TAIL_LINES", "5000"))
SEEN_HASH_LIMIT = int(os.getenv("TRAFFIC_SEEN_HASH_LIMIT", "20000"))
TIMESTAMP_RETENTION_DAYS = int(os.getenv("TRAFFIC_TIMESTAMP_RETENTION_DAYS", "30"))
TIMESTAMP_RETENTION_PER_IP = int(os.getenv("TRAFFIC_TIMESTAMP_RETENTION_PER_IP", "200"))

TRAFFIC_SESSION_GAP_MINUTES = int(os.getenv("TRAFFIC_SESSION_GAP_MINUTES", "30"))
TRAFFIC_SESSION_ACTIVE_GAP_CAP_SECONDS = int(
    os.getenv("TRAFFIC_SESSION_ACTIVE_GAP_CAP_SECONDS", "300")
)
TRAFFIC_VISITOR_SESSION_LIMIT = int(os.getenv("TRAFFIC_VISITOR_SESSION_LIMIT", "120"))
TRAFFIC_VISITOR_PATH_LIMIT = int(os.getenv("TRAFFIC_VISITOR_PATH_LIMIT", "20"))
TRAFFIC_RESPONSE_CACHE_SECONDS = int(os.getenv("TRAFFIC_RESPONSE_CACHE_SECONDS", "20"))

IP_COUNT_FILE = os.getenv("IP_COUNT_FILE", str(STATE_DIR / "ip_visit_counts.json"))
IP_TIMESTAMP_FILE = os.getenv("IP_TIMESTAMP_FILE", str(STATE_DIR / "ip_timestamps.json"))
IP_COUNTRY_FILE = os.getenv("IP_COUNTRY_FILE", str(STATE_DIR / "ip_country.json"))
IP_GEO_FILE = os.getenv("IP_GEO_FILE", str(STATE_DIR / "ip_geo.json"))
SEEN_LINE_HASHES_FILE = os.getenv(
    "SEEN_LINE_HASHES_FILE",
    str(STATE_DIR / "seen_log_line_hashes.json"),
)

LEGACY_IP_COUNT_FILE = str(SCRIPT_DIR / "ip_visit_counts.json")
LEGACY_IP_TIMESTAMP_FILE = str(SCRIPT_DIR / "ip_timestamps.json")
LEGACY_IP_COUNTRY_FILE = str(SCRIPT_DIR / "ip_country.json")
LEGACY_SEEN_LINE_HASHES_FILE = str(SCRIPT_DIR / "seen_log_line_hashes.json")
TRAFFIC_RESPONSE_CACHE = {
    "expires_at": None,
    "payload": None,
}

LEGACY_LOG_LINE_RE = re.compile(
    r'(?P<ip>[0-9a-fA-F:.]+)\s+\S+\s+\S+\s+\[(?P<ts>[^\]]+)\]\s+"(?P<request>[^"]*)"\s+'
    r'(?P<status>\d{3}|-)\s+\S+\s+"(?P<referrer>[^"]*)"\s+"(?P<ua>[^"]*)"'
)

STATIC_ASSET_RE = re.compile(
    r"\.(?:css|js|mjs|map|ico|png|jpg|jpeg|gif|svg|webp|woff|woff2|ttf|eot|txt|xml|json|pdf|zip|gz|tar|mp4|webm|mp3|wav)$",
    re.IGNORECASE,
)

BOT_TERMS = [
    "bot",
    "crawl",
    "crawler",
    "spider",
    "censys",
    "zgrab",
    "uptimerobot",
    "googlebot",
    "bingbot",
    "bingpreview",
    "duckduckbot",
    "facebookexternalhit",
    "slurp",
]

SUSPICIOUS_UA_TERMS = [
    "curl",
    "wget",
    "python",
    "scrapy",
    "nikto",
    "sqlmap",
    "masscan",
    "go-http-client",
    "nmap",
    "scanner",
]

BROWSER_TERMS = [
    "mozilla",
    "chrome",
    "safari",
    "firefox",
    "edge",
    "opera",
]

SUSPICIOUS_PATH_SNIPPETS = [
    ".env",
    "wlwmanifest.xml",
    "xmlrpc.php",
    "wp-includes",
    "/wordpress",
    "/blog/wp-",
    "/storage/logs",
    "laravel",
    ".git",
    "phpmyadmin",
    "/boaform",
    "/cgi-bin",
    "/actuator",
    "production.key",
    "mail.log",
    "email.log",
    ".aws/",
    ".msmtprc",
    ".muttrc",
    ".directadmin",
    ".cpanel",
    ".plesk",
]

REQUEST_CATEGORIES = ("human", "bot", "suspicious", "unknown")
UNKNOWN_HOST = "(unknown host)"
UNKNOWN_REFERRER = "(direct)"

COUNTRY_CODE_MAP = {
    "US": "United States",
    "CA": "Canada",
    "GB": "United Kingdom",
    "UK": "United Kingdom",
    "IE": "Ireland",
    "DE": "Germany",
    "FR": "France",
    "NL": "Netherlands",
    "BE": "Belgium",
    "SE": "Sweden",
    "NO": "Norway",
    "FI": "Finland",
    "DK": "Denmark",
    "IT": "Italy",
    "ES": "Spain",
    "CH": "Switzerland",
    "AT": "Austria",
    "PL": "Poland",
    "CZ": "Czech Republic",
    "RO": "Romania",
    "BG": "Bulgaria",
    "LT": "Lithuania",
    "UA": "Ukraine",
    "TR": "Turkey",
    "AU": "Australia",
    "NZ": "New Zealand",
    "JP": "Japan",
    "KR": "South Korea",
    "CN": "China",
    "HK": "Hong Kong",
    "IN": "India",
    "PK": "Pakistan",
    "SG": "Singapore",
    "ZA": "South Africa",
    "NG": "Nigeria",
    "GH": "Ghana",
    "BR": "Brazil",
    "MX": "Mexico",
}


def resolve_log_path():
    candidates = []

    for env_key in ("AOE2_TRAFFIC_LOG_PATH", "TRAFFIC_LOG_PATH"):
        raw = os.getenv(env_key, "").strip()
        if raw:
            candidates.append(raw)

    candidates.extend([DEFAULT_DEDICATED_LOG_PATH, DEFAULT_SHARED_LOG_PATH])

    seen = set()
    ordered_candidates = []
    for candidate in candidates:
        normalized = str(Path(candidate))
        if normalized in seen:
            continue
        seen.add(normalized)
        ordered_candidates.append(normalized)

    for candidate in ordered_candidates:
        if os.path.exists(candidate):
            return candidate

    return ordered_candidates[0]


def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def load_json(path, fallback_path=None):
    candidate_paths = [path]
    if fallback_path:
        candidate_paths.append(fallback_path)

    for candidate in candidate_paths:
        if not candidate or not os.path.exists(candidate):
            continue
        try:
            with open(candidate, encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            continue

    return {}


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False)


def parse_iso_timestamp(value):
    try:
        parsed = datetime.fromisoformat(value)
    except Exception:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_log_timestamp(value):
    try:
        return datetime.strptime(value, "%d/%b/%Y:%H:%M:%S %z").astimezone(timezone.utc)
    except Exception:
        return None


def trim_timestamp_strings(values, now):
    if not isinstance(values, list):
        return []

    cutoff = now - timedelta(days=TIMESTAMP_RETENTION_DAYS)
    kept = []

    for value in values[-(TIMESTAMP_RETENTION_PER_IP * 3):]:
        parsed = parse_iso_timestamp(value)
        if not parsed:
            continue
        if parsed >= cutoff:
            kept.append(parsed.isoformat())

    return kept[-TIMESTAMP_RETENTION_PER_IP:]


def latest_timestamp_string(values):
    latest = None
    for value in values if isinstance(values, list) else []:
        parsed = parse_iso_timestamp(value)
        if not parsed:
            continue
        if latest is None or parsed > latest:
            latest = parsed

    return latest.isoformat() if latest else None


def get_cached_traffic_payload(now):
    expires_at = TRAFFIC_RESPONSE_CACHE.get("expires_at")
    payload = TRAFFIC_RESPONSE_CACHE.get("payload")
    if (
        TRAFFIC_RESPONSE_CACHE_SECONDS > 0
        and isinstance(expires_at, datetime)
        and expires_at > now
        and isinstance(payload, dict)
    ):
        return payload
    return None


def cache_traffic_payload(payload, generated_at):
    if TRAFFIC_RESPONSE_CACHE_SECONDS <= 0:
        return payload

    TRAFFIC_RESPONSE_CACHE["payload"] = payload
    TRAFFIC_RESPONSE_CACHE["expires_at"] = generated_at + timedelta(
        seconds=TRAFFIC_RESPONSE_CACHE_SECONDS
    )
    return payload


def run_geoiplookup(ip):
    try:
        return subprocess.check_output(
            ["geoiplookup", ip],
            stderr=subprocess.DEVNULL,
            timeout=2,
            text=True,
        ).strip()
    except Exception:
        return ""


def parse_geoip_country_output(output):
    if not output or "IP Address not found" in output:
        return "??"

    match = re.search(r":\s*[A-Z0-9-]+,\s*(.+)$", output)
    if match:
        country = match.group(1).strip()
        return country if country else "??"

    fallback = output.split(":", 1)[-1].strip()
    return fallback if fallback else "??"


def get_country(ip):
    return parse_geoip_country_output(run_geoiplookup(ip))


def country_name_from_code(code):
    normalized = (code or "").strip().upper()
    return COUNTRY_CODE_MAP.get(normalized, normalized or "??")


def parse_geoip_city_output(output):
    if not output or "City Edition" not in output:
        return None

    try:
        payload = output.split(":", 1)[1].strip()
    except Exception:
        return None

    parts = [part.strip() for part in payload.split(",")]
    if len(parts) < 3:
        return None

    country_code = parts[0]
    area = parts[1] if len(parts) >= 2 else ""
    city = parts[2] if len(parts) >= 3 else ""

    return {
        "country": country_name_from_code(country_code),
        "area": area if area not in {"", "N/A"} else "",
        "city": city if city not in {"", "N/A"} else "",
    }


def get_geo_details(ip, geo_cache):
    cached = geo_cache.get(ip)
    if isinstance(cached, dict):
        country = (cached.get("country") or "").strip() or "??"
        area = (cached.get("area") or "").strip()
        city = (cached.get("city") or "").strip()
        return {
            "country": country,
            "area": area,
            "city": city,
        }

    output = run_geoiplookup(ip)
    country = parse_geoip_country_output(output)
    area = ""
    city = ""

    parsed_city = parse_geoip_city_output(output)
    if parsed_city:
        country = parsed_city.get("country") or country
        area = parsed_city.get("area") or ""
        city = parsed_city.get("city") or ""

    details = {
        "country": country or "??",
        "area": area,
        "city": city,
    }
    geo_cache[ip] = details
    return details


def normalize_host(value):
    if not value or value == "-":
        return UNKNOWN_HOST

    host = value.strip().lower()

    if "://" in host:
        try:
            parsed = urlparse(host)
            host = parsed.netloc or host
        except Exception:
            pass

    if host.endswith(":80"):
        host = host[:-3]
    elif host.endswith(":443"):
        host = host[:-4]

    return host or UNKNOWN_HOST


def normalize_path(raw_path):
    if not raw_path:
        return "(unknown)"
    try:
        parsed = urlsplit(raw_path)
        return parsed.path or "/"
    except Exception:
        return raw_path or "(unknown)"


def normalize_referrer(referrer):
    if not referrer or referrer == "-":
        return UNKNOWN_REFERRER
    try:
        parsed = urlparse(referrer)
        netloc = parsed.netloc or referrer
        return normalize_host(netloc)
    except Exception:
        return referrer


def read_recent_log_lines(path: str, tail_lines: int) -> list[str]:
    try:
        raw = subprocess.check_output(
            ["tail", "-n", str(tail_lines), path],
            stderr=subprocess.DEVNULL,
        )
    except Exception as exc:
        print(f"[traffic] tail failed for {path}: {exc}")
        return []

    try:
        return raw.decode("utf-8", errors="replace").splitlines()
    except Exception as exc:
        print(f"[traffic] decode failed for {path}: {exc}")
        return []


def parse_json_log_line(line):
    stripped = line.strip()
    if not stripped.startswith("{") or not stripped.endswith("}"):
        return None

    try:
        payload = json.loads(stripped)
    except Exception:
        return None

    timestamp_raw = payload.get("ts")
    parsed_timestamp = parse_iso_timestamp(timestamp_raw) if isinstance(timestamp_raw, str) else None
    if not parsed_timestamp:
        return None

    method = str(payload.get("method") or "").upper() or "(unknown)"
    raw_path = str(payload.get("request_uri") or payload.get("uri") or "(unknown)")
    normalized_path = normalize_path(raw_path)
    host = normalize_host(str(payload.get("host") or payload.get("server_name") or UNKNOWN_HOST))

    return {
        "ip": str(payload.get("remote_addr") or ""),
        "timestamp": parsed_timestamp,
        "timestamp_iso": parsed_timestamp.isoformat(),
        "request": str(payload.get("request") or ""),
        "method": method,
        "raw_path": raw_path,
        "normalized_path": normalized_path,
        "status": safe_int(payload.get("status")),
        "referrer": str(payload.get("referrer") or "-"),
        "referrer_host": normalize_referrer(str(payload.get("referrer") or "-")),
        "ua": str(payload.get("user_agent") or ""),
        "host": host,
        "raw": line,
    }


def parse_legacy_log_line(line):
    match = LEGACY_LOG_LINE_RE.match(line)
    if not match:
        return None

    parsed_timestamp = parse_log_timestamp(match.group("ts"))
    if not parsed_timestamp:
        return None

    request = match.group("request").strip()
    request_parts = request.split()
    method = request_parts[0].upper() if request_parts else "(unknown)"
    raw_path = request_parts[1] if len(request_parts) >= 2 else "(unknown)"
    normalized_path = normalize_path(raw_path)

    return {
        "ip": match.group("ip"),
        "timestamp": parsed_timestamp,
        "timestamp_iso": parsed_timestamp.isoformat(),
        "request": request,
        "method": method,
        "raw_path": raw_path,
        "normalized_path": normalized_path,
        "status": safe_int(match.group("status")),
        "referrer": match.group("referrer"),
        "referrer_host": normalize_referrer(match.group("referrer")),
        "ua": match.group("ua"),
        "host": UNKNOWN_HOST,
        "raw": line,
    }


def parse_log_line(line):
    return parse_json_log_line(line) or parse_legacy_log_line(line)


def is_suspicious_path(path):
    lowered = (path or "").lower()
    return any(snippet in lowered for snippet in SUSPICIOUS_PATH_SNIPPETS)


def classify_request(ua, path):
    lowered_ua = (ua or "").lower()

    if is_suspicious_path(path):
        return "suspicious"
    if any(term in lowered_ua for term in BOT_TERMS):
        return "bot"
    if any(term in lowered_ua for term in SUSPICIOUS_UA_TERMS):
        return "suspicious"
    if any(term in lowered_ua for term in BROWSER_TERMS):
        return "human"
    return "unknown"


def counter_rows(counter, limit=10):
    return [{"label": label, "count": count} for label, count in counter.most_common(limit)]


def build_ip_rows(items, ip_countries, ip_categories, ip_last_seen, limit=10):
    rows = []
    for ip, count in items[:limit]:
        rows.append(
            {
                "ip": ip,
                "count": safe_int(count),
                "country": ip_countries.get(ip, "??"),
                "category": ip_categories.get(ip, "unknown"),
                "last_seen": ip_last_seen.get(ip),
            }
        )
    return rows


def nested_counter_rows(counter_map, selected_hosts, limit=5):
    output = {}
    for host in selected_hosts:
        counter = counter_map.get(host)
        if not counter:
            continue
        output[host] = counter_rows(counter, limit=limit)
    return output


def unique_rows_from_sets(host_ip_sets, limit=10):
    items = sorted(
        ((host, len(ips)) for host, ips in host_ip_sets.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    return [{"label": host, "count": count} for host, count in items[:limit]]


def unique_count_for_host_aliases(host_ip_sets, aliases):
    merged = set()
    for alias in aliases:
        merged.update(host_ip_sets.get(alias, set()))
    return len(merged)


def counter_sum_for_aliases(counter, aliases):
    return sum(safe_int(counter.get(alias, 0)) for alias in aliases)


def get_primary_host_aliases():
    raw = os.getenv(
        "TRAFFIC_PRIMARY_HOST_ALIASES",
        "aoe2hdbets.com,www.aoe2hdbets.com",
    )
    aliases = []
    for value in raw.split(","):
        normalized = normalize_host(value)
        if normalized and normalized not in aliases and normalized != UNKNOWN_HOST:
            aliases.append(normalized)
    return aliases or ["aoe2hdbets.com"]


def normalize_user_agent_key(ua):
    normalized = re.sub(r"\s+", " ", (ua or "").strip().lower())
    return normalized[:240]


def detect_device_type(ua):
    lowered = (ua or "").lower()

    if any(term in lowered for term in BOT_TERMS):
        return "bot"
    if any(term in lowered for term in SUSPICIOUS_UA_TERMS):
        return "script"
    if "ipad" in lowered or "tablet" in lowered:
        return "tablet"
    if "iphone" in lowered or "android" in lowered or "mobile" in lowered:
        return "mobile"
    if any(term in lowered for term in ["windows", "macintosh", "linux", "x11", "cros"]):
        return "desktop"
    return "unknown"


def detect_os(ua):
    lowered = (ua or "").lower()

    if "windows nt 10.0" in lowered:
        return "Windows 10/11"
    if "windows nt 6.3" in lowered:
        return "Windows 8.1"
    if "windows nt 6.2" in lowered:
        return "Windows 8"
    if "windows nt 6.1" in lowered:
        return "Windows 7"
    if "iphone" in lowered or "ipad" in lowered or "cpu iphone os" in lowered:
        return "iOS"
    if "android" in lowered:
        return "Android"
    if "mac os x" in lowered or "macintosh" in lowered:
        return "macOS"
    if "cros" in lowered:
        return "ChromeOS"
    if "linux" in lowered or "x11" in lowered:
        return "Linux"
    return "Unknown"


def detect_browser(ua):
    lowered = (ua or "").lower()

    if "edg/" in lowered:
        return "Edge"
    if "opr/" in lowered or "opera" in lowered:
        return "Opera"
    if "firefox/" in lowered:
        return "Firefox"
    if "chrome/" in lowered and "edg/" not in lowered and "opr/" not in lowered:
        return "Chrome"
    if "safari/" in lowered and "chrome/" not in lowered:
        return "Safari"
    if "curl/" in lowered:
        return "curl"
    if "wget/" in lowered:
        return "wget"
    return "Unknown"


def is_page_like_path(path):
    normalized = normalize_path(path)
    if normalized in {"", "(unknown)"}:
        return False
    if normalized.startswith("/api/"):
        return False
    if normalized.startswith("/_next/"):
        return False
    if normalized.startswith("/.well-known/"):
        return False
    if STATIC_ASSET_RE.search(normalized):
        return False
    return True


def ordered_unique(values):
    seen = set()
    output = []
    for value in values:
        normalized = (value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def humanize_duration(seconds):
    seconds = max(safe_int(seconds), 0)

    if seconds < 60:
        return f"{seconds}s"

    minutes, rem = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {rem}s"

    hours, rem_minutes = divmod(minutes, 60)
    return f"{hours}h {rem_minutes}m"


def build_visitor_session(events, geo_details):
    if not events:
        return None

    first = events[0]
    last = events[-1]
    ip = first["ip"]
    host = first["host"]
    ua = first["ua"]

    page_paths = [event["normalized_path"] for event in events if is_page_like_path(event["normalized_path"])]
    all_paths = [event["normalized_path"] for event in events]

    ordered_pages = ordered_unique(page_paths)[:TRAFFIC_VISITOR_PATH_LIMIT]
    ordered_all_paths = ordered_unique(all_paths)[:TRAFFIC_VISITOR_PATH_LIMIT]

    entry_page = ordered_pages[0] if ordered_pages else first["normalized_path"]
    exit_page = ordered_pages[-1] if ordered_pages else last["normalized_path"]

    referrers = ordered_unique(
        [event["referrer_host"] for event in events if event["referrer_host"] != UNKNOWN_REFERRER]
    )[:10]

    category_counter = Counter(event["category"] for event in events)
    dominant_category = category_counter.most_common(1)[0][0] if category_counter else "unknown"

    time_on_site_seconds = 0
    for previous, current in zip(events, events[1:]):
        gap = int((current["timestamp"] - previous["timestamp"]).total_seconds())
        if gap <= 0:
            continue
        time_on_site_seconds += min(gap, TRAFFIC_SESSION_ACTIVE_GAP_CAP_SECONDS)

    return {
        "host": host,
        "ip": ip,
        "country": geo_details.get("country", "??"),
        "area": geo_details.get("area", ""),
        "city": geo_details.get("city", ""),
        "category": dominant_category,
        "device": detect_device_type(ua),
        "os": detect_os(ua),
        "browser": detect_browser(ua),
        "entry_page": entry_page,
        "exit_page": exit_page,
        "pages_visited": ordered_pages,
        "all_paths_seen": ordered_all_paths,
        "page_count": len(ordered_pages),
        "total_requests": len(events),
        "time_on_site_seconds": time_on_site_seconds,
        "time_on_site_human": humanize_duration(time_on_site_seconds),
        "first_seen": first["timestamp_iso"],
        "last_seen": last["timestamp_iso"],
        "first_referrer": first["referrer_host"],
        "referrers": referrers,
        "request_counts": {
            category: safe_int(category_counter.get(category, 0)) for category in REQUEST_CATEGORIES
        },
        "user_agent": ua,
    }


def build_visitor_sessions(recent_entries, geo_cache, host_aliases=None):
    grouped = defaultdict(list)
    session_gap_seconds = TRAFFIC_SESSION_GAP_MINUTES * 60
    alias_filter = set(host_aliases or [])

    for entry in recent_entries:
        host = entry["host"]
        if host == UNKNOWN_HOST:
            continue
        if alias_filter and host not in alias_filter:
            continue

        visitor_key = (
            host,
            entry["ip"],
            normalize_user_agent_key(entry["ua"]),
        )
        grouped[visitor_key].append(entry)

    sessions = []

    for (_, ip, _), events in grouped.items():
        events = sorted(events, key=lambda item: item["timestamp"])
        current_session = []

        for event in events:
            if not current_session:
                current_session = [event]
                continue

            gap_seconds = int((event["timestamp"] - current_session[-1]["timestamp"]).total_seconds())
            if gap_seconds > session_gap_seconds:
                geo_details = get_geo_details(ip, geo_cache)
                built = build_visitor_session(current_session, geo_details)
                if built:
                    sessions.append(built)
                current_session = [event]
            else:
                current_session.append(event)

        if current_session:
            geo_details = get_geo_details(ip, geo_cache)
            built = build_visitor_session(current_session, geo_details)
            if built:
                sessions.append(built)

    sessions.sort(key=lambda item: item["last_seen"], reverse=True)
    return sessions[:TRAFFIC_VISITOR_SESSION_LIMIT]


@router.get("/api/traffic")
async def get_traffic_stats(
    authorization: str = Header(default=None),
    x_admin_token: str = Header(default=None, alias="X-Admin-Token"),
    db: AsyncSession = Depends(get_db),
):
    verify_admin_token(authorization, x_admin_token)

    cached_payload = get_cached_traffic_payload(datetime.now(timezone.utc))
    if cached_payload is not None:
        return cached_payload

    try:
        result = await db.execute(select(User.uid, User.email, User.in_game_name))
        users = result.fetchall()

        postgres_total = len(users)
        missing_email_uids = sorted([uid for uid, email, _ in users if uid and not email])
        missing_name_uids = sorted([uid for uid, _, in_game_name in users if uid and not in_game_name])
        profile_gap_uids = sorted(set(missing_email_uids) | set(missing_name_uids))

        ip_counts = load_json(IP_COUNT_FILE, LEGACY_IP_COUNT_FILE)
        ip_timestamps = load_json(IP_TIMESTAMP_FILE, LEGACY_IP_TIMESTAMP_FILE)
        ip_countries = load_json(IP_COUNTRY_FILE, LEGACY_IP_COUNTRY_FILE)
        ip_geo = load_json(IP_GEO_FILE)
        seen_hashes = load_json(SEEN_LINE_HASHES_FILE, LEGACY_SEEN_LINE_HASHES_FILE)

        if not isinstance(ip_counts, dict):
            ip_counts = {}
        if not isinstance(ip_timestamps, dict):
            ip_timestamps = {}
        if not isinstance(ip_countries, dict):
            ip_countries = {}
        if not isinstance(ip_geo, dict):
            ip_geo = {}
        if not isinstance(seen_hashes, list):
            seen_hashes = []

        now = datetime.now(timezone.utc)
        day_ago = now - timedelta(hours=24)

        cleaned_ip_timestamps = {}
        for ip, values in ip_timestamps.items():
            cleaned_ip_timestamps[ip] = trim_timestamp_strings(values, now)
        ip_timestamps = cleaned_ip_timestamps

        seen_set = set(seen_hashes)
        new_hashes = []

        recent_raw_lines = []
        recent_entries = []

        category_ip_sets_24h = {
            "human": set(),
            "bot": set(),
            "suspicious": set(),
            "unknown": set(),
        }

        ip_request_counter_24h = Counter()
        ip_last_seen_24h = {}
        ip_category_24h = {}

        human_country_counter_24h = Counter()
        all_country_counter_24h = Counter()
        path_counter_24h = Counter()
        suspicious_path_counter_24h = Counter()
        referrer_counter_24h = Counter()
        status_counter_24h = Counter()
        method_counter_24h = Counter()
        category_request_counter_24h = Counter()

        host_counter_24h = Counter()
        human_host_counter_24h = Counter()
        suspicious_host_counter_24h = Counter()
        bot_host_counter_24h = Counter()
        unknown_host_counter_24h = Counter()

        path_counter_by_host_24h = defaultdict(Counter)
        suspicious_path_counter_by_host_24h = defaultdict(Counter)
        referrer_counter_by_host_24h = defaultdict(Counter)

        unique_ips_by_host_24h = defaultdict(set)
        unique_ips_by_host_by_category_24h = {
            category: defaultdict(set) for category in REQUEST_CATEGORIES
        }

        max_log_time = None

        log_path = resolve_log_path()
        lines = read_recent_log_lines(log_path, TAIL_LINES)

        for line in lines:
            parsed = parse_log_line(line)
            if not parsed:
                continue

            ip = parsed["ip"]
            parsed_ts = parsed["timestamp"]
            normalized_path = parsed["normalized_path"]
            category = classify_request(parsed["ua"], normalized_path)
            host = parsed["host"]

            line_hash = hashlib.sha1(line.encode("utf-8", errors="ignore")).hexdigest()
            if line_hash not in seen_set:
                seen_set.add(line_hash)
                new_hashes.append(line_hash)

                ip_counts[ip] = safe_int(ip_counts.get(ip, 0)) + 1

                timestamp_list = ip_timestamps.get(ip, [])
                if not isinstance(timestamp_list, list):
                    timestamp_list = []
                timestamp_list.append(parsed["timestamp_iso"])
                ip_timestamps[ip] = trim_timestamp_strings(timestamp_list, now)

                current_geo = get_geo_details(ip, ip_geo)
                ip_countries[ip] = current_geo.get("country", "??")

            if parsed_ts > day_ago:
                recent_raw_lines.append(line)

                if max_log_time is None or parsed_ts > max_log_time:
                    max_log_time = parsed_ts

                geo_details = ip_geo.get(ip) if isinstance(ip_geo.get(ip), dict) else None
                if not geo_details:
                    geo_details = get_geo_details(ip, ip_geo)

                country = geo_details.get("country", "??")

                category_ip_sets_24h[category].add(ip)
                ip_request_counter_24h[ip] += 1
                ip_last_seen_24h[ip] = parsed["timestamp_iso"]
                ip_category_24h[ip] = category

                category_request_counter_24h[category] += 1
                status_counter_24h[str(parsed["status"])] += 1
                method_counter_24h[parsed["method"]] += 1
                path_counter_24h[normalized_path] += 1
                referrer_counter_24h[parsed["referrer_host"]] += 1
                all_country_counter_24h[country] += 1

                if category == "human":
                    human_country_counter_24h[country] += 1
                if category == "suspicious":
                    suspicious_path_counter_24h[normalized_path] += 1

                if host != UNKNOWN_HOST:
                    host_counter_24h[host] += 1
                    path_counter_by_host_24h[host][normalized_path] += 1
                    referrer_counter_by_host_24h[host][parsed["referrer_host"]] += 1

                    unique_ips_by_host_24h[host].add(ip)
                    unique_ips_by_host_by_category_24h[category][host].add(ip)

                    if category == "human":
                        human_host_counter_24h[host] += 1
                    elif category == "suspicious":
                        suspicious_host_counter_24h[host] += 1
                        suspicious_path_counter_by_host_24h[host][normalized_path] += 1
                    elif category == "bot":
                        bot_host_counter_24h[host] += 1
                    else:
                        unknown_host_counter_24h[host] += 1

                recent_entries.append(
                    {
                        "ts": parsed["timestamp_iso"],
                        "timestamp_iso": parsed["timestamp_iso"],
                        "timestamp": parsed["timestamp"],
                        "ip": ip,
                        "host": host,
                        "category": category,
                        "method": parsed["method"],
                        "path": normalized_path,
                        "normalized_path": normalized_path,
                        "status": parsed["status"],
                        "referrer": parsed["referrer_host"],
                        "referrer_host": parsed["referrer_host"],
                        "country": country,
                        "ua": parsed["ua"],
                        "raw": parsed["raw"],
                    }
                )

        repeat_visitors = sum(1 for count in ip_counts.values() if safe_int(count) > 1)
        total_seen_requests = sum(safe_int(count) for count in ip_counts.values())
        total_requests_24h = sum(ip_request_counter_24h.values())

        all_time_last_seen = {
            ip: latest_timestamp_string(values) for ip, values in ip_timestamps.items()
        }

        top_repeat_rows = build_ip_rows(
            sorted(ip_counts.items(), key=lambda item: safe_int(item[1]), reverse=True),
            ip_countries,
            {},
            all_time_last_seen,
            limit=15,
        )

        top_ips_24h_rows = build_ip_rows(
            ip_request_counter_24h.most_common(15),
            ip_countries,
            ip_category_24h,
            ip_last_seen_24h,
            limit=15,
        )

        top_countries_human_rows = counter_rows(human_country_counter_24h, limit=10)
        top_countries_all_rows = counter_rows(all_country_counter_24h, limit=10)
        top_paths_rows = counter_rows(path_counter_24h, limit=12)
        top_suspicious_paths_rows = counter_rows(suspicious_path_counter_24h, limit=12)
        top_referrers_rows = counter_rows(referrer_counter_24h, limit=12)
        status_rows = counter_rows(status_counter_24h, limit=10)
        method_rows = counter_rows(method_counter_24h, limit=10)
        category_request_rows = counter_rows(category_request_counter_24h, limit=10)

        top_hosts_rows = counter_rows(host_counter_24h, limit=12)
        top_human_hosts_rows = counter_rows(human_host_counter_24h, limit=12)
        top_suspicious_hosts_rows = counter_rows(suspicious_host_counter_24h, limit=12)

        host_unique_ips_rows = unique_rows_from_sets(unique_ips_by_host_24h, limit=12)
        host_unique_human_ips_rows = unique_rows_from_sets(
            unique_ips_by_host_by_category_24h["human"],
            limit=12,
        )
        host_unique_suspicious_ips_rows = unique_rows_from_sets(
            unique_ips_by_host_by_category_24h["suspicious"],
            limit=12,
        )

        selected_hosts = [row["label"] for row in top_hosts_rows[:5]]
        top_paths_by_host_rows = nested_counter_rows(path_counter_by_host_24h, selected_hosts, limit=6)
        top_suspicious_paths_by_host_rows = nested_counter_rows(
            suspicious_path_counter_by_host_24h,
            [row["label"] for row in top_suspicious_hosts_rows[:5]],
            limit=6,
        )
        top_referrers_by_host_rows = nested_counter_rows(
            referrer_counter_by_host_24h,
            selected_hosts,
            limit=6,
        )

        primary_host_aliases = get_primary_host_aliases()
        primary_host_focus = {
            "host": primary_host_aliases[0],
            "aliases": primary_host_aliases,
            "unique_ips_24h": unique_count_for_host_aliases(unique_ips_by_host_24h, primary_host_aliases),
            "unique_human_ips_24h": unique_count_for_host_aliases(
                unique_ips_by_host_by_category_24h["human"],
                primary_host_aliases,
            ),
            "unique_bot_ips_24h": unique_count_for_host_aliases(
                unique_ips_by_host_by_category_24h["bot"],
                primary_host_aliases,
            ),
            "unique_suspicious_ips_24h": unique_count_for_host_aliases(
                unique_ips_by_host_by_category_24h["suspicious"],
                primary_host_aliases,
            ),
            "unique_unknown_ips_24h": unique_count_for_host_aliases(
                unique_ips_by_host_by_category_24h["unknown"],
                primary_host_aliases,
            ),
            "total_requests_24h": counter_sum_for_aliases(host_counter_24h, primary_host_aliases),
            "human_requests_24h": counter_sum_for_aliases(human_host_counter_24h, primary_host_aliases),
            "bot_requests_24h": counter_sum_for_aliases(bot_host_counter_24h, primary_host_aliases),
            "suspicious_requests_24h": counter_sum_for_aliases(
                suspicious_host_counter_24h,
                primary_host_aliases,
            ),
            "unknown_requests_24h": counter_sum_for_aliases(
                unknown_host_counter_24h,
                primary_host_aliases,
            ),
        }

        visitor_sessions_24h = build_visitor_sessions(recent_entries, ip_geo)
        primary_host_visitor_sessions_24h = build_visitor_sessions(
            recent_entries,
            ip_geo,
            host_aliases=primary_host_aliases,
        )

        save_json(IP_COUNT_FILE, ip_counts)
        save_json(IP_TIMESTAMP_FILE, ip_timestamps)
        save_json(IP_COUNTRY_FILE, ip_countries)
        save_json(IP_GEO_FILE, ip_geo)
        save_json(SEEN_LINE_HASHES_FILE, (seen_hashes + new_hashes)[-SEEN_HASH_LIMIT:])

        return cache_traffic_payload({
            "generated_at": now.isoformat(),
            "log_source_path": log_path,
            "postgres_total": postgres_total,
            "profile_gap_count": len(profile_gap_uids),
            "profile_gap_uids": profile_gap_uids,
            "missing_email_count": len(missing_email_uids),
            "missing_name_count": len(missing_name_uids),
            "traffic_log": "\n".join(recent_raw_lines[-50:]),
            "recent_entries": [
                {
                    key: value
                    for key, value in entry.items()
                    if key != "timestamp"
                }
                for entry in recent_entries[-50:]
            ],
            "summary": {
                "real_24h": len(category_ip_sets_24h["human"]),
                "repeat": repeat_visitors,
                "repeat_visitors": repeat_visitors,
                "bot": len(category_ip_sets_24h["bot"]),
                "suspicious": len(category_ip_sets_24h["suspicious"]),
                "unknown": len(category_ip_sets_24h["unknown"]),
                "total_all_time_ips": len(ip_counts),
                "top_repeat_ips": [[row["ip"], row["count"]] for row in top_repeat_rows[:10]],
                "top_countries": [[row["label"], row["count"]] for row in top_countries_human_rows[:10]],
                "total_seen_requests": total_seen_requests,
                "total_requests_24h": total_requests_24h,
                "last_log_time": max_log_time.isoformat() if max_log_time else None,
                "top_repeat_ips_detailed": top_repeat_rows,
                "top_ips_24h": top_ips_24h_rows,
                "top_countries_human_24h": top_countries_human_rows,
                "top_countries_all_24h": top_countries_all_rows,
                "top_paths_24h": top_paths_rows,
                "top_suspicious_paths_24h": top_suspicious_paths_rows,
                "top_referrers_24h": top_referrers_rows,
                "status_counts_24h": status_rows,
                "method_counts_24h": method_rows,
                "category_request_counts_24h": category_request_rows,
                "top_hosts_24h": top_hosts_rows,
                "top_human_hosts_24h": top_human_hosts_rows,
                "top_suspicious_hosts_24h": top_suspicious_hosts_rows,
                "host_unique_ips_24h": host_unique_ips_rows,
                "host_unique_human_ips_24h": host_unique_human_ips_rows,
                "host_unique_suspicious_ips_24h": host_unique_suspicious_ips_rows,
                "top_paths_by_host_24h": top_paths_by_host_rows,
                "top_suspicious_paths_by_host_24h": top_suspicious_paths_by_host_rows,
                "top_referrers_by_host_24h": top_referrers_by_host_rows,
                "primary_host_aliases": primary_host_aliases,
                "primary_host_focus": primary_host_focus,
                "visitor_sessions_24h": visitor_sessions_24h,
                "primary_host_visitor_sessions_24h": primary_host_visitor_sessions_24h,
            },
        }, now)

    except Exception as exc:
        print(f"[traffic] route failed: {exc}")
        return JSONResponse(
            status_code=500,
            content={"error": f"traffic route failed: {str(exc)}"},
        )
