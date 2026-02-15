"""
Flask Web Server for Restaurant Dashboard - iFood API Version
Integrated with iFood Merchant API instead of Excel files
Features: Real-time SSE, Background Refresh, Comparative Analytics, Data Caching
"""

from flask import Flask, request, jsonify, session, redirect, url_for, send_file, Response, stream_with_context
from werkzeug.middleware.proxy_fix import ProxyFix
from dashboarddb import DashboardDatabase
from ifood_api_with_mock import IFoodAPI, IFoodConfig
from ifood_data_processor import IFoodDataProcessor
import os
from pathlib import Path
import json
import html
import hashlib
import re
from typing import Dict, List, Optional
import traceback
from datetime import datetime, timedelta, timezone
from functools import wraps
import uuid
import threading
import time
import queue
import copy
import sys
import signal
import logging
import hmac
from urllib.parse import urlparse
from collections import deque

# Try to enable gzip compression
try:
    from flask_compress import Compress
    _HAS_COMPRESS = True
except ImportError:
    _HAS_COMPRESS = False

# Prevent UnicodeEncodeError crashes on terminals with limited encodings.
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(errors="replace")
except Exception:
    pass

# Optional Redis integration for distributed queue/cache/pubsub
try:
    import redis
    _HAS_REDIS = True
except ImportError:
    redis = None
    _HAS_REDIS = False

logging.basicConfig(
    level=getattr(logging, str(os.environ.get('LOG_LEVEL', 'INFO')).upper(), logging.INFO),
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
)
logger = logging.getLogger('dashboard')


# Configure paths FIRST - before Flask app creation
BASE_DIR = Path(__file__).parent.absolute()
STATIC_DIR = BASE_DIR / 'static'
DASHBOARD_OUTPUT = BASE_DIR / 'dashboard_output'
CONFIG_FILE = BASE_DIR / 'ifood_config.json'

# Create directories if they don't exist
STATIC_DIR.mkdir(exist_ok=True)
DASHBOARD_OUTPUT.mkdir(exist_ok=True)

# Create Flask app with static folder configured
app = Flask(__name__,
           static_folder=str(STATIC_DIR),
           static_url_path='/static')

# Detect reverse proxy (Railway, Render, Heroku, etc.)
IS_BEHIND_PROXY = any(var in os.environ for var in [
    'RAILWAY_ENVIRONMENT', 'RAILWAY_PROJECT_ID',
    'RENDER', 'RENDER_SERVICE_ID',
    'DYNO', 'K_SERVICE',
])

if IS_BEHIND_PROXY:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    print("ProxyFix enabled (detected reverse proxy)")

secret_key = os.environ.get('FLASK_SECRET_KEY') or os.environ.get('SECRET_KEY')
if secret_key:
    app.secret_key = secret_key
else:
    app.secret_key = os.urandom(32).hex()
    print("WARNING: SECRET_KEY not set; using ephemeral secret key for this process.")
app.permanent_session_lifetime = timedelta(days=7)
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = IS_BEHIND_PROXY
ENABLE_LEGACY_FALLBACK = str(
    os.environ.get('ENABLE_LEGACY_FALLBACK', '0' if IS_BEHIND_PROXY else '1')
).strip().lower() in ('1', 'true', 'yes', 'on')

# Redis-backed distributed features (queue/cache/pubsub)
REDIS_URL = os.environ.get('REDIS_URL', '').strip()
USE_REDIS_QUEUE = bool(_HAS_REDIS and REDIS_URL and str(os.environ.get('USE_REDIS_QUEUE', '1')).strip().lower() in ('1', 'true', 'yes', 'on'))
USE_REDIS_CACHE = bool(_HAS_REDIS and REDIS_URL and str(os.environ.get('USE_REDIS_CACHE', '1')).strip().lower() in ('1', 'true', 'yes', 'on'))
USE_REDIS_PUBSUB = bool(_HAS_REDIS and REDIS_URL and str(os.environ.get('USE_REDIS_PUBSUB', '1')).strip().lower() in ('1', 'true', 'yes', 'on'))
IFOOD_KEEPALIVE_POLLING = str(os.environ.get('IFOOD_KEEPALIVE_POLLING', '1')).strip().lower() in ('1', 'true', 'yes', 'on')
try:
    IFOOD_POLL_INTERVAL_SECONDS = int(os.environ.get('IFOOD_POLL_INTERVAL_SECONDS', '30') or 30)
except Exception:
    IFOOD_POLL_INTERVAL_SECONDS = 30
IFOOD_POLL_INTERVAL_SECONDS = max(10, IFOOD_POLL_INTERVAL_SECONDS)

# Enable gzip compression if available
if _HAS_COMPRESS:
    Compress(app)
    print("Response compression enabled")
else:
    # Fallback: manual gzip via after_request
    import gzip as _gzip
    import io as _io
    
    @app.after_request
    def compress_response(response):
        if (response.status_code < 200 or response.status_code >= 300 or
            response.direct_passthrough or
            'Content-Encoding' in response.headers or
            not response.content_type or
            'text/event-stream' in response.content_type):
            return response
        
        accept_encoding = request.headers.get('Accept-Encoding', '')
        if 'gzip' not in accept_encoding.lower():
            return response
        
        # Only compress text-like responses > 500 bytes
        if response.content_length and response.content_length < 500:
            return response
        
        if any(t in response.content_type for t in ['text/', 'application/json', 'application/javascript']):
            data = response.get_data()
            buf = _io.BytesIO()
            with _gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as gz:
                gz.write(data)
            response.set_data(buf.getvalue())
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Content-Length'] = len(response.get_data())
            response.headers['Vary'] = 'Accept-Encoding'
        return response
    print("Manual gzip compression enabled")

print(f"Base directory: {BASE_DIR}")
print(f"Static folder: {STATIC_DIR}")
print(f"Dashboard output: {DASHBOARD_OUTPUT}")
print(f"Config file: {CONFIG_FILE}")

@app.after_request
def add_cache_headers(response):
    """Set caching and compression headers"""
    if response.content_type and 'text/html' in response.content_type:
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    elif response.content_type and 'application/json' in response.content_type:
        # Allow short caching for API responses (5s)
        response.headers['Cache-Control'] = 'private, max-age=5'
    return response

# Database configuration
db = DashboardDatabase()

_REDIS_CLIENT = None
REDIS_INSTANCE_ID = str(uuid.uuid4())
REDIS_EVENTS_CHANNEL = 'timo:events'
REDIS_REFRESH_QUEUE = 'timo:jobs:refresh'
REDIS_REFRESH_STATUS_KEY = 'timo:refresh:status'
REDIS_REFRESH_LOCK_KEY = 'timo:refresh:lock'
REDIS_KEEPALIVE_LOCK_KEY = 'timo:ifood:keepalive:lock'
REDIS_CACHE_PREFIX = 'timo:cache:restaurants'
try:
    REDIS_SOCKET_TIMEOUT_SECONDS = float(os.environ.get('REDIS_SOCKET_TIMEOUT_SECONDS', '35') or 35)
except Exception:
    REDIS_SOCKET_TIMEOUT_SECONDS = 35.0
REDIS_SOCKET_TIMEOUT_SECONDS = max(5.0, REDIS_SOCKET_TIMEOUT_SECONDS)
try:
    REDIS_CONNECT_TIMEOUT_SECONDS = float(os.environ.get('REDIS_CONNECT_TIMEOUT_SECONDS', '5') or 5)
except Exception:
    REDIS_CONNECT_TIMEOUT_SECONDS = 5.0
REDIS_CONNECT_TIMEOUT_SECONDS = max(1.0, REDIS_CONNECT_TIMEOUT_SECONDS)


def get_redis_client():
    """Lazy Redis client initializer (returns None when unavailable)."""
    global _REDIS_CLIENT
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    if not (_HAS_REDIS and REDIS_URL):
        return None
    try:
        _REDIS_CLIENT = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
            socket_connect_timeout=REDIS_CONNECT_TIMEOUT_SECONDS
        )
        _REDIS_CLIENT.ping()
        return _REDIS_CLIENT
    except Exception as e:
        print(f"Redis unavailable: {e}")
        _REDIS_CLIENT = None
        return None

# In-memory cache for processed API responses
_api_cache = {}  # key: (org_id, month) -> {'data': [...], 'timestamp': datetime}
_API_CACHE_TTL = 30  # seconds
_DASHBOARD_SUMMARY_CACHE = {}
_DASHBOARD_SUMMARY_CACHE_LOCK = threading.Lock()
try:
    _DASHBOARD_SUMMARY_CACHE_TTL = int(os.environ.get('DASHBOARD_SUMMARY_CACHE_TTL_SECONDS', '20') or 20)
except Exception:
    _DASHBOARD_SUMMARY_CACHE_TTL = 20
_DASHBOARD_SUMMARY_CACHE_TTL = max(5, _DASHBOARD_SUMMARY_CACHE_TTL)


def _restaurants_cache_key(org_id, month_filter):
    safe_org = org_id if org_id is not None else 'global'
    safe_month = month_filter if month_filter is not None else 'all'
    return f"{REDIS_CACHE_PREFIX}:{safe_org}:{safe_month}"


def _dashboard_summary_cache_key(org_id, month_filter):
    safe_org = org_id if org_id is not None else 'global'
    safe_month = month_filter if month_filter is not None else 'all'
    return (safe_org, safe_month)


def get_cached_dashboard_summary(org_id, month_filter, last_refresh_iso):
    """Get cached dashboard summary payload for current org/month."""
    key = _dashboard_summary_cache_key(org_id, month_filter)
    with _DASHBOARD_SUMMARY_CACHE_LOCK:
        cached = _DASHBOARD_SUMMARY_CACHE.get(key)
    if not cached:
        return None
    age_seconds = (datetime.now() - cached['timestamp']).total_seconds()
    if age_seconds > _DASHBOARD_SUMMARY_CACHE_TTL:
        return None
    if cached.get('last_refresh') != last_refresh_iso:
        return None
    return cached.get('payload')


def set_cached_dashboard_summary(org_id, month_filter, last_refresh_iso, payload):
    """Cache dashboard summary payload for short-lived hot-path reuse."""
    key = _dashboard_summary_cache_key(org_id, month_filter)
    with _DASHBOARD_SUMMARY_CACHE_LOCK:
        _DASHBOARD_SUMMARY_CACHE[key] = {
            'payload': payload,
            'last_refresh': last_refresh_iso,
            'timestamp': datetime.now(),
        }


def invalidate_dashboard_summary_cache(org_id=None):
    """Invalidate dashboard summary cache globally or for one org."""
    with _DASHBOARD_SUMMARY_CACHE_LOCK:
        if org_id is None:
            _DASHBOARD_SUMMARY_CACHE.clear()
            return
        for key in list(_DASHBOARD_SUMMARY_CACHE.keys()):
            if key[0] == org_id:
                _DASHBOARD_SUMMARY_CACHE.pop(key, None)


def get_cached_restaurants(org_id, month_filter):
    """Get cached processed restaurant data if still fresh"""
    if USE_REDIS_CACHE:
        r = get_redis_client()
        if r:
            try:
                raw = r.get(_restaurants_cache_key(org_id, month_filter))
                if raw:
                    return json.loads(raw)
            except Exception as cache_read_error:
                logger.debug("Redis restaurants cache read failed: %s", cache_read_error)
    key = (org_id, month_filter)
    cached = _api_cache.get(key)
    if cached and (datetime.now() - cached['timestamp']).total_seconds() < _API_CACHE_TTL:
        return cached['data']
    return None

def set_cached_restaurants(org_id, month_filter, data):
    """Cache processed restaurant data"""
    if USE_REDIS_CACHE:
        r = get_redis_client()
        if r:
            try:
                r.setex(_restaurants_cache_key(org_id, month_filter), _API_CACHE_TTL, json.dumps(data, ensure_ascii=False, default=str))
            except Exception as cache_write_error:
                logger.debug("Redis restaurants cache write failed: %s", cache_write_error)
    key = (org_id, month_filter)
    _api_cache[key] = {'data': data, 'timestamp': datetime.now()}

def invalidate_cache(org_id=None):
    """Clear API cache entries globally or for one organization."""
    if USE_REDIS_CACHE:
        r = get_redis_client()
        if r:
            try:
                if org_id is None:
                    pattern = f"{REDIS_CACHE_PREFIX}:*"
                else:
                    pattern = f"{REDIS_CACHE_PREFIX}:{org_id}:*"
                for k in r.scan_iter(match=pattern):
                    r.delete(k)
            except Exception:
                pass

    if org_id is None:
        _api_cache.clear()
    else:
        for key in list(_api_cache.keys()):
            if key[0] == org_id:
                _api_cache.pop(key, None)
    invalidate_dashboard_summary_cache(org_id=org_id)

# Per-org data store: {org_id: {'restaurants': [], 'api': IFoodAPI, 'last_refresh': datetime, 'config': {}}}
ORG_DATA = {}
# Legacy global for backward compat during transition
RESTAURANTS_DATA = []
IFOOD_API = None
IFOOD_CONFIG = {}
LAST_DATA_REFRESH = None
APP_STARTED_AT = datetime.utcnow()
APP_INITIALIZED = False
_INIT_LOCK = threading.Lock()
_ORG_DATA_LOCK = threading.RLock()
_GLOBAL_STATE_LOCK = threading.RLock()

_RATE_LIMIT_LOCAL = {}
_RATE_LIMIT_LOCK = threading.Lock()
_TABLE_COLUMNS_CACHE = {}
_TABLE_COLUMNS_CACHE_LOCK = threading.Lock()

# Marketing metadata for plan cards in admin UI.
PLAN_CATALOG_UI = {
    'starter': {
        'subtitle': 'Centralização e relatórios',
        'badge': None,
        'highlight': False,
        'note': 'Em breve',
        'features_ui': [
            'Centralização e relatórios'
        ]
    },
    'pro': {
        'subtitle': 'O plano completo para agências',
        'badge': 'Agências',
        'highlight': True,
        'note': 'Em breve',
        'features_ui': [
            'Multiusuário',
            'Squads',
            'Links públicos',
            'Relatórios em PDF'
        ]
    },
    'enterprise': {
        'subtitle': 'Para operações avançadas',
        'badge': None,
        'highlight': False,
        'note': 'Em breve',
        'features_ui': [
            'Customizações avançadas',
            'Integrações sob demanda',
            'White Label'
        ]
    }
}


def get_org_data(org_id):
    """Get or initialize org data container"""
    if org_id not in ORG_DATA:
        with _ORG_DATA_LOCK:
            if org_id not in ORG_DATA:
                ORG_DATA[org_id] = {
                    'restaurants': [],
                    'api': None,
                    'last_refresh': None,
                    'config': {},
                    'init_attempted_at': None,
                    '_cache_sync_checked_at': 0.0
                }
    return ORG_DATA[org_id]


def _org_data_items_snapshot():
    with _ORG_DATA_LOCK:
        return list(ORG_DATA.items())


def _org_data_values_snapshot():
    with _ORG_DATA_LOCK:
        return list(ORG_DATA.values())


def get_current_org_id():
    """Get active org_id from session"""
    org_id = session.get('org_id') or (session.get('user', {}).get('primary_org_id'))
    # Normalize org_id to int when possible (avoids string/int mismatches)
    if isinstance(org_id, str):
        org_id_str = org_id.strip()
        if org_id_str.isdigit():
            return int(org_id_str)
    return org_id


def is_shared_mock_mode():
    """Return True when app is running in legacy global mock-data mode."""
    config_client_id = None
    if isinstance(IFOOD_CONFIG, dict):
        config_client_id = IFOOD_CONFIG.get('client_id')
    if str(config_client_id or '').strip().upper() == 'MOCK_DATA_MODE':
        return True
    return bool(IFOOD_API and getattr(IFOOD_API, 'use_mock_data', False))


_MERCHANT_UUID_RE = re.compile(
    r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
)


def normalize_merchant_id(value) -> str:
    """Normalize merchant id values that may include pasted labels/noise."""
    text = str(value or '').strip()
    if not text:
        return ''

    uuid_match = _MERCHANT_UUID_RE.search(text)
    if uuid_match:
        return uuid_match.group(0)

    compact = re.sub(r'[\r\n\t]+', ' ', text)
    compact = re.sub(r'\s+', ' ', compact).strip()
    return compact


def sanitize_merchant_name(value) -> str:
    """Sanitize merchant display names pasted from tabular sources."""
    text = str(value or '').strip()
    if not text:
        return ''

    compact = text.replace('\r', ' ').replace('\n', ' ').strip()
    if '\t' in compact:
        parts = [p.strip() for p in compact.split('\t') if p and str(p).strip()]
        candidates = []
        for part in parts:
            cleaned = re.sub(
                r'(?i)^\s*(tipo de loja|merchant id|merchant uuid)\s*[:\-]?\s*',
                '',
                str(part or '').strip()
            ).strip()
            cleaned_lower = cleaned.lower()
            if cleaned and cleaned_lower not in ('tipo de loja', 'merchant id', 'merchant uuid'):
                candidates.append(cleaned)
        if candidates:
            # Prefer the longest non-header token; this is usually the store name.
            compact = max(candidates, key=lambda item: len(str(item)))
        elif parts:
            compact = parts[0]

    compact = re.sub(r'\s{2,}', ' ', compact).strip()
    return compact


def _count_orders_in_restaurant_list(restaurants) -> int:
    total = 0
    if not isinstance(restaurants, list):
        return 0
    for restaurant in restaurants:
        if not isinstance(restaurant, dict):
            continue
        orders = restaurant.get('_orders_cache') or []
        if isinstance(orders, list):
            total += len(orders)
    return total


def _sync_org_restaurants_from_cache(org_id: int, org: dict, max_age_hours: int = 12, force: bool = False):
    """Refresh in-memory org restaurants from DB cache when cache is newer/richer."""
    if not org_id or not isinstance(org, dict):
        return

    now_ts = time.time()
    if not force:
        last_sync_check = float(org.get('_cache_sync_checked_at') or 0)
        if (now_ts - last_sync_check) < 20:
            return
    org['_cache_sync_checked_at'] = now_ts

    cache_meta = db.load_org_data_cache_meta(org_id, 'restaurants', max_age_hours=max_age_hours)
    if not isinstance(cache_meta, dict):
        return

    cached_restaurants = cache_meta.get('data')
    cache_created_at = cache_meta.get('created_at')
    if not isinstance(cached_restaurants, list) or not cached_restaurants:
        return

    current_restaurants = org.get('restaurants') or []
    current_last_refresh = org.get('last_refresh')
    current_order_count = _count_orders_in_restaurant_list(current_restaurants)
    cached_order_count = _count_orders_in_restaurant_list(cached_restaurants)

    should_replace = False
    if not current_restaurants:
        should_replace = True
    elif isinstance(cache_created_at, datetime):
        cache_is_newer = (
            (not isinstance(current_last_refresh, datetime))
            or (cache_created_at > (current_last_refresh + timedelta(seconds=5)))
        )
        if cache_is_newer:
            # Do not replace richer in-memory order caches with leaner snapshots.
            if current_order_count <= 0:
                should_replace = True
            elif cached_order_count >= current_order_count:
                should_replace = True
            elif len(cached_restaurants) > len(current_restaurants) and cached_order_count > 0:
                should_replace = True
    if not should_replace and current_order_count <= 0 and cached_order_count > 0:
        should_replace = True

    if should_replace:
        org['restaurants'] = cached_restaurants
        if isinstance(cache_created_at, datetime):
            org['last_refresh'] = cache_created_at


def get_current_org_restaurants():
    """Get restaurant data for the current session's org"""
    org_id = get_current_org_id()
    if org_id:
        org = get_org_data(org_id)
        _sync_org_restaurants_from_cache(org_id, org, max_age_hours=12)
        org_restaurants = org.get('restaurants') or []
        if org_restaurants:
            return org_restaurants

        # If this org has no scoped iFood source, prefer legacy global data.
        # This avoids stale org cache shadowing live legacy refresh/keepalive data.
        has_scoped_source = bool(org.get('api'))
        if not has_scoped_source:
            org_config = org.get('config') or db.get_org_ifood_config(org_id) or {}
            scoped_merchants = org_config.get('merchants') if isinstance(org_config, dict) else []
            if isinstance(scoped_merchants, str):
                try:
                    scoped_merchants = json.loads(scoped_merchants)
                except Exception:
                    scoped_merchants = []
            has_scoped_source = bool(scoped_merchants)
        if not has_scoped_source and ENABLE_LEGACY_FALLBACK and RESTAURANTS_DATA:
            return RESTAURANTS_DATA

        # Load tenant cache on-demand so newly selected orgs immediately show stores.
        cached_org_meta = db.load_org_data_cache_meta(org_id, 'restaurants', max_age_hours=12)
        cached_org_data = cached_org_meta.get('data') if isinstance(cached_org_meta, dict) else None
        if isinstance(cached_org_data, list) and cached_org_data:
            org['restaurants'] = cached_org_data
            cached_created_at = cached_org_meta.get('created_at') if isinstance(cached_org_meta, dict) else None
            org['last_refresh'] = cached_created_at if isinstance(cached_created_at, datetime) else datetime.now()
            return cached_org_data

        # Retry iFood init occasionally (supports env fallback credentials).
        now = time.time()
        attempted_at = org.get('init_attempted_at')
        if not attempted_at or (now - attempted_at) > 300:
            org['init_attempted_at'] = now
            api = org.get('api') or _init_org_ifood(org_id)
            if api:
                _load_org_restaurants(org_id)
                org_restaurants = org.get('restaurants') or []
                if org_restaurants:
                    return org_restaurants

        # In shared mock mode, expose legacy mock restaurants to all orgs.
        if is_shared_mock_mode():
            return RESTAURANTS_DATA
    if is_shared_mock_mode():
        return RESTAURANTS_DATA
    if ENABLE_LEGACY_FALLBACK:
        return RESTAURANTS_DATA
    return []


def get_current_org_api():
    """Get iFood API client for current org (fallback to legacy global)."""
    org_id = get_current_org_id()
    if org_id and org_id in ORG_DATA:
        org_api = ORG_DATA[org_id].get('api')
        if org_api:
            return org_api
        if is_shared_mock_mode():
            return IFOOD_API
    if is_shared_mock_mode():
        return IFOOD_API
    if not ENABLE_LEGACY_FALLBACK:
        return None
    return IFOOD_API


def get_current_org_last_refresh():
    """Get last refresh timestamp for current org."""
    org_id = get_current_org_id()
    if org_id and org_id in ORG_DATA:
        org_refresh = ORG_DATA[org_id].get('last_refresh')
        if org_refresh:
            return org_refresh
        if is_shared_mock_mode():
            return LAST_DATA_REFRESH
    if is_shared_mock_mode():
        return LAST_DATA_REFRESH
    if not ENABLE_LEGACY_FALLBACK:
        return None
    return LAST_DATA_REFRESH


def get_resilient_api_client():
    """
    Return the best available iFood API client for this request context.
    Falls back to org init and global init when org-scoped client is absent.
    """
    api = get_current_org_api()
    if api:
        return api

    org_id = get_current_org_id()
    if org_id:
        try:
            org = get_org_data(org_id)
            org_api = org.get('api')
            if org_api:
                return org_api
            org_api = _init_org_ifood(org_id)
            if org_api:
                org['api'] = org_api
                return org_api
        except Exception:
            pass

    global IFOOD_API
    try:
        if not IFOOD_API:
            initialize_ifood_api()
    except Exception:
        pass
    return IFOOD_API


def _restaurant_id_candidates(restaurant: dict):
    candidates = set()
    if not isinstance(restaurant, dict):
        return candidates
    for key in ('id', 'merchant_id', 'merchantId', 'ifood_merchant_id', '_resolved_merchant_id'):
        value = restaurant.get(key)
        if value is not None:
            text = str(value).strip()
            if text:
                candidates.add(text)
                candidates.add(text.lower())
                normalized = normalize_merchant_id(text)
                if normalized:
                    candidates.add(normalized)
                    candidates.add(normalized.lower())
    return candidates


def find_restaurant_by_identifier(restaurant_id: str, restaurants: Optional[List[Dict]] = None):
    """Find restaurant by any known identifier alias."""
    target = str(restaurant_id or '').strip()
    if not target:
        return None
    target_lower = target.lower()
    target_normalized = normalize_merchant_id(target)
    target_normalized_lower = target_normalized.lower() if target_normalized else ''
    pool = restaurants if isinstance(restaurants, list) else get_current_org_restaurants()
    for restaurant in pool:
        if not isinstance(restaurant, dict):
            continue
        candidates = _restaurant_id_candidates(restaurant)
        if (
            target in candidates
            or target_lower in candidates
            or (target_normalized and target_normalized in candidates)
            or (target_normalized_lower and target_normalized_lower in candidates)
        ):
            return restaurant
    return None


def _get_org_restaurant_pool(org_id: int):
    """Load org-scoped restaurant pool with cache fallback."""
    org = get_org_data(org_id)
    _sync_org_restaurants_from_cache(org_id, org, max_age_hours=12)
    source_restaurants = org.get('restaurants') or []
    if source_restaurants:
        return source_restaurants
    cached = db.load_org_data_cache(org_id, 'restaurants', max_age_hours=12)
    return cached if isinstance(cached, list) else []


def find_restaurant_in_org(restaurant_id: str, org_id: int):
    """Find restaurant by identifier within a specific org's data (no session required)."""
    return find_restaurant_by_identifier(restaurant_id, restaurants=_get_org_restaurant_pool(org_id))


def enrich_plan_payload(plan_row):
    """Attach plan marketing metadata used by admin UI."""
    name = (plan_row.get('name') or '').lower()
    marketing = PLAN_CATALOG_UI.get(name, {})
    payload = dict(plan_row)
    payload['subtitle'] = marketing.get('subtitle', '')
    payload['badge'] = marketing.get('badge')
    payload['highlight'] = bool(marketing.get('highlight'))
    payload['note'] = marketing.get('note', '')
    payload['features_ui'] = marketing.get('features_ui', [])
    return payload


ONBOARDING_STEPS = [
    {'id': 'connect_ifood', 'label': 'Conectar credenciais iFood'},
    {'id': 'add_merchants', 'label': 'Cadastrar merchants'},
    {'id': 'first_refresh', 'label': 'Executar primeiro refresh'},
    {'id': 'create_group', 'label': 'Criar primeiro grupo de cliente'},
    {'id': 'invite_team', 'label': 'Convidar equipe'},
]


def build_onboarding_state(org_id):
    settings = db.get_org_settings(org_id) or {}
    onboarding = settings.get('onboarding') if isinstance(settings.get('onboarding'), dict) else {}
    manual_completed = set(onboarding.get('completed_steps') or [])
    dismissed = bool(onboarding.get('dismissed'))

    cfg = db.get_org_ifood_config(org_id) or {}
    merchants = cfg.get('merchants') or []
    if isinstance(merchants, str):
        try:
            merchants = json.loads(merchants)
        except Exception:
            merchants = []

    has_credentials = bool((cfg.get('client_id') or '').strip() and (cfg.get('client_secret') or '').strip())
    has_merchants = len(merchants) > 0
    has_refresh = bool(ORG_DATA.get(org_id, {}).get('last_refresh') or LAST_DATA_REFRESH)
    has_invited = len(db.get_org_users(org_id)) > 1

    has_group = False
    conn = db.get_connection()
    if conn:
        cursor = conn.cursor()
        try:
            if _table_has_org_id(cursor, 'client_groups'):
                cursor.execute("SELECT 1 FROM client_groups WHERE org_id=%s LIMIT 1", (org_id,))
            else:
                cursor.execute("SELECT 1 FROM client_groups LIMIT 1")
            has_group = cursor.fetchone() is not None
        except Exception:
            has_group = False
        finally:
            cursor.close()
            conn.close()

    auto_completed = {
        'connect_ifood': has_credentials,
        'add_merchants': has_merchants,
        'first_refresh': has_refresh,
        'create_group': has_group,
        'invite_team': has_invited,
    }

    steps = []
    next_step = None
    for step in ONBOARDING_STEPS:
        sid = step['id']
        done = bool(auto_completed.get(sid) or sid in manual_completed)
        if not done and next_step is None:
            next_step = sid
        steps.append({
            'id': sid,
            'label': step['label'],
            'done': done,
            'auto_done': bool(auto_completed.get(sid))
        })

    completed_count = sum(1 for s in steps if s['done'])
    return {
        'dismissed': dismissed,
        'completed_steps': sorted(set(list(manual_completed) + [s['id'] for s in steps if s['done']])),
        'steps': steps,
        'completed_count': completed_count,
        'total_steps': len(steps),
        'is_complete': completed_count == len(steps),
        'next_step': next_step,
        'updated_at': onboarding.get('updated_at')
    }


def log_exception(context, exc):
    logger.exception("%s | %s", context, type(exc).__name__)


def internal_error_response(message='Internal server error', status=500):
    return jsonify({'success': False, 'error': message}), status


def ensure_csrf_token():
    token = session.get('_csrf_token')
    if not token:
        token = uuid.uuid4().hex
        session['_csrf_token'] = token
    return token


def _request_origin_matches_host():
    origin = request.headers.get('Origin') or request.headers.get('Referer')
    if not origin:
        return False
    try:
        parsed = urlparse(origin)
        return bool(parsed.scheme and parsed.netloc and parsed.scheme == request.scheme and parsed.netloc == request.host)
    except Exception:
        return False


def get_public_base_url():
    configured = str(os.environ.get('PUBLIC_BASE_URL') or '').strip().rstrip('/')
    if configured:
        return configured
    return request.host_url.rstrip('/')


def _rate_limit_key(scope):
    forwarded = request.headers.get('X-Forwarded-For', '')
    client_ip = forwarded.split(',')[0].strip() if forwarded else (request.remote_addr or 'unknown')
    return f"rl:{scope}:{client_ip}"


def _check_rate_limit(scope, limit, window_seconds):
    now = int(time.time())
    key = _rate_limit_key(scope)
    redis_client = get_redis_client()

    if redis_client:
        cutoff = now - window_seconds
        try:
            pipe = redis_client.pipeline()
            pipe.zremrangebyscore(key, 0, cutoff)
            pipe.zcard(key)
            pipe.expire(key, window_seconds + 5)
            _, count, _ = pipe.execute()
            if int(count or 0) >= limit:
                return False
            redis_client.zadd(key, {f"{now}:{uuid.uuid4().hex}": now})
            redis_client.expire(key, window_seconds + 5)
            return True
        except Exception:
            pass

    with _RATE_LIMIT_LOCK:
        bucket = _RATE_LIMIT_LOCAL.get(key)
        if bucket is None:
            bucket = deque()
            _RATE_LIMIT_LOCAL[key] = bucket
        cutoff = now - window_seconds
        while bucket and bucket[0] <= cutoff:
            bucket.popleft()
        if len(bucket) >= limit:
            return False
        bucket.append(now)
        return True


def rate_limit(limit, window_seconds, scope):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if not _check_rate_limit(scope=scope, limit=limit, window_seconds=window_seconds):
                return jsonify({'success': False, 'error': 'Too many requests'}), 429
            return f(*args, **kwargs)
        return wrapped
    return decorator


@app.before_request
def csrf_protect():
    if request.method not in ('POST', 'PUT', 'PATCH', 'DELETE'):
        return None
    if not request.path.startswith('/api/'):
        return None
    if 'user' not in session:
        return None

    csrf_exempt = {
        '/api/login',
        '/api/register',
    }
    if request.path in csrf_exempt:
        return None

    session_token = session.get('_csrf_token')
    header_token = request.headers.get('X-CSRF-Token') or request.headers.get('X-CSRFToken')
    if session_token and header_token and hmac.compare_digest(str(session_token), str(header_token)):
        return None

    if _request_origin_matches_host():
        return None

    return jsonify({'success': False, 'error': 'CSRF validation failed'}), 403


def parse_month_filter(raw_month):
    """Validate month query parameter.

    Returns:
        0 for "all" months,
        1..12 for a specific month,
        None for invalid input.
    """
    if raw_month in (None, '', 'all'):
        return 0
    raw_str = str(raw_month).strip()
    if not raw_str.isdigit():
        return None
    month_value = int(raw_str)
    if 1 <= month_value <= 12:
        return month_value
    return None


def month_filter_label(month_filter):
    """Serialize internal month filter value for API responses."""
    if month_filter in (None, 0):
        return 'all'
    return f"{int(month_filter):02d}"


def get_json_payload():
    """Safely parse JSON payloads and always return a dict."""
    payload = request.get_json(silent=True)
    return payload if isinstance(payload, dict) else {}


def escape_html_text(value):
    """Escape untrusted text for HTML placeholder replacement."""
    return html.escape(str(value if value is not None else ''), quote=True)


def safe_json_for_script(value):
    """Serialize JSON for inline <script> usage without closing script tags."""
    return json.dumps(value, ensure_ascii=False).replace('</', '<\\/')


def sanitize_restaurant_payload(restaurant):
    return {k: v for k, v in restaurant.items() if not k.startswith('_')}


def normalize_order_status_value(status_value):
    """Canonicalize diverse order status payloads into dashboard-friendly values."""
    if isinstance(status_value, dict):
        status_value = (
            status_value.get('orderStatus')
            or status_value.get('status')
            or status_value.get('state')
            or status_value.get('fullCode')
            or status_value.get('code')
        )

    status = str(status_value or '').strip().upper()
    if not status:
        return 'UNKNOWN'

    status = status.replace('-', '_').replace(' ', '_')
    if status == 'CANCELED':
        status = 'CANCELLED'

    if 'CANCEL' in status or status in {'CAN', 'DECLINED', 'REJECTED'}:
        return 'CANCELLED'

    if status in {'CON', 'CONCLUDED', 'COMPLETED', 'DELIVERED', 'FINISHED'}:
        return 'CONCLUDED'

    if status in {'CFM', 'CONFIRMED', 'PLACED', 'CREATED', 'PREPARING', 'READY', 'HANDOFF', 'IN_TRANSIT', 'DISPATCHED', 'PICKED_UP'}:
        return 'CONFIRMED'

    return status


def get_order_status(order):
    if not isinstance(order, dict):
        return 'UNKNOWN'

    for key in ('orderStatus', 'status', 'state', 'fullCode', 'code'):
        normalized = normalize_order_status_value(order.get(key))
        if normalized != 'UNKNOWN':
            return normalized

    metadata = order.get('metadata')
    if isinstance(metadata, dict):
        for key in ('orderStatus', 'status', 'state', 'fullCode', 'code'):
            normalized = normalize_order_status_value(metadata.get(key))
            if normalized != 'UNKNOWN':
                return normalized

    return 'UNKNOWN'


def _safe_float_amount(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def extract_order_amount(order: dict) -> float:
    """Best-effort extraction of order monetary amount from heterogeneous payloads."""
    if not isinstance(order, dict):
        return 0.0

    direct_total = _safe_float_amount(order.get('totalPrice'))
    if direct_total > 0:
        return direct_total

    total = order.get('total')
    if isinstance(total, dict):
        for key in ('orderAmount', 'totalPrice', 'amount'):
            amount = _safe_float_amount(total.get(key))
            if amount > 0:
                return amount
        sub_total = _safe_float_amount(total.get('subTotal'))
        delivery_fee = _safe_float_amount(total.get('deliveryFee'))
        combined = sub_total + delivery_fee
        if combined > 0:
            return combined

    for key in ('orderAmount', 'amount', 'totalAmount', 'value'):
        amount = _safe_float_amount(order.get(key))
        if amount > 0:
            return amount

    payment = order.get('payment')
    if isinstance(payment, dict):
        for key in ('amount', 'value', 'total', 'paidAmount'):
            amount = _safe_float_amount(payment.get(key))
            if amount > 0:
                return amount

    payments = order.get('payments')
    if isinstance(payments, list):
        paid_total = 0.0
        for p in payments:
            if not isinstance(p, dict):
                continue
            value = 0.0
            for key in ('amount', 'value', 'total', 'paidAmount'):
                value = _safe_float_amount(p.get(key))
                if value > 0:
                    break
            paid_total += value
        if paid_total > 0:
            return paid_total

    items = order.get('items')
    if isinstance(items, list) and items:
        items_total = 0.0
        for item in items:
            if not isinstance(item, dict):
                continue
            item_total = _safe_float_amount(item.get('totalPrice'))
            if item_total <= 0:
                qty = _safe_float_amount(item.get('quantity') or 1)
                unit = _safe_float_amount(item.get('unitPrice'))
                item_total = qty * unit if qty > 0 and unit > 0 else 0.0
            items_total += item_total
        if items_total > 0:
            return items_total

    return 0.0


def _order_needs_detail_enrichment(order: dict) -> bool:
    if not isinstance(order, dict):
        return False
    order_id = str(order.get('id') or order.get('orderId') or order.get('order_id') or '').strip()
    if not order_id:
        return False
    amount = extract_order_amount(order)
    status = get_order_status(order)
    return amount <= 0 or status in ('UNKNOWN', '')


def _enrich_orders_with_details(
    api_client,
    merchant_id: str,
    orders: list,
    max_lookups: int = 20,
    orders_already_normalized: bool = False
):
    if not api_client or not hasattr(api_client, 'get_order_details'):
        return orders, 0, 0
    if not isinstance(orders, list) or not orders:
        return orders, 0, 0

    merged_orders = []
    seen_order_ids = set()
    lookups = 0
    updated = 0
    merchant_text = str(merchant_id or '').strip()

    for raw_order in orders:
        if orders_already_normalized:
            order = raw_order
        else:
            order = normalize_order_payload(raw_order) if isinstance(raw_order, dict) else raw_order
        if not isinstance(order, dict):
            continue

        order_id = str(order.get('id') or order.get('orderId') or order.get('order_id') or '').strip()
        if (
            order_id
            and order_id not in seen_order_ids
            and lookups < max_lookups
            and _order_needs_detail_enrichment(order)
        ):
            seen_order_ids.add(order_id)
            lookups += 1
            details = None
            try:
                details = api_client.get_order_details(order_id)
            except Exception:
                details = None

            if isinstance(details, dict) and details:
                candidate = dict(order)
                for key, value in details.items():
                    if value is None:
                        continue
                    if isinstance(value, str) and not value.strip():
                        continue
                    candidate[key] = value
                if not candidate.get('id'):
                    candidate['id'] = order_id
                if merchant_text and not candidate.get('merchantId'):
                    candidate['merchantId'] = merchant_text
                normalized_candidate = normalize_order_payload(candidate)
                if normalized_candidate != order:
                    updated += 1
                merged_orders.append(normalized_candidate)
                continue

        merged_orders.append(order)

    return merged_orders, lookups, updated


def normalize_order_payload(order):
    """Best-effort normalization so downstream metrics use consistent fields.

    Returns a shallow copy with normalized fields — does not mutate the original.
    """
    if not isinstance(order, dict):
        return order

    order = order.copy()
    normalized_status = get_order_status(order)
    order['orderStatus'] = normalized_status

    if not order.get('createdAt'):
        created_candidate = None
        for key in (
            'created_at',
            'created',
            'createdDate',
            'creationDate',
            'orderCreatedAt',
            'orderDate',
            'timestamp',
            'date',
            'lastStatusDate',
            'updatedAt',
            'eventCreatedAt',
        ):
            value = order.get(key)
            if value in (None, ''):
                continue
            parsed = _parse_generic_datetime(value)
            created_candidate = parsed.isoformat() if parsed else value
            break
        if created_candidate:
            order['createdAt'] = created_candidate

    if not order.get('totalPrice'):
        amount = extract_order_amount(order)
        if amount > 0:
            order['totalPrice'] = amount

    return order


def filter_orders_by_month(orders, month_filter):
    if month_filter in (0, 'all'):
        return orders
    target_month = int(month_filter)
    filtered = []
    undated = []
    for order in orders:
        try:
            normalize_order_payload(order)
            order_date = _parse_order_datetime(order)
            if not order_date:
                undated.append(order)
                continue
            if order_date.month == target_month:
                filtered.append(order)
        except Exception:
            continue
    if filtered:
        return filtered
    if undated:
        # Keep undated events visible instead of collapsing the dashboard to zero.
        return undated
    return filtered


def resolve_current_org_fetch_days(default_days=30):
    """Resolve configured fetch window (days) for the current org."""
    days = default_days
    try:
        org_id = get_current_org_id()
        config = {}
        if org_id:
            org = get_org_data(org_id)
            config = org.get('config') or db.get_org_ifood_config(org_id) or {}
        elif isinstance(IFOOD_CONFIG, dict):
            config = IFOOD_CONFIG

        settings = config.get('settings') if isinstance(config, dict) else {}
        if isinstance(settings, dict) and settings.get('data_fetch_days') is not None:
            days = int(settings.get('data_fetch_days'))
        elif isinstance(config, dict) and config.get('data_fetch_days') is not None:
            days = int(config.get('data_fetch_days'))
    except Exception:
        days = default_days

    return max(1, min(int(days or default_days), 365))


def _normalize_orders_list(orders_payload):
    return [normalize_order_payload(o) for o in (orders_payload or []) if isinstance(o, dict)]


def _orders_have_identifiable_ids(orders_payload):
    return any(
        str(o.get('id') or o.get('orderId') or o.get('order_id') or '').strip()
        for o in (orders_payload or [])
        if isinstance(o, dict)
    )


def _normalized_candidate_id_list(*values):
    candidates = []
    seen_ids = set()
    for value in values:
        text = normalize_merchant_id(value)
        if text and text not in seen_ids:
            seen_ids.add(text)
            candidates.append(text)
    return candidates


def _set_restaurant_resolved_merchant_id(restaurant: dict, resolved_merchant_id):
    if not isinstance(restaurant, dict):
        return
    if not resolved_merchant_id:
        return
    normalized = normalize_merchant_id(resolved_merchant_id) or str(resolved_merchant_id).strip()
    if not normalized:
        return
    restaurant['_resolved_merchant_id'] = str(normalized)
    if not restaurant.get('merchant_id'):
        restaurant['merchant_id'] = str(normalized)


def _maybe_enrich_restaurant_orders(restaurant: dict, api, orders_payload: list, merchant_hint: str):
    if not isinstance(orders_payload, list) or not orders_payload or not api:
        return orders_payload
    if not any(_order_needs_detail_enrichment(o) for o in orders_payload if isinstance(o, dict)):
        return orders_payload

    now_ts = time.time()
    try:
        last_enriched_at = float((restaurant or {}).get('_orders_enriched_at') or 0)
    except Exception:
        last_enriched_at = 0.0
    if (now_ts - last_enriched_at) < 45:
        return orders_payload

    enriched_orders, _, _ = _enrich_orders_with_details(
        api,
        merchant_hint,
        orders_payload,
        max_lookups=25,
        orders_already_normalized=True
    )
    if isinstance(restaurant, dict):
        restaurant['_orders_enriched_at'] = now_ts
    return enriched_orders if isinstance(enriched_orders, list) else orders_payload


def _load_cached_org_restaurant_orders(org_id: int, restaurant: dict, restaurant_id: str):
    """Try to recover cached raw orders from persisted org snapshot."""
    try:
        cached_org_data = db.load_org_data_cache(org_id, 'restaurants', max_age_hours=12)
    except Exception:
        cached_org_data = []
    if not (isinstance(cached_org_data, list) and cached_org_data):
        return None, None

    candidate_ids = _normalized_candidate_id_list(
        restaurant_id,
        restaurant.get('merchant_id'),
        restaurant.get('merchantId'),
        restaurant.get('ifood_merchant_id'),
        restaurant.get('_resolved_merchant_id'),
        restaurant.get('id'),
    )

    cached_match = None
    for candidate_id in candidate_ids:
        cached_match = find_restaurant_by_identifier(candidate_id, restaurants=cached_org_data)
        if cached_match:
            break
    if not isinstance(cached_match, dict):
        return None, None

    cached_orders = _normalize_orders_list(cached_match.get('_orders_cache'))
    if not _orders_have_identifiable_ids(cached_orders):
        return None, None

    resolved_merchant_id = (
        cached_match.get('_resolved_merchant_id')
        or cached_match.get('merchant_id')
        or cached_match.get('merchantId')
        or restaurant.get('_resolved_merchant_id')
        or restaurant.get('merchant_id')
        or restaurant.get('merchantId')
        or restaurant_id
    )
    return cached_orders, resolved_merchant_id


def _collect_candidate_merchant_ids(api, restaurant: dict, restaurant_id: str, org_id_override: int = None):
    candidate_ids = _normalized_candidate_id_list(
        restaurant_id,
        restaurant.get('merchant_id'),
        restaurant.get('merchantId'),
        restaurant.get('ifood_merchant_id'),
        restaurant.get('_resolved_merchant_id'),
    )
    seen_ids = set(candidate_ids)

    def _add_candidate(value):
        candidate = normalize_merchant_id(value)
        if candidate and candidate not in seen_ids:
            seen_ids.add(candidate)
            candidate_ids.append(candidate)

    restaurant_name = str(restaurant.get('name') or '').strip().lower()

    try:
        org_id = org_id_override or get_current_org_id()
        config = {}
        if org_id:
            org = get_org_data(org_id)
            config = org.get('config') or db.get_org_ifood_config(org_id) or {}
        elif isinstance(IFOOD_CONFIG, dict):
            config = IFOOD_CONFIG

        configured_merchants = config.get('merchants') if isinstance(config, dict) else []
        if isinstance(configured_merchants, str):
            try:
                configured_merchants = json.loads(configured_merchants)
            except Exception:
                configured_merchants = []
        if isinstance(configured_merchants, list):
            if len(configured_merchants) == 1 and isinstance(configured_merchants[0], dict):
                _add_candidate(configured_merchants[0].get('merchant_id') or configured_merchants[0].get('id'))
            for merchant in configured_merchants:
                if not isinstance(merchant, dict):
                    continue
                merchant_id_value = merchant.get('merchant_id') or merchant.get('id')
                merchant_name = str(merchant.get('name') or '').strip().lower()
                if merchant_name and restaurant_name and (
                    merchant_name == restaurant_name
                    or merchant_name in restaurant_name
                    or restaurant_name in merchant_name
                ):
                    _add_candidate(merchant_id_value)
    except Exception:
        pass

    merchants_from_api = []
    if hasattr(api, 'get_merchants'):
        try:
            merchants_from_api = api.get_merchants() or []
        except Exception:
            merchants_from_api = []
    if isinstance(merchants_from_api, list):
        if len(merchants_from_api) == 1 and isinstance(merchants_from_api[0], dict):
            _add_candidate(merchants_from_api[0].get('id') or merchants_from_api[0].get('merchantId'))
        for merchant in merchants_from_api:
            if not isinstance(merchant, dict):
                continue
            merchant_id_value = merchant.get('id') or merchant.get('merchantId')
            merchant_name = str(merchant.get('name') or '').strip().lower()
            if merchant_name and restaurant_name and (
                merchant_name == restaurant_name
                or merchant_name in restaurant_name
                or restaurant_name in merchant_name
            ):
                _add_candidate(merchant_id_value)

    return candidate_ids


def _fetch_orders_from_candidate_merchants(api, candidate_ids, start_date, end_date, default_restaurant_id):
    fetched_orders = []
    resolved_merchant_id = str(default_restaurant_id or '')
    for candidate_id in candidate_ids:
        try:
            candidate_orders = api.get_orders(candidate_id, start_date, end_date) or []
        except Exception as e:
            print(f"WARN merchant {candidate_id}: on-demand orders hydration failed: {e}")
            candidate_orders = []
        if candidate_orders:
            fetched_orders = candidate_orders
            resolved_merchant_id = str(candidate_id)
            break
    if not fetched_orders and candidate_ids:
        resolved_merchant_id = str(candidate_ids[0])
    return fetched_orders, resolved_merchant_id


def ensure_restaurant_orders_cache(restaurant: dict, restaurant_id: str, org_id_override: int = None):
    """
    Ensure a store has raw orders cached for detail screens.
    DB snapshots intentionally strip internal cache fields, so this may need
    to rehydrate orders on demand from iFood API.
    """
    if not isinstance(restaurant, dict):
        return []

    def _refresh_metrics_from_cached_orders():
        try:
            merchant_for_refresh = (
                restaurant.get('_resolved_merchant_id')
                or restaurant.get('merchant_id')
                or restaurant.get('merchantId')
                or restaurant.get('id')
                or restaurant_id
            )
            if restaurant.get('_orders_cache'):
                _refresh_restaurant_metrics_from_cache(restaurant, merchant_for_refresh)
        except Exception:
            pass

    normalized_existing = _normalize_orders_list(restaurant.get('_orders_cache'))
    api = get_resilient_api_client()
    current_merchant_hint = (
        normalize_merchant_id(
            restaurant.get('_resolved_merchant_id')
            or restaurant.get('merchant_id')
            or restaurant.get('merchantId')
            or restaurant_id
        )
        or str(restaurant_id or '').strip()
    )

    if _orders_have_identifiable_ids(normalized_existing):
        normalized_existing = _maybe_enrich_restaurant_orders(
            restaurant, api, normalized_existing, current_merchant_hint
        )
        restaurant['_orders_cache'] = normalized_existing
        _refresh_metrics_from_cached_orders()
        return normalized_existing

    org_id = org_id_override or get_current_org_id()
    if org_id:
        cached_orders, resolved_from_cache = _load_cached_org_restaurant_orders(org_id, restaurant, restaurant_id)
        if _orders_have_identifiable_ids(cached_orders):
            cache_hint = normalize_merchant_id(resolved_from_cache) or str(restaurant_id or '').strip()
            cached_orders = _maybe_enrich_restaurant_orders(restaurant, api, cached_orders, cache_hint)
            restaurant['_orders_cache'] = cached_orders
            _set_restaurant_resolved_merchant_id(restaurant, resolved_from_cache)
            _refresh_metrics_from_cached_orders()
            return cached_orders

    if not api or not restaurant_id:
        restaurant['_orders_cache'] = normalized_existing if normalized_existing else []
        return restaurant['_orders_cache']

    days = resolve_current_org_fetch_days(default_days=30)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    candidate_ids = _collect_candidate_merchant_ids(api, restaurant, restaurant_id, org_id_override=org_id_override)
    fetched_orders, resolved_merchant_id = _fetch_orders_from_candidate_merchants(
        api,
        candidate_ids,
        start_date,
        end_date,
        default_restaurant_id=restaurant_id,
    )

    normalized_fetched = _normalize_orders_list(fetched_orders)
    normalized_fetched = _maybe_enrich_restaurant_orders(
        restaurant,
        api,
        normalized_fetched,
        normalize_merchant_id(resolved_merchant_id) or str(resolved_merchant_id or '').strip(),
    )
    _set_restaurant_resolved_merchant_id(restaurant, resolved_merchant_id)

    if normalized_fetched:
        restaurant['_orders_cache'] = normalized_fetched
        _refresh_metrics_from_cached_orders()
        return normalized_fetched

    if normalized_existing:
        restaurant['_orders_cache'] = normalized_existing
        _refresh_metrics_from_cached_orders()
        return normalized_existing

    restaurant['_orders_cache'] = normalized_fetched
    return normalized_fetched


def build_restaurant_cache_record(restaurant: Dict, max_orders: int = 300):
    """Build storage payload while preserving enough raw orders for future hydration."""
    if not isinstance(restaurant, dict):
        return {}
    clean = {k: v for k, v in restaurant.items() if not k.startswith('_')}
    orders = [
        normalize_order_payload(o)
        for o in (restaurant.get('_orders_cache') or [])
        if isinstance(o, dict)
    ]
    if max_orders > 0 and len(orders) > max_orders:
        orders = orders[-max_orders:]
    if orders:
        clean['_orders_cache'] = orders
    resolved_id = (
        restaurant.get('_resolved_merchant_id')
        or restaurant.get('merchant_id')
        or restaurant.get('merchantId')
        or restaurant.get('id')
    )
    if resolved_id:
        clean['_resolved_merchant_id'] = str(resolved_id)
    return clean


def aggregate_dashboard_summary(restaurants):
    total_orders = 0
    gross_revenue = 0.0
    net_revenue = 0.0
    positive_trend_count = 0
    negative_trend_count = 0

    for restaurant in restaurants:
        metrics = restaurant.get('metrics', {})
        trends = metrics.get('trends') or {}
        total_orders += int(metrics.get('total_pedidos') or metrics.get('vendas') or 0)
        gross_revenue += float(metrics.get('valor_bruto') or 0)
        net_revenue += float(metrics.get('liquido') or 0)
        trend_vendas = float(trends.get('vendas') or 0)
        if trend_vendas > 0:
            positive_trend_count += 1
        elif trend_vendas < 0:
            negative_trend_count += 1

    return {
        'store_count': len(restaurants),
        'total_orders': total_orders,
        'gross_revenue': gross_revenue,
        'net_revenue': net_revenue,
        'avg_ticket': (net_revenue / total_orders) if total_orders else 0,
        'positive_trend_count': positive_trend_count,
        'negative_trend_count': negative_trend_count
    }


def _parse_order_datetime(order):
    normalized_order = normalize_order_payload(order or {})
    for key in (
        'createdAt',
        'created_at',
        'created',
        'createdDate',
        'creationDate',
        'orderCreatedAt',
        'orderDate',
        'timestamp',
        'date',
        'lastStatusDate',
        'updatedAt',
        'eventCreatedAt',
    ):
        created_at = (normalized_order or {}).get(key)
        parsed = _parse_generic_datetime(created_at)
        if parsed:
            return parsed
    return None


def _parse_generic_datetime(raw_value):
    """Parse ISO datetime payloads from interruption/status APIs."""
    if not raw_value:
        return None
    if isinstance(raw_value, (int, float)):
        try:
            ts = float(raw_value)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            return datetime.utcfromtimestamp(ts)
        except Exception:
            return None
    raw_text = str(raw_value).strip()
    if not raw_text:
        return None
    if raw_text.isdigit():
        try:
            ts = float(raw_text)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            return datetime.utcfromtimestamp(ts)
        except Exception:
            return None
    try:
        parsed = datetime.fromisoformat(raw_text.replace('Z', '+00:00'))
        if getattr(parsed, 'tzinfo', None) is not None:
            # Convert aware timestamps to UTC before dropping tz to keep comparisons correct.
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def _extract_status_message_text(raw_message) -> str:
    """Normalize status/validation message payloads into readable text."""
    if isinstance(raw_message, dict):
        parts = []
        for key in ('title', 'subtitle', 'description', 'message'):
            value = str(raw_message.get(key) or '').strip()
            if value:
                parts.append(value)
        if parts:
            return " - ".join(parts)
        return str(raw_message).strip()
    return str(raw_message or '').strip()


def detect_restaurant_closure(api_client, merchant_id):
    """Infer if a store is currently closed using interruptions + merchant status."""
    now = datetime.utcnow()
    active_reason = None
    closed_until = None
    active_interruptions = 0
    status_payload = {}

    if not api_client or not merchant_id:
        return {'is_closed': False, 'closure_reason': None, 'closed_until': None, 'active_interruptions_count': 0}

    interruptions = []
    if hasattr(api_client, 'get_interruptions'):
        try:
            interruptions = api_client.get_interruptions(merchant_id) or []
        except Exception:
            interruptions = []

    for interruption in interruptions:
        if not isinstance(interruption, dict):
            continue
        start = _parse_generic_datetime(interruption.get('start'))
        end = _parse_generic_datetime(interruption.get('end'))
        is_active = False
        if start and end:
            is_active = start <= now <= end
        elif start and not end:
            is_active = start <= now
        elif end and not start:
            is_active = now <= end

        if is_active:
            active_interruptions += 1
            if not active_reason:
                active_reason = str(interruption.get('description') or 'Fechado temporariamente').strip()
            if end and (closed_until is None or end > closed_until):
                closed_until = end

    if hasattr(api_client, 'get_merchant_status'):
        try:
            status_payload = api_client.get_merchant_status(merchant_id) or {}
        except Exception:
            status_payload = {}

    # Some providers return status as list (or dict with nested list) instead of a flat dict.
    if isinstance(status_payload, dict):
        for list_key in ('data', 'statuses', 'items', 'results'):
            nested = status_payload.get(list_key)
            if isinstance(nested, list) and nested:
                first = nested[0]
                if isinstance(first, dict):
                    status_payload = first
                    break
    elif isinstance(status_payload, list):
        first_dict = next((item for item in status_payload if isinstance(item, dict)), None)
        if first_dict is not None:
            status_payload = first_dict
        elif status_payload:
            status_payload = {'state': str(status_payload[0])}
        else:
            status_payload = {}
    elif status_payload is None:
        status_payload = {}
    else:
        status_payload = {'state': str(status_payload)}

    state_raw = str(status_payload.get('state') or status_payload.get('status') or '').strip().upper()
    message = _extract_status_message_text(status_payload.get('message'))
    available_flag = status_payload.get('available')

    open_status_values = {'OK', 'OPEN', 'AVAILABLE', 'ONLINE', 'TRUE'}
    closed_status_values = {
        'CLOSED', 'CLOSE', 'OFFLINE', 'UNAVAILABLE', 'PAUSED', 'STOPPED',
        'FALSE', 'NOK', 'ERROR', 'DISCONNECTED', 'DOWN'
    }
    closed_by_state = (state_raw in closed_status_values)
    if not closed_by_state and isinstance(available_flag, bool):
        closed_by_state = (not available_flag)

    if not closed_by_state:
        validations = status_payload.get('validations') or []
        if isinstance(validations, list):
            open_validation_statuses = {'OK', 'OPEN', 'AVAILABLE', 'TRUE', 'SUCCESS'}
            closed_validation_statuses = {'CLOSED', 'OFFLINE', 'UNAVAILABLE', 'PAUSED', 'STOPPED', 'FALSE', 'NOK', 'NOT_OK', 'FAIL', 'FAILED', 'ERROR'}
            for validation in validations:
                if not isinstance(validation, dict):
                    continue
                code = str(validation.get('id') or validation.get('code') or '').strip().lower()
                raw_validation_status = validation.get('status')
                if raw_validation_status in (None, ''):
                    raw_validation_status = validation.get('state')
                if isinstance(raw_validation_status, bool):
                    validation_status = 'TRUE' if raw_validation_status else 'FALSE'
                else:
                    validation_status = str(raw_validation_status or '').strip().upper()

                is_connectivity_check = code in ('is-connected', 'is_connected', 'is.connected.config')
                is_opening_check = code in (
                    'opening-hours', 'opening_hours', 'is-open', 'is_open',
                    'during.opening-hours.config', 'during.opening.hours.config'
                )
                is_availability_check = code in ('is-available', 'is_available')

                if is_connectivity_check or is_opening_check or is_availability_check:
                    validation_message = _extract_status_message_text(
                        validation.get('message') or validation.get('description')
                    )
                    validation_message_lower = validation_message.lower()
                    message_suggests_closed = any(
                        token in validation_message_lower for token in ('fechad', 'closed', 'indispon', 'offline')
                    )
                    if (
                        validation_status in closed_validation_statuses
                        or (validation_status and validation_status not in open_validation_statuses)
                        or message_suggests_closed
                    ):
                        closed_by_state = True
                        if not active_reason:
                            fallback_reason = 'Loja indisponivel no iFood'
                            if is_connectivity_check:
                                fallback_reason = 'Integracao iFood desconectada'
                            elif is_opening_check:
                                fallback_reason = 'Fora do horario de funcionamento'
                            active_reason = validation_message or fallback_reason
                        break

    if not closed_by_state and message:
        msg_lower = message.lower()
        if any(token in msg_lower for token in ('fechad', 'closed', 'indispon', 'offline')):
            closed_by_state = True

    is_closed = bool(active_interruptions > 0 or closed_by_state)
    if not active_reason and is_closed:
        if message:
            active_reason = message
        elif closed_by_state:
            active_reason = f'Status: {state_raw}' if state_raw else 'Loja fechada no momento'
    if not is_closed:
        active_reason = None
        closed_until = None

    return {
        'is_closed': is_closed,
        'closure_reason': active_reason,
        'closed_until': closed_until.isoformat() if closed_until else None,
        'active_interruptions_count': active_interruptions
    }


def evaluate_restaurant_quality(restaurant, reference_last_refresh=None):
    """Build data-quality diagnostics for one restaurant payload."""
    issues = []
    score = 100
    now = datetime.utcnow()
    metrics = restaurant.get('metrics', {}) or {}
    total_orders = int(metrics.get('vendas') or metrics.get('total_pedidos') or restaurant.get('orders') or 0)
    manager = str(restaurant.get('manager') or '').strip()
    neighborhood = str(restaurant.get('neighborhood') or '').strip()

    order_dates = []
    for o in (restaurant.get('_orders_cache') or []):
        dt = _parse_order_datetime(o)
        if dt:
            order_dates.append(dt)

    last_order_at = max(order_dates) if order_dates else None
    last_order_age_days = (now - last_order_at).days if last_order_at else None

    if total_orders <= 0:
        issues.append({'code': 'no_orders', 'severity': 'critical', 'message': 'Loja sem pedidos no periodo carregado'})
        score -= 45
    elif total_orders < 20:
        issues.append({'code': 'low_sample_size', 'severity': 'medium', 'message': 'Baixa amostra de pedidos para analise confiavel'})
        score -= 12

    if not manager:
        issues.append({'code': 'missing_manager', 'severity': 'medium', 'message': 'Sem gestor atribuido'})
        score -= 10

    if not neighborhood:
        issues.append({'code': 'missing_neighborhood', 'severity': 'low', 'message': 'Bairro nao informado'})
        score -= 6

    if last_order_age_days is None:
        issues.append({'code': 'missing_order_timestamps', 'severity': 'high', 'message': 'Nao foi possivel validar recencia dos pedidos'})
        score -= 20
    elif last_order_age_days > 14:
        issues.append({'code': 'stale_orders', 'severity': 'high', 'message': f'Sem pedidos recentes ha {last_order_age_days} dias'})
        score -= 20

    if reference_last_refresh and isinstance(reference_last_refresh, datetime):
        refresh_age_minutes = int((datetime.utcnow() - reference_last_refresh).total_seconds() / 60)
        if refresh_age_minutes > 180:
            issues.append({'code': 'stale_refresh', 'severity': 'medium', 'message': f'Dados sem refresh ha {refresh_age_minutes} minutos'})
            score -= 10

    score = max(0, min(100, score))
    status = 'good'
    if score < 55:
        status = 'poor'
    elif score < 80:
        status = 'warning'

    return {
        'score': score,
        'status': status,
        'issues': issues,
        'issue_count': len(issues),
        'total_orders': total_orders,
        'last_order_at': last_order_at.isoformat() if last_order_at else None,
        'last_order_age_days': last_order_age_days
    }


def build_data_quality_payload(restaurants, reference_last_refresh=None):
    per_store = []
    issue_buckets = {}
    poor = 0
    warning = 0
    good = 0
    score_sum = 0

    for r in restaurants:
        quality = evaluate_restaurant_quality(r, reference_last_refresh=reference_last_refresh)
        per_store.append({
            'store_id': r.get('id'),
            'store_name': r.get('name'),
            'manager': r.get('manager'),
            'quality': quality
        })
        score_sum += quality['score']
        if quality['status'] == 'poor':
            poor += 1
        elif quality['status'] == 'warning':
            warning += 1
        else:
            good += 1
        for issue in quality['issues']:
            code = issue.get('code', 'unknown')
            issue_buckets[code] = issue_buckets.get(code, 0) + 1

    per_store.sort(key=lambda x: (x['quality']['score'], x['store_name'] or ''))
    avg_score = round(score_sum / len(per_store), 1) if per_store else 100.0

    return {
        'summary': {
            'store_count': len(per_store),
            'average_score': avg_score,
            'poor_count': poor,
            'warning_count': warning,
            'good_count': good,
            'issue_buckets': issue_buckets
        },
        'stores': per_store
    }

# ============================================================================
# REAL-TIME SSE (Server-Sent Events) INFRASTRUCTURE
# ============================================================================

class SSEManager:
    """Manages Server-Sent Events for real-time order tracking"""
    
    def __init__(self):
        self._clients = []  # List of queue objects, one per connected client
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._redis_thread = None
        if USE_REDIS_PUBSUB and get_redis_client():
            self._redis_thread = threading.Thread(target=self._redis_listener_loop, daemon=True, name="sse-redis-sub")
            self._redis_thread.start()
    
    def register(self):
        """Register a new SSE client, returns a queue for that client"""
        q = queue.Queue(maxsize=50)
        with self._lock:
            self._clients.append(q)
        return q
    
    def unregister(self, q):
        """Remove a client queue"""
        with self._lock:
            if q in self._clients:
                self._clients.remove(q)

    def _broadcast_local(self, event_type: str, data: dict):
        message = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
        dead_clients = []
        with self._lock:
            for q in self._clients:
                try:
                    q.put_nowait(message)
                except queue.Full:
                    dead_clients.append(q)
            for q in dead_clients:
                self._clients.remove(q)

    def _publish_redis(self, event_type: str, data: dict):
        r = get_redis_client()
        if not (USE_REDIS_PUBSUB and r):
            return
        try:
            payload = {
                'source': REDIS_INSTANCE_ID,
                'event_type': event_type,
                'data': data
            }
            r.publish(REDIS_EVENTS_CHANNEL, json.dumps(payload, ensure_ascii=False, default=str))
        except Exception:
            pass

    def _redis_listener_loop(self):
        """Subscribe to distributed SSE events and relay to local clients."""
        r = get_redis_client()
        if not r:
            return
        while not self._stop_event.is_set():
            pubsub = None
            try:
                pubsub = r.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(REDIS_EVENTS_CHANNEL)
                for message in pubsub.listen():
                    if self._stop_event.is_set():
                        break
                    if not message or message.get('type') != 'message':
                        continue
                    raw = message.get('data')
                    if not raw:
                        continue
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    if payload.get('source') == REDIS_INSTANCE_ID:
                        continue
                    event_type = payload.get('event_type')
                    event_data = payload.get('data', {})
                    if event_type:
                        self._broadcast_local(event_type, event_data)
            except Exception:
                time.sleep(2)
            finally:
                try:
                    if pubsub:
                        pubsub.close()
                except Exception:
                    pass
    
    def broadcast(self, event_type: str, data: dict):
        """Send an event to all connected clients"""
        self._broadcast_local(event_type, data)
        self._publish_redis(event_type, data)
    
    @property
    def client_count(self):
        with self._lock:
            return len(self._clients)

sse_manager = SSEManager()


# ============================================================================
# BACKGROUND DATA REFRESH
# ============================================================================

class BackgroundRefresher:
    """Background thread that periodically refreshes data from iFood API
    and persists snapshots to PostgreSQL for fast cold starts."""
    
    def __init__(self, interval_minutes=30):
        self.interval = interval_minutes * 60
        self._thread = None
        self._stop_event = threading.Event()
        self._refresh_lock = threading.Lock()
        self._is_refreshing = False
    
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="bg-refresh")
        self._thread.start()
        print(f"Ã°Å¸â€â€ž Background refresh started (every {self.interval // 60} min)")
    
    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
    
    def _run(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(self.interval)
            if self._stop_event.is_set():
                break
            self.refresh_now()
    
    def refresh_now(self):
        """Perform a data refresh (thread-safe)"""
        if not self._refresh_lock.acquire(blocking=False):
            return False  # Already refreshing
        lock_token = None
        try:
            lock_token = acquire_refresh_lock()
            if USE_REDIS_QUEUE and not lock_token:
                return False
            self._is_refreshing = True
            status_payload = {'status': 'refreshing', 'timestamp': datetime.now().isoformat()}
            set_refresh_status(status_payload)
            sse_manager.broadcast('refresh_status', status_payload)
            
            _do_data_refresh()
            
            complete_payload = {'status': 'complete', 'timestamp': datetime.now().isoformat(), 'count': len(RESTAURANTS_DATA)}
            set_refresh_status(complete_payload)
            sse_manager.broadcast('refresh_status', complete_payload)
            sse_manager.broadcast('data_updated', {'restaurant_count': len(RESTAURANTS_DATA), 'timestamp': datetime.now().isoformat()})
            return True
        except Exception as e:
            print(f"Ã¢ÂÅ’ Background refresh error: {e}")
            error_payload = {'status': 'error', 'error': str(e), 'timestamp': datetime.now().isoformat()}
            set_refresh_status(error_payload)
            sse_manager.broadcast('refresh_status', error_payload)
            return False
        finally:
            self._is_refreshing = False
            release_refresh_lock(lock_token)
            self._refresh_lock.release()
    
    @property
    def is_refreshing(self):
        return self._is_refreshing

bg_refresher = BackgroundRefresher()


def _acquire_redis_lock(lock_key: str, ttl_seconds: int, *, fallback_token=None, require_redis=False):
    """Acquire a distributed lock with shared semantics for refresh/keepalive loops."""
    r = get_redis_client()
    if not r:
        return None if require_redis else fallback_token
    token = str(uuid.uuid4())
    try:
        ok = r.set(lock_key, token, nx=True, ex=max(1, int(ttl_seconds)))
        return token if ok else None
    except Exception:
        return None if require_redis else fallback_token


def _release_redis_lock(lock_key: str, token, *, ignored_tokens=None):
    """Release a distributed lock only when owned by the current token."""
    if not token:
        return
    ignored = set(ignored_tokens or [])
    if token in ignored:
        return
    r = get_redis_client()
    if not r:
        return
    try:
        current = r.get(lock_key)
        if current == token:
            r.delete(lock_key)
    except Exception:
        pass


def acquire_refresh_lock(ttl_seconds=600):
    """Acquire distributed refresh lock when Redis queue is enabled."""
    if not USE_REDIS_QUEUE:
        return REDIS_INSTANCE_ID
    return _acquire_redis_lock(
        REDIS_REFRESH_LOCK_KEY,
        ttl_seconds,
        fallback_token=None,
        require_redis=True,
    )


def release_refresh_lock(token):
    """Release distributed refresh lock owned by token."""
    if not USE_REDIS_QUEUE:
        return
    _release_redis_lock(REDIS_REFRESH_LOCK_KEY, token)


def acquire_keepalive_lock():
    """Acquire keepalive polling lock to avoid duplicate polling across instances."""
    ttl_seconds = max(20, int(IFOOD_POLL_INTERVAL_SECONDS or 30) * 2)
    return _acquire_redis_lock(
        REDIS_KEEPALIVE_LOCK_KEY,
        ttl_seconds,
        fallback_token=REDIS_INSTANCE_ID,
        require_redis=False,
    )


def release_keepalive_lock(token):
    """Release keepalive polling lock owned by token."""
    _release_redis_lock(REDIS_KEEPALIVE_LOCK_KEY, token, ignored_tokens={REDIS_INSTANCE_ID})


def set_refresh_status(status_payload: dict):
    """Persist refresh status for all instances."""
    if not isinstance(status_payload, dict):
        return
    r = get_redis_client()
    if USE_REDIS_QUEUE and r:
        try:
            r.set(REDIS_REFRESH_STATUS_KEY, json.dumps(status_payload, ensure_ascii=False, default=str), ex=86400)
        except Exception:
            pass


def get_refresh_status():
    """Read shared refresh status when available."""
    r = get_redis_client()
    if USE_REDIS_QUEUE and r:
        try:
            raw = r.get(REDIS_REFRESH_STATUS_KEY)
            if raw:
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    return payload
        except Exception:
            pass
    return {
        'status': 'refreshing' if bg_refresher.is_refreshing else 'idle',
        'timestamp': datetime.now().isoformat()
    }


def enqueue_refresh_job(trigger='api'):
    """Enqueue a refresh request for worker processing."""
    r = get_redis_client()
    if not (USE_REDIS_QUEUE and r):
        return None
    job_id = str(uuid.uuid4())
    payload = {
        'job_id': job_id,
        'trigger': trigger,
        'requested_at': datetime.now().isoformat(),
        'requested_by_instance': REDIS_INSTANCE_ID
    }
    try:
        r.lpush(REDIS_REFRESH_QUEUE, json.dumps(payload, ensure_ascii=False, default=str))
        set_refresh_status({'status': 'queued', 'timestamp': datetime.now().isoformat(), 'job_id': job_id, 'trigger': trigger})
        return job_id
    except Exception:
        return None


_KEEPALIVE_POLL_CYCLE = 0


def _extract_org_merchant_ids(org_config):
    """Collect normalized merchant ids from org iFood config payload."""
    if not isinstance(org_config, dict):
        return []
    merchants = org_config.get('merchants') or []
    if isinstance(merchants, str):
        try:
            merchants = json.loads(merchants)
        except Exception:
            merchants = []
    if not isinstance(merchants, list):
        return []

    ids = []
    for m in merchants:
        if isinstance(m, str):
            merchant_id = normalize_merchant_id(m)
            if merchant_id:
                ids.append(str(merchant_id))
            continue
        if not isinstance(m, dict):
            continue
        merchant_id = normalize_merchant_id(m.get('merchant_id') or m.get('id'))
        if merchant_id:
            ids.append(str(merchant_id))
    # preserve insertion order, drop duplicates
    return list(dict.fromkeys(ids))


def _order_cache_key(order: dict) -> str:
    if not isinstance(order, dict):
        return ''
    return str(
        order.get('id')
        or order.get('orderId')
        or order.get('displayId')
        or f"{order.get('createdAt')}:{order.get('orderStatus')}"
    )


def _find_org_restaurant_record(org_data: dict, merchant_id: str):
    if not isinstance(org_data, dict):
        return None
    wanted = normalize_merchant_id(merchant_id)
    if not wanted:
        return None
    wanted_lower = wanted.lower()
    for restaurant in (org_data.get('restaurants') or []):
        if not isinstance(restaurant, dict):
            continue
        candidates = (
            restaurant.get('_resolved_merchant_id'),
            restaurant.get('merchant_id'),
            restaurant.get('merchantId'),
            restaurant.get('ifood_merchant_id'),
            restaurant.get('id'),
        )
        for candidate in candidates:
            normalized_candidate = normalize_merchant_id(candidate)
            if normalized_candidate == wanted or normalized_candidate.lower() == wanted_lower:
                return restaurant
    return None


def _extract_event_id_from_payload(event: dict):
    if not isinstance(event, dict):
        return None
    for key in ('id', 'eventId', 'event_id'):
        value = event.get(key)
        if value:
            return str(value)
    metadata = event.get('metadata')
    if isinstance(metadata, dict):
        for key in ('id', 'eventId', 'event_id'):
            value = metadata.get(key)
            if value:
                return str(value)
    return None


def _extract_order_id_from_poll_event(api_client, event: dict):
    if hasattr(api_client, '_extract_order_id_from_event'):
        try:
            value = api_client._extract_order_id_from_event(event)
            if value:
                return str(value)
        except Exception:
            pass
    if not isinstance(event, dict):
        return None
    for key in ('orderId', 'order_id'):
        value = event.get(key)
        if value:
            return str(value)
    metadata = event.get('metadata')
    if isinstance(metadata, dict):
        for key in ('orderId', 'order_id'):
            value = metadata.get(key)
            if value:
                return str(value)
    return None


def _extract_merchant_id_from_poll_event(event: dict):
    if not isinstance(event, dict):
        return None
    for key in ('merchantId', 'merchant_id'):
        value = event.get(key)
        if value:
            return str(value)
    merchant_obj = event.get('merchant')
    if isinstance(merchant_obj, dict):
        for key in ('id', 'merchantId', 'merchant_id'):
            value = merchant_obj.get(key)
            if value:
                return str(value)
    metadata = event.get('metadata')
    if isinstance(metadata, dict):
        for key in ('merchantId', 'merchant_id'):
            value = metadata.get(key)
            if value:
                return str(value)
        nested_merchant = metadata.get('merchant')
        if isinstance(nested_merchant, dict):
            for key in ('id', 'merchantId', 'merchant_id'):
                value = nested_merchant.get(key)
                if value:
                    return str(value)
    return None


def _extract_status_from_poll_event(api_client, event: dict):
    if hasattr(api_client, '_extract_order_status_from_event'):
        try:
            return api_client._extract_order_status_from_event(event)
        except Exception:
            pass
    if not isinstance(event, dict):
        return None
    for key in ('orderStatus', 'status', 'state', 'fullCode', 'code'):
        value = event.get(key)
        if value:
            return value
    metadata = event.get('metadata')
    if isinstance(metadata, dict):
        for key in ('orderStatus', 'status', 'state', 'fullCode', 'code'):
            value = metadata.get(key)
            if value:
                return value
    return None


def _merge_orders_into_restaurant_cache(restaurant: dict, incoming_orders: list) -> Dict[str, int]:
    if not isinstance(restaurant, dict):
        return {'added': 0, 'updated': 0, 'total': 0}

    def _has_meaningful_value(value):
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) > 0
        return True

    def _merge_order_payloads(existing_payload, incoming_payload):
        """Merge sparse incoming payloads without dropping richer cached fields."""
        if not isinstance(existing_payload, dict):
            return incoming_payload if isinstance(incoming_payload, dict) else existing_payload
        if not isinstance(incoming_payload, dict):
            return existing_payload

        merged_payload = dict(existing_payload)
        for key, incoming_value in incoming_payload.items():
            if key in merged_payload and isinstance(merged_payload.get(key), dict) and isinstance(incoming_value, dict):
                nested = dict(merged_payload.get(key) or {})
                for nested_key, nested_value in incoming_value.items():
                    if _has_meaningful_value(nested_value):
                        nested[nested_key] = nested_value
                merged_payload[key] = nested
                continue
            if _has_meaningful_value(incoming_value):
                merged_payload[key] = incoming_value
        return merged_payload

    merged = {}
    for existing in (restaurant.get('_orders_cache') or []):
        if not isinstance(existing, dict):
            continue
        normalized_existing = normalize_order_payload(existing)
        key = _order_cache_key(normalized_existing)
        if key:
            merged[key] = normalized_existing

    added = 0
    updated = 0
    for order in (incoming_orders or []):
        if not isinstance(order, dict):
            continue
        normalized_order = normalize_order_payload(order)
        key = _order_cache_key(normalized_order)
        if key:
            if key in merged:
                merged_order = _merge_order_payloads(merged.get(key), normalized_order)
                merged_order = normalize_order_payload(merged_order)
                if merged.get(key) != merged_order:
                    updated += 1
                merged[key] = merged_order
            else:
                added += 1
                merged[key] = normalized_order

    restaurant['_orders_cache'] = list(merged.values())
    return {
        'added': max(0, int(added)),
        'updated': max(0, int(updated)),
        'total': len(merged),
    }


def _refresh_restaurant_metrics_from_cache(restaurant: dict, merchant_id: str) -> bool:
    if not isinstance(restaurant, dict):
        return False
    orders = [
        normalize_order_payload(o)
        for o in (restaurant.get('_orders_cache') or [])
        if isinstance(o, dict)
    ]
    if not orders:
        return False

    merchant_lookup_id = str(
        merchant_id
        or restaurant.get('_resolved_merchant_id')
        or restaurant.get('merchant_id')
        or restaurant.get('merchantId')
        or restaurant.get('id')
        or ''
    ).strip()
    if not merchant_lookup_id:
        return False

    closure_snapshot = {
        'is_closed': restaurant.get('is_closed'),
        'closure_reason': restaurant.get('closure_reason'),
        'closed_until': restaurant.get('closed_until'),
        'active_interruptions_count': restaurant.get('active_interruptions_count'),
    }

    merchant_details = {
        'id': merchant_lookup_id,
        'name': restaurant.get('name', 'Unknown Restaurant'),
        'merchantManager': {'name': restaurant.get('manager', 'Gerente')},
        'address': {'neighborhood': restaurant.get('neighborhood', 'Centro')},
        'isSuperRestaurant': bool(
            restaurant.get('isSuperRestaurant')
            or restaurant.get('isSuper')
            or restaurant.get('super')
        ),
    }
    refreshed = IFoodDataProcessor.process_restaurant_data(merchant_details, orders, None)
    if not isinstance(refreshed, dict):
        return False

    for key, value in refreshed.items():
        if str(key).startswith('_'):
            continue
        restaurant[key] = value
    restaurant['_orders_cache'] = orders
    restaurant['_resolved_merchant_id'] = merchant_lookup_id
    if not restaurant.get('merchant_id'):
        restaurant['merchant_id'] = merchant_lookup_id
    for closure_key, closure_value in closure_snapshot.items():
        if closure_value is not None:
            restaurant[closure_key] = closure_value
    return True


def _refresh_restaurant_closure(org_data: dict, api_client, merchant_id: str) -> bool:
    restaurant_record = _find_org_restaurant_record(org_data, merchant_id)
    if not restaurant_record:
        return False
    closure = detect_restaurant_closure(api_client, merchant_id) or {}
    restaurant_record['is_closed'] = bool(closure.get('is_closed'))
    restaurant_record['closure_reason'] = closure.get('closure_reason')
    restaurant_record['closed_until'] = closure.get('closed_until')
    restaurant_record['active_interruptions_count'] = int(closure.get('active_interruptions_count') or 0)
    return True


def run_ifood_keepalive_poll_once():
    """Poll iFood order events to keep test merchants marked as connected/open."""
    global _KEEPALIVE_POLL_CYCLE
    summary = {
        'orgs_checked': 0,
        'merchants_polled': 0,
        'events_received': 0,
        'events_acknowledged': 0,
        'orders_cached': 0,
        'orders_updated': 0,
        'metrics_refreshed': 0,
        'errors': 0
    }

    if not IFOOD_KEEPALIVE_POLLING:
        return summary

    lock_token = acquire_keepalive_lock()
    if lock_token is None:
        return summary

    try:
        org_items = _org_data_items_snapshot()
        has_org_api = any(
            isinstance(org_data, dict) and org_data.get('api')
            for _, org_data in org_items
        )
        # Legacy single-tenant fallback: keepalive must still run when no org API is initialized.
        if IFOOD_API and not has_org_api:
            org_items.append((
                None,
                {
                    'api': IFOOD_API,
                    'config': IFOOD_CONFIG or {},
                    'restaurants': RESTAURANTS_DATA,
                }
            ))

        for org_id, org_data in org_items:
            if not isinstance(org_data, dict):
                continue
            api = org_data.get('api')
            if not api:
                continue

            config = org_data.get('config') or {}
            merchant_ids = _extract_org_merchant_ids(config)
            if not merchant_ids:
                # Refresh config lazily in worker mode when in-memory config is stale.
                if org_id is not None:
                    db_config = db.get_org_ifood_config(org_id) or {}
                    if isinstance(db_config, dict):
                        org_data['config'] = db_config
                        merchant_ids = _extract_org_merchant_ids(db_config)

            if not merchant_ids:
                continue

            summary['orgs_checked'] += 1
            summary['merchants_polled'] += len(merchant_ids)
            merchant_set = {str(mid) for mid in merchant_ids}
            events = []
            org_data_changed = False

            try:
                if hasattr(api, 'poll_events'):
                    events = api.poll_events(merchant_ids) or []
                elif hasattr(api, '_request'):
                    headers = {'x-polling-merchants': ','.join(merchant_ids)}
                    payload = api._request('GET', '/events/v1.0/events:polling', headers=headers)
                    if payload is None:
                        payload = api._request('GET', '/order/v1.0/events:polling', headers=headers)
                    if isinstance(payload, list):
                        events = [e for e in payload if isinstance(e, dict)]
                    elif isinstance(payload, dict):
                        for key in ('events', 'data', 'items'):
                            nested = payload.get(key)
                            if isinstance(nested, list):
                                events = [e for e in nested if isinstance(e, dict)]
                                break
                        if not events:
                            events = [payload]
                summary['events_received'] += len(events)
            except Exception:
                summary['errors'] += 1
                events = []

            events_by_merchant = {}
            orphan_events = []
            for event in events:
                if not isinstance(event, dict):
                    continue
                event_merchant_id = _extract_merchant_id_from_poll_event(event)
                if not event_merchant_id:
                    orphan_events.append(event)
                    continue
                if event_merchant_id not in merchant_set:
                    continue
                events_by_merchant.setdefault(event_merchant_id, []).append(event)
            if len(merchant_ids) == 1 and orphan_events:
                only_merchant_id = str(merchant_ids[0])
                events_by_merchant.setdefault(only_merchant_id, []).extend(orphan_events)

            for merchant_id in merchant_ids:
                try:
                    merchant_events = events_by_merchant.get(str(merchant_id), [])
                    if merchant_events:
                        max_status_entries = max(
                            100,
                            int(str(os.environ.get('KEEPALIVE_ORDER_STATUS_CACHE_MAX', '2000')).strip() or '2000')
                        )
                        latest_event_status_by_order = {}
                        order_ids = []
                        for event in merchant_events:
                            if not isinstance(event, dict):
                                continue
                            order_id = _extract_order_id_from_poll_event(api, event)
                            if not order_id:
                                continue
                            order_ids.append(str(order_id))
                            status_raw = _extract_status_from_poll_event(api, event)
                            normalized_status = normalize_order_status_value(status_raw)
                            if normalized_status == 'UNKNOWN':
                                continue
                            event_created_at = _parse_generic_datetime(event.get('createdAt'))
                            order_key = str(order_id)
                            existing = latest_event_status_by_order.get(order_key)
                            if not existing:
                                if len(latest_event_status_by_order) >= max_status_entries:
                                    continue
                                latest_event_status_by_order[order_key] = {
                                    'status': normalized_status,
                                    'created_at': event_created_at
                                }
                                continue
                            existing_created = existing.get('created_at')
                            if existing_created is None or (event_created_at and event_created_at >= existing_created):
                                latest_event_status_by_order[order_key] = {
                                    'status': normalized_status,
                                    'created_at': event_created_at
                                }

                        dedup_order_ids = list(dict.fromkeys([str(oid) for oid in order_ids if oid]))
                        resolved_orders = []
                        if hasattr(api, 'get_order_details'):
                            for order_id in dedup_order_ids:
                                try:
                                    details = api.get_order_details(order_id)
                                except Exception:
                                    details = None
                                if not isinstance(details, dict) or not details:
                                    continue
                                normalized_current_status = normalize_order_status_value(details.get('orderStatus'))
                                if normalized_current_status == 'UNKNOWN':
                                    event_info = latest_event_status_by_order.get(str(order_id))
                                    if event_info and event_info.get('status'):
                                        details['orderStatus'] = event_info.get('status')
                                resolved_orders.append(normalize_order_payload(details))

                        direct_orders = []
                        for event in merchant_events:
                            if not isinstance(event, dict):
                                continue
                            fallback_order_id = _extract_order_id_from_poll_event(api, event)
                            status_candidate = _extract_status_from_poll_event(api, event)
                            has_order_payload = (
                                ('orderStatus' in event)
                                or ('totalPrice' in event)
                                or ('total' in event)
                                or bool(status_candidate)
                            )
                            if not has_order_payload or not fallback_order_id:
                                continue
                            event_order = dict(event)
                            # Polling event id is usually the event id, not the order id.
                            event_order['id'] = str(fallback_order_id)
                            if status_candidate and not event_order.get('orderStatus'):
                                event_order['orderStatus'] = status_candidate
                            if not event_order.get('merchantId'):
                                event_order['merchantId'] = str(merchant_id)
                            direct_orders.append(normalize_order_payload(event_order))

                        merged_orders = {}
                        for order in resolved_orders + direct_orders:
                            if not isinstance(order, dict):
                                continue
                            key = _order_cache_key(order)
                            if key:
                                merged_orders[key] = order
                        incoming_orders = list(merged_orders.values())

                        if incoming_orders:
                            restaurant_record = _find_org_restaurant_record(org_data, merchant_id)
                            if restaurant_record:
                                merge_result = _merge_orders_into_restaurant_cache(restaurant_record, incoming_orders)
                                added_count = int((merge_result or {}).get('added') or 0)
                                updated_count = int((merge_result or {}).get('updated') or 0)
                                summary['orders_cached'] += added_count
                                summary['orders_updated'] += updated_count
                                if added_count > 0 or updated_count > 0:
                                    org_data_changed = True
                                    try:
                                        if _refresh_restaurant_metrics_from_cache(restaurant_record, merchant_id):
                                            summary['metrics_refreshed'] += 1
                                            org_data_changed = True
                                    except Exception:
                                        summary['errors'] += 1
                except Exception:
                    summary['errors'] += 1
                try:
                    _refresh_restaurant_closure(org_data, api, merchant_id)
                except Exception:
                    summary['errors'] += 1

            if org_data_changed and org_id is not None:
                try:
                    cache_order_limit = max(
                        1,
                        int(str(os.environ.get('ORDERS_CACHE_LIMIT', '300')).strip() or '300')
                    )
                    db.save_org_data_cache(
                        org_id,
                        'restaurants',
                        [
                            build_restaurant_cache_record(r, max_orders=cache_order_limit)
                            for r in (org_data.get('restaurants') or [])
                            if isinstance(r, dict)
                        ]
                    )
                    # Stamp last_refresh to prevent _sync_org_restaurants_from_cache from
                    # immediately replacing the in-memory state with the DB snapshot we
                    # just saved (the timestamp comparison uses org['last_refresh'], which
                    # the keepalive never previously updated).
                    org_data['last_refresh'] = datetime.now()
                except Exception:
                    summary['errors'] += 1

            if events and hasattr(api, 'acknowledge_events'):
                try:
                    ack_result = api.acknowledge_events(events)
                    if isinstance(ack_result, dict) and ack_result.get('success'):
                        summary['events_acknowledged'] += int(ack_result.get('acknowledged') or 0)
                    else:
                        summary['errors'] += 1
                except Exception:
                    summary['errors'] += 1
    finally:
        release_keepalive_lock(lock_token)

    if (summary['orders_cached'] > 0) or (summary['orders_updated'] > 0) or (summary['metrics_refreshed'] > 0):
        # New/updated orders or recomputed metrics were merged in-memory.
        # Drop response cache so UI reflects changes immediately.
        invalidate_cache()
        try:
            _save_data_snapshot()
        except Exception:
            pass

    _KEEPALIVE_POLL_CYCLE += 1
    if summary['errors'] > 0:
        print(
            f"iFood keepalive polling completed with errors "
            f"(orgs={summary['orgs_checked']}, merchants={summary['merchants_polled']}, "
            f"events={summary['events_received']}, acked={summary['events_acknowledged']}, "
            f"orders_cached={summary['orders_cached']}, orders_updated={summary['orders_updated']}, "
            f"metrics_refreshed={summary['metrics_refreshed']}, errors={summary['errors']})"
        )
    elif summary['merchants_polled'] > 0 and (_KEEPALIVE_POLL_CYCLE % 20 == 0):
        # avoid noisy logs: once every ~10 minutes at 30s interval
        print(
            f"iFood keepalive polling active "
            f"(orgs={summary['orgs_checked']}, merchants={summary['merchants_polled']}, "
            f"events={summary['events_received']}, acked={summary['events_acknowledged']}, "
            f"orders_cached={summary['orders_cached']}, orders_updated={summary['orders_updated']}, "
            f"metrics_refreshed={summary['metrics_refreshed']})"
        )
    return summary


_KEEPALIVE_THREAD = None
_KEEPALIVE_STOP_EVENT = threading.Event()
_KEEPALIVE_THREAD_LOCK = threading.Lock()


def _keepalive_loop():
    interval_seconds = max(10, int(IFOOD_POLL_INTERVAL_SECONDS or 30))
    while not _KEEPALIVE_STOP_EVENT.is_set():
        started_at = time.time()
        try:
            run_ifood_keepalive_poll_once()
        except Exception as e:
            print(f"Keepalive poller error: {e}")
        elapsed = time.time() - started_at
        wait_seconds = max(1.0, interval_seconds - elapsed)
        _KEEPALIVE_STOP_EVENT.wait(wait_seconds)


def start_keepalive_poller():
    global _KEEPALIVE_THREAD
    if not IFOOD_KEEPALIVE_POLLING:
        return
    with _KEEPALIVE_THREAD_LOCK:
        if _KEEPALIVE_THREAD and _KEEPALIVE_THREAD.is_alive():
            return
        _KEEPALIVE_STOP_EVENT.clear()
        _KEEPALIVE_THREAD = threading.Thread(target=_keepalive_loop, daemon=True, name="ifood-keepalive")
        _KEEPALIVE_THREAD.start()
        print(f"iFood keepalive poller started (every {IFOOD_POLL_INTERVAL_SECONDS}s)")


def run_refresh_worker_loop(interval_seconds=1800):
    """Redis-backed worker loop for reliable background refresh."""
    print(f"Refresh worker started (interval={interval_seconds}s)")
    r = get_redis_client()
    if not r:
        print("Refresh worker exiting: Redis not available")
        return

    stop_flag = {'stop': False}
    keepalive_enabled = bool(IFOOD_KEEPALIVE_POLLING)
    keepalive_interval = max(10, int(IFOOD_POLL_INTERVAL_SECONDS or 30))
    if keepalive_enabled:
        print(f"iFood keepalive polling enabled (every {keepalive_interval}s)")

    def _handle_stop(signum, frame):
        stop_flag['stop'] = True

    try:
        signal.signal(signal.SIGTERM, _handle_stop)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGINT, _handle_stop)
    except Exception:
        pass

    next_periodic = time.time() + interval_seconds
    next_keepalive = time.time() + keepalive_interval
    if keepalive_enabled:
        run_ifood_keepalive_poll_once()
    while not stop_flag['stop']:
        try:
            deadlines = [next_periodic]
            if keepalive_enabled:
                deadlines.append(next_keepalive)
            timeout = max(1, int(min(deadlines) - time.time()))
            item = r.brpop(REDIS_REFRESH_QUEUE, timeout=timeout)
            now = time.time()
            if item:
                _, raw = item
                try:
                    payload = json.loads(raw)
                except Exception:
                    payload = {}
                refreshed = bg_refresher.refresh_now()
                if refreshed:
                    set_refresh_status({
                        'status': 'done',
                        'timestamp': datetime.now().isoformat(),
                        'job_id': payload.get('job_id'),
                        'trigger': payload.get('trigger', 'queue')
                    })
                else:
                    set_refresh_status({
                        'status': 'busy',
                        'timestamp': datetime.now().isoformat(),
                        'job_id': payload.get('job_id'),
                        'trigger': payload.get('trigger', 'queue')
                    })
                # Reset periodic timer after active queue processing.
                next_periodic = now + interval_seconds
                now = time.time()

            if keepalive_enabled and now >= next_keepalive:
                run_ifood_keepalive_poll_once()
                next_keepalive = now + keepalive_interval

            if now >= next_periodic:
                refreshed = bg_refresher.refresh_now()
                set_refresh_status({
                    'status': 'done' if refreshed else 'busy',
                    'timestamp': datetime.now().isoformat(),
                    'trigger': 'periodic'
                })
                next_periodic = now + interval_seconds
        except Exception as e:
            print(f"Refresh worker error: {e}")
            time.sleep(2)


def _do_data_refresh():
    """Core refresh logic: fetch from API, update cache, save snapshot to DB"""
    global RESTAURANTS_DATA, LAST_DATA_REFRESH
    
    # Refresh per-org data (SaaS mode)
    for org_id, od in _org_data_items_snapshot():
        if od.get('api'):
            try:
                _load_org_restaurants(org_id)
            except Exception as e:
                print(f"Ã¢Å¡Â Ã¯Â¸Â Org {org_id} refresh error: {e}")
    
    # Also refresh legacy global data if configured
    if not IFOOD_API:
        return
    
    new_data = []
    merchants_config = IFOOD_CONFIG.get('merchants', [])
    days = IFOOD_CONFIG.get('data_fetch_days', 30)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    existing_orders_by_merchant = {}
    for existing in RESTAURANTS_DATA:
        if not isinstance(existing, dict):
            continue
        existing_mid = (
            existing.get('merchant_id')
            or existing.get('_resolved_merchant_id')
            or existing.get('id')
        )
        existing_orders = [
            normalize_order_payload(o)
            for o in (existing.get('_orders_cache') or [])
            if isinstance(o, dict)
        ]
        if existing_mid and existing_orders:
            existing_orders_by_merchant[str(existing_mid)] = existing_orders
    
    seen_merchant_ids = set()
    for merchant_config in merchants_config:
        if isinstance(merchant_config, str):
            merchant_config = {'merchant_id': merchant_config}
        if not isinstance(merchant_config, dict):
            continue
        merchant_id = normalize_merchant_id(merchant_config.get('merchant_id') or merchant_config.get('id'))
        if not merchant_id:
            continue
        if merchant_id in seen_merchant_ids:
            continue
        seen_merchant_ids.add(merchant_id)
        name = sanitize_merchant_name(merchant_config.get('name')) or f"Restaurant {str(merchant_id)[:8]}"
        manager_name = sanitize_merchant_name(merchant_config.get('manager')) or 'Gerente'
        
        try:
            merchant_details = IFOOD_API.get_merchant_details(merchant_id)
            if not merchant_details:
                merchant_details = {
                    'id': merchant_id,
                    'name': name,
                    'merchantManager': {'name': manager_name}
                }
            
            fetched_orders = IFOOD_API.get_orders(merchant_id, start_date, end_date) or []
            previous_orders = existing_orders_by_merchant.get(str(merchant_id), [])
            if previous_orders:
                if fetched_orders:
                    merged = {}
                    for order in previous_orders + fetched_orders:
                        if not isinstance(order, dict):
                            continue
                        normalized = normalize_order_payload(order)
                        order_key = str(
                            normalized.get('id')
                            or normalized.get('orderId')
                            or normalized.get('displayId')
                            or f"{normalized.get('createdAt')}:{normalized.get('orderStatus')}"
                        )
                        merged[order_key] = normalized
                    orders = list(merged.values())
                else:
                    orders = previous_orders
            else:
                orders = [
                    normalize_order_payload(order)
                    for order in fetched_orders
                    if isinstance(order, dict)
                ]
            
            financial_data = None
            if hasattr(IFOOD_API, 'get_financial_data'):
                try:
                    financial_data = IFOOD_API.get_financial_data(merchant_id, start_date, end_date)
                except Exception as financial_error:
                    logger.debug("iFood financial data unavailable for %s: %s", merchant_id, financial_error)
            
            restaurant_data = IFoodDataProcessor.process_restaurant_data(merchant_details, orders, financial_data)
            closure = detect_restaurant_closure(IFOOD_API, merchant_id)
            
            if name:
                restaurant_data['name'] = name
            if manager_name:
                restaurant_data['manager'] = manager_name
            restaurant_data['merchant_id'] = merchant_id
            
            restaurant_data['_orders_cache'] = orders
            restaurant_data['is_closed'] = bool(closure.get('is_closed'))
            restaurant_data['closure_reason'] = closure.get('closure_reason')
            restaurant_data['closed_until'] = closure.get('closed_until')
            restaurant_data['active_interruptions_count'] = int(closure.get('active_interruptions_count') or 0)
            new_data.append(restaurant_data)
            
            # Broadcast new order events for real-time tracking
            _detect_and_broadcast_new_orders(merchant_id, name, orders)
            
        except Exception as e:
            print(f"   Ã¢ÂÅ’ Failed to refresh {name}: {e}")
    
    # Atomic swap
    with _GLOBAL_STATE_LOCK:
        RESTAURANTS_DATA = new_data
        LAST_DATA_REFRESH = datetime.now()
    
    # Invalidate API response caches since data changed
    invalidate_cache()
    
    # Save snapshot to DB for fast cold starts
    _save_data_snapshot()
    
    print(f"Refreshed {len(new_data)} restaurant(s) at {LAST_DATA_REFRESH.strftime('%H:%M:%S')}")


# Track last seen order IDs per restaurant for new order detection
_last_order_ids = {}

def _detect_and_broadcast_new_orders(merchant_id: str, restaurant_name: str, orders: list):
    """Compare orders with previously seen ones and broadcast new arrivals"""
    global _last_order_ids
    
    current_ids = set(o.get('id', '') for o in orders[:20])  # Only check recent
    prev_ids = _last_order_ids.get(merchant_id, set())
    
    if prev_ids:  # Don't broadcast on first load
        new_ids = current_ids - prev_ids
        for order in orders:
            if order.get('id') in new_ids:
                sse_manager.broadcast('new_order', {
                    'restaurant_id': merchant_id,
                    'restaurant_name': restaurant_name,
                    'order_id': order.get('id'),
                    'display_id': order.get('displayId', ''),
                    'status': order.get('orderStatus', 'UNKNOWN'),
                    'total': order.get('totalPrice', 0),
                    'customer': order.get('customer', {}).get('name', 'Cliente'),
                    'is_new_customer': order.get('customer', {}).get('isNewCustomer', False),
                    'items_count': len(order.get('items', [])),
                    'order_type': order.get('orderType', 'DELIVERY'),
                    'timestamp': order.get('createdAt', datetime.now().isoformat())
                })
    
    _last_order_ids[merchant_id] = current_ids


def _save_data_snapshot():
    """Persist current restaurant data to PostgreSQL for fast cold starts"""
    try:
        conn = db.get_connection()
        if not conn:
            return
        cursor = conn.cursor()
        
        # Create snapshot table if needed
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_snapshots (
                id SERIAL PRIMARY KEY,
                snapshot_type VARCHAR(50) NOT NULL,
                data JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Prepare data. Keep a bounded raw-order cache so polling-only integrations
        # do not lose historical orders on restart.
        snapshot_order_limit = max(
            1,
            int(str(os.environ.get('ORDERS_SNAPSHOT_LIMIT', '300')).strip() or '300')
        )
        snapshot = [build_restaurant_cache_record(r, max_orders=snapshot_order_limit) for r in RESTAURANTS_DATA]
        
        # Upsert: delete old, insert new
        cursor.execute("DELETE FROM data_snapshots WHERE snapshot_type = 'restaurants'")
        cursor.execute(
            "INSERT INTO data_snapshots (snapshot_type, data) VALUES (%s, %s)",
            ('restaurants', json.dumps(snapshot, ensure_ascii=False, default=str))
        )
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Ã¢Å¡Â Ã¯Â¸Â Failed to save snapshot: {e}")


def _load_data_snapshot():
    """Load cached restaurant data from PostgreSQL for fast cold starts"""
    global RESTAURANTS_DATA, LAST_DATA_REFRESH
    try:
        conn = db.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT data, created_at FROM data_snapshots 
            WHERE snapshot_type = 'restaurants' 
            ORDER BY created_at DESC LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if row:
            data, created_at = row
            if isinstance(data, str):
                data = json.loads(data)
            
            # Only use snapshot if it's less than 2 hours old
            age = datetime.now() - created_at
            if age < timedelta(hours=2):
                with _GLOBAL_STATE_LOCK:
                    RESTAURANTS_DATA = data
                    LAST_DATA_REFRESH = created_at
                print(f"Ã¢Å¡Â¡ Loaded {len(data)} restaurants from DB snapshot ({age.seconds // 60} min old)")
                return True
            else:
                print(f"Ã¢ÂÂ³ DB snapshot too old ({age.seconds // 3600}h), will refresh from API")
                return False
        return False
    except Exception as e:
        print(f"Ã¢Å¡Â Ã¯Â¸Â Failed to load snapshot: {e}")
        return False



# ============================================================================
# IFOOD API INITIALIZATION
# ============================================================================

# ============================================================================
# IFOOD API INITIALIZATION - PER-ORG
# ============================================================================

def _init_org_ifood(org_id):
    """Initialize iFood API for a specific org from DB credentials"""
    config = db.get_org_ifood_config(org_id) or {}
    client_id = str(config.get('client_id') or '').strip()
    client_secret = str(config.get('client_secret') or '').strip()

    # Optional environment fallback for deployments that centralize iFood credentials.
    if not client_id or not client_secret:
        env_client_id = str(os.environ.get('IFOOD_CLIENT_ID') or '').strip()
        env_client_secret = str(os.environ.get('IFOOD_CLIENT_SECRET') or '').strip()
        if env_client_id and env_client_secret:
            client_id = env_client_id
            client_secret = env_client_secret

    if not client_id or not client_secret:
        return None
    org = get_org_data(org_id)
    org['config'] = config
    try:
        use_mock_data = bool(config.get('use_mock_data')) or str(client_id).strip().upper() == 'MOCK_DATA_MODE'
        api = IFoodAPI(client_id, client_secret, use_mock_data=use_mock_data)
        if api.authenticate():
            org['api'] = api
            print(f"Ã¢Å“â€¦ Org {org_id}: iFood API authenticated")
            return api
        else:
            auth_error = getattr(api, 'last_auth_error', None)
            if auth_error:
                print(f"Ã¢Å¡Â Ã¯Â¸Â Org {org_id}: iFood auth failed ({auth_error})")
            else:
                print(f"Ã¢Å¡Â Ã¯Â¸Â Org {org_id}: iFood auth failed")
    except Exception as e:
        print(f"Ã¢ÂÅ’ Org {org_id}: iFood init error: {e}")
    return None


def _load_org_restaurants(org_id):
    """Load restaurant data for a specific org"""
    org = get_org_data(org_id)
    api = org.get('api')
    config = org.get('config') or db.get_org_ifood_config(org_id) or {}
    if not api:
        return
    merchants_config = config.get('merchants', [])
    if isinstance(merchants_config, str):
        try:
            merchants_config = json.loads(merchants_config)
        except Exception:
            merchants_config = []
    if not merchants_config:
        try:
            merchants = api.get_merchants()
            if merchants:
                merchants_config = [{'merchant_id': m.get('id'), 'name': m.get('name', 'Restaurant')} for m in merchants]
                db.update_org_ifood_config(org_id, merchants=merchants_config)
        except Exception as merchants_error:
            logger.debug("Org %s merchants bootstrap skipped: %s", org_id, merchants_error)
    if not merchants_config:
        return
    settings = config.get('settings') if isinstance(config, dict) else {}
    if isinstance(settings, dict) and settings.get('data_fetch_days') is not None:
        days = int(settings.get('data_fetch_days'))
    elif isinstance(config, dict) and config.get('data_fetch_days') is not None:
        days = int(config.get('data_fetch_days'))
    else:
        days = 30
    days = max(1, min(int(days or 30), 365))
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    new_data = []
    existing_orders_by_store = {}
    for existing_store in (org.get('restaurants') or []):
        if not isinstance(existing_store, dict):
            continue
        existing_orders = existing_store.get('_orders_cache') or []
        if not (isinstance(existing_orders, list) and existing_orders):
            continue
        for candidate_id in _restaurant_id_candidates(existing_store):
            existing_orders_by_store[str(candidate_id)] = existing_orders

    def _fallback_restaurant(merchant_id_value, merchant_name_value, merchant_manager_value, neighborhood_value='Centro'):
        return {
            'id': merchant_id_value,
            'merchant_id': merchant_id_value,
            'name': merchant_name_value,
            'manager': merchant_manager_value,
            'neighborhood': neighborhood_value or 'Centro',
            'platforms': ['iFood'],
            'revenue': 0,
            'orders': 0,
            'ticket': 0,
            'trend': 0,
            'approval_rate': 0,
            'avatar_color': '#f97015',
            'rating': 0,
            'isSuper': False,
            'metrics': {
                'vendas': 0,
                'ticket_medio': 0,
                'valor_bruto': 0,
                'liquido': 0,
                'totals': {
                    'vendas': 0,
                    'ticket_medio': 0,
                    'valorBruto': 0,
                    'liquido': 0
                },
                'trends': {
                    'vendas': 0,
                    'ticket_medio': 0,
                    'valor_bruto': 0,
                    'liquido': 0
                }
            }
        }

    seen_merchant_ids = set()
    for mc in merchants_config:
        if isinstance(mc, str):
            mc = {'merchant_id': mc}
        if not isinstance(mc, dict):
            continue
        merchant_id = normalize_merchant_id(mc.get('merchant_id') or mc.get('id'))
        if not merchant_id:
            continue
        if merchant_id in seen_merchant_ids:
            continue
        seen_merchant_ids.add(merchant_id)
        merchant_name = sanitize_merchant_name(mc.get('name')) or f"Restaurant {str(merchant_id)[:8]}"
        merchant_manager = sanitize_merchant_name(mc.get('manager')) or 'Gerente'

        details = None
        try:
            details = api.get_merchant_details(merchant_id)
        except Exception as e:
            print(f"  WARN Org {org_id}, merchant {merchant_id}: details fetch failed: {e}")

        if not isinstance(details, dict):
            details = {
                'id': merchant_id,
                'name': merchant_name,
                'merchantManager': {'name': merchant_manager},
                'address': {'neighborhood': mc.get('neighborhood') or 'Centro'},
                'isSuperRestaurant': False
            }
        else:
            details['id'] = normalize_merchant_id(details.get('id') or merchant_id) or merchant_id
            api_name = sanitize_merchant_name(details.get('name'))
            details['name'] = merchant_name or api_name or f"Restaurant {str(merchant_id)[:8]}"
            manager_obj = details.get('merchantManager') if isinstance(details.get('merchantManager'), dict) else {}
            manager_obj_name = sanitize_merchant_name(manager_obj.get('name'))
            manager_obj['name'] = merchant_manager or manager_obj_name or 'Gerente'
            details['merchantManager'] = manager_obj
            if not isinstance(details.get('address'), dict):
                details['address'] = {'neighborhood': mc.get('neighborhood') or 'Centro'}

        orders = []
        try:
            orders = api.get_orders(merchant_id, start_date, end_date) or []
        except Exception as e:
            print(f"  WARN Org {org_id}, merchant {merchant_id}: orders fetch failed: {e}")

        previous_orders = []
        previous_order_candidates = _normalized_candidate_id_list(
            merchant_id,
            mc.get('merchant_id') if isinstance(mc, dict) else None,
            mc.get('id') if isinstance(mc, dict) else None,
            details.get('id') if isinstance(details, dict) else None,
        )
        for candidate_id in previous_order_candidates:
            previous_orders = existing_orders_by_store.get(str(candidate_id)) or []
            if previous_orders:
                break
        if previous_orders:
            if orders:
                merged = {}
                for order in previous_orders + orders:
                    if not isinstance(order, dict):
                        continue
                    order_key = str(
                        order.get('id')
                        or order.get('orderId')
                        or order.get('displayId')
                        or f"{order.get('createdAt')}:{order.get('orderStatus')}"
                    )
                    merged[order_key] = order
                orders = list(merged.values())
            else:
                orders = previous_orders

        normalized_orders = []
        for order in (orders or []):
            if not isinstance(order, dict):
                continue
            normalized_orders.append(normalize_order_payload(order))
        orders = normalized_orders

        try:
            restaurant_data = IFoodDataProcessor.process_restaurant_data(details, orders, None)
        except Exception as e:
            print(f"  WARN Org {org_id}, merchant {merchant_id}: data processing failed: {e}")
            neighborhood = (details.get('address') or {}).get('neighborhood') if isinstance(details, dict) else 'Centro'
            restaurant_data = _fallback_restaurant(merchant_id, merchant_name, merchant_manager, neighborhood)

        if merchant_name:
            restaurant_data['name'] = sanitize_merchant_name(merchant_name) or merchant_name
        if merchant_manager:
            restaurant_data['manager'] = sanitize_merchant_name(merchant_manager) or merchant_manager
        restaurant_data['merchant_id'] = normalize_merchant_id(merchant_id) or merchant_id

        closure = {
            'is_closed': False,
            'closure_reason': None,
            'closed_until': None,
            'active_interruptions_count': 0
        }
        try:
            fetched_closure = detect_restaurant_closure(api, merchant_id) or {}
            closure.update({
                'is_closed': bool(fetched_closure.get('is_closed')),
                'closure_reason': fetched_closure.get('closure_reason'),
                'closed_until': fetched_closure.get('closed_until'),
                'active_interruptions_count': int(fetched_closure.get('active_interruptions_count') or 0)
            })
        except Exception as e:
            print(f"  WARN Org {org_id}, merchant {merchant_id}: closure fetch failed: {e}")

        restaurant_data['_orders_cache'] = orders
        restaurant_data['is_closed'] = bool(closure.get('is_closed'))
        restaurant_data['closure_reason'] = closure.get('closure_reason')
        restaurant_data['closed_until'] = closure.get('closed_until')
        restaurant_data['active_interruptions_count'] = int(closure.get('active_interruptions_count') or 0)
        new_data.append(restaurant_data)
    org['restaurants'] = new_data
    org['last_refresh'] = datetime.now()
    cache_order_limit = max(
        1,
        int(str(os.environ.get('ORDERS_CACHE_LIMIT', '300')).strip() or '300')
    )
    db.save_org_data_cache(
        org_id,
        'restaurants',
        [build_restaurant_cache_record(r, max_orders=cache_order_limit) for r in new_data]
    )
    print(f"Ã¢Å“â€¦ Org {org_id}: loaded {len(new_data)} restaurants")


def initialize_all_orgs():
    """Initialize iFood API and load data for all active orgs with credentials"""
    global RESTAURANTS_DATA, IFOOD_API, IFOOD_CONFIG, LAST_DATA_REFRESH
    orgs = db.get_all_active_orgs()
    print(f"\nÃ°Å¸ÂÂ¢ Initializing {len(orgs)} organization(s)...")
    for org_info in orgs:
        org_id = org_info['id']
        # Try cache first
        cached_meta = db.load_org_data_cache_meta(org_id, 'restaurants', max_age_hours=2)
        cached = cached_meta.get('data') if isinstance(cached_meta, dict) else None
        if cached:
            od = get_org_data(org_id)
            od['restaurants'] = cached
            cache_created_at = cached_meta.get('created_at') if isinstance(cached_meta, dict) else None
            od['last_refresh'] = cache_created_at if isinstance(cache_created_at, datetime) else datetime.now()
            print(f"  Ã¢Å¡Â¡ Org {org_id} ({org_info['name']}): {len(cached)} restaurants from cache")
            # Init API in background
            threading.Thread(target=_init_and_refresh_org, args=(org_id,), daemon=True).start()
        else:
            api = _init_org_ifood(org_id)
            if api:
                _load_org_restaurants(org_id)
    # Set legacy globals for backward compat (use first org's data)
    if orgs:
        first_org_id = orgs[0].get('id')
        first = get_org_data(first_org_id) if first_org_id is not None else None
        if isinstance(first, dict):
            with _GLOBAL_STATE_LOCK:
                RESTAURANTS_DATA = first.get('restaurants') or []
                IFOOD_API = first.get('api')
                LAST_DATA_REFRESH = first.get('last_refresh')
                IFOOD_CONFIG = first.get('config', {})


def _init_and_refresh_org(org_id):
    """Background: init API and refresh data for an org"""
    api = _init_org_ifood(org_id)
    if api:
        _load_org_restaurants(org_id)


def initialize_ifood_api():
    """Initialize iFood API with credentials from config"""
    global IFOOD_API, IFOOD_CONFIG
    
    # Load configuration
    loaded_config = IFoodConfig.load_config(str(CONFIG_FILE))
    with _GLOBAL_STATE_LOCK:
        IFOOD_CONFIG = loaded_config or {}
    
    if not loaded_config:
        print("Ã¢Å¡Â Ã¯Â¸Â  No iFood configuration found")
        print(f"   Creating sample config at {CONFIG_FILE}")
        IFoodConfig.create_sample_config(str(CONFIG_FILE))
        return False
    
    client_id = loaded_config.get('client_id')
    client_secret = loaded_config.get('client_secret')
    
    if not client_id or client_id == 'your_ifood_client_id_here':
        print("Ã¢Å¡Â Ã¯Â¸Â  iFood API credentials not configured")
        print(f"   Please update {CONFIG_FILE} with your credentials")
        return False
    
    # Initialize API
    use_mock_data = bool(loaded_config.get('use_mock_data')) or str(client_id).strip().upper() == 'MOCK_DATA_MODE'
    created_api = IFoodAPI(client_id, client_secret, use_mock_data=use_mock_data)
    
    # Authenticate
    if created_api.authenticate():
        with _GLOBAL_STATE_LOCK:
            IFOOD_API = created_api
        print("Ã¢Å“â€¦ iFood API initialized successfully")
        return True
    else:
        print("Ã¢ÂÅ’ iFood API authentication failed")
        return False


def load_restaurants_from_ifood():
    """Load all restaurants from iFood API"""
    global RESTAURANTS_DATA, LAST_DATA_REFRESH

    previous_restaurants = [r for r in (RESTAURANTS_DATA or []) if isinstance(r, dict)]
    existing_orders_by_merchant = {}
    for existing in previous_restaurants:
        existing_mid = (
            existing.get('merchant_id')
            or existing.get('_resolved_merchant_id')
            or existing.get('id')
        )
        existing_orders = [
            normalize_order_payload(o)
            for o in (existing.get('_orders_cache') or [])
            if isinstance(o, dict)
        ]
        if existing_mid and existing_orders:
            existing_orders_by_merchant[str(existing_mid)] = existing_orders
    new_restaurants = []
    
    if not IFOOD_API:
        print("Ã¢ÂÅ’ iFood API not initialized")
        return
    
    print(f"\nÃ°Å¸â€œÅ  Fetching restaurant data from iFood API...")
    
    # Get merchants from config
    merchants_config = IFOOD_CONFIG.get('merchants', [])
    
    if not merchants_config:
        print("Ã¢Å¡Â Ã¯Â¸Â  No merchants configured in config file")
        # Try to fetch all merchants from API
        try:
            merchants = IFOOD_API.get_merchants()
            if merchants:
                print(f"   Found {len(merchants)} merchants from API")
                merchants_config = [
                    {'merchant_id': m.get('id'), 'name': m.get('name')}
                    for m in merchants
                ]
                # Save to config for future use
                IFOOD_CONFIG['merchants'] = merchants_config
                IFoodConfig.save_config(IFOOD_CONFIG, str(CONFIG_FILE))
        except Exception as e:
            print(f"Ã¢ÂÅ’ Error fetching merchants: {e}")
            return
    
    # Get data fetch period
    days = IFOOD_CONFIG.get('data_fetch_days', 30)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    print(f"   Fetching data from {start_date} to {end_date}")
    
    # Process each merchant
    seen_merchant_ids = set()
    for merchant_config in merchants_config:
        if isinstance(merchant_config, str):
            merchant_config = {'merchant_id': merchant_config}
        if not isinstance(merchant_config, dict):
            continue
        merchant_id = normalize_merchant_id(merchant_config.get('merchant_id') or merchant_config.get('id'))
        if not merchant_id:
            continue
        if merchant_id in seen_merchant_ids:
            continue
        seen_merchant_ids.add(merchant_id)
        name = sanitize_merchant_name(merchant_config.get('name')) or f"Restaurant {str(merchant_id)[:8]}"
        manager_name = sanitize_merchant_name(merchant_config.get('manager')) or 'Gerente'
        
        print(f"   Ã°Å¸â€œâ€ž Processing: {name}")
        
        try:
            # Get merchant details
            merchant_details = IFOOD_API.get_merchant_details(merchant_id)
            if not merchant_details:
                merchant_details = {
                    'id': merchant_id,
                    'name': name,
                    'merchantManager': {'name': manager_name}
                }
            
            # Get orders
            fetched_orders = IFOOD_API.get_orders(merchant_id, start_date, end_date) or []
            fetched_orders = [
                normalize_order_payload(order)
                for order in fetched_orders
                if isinstance(order, dict)
            ]
            previous_orders = existing_orders_by_merchant.get(str(merchant_id), [])
            if previous_orders:
                if fetched_orders:
                    merged = {}
                    for order in previous_orders + fetched_orders:
                        if not isinstance(order, dict):
                            continue
                        normalized = normalize_order_payload(order)
                        order_key = str(
                            normalized.get('id')
                            or normalized.get('orderId')
                            or normalized.get('displayId')
                            or f"{normalized.get('createdAt')}:{normalized.get('orderStatus')}"
                        )
                        merged[order_key] = normalized
                    orders = list(merged.values())
                else:
                    orders = previous_orders
            else:
                orders = fetched_orders
            print(f"      Found {len(orders)} orders")
            
            # Get financial data if available
            financial_data = None
            if hasattr(IFOOD_API, 'get_financial_data'):
                try:
                    financial_data = IFOOD_API.get_financial_data(merchant_id, start_date, end_date)
                except Exception as financial_error:
                    logger.debug("iFood financial data unavailable for %s: %s", merchant_id, financial_error)
            
            # Process into dashboard format
            restaurant_data = IFoodDataProcessor.process_restaurant_data(
                merchant_details, 
                orders,
                financial_data
            )
            closure = detect_restaurant_closure(IFOOD_API, merchant_id)
            
            # Override name and manager from config if provided
            if name:
                restaurant_data['name'] = name
            if manager_name:
                restaurant_data['manager'] = manager_name
            restaurant_data['merchant_id'] = merchant_id
            
            # Store raw orders for charts
            restaurant_data['_orders_cache'] = orders
            restaurant_data['is_closed'] = bool(closure.get('is_closed'))
            restaurant_data['closure_reason'] = closure.get('closure_reason')
            restaurant_data['closed_until'] = closure.get('closed_until')
            restaurant_data['active_interruptions_count'] = int(closure.get('active_interruptions_count') or 0)
            
            new_restaurants.append(restaurant_data)
            print(f"      Ã¢Å“â€¦ {restaurant_data['name']}")
            
        except Exception as e:
            print(f"      Ã¢ÂÅ’ Failed to process {name}: {e}")
            log_exception("request_exception", e)

    if new_restaurants:
        with _GLOBAL_STATE_LOCK:
            RESTAURANTS_DATA = new_restaurants
    elif previous_restaurants:
        # Safety fallback: do not wipe dashboard on transient upstream failures.
        with _GLOBAL_STATE_LOCK:
            RESTAURANTS_DATA = previous_restaurants

    with _GLOBAL_STATE_LOCK:
        LAST_DATA_REFRESH = datetime.now()
    print(f"\nÃ¢Å“â€¦ Loaded {len(RESTAURANTS_DATA)} restaurant(s) from iFood")


# ============================================================================
# AUTHENTICATION & SESSION MANAGEMENT
# ============================================================================

def _is_api_like_request():
    return (
        request.is_json
        or request.path.startswith('/api/')
        or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        or 'application/json' in (request.headers.get('Accept', ''))
    )


def _authentication_required_response():
    if _is_api_like_request():
        return jsonify({'error': 'Authentication required', 'redirect': url_for('login_page')}), 401
    return redirect(url_for('login_page'))


def _require_authenticated_user():
    user = session.get('user')
    if user:
        return user, None
    return None, _authentication_required_response()


def _guard_with_authenticated_user(f, check_fn):
    """Run auth/session check once, then run a role-specific validator."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user, error_response = _require_authenticated_user()
        if error_response:
            return error_response
        denial_response = check_fn(user or {})
        if denial_response is not None:
            return denial_response
        return f(*args, **kwargs)
    return decorated_function


def is_platform_admin_user(user: dict) -> bool:
    """Check whether current user is a global platform admin."""
    if not isinstance(user, dict):
        return False
    user_id = user.get('id')
    if not user_id:
        return False
    return bool(db.is_platform_admin(user_id))


def _check_login_access(_user):
    return None


def _check_admin_access(user):
    if is_platform_admin_user(user):
        return None
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'error': 'Organization context required'}), 403
    org_role = db.get_org_member_role(org_id, user.get('id'))
    if org_role not in ('owner', 'admin'):
        return jsonify({'error': 'Admin access required'}), 403
    return None


def _check_platform_admin_access(user):
    if not is_platform_admin_user(user):
        return jsonify({'error': 'Platform admin access required'}), 403
    return None


def login_required(f):
    """Decorator to require login for routes"""
    return _guard_with_authenticated_user(f, _check_login_access)


def admin_required(f):
    """Decorator to require admin privileges in current org or platform."""
    return _guard_with_authenticated_user(f, _check_admin_access)


def platform_admin_required(f):
    """Decorator to require global platform-admin privileges."""
    return _guard_with_authenticated_user(f, _check_platform_admin_access)


def org_owner_required(f):
    """Decorator to require owner/admin role in current organization."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return jsonify({'error': 'Authentication required', 'redirect': url_for('login_page')}), 401

        user = session.get('user', {})
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'Organization context required'}), 403

        if is_platform_admin_user(user):
            return f(*args, **kwargs)

        org_role = db.get_org_member_role(org_id, user.get('id'))
        if org_role not in ('owner', 'admin'):
            return jsonify({'success': False, 'error': 'Organization owner/admin required'}), 403
        return f(*args, **kwargs)
    return decorated_function


def require_feature(feature_name):
    """Decorator to enforce plan-based feature access in SaaS mode."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'Organization context required'}), 403

            if db.check_feature(org_id, feature_name):
                return f(*args, **kwargs)

            details = db.get_org_details(org_id) or {}
            payload = {
                'success': False,
                'error': 'Feature not available in current plan',
                'code': 'feature_not_enabled',
                'required_feature': feature_name,
                'plan': details.get('plan', 'free'),
                'plan_display': details.get('plan_display', 'Gratuito'),
                'upgrade_required': True
            }
            is_api = (
                request.is_json or
                request.path.startswith('/api/') or
                request.headers.get('X-Requested-With') == 'XMLHttpRequest' or
                'application/json' in (request.headers.get('Accept', ''))
            )
            if is_api:
                return jsonify(payload), 403
            return redirect(url_for('dashboard'))
        return decorated_function
    return decorator


# ============================================================================
# STATIC FILE ROUTES
# ============================================================================

@app.route('/')
def index():
    """Redirect to login or dashboard based on session"""
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login_page'))


@app.route('/login')
def login_page():
    """Serve login page"""
    login_file = DASHBOARD_OUTPUT / 'login.html'
    if login_file.exists():
        return send_file(login_file)
    return "Login page not found. Please check dashboard_output directory.", 404


@app.route('/invite/<token>')
def invite_page(token):
    """Invitation landing route used in invite URLs."""
    invite_token = (token or '').strip()
    if not invite_token:
        return "Invite not found", 404

    if 'user' in session:
        result = db.accept_invite(invite_token, session['user']['id'])
        if result and result.get('success'):
            session['org_id'] = result['org_id']
            return redirect(url_for('dashboard'))
        return redirect(url_for('login_page', invite=invite_token, invite_error=1))

    return redirect(url_for('login_page', invite=invite_token))


@app.route('/dashboard')
@login_required
def dashboard():
    """Serve main dashboard page (restaurants list)"""
    # Always serve index.html for the main dashboard view
    dashboard_file = DASHBOARD_OUTPUT / 'index.html'
    
    if dashboard_file.exists():
        return send_file(dashboard_file)
    return f"Dashboard page not found: {dashboard_file}", 404



@app.route('/admin')
@admin_required
def admin_page():
    """Serve admin page"""
    admin_file = DASHBOARD_OUTPUT / 'admin.html'
    if admin_file.exists():
        return send_file(admin_file)
    return "Admin page not found", 404


@app.route('/ops')
@platform_admin_required
def ops_page():
    """Serve operations panel page."""
    ops_file = DASHBOARD_OUTPUT / 'ops.html'
    if ops_file.exists():
        return send_file(ops_file)
    return "Ops page not found", 404


@app.route('/comparativo')
@admin_required
@require_feature('comparativo')
def comparativo_page():
    """Serve comparativo por gestor page"""
    comparativo_file = DASHBOARD_OUTPUT / 'comparativo.html'
    if comparativo_file.exists():
        return send_file(comparativo_file)
    return "Comparativo page not found", 404


@app.route('/hidden-stores')
@admin_required
def hidden_stores_page():
    """Serve hidden stores management page"""
    hidden_stores_file = DASHBOARD_OUTPUT / 'hidden_stores.html'
    if hidden_stores_file.exists():
        return send_file(hidden_stores_file)
    return "Hidden stores page not found", 404


@app.route('/squads')
@admin_required
def squads_page():
    """Serve squads management page"""
    squads_file = DASHBOARD_OUTPUT / 'squads.html'
    if squads_file.exists():
        return send_file(squads_file)
    return "Squads page not found", 404


@app.route('/restaurant/<restaurant_id>')
@login_required
def restaurant_page(restaurant_id):
    """Serve individual restaurant dashboard"""
    # Find restaurant in org data (supports alias IDs).
    restaurant = find_restaurant_by_identifier(restaurant_id)
    
    if not restaurant:
        return "Restaurant not found", 404

    # Ensure canonical merchant id is resolved before rendering template JS.
    try:
        ensure_restaurant_orders_cache(
            restaurant,
            restaurant.get('merchant_id') or restaurant.get('merchantId') or restaurant_id
        )
    except Exception:
        pass
    
    # Check if we have a template
    template_file = DASHBOARD_OUTPUT / 'restaurant_template.html'
    if template_file.exists():
        with open(template_file, 'r', encoding='utf-8') as f:
            template = f.read()
        
        resolved_id = (
            restaurant.get('_resolved_merchant_id')
            or restaurant.get('merchant_id')
            or restaurant.get('merchantId')
            or restaurant.get('id')
            or restaurant_id
        )
        # Replace placeholders with actual data
        rendered = template.replace('{{restaurant_name}}', escape_html_text(restaurant.get('name', 'Restaurante')))
        rendered = rendered.replace('{{restaurant_id}}', escape_html_text(resolved_id))
        rendered = rendered.replace('{{restaurant_manager}}', escape_html_text(restaurant.get('manager', 'Gerente')))
        rendered = rendered.replace('{{restaurant_data}}', safe_json_for_script(restaurant))
        
        return Response(rendered, mimetype='text/html')
    
    return "Restaurant template not found", 404


# ============================================================================
# API ROUTES - AUTHENTICATION
# ============================================================================

@app.route('/api/register', methods=['POST'])
@rate_limit(limit=5, window_seconds=3600, scope='register')
def api_register():
    """Self-service signup: create account + organization"""
    try:
        data = get_json_payload()
        if not data:
            return jsonify({'success': False, 'error': 'Payload invalido'}), 400
        email = (data.get('email') or '').strip().lower()
        password = data.get('password', '')
        full_name = (data.get('full_name') or '').strip()
        org_name = (data.get('org_name') or '').strip()
        if not all([email, password, full_name, org_name]):
            return jsonify({'success': False, 'error': 'Todos os campos sao obrigatorios'}), 400
        if len(password) < 8:
            return jsonify({'success': False, 'error': 'Senha deve ter no minimo 8 caracteres'}), 400
        result = db.register_user_and_org(email, password, full_name, org_name)
        if not result:
            return jsonify({'success': False, 'error': 'Email ja cadastrado'}), 409
        session['user'] = {
            'id': result['user_id'],
            'username': result['username'],
            'name': full_name,
            'email': email,
            'role': 'user',
            'is_platform_admin': False,
            'primary_org_id': result['org_id']
        }
        session['org_id'] = result['org_id']
        session.permanent = True
        ensure_csrf_token()
        db.log_action('user.registered', org_id=result['org_id'], user_id=result['user_id'], details={'email': email, 'org_name': org_name}, ip_address=request.remote_addr)
        return jsonify({'success': True, 'user_id': result['user_id'], 'org_id': result['org_id'], 'redirect': url_for('dashboard')})
    except Exception as e:
        print(f"Registration error: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/onboarding', methods=['GET'])
@admin_required
def api_onboarding_state():
    """Get onboarding checklist state for current organization."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403
    return jsonify({'success': True, 'onboarding': build_onboarding_state(org_id)})


@app.route('/api/onboarding', methods=['PATCH'])
@admin_required
def api_onboarding_update():
    """Patch onboarding settings for current organization."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403

    data = get_json_payload()
    onboarding = (db.get_org_settings(org_id) or {}).get('onboarding') or {}
    completed_steps = set(onboarding.get('completed_steps') or [])

    step_id = (data.get('complete_step') or '').strip()
    if step_id:
        completed_steps.add(step_id)

    if bool(data.get('reset_completed')):
        completed_steps = set()

    if 'dismissed' in data:
        onboarding['dismissed'] = bool(data.get('dismissed'))

    onboarding['completed_steps'] = sorted(completed_steps)
    onboarding['updated_at'] = datetime.utcnow().isoformat()

    ok = db.update_org_settings(org_id, {'onboarding': onboarding})
    if not ok:
        return jsonify({'success': False, 'error': 'Unable to save onboarding state'}), 500

    return jsonify({'success': True, 'onboarding': build_onboarding_state(org_id)})


@app.route('/api/data-quality')
@login_required
def api_data_quality():
    """Get data-quality overview for currently visible stores."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403

    user = session.get('user', {})
    allowed_ids = get_user_allowed_restaurant_ids(user.get('id'), user.get('role'))
    stores = []
    for r in get_current_org_restaurants():
        if allowed_ids is not None and r.get('id') not in allowed_ids:
            continue
        stores.append(r)
    last_refresh = ORG_DATA.get(org_id, {}).get('last_refresh') or LAST_DATA_REFRESH
    payload = build_data_quality_payload(stores, reference_last_refresh=last_refresh)
    payload['last_refresh'] = last_refresh.isoformat() if isinstance(last_refresh, datetime) else None
    return jsonify({'success': True, 'quality': payload})


@app.route('/api/saved-views', methods=['GET'])
@login_required
def api_saved_views_list():
    """List saved views for the current user/org"""
    org_id = get_current_org_id()
    user = session.get('user', {})
    if not org_id or not user:
        return jsonify({'success': False}), 403

    view_type = request.args.get('view_type')
    scope_id = request.args.get('scope_id')
    if not view_type:
        return jsonify({'success': False, 'error': 'view_type is required'}), 400

    views = db.list_saved_views(org_id, user.get('id'), view_type, scope_id)
    return jsonify({'success': True, 'views': views})


@app.route('/api/saved-views', methods=['POST'])
@login_required
def api_saved_views_create():
    """Create a saved view for the current user/org"""
    org_id = get_current_org_id()
    user = session.get('user', {})
    if not org_id or not user:
        return jsonify({'success': False}), 403

    data = get_json_payload() or {}
    view_type = (data.get('view_type') or '').strip()
    name = (data.get('name') or '').strip()
    payload = data.get('payload') or {}
    scope_id = data.get('scope_id')
    is_default = bool(data.get('is_default'))

    if not view_type or not name:
        return jsonify({'success': False, 'error': 'view_type and name are required'}), 400

    new_id = db.create_saved_view(org_id, user.get('id'), view_type, name, payload, scope_id, is_default)
    if not new_id:
        return jsonify({'success': False, 'error': 'Unable to create view'}), 500

    db.log_action('saved_view.created', org_id=org_id, user_id=user.get('id'),
                  details={'view_type': view_type, 'name': name, 'scope_id': scope_id},
                  ip_address=request.remote_addr)
    return jsonify({'success': True, 'id': new_id})


@app.route('/api/saved-views/<int:view_id>', methods=['DELETE'])
@login_required
def api_saved_views_delete(view_id):
    """Delete a saved view"""
    org_id = get_current_org_id()
    user = session.get('user', {})
    if not org_id or not user:
        return jsonify({'success': False}), 403

    ok = db.delete_saved_view(org_id, user.get('id'), view_id)
    if not ok:
        return jsonify({'success': False, 'error': 'View not found'}), 404

    db.log_action('saved_view.deleted', org_id=org_id, user_id=user.get('id'),
                  details={'view_id': view_id}, ip_address=request.remote_addr)
    return jsonify({'success': True})


@app.route('/api/saved-views/<int:view_id>/default', methods=['POST'])
@login_required
def api_saved_views_set_default(view_id):
    """Set a saved view as default"""
    org_id = get_current_org_id()
    user = session.get('user', {})
    if not org_id or not user:
        return jsonify({'success': False}), 403

    ok = db.set_default_saved_view(org_id, user.get('id'), view_id)
    if not ok:
        return jsonify({'success': False, 'error': 'View not found'}), 404

    db.log_action('saved_view.set_default', org_id=org_id, user_id=user.get('id'),
                  details={'view_id': view_id}, ip_address=request.remote_addr)
    return jsonify({'success': True})


@app.route('/api/saved-views/<int:view_id>/share', methods=['POST'])
@login_required
def api_saved_view_share_create(view_id):
    """Create or rotate share link for a saved view."""
    org_id = get_current_org_id()
    user = session.get('user', {})
    if not org_id or not user:
        return jsonify({'success': False}), 403

    data = get_json_payload()
    try:
        expires_hours = int(data.get('expires_hours', 24 * 7))
    except Exception:
        expires_hours = 24 * 7
    expires_hours = max(1, min(expires_hours, 24 * 90))

    shared = db.create_saved_view_share_link(org_id, user.get('id'), view_id, expires_hours=expires_hours)
    if not shared:
        return jsonify({'success': False, 'error': 'View not found'}), 404

    share_url = f"{get_public_base_url()}/dashboard?shared_view={shared['token']}"
    return jsonify({'success': True, 'share_url': share_url, 'token': shared['token'], 'expires_at': shared['expires_at']})


@app.route('/api/saved-views/<int:view_id>/share', methods=['DELETE'])
@login_required
def api_saved_view_share_revoke(view_id):
    """Revoke share link for a saved view."""
    org_id = get_current_org_id()
    user = session.get('user', {})
    if not org_id or not user:
        return jsonify({'success': False}), 403
    ok = db.revoke_saved_view_share_link(org_id, user.get('id'), view_id)
    if not ok:
        return jsonify({'success': False, 'error': 'View not found'}), 404
    return jsonify({'success': True})


@app.route('/api/saved-views/share/<token>')
@login_required
def api_saved_view_share_resolve(token):
    """Resolve a shared saved-view token into payload."""
    shared = db.get_saved_view_by_share_token((token or '').strip())
    if not shared:
        return jsonify({'success': False, 'error': 'Shared view not found'}), 404
    return jsonify({'success': True, 'view': shared})


# ============================================================================
# API ROUTES - RESTAURANT DATA
# ============================================================================

@app.route('/api/refresh-data', methods=['POST'])
@admin_required
@rate_limit(limit=20, window_seconds=60, scope='refresh_data')
def api_refresh_data():
    """Refresh restaurant data from iFood API."""
    try:
        org_id = get_current_org_id()
        if org_id:
            current_org = get_org_data(org_id)
            if not current_org.get('api'):
                _init_org_ifood(org_id)

        has_org_api = any(od.get('api') for od in _org_data_values_snapshot())
        if not IFOOD_API and not has_org_api:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

        if USE_REDIS_QUEUE and get_redis_client():
            job_id = enqueue_refresh_job(trigger='api')
            if not job_id:
                return jsonify({'success': False, 'error': 'Failed to enqueue refresh job'}), 500
            return jsonify({
                'success': True,
                'message': 'Refresh job queued',
                'status': 'queued',
                'job_id': job_id,
                'last_refresh': LAST_DATA_REFRESH.isoformat() if LAST_DATA_REFRESH else None
            })

        if bg_refresher.is_refreshing:
            return jsonify({'success': True, 'message': 'Refresh already in progress', 'status': 'refreshing'})

        # Fallback: trigger in-process refresh.
        threading.Thread(target=bg_refresher.refresh_now, daemon=True).start()

        return jsonify({
            'success': True,
            'message': 'Refresh started in background',
            'status': 'started',
            'last_refresh': LAST_DATA_REFRESH.isoformat() if LAST_DATA_REFRESH else None
        })
        
    except Exception as e:
        print(f"Error refreshing data: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


@app.route('/api/reload', methods=['POST'])
@login_required
def api_reload():
    """Alias for refresh-data"""
    return api_refresh_data()


# ============================================================================
# REAL-TIME SERVER-SENT EVENTS (SSE)
# ============================================================================

@app.route('/api/events')
@login_required
def sse_stream():
    """SSE endpoint for real-time order tracking and data updates"""
    def event_stream(client_queue):
        # Send initial connection event
        yield f"event: connected\ndata: {json.dumps({'timestamp': datetime.now().isoformat(), 'restaurants': len(RESTAURANTS_DATA)})}\n\n"
        
        try:
            while True:
                try:
                    message = client_queue.get(timeout=30)
                    yield message
                except queue.Empty:
                    # Send keepalive
                    yield f": keepalive {datetime.now().isoformat()}\n\n"
        except GeneratorExit:
            sse_manager.unregister(client_queue)
    
    client_queue = sse_manager.register()
    response = Response(
        stream_with_context(event_stream(client_queue)),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',  # Disable nginx buffering
            'Connection': 'keep-alive'
        }
    )
    return response


@app.route('/api/refresh-status')
@login_required
def api_refresh_status():
    """Get current refresh status and system info"""
    refresh_payload = get_refresh_status()
    refresh_status = refresh_payload.get('status')
    last_refresh = get_current_org_last_refresh()
    return jsonify({
        'success': True,
        'is_refreshing': refresh_status in ('refreshing', 'queued'),
        'refresh_status': refresh_payload,
        'last_refresh': last_refresh.isoformat() if last_refresh else None,
        'restaurant_count': len(get_current_org_restaurants()),
        'connected_clients': sse_manager.client_count,
        'refresh_interval_minutes': IFOOD_CONFIG.get('refresh_interval_minutes', 30)
    })


@app.route('/api/dashboard/summary')
@login_required
def api_dashboard_summary():
    """Return aggregate KPI summary for current org and month filter."""
    month_filter = parse_month_filter(request.args.get('month', 'all'))
    if month_filter is None:
        return jsonify({'success': False, 'error': 'Invalid month filter'}), 400
    org_id = get_current_org_id()
    last_refresh = get_current_org_last_refresh()
    last_refresh_iso = last_refresh.isoformat() if last_refresh else None
    cached_payload = get_cached_dashboard_summary(org_id, month_filter, last_refresh_iso)
    if cached_payload:
        return jsonify(cached_payload)

    restaurants = []
    for r in get_current_org_restaurants():
        orders = r.get('_orders_cache', [])
        if month_filter != 0:
            orders = filter_orders_by_month(orders, month_filter)
        if not orders:
            continue
        restaurant_data = IFoodDataProcessor.process_restaurant_data(
            {'id': r.get('id'), 'name': r.get('name', 'Restaurante'), 'merchantManager': {'name': r.get('manager', 'Gerente')}},
            orders,
            None
        )
        restaurant_data['name'] = r.get('name', 'Restaurante')
        restaurant_data['manager'] = r.get('manager', 'Gerente')
        restaurants.append(restaurant_data)
    summary = aggregate_dashboard_summary(restaurants)
    summary['last_refresh'] = last_refresh_iso
    payload = {'success': True, 'summary': summary, 'month_filter': month_filter_label(month_filter)}
    set_cached_dashboard_summary(org_id, month_filter, last_refresh_iso, payload)
    return jsonify(payload)


@app.route('/api/health')
def api_health():
    """Health probe."""
    conn = db.get_connection()
    ok = bool(conn)
    if conn:
        conn.close()
    last_refresh = get_current_org_last_refresh()
    return jsonify({
        'success': ok,
        'status': 'ok' if ok else 'degraded',
        'uptime_seconds': int((datetime.utcnow() - APP_STARTED_AT).total_seconds()),
        'restaurants_loaded': len(get_current_org_restaurants()),
        'last_refresh': last_refresh.isoformat() if last_refresh else None
    }), (200 if ok else 503)


@app.route('/api/debug/session')
@platform_admin_required
def api_debug_session():
    """Debug route for session cookie visibility and server cookie flags."""
    if not os.environ.get('ENABLE_SESSION_DEBUG'):
        return jsonify({'success': False, 'error': 'disabled'}), 404

    cookie_name = app.config.get('SESSION_COOKIE_NAME', 'session')
    cookie_value = request.cookies.get(cookie_name)
    return jsonify({
        'success': True,
        'has_session_cookie': cookie_value is not None,
        'session_cookie_name': cookie_name,
        'session_cookie_length': len(cookie_value) if cookie_value else 0,
        'cookie_flags': {
            'secure': app.config.get('SESSION_COOKIE_SECURE'),
            'httponly': app.config.get('SESSION_COOKIE_HTTPONLY'),
            'samesite': app.config.get('SESSION_COOKIE_SAMESITE'),
            'domain': app.config.get('SESSION_COOKIE_DOMAIN'),
            'path': app.config.get('SESSION_COOKIE_PATH'),
        },
        'session': {
            'keys': list(session.keys()),
            'permanent': session.permanent,
            'modified': session.modified,
            'new': session.new,
        },
        'request': {
            'is_secure': request.is_secure,
            'scheme': request.scheme,
            'host': request.host,
            'remote_addr': request.remote_addr,
            'forwarded_proto': request.headers.get('X-Forwarded-Proto'),
            'forwarded_host': request.headers.get('X-Forwarded-Host'),
            'forwarded_for': request.headers.get('X-Forwarded-For'),
        }
    })


# ============================================================================
# COMPARATIVE ANALYTICS API
# ============================================================================

@app.route('/api/analytics/compare')
@login_required
@require_feature('analytics')
def api_compare_periods():
    """Compare restaurant metrics between two time periods.
    
    Query params:
        restaurant_id: specific restaurant or 'all'
        period_a_start, period_a_end: first period (ISO dates)
        period_b_start, period_b_end: second period (ISO dates)
        preset: optional shortcut - 'week', 'month', 'quarter', 'yoy'
    """
    try:
        restaurant_id = request.args.get('restaurant_id', 'all')
        preset = request.args.get('preset')
        
        now = datetime.now()
        
        if preset == 'week':
            # This week vs last week
            period_b_end = now
            period_b_start = now - timedelta(days=7)
            period_a_end = period_b_start - timedelta(days=1)
            period_a_start = period_a_end - timedelta(days=6)
        elif preset == 'month':
            # This month vs last month
            period_b_start = now.replace(day=1)
            period_b_end = now
            last_month_end = period_b_start - timedelta(days=1)
            period_a_start = last_month_end.replace(day=1)
            period_a_end = last_month_end
        elif preset == 'quarter':
            # This quarter vs last quarter
            current_q_start_month = ((now.month - 1) // 3) * 3 + 1
            period_b_start = now.replace(month=current_q_start_month, day=1)
            period_b_end = now
            period_a_end = period_b_start - timedelta(days=1)
            prev_q_start_month = ((period_a_end.month - 1) // 3) * 3 + 1
            period_a_start = period_a_end.replace(month=prev_q_start_month, day=1)
        elif preset == 'yoy':
            # Last 30 days vs same 30 days last year
            period_b_end = now
            period_b_start = now - timedelta(days=30)
            period_a_start = period_b_start.replace(year=now.year - 1)
            period_a_end = period_b_end.replace(year=now.year - 1)
        else:
            # Custom dates
            period_a_start = datetime.strptime(request.args.get('period_a_start', ''), '%Y-%m-%d')
            period_a_end = datetime.strptime(request.args.get('period_a_end', ''), '%Y-%m-%d')
            period_b_start = datetime.strptime(request.args.get('period_b_start', ''), '%Y-%m-%d')
            period_b_end = datetime.strptime(request.args.get('period_b_end', ''), '%Y-%m-%d')
        
        # Collect restaurants to compare
        if restaurant_id == 'all':
            targets = get_current_org_restaurants()
        else:
            targets = [
                r for r in get_current_org_restaurants()
                if str((r or {}).get('id') or '') == str(restaurant_id or '')
            ]
            if not targets:
                return jsonify({'success': False, 'error': 'Restaurant not found'}), 404
        
        comparisons = []
        totals_a = {'revenue': 0, 'orders': 0, 'cancelled': 0, 'new_customers': 0, 'ticket_sum': 0}
        totals_b = {'revenue': 0, 'orders': 0, 'cancelled': 0, 'new_customers': 0, 'ticket_sum': 0}
        
        for restaurant in targets:
            orders = restaurant.get('_orders_cache', [])
            
            # Split orders into period A and B
            orders_a = _filter_orders_by_date(orders, period_a_start, period_a_end)
            orders_b = _filter_orders_by_date(orders, period_b_start, period_b_end)
            
            metrics_a = _calculate_period_metrics(orders_a)
            metrics_b = _calculate_period_metrics(orders_b)
            
            # Calculate deltas
            deltas = {}
            for key in metrics_a:
                if isinstance(metrics_a[key], (int, float)) and isinstance(metrics_b[key], (int, float)):
                    old_val = metrics_a[key]
                    new_val = metrics_b[key]
                    deltas[key] = {
                        'absolute': round(new_val - old_val, 2),
                        'percent': round(((new_val - old_val) / old_val * 100) if old_val != 0 else (100 if new_val > 0 else 0), 1)
                    }
            
            comparisons.append({
                'restaurant_id': restaurant.get('id'),
                'restaurant_name': restaurant.get('name', 'Unknown'),
                'period_a': metrics_a,
                'period_b': metrics_b,
                'deltas': deltas
            })
            
            # Accumulate totals
            for key in totals_a:
                totals_a[key] += metrics_a.get(key, 0)
                totals_b[key] += metrics_b.get(key, 0)
        
        # Calculate overall deltas
        overall_deltas = {}
        for key in totals_a:
            old_val = totals_a[key]
            new_val = totals_b[key]
            overall_deltas[key] = {
                'absolute': round(new_val - old_val, 2),
                'percent': round(((new_val - old_val) / old_val * 100) if old_val != 0 else (100 if new_val > 0 else 0), 1)
            }
        
        # Calculate averages
        totals_a['ticket'] = round(totals_a['revenue'] / totals_a['orders'], 2) if totals_a['orders'] > 0 else 0
        totals_b['ticket'] = round(totals_b['revenue'] / totals_b['orders'], 2) if totals_b['orders'] > 0 else 0
        
        return jsonify({
            'success': True,
            'period_a': {'start': period_a_start.strftime('%Y-%m-%d'), 'end': period_a_end.strftime('%Y-%m-%d')},
            'period_b': {'start': period_b_start.strftime('%Y-%m-%d'), 'end': period_b_end.strftime('%Y-%m-%d')},
            'restaurants': comparisons,
            'totals': {'period_a': totals_a, 'period_b': totals_b, 'deltas': overall_deltas},
            'preset': preset
        })
        
    except ValueError as e:
        return jsonify({'success': False, 'error': f'Invalid date format: {e}'}), 400
    except Exception as e:
        print(f"Error in compare: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


@app.route('/api/analytics/daily-comparison')
@login_required
@require_feature('analytics')
def api_daily_comparison():
    """Get day-by-day data for two periods for chart overlay.
    Returns arrays aligned by day offset for easy chart rendering."""
    try:
        restaurant_id = request.args.get('restaurant_id', 'all')
        preset = request.args.get('preset', 'week')
        
        now = datetime.now()
        
        if preset == 'week':
            period_b_start = now - timedelta(days=6)
            period_b_end = now
            period_a_start = period_b_start - timedelta(days=7)
            period_a_end = period_b_start - timedelta(days=1)
        elif preset == 'month':
            period_b_start = now.replace(day=1)
            period_b_end = now
            last_month_end = period_b_start - timedelta(days=1)
            period_a_start = last_month_end.replace(day=1)
            period_a_end = last_month_end
        else:
            period_a_start = datetime.strptime(request.args.get('period_a_start', ''), '%Y-%m-%d')
            period_a_end = datetime.strptime(request.args.get('period_a_end', ''), '%Y-%m-%d')
            period_b_start = datetime.strptime(request.args.get('period_b_start', ''), '%Y-%m-%d')
            period_b_end = datetime.strptime(request.args.get('period_b_end', ''), '%Y-%m-%d')
        
        # Collect orders
        all_orders = []
        if restaurant_id == 'all':
            for r in get_current_org_restaurants():
                all_orders.extend(r.get('_orders_cache', []))
        else:
            for r in get_current_org_restaurants():
                if str((r or {}).get('id') or '') == str(restaurant_id or ''):
                    all_orders = r.get('_orders_cache', [])
                    break
        
        orders_a = _filter_orders_by_date(all_orders, period_a_start, period_a_end)
        orders_b = _filter_orders_by_date(all_orders, period_b_start, period_b_end)
        
        daily_a = _aggregate_daily(orders_a, period_a_start, period_a_end)
        daily_b = _aggregate_daily(orders_b, period_b_start, period_b_end)
        
        return jsonify({
            'success': True,
            'period_a': {'start': period_a_start.strftime('%Y-%m-%d'), 'end': period_a_end.strftime('%Y-%m-%d'), 'daily': daily_a},
            'period_b': {'start': period_b_start.strftime('%Y-%m-%d'), 'end': period_b_end.strftime('%Y-%m-%d'), 'daily': daily_b}
        })
        
    except ValueError as e:
        return jsonify({'success': False, 'error': f'Invalid date format: {e}'}), 400
    except Exception as e:
        print(f"Error in daily comparison: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


# ============================================================================
# COMPARATIVE ANALYTICS HELPERS
# ============================================================================

def _filter_orders_by_date(orders, start_dt, end_dt):
    """Filter orders list by date range"""
    filtered = []
    start_d = start_dt.date() if hasattr(start_dt, 'date') else start_dt
    end_d = end_dt.date() if hasattr(end_dt, 'date') else end_dt
    
    for order in orders:
        try:
            created = normalize_order_payload(order).get('createdAt', '')
            if created:
                order_date = datetime.fromisoformat(created.replace('Z', '+00:00')).date()
                if start_d <= order_date <= end_d:
                    filtered.append(order)
        except Exception as filter_err:
            logger.debug("Skipping order during date filter: %s", filter_err)
            continue
    return filtered


def _calculate_period_metrics(orders):
    """Calculate key metrics for a set of orders"""
    concluded = [o for o in orders if get_order_status(o) == 'CONCLUDED']
    cancelled = [o for o in orders if get_order_status(o) == 'CANCELLED']
    
    revenue = sum(float(o.get('totalPrice', 0) or 0) for o in concluded)
    order_count = len(concluded)
    ticket = round(revenue / order_count, 2) if order_count > 0 else 0
    new_customers = sum(1 for o in concluded if o.get('customer', {}).get('isNewCustomer', False))
    
    # Ratings
    ratings = [o['feedback']['rating'] for o in concluded if o.get('feedback', {}).get('rating')]
    avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else 0
    
    cancel_rate = round(len(cancelled) / len(orders) * 100, 1) if orders else 0
    
    return {
        'revenue': round(revenue, 2),
        'orders': order_count,
        'ticket': ticket,
        'cancelled': len(cancelled),
        'cancel_rate': cancel_rate,
        'new_customers': new_customers,
        'avg_rating': avg_rating,
        'total_orders': len(orders)
    }


def _aggregate_daily(orders, start_dt, end_dt):
    """Aggregate orders into daily buckets aligned from start date"""
    start_d = start_dt.date() if hasattr(start_dt, 'date') else start_dt
    end_d = end_dt.date() if hasattr(end_dt, 'date') else end_dt
    
    # Initialize all days
    days = {}
    current = start_d
    while current <= end_d:
        days[current.isoformat()] = {'date': current.isoformat(), 'revenue': 0, 'orders': 0, 'cancelled': 0}
        current += timedelta(days=1)
    
    for order in orders:
        try:
            created = normalize_order_payload(order).get('createdAt', '')
            if created:
                d = datetime.fromisoformat(created.replace('Z', '+00:00')).date().isoformat()
                if d in days:
                    status = get_order_status(order)
                    if status == 'CONCLUDED':
                        days[d]['revenue'] += float(order.get('totalPrice', 0) or 0)
                        days[d]['orders'] += 1
                    elif status == 'CANCELLED':
                        days[d]['cancelled'] += 1
        except Exception as breakdown_err:
            logger.debug("Skipping order during daily breakdown: %s", breakdown_err)
            continue
    
    # Round revenue
    result = sorted(days.values(), key=lambda x: x['date'])
    for d in result:
        d['revenue'] = round(d['revenue'], 2)
    return result


# ============================================================================
# API ROUTES - IFOOD CONFIGURATION (ADMIN)
# ============================================================================

@app.route('/api/ifood/config')
@platform_admin_required
def api_ifood_config():
    """Get iFood configuration (without secrets)"""
    try:
        config = {
            'configured': bool(IFOOD_API),
            'merchant_count': len(IFOOD_CONFIG.get('merchants', [])),
            'merchants': [
                (
                    {
                        'merchant_id': normalize_merchant_id(m.get('merchant_id') or m.get('id')),
                        'name': sanitize_merchant_name(m.get('name')),
                        'manager': sanitize_merchant_name(m.get('manager'))
                    }
                    if isinstance(m, dict)
                    else {
                        'merchant_id': normalize_merchant_id(m),
                        'name': '',
                        'manager': ''
                    }
                )
                for m in (IFOOD_CONFIG.get('merchants', []) or [])
            ],
            'data_fetch_days': IFOOD_CONFIG.get('data_fetch_days', 30),
            'refresh_interval_minutes': IFOOD_CONFIG.get('refresh_interval_minutes', 60),
            'last_refresh': LAST_DATA_REFRESH.isoformat() if LAST_DATA_REFRESH else None
        }
        
        return jsonify({'success': True, 'config': config})
        
    except Exception as e:
        print(f"Error getting config: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/ifood/merchants', methods=['POST'])
@admin_required
def api_add_merchant():
    """Add a merchant to org config (or legacy global config for platform admins)."""
    try:
        data = get_json_payload()
        
        merchant_id = normalize_merchant_id(data.get('merchant_id'))
        name = sanitize_merchant_name(data.get('name'))
        manager = sanitize_merchant_name(data.get('manager')) or 'Gerente'
        
        if not merchant_id:
            return jsonify({'success': False, 'error': 'Merchant ID required'}), 400

        merchant_payload = {
            'merchant_id': merchant_id,
            'name': name or f'Restaurant {merchant_id[:8]}',
            'manager': manager
        }

        if is_platform_admin_user(session.get('user', {})):
            # Legacy global config path for platform operators.
            if 'merchants' not in IFOOD_CONFIG:
                IFOOD_CONFIG['merchants'] = []
            for m in IFOOD_CONFIG['merchants']:
                existing_id = normalize_merchant_id(
                    m.get('merchant_id') or m.get('id')
                    if isinstance(m, dict)
                    else m
                )
                if existing_id == merchant_id:
                    return jsonify({'success': False, 'error': 'Merchant already exists'}), 400
            IFOOD_CONFIG['merchants'].append(merchant_payload)
            IFoodConfig.save_config(IFOOD_CONFIG, str(CONFIG_FILE))
            load_restaurants_from_ifood()
            return jsonify({
                'success': True,
                'message': 'Merchant added successfully',
                'restaurant_count': len(RESTAURANTS_DATA)
            })

        # Tenant-safe org path.
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'Organization context required'}), 403
        org_cfg = db.get_org_ifood_config(org_id) or {}
        merchants = org_cfg.get('merchants') or []
        if isinstance(merchants, str):
            try:
                merchants = json.loads(merchants)
            except Exception:
                merchants = []
        for m in merchants:
            existing_id = normalize_merchant_id(
                m.get('merchant_id') or m.get('id')
                if isinstance(m, dict)
                else m
            )
            if existing_id == merchant_id:
                return jsonify({'success': False, 'error': 'Merchant already exists'}), 400
        merchants.append(merchant_payload)
        db.update_org_ifood_config(org_id, merchants=merchants)
        api = _init_org_ifood(org_id)
        if api:
            _load_org_restaurants(org_id)
        return jsonify({
            'success': True,
            'message': 'Merchant added successfully',
            'restaurant_count': len(get_current_org_restaurants())
        })
        
    except Exception as e:
        print(f"Error adding merchant: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/ifood/merchants/<merchant_id>', methods=['DELETE'])
@admin_required
def api_remove_merchant(merchant_id):
    """Remove a merchant from org config (or legacy global config for platform admins)."""
    try:
        target_merchant_id = normalize_merchant_id(merchant_id)
        if not target_merchant_id:
            return jsonify({'success': False, 'error': 'Merchant not found'}), 404

        if is_platform_admin_user(session.get('user', {})):
            if 'merchants' not in IFOOD_CONFIG:
                return jsonify({'success': False, 'error': 'No merchants configured'}), 404

            original_count = len(IFOOD_CONFIG['merchants'])
            IFOOD_CONFIG['merchants'] = [
                m for m in IFOOD_CONFIG['merchants'] 
                if normalize_merchant_id(
                    m.get('merchant_id') or m.get('id')
                    if isinstance(m, dict)
                    else m
                ) != target_merchant_id
            ]
            if len(IFOOD_CONFIG['merchants']) == original_count:
                return jsonify({'success': False, 'error': 'Merchant not found'}), 404

            IFoodConfig.save_config(IFOOD_CONFIG, str(CONFIG_FILE))
            load_restaurants_from_ifood()
            return jsonify({
                'success': True,
                'message': 'Merchant removed successfully',
                'restaurant_count': len(RESTAURANTS_DATA)
            })

        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'Organization context required'}), 403
        org_cfg = db.get_org_ifood_config(org_id) or {}
        merchants = org_cfg.get('merchants') or []
        if isinstance(merchants, str):
            try:
                merchants = json.loads(merchants)
            except Exception:
                merchants = []
        original_count = len(merchants)
        merchants = [
            m for m in merchants
            if normalize_merchant_id(
                m.get('merchant_id') or m.get('id')
                if isinstance(m, dict)
                else m
            ) != target_merchant_id
        ]
        if len(merchants) == original_count:
            return jsonify({'success': False, 'error': 'Merchant not found'}), 404
        db.update_org_ifood_config(org_id, merchants=merchants)
        api = _init_org_ifood(org_id)
        if api:
            _load_org_restaurants(org_id)
        return jsonify({
            'success': True,
            'message': 'Merchant removed successfully',
            'restaurant_count': len(get_current_org_restaurants())
        })
        
    except Exception as e:
        print(f"Error removing merchant: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/ifood/test')
@platform_admin_required
def api_test_ifood():
    """Test iFood API connection"""
    try:
        if not IFOOD_API:
            return jsonify({
                'success': False,
                'error': 'iFood API not configured',
                'configured': False
            })
        
        # Try to authenticate
        if IFOOD_API.authenticate():
            # Try to fetch merchants
            merchants = IFOOD_API.get_merchants()
            
            return jsonify({
                'success': True,
                'message': 'iFood API connection successful',
                'configured': True,
                'merchant_count': len(merchants) if merchants else 0
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Authentication failed',
                'configured': True
            })
            
    except Exception as e:
        print(f"Error testing iFood API: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'configured': bool(IFOOD_API)
        })


# ============================================================================
# API ROUTES - COMPARATIVO (ADMIN)
# ============================================================================

# In-memory storage for cancelled restaurants (in production, use database)
CANCELLED_RESTAURANTS = []

@app.route('/api/comparativo/stats')
@admin_required
@require_feature('comparativo')
def api_comparativo_stats():
    """Get consolidated stats for comparativo page"""
    try:
        total_stores = len(RESTAURANTS_DATA)
        stores_with_history = sum(1 for r in get_current_org_restaurants() if (r.get('metrics', {}).get('vendas') or r.get('metrics', {}).get('total_pedidos') or 0) > 0)
        
        total_revenue = 0
        positive_count = 0
        negative_count = 0
        previous_revenue = 0
        
        for r in get_current_org_restaurants():
            metrics = r.get('metrics', {})
            valor_bruto = metrics.get('valor_bruto') or 0
            total_revenue += valor_bruto
            
            trend = (metrics.get('trends') or {}).get('vendas') or 0
            if trend > 0:
                positive_count += 1
            elif trend < 0:
                negative_count += 1
            
            # Estimate previous revenue from trend
            if valor_bruto and trend != 0:
                previous_revenue += valor_bruto / (1 + trend / 100)
            else:
                previous_revenue += valor_bruto
        
        revenue_trend = ((total_revenue - previous_revenue) / previous_revenue * 100) if previous_revenue > 0 else 0
        
        return jsonify({
            'success': True,
            'stats': {
                'total_stores': total_stores,
                'stores_with_history': stores_with_history,
                'total_revenue': total_revenue,
                'revenue_trend': revenue_trend,
                'positive_count': positive_count,
                'negative_count': negative_count,
                'cancelled_count': len(CANCELLED_RESTAURANTS)
            }
        })
        
    except Exception as e:
        print(f"Error getting comparativo stats: {e}")
        return internal_error_response()


@app.route('/api/comparativo/managers')
@admin_required
@require_feature('comparativo')
def api_comparativo_managers():
    """Get data grouped by manager"""
    try:
        manager_map = {}
        
        for restaurant in RESTAURANTS_DATA:
            manager = restaurant.get('manager') or 'Sem Gestor'
            
            if manager not in manager_map:
                manager_map[manager] = {
                    'name': manager,
                    'restaurants': [],
                    'total_revenue': 0,
                    'total_orders': 0,
                    'positive_count': 0,
                    'negative_count': 0,
                    'services': set()
                }
            
            manager_data = manager_map[manager]
            manager_data['restaurants'].append({
                'id': restaurant.get('id'),
                'name': restaurant.get('name'),
                'metrics': restaurant.get('metrics', {})
            })
            
            metrics = restaurant.get('metrics', {})
            manager_data['total_revenue'] += metrics.get('valor_bruto') or 0
            manager_data['total_orders'] += metrics.get('total_pedidos') or 0
            
            trend = (metrics.get('trends') or {}).get('vendas') or 0
            if trend > 0:
                manager_data['positive_count'] += 1
            elif trend < 0:
                manager_data['negative_count'] += 1
            
            # Add services based on platforms
            platforms = restaurant.get('platforms') or []
            for p in platforms:
                pl = p.lower()
                if 'ifood' in pl:
                    manager_data['services'].add('ifood')
                elif '99' in pl:
                    manager_data['services'].add('99food')
                elif 'keeta' in pl:
                    manager_data['services'].add('keeta')
        
        # Convert sets to lists for JSON serialization
        managers = []
        for m in manager_map.values():
            m['services'] = list(m['services'])
            m['restaurant_count'] = len(m['restaurants'])
            managers.append(m)
        
        # Sort by revenue
        managers.sort(key=lambda x: x['total_revenue'], reverse=True)
        
        return jsonify({
            'success': True,
            'managers': managers
        })
        
    except Exception as e:
        print(f"Error getting managers data: {e}")
        return internal_error_response()


@app.route('/api/comparativo/cancelled')
@admin_required
@require_feature('comparativo')
def api_comparativo_cancelled():
    """Get cancelled restaurants"""
    return jsonify({
        'success': True,
        'cancelled': CANCELLED_RESTAURANTS
    })


@app.route('/api/comparativo/cancelled', methods=['POST'])
@admin_required
@require_feature('comparativo')
def api_cancel_restaurant():
    """Mark a restaurant as cancelled"""
    global RESTAURANTS_DATA
    try:
        data = get_json_payload()
        restaurant_id = data.get('restaurant_id')
        reason = data.get('reason', '')
        
        if not restaurant_id:
            return jsonify({'success': False, 'error': 'Restaurant ID required'}), 400
        
        # Find restaurant
        restaurant = None
        for r in get_current_org_restaurants():
            if str((r or {}).get('id') or '') == str(restaurant_id or ''):
                restaurant = r
                break
        
        if not restaurant:
            return jsonify({'success': False, 'error': 'Restaurant not found'}), 404
        
        # Add to cancelled list
        cancelled_entry = {
            'id': restaurant_id,
            'name': restaurant.get('name'),
            'manager': restaurant.get('manager'),
            'reason': reason,
            'cancelled_at': datetime.now().isoformat()
        }
        
        # Check if already cancelled
        for c in CANCELLED_RESTAURANTS:
            if c['id'] == restaurant_id:
                return jsonify({'success': False, 'error': 'Restaurant already cancelled'}), 400
        
        CANCELLED_RESTAURANTS.append(cancelled_entry)
        
        # Remove from active restaurants
        with _GLOBAL_STATE_LOCK:
            RESTAURANTS_DATA = [
                r for r in get_current_org_restaurants()
                if str((r or {}).get('id') or '') != str(restaurant_id or '')
            ]
        
        return jsonify({
            'success': True,
            'message': f'Restaurant {restaurant.get("name")} cancelled',
            'cancelled': cancelled_entry
        })
        
    except Exception as e:
        print(f"Error cancelling restaurant: {e}")
        return internal_error_response()


@app.route('/api/comparativo/cancelled/<restaurant_id>', methods=['DELETE'])
@admin_required
@require_feature('comparativo')
def api_restore_restaurant(restaurant_id):
    """Restore a cancelled restaurant"""
    try:
        global CANCELLED_RESTAURANTS
        
        # Find in cancelled list
        cancelled = None
        for c in CANCELLED_RESTAURANTS:
            if c['id'] == restaurant_id:
                cancelled = c
                break
        
        if not cancelled:
            return jsonify({'success': False, 'error': 'Cancelled restaurant not found'}), 404
        
        # Remove from cancelled list
        CANCELLED_RESTAURANTS = [c for c in CANCELLED_RESTAURANTS if c['id'] != restaurant_id]
        
        # Reload data to get the restaurant back
        load_restaurants_from_ifood()
        
        return jsonify({
            'success': True,
            'message': 'Restaurant restored successfully'
        })
        
    except Exception as e:
        print(f"Error restoring restaurant: {e}")
        return internal_error_response()


# ============================================================================
# API ROUTES - USER MANAGEMENT (ADMIN)
# ============================================================================

@app.route('/api/users')
@admin_required
def api_users():
    """Get users visible to current admin context."""
    try:
        current_user = session.get('user', {})
        if is_platform_admin_user(current_user):
            users = db.get_all_users()
            return jsonify({
                'success': True,
                'users': users
            })

        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'Organization context required'}), 403

        users = db.get_org_users(org_id)
        return jsonify({
            'success': True,
            'users': users
        })
    except Exception as e:
        print(f"Error getting users: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/users', methods=['POST'])
@admin_required
@rate_limit(limit=30, window_seconds=3600, scope='create_user')
def api_create_user():
    """Create user; org admins create tenant users, platform admins may create global admins."""
    try:
        data = get_json_payload()
        
        current_user = session.get('user', {})
        platform_admin = is_platform_admin_user(current_user)
        username = data.get('username')
        password = data.get('password')
        full_name = data.get('full_name')
        email = data.get('email')
        role = (data.get('role') or 'user').strip().lower()
        if role not in ('user', 'admin', 'site_admin'):
            return jsonify({'success': False, 'error': 'Invalid global role'}), 400
        org_role = (data.get('org_role') or ('admin' if role in ('admin', 'site_admin') else 'viewer')).strip().lower()
        org_id = get_current_org_id()
        
        if not all([username, password, full_name]):
            return jsonify({
                'success': False,
                'error': 'Username, password, and full name required'
            }), 400
        if len(str(password)) < 8:
            return jsonify({
                'success': False,
                'error': 'Password must have at least 8 characters'
            }), 400

        if org_role not in ('owner', 'admin', 'viewer'):
            return jsonify({'success': False, 'error': 'Invalid org role'}), 400

        if not platform_admin:
            if not org_id:
                return jsonify({'success': False, 'error': 'Organization context required'}), 403
            # Tenant admins cannot create global platform admins.
            role = 'user'
            if org_role == 'owner':
                return jsonify({'success': False, 'error': 'Only platform admins can assign owner role at creation'}), 403

        if org_id:
            user_limit = db.check_user_limit(org_id)
            if not user_limit.get('allowed'):
                return jsonify({
                    'success': False,
                    'error': 'User limit reached for current organization',
                    'code': 'user_limit_exceeded',
                    'current_users': user_limit.get('current'),
                    'max_users': user_limit.get('max')
                }), 409
        
        user_id = db.create_user(username, password, full_name, email, role)
        
        if user_id:
            assigned_to_org = False
            assigned_role = None
            if org_id:
                assign_result = db.assign_user_to_org(org_id, user_id, org_role)
                if not assign_result.get('success'):
                    # Best-effort cleanup to avoid orphan account if org assignment fails.
                    cleanup_conn = db.get_connection()
                    if cleanup_conn:
                        try:
                            cleanup_cursor = cleanup_conn.cursor()
                            cleanup_cursor.execute("DELETE FROM dashboard_users WHERE id=%s", (user_id,))
                            cleanup_conn.commit()
                            cleanup_cursor.close()
                        except Exception:
                            cleanup_conn.rollback()
                        finally:
                            cleanup_conn.close()
                    return jsonify({
                        'success': False,
                        'error': assign_result.get('error', 'Failed to assign user to organization')
                    }), 400
                assigned_to_org = True
                assigned_role = assign_result.get('org_role')

            return jsonify({
                'success': True,
                'message': 'User created successfully',
                'user_id': user_id,
                'assigned_to_org': assigned_to_org,
                'org_role': assigned_role
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Username already exists'
            }), 400
            
    except Exception as e:
        print(f"Error creating user: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/users/<int:user_id>/role', methods=['PATCH'])
@admin_required
def api_update_user_global_role(user_id):
    """Update a user's global role (platform/site admin only)."""
    try:
        current_user = session.get('user', {})
        if not is_platform_admin_user(current_user):
            return jsonify({'success': False, 'error': 'Platform admin access required'}), 403

        data = get_json_payload()
        role = (data.get('role') or '').strip().lower()
        if not role:
            return jsonify({'success': False, 'error': 'role is required'}), 400

        result = db.update_user_global_role(
            user_id,
            role,
            acting_user_id=current_user.get('id')
        )
        if not result.get('success'):
            code = str(result.get('error') or '')
            if code == 'user_not_found':
                return jsonify({'success': False, 'error': 'User not found'}), 404
            if code == 'invalid_role':
                return jsonify({'success': False, 'error': 'Invalid global role'}), 400
            if code == 'cannot_demote_last_site_admin':
                return jsonify({'success': False, 'error': 'Cannot demote the last site admin'}), 409
            if code == 'cannot_update_own_role':
                return jsonify({'success': False, 'error': 'You cannot change your own global role'}), 400
            return jsonify({'success': False, 'error': code or 'Failed to update global role'}), 400

        db.log_action(
            'user.global_role_updated',
            org_id=get_current_org_id(),
            user_id=current_user.get('id'),
            details={'target_user_id': user_id, 'role': result.get('role')},
            ip_address=request.remote_addr
        )

        return jsonify({
            'success': True,
            'user_id': user_id,
            'role': result.get('role'),
            'changed': bool(result.get('changed', True))
        })
    except Exception as e:
        print(f"Error updating user global role: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@admin_required
def api_delete_user(user_id):
    """Delete a user account (platform admin only)."""
    try:
        if not is_platform_admin_user(session.get('user', {})):
            return jsonify({'success': False, 'error': 'Platform admin access required'}), 403

        # Prevent self-deletion
        current_user_id = (session.get('user') or {}).get('id')
        if str(current_user_id or '').strip() == str(user_id):
            return jsonify({
                'success': False,
                'error': 'Cannot delete your own account'
            }), 400
        
        conn = db.get_connection()
        if not conn:
            return jsonify({
                'success': False,
                'error': 'Database connection failed'
            }), 500
        
        cursor = conn.cursor()
        
        # Check if user exists
        cursor.execute("SELECT username FROM dashboard_users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        
        if not user:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False,
                'error': 'User not found'
            }), 404
        
        # Delete user
        cursor.execute("DELETE FROM dashboard_users WHERE id = %s", (user_id,))
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'User {user[0]} deleted successfully'
        })
        
    except Exception as e:
        print(f"Error deleting user: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


# ============================================================================
# HIDDEN STORES API ENDPOINTS
# ============================================================================

def _table_has_column(cursor, table_name, column_name):
    """Return True when a table has the requested column."""
    columns = _get_table_columns(cursor, table_name)
    return str(column_name or '').strip().lower() in columns


def _get_table_columns(cursor, table_name):
    """Return cached column names for a table (lower-case)."""
    key = str(table_name or '').strip().lower()
    if not key:
        return set()

    with _TABLE_COLUMNS_CACHE_LOCK:
        cached = _TABLE_COLUMNS_CACHE.get(key)
    if cached is not None:
        return cached

    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
    """, (key,))
    columns = {str(row[0]).strip().lower() for row in (cursor.fetchall() or []) if row and row[0]}

    with _TABLE_COLUMNS_CACHE_LOCK:
        _TABLE_COLUMNS_CACHE[key] = columns
    return columns


def _clear_table_columns_cache(table_name=None):
    """Clear schema cache for one table (or all tables)."""
    with _TABLE_COLUMNS_CACHE_LOCK:
        if table_name is None:
            _TABLE_COLUMNS_CACHE.clear()
            return
        _TABLE_COLUMNS_CACHE.pop(str(table_name or '').strip().lower(), None)


def _prime_table_columns_cache(table_names):
    """Prime table-column cache at startup to avoid metadata lookups on hot routes."""
    names = [str(name or '').strip().lower() for name in (table_names or []) if str(name or '').strip()]
    if not names:
        return
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        if not conn:
            return
        cursor = conn.cursor()
        for table_name in names:
            _get_table_columns(cursor, table_name)
    except Exception:
        pass
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def _table_has_org_id(cursor, table_name):
    return _table_has_column(cursor, table_name, 'org_id')


@app.route('/api/hidden-stores', methods=['GET'])
@login_required
def get_hidden_stores():
    """Get list of all hidden stores"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        conn = db.get_connection()
        cursor = conn.cursor()

        if _table_has_org_id(cursor, 'hidden_stores'):
            cursor.execute("""
                SELECT store_id, store_name, hidden_at, hidden_by
                FROM hidden_stores
                WHERE org_id = %s
                ORDER BY hidden_at DESC
            """, (org_id,))
        else:
            cursor.execute("""
                SELECT store_id, store_name, hidden_at, hidden_by
                FROM hidden_stores
                ORDER BY hidden_at DESC
            """)
        hidden = cursor.fetchall()
        cursor.close()
        conn.close()
        
        hidden_list = [{
            'id': h[0],
            'name': h[1],
            'hidden_at': h[2].isoformat() if h[2] else None,
            'hidden_by': h[3]
        } for h in hidden]
        
        return jsonify({
            'success': True,
            'hidden_stores': hidden_list
        })
    except Exception as e:
        print(f"Error getting hidden stores: {e}")
        return internal_error_response()


@app.route('/api/stores/<store_id>/hide', methods=['POST'])
@admin_required
def hide_store(store_id):
    """Hide a store from the main dashboard"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload() or {}
        store_name = data.get('name', 'Unknown Store')
        hidden_by = session.get('user', {}).get('username', 'Unknown')
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        has_org_id = _table_has_org_id(cursor, 'hidden_stores')

        # Check if already hidden
        if has_org_id:
            cursor.execute(
                "SELECT store_id FROM hidden_stores WHERE store_id = %s AND org_id = %s",
                (store_id, org_id)
            )
        else:
            cursor.execute("SELECT store_id FROM hidden_stores WHERE store_id = %s", (store_id,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Store already hidden'}), 400
        
        # Insert into hidden stores
        if has_org_id:
            cursor.execute("""
                INSERT INTO hidden_stores (store_id, store_name, hidden_by, org_id)
                VALUES (%s, %s, %s, %s)
            """, (store_id, store_name, hidden_by, org_id))
        else:
            cursor.execute("""
                INSERT INTO hidden_stores (store_id, store_name, hidden_by)
                VALUES (%s, %s, %s)
            """, (store_id, store_name, hidden_by))
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Store "{store_name}" hidden successfully'
        })
    except Exception as e:
        print(f"Error hiding store: {e}")
        return internal_error_response()


@app.route('/api/stores/<store_id>/unhide', methods=['POST'])
@admin_required
def unhide_store(store_id):
    """Unhide a store and show it on the main dashboard"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        conn = db.get_connection()
        cursor = conn.cursor()
        has_org_id = _table_has_org_id(cursor, 'hidden_stores')
        
        # Get store name before deleting
        if has_org_id:
            cursor.execute(
                "SELECT store_name FROM hidden_stores WHERE store_id = %s AND org_id = %s",
                (store_id, org_id)
            )
        else:
            cursor.execute("SELECT store_name FROM hidden_stores WHERE store_id = %s", (store_id,))
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Store not found in hidden list'}), 404
        
        store_name = result[0]
        
        # Remove from hidden stores
        if has_org_id:
            cursor.execute(
                "DELETE FROM hidden_stores WHERE store_id = %s AND org_id = %s",
                (store_id, org_id)
            )
        else:
            cursor.execute("DELETE FROM hidden_stores WHERE store_id = %s", (store_id,))
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Store "{store_name}" is now visible'
        })
    except Exception as e:
        print(f"Error unhiding store: {e}")
        return internal_error_response()


# ============================================================================
# SQUADS API ENDPOINTS
# ============================================================================

def get_user_allowed_restaurant_ids(user_id, user_role):
    """Helper function to get allowed restaurant IDs for a user based on squad membership"""
    if user_role in ('admin', 'site_admin'):
        return None  # None means all restaurants allowed

    # Owners/admins in the active org should always see all restaurants.
    try:
        org_id = get_current_org_id()
        if org_id and user_id:
            org_role = db.get_org_member_role(org_id, user_id)
            if org_role in ('owner', 'admin'):
                return None
    except Exception:
        pass

    try:
        org_id = get_current_org_id()
        conn = db.get_connection()
        cursor = conn.cursor()

        if _table_has_org_id(cursor, 'squads') and org_id:
            cursor.execute("""
                SELECT DISTINCT sr.restaurant_id
                FROM squad_restaurants sr
                JOIN squad_members sm ON sr.squad_id = sm.squad_id
                JOIN squads s ON s.id = sr.squad_id
                WHERE sm.user_id = %s
                  AND s.org_id = %s
            """, (user_id, org_id))
        else:
            cursor.execute("""
                SELECT DISTINCT sr.restaurant_id
                FROM squad_restaurants sr
                JOIN squad_members sm ON sr.squad_id = sm.squad_id
                WHERE sm.user_id = %s
            """, (user_id,))

        restaurant_ids = [row[0] for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        # Return None if user has no squad assignments (sees all by default)
        return restaurant_ids if restaurant_ids else None

    except Exception as e:
        print(f"Error getting user allowed restaurants: {e}")
        return None  # Default to all on error


def _squad_belongs_to_org(cursor, squad_id, org_id):
    """Return True when squad is visible under current org context."""
    if _table_has_org_id(cursor, 'squads') and org_id:
        cursor.execute("SELECT id FROM squads WHERE id = %s AND org_id = %s", (squad_id, org_id))
    else:
        cursor.execute("SELECT id FROM squads WHERE id = %s", (squad_id,))
    return cursor.fetchone() is not None


def _get_squads_schema_flags(cursor):
    """Return cached squads schema capabilities."""
    columns = _get_table_columns(cursor, 'squads')
    has_old_schema = ('squad_id' in columns) and ('leader' in columns)
    has_org_id = 'org_id' in columns
    return has_old_schema, has_org_id


@app.route('/api/squads', methods=['GET'])
@login_required
def api_get_squads():
    """Get all squads with their members and restaurants"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        conn = db.get_connection()
        cursor = conn.cursor()

        # Check which schema we have (cached; avoids repeated information_schema hits)
        has_old_schema, has_org_id = _get_squads_schema_flags(cursor)

        if has_old_schema:
            # Old schema: id, squad_id, name, leader, members, restaurants, active, created_at
            if has_org_id:
                cursor.execute("""
                    SELECT id, squad_id, name, leader, members, restaurants, active, created_at
                    FROM squads
                    WHERE active = true AND org_id = %s
                    ORDER BY name
                """, (org_id,))
            else:
                cursor.execute("""
                    SELECT id, squad_id, name, leader, members, restaurants, active, created_at
                    FROM squads
                    WHERE active = true
                    ORDER BY name
                """)
        else:
            # New schema: id, name, description, created_at, created_by
            if has_org_id:
                cursor.execute("""
                    SELECT id, NULL as squad_id, name, created_by as leader,
                           NULL as members, NULL as restaurants, true as active, created_at
                    FROM squads
                    WHERE org_id = %s
                    ORDER BY name
                """, (org_id,))
            else:
                cursor.execute("""
                    SELECT id, NULL as squad_id, name, created_by as leader,
                           NULL as members, NULL as restaurants, true as active, created_at
                    FROM squads
                    ORDER BY name
                """)

        squads_raw = cursor.fetchall()

        squads = []
        for squad in squads_raw:
            squad_id = squad[0]

            # Parse members and restaurants from JSON text fields (old schema)
            try:
                members_list = json.loads(squad[4]) if squad[4] else []
            except Exception:
                members_list = []

            try:
                restaurants_list = json.loads(squad[5]) if squad[5] else []
            except Exception:
                restaurants_list = []

            # Get members for this squad from squad_members table
            cursor.execute("""
                SELECT u.id, u.full_name, u.username, u.role
                FROM squad_members sm
                JOIN dashboard_users u ON sm.user_id = u.id
                WHERE sm.squad_id = %s
                ORDER BY u.full_name
            """, (squad_id,))
            members_from_table = cursor.fetchall()

            # Get restaurants for this squad from squad_restaurants table
            cursor.execute("""
                SELECT restaurant_id, restaurant_name
                FROM squad_restaurants
                WHERE squad_id = %s
                ORDER BY restaurant_name
            """, (squad_id,))
            restaurants_from_table = cursor.fetchall()

            squads.append({
                'id': squad_id,
                'squad_id': squad[1] or str(squad_id),
                'name': squad[2],
                'leader': squad[3] or '',
                'description': '',
                'created_at': squad[7].isoformat() if squad[7] else None,
                'created_by': squad[3] or '',
                'active': squad[6] if squad[6] is not None else True,
                'members': [
                    {'id': m[0], 'name': m[1] or m[2], 'username': m[2], 'role': m[3]}
                    for m in members_from_table
                ] if members_from_table else members_list,
                'restaurants': [
                    {'id': r[0], 'name': r[1]}
                    for r in restaurants_from_table
                ] if restaurants_from_table else restaurants_list
            })

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'squads': squads
        })

    except Exception as e:
        print(f"Error getting squads: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


@app.route('/api/squads', methods=['POST'])
@admin_required
def api_create_squad():
    """Create a new squad"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()

        if not name:
            return jsonify({'success': False, 'error': 'Nome obrigatorio'}), 400

        created_by = session.get('user', {}).get('username', 'Unknown')

        conn = db.get_connection()
        cursor = conn.cursor()
        has_org_id = _table_has_org_id(cursor, 'squads')

        # Check if squad with same name exists
        if has_org_id:
            cursor.execute("SELECT id FROM squads WHERE name = %s AND org_id = %s", (name, org_id))
        else:
            cursor.execute("SELECT id FROM squads WHERE name = %s", (name,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Ja existe um squad com este nome'}), 400

        # Create squad - check which schema we have (cached metadata)
        has_old_schema, _ = _get_squads_schema_flags(cursor)

        if has_old_schema:
            squad_uid = str(uuid.uuid4())[:8]
            if has_org_id:
                cursor.execute("""
                    INSERT INTO squads (squad_id, name, leader, members, restaurants, org_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (squad_uid, name, created_by, '[]', '[]', org_id))
            else:
                cursor.execute("""
                    INSERT INTO squads (squad_id, name, leader, members, restaurants)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (squad_uid, name, created_by, '[]', '[]'))
        else:
            if has_org_id:
                cursor.execute("""
                    INSERT INTO squads (name, description, created_by, org_id)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (name, description, created_by, org_id))
            else:
                cursor.execute("""
                    INSERT INTO squads (name, description, created_by)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (name, description, created_by))
        squad_id = cursor.fetchone()[0]
        conn.commit()

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Squad criado com sucesso',
            'squad_id': squad_id
        })

    except Exception as e:
        print(f"Error creating squad: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


@app.route('/api/squads/<int:squad_id>', methods=['PUT'])
@admin_required
def api_update_squad(squad_id):
    """Update a squad"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        name = data.get('name', '').strip()
        _description = data.get('description', '').strip()

        if not name:
            return jsonify({'success': False, 'error': 'Nome obrigatorio'}), 400

        conn = db.get_connection()
        cursor = conn.cursor()
        has_org_id = _table_has_org_id(cursor, 'squads')

        # Check if squad exists in current org
        if has_org_id:
            cursor.execute("SELECT id FROM squads WHERE id = %s AND org_id = %s", (squad_id, org_id))
        else:
            cursor.execute("SELECT id FROM squads WHERE id = %s", (squad_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

        # Check for duplicate name (excluding current squad)
        if has_org_id:
            cursor.execute(
                "SELECT id FROM squads WHERE name = %s AND id != %s AND org_id = %s",
                (name, squad_id, org_id)
            )
        else:
            cursor.execute("SELECT id FROM squads WHERE name = %s AND id != %s", (name, squad_id))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Ja existe outro squad com este nome'}), 400

        # Update squad - only update name since description may not exist in old schema
        if has_org_id:
            cursor.execute("UPDATE squads SET name = %s WHERE id = %s AND org_id = %s", (name, squad_id, org_id))
        else:
            cursor.execute("UPDATE squads SET name = %s WHERE id = %s", (name, squad_id))
        conn.commit()

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Squad atualizado com sucesso'
        })

    except Exception as e:
        print(f"Error updating squad: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


@app.route('/api/squads/<int:squad_id>', methods=['DELETE'])
@admin_required
def api_delete_squad(squad_id):
    """Delete a squad"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        conn = db.get_connection()
        cursor = conn.cursor()
        has_org_id = _table_has_org_id(cursor, 'squads')

        # Check if squad exists in current org
        if has_org_id:
            cursor.execute("SELECT name FROM squads WHERE id = %s AND org_id = %s", (squad_id, org_id))
        else:
            cursor.execute("SELECT name FROM squads WHERE id = %s", (squad_id,))
        result = cursor.fetchone()
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

        squad_name = result[0]

        # Delete squad (cascade will delete members and restaurants)
        if has_org_id:
            cursor.execute("DELETE FROM squads WHERE id = %s AND org_id = %s", (squad_id, org_id))
        else:
            cursor.execute("DELETE FROM squads WHERE id = %s", (squad_id,))
        conn.commit()

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Squad "{squad_name}" excluido com sucesso'
        })

    except Exception as e:
        print(f"Error deleting squad: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


@app.route('/api/squads/<int:squad_id>/members', methods=['POST'])
@admin_required
def api_add_squad_members(squad_id):
    """Add members to a squad"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        user_ids = data.get('user_ids', [])

        if not user_ids:
            return jsonify({'success': False, 'error': 'Nenhum usuario selecionado'}), 400

        conn = db.get_connection()
        cursor = conn.cursor()

        # Check if squad exists in current org
        if not _squad_belongs_to_org(cursor, squad_id, org_id):
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

        added_count = 0
        for user_id in user_ids:
            try:
                cursor.execute("""
                    INSERT INTO squad_members (squad_id, user_id)
                    VALUES (%s, %s)
                    ON CONFLICT (squad_id, user_id) DO NOTHING
                """, (squad_id, user_id))
                if cursor.rowcount > 0:
                    added_count += 1
            except Exception as e:
                print(f"Error adding member {user_id}: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'{added_count} membro(s) adicionado(s)',
            'added_count': added_count
        })

    except Exception as e:
        print(f"Error adding squad members: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


@app.route('/api/squads/<int:squad_id>/members/<int:user_id>', methods=['DELETE'])
@admin_required
def api_remove_squad_member(squad_id, user_id):
    """Remove a member from a squad"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        conn = db.get_connection()
        cursor = conn.cursor()

        if not _squad_belongs_to_org(cursor, squad_id, org_id):
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

        cursor.execute("""
            DELETE FROM squad_members
            WHERE squad_id = %s AND user_id = %s
        """, (squad_id, user_id))

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Membro nao encontrado no squad'}), 404

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Membro removido do squad'
        })

    except Exception as e:
        print(f"Error removing squad member: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


@app.route('/api/squads/<int:squad_id>/restaurants', methods=['POST'])
@admin_required
def api_add_squad_restaurants(squad_id):
    """Add restaurants to a squad"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        restaurant_ids = data.get('restaurant_ids', [])

        if not restaurant_ids:
            return jsonify({'success': False, 'error': 'Nenhum restaurante selecionado'}), 400

        conn = db.get_connection()
        cursor = conn.cursor()

        # Check if squad exists in current org
        if not _squad_belongs_to_org(cursor, squad_id, org_id):
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

        restaurants_index = {}
        for restaurant in get_current_org_restaurants():
            if not isinstance(restaurant, dict):
                continue
            rid = str(restaurant.get('id') or '').strip()
            if rid:
                restaurants_index[rid] = restaurant.get('name', 'Unknown')

        added_count = 0
        seen_restaurant_ids = set()
        for restaurant_id in restaurant_ids:
            rid = str(restaurant_id or '').strip()
            if not rid or rid in seen_restaurant_ids:
                continue
            seen_restaurant_ids.add(rid)
            restaurant_name = restaurants_index.get(rid, 'Unknown')

            try:
                cursor.execute("""
                    INSERT INTO squad_restaurants (squad_id, restaurant_id, restaurant_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (squad_id, restaurant_id) DO NOTHING
                """, (squad_id, rid, restaurant_name))
                if cursor.rowcount > 0:
                    added_count += 1
            except Exception as e:
                print(f"Error adding restaurant {rid}: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'{added_count} restaurante(s) adicionado(s)',
            'added_count': added_count
        })

    except Exception as e:
        print(f"Error adding squad restaurants: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


@app.route('/api/squads/<int:squad_id>/restaurants/<restaurant_id>', methods=['DELETE'])
@admin_required
def api_remove_squad_restaurant(squad_id, restaurant_id):
    """Remove a restaurant from a squad"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        conn = db.get_connection()
        cursor = conn.cursor()

        if not _squad_belongs_to_org(cursor, squad_id, org_id):
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

        cursor.execute("""
            DELETE FROM squad_restaurants
            WHERE squad_id = %s AND restaurant_id = %s
        """, (squad_id, restaurant_id))

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Restaurante nao encontrado no squad'}), 404

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Restaurante removido do squad'
        })

    except Exception as e:
        print(f"Error removing squad restaurant: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


# ============================================================================
# GROUPS (CLIENT GROUPS) ROUTES
# ============================================================================


@app.route('/api/user/allowed-restaurants')
@login_required
def api_user_allowed_restaurants():
    """Get list of restaurant IDs the current user can access based on squad membership"""
    try:
        user = session.get('user', {})
        user_id = user.get('id')
        user_role = user.get('role')
        
        # Admins/site-admins see all restaurants
        if user_role in ('admin', 'site_admin'):
            return jsonify({
                'success': True,
                'allowed_all': True,
                'restaurant_ids': []
            })
        
        allowed_ids = get_user_allowed_restaurant_ids(user_id, user_role)
        restaurant_ids = allowed_ids or []
        
        # If user is not in any squad, they see all restaurants (default behavior)
        if not restaurant_ids:
            return jsonify({
                'success': True,
                'allowed_all': True,
                'restaurant_ids': []
            })
        
        return jsonify({
            'success': True,
            'allowed_all': False,
            'restaurant_ids': restaurant_ids
        })
        
    except Exception as e:
        print(f"Error getting allowed restaurants: {e}")
        log_exception("request_exception", e)
        return internal_error_response()


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.route('/cdn-cgi/<path:path>')
def cdn_cgi_fallback(path):
    """Fallback for Cloudflare CDN requests - prevents 404 errors"""
    return '', 204


@app.errorhandler(404)
def page_not_found(e):
    """Custom 404 error handler"""
    print(f"Ã¢ÂÅ’ 404 Error: {request.url}")
    
    if request.is_json or request.path.startswith('/api/'):
        return jsonify({'error': 'Not found'}), 404
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>404 - Not Found</title>
        <style>
            body {{ font-family: system-ui, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }}
            .error {{ background: #fee; border: 1px solid #fcc; padding: 20px; border-radius: 8px; }}
            a {{ color: #ef4444; }}
        </style>
    </head>
    <body>
        <div class="error">
            <h1>404 - Page Not Found</h1>
            <p>The requested URL was not found on this server.</p>
        </div>
        <p><a href="/login">Ã¢â€ Â Back to Login</a></p>
    </body>
    </html>
    """, 404


@app.errorhandler(500)
def internal_error(e):
    """Custom 500 error handler"""
    print("Ã¢ÂÅ’ 500 Error")
    log_exception("request_exception", e)
    
    if request.is_json or request.path.startswith('/api/'):
        return jsonify({'error': 'Internal server error'}), 500
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>500 - Internal Server Error</title></head>
    <body style="font-family: system-ui, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px;">
        <div style="background: #fee; border: 1px solid #fcc; padding: 20px; border-radius: 8px;">
            <h1>500 - Internal Server Error</h1>
            <p>An error occurred while processing your request.</p>
        </div>
        <p><a href="/login" style="color: #ef4444;">Ã¢â€ Â Back to Login</a></p>
    </body>
    </html>
    """, 500


# ============================================================================
# INITIALIZATION
# ============================================================================

def check_setup():
    """Check if setup is correct"""
    print("\n" + "="*60)
    print("Ã°Å¸â€Â Checking Setup...")
    print("="*60)
    
    issues = []
    
    # Check dashboard_output directory
    if not DASHBOARD_OUTPUT.exists():
        issues.append(f"Ã¢ÂÅ’ dashboard_output/ directory not found at {DASHBOARD_OUTPUT}")
        # Try to create it
        try:
            DASHBOARD_OUTPUT.mkdir(parents=True, exist_ok=True)
            print(f"   Created dashboard_output/ directory")
        except Exception as mkdir_error:
            print(f"   Note: could not create dashboard_output/ ({mkdir_error})")
    else:
        print(f"Ã¢Å“â€¦ dashboard_output/ directory exists")
        
        # Check for HTML files
        required_files = ['login.html', 'index.html']
        optional_files = ['admin.html', 'restaurant_template.html']
        
        for filename in required_files:
            file_path = DASHBOARD_OUTPUT / filename
            if file_path.exists():
                print(f"   Ã¢Å“â€¦ {filename}")
            else:
                issues.append(f"Ã¢ÂÅ’ Missing required: dashboard_output/{filename}")
        
        for filename in optional_files:
            file_path = DASHBOARD_OUTPUT / filename
            if file_path.exists():
                print(f"   Ã¢Å“â€¦ {filename}")
            else:
                print(f"   Ã¢Å¡Â Ã¯Â¸Â Optional: {filename} not found")
    
    # Check iFood config
    if not CONFIG_FILE.exists():
        print(f"Ã¢Å¡Â Ã¯Â¸Â  iFood config not found (will be created)")
    else:
        print(f"Ã¢Å“â€¦ iFood config file exists")
    
    if issues:
        print("\nÃ¢Å¡Â Ã¯Â¸Â  Issues found:")
        for issue in issues:
            print(f"   {issue}")
        print()
    else:
        print("\nÃ¢Å“â€¦ All checks passed!")
    
    return len([i for i in issues if i.startswith('Ã¢ÂÅ’')]) == 0


def initialize_database():
    """Initialize database tables and create default users if needed"""
    print("\nInitializing database...")
    conn = None
    cursor = None
    try:
        if not db.setup_tables():
            print("Database setup_tables() failed.")
            return False
        
        # Create hidden stores table
        conn = db.get_connection()
        if not conn:
            print("Database connection unavailable during initialization.")
            return False
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hidden_stores (
                store_id VARCHAR(255) PRIMARY KEY,
                store_name VARCHAR(255),
                hidden_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hidden_by VARCHAR(255)
            )
        """)
        
        # Create squads tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS squads (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(255)
            )
        """)
        
        # Migration: Add description column if it doesn't exist (for existing installations)
        try:
            cursor.execute("""
                ALTER TABLE squads ADD COLUMN IF NOT EXISTS description TEXT
            """)
        except Exception as e:
            # Column might already exist or DB doesn't support IF NOT EXISTS
            print(f"   Note: description column migration: {e}")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS squad_members (
                id SERIAL PRIMARY KEY,
                squad_id INTEGER NOT NULL REFERENCES squads(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES dashboard_users(id) ON DELETE CASCADE,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(squad_id, user_id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS squad_restaurants (
                id SERIAL PRIMARY KEY,
                squad_id INTEGER NOT NULL REFERENCES squads(id) ON DELETE CASCADE,
                restaurant_id VARCHAR(255) NOT NULL,
                restaurant_name VARCHAR(255),
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(squad_id, restaurant_id)
            )
        """)

        # Ensure org_id exists on tenant-scoped tables created in legacy setups.
        # Use fixed statements per table to avoid dynamic SQL interpolation.
        org_id_migrations = {
            'hidden_stores': """
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'hidden_stores' AND column_name = 'org_id'
                    ) THEN
                        ALTER TABLE hidden_stores
                        ADD COLUMN org_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE;
                    END IF;
                END $$;
            """,
            'squads': """
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'squads' AND column_name = 'org_id'
                    ) THEN
                        ALTER TABLE squads
                        ADD COLUMN org_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE;
                    END IF;
                END $$;
            """,
            'client_groups': """
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'client_groups' AND column_name = 'org_id'
                    ) THEN
                        ALTER TABLE client_groups
                        ADD COLUMN org_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE;
                    END IF;
                END $$;
            """
        }
        for tbl, stmt in org_id_migrations.items():
            try:
                cursor.execute(stmt)
            except Exception as migration_error:
                print(f"   Note: org_id migration for {tbl}: {migration_error}")
        
        conn.commit()
        # Migrations may change table columns; reset cached metadata.
        _clear_table_columns_cache()
        _prime_table_columns_cache((
            'hidden_stores',
            'squads',
            'client_groups',
            'group_stores',
            'squad_members',
            'squad_restaurants',
        ))
        cursor.close()
        cursor = None
        conn.close()
        conn = None
        
        users = db.get_all_users()
        if not users:
            bootstrap_defaults = str(os.environ.get('BOOTSTRAP_DEFAULT_USERS', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
            if bootstrap_defaults:
                print("Ã°Å¸â€˜Â¤ BOOTSTRAP_DEFAULT_USERS enabled: creating default users")
                db.create_default_users()
            else:
                print("No users found. Skipping insecure default-user bootstrap (set BOOTSTRAP_DEFAULT_USERS=true to enable temporarily).")
        else:
            print(f"   Found {len(users)} existing users")
        print("Database ready")
        return True
    except Exception as e:
        log_exception("database_initialization_failed", e)
        return False
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def initialize_app():
    """Initialize the application with SaaS multi-tenant support"""
    global APP_INITIALIZED
    with _INIT_LOCK:
        if APP_INITIALIZED:
            return

    print("="*60)
    print("TIMO Dashboard Server - SaaS Multi-Tenant")
    print("  Features: Per-org data, Self-service registration,")
    print("  Real-time SSE, Background Refresh, Plans & Billing")
    print("="*60)
    
    # Check setup
    setup_ok = check_setup()
    if not setup_ok:
        is_production = str(os.environ.get('FLASK_ENV', '')).strip().lower() == 'production'
        if is_production:
            raise RuntimeError(
                "Missing required dashboard_output files (login.html/index.html). "
                "Ensure deployment artifact includes dashboard_output/."
            )
        print("WARNING: Setup check failed; continuing because FLASK_ENV is not production.")
    
    # Initialize database (includes SaaS tables)
    db_ok = initialize_database()
    if not db_ok:
        raise RuntimeError("Database initialization failed")
    
    # Try per-org initialization first (SaaS mode)
    initialize_all_orgs()
    
    queue_mode = USE_REDIS_QUEUE and bool(get_redis_client())
    is_worker_process = (
        ('--worker' in sys.argv)
        or (str(os.environ.get('RUN_REFRESH_WORKER', '')).strip().lower() in ('1', 'true', 'yes', 'on'))
    )

    # Fallback: if no orgs have data, try legacy config file
    org_values_snapshot = _org_data_values_snapshot()
    if not any((od or {}).get('restaurants') for od in org_values_snapshot):
        print("\nNo org data found, trying legacy config file...")
        ifood_ok = initialize_ifood_api()
        if ifood_ok:
            snapshot_loaded = _load_data_snapshot()
            if snapshot_loaded:
                print("Fast start: serving cached data while refreshing in background")
                if queue_mode:
                    enqueue_refresh_job(trigger='startup')
                else:
                    threading.Thread(target=bg_refresher.refresh_now, daemon=True).start()
            else:
                print("First start: loading data from iFood API...")
                load_restaurants_from_ifood()
                _save_data_snapshot()
            refresh_minutes = IFOOD_CONFIG.get('refresh_interval_minutes', 30)
            if not queue_mode:
                bg_refresher.interval = refresh_minutes * 60
                bg_refresher.start()
    else:
        # Start background refresh for all orgs
        if not queue_mode:
            bg_refresher.interval = 1800  # 30 min
            bg_refresher.start()

    # Ensure keepalive polling can run on web instances when worker mode is not active.
    if IFOOD_KEEPALIVE_POLLING and not is_worker_process:
        start_keepalive_poller()
    
    org_values_snapshot = _org_data_values_snapshot()
    total_restaurants = sum(len((od or {}).get('restaurants') or []) for od in org_values_snapshot) + len(RESTAURANTS_DATA)
    total_orgs = len([o for o in org_values_snapshot if (o or {}).get('restaurants')])
    
    print("\n" + "="*60)
    print("TIMO Server Ready")
    print("="*60)
    print(f"\nOrganizations: {total_orgs}")
    print(f"Total Restaurants: {total_restaurants}")
    if queue_mode:
        print("Background refresh: Redis queue worker mode")
        if IFOOD_KEEPALIVE_POLLING:
            print(f"iFood keepalive polling: every {IFOOD_POLL_INTERVAL_SECONDS}s")
    else:
        print("Background refresh: every 30 min")
    print("SSE: ready on /api/events")
    print(f"\nAccess: http://localhost:{os.environ.get('PORT', 5000)}")
    print("="*60)
    print()
    APP_INITIALIZED = True


# ============================================================================
# MODULAR ROUTE REGISTRATION
# ============================================================================

from app_routes.auth_routes import register as register_auth_routes
from app_routes.org_routes import register as register_org_routes
from app_routes.ops_routes import register as register_ops_routes
from app_routes.restaurants_routes import register as register_restaurants_routes
from app_routes.groups_routes import register as register_groups_routes

register_auth_routes(app, globals())
register_org_routes(app, globals())
register_ops_routes(app, globals())
register_restaurants_routes(app, globals())
register_groups_routes(app, globals())


if __name__ == '__main__':
    import sys
    if '--bootstrap' in sys.argv:
        initialize_app()
        print("Bootstrap complete")
        sys.exit(0)

    initialize_app()
    run_worker = ('--worker' in sys.argv) or (str(os.environ.get('RUN_REFRESH_WORKER', '')).strip().lower() in ('1', 'true', 'yes', 'on'))

    if run_worker:
        if not (USE_REDIS_QUEUE and get_redis_client()):
            print("Worker mode requires Redis queue (set REDIS_URL and USE_REDIS_QUEUE=true).")
            sys.exit(1)
        interval_minutes = 30
        try:
            interval_minutes = int((IFOOD_CONFIG or {}).get('refresh_interval_minutes') or os.environ.get('REFRESH_INTERVAL_MINUTES', 30))
        except Exception:
            interval_minutes = 30
        if IFOOD_KEEPALIVE_POLLING:
            print(f"Running in WORKER mode (refresh interval: {interval_minutes} min, keepalive polling: {IFOOD_POLL_INTERVAL_SECONDS}s)")
        else:
            print(f"Running in WORKER mode (refresh interval: {interval_minutes} min)")
        run_refresh_worker_loop(interval_seconds=max(60, interval_minutes * 60))
        sys.exit(0)
    
    # Check if running in production mode
    if '--production' in sys.argv or os.environ.get('FLASK_ENV') == 'production':
        print("\nWARNING: For production, use a WSGI server instead:")
        print("   Option 1 (Linux/Mac): gunicorn -c gunicorn_config.py dashboardserver:app")
        print("   Option 2 (Windows):   python run_production.py")
        print("   Option 3 (Any OS):    waitress-serve --port=5000 dashboardserver:app")
        sys.exit(1)
    
    # Development mode
    print("\nRunning in DEVELOPMENT mode")
    print("For production, use: python dashboardserver.py --production")
    print()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
