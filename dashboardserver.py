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
from typing import Dict, List, Optional
import traceback
from datetime import datetime, timedelta
from functools import wraps
import uuid
import threading
import time
import queue
import copy
import csv
import io
import sys

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

# In-memory cache for processed API responses
_api_cache = {}  # key: (org_id, month) -> {'data': [...], 'timestamp': datetime}
_API_CACHE_TTL = 30  # seconds

def get_cached_restaurants(org_id, month_filter):
    """Get cached processed restaurant data if still fresh"""
    key = (org_id, month_filter)
    cached = _api_cache.get(key)
    if cached and (datetime.now() - cached['timestamp']).total_seconds() < _API_CACHE_TTL:
        return cached['data']
    return None

def set_cached_restaurants(org_id, month_filter, data):
    """Cache processed restaurant data"""
    key = (org_id, month_filter)
    _api_cache[key] = {'data': data, 'timestamp': datetime.now()}

def invalidate_cache():
    """Clear all API caches (called after data refresh)"""
    _api_cache.clear()

# Per-org data store: {org_id: {'restaurants': [], 'api': IFoodAPI, 'last_refresh': datetime, 'config': {}}}
ORG_DATA = {}
# Legacy global for backward compat during transition
RESTAURANTS_DATA = []
IFOOD_API = None
IFOOD_CONFIG = {}
LAST_DATA_REFRESH = None
APP_STARTED_AT = datetime.utcnow()

# Marketing metadata for plan cards in admin UI.
PLAN_CATALOG_UI = {
    'starter': {
        'subtitle': 'Centralizacao e relatorios',
        'badge': None,
        'highlight': False,
        'note': 'Em breve',
        'features_ui': [
            'Centralizacao e relatorios'
        ]
    },
    'pro': {
        'subtitle': 'O plano completo para agencias',
        'badge': 'Agencias',
        'highlight': True,
        'note': 'Em breve',
        'features_ui': [
            'Multiusuario',
            'Squads',
            'Links publicos',
            'Relatorios em PDF'
        ]
    },
    'enterprise': {
        'subtitle': 'Para operacoes avancadas',
        'badge': None,
        'highlight': False,
        'note': 'Em breve',
        'features_ui': [
            'Customizacoes avancadas',
            'Integracoes sob demanda',
            'White Label'
        ]
    }
}


def get_org_data(org_id):
    """Get or initialize org data container"""
    if org_id not in ORG_DATA:
        ORG_DATA[org_id] = {'restaurants': [], 'api': None, 'last_refresh': None, 'config': {}}
    return ORG_DATA[org_id]


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


def get_current_org_restaurants():
    """Get restaurant data for the current session's org"""
    org_id = get_current_org_id()
    if org_id and org_id in ORG_DATA:
        org_restaurants = ORG_DATA[org_id].get('restaurants') or []
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


def parse_month_filter(raw_month):
    """Validate month query parameter."""
    if raw_month in (None, '', 'all'):
        return 'all'
    if isinstance(raw_month, int):
        month_value = raw_month
    else:
        raw_str = str(raw_month).strip()
        if not raw_str.isdigit():
            return None
        month_value = int(raw_str)
    if 1 <= month_value <= 12:
        return f"{month_value:02d}"
    return None


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


def filter_orders_by_month(orders, month_filter):
    if month_filter == 'all':
        return orders
    target_month = int(month_filter)
    filtered = []
    for order in orders:
        try:
            created_at = order.get('createdAt')
            if not created_at:
                continue
            order_date = datetime.fromisoformat(str(created_at).replace('Z', '+00:00'))
            if order_date.month == target_month:
                filtered.append(order)
        except Exception:
            continue
    return filtered


def aggregate_dashboard_summary(restaurants):
    total_orders = 0
    gross_revenue = 0.0
    net_revenue = 0.0
    positive_trend_count = 0
    negative_trend_count = 0

    for restaurant in restaurants:
        metrics = restaurant.get('metrics', {})
        trends = metrics.get('trends') or {}
        total_orders += int(metrics.get('vendas') or 0)
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

# ============================================================================
# REAL-TIME SSE (Server-Sent Events) INFRASTRUCTURE
# ============================================================================

class SSEManager:
    """Manages Server-Sent Events for real-time order tracking"""
    
    def __init__(self):
        self._clients = []  # List of queue objects, one per connected client
        self._lock = threading.Lock()
    
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
    
    def broadcast(self, event_type: str, data: dict):
        """Send an event to all connected clients"""
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
        print(f"ðŸ”„ Background refresh started (every {self.interval // 60} min)")
    
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
        try:
            self._is_refreshing = True
            sse_manager.broadcast('refresh_status', {'status': 'refreshing', 'timestamp': datetime.now().isoformat()})
            
            _do_data_refresh()
            
            sse_manager.broadcast('refresh_status', {'status': 'complete', 'timestamp': datetime.now().isoformat(), 'count': len(RESTAURANTS_DATA)})
            sse_manager.broadcast('data_updated', {'restaurant_count': len(RESTAURANTS_DATA), 'timestamp': datetime.now().isoformat()})
            return True
        except Exception as e:
            print(f"âŒ Background refresh error: {e}")
            sse_manager.broadcast('refresh_status', {'status': 'error', 'error': str(e)})
            return False
        finally:
            self._is_refreshing = False
            self._refresh_lock.release()
    
    @property
    def is_refreshing(self):
        return self._is_refreshing

bg_refresher = BackgroundRefresher()


def _do_data_refresh():
    """Core refresh logic: fetch from API, update cache, save snapshot to DB"""
    global RESTAURANTS_DATA, LAST_DATA_REFRESH
    
    # Refresh per-org data (SaaS mode)
    for org_id, od in ORG_DATA.items():
        if od.get('api'):
            try:
                _load_org_restaurants(org_id)
            except Exception as e:
                print(f"âš ï¸ Org {org_id} refresh error: {e}")
    
    # Also refresh legacy global data if configured
    if not IFOOD_API:
        return
    
    new_data = []
    merchants_config = IFOOD_CONFIG.get('merchants', [])
    days = IFOOD_CONFIG.get('data_fetch_days', 30)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    for merchant_config in merchants_config:
        merchant_id = merchant_config.get('merchant_id')
        name = merchant_config.get('name', 'Unknown Restaurant')
        
        try:
            merchant_details = IFOOD_API.get_merchant_details(merchant_id)
            if not merchant_details:
                merchant_details = {
                    'id': merchant_id,
                    'name': name,
                    'merchantManager': {'name': merchant_config.get('manager', 'Gerente')}
                }
            
            orders = IFOOD_API.get_orders(merchant_id, start_date, end_date)
            
            financial_data = None
            if hasattr(IFOOD_API, 'get_financial_data'):
                try:
                    financial_data = IFOOD_API.get_financial_data(merchant_id, start_date, end_date)
                except:
                    pass
            
            restaurant_data = IFoodDataProcessor.process_restaurant_data(merchant_details, orders, financial_data)
            
            if merchant_config.get('name'):
                restaurant_data['name'] = merchant_config['name']
            if merchant_config.get('manager'):
                restaurant_data['manager'] = merchant_config['manager']
            
            restaurant_data['_orders_cache'] = orders
            new_data.append(restaurant_data)
            
            # Broadcast new order events for real-time tracking
            _detect_and_broadcast_new_orders(merchant_id, name, orders)
            
        except Exception as e:
            print(f"   âŒ Failed to refresh {name}: {e}")
    
    # Atomic swap
    RESTAURANTS_DATA = new_data
    LAST_DATA_REFRESH = datetime.now()
    
    # Invalidate API response caches since data changed
    invalidate_cache()
    
    # Save snapshot to DB for fast cold starts
    _save_data_snapshot()
    
    print(f"ðŸ”„ Refreshed {len(RESTAURANTS_DATA)} restaurant(s) at {LAST_DATA_REFRESH.strftime('%H:%M:%S')}")


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
        
        # Prepare data (strip internal caches for storage)
        snapshot = []
        for r in RESTAURANTS_DATA:
            clean = {k: v for k, v in r.items() if not k.startswith('_')}
            snapshot.append(clean)
        
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
        print(f"âš ï¸ Failed to save snapshot: {e}")


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
                RESTAURANTS_DATA = data
                LAST_DATA_REFRESH = created_at
                print(f"âš¡ Loaded {len(data)} restaurants from DB snapshot ({age.seconds // 60} min old)")
                return True
            else:
                print(f"â³ DB snapshot too old ({age.seconds // 3600}h), will refresh from API")
                return False
        return False
    except Exception as e:
        print(f"âš ï¸ Failed to load snapshot: {e}")
        return False



# ============================================================================
# IFOOD API INITIALIZATION
# ============================================================================

# ============================================================================
# IFOOD API INITIALIZATION - PER-ORG
# ============================================================================

def _init_org_ifood(org_id):
    """Initialize iFood API for a specific org from DB credentials"""
    config = db.get_org_ifood_config(org_id)
    if not config or not config.get('client_id'):
        return None
    org = get_org_data(org_id)
    org['config'] = config
    try:
        api = IFoodAPI(config['client_id'], config['client_secret'])
        if api.authenticate():
            org['api'] = api
            print(f"âœ… Org {org_id}: iFood API authenticated")
            return api
        else:
            print(f"âš ï¸ Org {org_id}: iFood auth failed")
    except Exception as e:
        print(f"âŒ Org {org_id}: iFood init error: {e}")
    return None


def _load_org_restaurants(org_id):
    """Load restaurant data for a specific org"""
    org = get_org_data(org_id)
    api = org.get('api')
    config = org.get('config') or db.get_org_ifood_config(org_id) or {}
    if not api:
        return
    merchants_config = config.get('merchants', [])
    if not merchants_config:
        try:
            merchants = api.get_merchants()
            if merchants:
                merchants_config = [{'merchant_id': m.get('id'), 'name': m.get('name', 'Restaurant')} for m in merchants]
                db.update_org_ifood_config(org_id, merchants=merchants_config)
        except:
            pass
    if not merchants_config:
        return
    days = config.get('settings', {}).get('data_fetch_days', 30)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    new_data = []
    for mc in merchants_config:
        merchant_id = mc.get('merchant_id') or mc.get('id')
        if not merchant_id:
            continue
        try:
            details = api.get_merchant_details(merchant_id)
            if not details:
                continue
            orders = api.get_orders(merchant_id, start_date, end_date)
            restaurant_data = IFoodDataProcessor.process_restaurant_data(details, orders or [], None)
            restaurant_data['_orders_cache'] = orders or []
            new_data.append(restaurant_data)
        except Exception as e:
            print(f"  âš ï¸ Org {org_id}, merchant {merchant_id}: {e}")
    org['restaurants'] = new_data
    org['last_refresh'] = datetime.now()
    db.save_org_data_cache(org_id, 'restaurants', [{k:v for k,v in r.items() if not k.startswith('_')} for r in new_data])
    print(f"âœ… Org {org_id}: loaded {len(new_data)} restaurants")


def initialize_all_orgs():
    """Initialize iFood API and load data for all active orgs with credentials"""
    global RESTAURANTS_DATA, IFOOD_API, IFOOD_CONFIG, LAST_DATA_REFRESH
    orgs = db.get_all_active_orgs()
    print(f"\nðŸ¢ Initializing {len(orgs)} organization(s)...")
    for org_info in orgs:
        org_id = org_info['id']
        # Try cache first
        cached = db.load_org_data_cache(org_id, 'restaurants', max_age_hours=2)
        if cached:
            od = get_org_data(org_id)
            od['restaurants'] = cached
            od['last_refresh'] = datetime.now()
            print(f"  âš¡ Org {org_id} ({org_info['name']}): {len(cached)} restaurants from cache")
            # Init API in background
            threading.Thread(target=_init_and_refresh_org, args=(org_id,), daemon=True).start()
        else:
            api = _init_org_ifood(org_id)
            if api:
                _load_org_restaurants(org_id)
    # Set legacy globals for backward compat (use first org's data)
    if orgs and orgs[0]['id'] in ORG_DATA:
        first = ORG_DATA[orgs[0]['id']]
        RESTAURANTS_DATA = first['restaurants']
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
    IFOOD_CONFIG = IFoodConfig.load_config(str(CONFIG_FILE))
    
    if not IFOOD_CONFIG:
        print("âš ï¸  No iFood configuration found")
        print(f"   Creating sample config at {CONFIG_FILE}")
        IFoodConfig.create_sample_config(str(CONFIG_FILE))
        return False
    
    client_id = IFOOD_CONFIG.get('client_id')
    client_secret = IFOOD_CONFIG.get('client_secret')
    
    if not client_id or client_id == 'your_ifood_client_id_here':
        print("âš ï¸  iFood API credentials not configured")
        print(f"   Please update {CONFIG_FILE} with your credentials")
        return False
    
    # Initialize API
    IFOOD_API = IFoodAPI(client_id, client_secret)
    
    # Authenticate
    if IFOOD_API.authenticate():
        print("âœ… iFood API initialized successfully")
        return True
    else:
        print("âŒ iFood API authentication failed")
        return False


def load_restaurants_from_ifood():
    """Load all restaurants from iFood API"""
    global RESTAURANTS_DATA, LAST_DATA_REFRESH
    
    RESTAURANTS_DATA = []
    
    if not IFOOD_API:
        print("âŒ iFood API not initialized")
        return
    
    print(f"\nðŸ“Š Fetching restaurant data from iFood API...")
    
    # Get merchants from config
    merchants_config = IFOOD_CONFIG.get('merchants', [])
    
    if not merchants_config:
        print("âš ï¸  No merchants configured in config file")
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
            print(f"âŒ Error fetching merchants: {e}")
            return
    
    # Get data fetch period
    days = IFOOD_CONFIG.get('data_fetch_days', 30)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    print(f"   Fetching data from {start_date} to {end_date}")
    
    # Process each merchant
    for merchant_config in merchants_config:
        merchant_id = merchant_config.get('merchant_id')
        name = merchant_config.get('name', 'Unknown Restaurant')
        
        print(f"   ðŸ“„ Processing: {name}")
        
        try:
            # Get merchant details
            merchant_details = IFOOD_API.get_merchant_details(merchant_id)
            if not merchant_details:
                merchant_details = {
                    'id': merchant_id,
                    'name': name,
                    'merchantManager': {'name': merchant_config.get('manager', 'Gerente')}
                }
            
            # Get orders
            orders = IFOOD_API.get_orders(merchant_id, start_date, end_date)
            print(f"      Found {len(orders)} orders")
            
            # Get financial data if available
            financial_data = None
            if hasattr(IFOOD_API, 'get_financial_data'):
                try:
                    financial_data = IFOOD_API.get_financial_data(merchant_id, start_date, end_date)
                except:
                    pass
            
            # Process into dashboard format
            restaurant_data = IFoodDataProcessor.process_restaurant_data(
                merchant_details, 
                orders,
                financial_data
            )
            
            # Override name and manager from config if provided
            if merchant_config.get('name'):
                restaurant_data['name'] = merchant_config['name']
            if merchant_config.get('manager'):
                restaurant_data['manager'] = merchant_config['manager']
            
            # Store raw orders for charts
            restaurant_data['_orders_cache'] = orders
            
            RESTAURANTS_DATA.append(restaurant_data)
            print(f"      âœ… {restaurant_data['name']}")
            
        except Exception as e:
            print(f"      âŒ Failed to process {name}: {e}")
            traceback.print_exc()
    
    LAST_DATA_REFRESH = datetime.now()
    print(f"\nâœ… Loaded {len(RESTAURANTS_DATA)} restaurant(s) from iFood")


# ============================================================================
# AUTHENTICATION & SESSION MANAGEMENT
# ============================================================================

def login_required(f):
    """Decorator to require login for routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            # Check if this is an API/AJAX request
            is_api = (request.is_json or 
                      request.path.startswith('/api/') or
                      request.headers.get('X-Requested-With') == 'XMLHttpRequest' or
                      'application/json' in (request.headers.get('Accept', '')))
            if is_api:
                return jsonify({'error': 'Authentication required', 'redirect': '/login'}), 401
            return redirect('/login')
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


def admin_required(f):
    """Decorator to require admin privileges in current org or platform."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            is_api = (request.is_json or 
                      request.path.startswith('/api/') or
                      request.headers.get('X-Requested-With') == 'XMLHttpRequest' or
                      'application/json' in (request.headers.get('Accept', '')))
            if is_api:
                return jsonify({'error': 'Authentication required', 'redirect': '/login'}), 401
            return redirect('/login')

        user = session.get('user', {})
        if is_platform_admin_user(user):
            return f(*args, **kwargs)

        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'error': 'Organization context required'}), 403

        org_role = db.get_org_member_role(org_id, user.get('id'))
        if org_role not in ('owner', 'admin'):
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function


def platform_admin_required(f):
    """Decorator to require global platform-admin privileges."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            is_api = (
                request.is_json
                or request.path.startswith('/api/')
                or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
                or 'application/json' in (request.headers.get('Accept', ''))
            )
            if is_api:
                return jsonify({'error': 'Authentication required', 'redirect': '/login'}), 401
            return redirect('/login')

        user = session.get('user', {})
        if not is_platform_admin_user(user):
            return jsonify({'error': 'Platform admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function


def org_owner_required(f):
    """Decorator to require owner/admin role in current organization."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return jsonify({'error': 'Authentication required', 'redirect': '/login'}), 401

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
            return redirect('/dashboard')
        return decorated_function
    return decorator


# ============================================================================
# STATIC FILE ROUTES
# ============================================================================

@app.route('/')
def index():
    """Redirect to login or dashboard based on session"""
    if 'user' in session:
        return redirect('/dashboard')
    return redirect('/login')


@app.route('/login')
def login_page():
    """Serve login page"""
    login_file = DASHBOARD_OUTPUT / 'login.html'
    if login_file.exists():
        return send_file(login_file)
    return "Login page not found. Please check dashboard_output directory.", 404


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
    # Find restaurant in org data
    restaurant = None
    for r in get_current_org_restaurants():
        if r['id'] == restaurant_id:
            restaurant = r
            break
    
    if not restaurant:
        return "Restaurant not found", 404
    
    # Check if we have a template
    template_file = DASHBOARD_OUTPUT / 'restaurant_template.html'
    if template_file.exists():
        with open(template_file, 'r', encoding='utf-8') as f:
            template = f.read()
        
        # Replace placeholders with actual data
        rendered = template.replace('{{restaurant_name}}', escape_html_text(restaurant.get('name', 'Restaurante')))
        rendered = rendered.replace('{{restaurant_id}}', escape_html_text(restaurant.get('id', restaurant_id)))
        rendered = rendered.replace('{{restaurant_manager}}', escape_html_text(restaurant.get('manager', 'Gerente')))
        rendered = rendered.replace('{{restaurant_data}}', safe_json_for_script(restaurant))
        
        return Response(rendered, mimetype='text/html')
    
    return "Restaurant template not found", 404


# ============================================================================
# API ROUTES - AUTHENTICATION
# ============================================================================

@app.route('/api/login', methods=['POST'])
def api_login():
    """Handle login requests"""
    try:
        data = get_json_payload()
        if not data:
            return jsonify({'success': False, 'error': 'Invalid JSON payload'}), 400
        email = data.get('email', '').strip()
        password = data.get('password', '')
        
        if not email or not password:
            return jsonify({
                'success': False,
                'error': 'Email and password required'
            }), 400
        
        # Authenticate using email
        user = db.authenticate_user_by_email(email, password)
        
        if user:
            user['is_platform_admin'] = bool(db.is_platform_admin(user.get('id')))
            session['user'] = user
            session.permanent = True
            
            # Set org context
            orgs = db.get_user_orgs(user['id'])
            if orgs:
                session['org_id'] = orgs[0]['id']
                session['org_name'] = orgs[0]['name']
                session['org_plan'] = orgs[0]['plan']
            
            redirect_url = '/dashboard'
            
            return jsonify({
                'success': True,
                'user': user,
                'redirect': redirect_url
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Invalid email or password'
            }), 401
            
    except Exception as e:
        print(f"Login error: {e}")
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': 'Server error during login'
        }), 500


@app.route('/api/logout', methods=['POST'])
def api_logout():
    """Logout user and clear session"""
    session.clear()
    return jsonify({'success': True, 'message': 'Logged out successfully'})


@app.route('/api/me')
@login_required
def api_me():
    """Get current user info with org context"""
    user = session.get('user') or {}
    org_id = get_current_org_id()
    org_info = None
    org_role = None
    if org_id:
        org_info = db.get_org_details(org_id)
        org_role = db.get_org_member_role(org_id, user.get('id'))

    user_payload = dict(user)
    user_payload['is_platform_admin'] = bool(is_platform_admin_user(user))
    user_payload['org_role'] = org_role
    if (not user_payload.get('is_platform_admin')) and org_role in ('owner', 'admin'):
        user_payload['role'] = 'admin'

    return jsonify({
        'success': True,
        'user': user_payload,
        'org_id': org_id,
        'org': org_info
    })


# ============================================================================
# SaaS API ROUTES
# ============================================================================

@app.route('/api/register', methods=['POST'])
def api_register():
    """Self-service signup: create account + organization"""
    try:
        data = get_json_payload()
        if not data:
            return jsonify({'success': False, 'error': 'Payload inválido'}), 400
        email = (data.get('email') or '').strip().lower()
        password = data.get('password', '')
        full_name = (data.get('full_name') or '').strip()
        org_name = (data.get('org_name') or '').strip()
        if not all([email, password, full_name, org_name]):
            return jsonify({'success': False, 'error': 'Todos os campos sÃ£o obrigatÃ³rios'}), 400
        if len(password) < 8:
            return jsonify({'success': False, 'error': 'Senha deve ter no mÃ­nimo 8 caracteres'}), 400
        result = db.register_user_and_org(email, password, full_name, org_name)
        if not result:
            return jsonify({'success': False, 'error': 'Email jÃ¡ cadastrado'}), 409
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
        db.log_action('user.registered', org_id=result['org_id'], user_id=result['user_id'], details={'email': email, 'org_name': org_name}, ip_address=request.remote_addr)
        return jsonify({'success': True, 'user_id': result['user_id'], 'org_id': result['org_id'], 'redirect': '/dashboard'})
    except Exception as e:
        print(f"Registration error: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/orgs', methods=['GET', 'POST'])
@login_required
def api_user_orgs():
    """Get or create organizations for the current user."""
    user = session.get('user') or {}
    user_id = user.get('id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401

    if request.method == 'GET':
        orgs = db.get_user_orgs(user_id)
        return jsonify({'success': True, 'organizations': orgs})

    data = get_json_payload()
    if not data:
        return jsonify({'success': False, 'error': 'Invalid payload'}), 400

    org_name = (data.get('org_name') or data.get('name') or '').strip()
    if not org_name:
        return jsonify({'success': False, 'error': 'Organization name is required'}), 400

    current_org_id = get_current_org_id()
    copy_users = bool(data.get('copy_users', True))
    current_org = db.get_org_details(current_org_id) if current_org_id else None

    plan = (data.get('plan') or '').strip().lower()
    if not plan:
        plan = (current_org or {}).get('plan') or 'free'

    source_members = []
    if copy_users and current_org_id:
        current_member_role = db.get_org_member_role(current_org_id, user_id)
        if current_member_role not in ('owner', 'admin'):
            return jsonify({'success': False, 'error': 'Owner/admin role required to copy users'}), 403
        source_members = db.get_org_users(current_org_id)

    created = db.create_organization(
        org_name,
        user_id,
        plan=plan,
        billing_email=(user.get('email') or None)
    )
    if not created:
        return jsonify({'success': False, 'error': 'Unable to create organization'}), 400

    # Ensure in-memory org bucket exists for the new org context.
    get_org_data(created['id'])

    copied_users = 0
    copy_errors = []
    if copy_users and source_members:
        for member in source_members:
            member_id = member.get('id')
            if not member_id or member_id == user_id:
                continue
            member_org_role = str(member.get('org_role') or 'viewer').strip().lower()
            if member_org_role not in ('owner', 'admin', 'viewer'):
                member_org_role = 'viewer'

            assign_result = db.assign_user_to_org(created['id'], member_id, member_org_role)
            if assign_result.get('success'):
                copied_users += 1
                continue

            if assign_result.get('error') == 'already_member':
                continue

            copy_errors.append({
                'user_id': member_id,
                'error': assign_result.get('error') or 'assign_failed'
            })

    # Switch active organization so the sidebar/current page updates immediately.
    session['org_id'] = created['id']
    session['org_name'] = created['name']
    session['org_plan'] = created['plan']
    if session.get('user'):
        session['user']['primary_org_id'] = created['id']

    db.log_action(
        'org.created',
        org_id=created['id'],
        user_id=user_id,
        details={
            'name': created['name'],
            'plan': created.get('plan'),
            'copy_users': copy_users,
            'copied_users': copied_users,
            'copy_errors': copy_errors[:10],
            'source_org_id': current_org_id
        },
        ip_address=request.remote_addr
    )

    return jsonify({
        'success': True,
        'organization': created,
        'org_id': created['id'],
        'copied_users': copied_users,
        'copy_errors': copy_errors,
        'organizations': db.get_user_orgs(user_id)
    }), 201


@app.route('/api/orgs/switch', methods=['POST'])
@login_required
def api_switch_org():
    """Switch active organization"""
    data = get_json_payload()
    if not data:
        return jsonify({'success': False, 'error': 'Payload inválido'}), 400
    org_id = data.get('org_id')
    # Normalize org_id to int if it's a numeric string
    if isinstance(org_id, str):
        org_id_str = org_id.strip()
        if org_id_str.isdigit():
            org_id = int(org_id_str)
    orgs = db.get_user_orgs(session['user']['id'])
    if not any(o['id'] == org_id for o in orgs):
        return jsonify({'success': False, 'error': 'Not a member'}), 403
    session['org_id'] = org_id
    for o in orgs:
        if o['id'] == org_id:
            session['org_name'] = o['name']
            session['org_plan'] = o['plan']
    return jsonify({'success': True, 'org_id': org_id})


@app.route('/api/org/details')
@login_required
def api_org_details():
    """Get current org details"""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403
    details = db.get_org_details(org_id)
    return jsonify({'success': True, 'organization': details})


@app.route('/api/org/ifood-config', methods=['GET', 'POST'])
@login_required
@org_owner_required
def api_org_ifood_config():
    """Get or update iFood credentials for current org"""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization'}), 403
    if request.method == 'GET':
        config = db.get_org_ifood_config(org_id) or {}
        client_id = config.get('client_id')
        client_secret = config.get('client_secret')
        merchants = config.get('merchants', []) or []

        org_has_credentials = bool(client_id)
        org_mode = 'none'
        if org_has_credentials:
            org_mode = 'mock' if str(client_id).strip().upper() == 'MOCK_DATA_MODE' else 'live'

        # Current org API instance (if initialized for this tenant)
        org_api = None
        if org_id in ORG_DATA:
            org_api = ORG_DATA[org_id].get('api')
        org_connected = bool(org_api)

        # Legacy global fallback (single-tenant/mock mode)
        legacy_client_id = (IFOOD_CONFIG or {}).get('client_id') if isinstance(IFOOD_CONFIG, dict) else None
        legacy_mode = 'none'
        if legacy_client_id:
            legacy_mode = 'mock' if str(legacy_client_id).strip().upper() == 'MOCK_DATA_MODE' else 'live'
        legacy_available = bool(IFOOD_API and legacy_client_id)

        using_legacy_fallback = (not org_has_credentials) and legacy_available
        connection_active = org_connected or using_legacy_fallback
        effective_mode = org_mode if org_mode != 'none' else legacy_mode
        source = 'org' if org_has_credentials else ('legacy' if using_legacy_fallback else 'none')

        masked = None
        if client_secret:
            s = client_secret
            masked = s[:4] + '****' + s[-4:] if len(s) > 8 else '****'
        return jsonify({
            'success': True,
            'config': {
                'client_id': client_id,
                'client_secret_masked': masked,
                'merchants': merchants,
                'has_credentials': org_has_credentials,
                'connection_active': bool(connection_active),
                'mode': effective_mode,
                'source': source,
                'using_legacy_fallback': bool(using_legacy_fallback),
                'use_mock_data': effective_mode == 'mock'
            }
        })
    # POST
    data = get_json_payload()
    if not data:
        return jsonify({'success': False, 'error': 'Payload inválido'}), 400
    db.update_org_ifood_config(org_id, data.get('client_id'), data.get('client_secret') if data.get('client_secret') != '****' else None, data.get('merchants'))
    db.log_action('org.ifood_config_updated', org_id=org_id, user_id=session['user']['id'], ip_address=request.remote_addr)
    # Reinitialize this org's API connection
    _init_org_ifood(org_id)
    return jsonify({'success': True})


@app.route('/api/org/invite', methods=['POST'])
@login_required
@org_owner_required
def api_create_invite():
    """Create a team invite"""
    org_id = get_current_org_id()
    if not org_id: return jsonify({'success': False}), 403
    data = get_json_payload()
    if not data:
        return jsonify({'success': False, 'error': 'Payload inválido'}), 400
    email = (data.get('email') or '').strip().lower()
    role = (data.get('role') or 'viewer').strip().lower()
    if role not in ('viewer', 'admin'):
        return jsonify({'success': False, 'error': 'Invalid role'}), 400
    if not email: return jsonify({'success': False, 'error': 'Email obrigatÃ³rio'}), 400
    token = db.create_invite(org_id, email, role, session['user']['id'])
    if not token: return jsonify({'success': False, 'error': 'Limite de membros atingido'}), 400
    invite_url = f"{request.host_url}invite/{token}"
    db.log_action('org.member_invited', org_id=org_id, user_id=session['user']['id'], details={'email': email, 'role': role}, ip_address=request.remote_addr)
    return jsonify({'success': True, 'invite_url': invite_url, 'token': token})


@app.route('/api/invite/<token>/accept', methods=['POST'])
@login_required
def api_accept_invite(token):
    """Accept a team invite"""
    result = db.accept_invite(token, session['user']['id'])
    if not result: return jsonify({'success': False, 'error': 'Convite invÃ¡lido ou expirado'}), 400
    session['org_id'] = result['org_id']
    return jsonify({'success': True, 'org_id': result['org_id'], 'redirect': '/dashboard'})


@app.route('/api/plans')
def api_get_plans():
    """Get available subscription plans"""
    include_free = str(request.args.get('include_free', '')).lower() in ('1', 'true', 'yes')
    plans = db.list_active_plans(include_free=include_free)
    if not plans:
        return jsonify({'success': True, 'plans': []})
    return jsonify({'success': True, 'plans': [enrich_plan_payload(p) for p in plans]})


@app.route('/api/org/subscription')
@login_required
def api_get_org_subscription():
    """Return current org subscription details, usage and available plans."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403

    current_subscription = db.get_org_subscription(org_id)
    plan_options = []

    user_limit = db.check_user_limit(org_id)
    restaurant_limit = db.check_restaurant_limit(org_id)

    for plan in db.list_active_plans(include_free=False):
        plan_payload = enrich_plan_payload(plan)
        users_ok = int(user_limit.get('current') or 0) <= int(plan_payload.get('max_users') or 0)
        restaurants_ok = int(restaurant_limit.get('current') or 0) <= int(plan_payload.get('max_restaurants') or 0)
        plan_payload['eligible'] = bool(users_ok and restaurants_ok)
        plan_payload['eligibility'] = {
            'users_ok': users_ok,
            'restaurants_ok': restaurants_ok
        }
        plan_options.append(plan_payload)

    history = db.list_org_subscription_history(org_id, limit=12)

    return jsonify({
        'success': True,
        'subscription': current_subscription,
        'plans': plan_options,
        'usage': {
            'users': user_limit,
            'restaurants': restaurant_limit
        },
        'history': history
    })


@app.route('/api/org/subscription', methods=['POST'])
@login_required
@org_owner_required
def api_change_org_subscription():
    """Change current organization plan and persist a subscription record."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403

    data = get_json_payload()
    target_plan = (data.get('plan') or '').strip().lower()
    if not target_plan:
        return jsonify({'success': False, 'error': 'plan is required'}), 400

    if target_plan not in PLAN_CATALOG_UI:
        return jsonify({'success': False, 'error': 'Unsupported plan'}), 400

    reason = (data.get('reason') or 'admin_portal').strip()[:120]
    change = db.change_org_plan(org_id, target_plan, changed_by=session['user']['id'], reason=reason)
    if not change.get('success'):
        status = 409 if str(change.get('error', '')).endswith('_limit_exceeded') else 400
        return jsonify(change), status

    session['org_plan'] = target_plan
    db.log_action(
        'org.plan_changed',
        org_id=org_id,
        user_id=session['user']['id'],
        details={
            'target_plan': target_plan,
            'previous_plan': change.get('previous_plan'),
            'reason': reason,
            'usage': change.get('usage', {})
        },
        ip_address=request.remote_addr
    )

    return jsonify({
        'success': True,
        'change': change,
        'subscription': db.get_org_subscription(org_id),
        'organization': db.get_org_details(org_id)
    })


@app.route('/api/org/limits')
@login_required
def api_org_limits():
    """Get current org usage vs limits"""
    org_id = get_current_org_id()
    if not org_id: return jsonify({'success': False}), 403
    limits = db.check_restaurant_limit(org_id)
    details = db.get_org_details(org_id)
    return jsonify({'success': True, 'restaurants': limits, 'plan': details.get('plan') if details else 'free', 'plan_display': details.get('plan_display') if details else 'Gratuito'})


@app.route('/api/org/users')
@login_required
def api_org_users():
    """Get users in current org"""
    org_id = get_current_org_id()
    if not org_id: return jsonify({'success': False}), 403
    users = db.get_org_users(org_id)
    return jsonify({'success': True, 'users': users})


@app.route('/api/org/users/candidates')
@login_required
@org_owner_required
def api_org_user_candidates():
    """List users that are not yet members of the current org."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403
    users = db.list_users_not_in_org(org_id)
    return jsonify({'success': True, 'users': users})


@app.route('/api/org/users/assign', methods=['POST'])
@login_required
@org_owner_required
def api_org_user_assign():
    """Assign an existing user to the current organization."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403

    data = get_json_payload()
    user_id = data.get('user_id')
    if isinstance(user_id, str) and user_id.strip().isdigit():
        user_id = int(user_id.strip())
    org_role = (data.get('org_role') or 'viewer').strip().lower()

    if not isinstance(user_id, int):
        return jsonify({'success': False, 'error': 'user_id is required'}), 400

    result = db.assign_user_to_org(org_id, user_id, org_role)
    if not result.get('success'):
        code = str(result.get('error') or '')
        if code in ('user_not_found', 'org_not_found'):
            status = 404
        elif code in ('already_member', 'user_limit_exceeded'):
            status = 409
        else:
            status = 400
        return jsonify(result), status

    db.log_action(
        'org.member_assigned',
        org_id=org_id,
        user_id=session['user']['id'],
        details={'assigned_user_id': user_id, 'org_role': result.get('org_role')},
        ip_address=request.remote_addr
    )
    return jsonify({'success': True, 'user_id': user_id, 'org_role': result.get('org_role')})


@app.route('/api/org/users/<int:user_id>/role', methods=['PATCH'])
@login_required
@org_owner_required
def api_org_user_role_update(user_id):
    """Update a member role inside current organization."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403

    data = get_json_payload()
    org_role = (data.get('org_role') or '').strip().lower()
    if not org_role:
        return jsonify({'success': False, 'error': 'org_role is required'}), 400

    result = db.update_org_member_role(org_id, user_id, org_role)
    if not result.get('success'):
        code = str(result.get('error') or '')
        status = 404 if code == 'member_not_found' else 400
        return jsonify(result), status

    db.log_action(
        'org.member_role_updated',
        org_id=org_id,
        user_id=session['user']['id'],
        details={'target_user_id': user_id, 'org_role': result.get('org_role')},
        ip_address=request.remote_addr
    )
    return jsonify({'success': True, 'user_id': user_id, 'org_role': result.get('org_role')})


@app.route('/api/org/users/<int:user_id>', methods=['DELETE'])
@login_required
@org_owner_required
def api_org_user_remove(user_id):
    """Remove a user from current organization (keeps account intact)."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False, 'error': 'No organization selected'}), 403

    if session.get('user', {}).get('id') == user_id:
        return jsonify({'success': False, 'error': 'Cannot remove your own membership from active org'}), 400

    result = db.remove_user_from_org(org_id, user_id)
    if not result.get('success'):
        code = str(result.get('error') or '')
        status = 404 if code == 'member_not_found' else 400
        return jsonify(result), status

    db.log_action(
        'org.member_removed',
        org_id=org_id,
        user_id=session['user']['id'],
        details={'removed_user_id': user_id},
        ip_address=request.remote_addr
    )
    return jsonify({'success': True, 'user_id': user_id})


@app.route('/api/org/capabilities')
@login_required
def api_org_capabilities():
    """Get tenant plan, enabled features and usage health."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({'success': False}), 403

    details = db.get_org_details(org_id) or {}
    features = details.get('features') or []
    if isinstance(features, str):
        try:
            features = json.loads(features)
        except Exception:
            features = []

    restaurant_limit = db.check_restaurant_limit(org_id)
    users = db.get_org_users(org_id)
    user_count = len(users)
    max_users = int(details.get('max_users') or 0)
    restaurant_current = int(restaurant_limit.get('current') or 0)
    restaurant_max = int(restaurant_limit.get('max') or 0)

    users_pct = (user_count / max_users * 100) if max_users > 0 else 0
    restaurants_pct = (restaurant_current / restaurant_max * 100) if restaurant_max > 0 else 0
    near_limit = users_pct >= 80 or restaurants_pct >= 80

    return jsonify({
        'success': True,
        'plan': details.get('plan', 'free'),
        'plan_display': details.get('plan_display', 'Gratuito'),
        'subscription': db.get_org_subscription(org_id),
        'features': features,
        'limits': {
            'users': {
                'current': user_count,
                'max': max_users,
                'usage_pct': round(users_pct, 1)
            },
            'restaurants': {
                'current': restaurant_current,
                'max': restaurant_max,
                'usage_pct': round(restaurants_pct, 1)
            }
        },
        'health': {
            'near_limit': near_limit,
            'users_near_limit': users_pct >= 80,
            'restaurants_near_limit': restaurants_pct >= 80
        }
    })


# ============================================================================
# API ROUTES - SAVED VIEWS
# ============================================================================

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


# ============================================================================
# API ROUTES - RESTAURANT DATA
# ============================================================================

@app.route('/api/restaurants')
@login_required
def api_restaurants():
    """Get list of all restaurants with optional month filtering and squad-based access control"""
    try:
        # Get month filter from query parameters
        month_filter = parse_month_filter(request.args.get('month', 'all'))
        if month_filter is None:
            return jsonify({'success': False, 'error': 'Invalid month filter'}), 400
        
        # Check in-memory cache first (avoids re-processing orders every request)
        org_id = get_current_org_id()
        cached = get_cached_restaurants(org_id, month_filter)
        if cached:
            return jsonify(cached)
        
        # Get user's allowed restaurants based on squad membership
        user = session.get('user', {})
        allowed_ids = get_user_allowed_restaurant_ids(user.get('id'), user.get('role'))
        
        # Return data without internal caches
        restaurants = []
        for r in get_current_org_restaurants():
            # Skip if user doesn't have access to this restaurant (squad filtering)
            if allowed_ids is not None and r['id'] not in allowed_ids:
                continue
            
            # If month filter is specified, reprocess with filtered orders
            if month_filter != 'all':
                # Get cached orders
                orders = r.get('_orders_cache', [])
                
                # Filter orders by month
                filtered_orders = filter_orders_by_month(orders, month_filter)
                
                # Reprocess restaurant data with filtered orders
                if filtered_orders or month_filter != 'all':
                    restaurant_name = r.get('name', 'Unknown Restaurant')
                    restaurant_manager = r.get('manager', 'Gerente')
                    # Get merchant details (reconstruct basic structure)
                    merchant_details = {
                        'id': r['id'],
                        'name': restaurant_name,
                        'merchantManager': {'name': restaurant_manager},
                        'address': {'neighborhood': r.get('neighborhood', 'Centro')}
                    }
                    
                    # Reprocess with filtered orders
                    from ifood_data_processor import IFoodDataProcessor
                    restaurant_data = IFoodDataProcessor.process_restaurant_data(
                        merchant_details,
                        filtered_orders,
                        None
                    )
                    
                    # Keep original name and manager
                    restaurant_data['name'] = restaurant_name
                    restaurant_data['manager'] = restaurant_manager
                    
                    # Remove internal caches before sending
                    restaurant = {k: v for k, v in restaurant_data.items() if not k.startswith('_')}
                    restaurants.append(restaurant)
                else:
                    # No orders for this month, return empty metrics
                    restaurant = {k: v for k, v in r.items() if not k.startswith('_')}
                    # Reset metrics to zero
                    if 'metrics' in restaurant:
                        for key in restaurant['metrics']:
                            if isinstance(restaurant['metrics'][key], (int, float)):
                                restaurant['metrics'][key] = 0
                            elif isinstance(restaurant['metrics'][key], dict):
                                for subkey in restaurant['metrics'][key]:
                                    restaurant['metrics'][key][subkey] = 0
                    restaurant['revenue'] = 0
                    restaurant['orders'] = 0
                    restaurant['ticket'] = 0
                    restaurant['trend'] = 0
                    restaurants.append(restaurant)
            else:
                # No filter, return all data
                restaurant = {k: v for k, v in r.items() if not k.startswith('_')}
                restaurants.append(restaurant)
        
        org_id = get_current_org_id()
        org_refresh = ORG_DATA.get(org_id, {}).get('last_refresh') if org_id else None
        
        result = {
            'success': True,
            'restaurants': restaurants,
            'last_refresh': (org_refresh or LAST_DATA_REFRESH).isoformat() if (org_refresh or LAST_DATA_REFRESH) else None,
            'month_filter': month_filter
        }
        
        # Cache the processed result
        set_cached_restaurants(org_id, month_filter, result)
        
        return jsonify(result)
    except Exception as e:
        print(f"Error getting restaurants: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500



@app.route('/api/restaurant/<restaurant_id>')
@login_required
def api_restaurant_detail(restaurant_id):
    """Get detailed data for a specific restaurant with optional date filtering"""
    try:
        # Get date filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Find restaurant in org data
        restaurant = None
        for r in get_current_org_restaurants():
            if r['id'] == restaurant_id:
                restaurant = r
                break
        
        if not restaurant:
            return jsonify({'success': False, 'error': 'Restaurant not found'}), 404
        
        # Get all orders from cache
        all_orders = restaurant.get('_orders_cache', [])
        
        # Filter orders by date range if provided
        filtered_orders = all_orders
        if start_date or end_date:
            filtered_orders = []
            for order in all_orders:
                try:
                    order_date_str = order.get('createdAt', '')
                    if order_date_str:
                        # Parse order date
                        order_date = datetime.fromisoformat(order_date_str.replace('Z', '+00:00')).date()
                        
                        # Check date range
                        include_order = True
                        if start_date:
                            start = datetime.strptime(start_date, '%Y-%m-%d').date()
                            if order_date < start:
                                include_order = False
                        if end_date:
                            end = datetime.strptime(end_date, '%Y-%m-%d').date()
                            if order_date > end:
                                include_order = False
                        
                        if include_order:
                            filtered_orders.append(order)
                except:
                    continue
        
        # Reprocess restaurant data with filtered orders if date filtering is applied
        if (start_date or end_date) and filtered_orders:
            # Get merchant details
            merchant_details = {
                'id': restaurant_id,
                'name': restaurant.get('name', 'Unknown'),
                'merchantManager': {'name': restaurant.get('manager', 'Gerente')}
            }
            
            # Reprocess with filtered orders
            response_data = IFoodDataProcessor.process_restaurant_data(
                merchant_details,
                filtered_orders,
                None
            )
            
            # Keep original name and manager
            response_data['name'] = restaurant['name']
            response_data['manager'] = restaurant['manager']
        else:
            # Clean data for response (no date filtering)
            response_data = {k: v for k, v in restaurant.items() if not k.startswith('_')}
        
        # Generate chart data from filtered orders
        chart_data = {}
        interruptions = []
        
        api = get_current_org_api()
        if api:
            # Get interruptions
            try:
                interruptions = api.get_interruptions(restaurant_id) or []
            except:
                pass
        
        # Generate charts from filtered orders
        orders_for_charts = filtered_orders if (start_date or end_date) else all_orders
        top_n = request.args.get('top_n', default=10, type=int)
        top_n = max(1, min(top_n or 10, 50))
        menu_performance = IFoodDataProcessor.calculate_menu_item_performance(orders_for_charts, top_n=top_n)

        if orders_for_charts:
            if hasattr(IFoodDataProcessor, 'generate_charts_data_with_interruptions'):
                chart_data = IFoodDataProcessor.generate_charts_data_with_interruptions(
                    orders_for_charts,
                    interruptions
                )
            else:
                chart_data = IFoodDataProcessor.generate_charts_data(orders_for_charts)
                chart_data['interruptions'] = []
        
        # Extract reviews from orders
        reviews_list = []
        rating_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        for order in orders_for_charts:
            fb = order.get('feedback')
            if fb and fb.get('rating'):
                r = fb['rating']
                if r in rating_counts:
                    rating_counts[r] += 1
                reviews_list.append({
                    'rating': r,
                    'comment': fb.get('comment'),
                    'compliments': fb.get('compliments', []),
                    'complaints': fb.get('complaints', []),
                    'customer_name': order.get('customer', {}).get('name', 'Cliente'),
                    'date': order.get('createdAt'),
                    'order_id': order.get('displayId', order.get('id', ''))
                })

        total_reviews = sum(rating_counts.values())
        avg_review_rating = round(
            sum(k * v for k, v in rating_counts.items()) / total_reviews, 1
        ) if total_reviews else 0

        return jsonify({
            'success': True,
            'restaurant': response_data,
            'charts': chart_data,
            'menu_performance': menu_performance,
            'interruptions': interruptions,
            'reviews': {
                'average_rating': avg_review_rating,
                'total_reviews': total_reviews,
                'rating_distribution': rating_counts,
                'items': sorted(reviews_list, key=lambda x: x['date'] or '', reverse=True)
            },
            'filter': {
                'start_date': start_date,
                'end_date': end_date,
                'total_orders_filtered': len(filtered_orders) if (start_date or end_date) else len(all_orders)
            }
        })

    except Exception as e:
        print(f"Error getting restaurant detail: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/restaurant/<restaurant_id>/orders')
@login_required
def api_restaurant_orders(restaurant_id):
    """Get orders for a specific restaurant"""
    try:
        # Find restaurant
        restaurant = None
        for r in get_current_org_restaurants():
            if r['id'] == restaurant_id:
                restaurant = r
                break
        
        if not restaurant:
            return jsonify({'success': False, 'error': 'Restaurant not found'}), 404
        
        # Get parameters
        try:
            per_page = int(request.args.get('per_page', 100))
            page = int(request.args.get('page', 1))
        except ValueError:
            return jsonify({'success': False, 'error': 'Invalid pagination parameters'}), 400

        per_page = max(1, min(per_page, 500))
        page = max(1, page)
        status = request.args.get('status')
        
        # Get orders from cache
        orders = restaurant.get('_orders_cache', [])
        
        # Filter by status if provided
        if status:
            orders = [o for o in orders if o.get('orderStatus') == status]
        
        # Paginate
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_orders = orders[start_idx:end_idx]
        
        return jsonify({
            'success': True,
            'orders': paginated_orders,
            'total': len(orders),
            'page': page,
            'per_page': per_page,
            'total_pages': (len(orders) + per_page - 1) // per_page
        })
        
    except Exception as e:
        print(f"Error getting restaurant orders: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/restaurant/<restaurant_id>/menu-performance')
@login_required
def api_restaurant_menu_performance(restaurant_id):
    """Get menu item performance for a specific restaurant."""
    try:
        restaurant = None
        for r in get_current_org_restaurants():
            if r['id'] == restaurant_id:
                restaurant = r
                break

        if not restaurant:
            return jsonify({'success': False, 'error': 'Restaurant not found'}), 404

        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        top_n = request.args.get('top_n', default=10, type=int)
        top_n = max(1, min(top_n or 10, 50))

        orders = restaurant.get('_orders_cache', [])
        if start_date or end_date:
            filtered = []
            for order in orders:
                try:
                    created_at = order.get('createdAt', '')
                    if not created_at:
                        continue
                    order_date = datetime.fromisoformat(str(created_at).replace('Z', '+00:00')).date()
                    include_order = True
                    if start_date:
                        if order_date < datetime.strptime(start_date, '%Y-%m-%d').date():
                            include_order = False
                    if end_date:
                        if order_date > datetime.strptime(end_date, '%Y-%m-%d').date():
                            include_order = False
                    if include_order:
                        filtered.append(order)
                except Exception:
                    continue
            orders = filtered

        performance = IFoodDataProcessor.calculate_menu_item_performance(orders, top_n=top_n)
        return jsonify({
            'success': True,
            'restaurant_id': restaurant_id,
            'menu_performance': performance,
            'filter': {
                'start_date': start_date,
                'end_date': end_date,
                'top_n': top_n,
                'orders_considered': len(orders)
            }
        })
    except Exception as e:
        print(f"Error getting menu performance: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500





# ============================================================================
# API ROUTES - RESTAURANT INTERRUPTIONS
# ============================================================================

@app.route('/api/restaurant/<restaurant_id>/interruptions')
@login_required
def api_restaurant_interruptions(restaurant_id):
    """Get interruptions for a specific restaurant"""
    try:
        api = get_current_org_api()
        if not api:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
        
        # Get interruptions
        interruptions = api.get_interruptions(restaurant_id)
        
        return jsonify({
            'success': True,
            'interruptions': interruptions or []
        })
        
    except Exception as e:
        print(f"Error getting interruptions: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/restaurant/<restaurant_id>/status')
@login_required
def api_restaurant_status(restaurant_id):
    """Get operational status for a specific restaurant"""
    try:
        api = get_current_org_api()
        if not api:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
        
        # Get status
        status = api.get_merchant_status(restaurant_id)
        
        return jsonify({
            'success': True,
            'status': status or {'state': 'UNKNOWN', 'message': 'Unable to fetch status'}
        })
        
    except Exception as e:
        print(f"Error getting status: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/restaurant/<restaurant_id>/interruptions', methods=['POST'])
@admin_required
def api_create_interruption(restaurant_id):
    """Create a new interruption (close store temporarily)"""
    try:
        api = get_current_org_api()
        if not api:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
        
        data = get_json_payload()
        start = data.get('start')
        end = data.get('end')
        description = data.get('description', '')
        
        if not start or not end:
            return jsonify({'success': False, 'error': 'Start and end times required'}), 400
        
        # Create interruption
        result = api.create_interruption(restaurant_id, start, end, description)
        
        if result:
            return jsonify({
                'success': True,
                'interruption': result,
                'message': 'Interruption created successfully'
            })
        else:
            return jsonify({'success': False, 'error': 'Failed to create interruption'}), 500
        
    except Exception as e:
        print(f"Error creating interruption: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/restaurant/<restaurant_id>/interruptions/<interruption_id>', methods=['DELETE'])
@admin_required
def api_delete_interruption(restaurant_id, interruption_id):
    """Delete an interruption (reopen store)"""
    try:
        api = get_current_org_api()
        if not api:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
        
        # Delete interruption
        success = api.delete_interruption(restaurant_id, interruption_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Interruption removed successfully'
            })
        else:
            return jsonify({'success': False, 'error': 'Failed to remove interruption'}), 500
        
    except Exception as e:
        print(f"Error deleting interruption: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500





@app.route('/api/refresh-data', methods=['POST'])
@admin_required
def api_refresh_data():
    """Refresh restaurant data from iFood API (now uses background thread)"""
    try:
        has_org_api = any(od.get('api') for od in ORG_DATA.values())
        if not IFOOD_API and not has_org_api:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
        
        if bg_refresher.is_refreshing:
            return jsonify({'success': True, 'message': 'Refresh already in progress', 'status': 'refreshing'})
        
        # Trigger async refresh
        threading.Thread(target=bg_refresher.refresh_now, daemon=True).start()
        
        return jsonify({
            'success': True,
            'message': 'Refresh started in background',
            'status': 'started',
            'last_refresh': LAST_DATA_REFRESH.isoformat() if LAST_DATA_REFRESH else None
        })
        
    except Exception as e:
        print(f"Error refreshing data: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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
    return jsonify({
        'success': True,
        'is_refreshing': bg_refresher.is_refreshing,
        'last_refresh': get_current_org_last_refresh().isoformat() if get_current_org_last_refresh() else None,
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
    restaurants = []
    for r in get_current_org_restaurants():
        orders = r.get('_orders_cache', [])
        if month_filter != 'all':
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
    summary['last_refresh'] = get_current_org_last_refresh().isoformat() if get_current_org_last_refresh() else None
    return jsonify({'success': True, 'summary': summary, 'month_filter': month_filter})


@app.route('/api/restaurants/export.csv')
@login_required
@require_feature('export')
def api_restaurants_export_csv():
    """Export visible restaurants as CSV."""
    month_filter = parse_month_filter(request.args.get('month', 'all'))
    if month_filter is None:
        return jsonify({'success': False, 'error': 'Invalid month filter'}), 400

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['restaurant_id', 'restaurant_name', 'manager', 'orders', 'ticket_medio', 'valor_bruto', 'liquido'])

    for r in get_current_org_restaurants():
        metrics = r.get('metrics', {})
        writer.writerow([
            r.get('id', ''),
            r.get('name', ''),
            r.get('manager', ''),
            metrics.get('vendas', 0),
            metrics.get('ticket_medio', 0),
            metrics.get('valor_bruto', 0),
            metrics.get('liquido', 0)
        ])

    filename = f"restaurants-{month_filter}.csv"
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )


@app.route('/api/health')
def api_health():
    """Health probe."""
    conn = db.get_connection()
    ok = bool(conn)
    if conn:
        conn.close()
    return jsonify({
        'success': ok,
        'status': 'ok' if ok else 'degraded',
        'uptime_seconds': int((datetime.utcnow() - APP_STARTED_AT).total_seconds()),
        'restaurants_loaded': len(get_current_org_restaurants()),
        'last_refresh': get_current_org_last_refresh().isoformat() if get_current_org_last_refresh() else None
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
            targets = [r for r in get_current_org_restaurants() if r['id'] == restaurant_id]
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
                'restaurant_id': restaurant['id'],
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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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
                if r['id'] == restaurant_id:
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
        
    except Exception as e:
        print(f"Error in daily comparison: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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
            created = order.get('createdAt', '')
            if created:
                order_date = datetime.fromisoformat(created.replace('Z', '+00:00')).date()
                if start_d <= order_date <= end_d:
                    filtered.append(order)
        except:
            continue
    return filtered


def _calculate_period_metrics(orders):
    """Calculate key metrics for a set of orders"""
    concluded = [o for o in orders if o.get('orderStatus') == 'CONCLUDED']
    cancelled = [o for o in orders if o.get('orderStatus') == 'CANCELLED']
    
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
            created = order.get('createdAt', '')
            if created:
                d = datetime.fromisoformat(created.replace('Z', '+00:00')).date().isoformat()
                if d in days:
                    if order.get('orderStatus') == 'CONCLUDED':
                        days[d]['revenue'] += float(order.get('totalPrice', 0) or 0)
                        days[d]['orders'] += 1
                    elif order.get('orderStatus') == 'CANCELLED':
                        days[d]['cancelled'] += 1
        except:
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
                {'merchant_id': m.get('merchant_id'), 'name': m.get('name'), 'manager': m.get('manager')}
                for m in IFOOD_CONFIG.get('merchants', [])
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
        
        merchant_id = data.get('merchant_id')
        name = data.get('name')
        manager = data.get('manager', 'Gerente')
        
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
                if m.get('merchant_id') == merchant_id:
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
            if m.get('merchant_id') == merchant_id:
                return jsonify({'success': False, 'error': 'Merchant already exists'}), 400
        merchants.append(merchant_payload)
        db.update_org_ifood_config(org_id, merchants=merchants)
        _init_org_ifood(org_id)
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
        if is_platform_admin_user(session.get('user', {})):
            if 'merchants' not in IFOOD_CONFIG:
                return jsonify({'success': False, 'error': 'No merchants configured'}), 404

            original_count = len(IFOOD_CONFIG['merchants'])
            IFOOD_CONFIG['merchants'] = [
                m for m in IFOOD_CONFIG['merchants'] 
                if m.get('merchant_id') != merchant_id
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
        merchants = [m for m in merchants if m.get('merchant_id') != merchant_id]
        if len(merchants) == original_count:
            return jsonify({'success': False, 'error': 'Merchant not found'}), 404
        db.update_org_ifood_config(org_id, merchants=merchants)
        _init_org_ifood(org_id)
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
        return jsonify({'success': False, 'error': str(e)}), 500


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
        return jsonify({'success': False, 'error': str(e)}), 500


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
            if r['id'] == restaurant_id:
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
        RESTAURANTS_DATA = [r for r in get_current_org_restaurants() if r['id'] != restaurant_id]
        
        return jsonify({
            'success': True,
            'message': f'Restaurant {restaurant.get("name")} cancelled',
            'cancelled': cancelled_entry
        })
        
    except Exception as e:
        print(f"Error cancelling restaurant: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


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
        return jsonify({'success': False, 'error': str(e)}), 500


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
        org_role = (data.get('org_role') or ('admin' if role == 'admin' else 'viewer')).strip().lower()
        org_id = get_current_org_id()
        
        if not all([username, password, full_name]):
            return jsonify({
                'success': False,
                'error': 'Username, password, and full name required'
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


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@admin_required
def api_delete_user(user_id):
    """Delete a user account (platform admin only)."""
    try:
        if not is_platform_admin_user(session.get('user', {})):
            return jsonify({'success': False, 'error': 'Platform admin access required'}), 403

        # Prevent self-deletion
        if session['user'].get('id') == user_id:
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
    cursor.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        LIMIT 1
    """, (table_name, column_name))
    return cursor.fetchone() is not None


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
        return jsonify({'success': False, 'error': str(e)}), 500


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
        return jsonify({'success': False, 'error': str(e)}), 500


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
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# SQUADS API ENDPOINTS
# ============================================================================

def get_user_allowed_restaurant_ids(user_id, user_role):
    """Helper function to get allowed restaurant IDs for a user based on squad membership"""
    if user_role == 'admin':
        return None  # None means all restaurants allowed

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

        # Check which schema we have by inspecting columns
        cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'squads'
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in cursor.fetchall()]

        # Determine schema type
        has_old_schema = 'squad_id' in columns and 'leader' in columns
        has_org_id = 'org_id' in columns

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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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

        # Create squad - check which schema we have
        cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'squads'
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in cursor.fetchall()]
        has_old_schema = 'squad_id' in columns and 'leader' in columns

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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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

        added_count = 0
        for restaurant_id in restaurant_ids:
            # Find restaurant name from current org data
            restaurant_name = 'Unknown'
            for r in get_current_org_restaurants():
                if r['id'] == restaurant_id:
                    restaurant_name = r.get('name', 'Unknown')
                    break

            try:
                cursor.execute("""
                    INSERT INTO squad_restaurants (squad_id, restaurant_id, restaurant_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (squad_id, restaurant_id) DO NOTHING
                """, (squad_id, restaurant_id, restaurant_name))
                if cursor.rowcount > 0:
                    added_count += 1
            except Exception as e:
                print(f"Error adding restaurant {restaurant_id}: {e}")

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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# GROUPS (CLIENT GROUPS) ROUTES
# ============================================================================

# Page route for grupos
@app.route('/grupos')
@login_required
def grupos_page():
    """Serve client groups management page"""
    grupos_file = DASHBOARD_OUTPUT / 'grupos.html'
    if grupos_file.exists():
        return send_file(grupos_file)
    return "Grupos page not found", 404


@app.route('/grupos/comparativo')
@login_required
def grupos_comparativo_page():
    """Serve multi-store comparison page for groups."""
    comp_file = DASHBOARD_OUTPUT / 'grupos_comparativo.html'
    if comp_file.exists():
        return send_file(comp_file)
    return "Grupos comparativo page not found", 404


# Public group page (no auth required)
@app.route('/grupo/<slug>')
def public_group_page(slug):
    """Serve public group dashboard - NO AUTH REQUIRED"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get group by slug
        has_group_org = _table_has_org_id(cursor, 'client_groups')
        if has_group_org:
            cursor.execute("""
                SELECT id, name, slug, active, org_id
                FROM client_groups
                WHERE slug = %s AND active = true
            """, (slug,))
        else:
            cursor.execute("""
                SELECT id, name, slug, active, NULL::INTEGER as org_id
                FROM client_groups
                WHERE slug = %s AND active = true
            """, (slug,))
        
        group = cursor.fetchone()
        
        if not group:
            cursor.close()
            conn.close()
            return "Group not found", 404
        
        group_id = group[0]
        group_name = group[1]
        group_org_id = group[4]
        
        # Get stores in this group
        cursor.execute("""
            SELECT store_id, store_name
            FROM group_stores
            WHERE group_id = %s
            ORDER BY store_name
        """, (group_id,))
        
        store_rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Resolve store data using the group's organization when available.
        source_restaurants = get_current_org_restaurants()
        if group_org_id:
            source_restaurants = ORG_DATA.get(group_org_id, {}).get('restaurants') or []
            if not source_restaurants:
                cached_org_data = db.load_org_data_cache(group_org_id, 'restaurants', max_age_hours=12)
                if isinstance(cached_org_data, list):
                    source_restaurants = cached_org_data

        # Get store data from the resolved org source
        stores_data = []
        for store_row in store_rows:
            store_id = store_row[0]
            store_name = store_row[1]
            
            # Find in resolved org data
            for r in source_restaurants:
                if r['id'] == store_id:
                    # Clean data (remove internal caches)
                    store_data = {k: v for k, v in r.items() if not k.startswith('_')}
                    stores_data.append(store_data)
                    break
            else:
                # Store not found in data, add placeholder
                stores_data.append({
                    'id': store_id,
                    'name': store_name,
                    'metrics': {}
                })
        
        # Prepare group data
        group_data = {
            'id': group_id,
            'name': group_name,
            'slug': slug,
            'stores': stores_data
        }
        
        # Load template
        template_file = DASHBOARD_OUTPUT / 'grupo_public.html'
        if template_file.exists():
            with open(template_file, 'r', encoding='utf-8') as f:
                template = f.read()
            
            # Replace placeholders
            rendered = template.replace('{{group_name}}', escape_html_text(group_name))
            rendered = rendered.replace('{{group_initial}}', escape_html_text(group_name[0].upper() if group_name else 'G'))
            rendered = rendered.replace('{{group_data}}', safe_json_for_script(group_data))
            
            return Response(rendered, mimetype='text/html')
        
        return "Template not found", 404
        
    except Exception as e:
        print(f"Error loading public group: {e}")
        traceback.print_exc()
        return "Error loading group", 500


# API: Get all groups
@app.route('/api/groups', methods=['GET'])
@login_required
def api_get_groups():
    """Get all client groups with their stores"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        conn = db.get_connection()
        cursor = conn.cursor()
        has_org_id = _table_has_org_id(cursor, 'client_groups')
        
        # Get all groups
        if has_org_id:
            cursor.execute("""
                SELECT id, name, slug, active, created_by, created_at
                FROM client_groups
                WHERE org_id = %s
                ORDER BY name
            """, (org_id,))
        else:
            cursor.execute("""
                SELECT id, name, slug, active, created_by, created_at
                FROM client_groups
                ORDER BY name
            """)
        
        groups_raw = cursor.fetchall()
        groups = []
        
        for g in groups_raw:
            group_id = g[0]
            
            # Get stores for this group
            cursor.execute("""
                SELECT store_id, store_name
                FROM group_stores
                WHERE group_id = %s
                ORDER BY store_name
            """, (group_id,))
            
            stores = [{'id': s[0], 'name': s[1]} for s in cursor.fetchall()]
            
            groups.append({
                'id': group_id,
                'name': g[1],
                'slug': g[2],
                'active': g[3],
                'created_by': g[4],
                'created_at': g[5].isoformat() if g[5] else None,
                'stores': stores
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'groups': groups
        })
        
    except Exception as e:
        print(f"Error getting groups: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/groups/<int:group_id>/comparison', methods=['GET'])
@login_required
def api_group_comparison(group_id):
    """Compare stores inside a client group over a date range."""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        start_str = request.args.get('start_date')
        end_str = request.args.get('end_date')
        sort_by = request.args.get('sort_by', 'revenue')
        sort_dir = request.args.get('sort_dir', 'desc')

        end_dt = datetime.strptime(end_str, '%Y-%m-%d') if end_str else datetime.now()
        start_dt = datetime.strptime(start_str, '%Y-%m-%d') if start_str else (end_dt - timedelta(days=30))
        if start_dt > end_dt:
            return jsonify({'success': False, 'error': 'start_date must be before end_date'}), 400

        conn = db.get_connection()
        cursor = conn.cursor()
        has_org_id = _table_has_org_id(cursor, 'client_groups')

        if has_org_id:
            cursor.execute("""
                SELECT id, name, slug, active
                FROM client_groups
                WHERE id = %s AND org_id = %s
            """, (group_id, org_id))
        else:
            cursor.execute("""
                SELECT id, name, slug, active
                FROM client_groups
                WHERE id = %s
            """, (group_id,))
        group_row = cursor.fetchone()
        if not group_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Grupo nÃ£o encontrado'}), 404

        cursor.execute("""
            SELECT store_id, store_name
            FROM group_stores
            WHERE group_id = %s
            ORDER BY store_name
        """, (group_id,))
        store_rows = cursor.fetchall()
        cursor.close()
        conn.close()

        user = session.get('user', {})
        allowed_ids = get_user_allowed_restaurant_ids(user.get('id'), user.get('role'))

        restaurants_map = {r.get('id'): r for r in get_current_org_restaurants()}
        comparison_rows = []
        group_orders = []

        for store_id, store_name in store_rows:
            if allowed_ids is not None and store_id not in allowed_ids:
                continue

            r = restaurants_map.get(store_id)
            if not r:
                comparison_rows.append({
                    'store_id': store_id,
                    'store_name': store_name,
                    'manager': None,
                    'available': False,
                    'metrics': _calculate_period_metrics([])
                })
                continue

            orders = r.get('_orders_cache', [])
            filtered_orders = _filter_orders_by_date(orders, start_dt, end_dt)
            metrics = _calculate_period_metrics(filtered_orders)
            group_orders.extend(filtered_orders)

            comparison_rows.append({
                'store_id': store_id,
                'store_name': r.get('name', store_name),
                'manager': r.get('manager'),
                'available': True,
                'metrics': metrics
            })

        if not comparison_rows:
            return jsonify({
                'success': True,
                'group': {
                    'id': group_row[0],
                    'name': group_row[1],
                    'slug': group_row[2],
                    'active': group_row[3]
                },
                'period': {
                    'start_date': start_dt.strftime('%Y-%m-%d'),
                    'end_date': end_dt.strftime('%Y-%m-%d')
                },
                'summary': _calculate_period_metrics([]),
                'stores': []
            })

        key_map = {
            'revenue': lambda x: x['metrics'].get('revenue', 0),
            'orders': lambda x: x['metrics'].get('orders', 0),
            'ticket': lambda x: x['metrics'].get('ticket', 0),
            'cancel_rate': lambda x: x['metrics'].get('cancel_rate', 0),
            'avg_rating': lambda x: x['metrics'].get('avg_rating', 0)
        }
        sort_fn = key_map.get(sort_by, key_map['revenue'])
        reverse = sort_dir != 'asc'
        comparison_rows.sort(key=sort_fn, reverse=reverse)

        total_revenue = sum(s['metrics'].get('revenue', 0) for s in comparison_rows)
        for i, row in enumerate(comparison_rows, start=1):
            row['rank'] = i
            rev = row['metrics'].get('revenue', 0)
            row['metrics']['revenue_share'] = round((rev / total_revenue * 100) if total_revenue > 0 else 0, 2)

        summary = _calculate_period_metrics(group_orders)
        best_revenue = max(comparison_rows, key=lambda x: x['metrics'].get('revenue', 0))
        best_orders = max(comparison_rows, key=lambda x: x['metrics'].get('orders', 0))
        lowest_cancel = min(comparison_rows, key=lambda x: x['metrics'].get('cancel_rate', 100))

        return jsonify({
            'success': True,
            'group': {
                'id': group_row[0],
                'name': group_row[1],
                'slug': group_row[2],
                'active': group_row[3]
            },
            'period': {
                'start_date': start_dt.strftime('%Y-%m-%d'),
                'end_date': end_dt.strftime('%Y-%m-%d')
            },
            'sort': {'by': sort_by, 'dir': sort_dir},
            'summary': summary,
            'benchmarks': {
                'best_revenue_store': {'id': best_revenue['store_id'], 'name': best_revenue['store_name']},
                'best_orders_store': {'id': best_orders['store_id'], 'name': best_orders['store_name']},
                'lowest_cancel_store': {'id': lowest_cancel['store_id'], 'name': lowest_cancel['store_name']}
            },
            'stores': comparison_rows
        })

    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        print(f"Error group comparison: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# API: Create group
@app.route('/api/groups', methods=['POST'])
@admin_required
def api_create_group():
    """Create a new client group"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        name = data.get('name', '').strip()
        slug = data.get('slug', '').strip()
        store_ids = data.get('store_ids', [])
        
        if not name:
            return jsonify({'success': False, 'error': 'Nome Ã© obrigatÃ³rio'}), 400
        
        # Generate slug if not provided
        if not slug:
            import re
            slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
        
        # Ensure slug is valid
        import re
        slug = re.sub(r'[^a-z0-9-]', '', slug.lower())
        
        created_by = session.get('user', {}).get('username', 'Unknown')
        
        conn = db.get_connection()
        cursor = conn.cursor()
        has_org_id = _table_has_org_id(cursor, 'client_groups')
        
        # Check if slug exists
        cursor.execute("SELECT id FROM client_groups WHERE slug = %s", (slug,))
        if cursor.fetchone():
            # Add random suffix
            import random
            slug = f"{slug}-{random.randint(100, 999)}"
        
        # Create group
        if has_org_id:
            cursor.execute("""
                INSERT INTO client_groups (name, slug, created_by, org_id)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (name, slug, created_by, org_id))
        else:
            cursor.execute("""
                INSERT INTO client_groups (name, slug, created_by)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (name, slug, created_by))
        
        group_id = cursor.fetchone()[0]
        
        # Add stores
        for store_id in store_ids:
            # Get store name from RESTAURANTS_DATA
            store_name = store_id
            for r in get_current_org_restaurants():
                if r['id'] == store_id:
                    store_name = r.get('name', store_id)
                    break
            
            cursor.execute("""
                INSERT INTO group_stores (group_id, store_id, store_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (group_id, store_id) DO NOTHING
            """, (group_id, store_id, store_name))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Grupo criado com sucesso',
            'group_id': group_id,
            'slug': slug
        })
        
    except Exception as e:
        print(f"Error creating group: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# API: Update group
@app.route('/api/groups/<int:group_id>', methods=['PUT'])
@admin_required
def api_update_group(group_id):
    """Update a client group"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        name = data.get('name', '').strip()
        slug = data.get('slug', '').strip()
        store_ids = data.get('store_ids', [])
        active = data.get('active', True)
        
        if not name:
            return jsonify({'success': False, 'error': 'Nome Ã© obrigatÃ³rio'}), 400
        
        conn = db.get_connection()
        cursor = conn.cursor()
        has_org_id = _table_has_org_id(cursor, 'client_groups')
        
        # Check if group exists
        if has_org_id:
            cursor.execute("SELECT id, slug FROM client_groups WHERE id = %s AND org_id = %s", (group_id, org_id))
        else:
            cursor.execute("SELECT id, slug FROM client_groups WHERE id = %s", (group_id,))
        existing = cursor.fetchone()
        if not existing:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Grupo nÃ£o encontrado'}), 404
        
        # If slug changed, validate it
        if slug and slug != existing[1]:
            import re
            slug = re.sub(r'[^a-z0-9-]', '', slug.lower())
            
            if has_org_id:
                cursor.execute(
                    "SELECT id FROM client_groups WHERE slug = %s AND id != %s AND org_id = %s",
                    (slug, group_id, org_id)
                )
            else:
                cursor.execute("SELECT id FROM client_groups WHERE slug = %s AND id != %s", (slug, group_id))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Slug jÃ¡ existe'}), 400
        else:
            slug = existing[1]
        
        # Update group
        if has_org_id:
            cursor.execute("""
                UPDATE client_groups
                SET name = %s, slug = %s, active = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s AND org_id = %s
            """, (name, slug, active, group_id, org_id))
        else:
            cursor.execute("""
                UPDATE client_groups
                SET name = %s, slug = %s, active = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (name, slug, active, group_id))
        
        # Update stores - remove all and re-add
        cursor.execute("DELETE FROM group_stores WHERE group_id = %s", (group_id,))
        
        for store_id in store_ids:
            store_name = store_id
            for r in get_current_org_restaurants():
                if r['id'] == store_id:
                    store_name = r.get('name', store_id)
                    break
            
            cursor.execute("""
                INSERT INTO group_stores (group_id, store_id, store_name)
                VALUES (%s, %s, %s)
            """, (group_id, store_id, store_name))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Grupo atualizado com sucesso'
        })
        
    except Exception as e:
        print(f"Error updating group: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# API: Delete group
@app.route('/api/groups/<int:group_id>', methods=['DELETE'])
@admin_required
def api_delete_group(group_id):
    """Delete a client group"""
    try:
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        conn = db.get_connection()
        cursor = conn.cursor()
        has_org_id = _table_has_org_id(cursor, 'client_groups')
        
        # Check if group exists
        if has_org_id:
            cursor.execute("SELECT name FROM client_groups WHERE id = %s AND org_id = %s", (group_id, org_id))
        else:
            cursor.execute("SELECT name FROM client_groups WHERE id = %s", (group_id,))
        result = cursor.fetchone()
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Grupo nÃ£o encontrado'}), 404
        
        group_name = result[0]
        
        # Delete group (cascade will delete stores)
        if has_org_id:
            cursor.execute("DELETE FROM client_groups WHERE id = %s AND org_id = %s", (group_id, org_id))
        else:
            cursor.execute("DELETE FROM client_groups WHERE id = %s", (group_id,))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Grupo "{group_name}" excluÃ­do com sucesso'
        })
        
    except Exception as e:
        print(f"Error deleting group: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/user/allowed-restaurants')
@login_required
def api_user_allowed_restaurants():
    """Get list of restaurant IDs the current user can access based on squad membership"""
    try:
        user = session.get('user', {})
        user_id = user.get('id')
        user_role = user.get('role')
        
        # Admins see all restaurants
        if user_role == 'admin':
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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


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
    print(f"âŒ 404 Error: {request.url}")
    
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
        <p><a href="/login">â† Back to Login</a></p>
    </body>
    </html>
    """, 404


@app.errorhandler(500)
def internal_error(e):
    """Custom 500 error handler"""
    print("âŒ 500 Error")
    traceback.print_exc()
    
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
        <p><a href="/login" style="color: #ef4444;">â† Back to Login</a></p>
    </body>
    </html>
    """, 500


# ============================================================================
# INITIALIZATION
# ============================================================================

def check_setup():
    """Check if setup is correct"""
    print("\n" + "="*60)
    print("ðŸ” Checking Setup...")
    print("="*60)
    
    issues = []
    
    # Check dashboard_output directory
    if not DASHBOARD_OUTPUT.exists():
        issues.append(f"âŒ dashboard_output/ directory not found at {DASHBOARD_OUTPUT}")
        # Try to create it
        try:
            DASHBOARD_OUTPUT.mkdir(parents=True, exist_ok=True)
            print(f"   Created dashboard_output/ directory")
        except:
            pass
    else:
        print(f"âœ… dashboard_output/ directory exists")
        
        # Check for HTML files
        required_files = ['login.html', 'index.html']
        optional_files = ['admin.html', 'restaurant_template.html']
        
        for filename in required_files:
            file_path = DASHBOARD_OUTPUT / filename
            if file_path.exists():
                print(f"   âœ… {filename}")
            else:
                issues.append(f"âŒ Missing required: dashboard_output/{filename}")
        
        for filename in optional_files:
            file_path = DASHBOARD_OUTPUT / filename
            if file_path.exists():
                print(f"   âœ… {filename}")
            else:
                print(f"   âš ï¸ Optional: {filename} not found")
    
    # Check iFood config
    if not CONFIG_FILE.exists():
        print(f"âš ï¸  iFood config not found (will be created)")
    else:
        print(f"âœ… iFood config file exists")
    
    if issues:
        print("\nâš ï¸  Issues found:")
        for issue in issues:
            print(f"   {issue}")
        print()
    else:
        print("\nâœ… All checks passed!")
    
    return len([i for i in issues if i.startswith('âŒ')]) == 0


def initialize_database():
    """Initialize database tables and create default users if needed"""
    print("\nInitializing database...")
    try:
        db.setup_tables()
        
        # Create hidden stores table
        conn = db.get_connection()
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
        for tbl in ('hidden_stores', 'squads', 'client_groups'):
            try:
                cursor.execute(f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_name = '{tbl}' AND column_name = 'org_id'
                        ) THEN
                            ALTER TABLE {tbl}
                            ADD COLUMN org_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE;
                        END IF;
                    END $$;
                """)
            except Exception as migration_error:
                print(f"   Note: org_id migration for {tbl}: {migration_error}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        users = db.get_all_users()
        if not users:
            bootstrap_defaults = str(os.environ.get('BOOTSTRAP_DEFAULT_USERS', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
            if bootstrap_defaults:
                print("ðŸ‘¤ BOOTSTRAP_DEFAULT_USERS enabled: creating default users")
                db.create_default_users()
            else:
                print("No users found. Skipping insecure default-user bootstrap (set BOOTSTRAP_DEFAULT_USERS=true to enable temporarily).")
        else:
            print(f"   Found {len(users)} existing users")
        print("Database ready")
    except Exception as e:
        print(f"Database initialization failed: {e}")
        print("âš ï¸  Server will run but authentication may not work")


def initialize_app():
    """Initialize the application with SaaS multi-tenant support"""
    print("="*60)
    print("TIMO Dashboard Server - SaaS Multi-Tenant")
    print("  Features: Per-org data, Self-service registration,")
    print("  Real-time SSE, Background Refresh, Plans & Billing")
    print("="*60)
    
    # Check setup
    setup_ok = check_setup()
    
    # Initialize database (includes SaaS tables)
    initialize_database()
    
    # Try per-org initialization first (SaaS mode)
    initialize_all_orgs()
    
    # Fallback: if no orgs have data, try legacy config file
    if not any(od['restaurants'] for od in ORG_DATA.values()):
        print("\nNo org data found, trying legacy config file...")
        ifood_ok = initialize_ifood_api()
        if ifood_ok:
            snapshot_loaded = _load_data_snapshot()
            if snapshot_loaded:
                print("Fast start: serving cached data while refreshing in background")
                threading.Thread(target=bg_refresher.refresh_now, daemon=True).start()
            else:
                print("First start: loading data from iFood API...")
                load_restaurants_from_ifood()
                _save_data_snapshot()
            refresh_minutes = IFOOD_CONFIG.get('refresh_interval_minutes', 30)
            bg_refresher.interval = refresh_minutes * 60
            bg_refresher.start()
    else:
        # Start background refresh for all orgs
        bg_refresher.interval = 1800  # 30 min
        bg_refresher.start()
    
    total_restaurants = sum(len(od['restaurants']) for od in ORG_DATA.values()) + len(RESTAURANTS_DATA)
    total_orgs = len([o for o in ORG_DATA.values() if o['restaurants']])
    
    print("\n" + "="*60)
    print("TIMO Server Ready")
    print("="*60)
    print(f"\nOrganizations: {total_orgs}")
    print(f"Total Restaurants: {total_restaurants}")
    print("Background refresh: every 30 min")
    print("SSE: ready on /api/events")
    print(f"\nAccess: http://localhost:{os.environ.get('PORT', 5000)}")
    print("="*60)
    print()


# Run initialization
initialize_app()

if __name__ == '__main__':
    import sys
    
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
