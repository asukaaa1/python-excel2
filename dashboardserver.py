"""
Flask Web Server for Restaurant Dashboard - iFood API Version
Integrated with iFood Merchant API instead of Excel files
Features: Real-time SSE, Background Refresh, Comparative Analytics, Data Caching
"""

from flask import Flask, request, jsonify, session, redirect, url_for, send_file, render_template_string, Response, stream_with_context
from werkzeug.middleware.proxy_fix import ProxyFix
from dashboarddb import DashboardDatabase
from ifood_api_with_mock import IFoodAPI, IFoodConfig
from ifood_data_processor import IFoodDataProcessor
import os
from pathlib import Path
import json
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
    print("🔧 ProxyFix enabled (detected reverse proxy)")

app.secret_key = os.environ.get('SECRET_KEY', '1a2bfcf2e328076efb65896cfd29b249698f0fe5a355a10a1e80efadc0a8d4bf')
app.permanent_session_lifetime = timedelta(days=7)
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = IS_BEHIND_PROXY

print(f"📁 Base directory: {BASE_DIR}")
print(f"📁 Static folder: {STATIC_DIR}")
print(f"📁 Dashboard output: {DASHBOARD_OUTPUT}")
print(f"📁 Config file: {CONFIG_FILE}")

@app.after_request
def add_cache_headers(response):
    """Prevent caching of HTML pages to avoid stale session issues"""
    if response.content_type and 'text/html' in response.content_type:
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response

# Database configuration
db = DashboardDatabase()

# Global variables
RESTAURANTS_DATA = []
IFOOD_API = None
IFOOD_CONFIG = {}
LAST_DATA_REFRESH = None

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
        print(f"🔄 Background refresh started (every {self.interval // 60} min)")
    
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
            print(f"❌ Background refresh error: {e}")
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
            print(f"   ❌ Failed to refresh {name}: {e}")
    
    # Atomic swap
    RESTAURANTS_DATA = new_data
    LAST_DATA_REFRESH = datetime.now()
    
    # Save snapshot to DB for fast cold starts
    _save_data_snapshot()
    
    print(f"🔄 Refreshed {len(RESTAURANTS_DATA)} restaurant(s) at {LAST_DATA_REFRESH.strftime('%H:%M:%S')}")


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
        print(f"⚠️ Failed to save snapshot: {e}")


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
                print(f"⚡ Loaded {len(data)} restaurants from DB snapshot ({age.seconds // 60} min old)")
                return True
            else:
                print(f"⏳ DB snapshot too old ({age.seconds // 3600}h), will refresh from API")
                return False
        return False
    except Exception as e:
        print(f"⚠️ Failed to load snapshot: {e}")
        return False



# ============================================================================
# IFOOD API INITIALIZATION
# ============================================================================

def initialize_ifood_api():
    """Initialize iFood API with credentials from config"""
    global IFOOD_API, IFOOD_CONFIG
    
    # Load configuration
    IFOOD_CONFIG = IFoodConfig.load_config(str(CONFIG_FILE))
    
    if not IFOOD_CONFIG:
        print("⚠️  No iFood configuration found")
        print(f"   Creating sample config at {CONFIG_FILE}")
        IFoodConfig.create_sample_config(str(CONFIG_FILE))
        return False
    
    client_id = IFOOD_CONFIG.get('client_id')
    client_secret = IFOOD_CONFIG.get('client_secret')
    
    if not client_id or client_id == 'your_ifood_client_id_here':
        print("⚠️  iFood API credentials not configured")
        print(f"   Please update {CONFIG_FILE} with your credentials")
        return False
    
    # Initialize API
    IFOOD_API = IFoodAPI(client_id, client_secret)
    
    # Authenticate
    if IFOOD_API.authenticate():
        print("✅ iFood API initialized successfully")
        return True
    else:
        print("❌ iFood API authentication failed")
        return False


def load_restaurants_from_ifood():
    """Load all restaurants from iFood API"""
    global RESTAURANTS_DATA, LAST_DATA_REFRESH
    
    RESTAURANTS_DATA = []
    
    if not IFOOD_API:
        print("❌ iFood API not initialized")
        return
    
    print(f"\n📊 Fetching restaurant data from iFood API...")
    
    # Get merchants from config
    merchants_config = IFOOD_CONFIG.get('merchants', [])
    
    if not merchants_config:
        print("⚠️  No merchants configured in config file")
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
            print(f"❌ Error fetching merchants: {e}")
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
        
        print(f"   📄 Processing: {name}")
        
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
            print(f"      ✅ {restaurant_data['name']}")
            
        except Exception as e:
            print(f"      ❌ Failed to process {name}: {e}")
            traceback.print_exc()
    
    LAST_DATA_REFRESH = datetime.now()
    print(f"\n✅ Loaded {len(RESTAURANTS_DATA)} restaurant(s) from iFood")


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


def admin_required(f):
    """Decorator to require admin role"""
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
        if session['user'].get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function


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
    # Find restaurant
    restaurant = None
    for r in RESTAURANTS_DATA:
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
        rendered = template.replace('{{restaurant_name}}', restaurant['name'])
        rendered = rendered.replace('{{restaurant_id}}', restaurant['id'])
        rendered = rendered.replace('{{restaurant_manager}}', restaurant.get('manager', 'Gerente'))
        rendered = rendered.replace('{{restaurant_data}}', json.dumps(restaurant, ensure_ascii=False))
        
        return render_template_string(rendered)
    
    return "Restaurant template not found", 404


# ============================================================================
# API ROUTES - AUTHENTICATION
# ============================================================================

@app.route('/api/login', methods=['POST'])
def api_login():
    """Handle login requests"""
    try:
        data = request.get_json()
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
            session['user'] = user
            session.permanent = True
            
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
    """Get current user info"""
    return jsonify({
        'success': True,
        'user': session.get('user')
    })


# ============================================================================
# API ROUTES - RESTAURANT DATA
# ============================================================================

@app.route('/api/restaurants')
@login_required
def api_restaurants():
    """Get list of all restaurants with optional month filtering and squad-based access control"""
    try:
        # Get month filter from query parameters
        month_filter = request.args.get('month', 'all')
        
        # Get user's allowed restaurants based on squad membership
        user = session.get('user', {})
        allowed_ids = get_user_allowed_restaurant_ids(user.get('id'), user.get('role'))
        
        # Return data without internal caches
        restaurants = []
        for r in RESTAURANTS_DATA:
            # Skip if user doesn't have access to this restaurant (squad filtering)
            if allowed_ids is not None and r['id'] not in allowed_ids:
                continue
            
            # If month filter is specified, reprocess with filtered orders
            if month_filter != 'all':
                # Get cached orders
                orders = r.get('_orders_cache', [])
                
                # Filter orders by month
                filtered_orders = []
                for order in orders:
                    try:
                        created_at = order.get('createdAt', '')
                        if created_at:
                            order_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            if order_date.month == int(month_filter):
                                filtered_orders.append(order)
                    except:
                        pass
                
                # Reprocess restaurant data with filtered orders
                if filtered_orders or month_filter != 'all':
                    # Get merchant details (reconstruct basic structure)
                    merchant_details = {
                        'id': r['id'],
                        'name': r['name'],
                        'merchantManager': {'name': r.get('manager', 'Gerente')},
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
                    restaurant_data['name'] = r['name']
                    restaurant_data['manager'] = r['manager']
                    
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
        
        return jsonify({
            'success': True,
            'restaurants': restaurants,
            'last_refresh': LAST_DATA_REFRESH.isoformat() if LAST_DATA_REFRESH else None,
            'month_filter': month_filter
        })
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
        
        # Find restaurant
        restaurant = None
        for r in RESTAURANTS_DATA:
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
        
        if IFOOD_API:
            # Get interruptions
            try:
                interruptions = IFOOD_API.get_interruptions(restaurant_id) or []
            except:
                pass
        
        # Generate charts from filtered orders
        orders_for_charts = filtered_orders if (start_date or end_date) else all_orders
        if orders_for_charts:
            if hasattr(IFoodDataProcessor, 'generate_charts_data_with_interruptions'):
                chart_data = IFoodDataProcessor.generate_charts_data_with_interruptions(
                    orders_for_charts,
                    interruptions
                )
            else:
                chart_data = IFoodDataProcessor.generate_charts_data(orders_for_charts)
                chart_data['interruptions'] = []
        
        return jsonify({
            'success': True,
            'restaurant': response_data,
            'charts': chart_data,
            'interruptions': interruptions,
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
        for r in RESTAURANTS_DATA:
            if r['id'] == restaurant_id:
                restaurant = r
                break
        
        if not restaurant:
            return jsonify({'success': False, 'error': 'Restaurant not found'}), 404
        
        # Get parameters
        per_page = int(request.args.get('per_page', 100))
        page = int(request.args.get('page', 1))
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





# ============================================================================
# API ROUTES - RESTAURANT INTERRUPTIONS
# ============================================================================

@app.route('/api/restaurant/<restaurant_id>/interruptions')
@login_required
def api_restaurant_interruptions(restaurant_id):
    """Get interruptions for a specific restaurant"""
    try:
        if not IFOOD_API:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
        
        # Get interruptions
        interruptions = IFOOD_API.get_interruptions(restaurant_id)
        
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
        if not IFOOD_API:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
        
        # Get status
        status = IFOOD_API.get_merchant_status(restaurant_id)
        
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
        if not IFOOD_API:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
        
        data = request.get_json()
        start = data.get('start')
        end = data.get('end')
        description = data.get('description', '')
        
        if not start or not end:
            return jsonify({'success': False, 'error': 'Start and end times required'}), 400
        
        # Create interruption
        result = IFOOD_API.create_interruption(restaurant_id, start, end, description)
        
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
        if not IFOOD_API:
            return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
        
        # Delete interruption
        success = IFOOD_API.delete_interruption(restaurant_id, interruption_id)
        
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
@login_required
def api_refresh_data():
    """Refresh restaurant data from iFood API (now uses background thread)"""
    try:
        if not IFOOD_API:
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
        'last_refresh': LAST_DATA_REFRESH.isoformat() if LAST_DATA_REFRESH else None,
        'restaurant_count': len(RESTAURANTS_DATA),
        'connected_clients': sse_manager.client_count,
        'refresh_interval_minutes': IFOOD_CONFIG.get('refresh_interval_minutes', 30)
    })


# ============================================================================
# COMPARATIVE ANALYTICS API
# ============================================================================

@app.route('/api/analytics/compare')
@login_required
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
            targets = RESTAURANTS_DATA
        else:
            targets = [r for r in RESTAURANTS_DATA if r['id'] == restaurant_id]
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
            for r in RESTAURANTS_DATA:
                all_orders.extend(r.get('_orders_cache', []))
        else:
            for r in RESTAURANTS_DATA:
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
@admin_required
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
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ifood/merchants', methods=['POST'])
@admin_required
def api_add_merchant():
    """Add a merchant to the configuration"""
    try:
        data = request.get_json()
        
        merchant_id = data.get('merchant_id')
        name = data.get('name')
        manager = data.get('manager', 'Gerente')
        
        if not merchant_id:
            return jsonify({'success': False, 'error': 'Merchant ID required'}), 400
        
        # Add to config
        if 'merchants' not in IFOOD_CONFIG:
            IFOOD_CONFIG['merchants'] = []
        
        # Check for duplicates
        for m in IFOOD_CONFIG['merchants']:
            if m.get('merchant_id') == merchant_id:
                return jsonify({'success': False, 'error': 'Merchant already exists'}), 400
        
        IFOOD_CONFIG['merchants'].append({
            'merchant_id': merchant_id,
            'name': name or f'Restaurant {merchant_id[:8]}',
            'manager': manager
        })
        
        # Save config
        IFoodConfig.save_config(IFOOD_CONFIG, str(CONFIG_FILE))
        
        # Reload data
        load_restaurants_from_ifood()
        
        return jsonify({
            'success': True,
            'message': 'Merchant added successfully',
            'restaurant_count': len(RESTAURANTS_DATA)
        })
        
    except Exception as e:
        print(f"Error adding merchant: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ifood/merchants/<merchant_id>', methods=['DELETE'])
@admin_required
def api_remove_merchant(merchant_id):
    """Remove a merchant from the configuration"""
    try:
        if 'merchants' not in IFOOD_CONFIG:
            return jsonify({'success': False, 'error': 'No merchants configured'}), 404
        
        # Find and remove merchant
        original_count = len(IFOOD_CONFIG['merchants'])
        IFOOD_CONFIG['merchants'] = [
            m for m in IFOOD_CONFIG['merchants'] 
            if m.get('merchant_id') != merchant_id
        ]
        
        if len(IFOOD_CONFIG['merchants']) == original_count:
            return jsonify({'success': False, 'error': 'Merchant not found'}), 404
        
        # Save config
        IFoodConfig.save_config(IFOOD_CONFIG, str(CONFIG_FILE))
        
        # Reload data
        load_restaurants_from_ifood()
        
        return jsonify({
            'success': True,
            'message': 'Merchant removed successfully',
            'restaurant_count': len(RESTAURANTS_DATA)
        })
        
    except Exception as e:
        print(f"Error removing merchant: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ifood/test')
@admin_required
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
            'error': str(e),
            'configured': bool(IFOOD_API)
        })


# ============================================================================
# API ROUTES - COMPARATIVO (ADMIN)
# ============================================================================

# In-memory storage for cancelled restaurants (in production, use database)
CANCELLED_RESTAURANTS = []

@app.route('/api/comparativo/stats')
@admin_required
def api_comparativo_stats():
    """Get consolidated stats for comparativo page"""
    try:
        total_stores = len(RESTAURANTS_DATA)
        stores_with_history = sum(1 for r in RESTAURANTS_DATA if (r.get('metrics', {}).get('vendas') or r.get('metrics', {}).get('total_pedidos') or 0) > 0)
        
        total_revenue = 0
        positive_count = 0
        negative_count = 0
        previous_revenue = 0
        
        for r in RESTAURANTS_DATA:
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
def api_comparativo_cancelled():
    """Get cancelled restaurants"""
    return jsonify({
        'success': True,
        'cancelled': CANCELLED_RESTAURANTS
    })


@app.route('/api/comparativo/cancelled', methods=['POST'])
@admin_required
def api_cancel_restaurant():
    """Mark a restaurant as cancelled"""
    global RESTAURANTS_DATA
    try:
        data = request.get_json()
        restaurant_id = data.get('restaurant_id')
        reason = data.get('reason', '')
        
        if not restaurant_id:
            return jsonify({'success': False, 'error': 'Restaurant ID required'}), 400
        
        # Find restaurant
        restaurant = None
        for r in RESTAURANTS_DATA:
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
        RESTAURANTS_DATA = [r for r in RESTAURANTS_DATA if r['id'] != restaurant_id]
        
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
    """Get all users (admin only)"""
    try:
        users = db.get_all_users()
        return jsonify({
            'success': True,
            'users': users
        })
    except Exception as e:
        print(f"Error getting users: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/users', methods=['POST'])
@admin_required
def api_create_user():
    """Create new user (admin only)"""
    try:
        data = request.get_json()
        
        username = data.get('username')
        password = data.get('password')
        full_name = data.get('full_name')
        email = data.get('email')
        role = data.get('role', 'user')
        
        if not all([username, password, full_name]):
            return jsonify({
                'success': False,
                'error': 'Username, password, and full name required'
            }), 400
        
        user_id = db.create_user(username, password, full_name, email, role)
        
        if user_id:
            return jsonify({
                'success': True,
                'message': 'User created successfully',
                'user_id': user_id
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Username already exists'
            }), 400
            
    except Exception as e:
        print(f"Error creating user: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@admin_required
def api_delete_user(user_id):
    """Delete user (admin only)"""
    try:
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
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# HIDDEN STORES API ENDPOINTS
# ============================================================================

@app.route('/api/hidden-stores', methods=['GET'])
@login_required
def get_hidden_stores():
    """Get list of all hidden stores"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
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
        data = request.get_json() or {}
        store_name = data.get('name', 'Unknown Store')
        hidden_by = session.get('user', {}).get('username', 'Unknown')
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if already hidden
        cursor.execute("SELECT store_id FROM hidden_stores WHERE store_id = %s", (store_id,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Store already hidden'}), 400
        
        # Insert into hidden stores
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
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get store name before deleting
        cursor.execute("SELECT store_name FROM hidden_stores WHERE store_id = %s", (store_id,))
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Store not found in hidden list'}), 404
        
        store_name = result[0]
        
        # Remove from hidden stores
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
        conn = db.get_connection()
        cursor = conn.cursor()
        
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


@app.route('/api/squads', methods=['GET'])
@login_required
def api_get_squads():
    """Get all squads with their members and restaurants"""
    try:
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
        
        if has_old_schema:
            # Old schema: id, squad_id, name, leader, members, restaurants, active, created_at
            cursor.execute("""
                SELECT id, squad_id, name, leader, members, restaurants, active, created_at
                FROM squads 
                WHERE active = true
                ORDER BY name
            """)
        else:
            # New schema: id, name, description, created_at, created_by
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
            except:
                members_list = []
            
            try:
                restaurants_list = json.loads(squad[5]) if squad[5] else []
            except:
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
        data = request.get_json()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        
        if not name:
            return jsonify({'success': False, 'error': 'Nome é obrigatório'}), 400
        
        created_by = session.get('user', {}).get('username', 'Unknown')
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if squad with same name exists
        cursor.execute("SELECT id FROM squads WHERE name = %s", (name,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Já existe um squad com este nome'}), 400
        
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
            cursor.execute("""
                INSERT INTO squads (squad_id, name, leader, members, restaurants)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (squad_uid, name, created_by, '[]', '[]'))
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
        data = request.get_json()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        
        if not name:
            return jsonify({'success': False, 'error': 'Nome é obrigatório'}), 400
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if squad exists
        cursor.execute("SELECT id FROM squads WHERE id = %s", (squad_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad não encontrado'}), 404
        
        # Check for duplicate name (excluding current squad)
        cursor.execute("SELECT id FROM squads WHERE name = %s AND id != %s", (name, squad_id))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Já existe outro squad com este nome'}), 400
        
        # Update squad - only update name since description doesn't exist in current schema
        cursor.execute("""
            UPDATE squads SET name = %s WHERE id = %s
        """, (name, squad_id))
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
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if squad exists
        cursor.execute("SELECT name FROM squads WHERE id = %s", (squad_id,))
        result = cursor.fetchone()
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad não encontrado'}), 404
        
        squad_name = result[0]
        
        # Delete squad (cascade will delete members and restaurants)
        cursor.execute("DELETE FROM squads WHERE id = %s", (squad_id,))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Squad "{squad_name}" excluído com sucesso'
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
        data = request.get_json()
        user_ids = data.get('user_ids', [])
        
        if not user_ids:
            return jsonify({'success': False, 'error': 'Nenhum usuário selecionado'}), 400
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if squad exists
        cursor.execute("SELECT id FROM squads WHERE id = %s", (squad_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad não encontrado'}), 404
        
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
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM squad_members 
            WHERE squad_id = %s AND user_id = %s
        """, (squad_id, user_id))
        
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Membro não encontrado no squad'}), 404
        
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
        data = request.get_json()
        restaurant_ids = data.get('restaurant_ids', [])
        
        if not restaurant_ids:
            return jsonify({'success': False, 'error': 'Nenhum restaurante selecionado'}), 400
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if squad exists
        cursor.execute("SELECT id FROM squads WHERE id = %s", (squad_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Squad não encontrado'}), 404
        
        added_count = 0
        for restaurant_id in restaurant_ids:
            # Find restaurant name from RESTAURANTS_DATA
            restaurant_name = 'Unknown'
            for r in RESTAURANTS_DATA:
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
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM squad_restaurants 
            WHERE squad_id = %s AND restaurant_id = %s
        """, (squad_id, restaurant_id))
        
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Restaurante não encontrado no squad'}), 404
        
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


# Public group page (no auth required)
@app.route('/grupo/<slug>')
def public_group_page(slug):
    """Serve public group dashboard - NO AUTH REQUIRED"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get group by slug
        cursor.execute("""
            SELECT id, name, slug, active
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
        
        # Get store data from RESTAURANTS_DATA
        stores_data = []
        for store_row in store_rows:
            store_id = store_row[0]
            store_name = store_row[1]
            
            # Find in global data
            for r in RESTAURANTS_DATA:
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
            rendered = template.replace('{{group_name}}', group_name)
            rendered = rendered.replace('{{group_initial}}', group_name[0].upper() if group_name else 'G')
            rendered = rendered.replace('{{group_data}}', json.dumps(group_data, ensure_ascii=False))
            
            return render_template_string(rendered)
        
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
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get all groups
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


# API: Create group
@app.route('/api/groups', methods=['POST'])
@admin_required
def api_create_group():
    """Create a new client group"""
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        slug = data.get('slug', '').strip()
        store_ids = data.get('store_ids', [])
        
        if not name:
            return jsonify({'success': False, 'error': 'Nome é obrigatório'}), 400
        
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
        
        # Check if slug exists
        cursor.execute("SELECT id FROM client_groups WHERE slug = %s", (slug,))
        if cursor.fetchone():
            # Add random suffix
            import random
            slug = f"{slug}-{random.randint(100, 999)}"
        
        # Create group
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
            for r in RESTAURANTS_DATA:
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
        data = request.get_json()
        name = data.get('name', '').strip()
        slug = data.get('slug', '').strip()
        store_ids = data.get('store_ids', [])
        active = data.get('active', True)
        
        if not name:
            return jsonify({'success': False, 'error': 'Nome é obrigatório'}), 400
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if group exists
        cursor.execute("SELECT id, slug FROM client_groups WHERE id = %s", (group_id,))
        existing = cursor.fetchone()
        if not existing:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Grupo não encontrado'}), 404
        
        # If slug changed, validate it
        if slug and slug != existing[1]:
            import re
            slug = re.sub(r'[^a-z0-9-]', '', slug.lower())
            
            cursor.execute("SELECT id FROM client_groups WHERE slug = %s AND id != %s", (slug, group_id))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Slug já existe'}), 400
        else:
            slug = existing[1]
        
        # Update group
        cursor.execute("""
            UPDATE client_groups 
            SET name = %s, slug = %s, active = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (name, slug, active, group_id))
        
        # Update stores - remove all and re-add
        cursor.execute("DELETE FROM group_stores WHERE group_id = %s", (group_id,))
        
        for store_id in store_ids:
            store_name = store_id
            for r in RESTAURANTS_DATA:
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
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if group exists
        cursor.execute("SELECT name FROM client_groups WHERE id = %s", (group_id,))
        result = cursor.fetchone()
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Grupo não encontrado'}), 404
        
        group_name = result[0]
        
        # Delete group (cascade will delete stores)
        cursor.execute("DELETE FROM client_groups WHERE id = %s", (group_id,))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Grupo "{group_name}" excluído com sucesso'
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
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get all restaurant IDs the user has access to through squads
        cursor.execute("""
            SELECT DISTINCT sr.restaurant_id
            FROM squad_restaurants sr
            JOIN squad_members sm ON sr.squad_id = sm.squad_id
            WHERE sm.user_id = %s
        """, (user_id,))
        
        restaurant_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
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
    print(f"❌ 404 Error: {request.url}")
    
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
            .debug {{ background: #f5f5f5; padding: 15px; margin-top: 20px; border-radius: 8px; font-family: monospace; }}
            a {{ color: #ef4444; }}
        </style>
    </head>
    <body>
        <div class="error">
            <h1>404 - Page Not Found</h1>
            <p>The requested URL was not found on this server.</p>
            <p><strong>Requested URL:</strong> {request.url}</p>
        </div>
        <div class="debug">
            <h3>🔍 Debug Information:</h3>
            <p><strong>Dashboard output directory:</strong> {DASHBOARD_OUTPUT}</p>
            <p><strong>Config file:</strong> {CONFIG_FILE}</p>
            <p><strong>Loaded restaurants:</strong> {len(RESTAURANTS_DATA)}</p>
            <p><strong>iFood API status:</strong> {'✅ Connected' if IFOOD_API else '❌ Not configured'}</p>
            <p><strong>Last data refresh:</strong> {LAST_DATA_REFRESH.strftime('%Y-%m-%d %H:%M:%S') if LAST_DATA_REFRESH else 'Never'}</p>
        </div>
        <p><a href="/login">← Back to Login</a></p>
    </body>
    </html>
    """, 404


@app.errorhandler(500)
def internal_error(e):
    """Custom 500 error handler"""
    print(f"❌ 500 Error: {str(e)}")
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
            <pre>{str(e)}</pre>
        </div>
        <p><a href="/login" style="color: #ef4444;">← Back to Login</a></p>
    </body>
    </html>
    """, 500


# ============================================================================
# INITIALIZATION
# ============================================================================

def check_setup():
    """Check if setup is correct"""
    print("\n" + "="*60)
    print("🔍 Checking Setup...")
    print("="*60)
    
    issues = []
    
    # Check dashboard_output directory
    if not DASHBOARD_OUTPUT.exists():
        issues.append(f"❌ dashboard_output/ directory not found at {DASHBOARD_OUTPUT}")
        # Try to create it
        try:
            DASHBOARD_OUTPUT.mkdir(parents=True, exist_ok=True)
            print(f"   Created dashboard_output/ directory")
        except:
            pass
    else:
        print(f"✅ dashboard_output/ directory exists")
        
        # Check for HTML files
        required_files = ['login.html', 'index.html']
        optional_files = ['admin.html', 'restaurant_template.html']
        
        for filename in required_files:
            file_path = DASHBOARD_OUTPUT / filename
            if file_path.exists():
                print(f"   ✅ {filename}")
            else:
                issues.append(f"❌ Missing required: dashboard_output/{filename}")
        
        for filename in optional_files:
            file_path = DASHBOARD_OUTPUT / filename
            if file_path.exists():
                print(f"   ✅ {filename}")
            else:
                print(f"   ⚠️ Optional: {filename} not found")
    
    # Check iFood config
    if not CONFIG_FILE.exists():
        print(f"⚠️  iFood config not found (will be created)")
    else:
        print(f"✅ iFood config file exists")
    
    if issues:
        print("\n⚠️  Issues found:")
        for issue in issues:
            print(f"   {issue}")
        print()
    else:
        print("\n✅ All checks passed!")
    
    return len([i for i in issues if i.startswith('❌')]) == 0


def initialize_database():
    """Initialize database tables and create default users if needed"""
    print("\n🔧 Initializing database...")
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
        
        conn.commit()
        cursor.close()
        conn.close()
        
        users = db.get_all_users()
        if not users:
            print("👤 Creating default users...")
            db.create_default_users()
        else:
            print(f"   Found {len(users)} existing users")
        print("✅ Database ready")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        print("⚠️  Server will run but authentication may not work")


def initialize_app():
    """Initialize the application with fast cold start support"""
    print("="*60)
    print("Restaurant Dashboard Server - iFood API Version")
    print("  Features: Real-time SSE, Background Refresh,")
    print("  Comparative Analytics, Data Caching")
    print("="*60)
    
    # Check setup
    setup_ok = check_setup()
    
    # Initialize database
    initialize_database()
    
    # Initialize iFood API
    ifood_ok = initialize_ifood_api()
    
    if ifood_ok:
        # Try fast cold start from DB snapshot first
        snapshot_loaded = _load_data_snapshot()
        
        if snapshot_loaded:
            # Data loaded from cache - start bg refresh to update in background
            print("⚡ Fast start: serving cached data while refreshing in background")
            threading.Thread(target=bg_refresher.refresh_now, daemon=True).start()
        else:
            # No cache, load from API (blocking on first start)
            print("📊 First start: loading data from iFood API...")
            load_restaurants_from_ifood()
            _save_data_snapshot()
        
        # Start periodic background refresh
        refresh_minutes = IFOOD_CONFIG.get('refresh_interval_minutes', 30)
        bg_refresher.interval = refresh_minutes * 60
        bg_refresher.start()
    else:
        print("\n⚠️  iFood API not configured properly")
        print(f"   Please update {CONFIG_FILE} with your credentials")
    
    print("\n" + "="*60)
    if setup_ok and ifood_ok:
        print("🚀 Server Ready!")
    else:
        print("⚠️  Server Starting with Issues")
    print("="*60)
    print(f"\n🌐 Access the dashboard at: http://localhost:5000")
    print(f"👤 Default credentials:")
    print(f"   Admin:  admin@dashboard.com / admin123")
    print(f"   User:   user@dashboard.com / user123")
    print(f"\n📁 Files:")
    print(f"   Dashboard: {DASHBOARD_OUTPUT}")
    print(f"   Config:    {CONFIG_FILE}")
    print(f"\n📊 Status:")
    print(f"   Restaurants: {len(RESTAURANTS_DATA)}")
    print(f"   iFood API:   {'✅ Connected' if IFOOD_API else '❌ Not configured'}")
    print(f"   Background:  🔄 Every {IFOOD_CONFIG.get('refresh_interval_minutes', 30)} min")
    print(f"   SSE:         ✅ Ready on /api/events")
    print(f"\nPress Ctrl+C to stop the server")
    print("="*60)
    print()


# Run initialization
initialize_app()

if __name__ == '__main__':
    import sys
    
    # Check if running in production mode
    if '--production' in sys.argv or os.environ.get('FLASK_ENV') == 'production':
        print("\n⚠️  WARNING: For production, use a WSGI server instead:")
        print("   Option 1 (Linux/Mac): gunicorn -c gunicorn_config.py dashboardserver:app")
        print("   Option 2 (Windows):   python run_production.py")
        print("   Option 3 (Any OS):    waitress-serve --port=5000 dashboardserver:app")
        sys.exit(1)
    
    # Development mode
    print("\n⚠️  Running in DEVELOPMENT mode")
    print("   For production, use: python dashboardserver.py --production")
    print()