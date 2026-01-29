"""
Flask Web Server for Restaurant Dashboard - FIXED VERSION
Fixed routing issues and dynamic restaurant page generation
"""

from flask import Flask, request, jsonify, session, redirect, url_for, send_file, render_template_string
from dashboarddb import DashboardDatabase
import os
from pathlib import Path
import json
import pandas as pd
import numpy as np
import hashlib
from typing import Dict, List, Optional
import traceback
import re

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this-in-production'  # Change this!

# Database configuration
db = DashboardDatabase(
    host='localhost',
    port=5432,
    database='passwords',
    user='postgres',
    password='passwords'  # CHANGE THIS!
)

# Global variables
RESTAURANTS_DATA = []
EXCEL_DATA_CACHE = {}

# Configure paths
BASE_DIR = Path(__file__).parent.absolute()
DASHBOARD_OUTPUT = BASE_DIR / 'dashboard_output'
EXCEL_FILES_DIR = BASE_DIR / 'excel_files'

print(f"📁 Base directory: {BASE_DIR}")
print(f"📁 Dashboard output: {DASHBOARD_OUTPUT}")
print(f"📁 Excel files: {EXCEL_FILES_DIR}")


class ExcelProcessor:
    """Process Excel files and extract restaurant data"""
    
    @staticmethod
    def find_label_column(df: pd.DataFrame) -> Optional[str]:
        """Find the best column to use as labels"""
        label_keywords = ['mês', 'mes', 'month', 'data', 'date', 'dia', 'day', 
                         'período', 'periodo', 'period', 'semana', 'week']
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if any(keyword in col_lower for keyword in label_keywords):
                return col
        
        for col in df.columns:
            if df[col].dtype == 'object':
                return col
        
        return df.columns[0] if len(df.columns) > 0 else None
    
    @staticmethod
    def get_numeric_columns(df: pd.DataFrame, exclude_col: Optional[str] = None) -> List[str]:
        """Get all numeric columns with actual data"""
        numeric_cols = []
        for col in df.select_dtypes(include=[np.number]).columns:
            if col != exclude_col and df[col].notna().sum() > 0:
                numeric_cols.append(col)
        return numeric_cols
    
    @staticmethod
    def normalize_key(text: str) -> str:
        """Normalize column name to a consistent key format (remove accents, lowercase, underscores)"""
        import unicodedata
        # Normalize unicode characters and remove accents
        normalized = unicodedata.normalize('NFKD', text)
        ascii_text = normalized.encode('ASCII', 'ignore').decode('ASCII')
        # Lowercase and replace spaces with underscores
        return ascii_text.lower().replace(' ', '_')
    
    @staticmethod
    def extract_summary_metrics(df: pd.DataFrame) -> Dict:
        """Extract summary metrics from a dataframe"""
        label_col = ExcelProcessor.find_label_column(df)
        numeric_cols = ExcelProcessor.get_numeric_columns(df, label_col)
        if label_col and len(df) > 0:
            last_val = df[label_col].iloc[-1]
            result = {'last_period': str(last_val)}
        
        if not numeric_cols:
            return {'vendas': 0, 'ticket_medio': 0, 'valor_bruto': 0, 'liquido': 0, 'trends': {}}
        
        metrics = {}
        trends = {}
        
        for col in numeric_cols:
            valid_data = df[col].dropna()
            key = ExcelProcessor.normalize_key(col)  # Use normalized key (no accents)
            if len(valid_data) > 0:
                metrics[key] = float(valid_data.iloc[-1])
                
                if len(valid_data) >= 2:
                    current = valid_data.iloc[-1]
                    previous = valid_data.iloc[-2]
                    if previous != 0:
                        trend = ((current - previous) / previous) * 100
                        trends[key] = float(trend)  # Use normalized key for trends too
        
        result = {
            'vendas': metrics.get('vendas', metrics.get('pedidos', 0)),
            'ticket_medio': metrics.get('ticket_medio', 0),
            'valor_bruto': metrics.get('valor_bruto', metrics.get('faturamento', 0)),
            'liquido': metrics.get('liquido', 0),
            'trends': {
                'vendas': trends.get('vendas', trends.get('pedidos', 0)),
                'ticket_medio': trends.get('ticket_medio', 0),
                'valor_bruto': trends.get('valor_bruto', trends.get('faturamento', 0)),
                'liquido': trends.get('liquido', 0),
            }
        }
        
        if label_col and len(df) > 0:
            result['last_period'] = str(df[label_col].iloc[-1])
        
        return result
    
    @staticmethod
    def read_excel_file(file_path: str) -> Dict[str, pd.DataFrame]:
        """Read all sheets from an Excel file"""
        try:
            xl = pd.ExcelFile(file_path, engine='openpyxl')
            cleaned_data = {}
            
            for sheet_name in xl.sheet_names:
                try:
                    df = pd.read_excel(xl, sheet_name=sheet_name, header=1)
                    df = df.dropna(axis=1, how='all')
                    df = df.dropna(how='all')
                    df = df.loc[:, ~df.columns.astype(str).str.contains('^Unnamed')]
                    df = df.reset_index(drop=True)
                    
                    numeric_cols = df.select_dtypes(include=[np.number]).columns
                    if len(df) > 0 and len(numeric_cols) > 0:
                        cleaned_data[sheet_name] = df
                        
                except Exception as e:
                    print(f"   ⚠️  Error reading sheet {sheet_name}: {e}")
                    continue
            
            return cleaned_data
            
        except Exception as e:
            print(f"   ❌ Error reading Excel file {file_path}: {e}")
            return {}
    
    @staticmethod
    def process_restaurant_excel(file_path: str, name: str = None, manager: str = "Gerente") -> Optional[Dict]:
        """Process a restaurant Excel file and return restaurant data"""
        if not Path(file_path).exists():
            print(f"   ❌ File not found: {file_path}")
            return None
        
        if not name:
            name = Path(file_path).stem.replace('_', ' ').replace('-', ' ').title()
        
        restaurant_id = hashlib.md5(name.encode()).hexdigest()[:8]
        
        excel_data = ExcelProcessor.read_excel_file(file_path)
        
        if not excel_data:
            print(f"   ❌ No valid data in {file_path}")
            return None
        
        platforms = list(excel_data.keys())[:3]
        first_sheet = list(excel_data.values())[0]
        metrics = ExcelProcessor.extract_summary_metrics(first_sheet)
        
        restaurant_data = {
            'id': restaurant_id,
            'name': name,
            'file': str(file_path),
            'manager': manager,
            'platforms': platforms,
            'metrics': metrics
        }
        
        EXCEL_DATA_CACHE[restaurant_id] = excel_data
        
        return restaurant_data


def load_restaurants_from_excel():
    """Load all restaurants from Excel files"""
    global RESTAURANTS_DATA
    
    RESTAURANTS_DATA = []
    
    EXCEL_FILES_DIR.mkdir(exist_ok=True)
    
    excel_files = list(EXCEL_FILES_DIR.glob('*.xlsx')) + list(EXCEL_FILES_DIR.glob('*.xls'))
    
    if not excel_files:
        print(f"⚠️  No Excel files found in {EXCEL_FILES_DIR}")
        print(f"   Place your Excel files in this directory and restart the server")
        return
    
    print(f"\n📊 Processing {len(excel_files)} Excel file(s)...")
    
    for excel_file in excel_files:
        print(f"   📄 Processing: {excel_file.name}")
        restaurant_data = ExcelProcessor.process_restaurant_excel(str(excel_file))
        
        if restaurant_data:
            RESTAURANTS_DATA.append(restaurant_data)
            print(f"   ✅ {restaurant_data['name']}")
        else:
            print(f"   ❌ Failed to process {excel_file.name}")
    
    print(f"\n✅ Loaded {len(RESTAURANTS_DATA)} restaurant(s)")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def read_html_file(filename):
    """Read HTML file with error handling"""
    file_path = DASHBOARD_OUTPUT / filename
    
    print(f"🔍 Looking for: {file_path}")
    
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        print(f"📁 Files in dashboard_output:")
        if DASHBOARD_OUTPUT.exists():
            for f in DASHBOARD_OUTPUT.iterdir():
                print(f"   - {f.name}")
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"✅ Successfully read: {filename}")
        return content
    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        return None


def inject_restaurant_data(html_content):
    """Inject restaurant data into HTML"""
    if not html_content:
        return None
    
    restaurants_json = json.dumps(RESTAURANTS_DATA, ensure_ascii=False)
    pattern = r'const restaurantsData = \[.*?\];'
    replacement = f'const restaurantsData = {restaurants_json};'
    
    return re.sub(pattern, replacement, html_content, flags=re.DOTALL)


def serve_html_with_auth(filename):
    """Serve HTML file (auth should be checked by route handler)"""
    html_content = read_html_file(filename)
    
    if html_content is None:
        return f"""
        <!DOCTYPE html>
        <html>
        <head><title>File Not Found</title></head>
        <body style="font-family: sans-serif; max-width: 600px; margin: 50px auto; padding: 20px;">
            <div style="background: #fee; border: 1px solid #fcc; padding: 20px; border-radius: 5px;">
                <h1>⚠️ File Not Found</h1>
                <p>The file {filename} is missing from dashboard_output/</p>
                <p>Expected location: <code>{DASHBOARD_OUTPUT / filename}</code></p>
            </div>
            <p><a href="/login">← Back to Login</a></p>
        </body>
        </html>
        """, 404
    
    html_content = inject_restaurant_data(html_content)
    return html_content


def generate_restaurant_html(restaurant: Dict, excel_data: Dict) -> str:
    """Generate HTML for individual restaurant dashboard using the exact template"""
    
    # Color palette for metrics
    color_palette = [
        {'border': '#ef4444', 'bg': 'rgba(239, 68, 68, 0.1)'},      # Red
        {'border': '#3b82f6', 'bg': 'rgba(59, 130, 246, 0.1)'},     # Blue
        {'border': '#22c55e', 'bg': 'rgba(34, 197, 94, 0.1)'},      # Green
        {'border': '#f59e0b', 'bg': 'rgba(245, 158, 11, 0.1)'},     # Amber
        {'border': '#8b5cf6', 'bg': 'rgba(139, 92, 246, 0.1)'},     # Violet
        {'border': '#ec4899', 'bg': 'rgba(236, 72, 153, 0.1)'},     # Pink
        {'border': '#06b6d4', 'bg': 'rgba(6, 182, 212, 0.1)'},      # Cyan
        {'border': '#f97316', 'bg': 'rgba(249, 115, 22, 0.1)'}      # Orange
    ]
    
    # Prepare data for all sheets
    all_sheets_chart_data = {}
    tables_data = []
    sheet_names = []
    
    for sheet_name, df in excel_data.items():
        sheet_names.append(sheet_name)
        
        label_col = ExcelProcessor.find_label_column(df)
        numeric_cols = ExcelProcessor.get_numeric_columns(df, label_col)
        
        if label_col and numeric_cols:
            labels = df[label_col].astype(str).tolist()
            
            # Build datasets for chart with different colors
            datasets = {}
            columns = []
            
            for idx, col in enumerate(numeric_cols):
                values = df[col].fillna(0).tolist()
                colors = color_palette[idx % len(color_palette)]
                
                datasets[col] = {
                    'label': col,
                    'data': values,
                    'borderColor': colors['border'],
                    'backgroundColor': colors['bg']
                }
                columns.append(col)
            
            all_sheets_chart_data[sheet_name] = {
                'labels': labels,
                'datasets': datasets,
                'columns': columns
            }
            
            # Build table data
            table_rows = []
            for idx, row in df.iterrows():
                row_data = {label_col: row[label_col]}
                for col in numeric_cols:
                    row_data[col] = row[col] if pd.notna(row[col]) else None
                table_rows.append(row_data)
            
            tables_data.append({
                'name': sheet_name,
                'columns': [label_col] + numeric_cols,
                'data': table_rows
            })
    
    # Generate platforms HTML
    platforms_html = ''.join([f'<span class="platform-tag">{p}</span>' for p in restaurant['platforms']])
    
    # Read template file
    template_path = Path(__file__).parent / 'restaurant_template.html'
    with open(template_path, 'r', encoding='utf-8') as f:
        template = f.read()
    
    # Replace placeholders
    html = template.replace('{{restaurant_name}}', restaurant['name'])
    html = html.replace('{{restaurant_manager}}', restaurant['manager'])
    html = html.replace('{{platforms_html}}', platforms_html)
    html = html.replace('{{all_sheets_chart_data}}', json.dumps(all_sheets_chart_data, ensure_ascii=False))
    html = html.replace('{{tables_data}}', json.dumps(tables_data, ensure_ascii=False))
    html = html.replace('{{sheet_names}}', json.dumps(sheet_names, ensure_ascii=False))
    
    return html


# ============================================================================
# ROUTES
# ============================================================================

@app.route('/')
def index():
    """Main entry point"""
    print(f"🔍 Route: / (index)")
    if 'user' not in session:
        print("   → Redirecting to login (no session)")
        return redirect(url_for('login'))
    print("   → Serving dashboard (has session)")
    return serve_html_with_auth('index.html')


@app.route('/login')
@app.route('/login.html')
def login():
    """Serve login page"""
    print(f"🔍 Route: /login")
    
    html_content = read_html_file('login.html')
    
    if html_content is None:
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Login - File Not Found</title>
            <style>
                body { font-family: sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
                .error { background: #fee; border: 1px solid #fcc; padding: 20px; border-radius: 5px; }
                .info { background: #eff; border: 1px solid #cef; padding: 15px; margin-top: 20px; border-radius: 5px; }
            </style>
        </head>
        <body>
            <div class="error">
                <h1>⚠️ Login Page Not Found</h1>
                <p>The login.html file is missing from the dashboard_output directory.</p>
            </div>
            <div class="info">
                <h3>📁 Expected file location:</h3>
                <code>{}</code>
                
                <h3>✅ How to fix:</h3>
                <ol>
                    <li>Make sure login.html exists in the dashboard_output/ directory</li>
                    <li>Copy it from your original files</li>
                    <li>Restart the server</li>
                </ol>
            </div>
        </body>
        </html>
        """.format(DASHBOARD_OUTPUT / 'login.html'), 404
    
    return html_content




def authenticate_user_by_email(self, email: str, password: str) -> Optional[Dict]:
        """Authenticate user by email and return user data"""
        conn = self.get_connection()
        if not conn:
            return None
        
        cursor = conn.cursor()
        password_hash = self.hash_password(password)
        
        try:
            cursor.execute("""
                SELECT id, username, full_name, email, role, last_login
                FROM dashboard_users
                WHERE email = %s AND password_hash = %s
            """, (email, password_hash))
            
            result = cursor.fetchone()
            
            if result:
                # Update last login
                cursor.execute("""
                    UPDATE dashboard_users 
                    SET last_login = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (result[0],))
                conn.commit()
                
                return {
                    'id': result[0],
                    'username': result[1],
                    'name': result[2],
                    'email': result[3],
                    'role': result[4],
                    'last_login': str(result[5]) if result[5] else None
                }
            
            return None
            
        except Exception as e:
            print(f"Authentication error: {e}")
            return None
        finally:
            cursor.close()
            conn.close()
    


@app.route('/api/login', methods=['POST'])
def api_login():
    """Handle login POST request - authenticates by email"""
    print(f"🔍 API: /api/login (POST)")
    
    data = request.json
    email = data.get('email')
    password = data.get('password')
    
    print(f"   📧 Login attempt: {email}")

    
    
    # Authenticate by email

    user = db.authenticate_user_by_email(email, password)
    
    
    if user:
        session['user'] = user
        print(f"   ✅ Login successful: {email}")
        return jsonify({
            'success': True,
            'user': user,
            'redirect': '/dashboard'
        })
    else:
        print(f"   ❌ Login failed: {email}")
        return jsonify({
            'success': False,
            'message': 'Email ou senha incorretos'
        }), 401









@app.route('/dashboard')
@app.route('/index.html')
def dashboard():
    """Serve main dashboard"""
    print(f"🔍 Route: /dashboard")
    if 'user' not in session:
        return redirect(url_for('login'))
    return serve_html_with_auth('index.html')


@app.route('/admin')
@app.route('/admin.html')
def admin():
    """Serve admin page"""
    print(f"🔍 Route: /admin")
    
    if 'user' not in session:
        return redirect(url_for('login'))
    
    user = session['user']
    if user['role'] != 'admin':
        return "Access denied - Admin role required", 403
    
    return serve_html_with_auth('admin.html')


@app.route('/api/user')
def api_user():
    """Get current user info"""
    print(f"🔍 API: /api/user")
    if 'user' in session:
        return jsonify(session['user'])
    return jsonify({'error': 'Not authenticated'}), 401


@app.route('/api/restaurants')
def api_restaurants():
    """Get all restaurants"""
    print(f"🔍 API: /api/restaurants")
    if 'user' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    return jsonify(RESTAURANTS_DATA)


@app.route('/api/reload', methods=['POST'])
def api_reload():
    """Reload all Excel files - Admin only"""
    print(f"🔍 API: /api/reload (POST)")
    
    if 'user' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    if session['user'].get('role') != 'admin':
        return jsonify({'error': 'Admin access required'}), 403
    
    try:
        old_count = len(RESTAURANTS_DATA)
        load_restaurants_from_excel()
        new_count = len(RESTAURANTS_DATA)
        
        return jsonify({
            'success': True,
            'message': f'Reloaded successfully',
            'previous_count': old_count,
            'current_count': new_count,
            'restaurants': RESTAURANTS_DATA
        })
    except Exception as e:
        print(f"❌ Reload error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload', methods=['POST'])
def api_upload():
    """Upload new Excel file - Admin only"""
    print(f"🔍 API: /api/upload (POST)")
    
    if 'user' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    if session['user'].get('role') != 'admin':
        return jsonify({'error': 'Admin access required'}), 403
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'error': 'Only Excel files (.xlsx, .xls) are allowed'}), 400
    
    # Get optional name and manager from form data
    custom_name = request.form.get('name', '').strip()
    custom_manager = request.form.get('manager', 'Gerente').strip()
    
    try:
        # Save the file
        from werkzeug.utils import secure_filename
        filename = secure_filename(file.filename)
        
        # If custom name provided, use it for the filename
        if custom_name:
            # Keep the extension from original file
            ext = Path(filename).suffix
            safe_name = secure_filename(custom_name.replace(' ', '_'))
            filename = f"{safe_name}{ext}"
        
        file_path = EXCEL_FILES_DIR / filename
        file.save(str(file_path))
        
        print(f"✅ File uploaded: {filename}")
        if custom_name:
            print(f"   Custom name: {custom_name}")
        if custom_manager:
            print(f"   Manager: {custom_manager}")
        
        # Process this specific file with custom name and manager
        restaurant_data = ExcelProcessor.process_restaurant_excel(
            str(file_path), 
            name=custom_name if custom_name else None,
            manager=custom_manager
        )
        
        if restaurant_data:
            # Add to global data
            RESTAURANTS_DATA.append(restaurant_data)
            print(f"   ✅ Processed: {restaurant_data['name']}")
        
        return jsonify({
            'success': True,
            'message': f'File "{filename}" uploaded successfully',
            'filename': filename,
            'restaurants': RESTAURANTS_DATA
        })
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/delete/<restaurant_id>', methods=['DELETE'])
def api_delete_restaurant(restaurant_id):
    """Delete a restaurant Excel file - Admin only"""
    print(f"🔍 API: /api/delete/{restaurant_id} (DELETE)")
    
    if 'user' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    if session['user'].get('role') != 'admin':
        return jsonify({'error': 'Admin access required'}), 403
    
    # Find the restaurant
    restaurant = None
    for r in RESTAURANTS_DATA:
        if r['id'] == restaurant_id:
            restaurant = r
            break
    
    if not restaurant:
        return jsonify({'error': 'Restaurant not found'}), 404
    
    try:
        # Delete the file
        file_path = Path(restaurant['file'])
        if file_path.exists():
            file_path.unlink()
            print(f"✅ File deleted: {file_path.name}")
        
        # Remove from cache
        if restaurant_id in EXCEL_DATA_CACHE:
            del EXCEL_DATA_CACHE[restaurant_id]
        
        # Reload data
        load_restaurants_from_excel()
        
        return jsonify({
            'success': True,
            'message': f'Restaurant "{restaurant["name"]}" deleted',
            'restaurants': RESTAURANTS_DATA
        })
    except Exception as e:
        print(f"❌ Delete error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/restaurants/<restaurant_id>.html')
def restaurant_detail(restaurant_id):
    """Serve individual restaurant dashboard"""
    print(f"🔍 Route: /restaurants/{restaurant_id}.html")
    
    if 'user' not in session:
        return redirect(url_for('login'))
    
    # Find the restaurant data
    restaurant = None
    for r in RESTAURANTS_DATA:
        if r['id'] == restaurant_id:
            restaurant = r
            break
    
    if not restaurant:
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Restaurant Not Found</title>
            <style>
                body {{ font-family: sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }}
                .error {{ background: #fee; border: 1px solid #fcc; padding: 20px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <div class="error">
                <h1>⚠️ Restaurant Not Found</h1>
                <p>Restaurant ID: {restaurant_id}</p>
                <p><a href="/dashboard">← Back to Dashboard</a></p>
            </div>
        </body>
        </html>
        """, 404
    
    # Get Excel data for this restaurant
    excel_data = EXCEL_DATA_CACHE.get(restaurant_id, {})
    
    # Generate HTML dynamically
    html = generate_restaurant_html(restaurant, excel_data)
    return html



# ADMIN API ENDPOINTS

@app.route('/api/me', methods=['GET'])
def api_get_current_user():
    """Get the currently logged in user with fresh data from database"""
    if 'user' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    user_id = session['user'].get('id')
    
    if not user_id:
        return jsonify({'success': True, 'user': session['user']})
    
    # Fetch fresh data from database
    try:
        conn = db.get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, username, full_name, email, role, last_login
                FROM dashboard_users WHERE id = %s
            """, (user_id,))
            
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if row:
                fresh_user = {
                    'id': row[0],
                    'username': row[1],
                    'name': row[2],
                    'email': row[3],
                    'role': row[4],
                    'last_login': str(row[5]) if row[5] else None
                }
                # Update session with fresh data
                session['user'] = fresh_user
                return jsonify({'success': True, 'user': fresh_user})
    except Exception as e:
        print(f"Error fetching fresh user data: {e}")
    
    # Fallback to session data
    return jsonify({'success': True, 'user': session['user']})

@app.route('/api/admin/users', methods=['GET'])
def api_get_all_users():
    """Get all users - admin only"""
    if 'user' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    if session['user'].get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Admin access required'}), 403
    try:
        users = db.get_all_users()
        return jsonify({'success': True, 'users': users, 'restaurant_count': len(RESTAURANTS_DATA)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/users/<int:user_id>', methods=['GET'])
def api_get_user(user_id):
    """Get a single user by ID"""
    if 'user' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    if session['user'].get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Admin access required'}), 403
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, full_name, email, role, created_at, last_login FROM dashboard_users WHERE id = %s", (user_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return jsonify({'success': True, 'user': {'id': row[0], 'username': row[1], 'name': row[2], 'email': row[3], 'role': row[4], 'created_at': str(row[5]), 'last_login': str(row[6]) if row[6] else None}})
        return jsonify({'success': False, 'error': 'User not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/users', methods=['POST'])
def api_create_user():
    """Create a new user"""
    if 'user' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    if session['user'].get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Admin access required'}), 403
    try:
        data = request.get_json()
        username = data.get('username', '').strip().lower()
        password = data.get('password', '')
        full_name = data.get('full_name', '').strip()
        email = data.get('email', '').strip() or None
        role = data.get('role', 'user')
        if not username or not password or not full_name:
            return jsonify({'success': False, 'error': 'All fields required'}), 400
        if not email:
            return jsonify({'success': False, 'error': 'Email is required'}), 400
        user_id = db.create_user(username=username, password=password, full_name=full_name, email=email, role=role)
        if user_id:
            return jsonify({'success': True, 'user_id': user_id})
        return jsonify({'success': False, 'error': 'User already exists'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/users/<int:user_id>', methods=['PUT'])
def api_update_user(user_id):
    """Update an existing user"""
    if 'user' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    if session['user'].get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Admin access required'}), 403
    try:
        data = request.get_json()
        full_name = data.get('full_name', '').strip()
        email = data.get('email', '').strip() or None
        role = data.get('role', 'user')
        password = data.get('password', '')
        conn = db.get_connection()
        cursor = conn.cursor()
        if password:
            password_hash = db.hash_password(password)
            cursor.execute("UPDATE dashboard_users SET full_name = %s, email = %s, role = %s, password_hash = %s WHERE id = %s", (full_name, email, role, password_hash, user_id))
        else:
            cursor.execute("UPDATE dashboard_users SET full_name = %s, email = %s, role = %s WHERE id = %s", (full_name, email, role, user_id))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/users/<int:user_id>', methods=['DELETE'])
def api_delete_user(user_id):
    """Delete a user"""
    if 'user' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    if session['user'].get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Admin access required'}), 403
    if session['user'].get('id') == user_id:
        return jsonify({'success': False, 'error': 'Cannot delete yourself'}), 400
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dashboard_users WHERE id = %s", (user_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500




@app.errorhandler(404)
def page_not_found(e):
    """Custom 404 error handler"""
    print(f"❌ 404 Error: {request.url}")
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>404 - Not Found</title>
        <style>
            body {{ font-family: sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }}
            .error {{ background: #fee; border: 1px solid #fcc; padding: 20px; border-radius: 5px; }}
            .debug {{ background: #f5f5f5; padding: 15px; margin-top: 20px; border-radius: 5px; font-family: monospace; }}
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
            <p><strong>Excel files directory:</strong> {EXCEL_FILES_DIR}</p>
            <p><strong>Loaded restaurants:</strong> {len(RESTAURANTS_DATA)}</p>
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
    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>500 - Internal Server Error</title></head>
    <body style="font-family: sans-serif; max-width: 800px; margin: 50px auto; padding: 20px;">
        <div style="background: #fee; border: 1px solid #fcc; padding: 20px; border-radius: 5px;">
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
    else:
        print(f"✅ dashboard_output/ directory exists")
        
        # Check for HTML files
        required_files = ['login.html', 'index.html', 'admin.html']
        for filename in required_files:
            file_path = DASHBOARD_OUTPUT / filename
            if file_path.exists():
                print(f"   ✅ {filename}")
            else:
                issues.append(f"❌ Missing: dashboard_output/{filename}")
    
    # Check excel_files directory
    if not EXCEL_FILES_DIR.exists():
        issues.append(f"⚠️  excel_files/ directory not found (will be created)")
        EXCEL_FILES_DIR.mkdir(exist_ok=True)
    else:
        excel_files = list(EXCEL_FILES_DIR.glob('*.xlsx')) + list(EXCEL_FILES_DIR.glob('*.xls'))
        if excel_files:
            print(f"✅ excel_files/ directory with {len(excel_files)} file(s)")
        else:
            print(f"⚠️  excel_files/ directory is empty")
            issues.append(f"⚠️  No Excel files found in {EXCEL_FILES_DIR}")
    
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
        users = db.get_all_users()
        if not users:
            print("👤 Creating default users...")
            db.create_default_users()
        print("✅ Database ready")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        print("⚠️  Server will run but authentication may not work")


def initialize_app():
    """Initialize the application"""
    print("="*60)
    print("Restaurant Dashboard Server - Fixed Version")
    print("="*60)
    
    # Check setup
    setup_ok = check_setup()
    
    # Initialize database
    initialize_database()
    
    # Load restaurant data
    load_restaurants_from_excel()
    
    print("\n" + "="*60)
    if setup_ok:
        print("🚀 Server Ready!")
    else:
        print("⚠️  Server Starting with Issues")
    print("="*60)
    print(f"\n🌐 Access the dashboard at: http://localhost:5000")
    print(f"👤 Default credentials:")
    print(f"   Admin:  admin / admin123")
    print(f"   User:   usuario / user123")
    print(f"\n📁 Directories:")
    print(f"   Dashboard: {DASHBOARD_OUTPUT}")
    print(f"   Excel:     {EXCEL_FILES_DIR}")
    print(f"\nPress Ctrl+C to stop the server")
    print("="*60)
    print()


if __name__ == '__main__':
    initialize_app()
    
    # Run with debug mode for better error messages
    app.run(debug=True, host='0.0.0.0', port=5000)