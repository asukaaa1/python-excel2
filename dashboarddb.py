"""
PostgreSQL Database Module for Restaurant Dashboard
Handles user authentication and password storage
UPDATED VERSION - Supports DATABASE_URL from Render
"""

import psycopg2
from psycopg2 import sql
import bcrypt
import json
import os
import secrets
from datetime import datetime, timedelta
from urllib.parse import urlparse
from typing import Optional, Dict, List

class DashboardDatabase:
    """Handle PostgreSQL database operations for dashboard authentication"""
    
    def __init__(self, host='localhost', port=5432, database='passwords', 
                 user='postgres', password='passwords'):
        """
        Initialize database connection
        Supports both individual params and DATABASE_URL environment variable
        """
        # Check if DATABASE_URL is provided (Render, Heroku, etc.)
        database_url = os.environ.get('DATABASE_URL')
        
        if database_url:
            # Parse DATABASE_URL
            # Format: postgresql://user:password@host:port/database
            parsed = urlparse(database_url)
            
            self.config = {
                'host': parsed.hostname,
                'port': parsed.port or 5432,
                'database': parsed.path[1:],  # Remove leading slash
                'user': parsed.username,
                'password': parsed.password,
                'client_encoding': 'utf8'
            }
            print(f"📊 Using DATABASE_URL: {parsed.hostname}")
        else:
            # Use individual parameters
            self.config = {
                'host': host,
                'port': port,
                'database': database,
                'user': user,
                'password': password,
                'client_encoding': 'utf8'
            }
            print(f"📊 Using individual DB params: {host}:{port}/{database}")
    
    def get_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**self.config)
            conn.set_client_encoding('UTF8')
            return conn
        except Exception as e:
            print(f"Database connection error: {e}")
            return None
    
    def setup_tables(self):
        """Create necessary tables for dashboard authentication"""
        conn = self.get_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        try:
            # Create users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dashboard_users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    full_name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    role VARCHAR(20) NOT NULL DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP
                )
            """)
            
            # Create restaurant assignments table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_restaurants (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES dashboard_users(id) ON DELETE CASCADE,
                    restaurant_id VARCHAR(50) NOT NULL,
                    restaurant_name VARCHAR(100),
                    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, restaurant_id)
                )
            """)
            
            # Create squads table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS squads (
                    id SERIAL PRIMARY KEY,
                    squad_id VARCHAR(50) UNIQUE NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    leader VARCHAR(100) NOT NULL,
                    members TEXT,
                    restaurants TEXT,
                    active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create client_groups table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS client_groups (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    slug VARCHAR(100) UNIQUE NOT NULL,
                    active BOOLEAN DEFAULT true,
                    created_by VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create group_stores junction table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS group_stores (
                    id SERIAL PRIMARY KEY,
                    group_id INTEGER REFERENCES client_groups(id) ON DELETE CASCADE,
                    store_id VARCHAR(100) NOT NULL,
                    store_name VARCHAR(200),
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(group_id, store_id)
                )
            """)
            
            # ── SaaS: Organizations (tenants) ──
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS organizations (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    slug VARCHAR(100) UNIQUE NOT NULL,
                    plan VARCHAR(50) NOT NULL DEFAULT 'free',
                    plan_started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    plan_expires_at TIMESTAMP,
                    max_restaurants INTEGER NOT NULL DEFAULT 3,
                    max_users INTEGER NOT NULL DEFAULT 2,
                    ifood_client_id VARCHAR(255),
                    ifood_client_secret VARCHAR(255),
                    ifood_merchants JSONB DEFAULT '[]'::jsonb,
                    stripe_customer_id VARCHAR(255),
                    stripe_subscription_id VARCHAR(255),
                    billing_email VARCHAR(255),
                    settings JSONB DEFAULT '{}'::jsonb,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # ── SaaS: Org membership ──
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS org_members (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                    user_id INTEGER NOT NULL REFERENCES dashboard_users(id) ON DELETE CASCADE,
                    org_role VARCHAR(30) NOT NULL DEFAULT 'viewer',
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(org_id, user_id)
                )
            """)
            
            # ── SaaS: Team invites ──
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS org_invites (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                    email VARCHAR(255) NOT NULL,
                    org_role VARCHAR(30) NOT NULL DEFAULT 'viewer',
                    token VARCHAR(100) UNIQUE NOT NULL,
                    invited_by INTEGER REFERENCES dashboard_users(id),
                    accepted_at TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # ── SaaS: Plans ──
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS plans (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) UNIQUE NOT NULL,
                    display_name VARCHAR(100) NOT NULL,
                    price_monthly DECIMAL(10,2) NOT NULL DEFAULT 0,
                    max_restaurants INTEGER NOT NULL DEFAULT 3,
                    max_users INTEGER NOT NULL DEFAULT 2,
                    features JSONB DEFAULT '[]'::jsonb,
                    is_active BOOLEAN DEFAULT true
                )
            """)
            
            # Seed default plans
            cursor.execute("""
                INSERT INTO plans (name, display_name, price_monthly, max_restaurants, max_users, features)
                VALUES
                    ('free', 'Gratuito', 0, 3, 2, '["dashboard","basic_analytics"]'::jsonb),
                    ('starter', 'Starter', 97, 10, 5, '["dashboard","analytics","comparativo","export","email_reports"]'::jsonb),
                    ('pro', 'Profissional', 197, 50, 15, '["dashboard","analytics","comparativo","export","email_reports","api_access","realtime","squads"]'::jsonb),
                    ('enterprise', 'Enterprise', 497, 999, 99, '["dashboard","analytics","comparativo","export","email_reports","api_access","realtime","squads","white_label","priority_support"]'::jsonb)
                ON CONFLICT (name) DO NOTHING
            """)
            
            # ── SaaS: Audit log ──
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL,
                    user_id INTEGER REFERENCES dashboard_users(id) ON DELETE SET NULL,
                    action VARCHAR(100) NOT NULL,
                    details JSONB DEFAULT '{}'::jsonb,
                    ip_address VARCHAR(45),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ── Saved views (filters/date ranges) ──
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS saved_views (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                    user_id INTEGER NOT NULL REFERENCES dashboard_users(id) ON DELETE CASCADE,
                    view_type VARCHAR(50) NOT NULL,
                    name VARCHAR(120) NOT NULL,
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    scope_id VARCHAR(100),
                    is_default BOOLEAN DEFAULT false,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(org_id, user_id, view_type, name, scope_id)
                )
            """)
            
            # ── SaaS: Per-org data snapshots ──
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS org_data_cache (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                    cache_key VARCHAR(100) NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(org_id, cache_key)
                )
            """)
            
            # ── Migration: add primary_org_id to users if missing ──
            try:
                cursor.execute("""
                    DO $$ BEGIN
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                            WHERE table_name='dashboard_users' AND column_name='primary_org_id')
                        THEN ALTER TABLE dashboard_users ADD COLUMN primary_org_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL;
                        END IF;
                    END $$;
                """)
            except Exception:
                pass
            
            # ── Migration: add org_id to existing tables ──
            for tbl in ['squads', 'client_groups', 'hidden_stores']:
                try:
                    cursor.execute(f"""
                        DO $$ BEGIN
                            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                WHERE table_name='{tbl}' AND column_name='org_id')
                            THEN ALTER TABLE {tbl} ADD COLUMN org_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE;
                            END IF;
                        END $$;
                    """)
                except Exception:
                    pass
            
            conn.commit()
            print("✅ Database tables created successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Error creating tables: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    def hash_password(self, password: str) -> str:
        """Hash password using bcrypt"""
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')
    
    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify a password against its hash"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
        except Exception as e:
            print(f"Password verification error: {e}")
            return False
    
    def create_user(self, username: str, password: str, full_name: str, 
                   email: str = None, role: str = 'user') -> Optional[int]:
        """Create a new user"""
        conn = self.get_connection()
        if not conn:
            return None
        
        cursor = conn.cursor()
        password_hash = self.hash_password(password)
        
        try:
            cursor.execute("""
                INSERT INTO dashboard_users (username, password_hash, full_name, email, role)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (username, password_hash, full_name, email, role))
            
            user_id = cursor.fetchone()[0]
            conn.commit()
            print(f"✅ User '{username}' created with ID: {user_id}")
            return user_id
            
        except psycopg2.IntegrityError:
            conn.rollback()
            print(f"⚠️  User '{username}' already exists")
            return None
        except Exception as e:
            conn.rollback()
            print(f"❌ Error creating user: {e}")
            return None
        finally:
            cursor.close()
            conn.close()
    
    def authenticate_user(self, username: str, password: str) -> Optional[Dict]:
        """Authenticate user and return user data"""
        conn = self.get_connection()
        if not conn:
            return None
        
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT id, username, full_name, email, role, last_login, password_hash
                FROM dashboard_users
                WHERE username = %s
            """, (username,))
            
            result = cursor.fetchone()
            
            if result and self.verify_password(password, result[6]):
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
            print(f"❌ Authentication error: {e}")
            return None
        finally:
            cursor.close()
            conn.close()
    
    def get_user_restaurants(self, user_id: int) -> List[Dict]:
        """Get all restaurants assigned to a user"""
        conn = self.get_connection()
        if not conn:
            return []
        
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT restaurant_id, restaurant_name, assigned_at
                FROM user_restaurants
                WHERE user_id = %s
                ORDER BY assigned_at DESC
            """, (user_id,))
            
            results = cursor.fetchall()
            return [
                {
                    'id': row[0],
                    'name': row[1],
                    'assigned_at': str(row[2])
                }
                for row in results
            ]
            
        except Exception as e:
            print(f"❌ Error fetching restaurants: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
    
    def assign_restaurant(self, user_id: int, restaurant_id: str, restaurant_name: str):
        """Assign a restaurant to a user"""
        conn = self.get_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO user_restaurants (user_id, restaurant_id, restaurant_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, restaurant_id) DO NOTHING
            """, (user_id, restaurant_id, restaurant_name))
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"❌ Error assigning restaurant: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
            
    def authenticate_user_by_email(self, email: str, password: str) -> Optional[Dict]:
        """Authenticate user by email and return user data"""
        conn = self.get_connection()
        if not conn:
            return None
        
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT id, username, full_name, email, role, last_login, password_hash
                FROM dashboard_users
                WHERE email = %s
            """, (email,))
            
            result = cursor.fetchone()
            
            if result and self.verify_password(password, result[6]):
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
            print(f"❌ Authentication error: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def create_default_users(self):
        """Create default admin and user accounts"""
        print("\n👤 Creating default users...")
        
        # Create admin user
        self.create_user(
            username='admin',
            password='admin123',
            full_name='Administrador',
            email='admin@dashboard.com',
            role='admin'
        )
        
        # Create regular user
        self.create_user(
            username='usuario',
            password='user123',
            full_name='Usuario Padrao',
            email='user@dashboard.com',
            role='user'
        )
    
    def get_all_users(self) -> List[Dict]:
        """Get all users (for admin panel)"""
        conn = self.get_connection()
        if not conn:
            return []
        
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT id, username, full_name, email, role, created_at, last_login
                FROM dashboard_users
                ORDER BY created_at DESC
            """)
            
            results = cursor.fetchall()
            return [
                {
                    'id': row[0],
                    'username': row[1],
                    'name': row[2],
                    'email': row[3],
                    'role': row[4],
                    'created_at': str(row[5]),
                    'last_login': str(row[6]) if row[6] else None
                }
                for row in results
            ]
            
        except Exception as e:
            print(f"❌ Error fetching users: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    # ================================================================
    # SaaS: ORGANIZATION CRUD
    # ================================================================

    def create_organization(self, name, owner_user_id, plan='free',
                            ifood_client_id=None, ifood_client_secret=None,
                            billing_email=None):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            base_slug = ''.join(c for c in name.lower().replace(' ','-') if c.isalnum() or c=='-')[:80]
            slug = base_slug; sfx = 0
            while True:
                cursor.execute("SELECT id FROM organizations WHERE slug=%s", (slug,))
                if not cursor.fetchone(): break
                sfx += 1; slug = f"{base_slug}-{sfx}"
            cursor.execute("SELECT max_restaurants, max_users FROM plans WHERE name=%s", (plan,))
            pr = cursor.fetchone()
            max_r, max_u = (pr[0], pr[1]) if pr else (3, 2)
            cursor.execute("""
                INSERT INTO organizations (name, slug, plan, max_restaurants, max_users,
                    ifood_client_id, ifood_client_secret, billing_email)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (name, slug, plan, max_r, max_u, ifood_client_id, ifood_client_secret, billing_email))
            org_id = cursor.fetchone()[0]
            cursor.execute("INSERT INTO org_members (org_id,user_id,org_role) VALUES (%s,%s,'owner')", (org_id, owner_user_id))
            cursor.execute("UPDATE dashboard_users SET primary_org_id=%s WHERE id=%s", (org_id, owner_user_id))
            conn.commit()
            return {'id': org_id, 'name': name, 'slug': slug, 'plan': plan, 'max_restaurants': max_r, 'max_users': max_u}
        except Exception as e:
            conn.rollback(); print(f"❌ Error creating org: {e}"); return None
        finally:
            cursor.close(); conn.close()

    def get_user_orgs(self, user_id):
        conn = self.get_connection()
        if not conn: return []
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT o.id, o.name, o.slug, o.plan, o.max_restaurants, o.max_users,
                       o.is_active, om.org_role, o.created_at
                FROM organizations o JOIN org_members om ON o.id=om.org_id
                WHERE om.user_id=%s AND o.is_active=true
                ORDER BY om.org_role='owner' DESC, o.name
            """, (user_id,))
            return [{'id':r[0],'name':r[1],'slug':r[2],'plan':r[3],'max_restaurants':r[4],
                     'max_users':r[5],'is_active':r[6],'org_role':r[7],'created_at':str(r[8])}
                    for r in cursor.fetchall()]
        except Exception as e:
            print(f"❌ get_user_orgs: {e}"); return []
        finally:
            cursor.close(); conn.close()

    def get_org_details(self, org_id):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT o.*, (SELECT COUNT(*) FROM org_members WHERE org_id=o.id) as member_count,
                    p.display_name as plan_display, p.price_monthly, p.features
                FROM organizations o LEFT JOIN plans p ON o.plan=p.name WHERE o.id=%s
            """, (org_id,))
            row = cursor.fetchone()
            if not row: return None
            cols = [d[0] for d in cursor.description]
            d = dict(zip(cols, row))
            # Serialize non-JSON types
            for k,v in d.items():
                if isinstance(v, datetime): d[k] = v.isoformat()
                elif hasattr(v, '__float__'): d[k] = float(v)
            return d
        except Exception as e:
            print(f"❌ get_org_details: {e}"); return None
        finally:
            cursor.close(); conn.close()

    def get_org_ifood_config(self, org_id):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT ifood_client_id, ifood_client_secret, ifood_merchants, settings
                FROM organizations WHERE id=%s AND is_active=true
            """, (org_id,))
            row = cursor.fetchone()
            if not row: return None
            merchants = row[2] if row[2] else []
            if isinstance(merchants, str): merchants = json.loads(merchants)
            return {'client_id':row[0],'client_secret':row[1],'merchants':merchants,'settings':row[3] or {}}
        except Exception as e:
            print(f"❌ get_org_ifood_config: {e}"); return None
        finally:
            cursor.close(); conn.close()

    def update_org_ifood_config(self, org_id, client_id=None, client_secret=None, merchants=None):
        conn = self.get_connection()
        if not conn: return False
        cursor = conn.cursor()
        try:
            updates, params = [], []
            if client_id is not None: updates.append("ifood_client_id=%s"); params.append(client_id)
            if client_secret is not None: updates.append("ifood_client_secret=%s"); params.append(client_secret)
            if merchants is not None:
                updates.append("ifood_merchants=%s::jsonb")
                params.append(merchants if isinstance(merchants,str) else json.dumps(merchants))
            if updates:
                updates.append("updated_at=CURRENT_TIMESTAMP"); params.append(org_id)
                cursor.execute(f"UPDATE organizations SET {','.join(updates)} WHERE id=%s", params)
                conn.commit()
            return True
        except Exception as e:
            conn.rollback(); print(f"❌ update_org_ifood_config: {e}"); return False
        finally:
            cursor.close(); conn.close()

    def get_all_active_orgs(self):
        conn = self.get_connection()
        if not conn: return []
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, name, slug, plan, ifood_client_id, ifood_client_secret, ifood_merchants
                FROM organizations WHERE is_active=true AND ifood_client_id IS NOT NULL
            """)
            return [{'id':r[0],'name':r[1],'slug':r[2],'plan':r[3],
                     'ifood_client_id':r[4],'ifood_client_secret':r[5],
                     'ifood_merchants':r[6] if r[6] else []} for r in cursor.fetchall()]
        except Exception as e:
            print(f"❌ get_all_active_orgs: {e}"); return []
        finally:
            cursor.close(); conn.close()

    # ================================================================
    # SaaS: TEAM INVITES
    # ================================================================

    def create_invite(self, org_id, email, org_role, invited_by):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT o.max_users, COUNT(om.id) FROM organizations o LEFT JOIN org_members om ON o.id=om.org_id WHERE o.id=%s GROUP BY o.max_users", (org_id,))
            row = cursor.fetchone()
            if row and row[1] >= row[0]: return None
            token = secrets.token_urlsafe(32)
            expires = datetime.now() + timedelta(days=7)
            cursor.execute("INSERT INTO org_invites (org_id,email,org_role,token,invited_by,expires_at) VALUES (%s,%s,%s,%s,%s,%s)", (org_id, email.lower(), org_role, token, invited_by, expires))
            conn.commit(); return token
        except Exception as e:
            conn.rollback(); print(f"❌ create_invite: {e}"); return None
        finally:
            cursor.close(); conn.close()

    def accept_invite(self, token, user_id):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, org_id, org_role, expires_at, accepted_at FROM org_invites WHERE token=%s", (token,))
            row = cursor.fetchone()
            if not row or row[4] or row[3] < datetime.now(): return None
            invite_id, org_id, org_role = row[0], row[1], row[2]
            cursor.execute("INSERT INTO org_members (org_id,user_id,org_role) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING", (org_id, user_id, org_role))
            cursor.execute("UPDATE org_invites SET accepted_at=CURRENT_TIMESTAMP WHERE id=%s", (invite_id,))
            cursor.execute("UPDATE dashboard_users SET primary_org_id=%s WHERE id=%s AND primary_org_id IS NULL", (org_id, user_id))
            conn.commit(); return {'org_id': org_id, 'org_role': org_role}
        except Exception as e:
            conn.rollback(); print(f"❌ accept_invite: {e}"); return None
        finally:
            cursor.close(); conn.close()

    # ================================================================
    # SaaS: PLAN / FEATURE GATING
    # ================================================================

    def check_feature(self, org_id, feature):
        conn = self.get_connection()
        if not conn: return False
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT p.features FROM organizations o JOIN plans p ON o.plan=p.name WHERE o.id=%s AND o.is_active=true", (org_id,))
            row = cursor.fetchone()
            if not row: return False
            features = row[0] if row[0] else []
            if isinstance(features, str): features = json.loads(features)
            return feature in features
        except: return False
        finally:
            cursor.close(); conn.close()

    def check_restaurant_limit(self, org_id):
        conn = self.get_connection()
        if not conn: return {'allowed':False,'current':0,'max':0}
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT max_restaurants, ifood_merchants FROM organizations WHERE id=%s", (org_id,))
            row = cursor.fetchone()
            if not row: return {'allowed':False,'current':0,'max':0}
            merchants = row[1] if row[1] else []
            if isinstance(merchants, str): merchants = json.loads(merchants)
            return {'allowed': len(merchants) < row[0], 'current': len(merchants), 'max': row[0]}
        except: return {'allowed':False,'current':0,'max':0}
        finally:
            cursor.close(); conn.close()

    # ================================================================
    # SaaS: SELF-SERVICE REGISTRATION
    # ================================================================

    def register_user_and_org(self, email, password, full_name, org_name):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM dashboard_users WHERE email=%s", (email.lower(),))
            if cursor.fetchone(): return None
            password_hash = self.hash_password(password)
            username = email.split('@')[0][:50]
            base = username; sfx = 0
            while True:
                cursor.execute("SELECT id FROM dashboard_users WHERE username=%s", (username,))
                if not cursor.fetchone(): break
                sfx += 1; username = f"{base}{sfx}"
            cursor.execute("INSERT INTO dashboard_users (username,password_hash,full_name,email,role) VALUES (%s,%s,%s,%s,'admin') RETURNING id", (username, password_hash, full_name, email.lower()))
            user_id = cursor.fetchone()[0]
            slug = ''.join(c for c in org_name.lower().replace(' ','-') if c.isalnum() or c=='-')[:80]
            base_slug = slug; sfx = 0
            while True:
                cursor.execute("SELECT id FROM organizations WHERE slug=%s", (slug,))
                if not cursor.fetchone(): break
                sfx += 1; slug = f"{base_slug}-{sfx}"
            cursor.execute("INSERT INTO organizations (name,slug,plan,billing_email) VALUES (%s,%s,'free',%s) RETURNING id", (org_name, slug, email.lower()))
            org_id = cursor.fetchone()[0]
            cursor.execute("INSERT INTO org_members (org_id,user_id,org_role) VALUES (%s,%s,'owner')", (org_id, user_id))
            cursor.execute("UPDATE dashboard_users SET primary_org_id=%s WHERE id=%s", (org_id, user_id))
            conn.commit()
            return {'user_id':user_id,'username':username,'org_id':org_id,'org_slug':slug,'plan':'free'}
        except Exception as e:
            conn.rollback(); print(f"❌ register: {e}"); return None
        finally:
            cursor.close(); conn.close()

    # ================================================================
    # SaaS: AUDIT LOG
    # ================================================================

    def log_action(self, action, org_id=None, user_id=None, details=None, ip_address=None):
        conn = self.get_connection()
        if not conn: return
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO audit_log (org_id,user_id,action,details,ip_address) VALUES (%s,%s,%s,%s,%s)",
                (org_id, user_id, action, json.dumps(details or {}, ensure_ascii=False, default=str), ip_address))
            conn.commit()
        except: conn.rollback()
        finally:
            cursor.close(); conn.close()

    # ================================================================
    # Saved Views
    # ================================================================

    def list_saved_views(self, org_id, user_id, view_type, scope_id=None):
        conn = self.get_connection()
        if not conn: return []
        cursor = conn.cursor()
        try:
            if scope_id is None:
                cursor.execute("""
                    SELECT id, name, payload, scope_id, is_default, created_at, updated_at
                    FROM saved_views
                    WHERE org_id=%s AND user_id=%s AND view_type=%s
                    ORDER BY is_default DESC, created_at DESC
                """, (org_id, user_id, view_type))
            else:
                cursor.execute("""
                    SELECT id, name, payload, scope_id, is_default, created_at, updated_at
                    FROM saved_views
                    WHERE org_id=%s AND user_id=%s AND view_type=%s AND scope_id=%s
                    ORDER BY is_default DESC, created_at DESC
                """, (org_id, user_id, view_type, scope_id))
            rows = cursor.fetchall()
            result = []
            for r in rows:
                payload = r[2] if r[2] else {}
                if isinstance(payload, str):
                    payload = json.loads(payload)
                result.append({
                    'id': r[0],
                    'name': r[1],
                    'payload': payload,
                    'scope_id': r[3],
                    'is_default': r[4],
                    'created_at': r[5].isoformat() if isinstance(r[5], datetime) else str(r[5]),
                    'updated_at': r[6].isoformat() if isinstance(r[6], datetime) else str(r[6])
                })
            return result
        except Exception as e:
            print(f"❌ list_saved_views: {e}")
            return []
        finally:
            cursor.close(); conn.close()

    def create_saved_view(self, org_id, user_id, view_type, name, payload, scope_id=None, is_default=False):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            if is_default:
                cursor.execute("""
                    UPDATE saved_views SET is_default=false
                    WHERE org_id=%s AND user_id=%s AND view_type=%s AND scope_id IS NOT DISTINCT FROM %s
                """, (org_id, user_id, view_type, scope_id))
            cursor.execute("""
                INSERT INTO saved_views (org_id, user_id, view_type, name, payload, scope_id, is_default)
                VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s)
                RETURNING id
            """, (org_id, user_id, view_type, name, json.dumps(payload or {}), scope_id, is_default))
            new_id = cursor.fetchone()[0]
            conn.commit()
            return new_id
        except Exception as e:
            conn.rollback(); print(f"❌ create_saved_view: {e}"); return None
        finally:
            cursor.close(); conn.close()

    def delete_saved_view(self, org_id, user_id, view_id):
        conn = self.get_connection()
        if not conn: return False
        cursor = conn.cursor()
        try:
            cursor.execute("""
                DELETE FROM saved_views
                WHERE id=%s AND org_id=%s AND user_id=%s
            """, (view_id, org_id, user_id))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback(); print(f"❌ delete_saved_view: {e}"); return False
        finally:
            cursor.close(); conn.close()

    def set_default_saved_view(self, org_id, user_id, view_id):
        conn = self.get_connection()
        if not conn: return False
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT view_type, scope_id FROM saved_views
                WHERE id=%s AND org_id=%s AND user_id=%s
            """, (view_id, org_id, user_id))
            row = cursor.fetchone()
            if not row:
                return False
            view_type, scope_id = row[0], row[1]
            cursor.execute("""
                UPDATE saved_views SET is_default=false
                WHERE org_id=%s AND user_id=%s AND view_type=%s AND scope_id IS NOT DISTINCT FROM %s
            """, (org_id, user_id, view_type, scope_id))
            cursor.execute("""
                UPDATE saved_views SET is_default=true, updated_at=CURRENT_TIMESTAMP
                WHERE id=%s AND org_id=%s AND user_id=%s
            """, (view_id, org_id, user_id))
            conn.commit()
            return True
        except Exception as e:
            conn.rollback(); print(f"❌ set_default_saved_view: {e}"); return False
        finally:
            cursor.close(); conn.close()

    # ================================================================
    # SaaS: PER-ORG DATA CACHE
    # ================================================================

    def save_org_data_cache(self, org_id, cache_key, data):
        conn = self.get_connection()
        if not conn: return
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO org_data_cache (org_id, cache_key, data) VALUES (%s,%s,%s)
                ON CONFLICT (org_id, cache_key) DO UPDATE SET data=%s, created_at=CURRENT_TIMESTAMP
            """, (org_id, cache_key, json.dumps(data, ensure_ascii=False, default=str),
                  json.dumps(data, ensure_ascii=False, default=str)))
            conn.commit()
        except Exception as e:
            conn.rollback(); print(f"⚠️ save_org_cache: {e}")
        finally:
            cursor.close(); conn.close()

    def load_org_data_cache(self, org_id, cache_key, max_age_hours=2):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT data, created_at FROM org_data_cache WHERE org_id=%s AND cache_key=%s", (org_id, cache_key))
            row = cursor.fetchone()
            if row:
                data, created = row
                if datetime.now() - created < timedelta(hours=max_age_hours):
                    if isinstance(data, str): data = json.loads(data)
                    return data
            return None
        except: return None
        finally:
            cursor.close(); conn.close()

    def get_org_member_role(self, org_id, user_id):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT org_role FROM org_members WHERE org_id=%s AND user_id=%s", (org_id, user_id))
            row = cursor.fetchone()
            return row[0] if row else None
        except: return None
        finally:
            cursor.close(); conn.close()

    def get_org_users(self, org_id):
        conn = self.get_connection()
        if not conn: return []
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT u.id, u.username, u.full_name, u.email, u.role, om.org_role, u.last_login
                FROM dashboard_users u JOIN org_members om ON u.id=om.user_id
                WHERE om.org_id=%s ORDER BY om.org_role='owner' DESC, u.full_name
            """, (org_id,))
            return [{'id':r[0],'username':r[1],'name':r[2],'email':r[3],'role':r[4],'org_role':r[5],'last_login':str(r[6]) if r[6] else None} for r in cursor.fetchall()]
        except: return []
        finally:
            cursor.close(); conn.close()


def setup_database():
    """Quick setup function"""
    print("=" * 60)
    print("Dashboard Database Setup")
    print("=" * 60)
    print()
    
    # Initialize database (will use DATABASE_URL if available)
    db = DashboardDatabase()
    
    # Create tables
    if db.setup_tables():
        # Create default users
        db.create_default_users()
        
        print("\n" + "=" * 60)
        print("✅ Database setup complete!")
        print("=" * 60)
        print("\nDefault credentials:")
        print("  Admin:  admin@dashboard.com / admin123")
        print("  User:   user@dashboard.com / user123")
    else:
        print("\n❌ Database setup failed!")


if __name__ == "__main__":
    setup_database()
