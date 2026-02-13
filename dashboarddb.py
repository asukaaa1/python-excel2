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
import sys
import secrets
from datetime import datetime, timedelta
from urllib.parse import urlparse
from typing import Optional, Dict, List

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="backslashreplace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(errors="backslashreplace")
except Exception:
    pass

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
            print(f"ðŸ“Š Using DATABASE_URL: {parsed.hostname}")
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
            print(f"ðŸ“Š Using individual DB params: {host}:{port}/{database}")
    
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
            
            # â”€â”€ SaaS: Organizations (tenants) â”€â”€
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
            
            # â”€â”€ SaaS: Org membership â”€â”€
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
            
            # â”€â”€ SaaS: Team invites â”€â”€
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
            
            # â”€â”€ SaaS: Plans â”€â”€
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

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS org_subscriptions (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                    plan_name VARCHAR(50) NOT NULL REFERENCES plans(name),
                    status VARCHAR(30) NOT NULL DEFAULT 'active',
                    billing_cycle VARCHAR(20) NOT NULL DEFAULT 'monthly',
                    current_period_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    current_period_end TIMESTAMP,
                    canceled_at TIMESTAMP,
                    ended_at TIMESTAMP,
                    changed_by INTEGER REFERENCES dashboard_users(id) ON DELETE SET NULL,
                    change_reason VARCHAR(120),
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_org_subscriptions_org_status ON org_subscriptions(org_id, status)")
             
            # Seed default plans
            cursor.execute("""
                INSERT INTO plans (name, display_name, price_monthly, max_restaurants, max_users, features)
                VALUES
                    ('free', 'Free', 0, 2, 1, '["dashboard"]'::jsonb),
                    ('starter', 'Starter', 97, 8, 3, '["dashboard","analytics","comparativo","export"]'::jsonb),
                    ('pro', 'Pro', 197, 35, 20, '["dashboard","analytics","comparativo","export","squads","public_links","pdf_reports","multiuser"]'::jsonb),
                    ('enterprise', 'Enterprise', 497, 250, 120, '["dashboard","analytics","comparativo","export","squads","public_links","pdf_reports","multiuser","advanced_customizations","on_demand_integrations","white_label"]'::jsonb)
                ON CONFLICT (name) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    price_monthly = EXCLUDED.price_monthly,
                    max_restaurants = EXCLUDED.max_restaurants,
                    max_users = EXCLUDED.max_users,
                    features = EXCLUDED.features,
                    is_active = true
            """)

            cursor.execute("""
                INSERT INTO org_subscriptions (
                    org_id, plan_name, status, billing_cycle, current_period_start, current_period_end, change_reason, metadata
                )
                SELECT
                    o.id,
                    o.plan,
                    'active',
                    'monthly',
                    COALESCE(o.plan_started_at, CURRENT_TIMESTAMP),
                    o.plan_expires_at,
                    'bootstrap',
                    jsonb_build_object('source', 'setup_tables')
                FROM organizations o
                WHERE o.is_active = true
                  AND NOT EXISTS (
                      SELECT 1 FROM org_subscriptions s
                      WHERE s.org_id = o.id AND s.status = 'active'
                  )
            """)
            
            # â”€â”€ SaaS: Audit log â”€â”€
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

            # â”€â”€ Saved views (filters/date ranges) â”€â”€
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
                    share_token VARCHAR(120) UNIQUE,
                    is_public BOOLEAN DEFAULT false,
                    expires_at TIMESTAMP,
                    shared_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(org_id, user_id, view_type, name, scope_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS group_templates (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                    name VARCHAR(120) NOT NULL,
                    description TEXT,
                    store_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_by VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(org_id, name)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS group_share_links (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                    group_id INTEGER NOT NULL REFERENCES client_groups(id) ON DELETE CASCADE,
                    token VARCHAR(120) UNIQUE NOT NULL,
                    created_by INTEGER REFERENCES dashboard_users(id) ON DELETE SET NULL,
                    expires_at TIMESTAMP,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_share_links_lookup ON group_share_links(token, is_active)")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS restaurant_share_links (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                    restaurant_id VARCHAR(100) NOT NULL,
                    token VARCHAR(120) UNIQUE NOT NULL,
                    created_by INTEGER REFERENCES dashboard_users(id) ON DELETE SET NULL,
                    expires_at TIMESTAMP,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_restaurant_share_links_lookup ON restaurant_share_links(token, is_active)")

            # â"€â"€ SaaS: Per-org data snapshots â"€â"€
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
            
            # â”€â”€ Migration: add primary_org_id to users if missing â”€â”€
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
            
            # â”€â”€ Migration: add org_id to existing tables â”€â”€
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

            for col_name, col_sql in [
                ('share_token', "VARCHAR(120)"),
                ('is_public', "BOOLEAN DEFAULT false"),
                ('expires_at', "TIMESTAMP"),
                ('shared_at', "TIMESTAMP"),
            ]:
                try:
                    cursor.execute(f"""
                        DO $$ BEGIN
                            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                WHERE table_name='saved_views' AND column_name='{col_name}')
                            THEN ALTER TABLE saved_views ADD COLUMN {col_name} {col_sql};
                            END IF;
                        END $$;
                    """)
                except Exception:
                    pass

            # Create indexes after legacy-column migrations (older DBs may miss share_token initially).
            try:
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_saved_views_token ON saved_views(share_token)")
            except Exception as index_error:
                print(f"⚠️ idx_saved_views_token migration: {index_error}")
            
            conn.commit()
            print("âœ… Database tables created successfully!")
            return True
            
        except Exception as e:
            print(f"âŒ Error creating tables: {e}")
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
            print(f"âœ… User '{username}' created with ID: {user_id}")
            return user_id
            
        except psycopg2.IntegrityError:
            conn.rollback()
            print(f"âš ï¸  User '{username}' already exists")
            return None
        except Exception as e:
            conn.rollback()
            print(f"âŒ Error creating user: {e}")
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
            print(f"âŒ Authentication error: {e}")
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
            print(f"âŒ Error fetching restaurants: {e}")
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
            print(f"âŒ Error assigning restaurant: {e}")
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
            print(f"âŒ Authentication error: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def create_default_users(self):
        """Create default admin and user accounts"""
        print("\nðŸ‘¤ Creating default users...")
        
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
        """Get all users (for platform/site admin panel)."""
        conn = self.get_connection()
        if not conn:
            return []
        
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT
                    u.id,
                    u.username,
                    u.full_name,
                    u.email,
                    u.role,
                    u.created_at,
                    u.last_login,
                    COUNT(om.org_id) AS org_count,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'id', o.id,
                                'name', o.name,
                                'org_role', om.org_role
                            )
                            ORDER BY o.name
                        ) FILTER (WHERE o.id IS NOT NULL),
                        '[]'::json
                    ) AS organizations
                FROM dashboard_users u
                LEFT JOIN org_members om ON om.user_id = u.id
                LEFT JOIN organizations o ON o.id = om.org_id
                GROUP BY u.id, u.username, u.full_name, u.email, u.role, u.created_at, u.last_login
                ORDER BY u.created_at DESC
            """)
            
            results = cursor.fetchall()
            def _decode_orgs(raw_orgs):
                if isinstance(raw_orgs, list):
                    return raw_orgs
                if isinstance(raw_orgs, str):
                    try:
                        decoded = json.loads(raw_orgs)
                        return decoded if isinstance(decoded, list) else []
                    except Exception:
                        return []
                return []
            return [
                {
                    'id': row[0],
                    'username': row[1],
                    'name': row[2],
                    'email': row[3],
                    'role': row[4],
                    'created_at': str(row[5]),
                    'last_login': str(row[6]) if row[6] else None,
                    'org_count': int(row[7] or 0),
                    'organizations': _decode_orgs(row[8])
                }
                for row in results
            ]
            
        except Exception as e:
            print(f"âŒ Error fetching users: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def update_user_global_role(self, user_id: int, role: str, acting_user_id: Optional[int] = None) -> Dict:
        """Update the global role for a user (platform/site admin operation)."""
        conn = self.get_connection()
        if not conn:
            return {'success': False, 'error': 'db_unavailable'}

        cursor = conn.cursor()
        try:
            target_role = str(role or '').strip().lower()
            if target_role not in ('user', 'admin', 'site_admin'):
                return {'success': False, 'error': 'invalid_role'}

            cursor.execute("SELECT id, role FROM dashboard_users WHERE id=%s", (user_id,))
            row = cursor.fetchone()
            if not row:
                return {'success': False, 'error': 'user_not_found'}

            current_role = str(row[1] or '').strip().lower()

            same_user = False
            try:
                same_user = (
                    acting_user_id is not None
                    and int(acting_user_id) == int(user_id)
                )
            except Exception:
                same_user = False

            if same_user:
                return {'success': False, 'error': 'cannot_update_own_role'}

            if current_role == 'site_admin' and target_role != 'site_admin':
                cursor.execute("SELECT COUNT(*) FROM dashboard_users WHERE LOWER(role) = 'site_admin'")
                site_admin_count = int(cursor.fetchone()[0] or 0)
                if site_admin_count <= 1:
                    return {'success': False, 'error': 'cannot_demote_last_site_admin'}

            if current_role == target_role:
                return {'success': True, 'role': target_role, 'changed': False}

            cursor.execute(
                "UPDATE dashboard_users SET role=%s WHERE id=%s",
                (target_role, user_id)
            )
            conn.commit()
            return {'success': True, 'role': target_role, 'changed': True}
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            cursor.close()
            conn.close()

    def is_platform_admin(self, user_id: int) -> bool:
        """Return True when a user has global site/platform-admin privileges."""
        conn = self.get_connection()
        if not conn:
            return False

        cursor = conn.cursor()
        try:
            cursor.execute("SELECT role, email FROM dashboard_users WHERE id = %s", (user_id,))
            row = cursor.fetchone()
            if not row:
                return False

            role = str(row[0] or '').strip().lower()
            if role == 'site_admin':
                return True
            if role != 'admin':
                return False

            allowed_admins_raw = (os.environ.get('PLATFORM_ADMIN_EMAILS') or '').strip()
            if not allowed_admins_raw:
                return str(os.environ.get('ALLOW_ANY_PLATFORM_ADMIN', '')).strip().lower() in ('1', 'true', 'yes', 'on')

            allowed_admins = {
                email.strip().lower()
                for email in allowed_admins_raw.split(',')
                if email.strip()
            }
            user_email = (row[1] or '').strip().lower()
            return user_email in allowed_admins
        except Exception:
            return False
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
            conn.rollback(); print(f"âŒ Error creating org: {e}"); return None
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
            print(f"âŒ get_user_orgs: {e}"); return []
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
            print(f"âŒ get_org_details: {e}"); return None
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
            print(f"âŒ get_org_ifood_config: {e}"); return None
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
            conn.rollback(); print(f"âŒ update_org_ifood_config: {e}"); return False
        finally:
            cursor.close(); conn.close()

    def get_org_settings(self, org_id):
        conn = self.get_connection()
        if not conn:
            return {}
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT settings FROM organizations WHERE id=%s", (org_id,))
            row = cursor.fetchone()
            settings = row[0] if row else {}
            if isinstance(settings, str):
                settings = json.loads(settings)
            return settings if isinstance(settings, dict) else {}
        except Exception as e:
            print(f"âŒ get_org_settings: {e}")
            return {}
        finally:
            cursor.close(); conn.close()

    def update_org_settings(self, org_id, settings_patch=None, replace=False):
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        try:
            current = self.get_org_settings(org_id)
            if replace:
                next_settings = settings_patch if isinstance(settings_patch, dict) else {}
            else:
                next_settings = dict(current or {})
                if isinstance(settings_patch, dict):
                    next_settings.update(settings_patch)

            cursor.execute("""
                UPDATE organizations
                SET settings=%s::jsonb, updated_at=CURRENT_TIMESTAMP
                WHERE id=%s
            """, (json.dumps(next_settings, ensure_ascii=False, default=str), org_id))
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            print(f"âŒ update_org_settings: {e}")
            return False
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
            print(f"âŒ get_all_active_orgs: {e}"); return []
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
            role = (org_role or 'viewer').strip().lower()
            if role not in ('viewer', 'admin'):
                return None
            cursor.execute("SELECT o.max_users, COUNT(om.id) FROM organizations o LEFT JOIN org_members om ON o.id=om.org_id WHERE o.id=%s GROUP BY o.max_users", (org_id,))
            row = cursor.fetchone()
            if row and row[1] >= row[0]: return None
            token = secrets.token_urlsafe(32)
            expires = datetime.now() + timedelta(days=7)
            cursor.execute("INSERT INTO org_invites (org_id,email,org_role,token,invited_by,expires_at) VALUES (%s,%s,%s,%s,%s,%s)", (org_id, email.lower(), role, token, invited_by, expires))
            conn.commit(); return token
        except Exception as e:
            conn.rollback(); print(f"âŒ create_invite: {e}"); return None
        finally:
            cursor.close(); conn.close()

    def accept_invite(self, token, user_id):
        conn = self.get_connection()
        if not conn:
            return {'success': False, 'error': 'db_unavailable'}
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT id, org_id, org_role, expires_at, accepted_at, email FROM org_invites WHERE token=%s",
                (token,)
            )
            row = cursor.fetchone()
            if not row:
                return {'success': False, 'error': 'invite_not_found'}
            if row[4]:
                return {'success': False, 'error': 'invite_already_accepted'}
            if row[3] < datetime.now():
                return {'success': False, 'error': 'invite_expired'}

            invite_id, org_id, org_role = row[0], row[1], row[2]
            invited_email = (row[5] or '').strip().lower()
            cursor.execute("SELECT email FROM dashboard_users WHERE id=%s", (user_id,))
            user_row = cursor.fetchone()
            user_email = (user_row[0] or '').strip().lower() if user_row else ''
            if not user_email or user_email != invited_email:
                return {'success': False, 'error': 'invite_email_mismatch'}

            if org_role not in ('viewer', 'admin', 'owner'):
                org_role = 'viewer'
            cursor.execute(
                "INSERT INTO org_members (org_id,user_id,org_role) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (org_id, user_id, org_role)
            )
            cursor.execute("UPDATE org_invites SET accepted_at=CURRENT_TIMESTAMP WHERE id=%s", (invite_id,))
            cursor.execute(
                "UPDATE dashboard_users SET primary_org_id=%s WHERE id=%s AND primary_org_id IS NULL",
                (org_id, user_id)
            )
            conn.commit()
            return {'success': True, 'org_id': org_id, 'org_role': org_role}
        except Exception as e:
            conn.rollback()
            print(f"accept_invite error: {e}")
            return {'success': False, 'error': 'invite_accept_failed'}
        finally:
            cursor.close(); conn.close()

    # ================================================================
    # SaaS: PLAN / FEATURE GATING
    # ================================================================

    def list_active_plans(self, include_free=False):
        conn = self.get_connection()
        if not conn:
            return []
        cursor = conn.cursor()
        try:
            if include_free:
                cursor.execute("""
                    SELECT name, display_name, price_monthly, max_restaurants, max_users, features
                    FROM plans
                    WHERE is_active=true
                    ORDER BY price_monthly, id
                """)
            else:
                cursor.execute("""
                    SELECT name, display_name, price_monthly, max_restaurants, max_users, features
                    FROM plans
                    WHERE is_active=true AND name <> 'free'
                    ORDER BY price_monthly, id
                """)

            plans = []
            for row in cursor.fetchall():
                features = row[5] if row[5] else []
                if isinstance(features, str):
                    features = json.loads(features)
                plans.append({
                    'name': row[0],
                    'display_name': row[1],
                    'price_monthly': float(row[2] or 0),
                    'max_restaurants': int(row[3] or 0),
                    'max_users': int(row[4] or 0),
                    'features': features
                })
            return plans
        except Exception:
            return []
        finally:
            cursor.close()
            conn.close()

    def get_plan(self, plan_name):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT name, display_name, price_monthly, max_restaurants, max_users, features
                FROM plans
                WHERE name=%s AND is_active=true
                LIMIT 1
            """, (plan_name,))
            row = cursor.fetchone()
            if not row:
                return None
            features = row[5] if row[5] else []
            if isinstance(features, str):
                features = json.loads(features)
            return {
                'name': row[0],
                'display_name': row[1],
                'price_monthly': float(row[2] or 0),
                'max_restaurants': int(row[3] or 0),
                'max_users': int(row[4] or 0),
                'features': features
            }
        except Exception:
            return None
        finally:
            cursor.close()
            conn.close()

    def check_user_limit(self, org_id):
        conn = self.get_connection()
        if not conn:
            return {'allowed': False, 'current': 0, 'max': 0}
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT max_users FROM organizations WHERE id=%s", (org_id,))
            row = cursor.fetchone()
            if not row:
                return {'allowed': False, 'current': 0, 'max': 0}
            max_users = int(row[0] or 0)
            cursor.execute("SELECT COUNT(*) FROM org_members WHERE org_id=%s", (org_id,))
            current_users = int(cursor.fetchone()[0] or 0)
            return {'allowed': current_users < max_users, 'current': current_users, 'max': max_users}
        except Exception:
            return {'allowed': False, 'current': 0, 'max': 0}
        finally:
            cursor.close()
            conn.close()

    def get_org_subscription(self, org_id):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    s.id, s.org_id, s.plan_name, s.status, s.billing_cycle,
                    s.current_period_start, s.current_period_end, s.canceled_at,
                    s.ended_at, s.change_reason, s.metadata, s.created_at,
                    p.display_name, p.price_monthly
                FROM org_subscriptions s
                LEFT JOIN plans p ON p.name = s.plan_name
                WHERE s.org_id=%s AND s.status='active'
                ORDER BY s.created_at DESC
                LIMIT 1
            """, (org_id,))
            row = cursor.fetchone()
            if not row:
                cursor.execute("""
                    SELECT o.plan, o.plan_started_at, o.plan_expires_at, p.display_name, p.price_monthly
                    FROM organizations o
                    LEFT JOIN plans p ON p.name = o.plan
                    WHERE o.id=%s
                    LIMIT 1
                """, (org_id,))
                fallback = cursor.fetchone()
                if not fallback:
                    return None
                return {
                    'id': None,
                    'org_id': org_id,
                    'plan_name': fallback[0],
                    'status': 'active',
                    'billing_cycle': 'monthly',
                    'current_period_start': fallback[1].isoformat() if fallback[1] else None,
                    'current_period_end': fallback[2].isoformat() if fallback[2] else None,
                    'canceled_at': None,
                    'ended_at': None,
                    'change_reason': 'legacy',
                    'metadata': {},
                    'created_at': fallback[1].isoformat() if fallback[1] else None,
                    'plan_display': fallback[3] or fallback[0],
                    'price_monthly': float(fallback[4] or 0)
                }

            metadata = row[10] if row[10] else {}
            if isinstance(metadata, str):
                metadata = json.loads(metadata)

            return {
                'id': row[0],
                'org_id': row[1],
                'plan_name': row[2],
                'status': row[3],
                'billing_cycle': row[4],
                'current_period_start': row[5].isoformat() if row[5] else None,
                'current_period_end': row[6].isoformat() if row[6] else None,
                'canceled_at': row[7].isoformat() if row[7] else None,
                'ended_at': row[8].isoformat() if row[8] else None,
                'change_reason': row[9],
                'metadata': metadata,
                'created_at': row[11].isoformat() if row[11] else None,
                'plan_display': row[12] or row[2],
                'price_monthly': float(row[13] or 0)
            }
        except Exception:
            return None
        finally:
            cursor.close()
            conn.close()

    def list_org_subscription_history(self, org_id, limit=12):
        conn = self.get_connection()
        if not conn:
            return []
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    s.id, s.plan_name, s.status, s.billing_cycle, s.current_period_start,
                    s.current_period_end, s.canceled_at, s.ended_at, s.change_reason, s.created_at,
                    p.display_name, p.price_monthly
                FROM org_subscriptions s
                LEFT JOIN plans p ON p.name = s.plan_name
                WHERE s.org_id=%s
                ORDER BY s.created_at DESC
                LIMIT %s
            """, (org_id, int(limit)))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                result.append({
                    'id': row[0],
                    'plan_name': row[1],
                    'status': row[2],
                    'billing_cycle': row[3],
                    'current_period_start': row[4].isoformat() if row[4] else None,
                    'current_period_end': row[5].isoformat() if row[5] else None,
                    'canceled_at': row[6].isoformat() if row[6] else None,
                    'ended_at': row[7].isoformat() if row[7] else None,
                    'change_reason': row[8],
                    'created_at': row[9].isoformat() if row[9] else None,
                    'plan_display': row[10] or row[1],
                    'price_monthly': float(row[11] or 0)
                })
            return result
        except Exception:
            return []
        finally:
            cursor.close()
            conn.close()

    def change_org_plan(self, org_id, new_plan, changed_by=None, reason='manual_change'):
        conn = self.get_connection()
        if not conn:
            return {'success': False, 'error': 'db_unavailable'}
        cursor = conn.cursor()
        try:
            plan_name = (new_plan or '').strip().lower()
            if not plan_name:
                return {'success': False, 'error': 'invalid_plan'}

            cursor.execute("""
                SELECT id, plan, max_restaurants, max_users, ifood_merchants
                FROM organizations
                WHERE id=%s
                FOR UPDATE
            """, (org_id,))
            org_row = cursor.fetchone()
            if not org_row:
                return {'success': False, 'error': 'organization_not_found'}

            current_plan = org_row[1]
            merchants = org_row[4] if org_row[4] else []
            if isinstance(merchants, str):
                merchants = json.loads(merchants)
            merchants_count = len(merchants)

            cursor.execute("""
                SELECT name, display_name, price_monthly, max_restaurants, max_users, features
                FROM plans
                WHERE name=%s AND is_active=true
                LIMIT 1
            """, (plan_name,))
            plan_row = cursor.fetchone()
            if not plan_row:
                return {'success': False, 'error': 'plan_not_found'}

            plan_features = plan_row[5] if plan_row[5] else []
            if isinstance(plan_features, str):
                plan_features = json.loads(plan_features)

            target_max_restaurants = int(plan_row[3] or 0)
            target_max_users = int(plan_row[4] or 0)

            cursor.execute("SELECT COUNT(*) FROM org_members WHERE org_id=%s", (org_id,))
            users_count = int(cursor.fetchone()[0] or 0)

            if merchants_count > target_max_restaurants:
                return {
                    'success': False,
                    'error': 'restaurant_limit_exceeded',
                    'current_restaurants': merchants_count,
                    'plan_max_restaurants': target_max_restaurants
                }

            if users_count > target_max_users:
                return {
                    'success': False,
                    'error': 'user_limit_exceeded',
                    'current_users': users_count,
                    'plan_max_users': target_max_users
                }

            if current_plan == plan_name:
                cursor.execute("""
                    SELECT id FROM org_subscriptions
                    WHERE org_id=%s AND status='active'
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (org_id,))
                active_row = cursor.fetchone()
                if not active_row:
                    cursor.execute("""
                        INSERT INTO org_subscriptions (
                            org_id, plan_name, status, billing_cycle, current_period_start,
                            current_period_end, changed_by, change_reason, metadata
                        )
                        VALUES (%s, %s, 'active', 'monthly', CURRENT_TIMESTAMP, %s, %s, %s, %s::jsonb)
                    """, (
                        org_id, plan_name, datetime.now() + timedelta(days=30), changed_by,
                        (reason or 'manual_change')[:120],
                        json.dumps({'source': 'backfill_on_change'}, ensure_ascii=False)
                    ))
                    conn.commit()

                return {
                    'success': True,
                    'changed': False,
                    'previous_plan': current_plan,
                    'plan': {
                        'name': plan_row[0],
                        'display_name': plan_row[1],
                        'price_monthly': float(plan_row[2] or 0),
                        'max_restaurants': target_max_restaurants,
                        'max_users': target_max_users,
                        'features': plan_features
                    },
                    'usage': {
                        'users': users_count,
                        'restaurants': merchants_count
                    }
                }

            period_end = datetime.now() + timedelta(days=30)
            metadata = {
                'price_monthly': float(plan_row[2] or 0),
                'features': plan_features
            }

            cursor.execute("""
                UPDATE org_subscriptions
                SET status='replaced', ended_at=CURRENT_TIMESTAMP
                WHERE org_id=%s AND status='active'
            """, (org_id,))

            cursor.execute("""
                INSERT INTO org_subscriptions (
                    org_id, plan_name, status, billing_cycle, current_period_start,
                    current_period_end, changed_by, change_reason, metadata
                )
                VALUES (%s, %s, 'active', 'monthly', CURRENT_TIMESTAMP, %s, %s, %s, %s::jsonb)
            """, (
                org_id, plan_name, period_end, changed_by,
                (reason or 'manual_change')[:120],
                json.dumps(metadata, ensure_ascii=False)
            ))

            cursor.execute("""
                UPDATE organizations
                SET
                    plan=%s,
                    max_restaurants=%s,
                    max_users=%s,
                    plan_started_at=CURRENT_TIMESTAMP,
                    plan_expires_at=%s,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=%s
            """, (plan_name, target_max_restaurants, target_max_users, period_end, org_id))

            conn.commit()
            return {
                'success': True,
                'changed': current_plan != plan_name,
                'previous_plan': current_plan,
                'plan': {
                    'name': plan_row[0],
                    'display_name': plan_row[1],
                    'price_monthly': float(plan_row[2] or 0),
                    'max_restaurants': target_max_restaurants,
                    'max_users': target_max_users,
                    'features': plan_features
                },
                'usage': {
                    'users': users_count,
                    'restaurants': merchants_count
                }
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            cursor.close()
            conn.close()

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
            cursor.execute("INSERT INTO dashboard_users (username,password_hash,full_name,email,role) VALUES (%s,%s,%s,%s,'user') RETURNING id", (username, password_hash, full_name, email.lower()))
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
            conn.rollback(); print(f"âŒ register: {e}"); return None
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
                    SELECT id, name, payload, scope_id, is_default, created_at, updated_at,
                           share_token, is_public, expires_at, shared_at
                    FROM saved_views
                    WHERE org_id=%s AND user_id=%s AND view_type=%s
                    ORDER BY is_default DESC, created_at DESC
                """, (org_id, user_id, view_type))
            else:
                cursor.execute("""
                    SELECT id, name, payload, scope_id, is_default, created_at, updated_at,
                           share_token, is_public, expires_at, shared_at
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
                    'updated_at': r[6].isoformat() if isinstance(r[6], datetime) else str(r[6]),
                    'share_token': r[7],
                    'is_public': bool(r[8]),
                    'expires_at': r[9].isoformat() if isinstance(r[9], datetime) else (str(r[9]) if r[9] else None),
                    'shared_at': r[10].isoformat() if isinstance(r[10], datetime) else (str(r[10]) if r[10] else None)
                })
            return result
        except Exception as e:
            print(f"âŒ list_saved_views: {e}")
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
            conn.rollback(); print(f"âŒ create_saved_view: {e}"); return None
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
            conn.rollback(); print(f"âŒ delete_saved_view: {e}"); return False
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
            conn.rollback(); print(f"âŒ set_default_saved_view: {e}"); return False
        finally:
            cursor.close(); conn.close()

    def create_saved_view_share_link(self, org_id, user_id, view_id, expires_hours=168):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id FROM saved_views
                WHERE id=%s AND org_id=%s AND user_id=%s
            """, (view_id, org_id, user_id))
            if not cursor.fetchone():
                return None

            token = secrets.token_urlsafe(24)
            expires_at = datetime.utcnow() + timedelta(hours=max(1, int(expires_hours or 168)))
            cursor.execute("""
                UPDATE saved_views
                SET share_token=%s,
                    is_public=true,
                    expires_at=%s,
                    shared_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=%s AND org_id=%s AND user_id=%s
            """, (token, expires_at, view_id, org_id, user_id))
            conn.commit()
            return {
                'token': token,
                'expires_at': expires_at.isoformat()
            }
        except Exception as e:
            conn.rollback()
            print(f"âŒ create_saved_view_share_link: {e}")
            return None
        finally:
            cursor.close(); conn.close()

    def revoke_saved_view_share_link(self, org_id, user_id, view_id):
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE saved_views
                SET share_token=NULL,
                    is_public=false,
                    expires_at=NULL,
                    shared_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=%s AND org_id=%s AND user_id=%s
            """, (view_id, org_id, user_id))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            print(f"âŒ revoke_saved_view_share_link: {e}")
            return False
        finally:
            cursor.close(); conn.close()

    def get_saved_view_by_share_token(self, token):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, org_id, user_id, view_type, name, payload, scope_id, expires_at
                FROM saved_views
                WHERE share_token=%s AND is_public=true
                LIMIT 1
            """, (token,))
            row = cursor.fetchone()
            if not row:
                return None
            expires_at = row[7]
            if expires_at and expires_at < datetime.utcnow():
                return None
            payload = row[5] if row[5] else {}
            if isinstance(payload, str):
                payload = json.loads(payload)
            return {
                'id': row[0],
                'org_id': row[1],
                'user_id': row[2],
                'view_type': row[3],
                'name': row[4],
                'payload': payload,
                'scope_id': row[6],
                'expires_at': expires_at.isoformat() if isinstance(expires_at, datetime) else (str(expires_at) if expires_at else None)
            }
        except Exception as e:
            print(f"âŒ get_saved_view_by_share_token: {e}")
            return None
        finally:
            cursor.close(); conn.close()

    def list_group_templates(self, org_id):
        conn = self.get_connection()
        if not conn:
            return []
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, name, description, store_ids, created_by, created_at, updated_at
                FROM group_templates
                WHERE org_id=%s
                ORDER BY name
            """, (org_id,))
            rows = cursor.fetchall()
            result = []
            for r in rows:
                store_ids = r[3] if r[3] else []
                if isinstance(store_ids, str):
                    store_ids = json.loads(store_ids)
                result.append({
                    'id': r[0],
                    'name': r[1],
                    'description': r[2],
                    'store_ids': store_ids,
                    'created_by': r[4],
                    'created_at': r[5].isoformat() if isinstance(r[5], datetime) else str(r[5]),
                    'updated_at': r[6].isoformat() if isinstance(r[6], datetime) else str(r[6])
                })
            return result
        except Exception as e:
            print(f"âŒ list_group_templates: {e}")
            return []
        finally:
            cursor.close(); conn.close()

    def create_group_template(self, org_id, name, store_ids, created_by=None, description=None):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO group_templates (org_id, name, description, store_ids, created_by)
                VALUES (%s, %s, %s, %s::jsonb, %s)
                RETURNING id
            """, (org_id, name, description, json.dumps(store_ids or []), created_by))
            template_id = cursor.fetchone()[0]
            conn.commit()
            return template_id
        except Exception as e:
            conn.rollback()
            print(f"âŒ create_group_template: {e}")
            return None
        finally:
            cursor.close(); conn.close()

    def delete_group_template(self, org_id, template_id):
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM group_templates WHERE id=%s AND org_id=%s", (template_id, org_id))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            print(f"âŒ delete_group_template: {e}")
            return False
        finally:
            cursor.close(); conn.close()

    def get_group_template(self, org_id, template_id):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, name, description, store_ids
                FROM group_templates
                WHERE id=%s AND org_id=%s
            """, (template_id, org_id))
            row = cursor.fetchone()
            if not row:
                return None
            store_ids = row[3] if row[3] else []
            if isinstance(store_ids, str):
                store_ids = json.loads(store_ids)
            return {'id': row[0], 'name': row[1], 'description': row[2], 'store_ids': store_ids}
        except Exception as e:
            print(f"âŒ get_group_template: {e}")
            return None
        finally:
            cursor.close(); conn.close()

    def list_group_share_links(self, org_id, group_id):
        conn = self.get_connection()
        if not conn:
            return []
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, token, expires_at, is_active, created_at, created_by
                FROM group_share_links
                WHERE org_id=%s AND group_id=%s
                ORDER BY created_at DESC
            """, (org_id, group_id))
            rows = cursor.fetchall()
            return [{
                'id': r[0],
                'token': r[1],
                'expires_at': r[2].isoformat() if isinstance(r[2], datetime) else (str(r[2]) if r[2] else None),
                'is_active': bool(r[3]),
                'created_at': r[4].isoformat() if isinstance(r[4], datetime) else str(r[4]),
                'created_by': r[5]
            } for r in rows]
        except Exception as e:
            print(f"âŒ list_group_share_links: {e}")
            return []
        finally:
            cursor.close(); conn.close()

    def create_group_share_link(self, org_id, group_id, created_by=None, expires_hours=168):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            token = secrets.token_urlsafe(24)
            expires_at = datetime.utcnow() + timedelta(hours=max(1, int(expires_hours or 168)))
            cursor.execute("""
                INSERT INTO group_share_links (org_id, group_id, token, created_by, expires_at, is_active)
                VALUES (%s, %s, %s, %s, %s, true)
                RETURNING id
            """, (org_id, group_id, token, created_by, expires_at))
            link_id = cursor.fetchone()[0]
            conn.commit()
            return {'id': link_id, 'token': token, 'expires_at': expires_at.isoformat()}
        except Exception as e:
            conn.rollback()
            print(f"âŒ create_group_share_link: {e}")
            return None
        finally:
            cursor.close(); conn.close()

    def revoke_group_share_link(self, org_id, group_id, link_id):
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE group_share_links
                SET is_active=false
                WHERE id=%s AND org_id=%s AND group_id=%s
            """, (link_id, org_id, group_id))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            print(f"âŒ revoke_group_share_link: {e}")
            return False
        finally:
            cursor.close(); conn.close()

    def get_group_by_share_token(self, token):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT gsl.group_id, gsl.org_id, gsl.expires_at, cg.name, cg.slug, cg.active
                FROM group_share_links gsl
                JOIN client_groups cg ON cg.id = gsl.group_id
                WHERE gsl.token=%s AND gsl.is_active=true
                LIMIT 1
            """, (token,))
            row = cursor.fetchone()
            if not row:
                return None
            expires_at = row[2]
            if expires_at and expires_at < datetime.utcnow():
                return None
            return {
                'group_id': row[0],
                'org_id': row[1],
                'expires_at': expires_at.isoformat() if isinstance(expires_at, datetime) else (str(expires_at) if expires_at else None),
                'group_name': row[3],
                'group_slug': row[4],
                'group_active': bool(row[5])
            }
        except Exception as e:
            print(f"âŒ get_group_by_share_token: {e}")
            return None
        finally:
            cursor.close(); conn.close()

    # ================================================================
    # SaaS: RESTAURANT SHARE LINKS
    # ================================================================

    def create_restaurant_share_link(self, org_id, restaurant_id, created_by=None, expires_hours=168):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            token = secrets.token_urlsafe(24)
            expires_hours = max(1, min(int(expires_hours or 168), 2160))
            expires_at = datetime.utcnow() + timedelta(hours=expires_hours)
            cursor.execute("""
                INSERT INTO restaurant_share_links (org_id, restaurant_id, token, created_by, expires_at, is_active)
                VALUES (%s, %s, %s, %s, %s, true)
                RETURNING id
            """, (org_id, restaurant_id, token, created_by, expires_at))
            link_id = cursor.fetchone()[0]
            conn.commit()
            return {'id': link_id, 'token': token, 'expires_at': expires_at.isoformat()}
        except Exception as e:
            conn.rollback()
            print(f"create_restaurant_share_link error: {e}")
            return None
        finally:
            cursor.close(); conn.close()

    def get_restaurant_by_share_token(self, token):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT restaurant_id, org_id, expires_at
                FROM restaurant_share_links
                WHERE token=%s AND is_active=true
                LIMIT 1
            """, (token,))
            row = cursor.fetchone()
            if not row:
                return None
            expires_at = row[2]
            if expires_at and expires_at < datetime.utcnow():
                return None
            return {
                'restaurant_id': row[0],
                'org_id': row[1],
                'expires_at': expires_at.isoformat() if isinstance(expires_at, datetime) else (str(expires_at) if expires_at else None)
            }
        except Exception as e:
            print(f"get_restaurant_by_share_token error: {e}")
            return None
        finally:
            cursor.close(); conn.close()

    def list_restaurant_share_links(self, org_id, restaurant_id):
        conn = self.get_connection()
        if not conn:
            return []
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, token, expires_at, is_active, created_at, created_by
                FROM restaurant_share_links
                WHERE org_id=%s AND restaurant_id=%s
                ORDER BY created_at DESC
            """, (org_id, restaurant_id))
            rows = cursor.fetchall()
            return [{
                'id': r[0],
                'token': r[1],
                'expires_at': r[2].isoformat() if isinstance(r[2], datetime) else (str(r[2]) if r[2] else None),
                'is_active': bool(r[3]),
                'created_at': r[4].isoformat() if isinstance(r[4], datetime) else str(r[4]),
                'created_by': r[5]
            } for r in rows]
        except Exception as e:
            print(f"list_restaurant_share_links error: {e}")
            return []
        finally:
            cursor.close(); conn.close()

    def revoke_restaurant_share_link(self, org_id, restaurant_id, link_id):
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE restaurant_share_links
                SET is_active=false
                WHERE id=%s AND org_id=%s AND restaurant_id=%s
            """, (link_id, org_id, restaurant_id))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            print(f"revoke_restaurant_share_link error: {e}")
            return False
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
            conn.rollback(); print(f"âš ï¸ save_org_cache: {e}")
        finally:
            cursor.close(); conn.close()

    def load_org_data_cache_meta(self, org_id, cache_key, max_age_hours=2):
        conn = self.get_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT data, created_at FROM org_data_cache WHERE org_id=%s AND cache_key=%s", (org_id, cache_key))
            row = cursor.fetchone()
            if row:
                data, created = row
                if datetime.now() - created < timedelta(hours=max_age_hours):
                    if isinstance(data, str):
                        data = json.loads(data)
                    return {'data': data, 'created_at': created}
            return None
        except:
            return None
        finally:
            cursor.close()
            conn.close()

    def list_all_organizations(self) -> List[Dict]:
        """List all organizations (global view for site/platform admins)."""
        conn = self.get_connection()
        if not conn:
            return []

        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    o.id,
                    o.name,
                    o.slug,
                    o.plan,
                    COALESCE(p.display_name, o.plan) AS plan_display,
                    o.max_restaurants,
                    o.max_users,
                    o.is_active,
                    o.created_at,
                    COUNT(om.user_id) AS users_count,
                    COALESCE(jsonb_array_length(o.ifood_merchants), 0) AS merchants_count
                FROM organizations o
                LEFT JOIN plans p ON p.name = o.plan
                LEFT JOIN org_members om ON om.org_id = o.id
                GROUP BY o.id, o.name, o.slug, o.plan, p.display_name, o.max_restaurants, o.max_users, o.is_active, o.created_at, o.ifood_merchants
                ORDER BY o.name ASC
            """)

            orgs = []
            for row in cursor.fetchall():
                orgs.append({
                    'id': row[0],
                    'name': row[1],
                    'slug': row[2],
                    'plan': row[3],
                    'plan_display': row[4],
                    'max_restaurants': int(row[5] or 0),
                    'max_users': int(row[6] or 0),
                    'is_active': bool(row[7]),
                    'created_at': str(row[8]) if row[8] else None,
                    'users_count': int(row[9] or 0),
                    'merchants_count': int(row[10] or 0),
                    'org_role': 'site_admin'
                })
            return orgs
        except Exception as e:
            print(f"❌ list_all_organizations: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def load_org_data_cache(self, org_id, cache_key, max_age_hours=2):
        cache_meta = self.load_org_data_cache_meta(org_id, cache_key, max_age_hours=max_age_hours)
        if isinstance(cache_meta, dict):
            return cache_meta.get('data')
        return None

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

    def list_users_not_in_org(self, org_id):
        conn = self.get_connection()
        if not conn: return []
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT u.id, u.username, u.full_name, u.email, u.role
                FROM dashboard_users u
                WHERE NOT EXISTS (
                    SELECT 1 FROM org_members om
                    WHERE om.org_id=%s AND om.user_id=u.id
                )
                ORDER BY u.full_name NULLS LAST, u.username
            """, (org_id,))
            return [{
                'id': r[0],
                'username': r[1],
                'name': r[2],
                'email': r[3],
                'role': r[4]
            } for r in cursor.fetchall()]
        except Exception:
            return []
        finally:
            cursor.close(); conn.close()

    def assign_user_to_org(self, org_id, user_id, org_role='viewer'):
        conn = self.get_connection()
        if not conn:
            return {'success': False, 'error': 'db_unavailable'}
        cursor = conn.cursor()
        try:
            role = (org_role or 'viewer').strip().lower()
            if role not in ('owner', 'admin', 'viewer'):
                return {'success': False, 'error': 'invalid_org_role'}

            cursor.execute("SELECT id FROM organizations WHERE id=%s AND is_active=true", (org_id,))
            if not cursor.fetchone():
                return {'success': False, 'error': 'org_not_found'}

            cursor.execute("SELECT id, primary_org_id FROM dashboard_users WHERE id=%s", (user_id,))
            user_row = cursor.fetchone()
            if not user_row:
                return {'success': False, 'error': 'user_not_found'}

            cursor.execute("SELECT 1 FROM org_members WHERE org_id=%s AND user_id=%s", (org_id, user_id))
            if cursor.fetchone():
                return {'success': False, 'error': 'already_member'}

            cursor.execute("SELECT max_users FROM organizations WHERE id=%s", (org_id,))
            max_users = int(cursor.fetchone()[0] or 0)
            cursor.execute("SELECT COUNT(*) FROM org_members WHERE org_id=%s", (org_id,))
            current_users = int(cursor.fetchone()[0] or 0)
            if current_users >= max_users:
                return {
                    'success': False,
                    'error': 'user_limit_exceeded',
                    'current_users': current_users,
                    'max_users': max_users
                }

            cursor.execute("""
                INSERT INTO org_members (org_id, user_id, org_role)
                VALUES (%s, %s, %s)
                ON CONFLICT (org_id, user_id) DO NOTHING
            """, (org_id, user_id, role))

            if user_row[1] is None:
                cursor.execute("UPDATE dashboard_users SET primary_org_id=%s WHERE id=%s", (org_id, user_id))

            conn.commit()
            return {'success': True, 'org_role': role}
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            cursor.close(); conn.close()

    def update_org_member_role(self, org_id, user_id, org_role, acting_user_id=None):
        conn = self.get_connection()
        if not conn:
            return {'success': False, 'error': 'db_unavailable'}
        cursor = conn.cursor()
        try:
            role = (org_role or '').strip().lower()
            if role not in ('owner', 'admin', 'viewer'):
                return {'success': False, 'error': 'invalid_org_role'}

            cursor.execute("SELECT org_role FROM org_members WHERE org_id=%s AND user_id=%s", (org_id, user_id))
            row = cursor.fetchone()
            if not row:
                return {'success': False, 'error': 'member_not_found'}

            current_role = row[0]
            # Guardrail: org admins cannot promote themselves to owner.
            try:
                same_user = (
                    acting_user_id is not None
                    and int(acting_user_id) == int(user_id)
                )
            except Exception:
                same_user = False
            if same_user and str(current_role or '').strip().lower() == 'admin' and role == 'owner':
                return {'success': False, 'error': 'admin_cannot_self_promote_to_owner'}

            if current_role == 'owner' and role != 'owner':
                cursor.execute("SELECT COUNT(*) FROM org_members WHERE org_id=%s AND org_role='owner'", (org_id,))
                owner_count = int(cursor.fetchone()[0] or 0)
                if owner_count <= 1:
                    return {'success': False, 'error': 'cannot_demote_last_owner'}

            cursor.execute("""
                UPDATE org_members
                SET org_role=%s
                WHERE org_id=%s AND user_id=%s
            """, (role, org_id, user_id))
            conn.commit()
            return {'success': True, 'org_role': role}
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            cursor.close(); conn.close()

    def remove_user_from_org(self, org_id, user_id):
        conn = self.get_connection()
        if not conn:
            return {'success': False, 'error': 'db_unavailable'}
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT org_role FROM org_members WHERE org_id=%s AND user_id=%s", (org_id, user_id))
            row = cursor.fetchone()
            if not row:
                return {'success': False, 'error': 'member_not_found'}

            current_role = row[0]
            if current_role == 'owner':
                cursor.execute("SELECT COUNT(*) FROM org_members WHERE org_id=%s AND org_role='owner'", (org_id,))
                owner_count = int(cursor.fetchone()[0] or 0)
                if owner_count <= 1:
                    return {'success': False, 'error': 'cannot_remove_last_owner'}

            cursor.execute("DELETE FROM org_members WHERE org_id=%s AND user_id=%s", (org_id, user_id))

            cursor.execute("SELECT primary_org_id FROM dashboard_users WHERE id=%s", (user_id,))
            user_row = cursor.fetchone()
            if user_row and user_row[0] == org_id:
                cursor.execute("""
                    SELECT org_id FROM org_members
                    WHERE user_id=%s
                    ORDER BY joined_at ASC
                    LIMIT 1
                """, (user_id,))
                next_org = cursor.fetchone()
                next_org_id = next_org[0] if next_org else None
                cursor.execute("UPDATE dashboard_users SET primary_org_id=%s WHERE id=%s", (next_org_id, user_id))

            conn.commit()
            return {'success': True}
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
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
        print("âœ… Database setup complete!")
        print("=" * 60)
        print("\nDefault credentials:")
        print("  Admin:  admin@dashboard.com / admin123")
        print("  User:   user@dashboard.com / user123")
    else:
        print("\nâŒ Database setup failed!")


if __name__ == "__main__":
    setup_database()
