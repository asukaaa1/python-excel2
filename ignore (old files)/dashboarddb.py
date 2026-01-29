"""
PostgreSQL Database Module for Restaurant Dashboard
Handles user authentication and password storage
"""

import psycopg2
from psycopg2 import sql
import hashlib
import json
from typing import Optional, Dict, List

class DashboardDatabase:
    """Handle PostgreSQL database operations for dashboard authentication"""
    
    def __init__(self, host='localhost', port=5432, database='passwords', 
                 user='postgres', password='password'):
        """Initialize database connection"""
        self.config = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password,
            'client_encoding': 'utf8'
        }
    
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
            
            conn.commit()
            print("Database tables created successfully!")
            return True
            
        except Exception as e:
            print(f"Error creating tables: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    def hash_password(self, password: str) -> str:
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()
    
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
            print(f"User '{username}' created with ID: {user_id}")
            return user_id
            
        except psycopg2.IntegrityError:
            conn.rollback()
            print(f"User '{username}' already exists")
            return None
        except Exception as e:
            conn.rollback()
            print(f"Error creating user: {e}")
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
        password_hash = self.hash_password(password)
        
        try:
            cursor.execute("""
                SELECT id, username, full_name, email, role, last_login
                FROM dashboard_users
                WHERE username = %s AND password_hash = %s
            """, (username, password_hash))
            
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
            print(f"Error fetching restaurants: {e}")
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
            print(f"Error assigning restaurant: {e}")
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





    def create_default_users(self):
        """Create default admin and user accounts"""
        print("\nCreating default users...")
        
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
            print(f"Error fetching users: {e}")
            return []
        finally:
            cursor.close()
            conn.close()


def setup_database():
    """Quick setup function"""
    print("=" * 60)
    print("Dashboard Database Setup")
    print("=" * 60)
    print()
    
    # Initialize database
    db = DashboardDatabase(
        host='localhost',
        port=5432,
        database='passwords',
        user='postgres',
        password='your_password_here'  # CHANGE THIS!
    )
    
    # Create tables
    if db.setup_tables():
        # Create default users
        db.create_default_users()
        
        print("\n" + "=" * 60)
        print("Database setup complete!")
        print("=" * 60)
        print("\nDefault credentials:")
        print("  Admin:  admin / admin123")
        print("  User:   usuario / user123")
    else:
        print("\nDatabase setup failed!")


if __name__ == "__main__":
    setup_database()