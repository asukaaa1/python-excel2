"""
Database Migration Script - Add last_login column
Run this script once to add the last_login column to the dashboard_users table
"""

import psycopg2
from datetime import datetime

# Database configuration - UPDATE THESE WITH YOUR SETTINGS
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'passwords',
    'user': 'postgres',
    'password': 'passwords'  # CHANGE THIS!
}

def migrate_database():
    """Add last_login column if it doesn't exist"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("🔍 Checking if last_login column exists...")
        
        # Check if column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='dashboard_users' AND column_name='last_login'
        """)
        
        if cursor.fetchone():
            print("✅ last_login column already exists")
        else:
            print("➕ Adding last_login column...")
            cursor.execute("""
                ALTER TABLE dashboard_users 
                ADD COLUMN last_login TIMESTAMP
            """)
            conn.commit()
            print("✅ last_login column added successfully")
        
        cursor.close()
        conn.close()
        print("\n✅ Migration completed successfully!")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        print("\nIf the table doesn't exist yet, this is normal.")
        print("The table will be created when you start the server.")

if __name__ == '__main__':
    print("="*60)
    print("Database Migration - Add last_login column")
    print("="*60)
    migrate_database()