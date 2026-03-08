"""
Password Migration Script
Migrates existing SHA-256 hashed passwords to bcrypt

Run this ONCE after updating to the secure version.
It will rehash all existing passwords.

Usage:
    python migrate_passwords.py
"""

import psycopg2
import bcrypt
import os
import sys
import secrets
from getpass import getpass

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="backslashreplace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(errors="backslashreplace")
except Exception:
    pass


def migrate_passwords():
    print("=" * 60)
    print("🔒 Password Migration Script")
    print("=" * 60)
    print()
    print("⚠️  WARNING: This script will reset ALL user passwords!")
    print("   Each account will receive a one-time random password.")
    print()
    
    confirm = input("Do you want to continue? (yes/no): ").strip().lower()
    if confirm != 'yes':
        print("Migration cancelled.")
        return
    
    # Database configuration
    config = {
        'host': os.environ.get('DATABASE_HOST', 'localhost'),
        'port': int(os.environ.get('DATABASE_PORT', 5432)),
        'database': os.environ.get('DATABASE_NAME', 'passwords'),
        'user': os.environ.get('DATABASE_USER', 'postgres'),
        'password': os.environ.get('DATABASE_PASSWORD') or getpass("Database password: ")
    }
    
    try:
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        # Get all users
        cursor.execute("SELECT id, username, email FROM dashboard_users")
        users = cursor.fetchall()
        
        print(f"\nFound {len(users)} user(s) to migrate.")
        print()
        
        for user_id, username, email in users:
            new_password = secrets.token_urlsafe(12)
            
            # Hash with bcrypt
            salt = bcrypt.gensalt(rounds=12)
            password_hash = bcrypt.hashpw(new_password.encode('utf-8'), salt).decode('utf-8')
            
            # Update password
            cursor.execute(
                "UPDATE dashboard_users SET password_hash = %s WHERE id = %s",
                (password_hash, user_id)
            )
            
            print(f"✅ Migrated: {username} ({email}) - New password: {new_password}")
        
        conn.commit()
        
        print()
        print("=" * 60)
        print("✅ Migration complete!")
        print("=" * 60)
        print()
        print("Passwords above are one-time credentials.")
        print()
        print("⚠️  Please have users change their passwords after logging in.")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    migrate_passwords()
