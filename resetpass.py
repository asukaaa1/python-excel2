"""
Password Reset Script
This will reset the passwords for your existing users using bcrypt
"""

import psycopg2
import bcrypt
import sys

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="backslashreplace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(errors="backslashreplace")
except Exception:
    pass

def reset_passwords():
    """Reset passwords for all admin users"""
    print("="*70)
    print("PASSWORD RESET SCRIPT")
    print("="*70)
    print()
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='passwords',
            user='postgres',
            password='passwords'
        )
        cursor = conn.cursor()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return
    
    # Get all users with admin@dashboard.com email
    cursor.execute("""
        SELECT id, username, email, role 
        FROM dashboard_users 
        WHERE email = 'admin@dashboard.com'
    """)
    
    users = cursor.fetchall()
    
    if not users:
        print("⚠️  No users found with email admin@dashboard.com")
        cursor.close()
        conn.close()
        return
    
    print(f"\nFound {len(users)} user(s) with admin@dashboard.com:")
    for user in users:
        print(f"  - ID: {user[0]}, Username: {user[1]}, Role: {user[3]}")
    
    print("\nResetting password to 'admin123' for all these users...")
    
    # Generate bcrypt hash for 'admin123'
    password = 'admin123'
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    hashed_str = hashed.decode('utf-8')
    
    print(f"New hash: {hashed_str[:50]}...")
    
    # Update all users with admin@dashboard.com
    try:
        cursor.execute("""
            UPDATE dashboard_users 
            SET password_hash = %s 
            WHERE email = 'admin@dashboard.com'
        """, (hashed_str,))
        
        conn.commit()
        print(f"\n✅ Password reset successful!")
        print(f"   {cursor.rowcount} user(s) updated")
        
    except Exception as e:
        print(f"\n❌ Error updating passwords: {e}")
        conn.rollback()
    
    # Also reset user@dashboard.com
    print("\n" + "-"*70)
    print("Resetting password for user@dashboard.com...")
    
    password = 'user123'
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    hashed_str = hashed.decode('utf-8')
    
    try:
        cursor.execute("""
            UPDATE dashboard_users 
            SET password_hash = %s 
            WHERE email = 'user@dashboard.com'
        """, (hashed_str,))
        
        conn.commit()
        print(f"✅ Password reset successful!")
        print(f"   {cursor.rowcount} user(s) updated")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*70)
    print("PASSWORD RESET COMPLETE!")
    print("="*70)
    print("\nYou can now login with:")
    print("  Email: admin@dashboard.com")
    print("  Password: admin123")
    print("\n  Email: user@dashboard.com")
    print("  Password: user123")
    print()

if __name__ == "__main__":
    reset_passwords()
