"""
Test script to verify database users and authentication
"""

from dashboarddb import DashboardDatabase
import sys

def test_database():
    """Test database connection and users"""
    print("="*60)
    print("Database Test Script")
    print("="*60)
    print()
    
    # Initialize database
    db = DashboardDatabase(
        host='localhost',
        port=5432,
        database='passwords',
        user='postgres',
        password='passwords'  # Match your dashboardserver.py
    )
    
    # Test connection
    print("1. Testing database connection...")
    conn = db.get_connection()
    if conn:
        print("   ✅ Connection successful!")
        conn.close()
    else:
        print("   ❌ Connection failed!")
        print("   Please check your database credentials in dashboarddb.py")
        sys.exit(1)
    
    # Get all users
    print("\n2. Fetching all users from database...")
    users = db.get_all_users()
    
    if not users:
        print("   ⚠️  No users found in database!")
        print("   Running setup to create default users...")
        db.setup_tables()
        db.create_default_users()
        users = db.get_all_users()
    
    print(f"   Found {len(users)} user(s):")
    for user in users:
        print(f"\n   User ID: {user['id']}")
        print(f"   Username: {user['username']}")
        print(f"   Email: {user['email']}")
        print(f"   Full Name: {user['name']}")
        print(f"   Role: {user['role']}")
    
    # Test authentication with email
    print("\n" + "="*60)
    print("3. Testing Authentication")
    print("="*60)
    
    test_credentials = [
        ('admin@dashboard.com', 'admin123', 'Admin'),
        ('user@dashboard.com', 'user123', 'Regular User')
    ]
    
    for email, password, description in test_credentials:
        print(f"\n   Testing {description}:")
        print(f"   Email: {email}")
        print(f"   Password: {password}")
        
        result = db.authenticate_user_by_email(email, password)
        
        if result:
            print(f"   ✅ Authentication successful!")
            print(f"   Logged in as: {result['name']} ({result['role']})")
        else:
            print(f"   ❌ Authentication failed!")
            print(f"   Please verify:")
            print(f"      - Email exists in database")
            print(f"      - Password is correct")
            print(f"      - Password hashing is working")
    
    print("\n" + "="*60)
    print("Test Complete!")
    print("="*60)
    print("\nIf authentication failed, try:")
    print("1. Delete and recreate the users:")
    print("   python dashboarddb.py")
    print("2. Check database password in dashboarddb.py")
    print("3. Verify PostgreSQL is running")

if __name__ == "__main__":
    test_database()