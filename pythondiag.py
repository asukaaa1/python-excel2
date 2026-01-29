"""
Complete System Diagnostic - Run this and show me ALL output
"""

import psycopg2
import hashlib
import json
import sys

print("="*70)
print("COMPLETE LOGIN SYSTEM DIAGNOSTIC")
print("="*70)
print()

# Step 1: Test PostgreSQL connection
print("STEP 1: Testing PostgreSQL Connection")
print("-"*70)

passwords = ['passwords', 'password', 'postgres', 'admin', '']
working_password = None

for pwd in passwords:
    try:
        print(f"Trying password: '{pwd}'...")
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='passwords',
            user='postgres',
            password=pwd,
            connect_timeout=3
        )
        print(f"✅ SUCCESS! Working password is: '{pwd}'")
        working_password = pwd
        conn.close()
        break
    except psycopg2.OperationalError as e:
        if "password authentication failed" in str(e):
            print(f"   ❌ Wrong password")
        elif "database" in str(e) and "does not exist" in str(e):
            print(f"   ❌ Database 'passwords' doesn't exist")
        else:
            print(f"   ❌ Error: {e}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

if not working_password:
    print("\n❌ FAILED: Could not connect to PostgreSQL")
    print("\nPossible issues:")
    print("1. PostgreSQL is not running")
    print("2. Database 'passwords' doesn't exist")
    print("3. Password is different than the ones tested")
    print("\nPlease tell me:")
    print("- What is your PostgreSQL password?")
    print("- Does the 'passwords' database exist?")
    sys.exit(1)

print()

# Step 2: Check if tables exist
print("STEP 2: Checking Database Tables")
print("-"*70)

try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='passwords',
        user='postgres',
        password=working_password
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()
    
    if tables:
        print("Found tables:")
        for table in tables:
            print(f"  - {table[0]}")
    else:
        print("⚠️  No tables found in database")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ Error checking tables: {e}")

print()

# Step 3: Check if users exist
print("STEP 3: Checking Users in Database")
print("-"*70)

try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='passwords',
        user='postgres',
        password=working_password
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, username, email, role, password_hash 
        FROM dashboard_users
    """)
    users = cursor.fetchall()
    
    if users:
        print(f"Found {len(users)} user(s):")
        for user in users:
            print(f"\n  User ID: {user[0]}")
            print(f"  Username: {user[1]}")
            print(f"  Email: {user[2]}")
            print(f"  Role: {user[3]}")
            print(f"  Password Hash: {user[4][:50]}...")
    else:
        print("⚠️  No users found in database!")
        print("   Users need to be created first")
    
    cursor.close()
    conn.close()
    
except psycopg2.errors.UndefinedTable:
    print("❌ Table 'dashboard_users' doesn't exist!")
    print("   You need to run: python dashboarddb.py")
except Exception as e:
    print(f"❌ Error: {e}")

print()

# Step 4: Test password hashing
print("STEP 4: Testing Password Hashing")
print("-"*70)

test_password = "admin123"
expected_hash = hashlib.sha256(test_password.encode()).hexdigest()

print(f"Test password: '{test_password}'")
print(f"Expected hash: {expected_hash}")

# Check if admin user's hash matches
try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='passwords',
        user='postgres',
        password=working_password
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT password_hash FROM dashboard_users 
        WHERE email = 'admin@dashboard.com'
    """)
    result = cursor.fetchone()
    
    if result:
        stored_hash = result[0]
        print(f"Stored hash:   {stored_hash}")
        
        if stored_hash == expected_hash:
            print("✅ Password hash MATCHES! Password should work.")
        else:
            print("❌ Password hash DOES NOT MATCH!")
            print("   The password in database is NOT 'admin123'")
    else:
        print("⚠️  Admin user not found in database")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")

print()

# Step 5: Test authentication function
print("STEP 5: Testing Authentication")
print("-"*70)

try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='passwords',
        user='postgres',
        password=working_password
    )
    cursor = conn.cursor()
    
    test_email = "admin@dashboard.com"
    test_pass = "admin123"
    pass_hash = hashlib.sha256(test_pass.encode()).hexdigest()
    
    print(f"Attempting to authenticate:")
    print(f"  Email: {test_email}")
    print(f"  Password: {test_pass}")
    print(f"  Hash: {pass_hash}")
    
    cursor.execute("""
        SELECT id, username, full_name, email, role 
        FROM dashboard_users
        WHERE email = %s AND password_hash = %s
    """, (test_email, pass_hash))
    
    result = cursor.fetchone()
    
    if result:
        print(f"\n✅ AUTHENTICATION SUCCESSFUL!")
        print(f"   User ID: {result[0]}")
        print(f"   Username: {result[1]}")
        print(f"   Full Name: {result[2]}")
        print(f"   Email: {result[3]}")
        print(f"   Role: {result[4]}")
    else:
        print(f"\n❌ AUTHENTICATION FAILED!")
        print(f"   No user found with email '{test_email}' and password 'admin123'")
        
        # Check if email exists
        cursor.execute("SELECT email, password_hash FROM dashboard_users WHERE email = %s", (test_email,))
        email_check = cursor.fetchone()
        if email_check:
            print(f"\n   Email EXISTS in database")
            print(f"   Stored hash: {email_check[1]}")
            print(f"   Expected hash: {pass_hash}")
            print(f"   → Password is wrong OR hash algorithm mismatch")
        else:
            print(f"\n   Email DOES NOT EXIST in database")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error during authentication test: {e}")

print()
print("="*70)
print("DIAGNOSTIC COMPLETE")
print("="*70)
print()
print("SUMMARY:")
print(f"  Database Password: {working_password}")
print()
print("Next steps:")
print("1. Share this ENTIRE output with me")
print("2. I'll tell you exactly what's wrong")
print("3. We'll fix it together")
print()
print("To create/recreate users, run:")
print(f"  python dashboarddb.py")