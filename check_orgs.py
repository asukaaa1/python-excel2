"""
Check existing organizations and their iFood configuration
"""

from dashboarddb import DashboardDatabase

db = DashboardDatabase()

print("=" * 70)
print("Organization Status Check")
print("=" * 70)

# Get connection and fetch ALL organizations
conn = db.get_connection()
if not conn:
    print("❌ Could not connect to database!")
    exit(1)

cursor = conn.cursor()

# Fetch all organizations
cursor.execute("""
    SELECT id, name, slug, plan, is_active,
           ifood_client_id, ifood_client_secret, ifood_merchants,
           created_at
    FROM organizations
    ORDER BY created_at DESC
""")

orgs = cursor.fetchall()
cursor.close()
conn.close()

if not orgs:
    print("\n❌ No organizations found in database!")
    print("\nYou need to create an organization first.")
    exit(1)

print(f"\n✅ Found {len(orgs)} organization(s):\n")

for org in orgs:
    org_id, name, slug, plan, is_active, client_id, client_secret, merchants, created_at = org

    status = "✅ Active" if is_active else "❌ Inactive"
    has_creds = "✅ Yes" if client_id and client_secret else "❌ No"

    merchant_list = merchants if merchants else []
    if isinstance(merchant_list, str):
        import json
        merchant_list = json.loads(merchant_list)

    print(f"Organization: {name}")
    print(f"  - ID: {org_id}")
    print(f"  - Slug: {slug}")
    print(f"  - Plan: {plan}")
    print(f"  - Status: {status}")
    print(f"  - iFood Credentials: {has_creds}")
    print(f"  - Merchant Count: {len(merchant_list)}")
    if merchant_list:
        print(f"  - Merchants: {merchant_list}")
    print(f"  - Created: {created_at}")
    print()

# Check if we need to configure iFood credentials
active_with_creds = [o for o in orgs if o[4] and o[5] and o[6]]  # is_active, client_id, client_secret

if not active_with_creds:
    print("=" * 70)
    print("⚠️  ACTION REQUIRED: iFood API Credentials Missing")
    print("=" * 70)
    print("\nNone of your organizations have iFood API credentials configured.")
    print("\nTo fix this, you need to:")
    print("  1. Obtain iFood API credentials (client_id and client_secret)")
    print("  2. Update your organization with these credentials")
    print("\nI can help you add the credentials. Do you have them?")
