"""
Configure iFood API for organization
This script sets up the iFood API credentials and fixes merchant format
"""

import sys
import json
import os
from dashboarddb import DashboardDatabase

CLIENT_ID = str(os.environ.get('IFOOD_CLIENT_ID') or '').strip()
CLIENT_SECRET = str(os.environ.get('IFOOD_CLIENT_SECRET') or '').strip()
TEST_MERCHANT_ID = str(
    os.environ.get('IFOOD_TEST_MERCHANT_ID')
    or 'd91ad16e-0abc-4149-8e86-b10a477659b8'
).strip()
TEST_MERCHANT_NAME = str(
    os.environ.get('IFOOD_TEST_MERCHANT_NAME')
    or 'Teste - PRODUTORA DUO LTDA'
).strip()


def _mask_secret(value: str, visible_suffix: int = 4) -> str:
    secret = str(value or '')
    if not secret:
        return '(not set)'
    if len(secret) <= visible_suffix:
        return '*' * len(secret)
    return f"{'*' * max(8, len(secret) - visible_suffix)}{secret[-visible_suffix:]}"

def main():
    db = DashboardDatabase()

    print("=" * 70)
    print("iFood API Configuration Tool")
    print("=" * 70)

    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ Missing iFood credentials.")
        print("Set IFOOD_CLIENT_ID and IFOOD_CLIENT_SECRET before running this script.")
        return 1

    # Get organization
    try:
        org_id = int(str(os.environ.get('IFOOD_CONFIG_ORG_ID', '1')).strip() or '1')
    except Exception:
        print("❌ Invalid IFOOD_CONFIG_ORG_ID value.")
        return 1

    print(f"\n📋 Configuring organization ID: {org_id}")

    # Step 1: Get current config
    conn = db.get_connection()
    if not conn:
        print("❌ Could not connect to database!")
        return 1

    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, slug, ifood_merchants
        FROM organizations
        WHERE id = %s
    """, (org_id,))

    row = cursor.fetchone()
    if not row:
        print(f"❌ Organization {org_id} not found!")
        cursor.close()
        conn.close()
        return 1

    org_name, org_slug, current_merchants = row
    print(f"   Organization: {org_name} ({org_slug})")

    # Step 2: Fix merchant format
    print(f"\n🔧 Fixing merchant data format...")
    print(f"   Current format: {current_merchants}")

    # Extract merchant IDs from the current format
    merchant_ids = []
    if current_merchants:
        if isinstance(current_merchants, str):
            current_merchants = json.loads(current_merchants)

        if isinstance(current_merchants, list):
            for item in current_merchants:
                if isinstance(item, dict):
                    # Object format: extract merchant_id
                    merchant_id = item.get('merchant_id')
                    if merchant_id:
                        merchant_ids.append(merchant_id)
                elif isinstance(item, str):
                    # Already in correct format
                    merchant_ids.append(item)

    # Ensure our test merchant is included
    if TEST_MERCHANT_ID not in merchant_ids:
        merchant_ids.append(TEST_MERCHANT_ID)

    print(f"   Fixed format: {merchant_ids}")

    # Step 3: Update organization with credentials and fixed merchant list
    print(f"\n🔑 Adding iFood API credentials...")
    print(f"   Client ID: {_mask_secret(CLIENT_ID)}")
    print(f"   Client Secret: {_mask_secret(CLIENT_SECRET)}")

    success = db.update_org_ifood_config(
        org_id=org_id,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        merchants=merchant_ids
    )

    cursor.close()
    conn.close()

    if not success:
        print("\n❌ FAILED to update organization configuration!")
        return 1

    print("\n✅ Configuration updated successfully!")

    # Step 4: Verify the configuration
    print(f"\n🔍 Verifying configuration...")
    config = db.get_org_ifood_config(org_id)

    if not config:
        print("❌ Could not retrieve configuration!")
        return 1

    print(f"   ✅ Client ID: {config['client_id']}")
    print(f"   ✅ Client Secret: Configured")
    print(f"   ✅ Merchants ({len(config['merchants'])} total):")
    for idx, merchant_id in enumerate(config['merchants'], 1):
        marker = "🎯" if merchant_id == TEST_MERCHANT_ID else "  "
        print(f"      {marker} {idx}. {merchant_id}")

    # Step 5: Test authentication
    print(f"\n🧪 Testing iFood API authentication...")
    from ifood_api import IFoodAPI

    api = IFoodAPI(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        use_mock_data=False
    )

    auth_success = api.authenticate()

    if auth_success:
        print("   ✅ Authentication successful!")
        print(f"   Token expires: {api.token_expires_at}")

        # Try to get merchant details
        print(f"\n📊 Fetching merchant details for {TEST_MERCHANT_ID}...")
        merchant_details = api.get_merchant_details(TEST_MERCHANT_ID)

        if merchant_details:
            print(f"   ✅ Merchant found!")
            print(f"      Name: {merchant_details.get('name', 'N/A')}")
            print(f"      Status: {merchant_details.get('status', 'N/A')}")
        else:
            print(f"   ⚠️  Could not fetch merchant details (this may be normal for test merchants)")
    else:
        print("   ❌ Authentication failed!")
        if api.last_auth_error:
            print(f"      Error: {api.last_auth_error}")
        return 1

    # Success!
    print("\n" + "=" * 70)
    print("✅ Configuration Complete!")
    print("=" * 70)
    print("\n📌 What happens next:")
    print("   1. The system will poll iFood every 30 seconds for new orders")
    print("   2. New orders from your test merchant will be captured automatically")
    print("   3. Orders will appear in your dashboard")
    print("\n💡 To verify it's working:")
    print("   1. Generate test orders via iFood test API")
    print("   2. Wait up to 30 seconds for the next polling cycle")
    print("   3. Check dashboard_output/ifood_homologation_evidence.jsonl")
    print("   4. Look for events_received > 0 and orders_persisted > 0")
    print("\n🔄 If your dashboard server is running, restart it to pick up the new config:")
    print("   - Stop the server (Ctrl+C)")
    print("   - Start it again")

    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code or 0)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
