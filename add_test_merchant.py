"""
Add iFood Test Merchant to Organization
This script adds your test restaurant to the organization's merchant list
"""

import sys
from dashboarddb import DashboardDatabase

# Your test merchant details
TEST_MERCHANT_ID = "d91ad16e-0abc-4149-8e86-b10a477659b8"
TEST_MERCHANT_NAME = "Teste - PRODUTORA DUO LTDA"

def main():
    db = DashboardDatabase()

    print("=" * 70)
    print("iFood Test Merchant Configuration Tool")
    print("=" * 70)

    # Step 1: Get all active organizations
    print("\n📋 Fetching active organizations...")
    orgs = db.get_all_active_orgs()

    if not orgs:
        print("❌ No active organizations found with iFood credentials!")
        print("\nPlease ensure you have:")
        print("  1. An active organization in the database")
        print("  2. iFood API credentials configured (client_id and client_secret)")
        return

    print(f"\n✅ Found {len(orgs)} organization(s) with iFood credentials:\n")

    for i, org in enumerate(orgs, 1):
        merchant_count = len(org.get('ifood_merchants', []))
        print(f"{i}. {org['name']} (slug: {org['slug']})")
        print(f"   - Plan: {org['plan']}")
        print(f"   - Current merchants: {merchant_count}")

        # Show current merchants
        if org.get('ifood_merchants'):
            print(f"   - Merchant IDs: {org['ifood_merchants']}")
        print()

    # Step 2: Select organization (or auto-select if only one)
    if len(orgs) == 1:
        selected_org = orgs[0]
        print(f"🎯 Auto-selecting: {selected_org['name']}")
    else:
        while True:
            try:
                choice = input(f"\nSelect organization (1-{len(orgs)}): ").strip()
                idx = int(choice) - 1
                if 0 <= idx < len(orgs):
                    selected_org = orgs[idx]
                    break
                else:
                    print(f"Please enter a number between 1 and {len(orgs)}")
            except (ValueError, KeyboardInterrupt):
                print("\n\nCancelled.")
                return

    org_id = selected_org['id']
    org_name = selected_org['name']
    current_merchants = selected_org.get('ifood_merchants', [])

    # Ensure current_merchants is a list
    if isinstance(current_merchants, str):
        import json
        current_merchants = json.loads(current_merchants)

    print(f"\n📝 Organization: {org_name}")
    print(f"   ID: {org_id}")
    print(f"   Current merchants: {current_merchants}")

    # Step 3: Check if merchant already exists
    if TEST_MERCHANT_ID in current_merchants:
        print(f"\n⚠️  Merchant {TEST_MERCHANT_ID} is already configured!")
        print(f"   Merchant name: {TEST_MERCHANT_NAME}")
        print("\n✅ No changes needed. Your merchant is already in the system.")
        return

    # Step 4: Add the merchant
    print(f"\n➕ Adding test merchant...")
    print(f"   Merchant ID: {TEST_MERCHANT_ID}")
    print(f"   Merchant Name: {TEST_MERCHANT_NAME}")

    new_merchants = current_merchants + [TEST_MERCHANT_ID]

    success = db.update_org_ifood_config(
        org_id=org_id,
        merchants=new_merchants
    )

    if success:
        print("\n✅ SUCCESS! Test merchant added to organization.")
        print(f"\n📊 Updated merchant list ({len(new_merchants)} total):")
        for idx, merchant_id in enumerate(new_merchants, 1):
            marker = "🆕" if merchant_id == TEST_MERCHANT_ID else "  "
            print(f"   {marker} {idx}. {merchant_id}")

        print("\n" + "=" * 70)
        print("✅ Configuration Complete!")
        print("=" * 70)
        print("\n📌 Next Steps:")
        print("   1. The system will now poll events for your test merchant")
        print("   2. Orders created via the iFood test API will be captured")
        print("   3. Check your dashboard - new orders should appear shortly")
        print(f"   4. The keepalive polling runs every 30 seconds")
        print("\n💡 Tip: Monitor dashboard_output/ifood_homologation_evidence.jsonl")
        print("   to see polling activity and order ingestion.")

    else:
        print("\n❌ FAILED to update organization configuration.")
        print("   Please check the database connection and try again.")
        return 1

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
