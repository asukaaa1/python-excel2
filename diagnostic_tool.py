#!/usr/bin/env python3
"""
Dashboard Data Diagnostic Tool
Checks why your dashboard is showing zeros
"""

import json
import sys
from pathlib import Path

def check_config():
    """Check the configuration file"""
    print("=" * 70)
    print("🔍 CHECKING CONFIGURATION")
    print("=" * 70)
    
    config_file = Path("ifood_config.json")
    
    if not config_file.exists():
        print("❌ ifood_config.json NOT FOUND!")
        print("   Create it with: python setup_mock_dashboard.py")
        return None
    
    print("✅ Config file exists")
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        print(f"\n📋 Configuration:")
        print(f"   Mock Mode: {config.get('use_mock_data', False)}")
        print(f"   Client ID: {config.get('client_id', 'NOT SET')}")
        print(f"   Merchants: {len(config.get('merchants', []))}")
        
        if config.get('use_mock_data'):
            print("\n✅ MOCK MODE ENABLED - Should show test data")
        else:
            print("\n⚠️  REAL API MODE - Requires valid iFood credentials")
        
        return config
        
    except Exception as e:
        print(f"❌ Error reading config: {e}")
        return None


def check_api_file():
    """Check if the API file supports mock data"""
    print("\n" + "=" * 70)
    print("🔍 CHECKING API FILE")
    print("=" * 70)
    
    api_file = Path("ifood_api.py")
    
    if not api_file.exists():
        print("❌ ifood_api.py NOT FOUND!")
        return False
    
    print("✅ API file exists")
    
    try:
        with open(api_file, 'r') as f:
            content = f.read()
        
        has_mock_support = 'use_mock_data' in content or 'MOCK_DATA_MODE' in content
        has_mock_import = 'mock_ifood_data' in content or 'MockIFoodDataGenerator' in content
        
        print(f"\n📋 API File Analysis:")
        print(f"   Has mock support: {'✅ YES' if has_mock_support else '❌ NO'}")
        print(f"   Imports mock module: {'✅ YES' if has_mock_import else '❌ NO'}")
        
        if not has_mock_support:
            print("\n⚠️  Your ifood_api.py doesn't support mock data!")
            print("   Solution: Replace it with ifood_api_with_mock.py")
        
        return has_mock_support
        
    except Exception as e:
        print(f"❌ Error reading API file: {e}")
        return False


def check_mock_data_file():
    """Check if mock data generator exists"""
    print("\n" + "=" * 70)
    print("🔍 CHECKING MOCK DATA GENERATOR")
    print("=" * 70)
    
    mock_file = Path("mock_ifood_data.py")
    
    if not mock_file.exists():
        print("❌ mock_ifood_data.py NOT FOUND!")
        print("   This file is required for mock mode")
        return False
    
    print("✅ Mock data generator exists")
    
    try:
        # Try to import it
        sys.path.insert(0, str(Path.cwd()))
        from mock_ifood_data import MockIFoodDataGenerator
        
        print("\n📊 Testing data generation...")
        sample = MockIFoodDataGenerator.generate_merchant_data(
            restaurant_info={"name": "Test Restaurant", "manager": "Test Manager"},
            num_orders=10,
            days=7
        )
        
        print(f"   Generated orders: {len(sample.get('orders', []))}")
        print(f"   Concluded orders: {len(sample.get('concluded_orders', []))}")
        print(f"   Total revenue: R$ {sample.get('total_revenue', 0):,.2f}")
        
        if sample.get('total_revenue', 0) > 0:
            print("\n✅ Mock data generation WORKING!")
            return True
        else:
            print("\n⚠️  Mock data generated but has no revenue")
            return False
        
    except ImportError as e:
        print(f"❌ Cannot import mock data generator: {e}")
        return False
    except Exception as e:
        print(f"❌ Error testing mock data: {e}")
        return False


def test_api_connection():
    """Test if the API can connect and fetch data"""
    print("\n" + "=" * 70)
    print("🔍 TESTING API CONNECTION")
    print("=" * 70)
    
    try:
        sys.path.insert(0, str(Path.cwd()))
        from ifood_api import IFoodAPI, IFoodConfig
        
        # Load config
        config = IFoodConfig.load_config("ifood_config.json")
        if not config:
            print("❌ Cannot load config")
            return False
        
        # Initialize API
        use_mock = config.get('use_mock_data', False)
        client_id = config.get('client_id', '')
        client_secret = config.get('client_secret', '')
        
        print(f"\n📡 Initializing API...")
        print(f"   Mode: {'MOCK' if use_mock else 'REAL'}")
        
        api = IFoodAPI(client_id, client_secret, use_mock_data=use_mock)
        
        # Test authentication
        print(f"\n🔐 Testing authentication...")
        if api.authenticate():
            print("✅ Authentication successful!")
        else:
            print("❌ Authentication failed!")
            return False
        
        # Test fetching merchants
        print(f"\n🏪 Testing merchant fetch...")
        merchants = config.get('merchants', [])
        
        if not merchants:
            print("⚠️  No merchants configured!")
            return False
        
        print(f"   Found {len(merchants)} merchants in config")
        
        # Test first merchant
        test_merchant = merchants[0]
        merchant_id = test_merchant.get('merchant_id')
        
        print(f"\n📊 Testing data fetch for: {test_merchant.get('name')}")
        print(f"   Merchant ID: {merchant_id}")
        
        # Get orders
        orders = api.get_orders(merchant_id)
        concluded_orders = [o for o in orders if o.get('orderStatus') == 'CONCLUDED']
        
        print(f"\n📈 Results:")
        print(f"   Total orders: {len(orders)}")
        print(f"   Concluded orders: {len(concluded_orders)}")
        
        if concluded_orders:
            total_revenue = sum(o.get('totalPrice', 0) for o in concluded_orders)
            print(f"   Total revenue: R$ {total_revenue:,.2f}")
            
            if total_revenue > 0:
                print("\n✅ DATA IS AVAILABLE!")
                print("   If dashboard shows zeros, the problem is in the frontend")
                return True
            else:
                print("\n⚠️  Orders exist but have zero revenue")
                print("   Check order data structure")
        else:
            print("\n❌ NO CONCLUDED ORDERS FOUND!")
            print("   This is why your dashboard shows zeros")
        
        return False
        
    except ImportError as e:
        print(f"❌ Cannot import API module: {e}")
        print("   Make sure ifood_api.py is in the current directory")
        return False
    except Exception as e:
        print(f"❌ Error testing API: {e}")
        import traceback
        traceback.print_exc()
        return False


def print_solution(config_ok, api_ok, mock_ok, connection_ok):
    """Print the solution based on what's wrong"""
    print("\n" + "=" * 70)
    print("💡 DIAGNOSIS & SOLUTION")
    print("=" * 70)
    
    if config_ok and api_ok and mock_ok and connection_ok:
        print("\n✅ Everything looks good!")
        print("\nIf dashboard still shows zeros, check:")
        print("1. Browser console for JavaScript errors")
        print("2. Server is running: python dashboardserver.py")
        print("3. Correct URL: http://localhost:5000")
        print("4. Clear browser cache and reload")
        return
    
    print("\n🔧 PROBLEMS FOUND:")
    print()
    
    if not config_ok:
        print("❌ Configuration Issue")
        print("   Fix: Run python setup_mock_dashboard.py")
        print()
    
    if not api_ok:
        print("❌ API File Missing Mock Support")
        print("   Fix: Replace ifood_api.py with ifood_api_with_mock.py")
        print("   Command: mv ifood_api_with_mock.py ifood_api.py")
        print()
    
    if not mock_ok:
        print("❌ Mock Data Generator Missing")
        print("   Fix: Make sure mock_ifood_data.py is in the same directory")
        print()
    
    if not connection_ok:
        print("❌ API Not Returning Data")
        print("   If using MOCK mode:")
        print("     - Make sure use_mock_data is true in config")
        print("     - Make sure all files are in place")
        print("   If using REAL API:")
        print("     - Check your iFood credentials")
        print("     - Make sure merchant IDs are correct")
        print("     - Check you have orders in the date range")
        print()
    
    print("=" * 70)
    print("\n🚀 QUICK FIX (use mock data):")
    print()
    print("1. python setup_mock_dashboard.py")
    print("2. mv ifood_api_with_mock.py ifood_api.py")
    print("3. python dashboardserver.py")
    print()


def main():
    """Run all diagnostics"""
    print("\n" + "=" * 70)
    print("🏥 DASHBOARD DATA DIAGNOSTIC TOOL")
    print("=" * 70)
    print("\nThis tool will help you figure out why your dashboard shows zeros")
    print()
    
    config_ok = check_config() is not None
    api_ok = check_api_file()
    mock_ok = check_mock_data_file()
    connection_ok = test_api_connection()
    
    print_solution(config_ok, api_ok, mock_ok, connection_ok)
    
    print("\n" + "=" * 70)
    print("Diagnostic complete!")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
