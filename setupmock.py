#!/usr/bin/env python3
"""
Dashboard Setup Script - Configures Mock Data for Testing
Run this to set up your dashboard with realistic sample data
"""

import json
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from mock_ifood_data import MockIFoodDataGenerator
    print("✅ Mock data generator loaded")
except ImportError:
    print("❌ Could not load mock_ifood_data.py")
    print("   Make sure mock_ifood_data.py is in the same directory")
    sys.exit(1)


def setup_mock_dashboard(num_restaurants=5, orders_per_restaurant=200, output_dir="."):
    """Set up dashboard with mock data
    
    Args:
        num_restaurants: Number of test restaurants to create
        orders_per_restaurant: Number of orders per restaurant
        output_dir: Directory to save config file
    """
    print("=" * 70)
    print("🎭 Dashboard Mock Data Setup")
    print("=" * 70)
    print()
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    config_file = output_path / "ifood_config.json"
    
    print(f"📝 Creating configuration file: {config_file}")
    print(f"   Restaurants: {num_restaurants}")
    print(f"   Orders per restaurant: {orders_per_restaurant}")
    print()
    
    # Generate mock restaurants
    print("🏪 Generating restaurant data...")
    restaurants = MockIFoodDataGenerator.RESTAURANTS[:num_restaurants]
    
    merchants_config = []
    for i, restaurant in enumerate(restaurants, 1):
        merchant_id = MockIFoodDataGenerator.generate_merchant_id(restaurant['name'])
        merchants_config.append({
            "merchant_id": merchant_id,
            "name": restaurant['name'],
            "manager": restaurant['manager']
        })
        print(f"   {i}. {restaurant['name']} (Manager: {restaurant['manager']})")
    
    print()
    
    # Create configuration
    config = {
        "use_mock_data": True,
        "client_id": "MOCK_DATA_MODE",
        "client_secret": "MOCK_DATA_MODE",
        "data_fetch_days": 30,
        "orders_per_restaurant": orders_per_restaurant,
        "merchants": merchants_config,
        "refresh_interval_minutes": 60,
        "instructions": {
            "mock_data_mode": "Currently using MOCK DATA for testing",
            "to_use_real_api": [
                "1. Get your credentials from iFood Portal: https://portal.ifood.com.br",
                "2. Go to Configurações > Integrações",
                "3. Create new API integration",
                "4. Update this config with your real client_id and client_secret",
                "5. Set use_mock_data to false",
                "6. Add your real merchant IDs",
                "7. Restart the dashboard server"
            ],
            "api_documentation": "https://developer.ifood.com.br/docs"
        }
    }
    
    # Save configuration
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Configuration saved to: {config_file}")
    print()
    
    # Generate sample statistics
    print("📊 Sample Data Statistics:")
    print("=" * 70)
    
    total_orders = 0
    total_revenue = 0
    
    for merchant_config in merchants_config:
        # Generate sample data to show statistics
        sample_data = MockIFoodDataGenerator.generate_merchant_data(
            restaurant_info={
                'name': merchant_config['name'],
                'manager': merchant_config['manager']
            },
            num_orders=orders_per_restaurant,
            days=30
        )
        
        concluded_orders = sample_data['concluded_orders']
        restaurant_revenue = sum(o['totalPrice'] for o in concluded_orders)
        
        total_orders += len(concluded_orders)
        total_revenue += restaurant_revenue
        
        print(f"\n🏪 {merchant_config['name']}")
        print(f"   Orders (concluded): {len(concluded_orders)}")
        print(f"   Revenue: R$ {restaurant_revenue:,.2f}")
        print(f"   Average Ticket: R$ {restaurant_revenue/len(concluded_orders):,.2f}" if concluded_orders else "   No orders")
    
    print()
    print("=" * 70)
    print(f"📈 TOTAL ACROSS ALL RESTAURANTS:")
    print(f"   Total Orders: {total_orders}")
    print(f"   Total Revenue: R$ {total_revenue:,.2f}")
    print(f"   Average Ticket: R$ {total_revenue/total_orders:,.2f}" if total_orders else "   No orders")
    print("=" * 70)
    print()
    
    # Print next steps
    print("✅ Setup Complete!")
    print()
    print("🚀 NEXT STEPS:")
    print("=" * 70)
    print()
    print("1. Copy these files to your dashboard directory:")
    print(f"   - {config_file.name}")
    print("   - mock_ifood_data.py")
    print("   - ifood_api_with_mock.py (rename to ifood_api.py)")
    print()
    print("2. Replace your current ifood_api.py with ifood_api_with_mock.py:")
    print("   mv ifood_api_with_mock.py ifood_api.py")
    print()
    print("3. Start your dashboard server:")
    print("   python dashboardserver.py")
    print()
    print("4. Login with:")
    print("   Admin:  admin@dashboard.com / admin123")
    print("   User:   user@dashboard.com / user123")
    print()
    print("5. You should now see 5 restaurants with realistic data!")
    print()
    print("=" * 70)
    print()
    print("💡 TIP: To switch to real iFood API later:")
    print("   1. Edit ifood_config.json")
    print("   2. Set 'use_mock_data' to false")
    print("   3. Add your real client_id and client_secret")
    print("   4. Update merchant IDs with your real restaurant IDs")
    print("   5. Restart the server")
    print()
    
    return config


def create_test_data_file(output_dir=".", filename="test_data_sample.json"):
    """Create a sample JSON file with mock data for inspection"""
    print("📄 Creating sample data file for inspection...")
    
    sample_restaurant = MockIFoodDataGenerator.generate_merchant_data(
        restaurant_info={"name": "Pizzaria Bella Napoli", "manager": "Marco Rossi"},
        num_orders=50,
        days=7
    )
    
    output_file = Path(output_dir) / filename
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(sample_restaurant, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Sample data saved to: {output_file}")
    print(f"   You can open this file to see what the data looks like")
    print()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Set up dashboard with mock data for testing')
    parser.add_argument('--restaurants', type=int, default=5, help='Number of restaurants (default: 5)')
    parser.add_argument('--orders', type=int, default=200, help='Orders per restaurant (default: 200)')
    parser.add_argument('--output', type=str, default='.', help='Output directory (default: current)')
    parser.add_argument('--sample', action='store_true', help='Also create a sample data file')
    
    args = parser.parse_args()
    
    # Run setup
    config = setup_mock_dashboard(
        num_restaurants=args.restaurants,
        orders_per_restaurant=args.orders,
        output_dir=args.output
    )
    
    # Create sample file if requested
    if args.sample:
        create_test_data_file(output_dir=args.output)
    
    print("🎉 All done! Your dashboard is ready to run with mock data!")
