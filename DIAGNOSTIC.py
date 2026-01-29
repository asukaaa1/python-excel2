#!/usr/bin/env python3
"""
Quick diagnostic to check restaurant ID mismatch
"""

import json
from pathlib import Path

print("=" * 70)
print("RESTAURANT ID DIAGNOSTIC")
print("=" * 70)

# Check if config exists
config_file = Path('ifood_config.json')
if not config_file.exists():
    print("\n❌ ifood_config.json NOT FOUND!")
    print("\nThis is your problem. The server has no configuration.")
    print("\nSOLUTION:")
    print("1. Copy ifood_config_fixed.json to ifood_config.json")
    print("2. Restart your server")
    print("3. Try again")
else:
    print("\n✅ ifood_config.json exists")
    
    try:
        with open('ifood_config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        print("\n📋 Current Configuration:")
        print(f"   Mode: {config.get('client_id')}")
        
        merchants = config.get('merchants', [])
        if not merchants:
            print("\n❌ NO MERCHANTS CONFIGURED!")
            print("\nThis is your problem. No restaurants in config.")
            print("\nSOLUTION:")
            print("1. Replace your ifood_config.json with ifood_config_fixed.json")
            print("2. Restart your server")
        else:
            print(f"\n✅ Found {len(merchants)} merchant(s):")
            for m in merchants:
                mid = m.get('merchant_id', 'NO ID')
                name = m.get('name', 'NO NAME')
                print(f"\n   • {name}")
                print(f"     ID: {mid}")
                print(f"     URL: http://localhost:5000/restaurant/{mid}")
            
            # Check if mock-restaurant-8 is in there
            ids = [m.get('merchant_id') for m in merchants]
            if 'mock-restaurant-8' in ids:
                print("\n✅ 'mock-restaurant-8' IS configured - URL should work")
            else:
                print("\n❌ 'mock-restaurant-8' is NOT in your config!")
                print("\n   You're trying to access: mock-restaurant-8")
                print(f"   But you have: {', '.join(ids)}")
                print("\nSOLUTIONS:")
                print("\n   Option 1 - Use the correct URL:")
                for m in merchants:
                    print(f"   → http://localhost:5000/restaurant/{m.get('merchant_id')}")
                print("\n   Option 2 - Fix your config:")
                print("   → Replace ifood_config.json with ifood_config_fixed.json")
                print("   → Restart server")
    except Exception as e:
        print(f"\n❌ Error reading config: {e}")

print("\n" + "=" * 70)
print("WHAT TO DO NEXT:")
print("=" * 70)
print("""
If 'mock-restaurant-8' is NOT configured:

1. Stop your server (Ctrl+C)

2. Replace config file:
   On Windows:
     copy ifood_config_fixed.json ifood_config.json
   On Mac/Linux:
     cp ifood_config_fixed.json ifood_config.json

3. Restart server:
   python dashboardserver.py

4. Check server says: "Successfully loaded 1/1 restaurant(s)"

5. Go to: http://localhost:5000/restaurant/mock-restaurant-8

OR just use the correct URL shown above!
""")