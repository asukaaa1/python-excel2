"""
iFood API Module - FIXED VERSION
Fixed mock data structure to properly return merchant details and orders separately
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from pathlib import Path
import time

# Import mock data generator
try:
    from mock_ifood_data import MockIFoodDataGenerator
    MOCK_AVAILABLE = True
except ImportError:
    MOCK_AVAILABLE = False
    print("⚠️  Mock data generator not available")


class IFoodAPI:
    """Client for iFood Merchant API with mock data support and interruptions tracking"""
    
    # API Endpoints
    BASE_URL = "https://merchant-api.ifood.com.br"
    AUTH_URL = "https://merchant-api.ifood.com.br/authentication/v1.0/oauth/token"
    
    def __init__(self, client_id: str, client_secret: str, use_mock_data: bool = False):
        """Initialize iFood API client
        
        Args:
            client_id: iFood API Client ID (or "MOCK_DATA_MODE" for testing)
            client_secret: iFood API Client Secret
            use_mock_data: If True, use mock data instead of real API
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.use_mock_data = use_mock_data or client_id == "MOCK_DATA_MODE"
        self.access_token = None
        self.token_expires_at = None
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Mock data cache - FIXED: Store complete merchant data
        self._mock_merchants = {}
        
        if self.use_mock_data:
            print("🎭 Running in MOCK DATA mode - using sample data for testing")
    
    def authenticate(self) -> bool:
        """Authenticate with iFood API (or fake it for mock mode)"""
        if self.use_mock_data:
            print("✅ Mock authentication successful")
            self.access_token = "MOCK_TOKEN"
            self.token_expires_at = datetime.now() + timedelta(hours=24)
            return True
        
        try:
            # Real authentication code
            if self.access_token and self.token_expires_at:
                if datetime.now() < self.token_expires_at:
                    return True
            
            payload = {
                'grantType': 'client_credentials',
                'clientId': self.client_id,
                'clientSecret': self.client_secret
            }
            
            response = requests.post(
                self.AUTH_URL,
                data=payload,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('accessToken')
                expires_in = data.get('expiresIn', 3600)
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
                
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}'
                })
                
                print(f"✅ iFood API authenticated successfully")
                return True
            else:
                print(f"❌ Authentication failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Authentication error: {e}")
            return False
    
    def get_merchant_details(self, merchant_id: str) -> Optional[Dict]:
        """Get merchant details (real or mock)
        
        FIXED: Returns only the merchant details, not the full structure
        """
        if self.use_mock_data:
            # Generate mock data if not cached
            if merchant_id not in self._mock_merchants:
                full_data = MockIFoodDataGenerator.generate_merchant_data(
                    merchant_id=merchant_id,
                    num_orders=150,
                    days=30
                )
                # Cache the complete data
                self._mock_merchants[merchant_id] = full_data
            
            # Return ONLY the details part
            return self._mock_merchants[merchant_id]['details']
        
        # Real API call
        return self._request('GET', f'/merchant/v1.0/merchants/{merchant_id}')
    
    def get_orders(self, merchant_id: str, start_date: str = None, 
                   end_date: str = None, status: str = None) -> List[Dict]:
        """Get orders (real or mock)
        
        FIXED: Returns the orders from cached mock data
        """
        if self.use_mock_data:
            # Get orders from cache (they were generated with merchant details)
            if merchant_id not in self._mock_merchants:
                # Generate if not cached yet
                full_data = MockIFoodDataGenerator.generate_merchant_data(
                    merchant_id=merchant_id,
                    num_orders=150,
                    days=30
                )
                self._mock_merchants[merchant_id] = full_data
            
            # Get orders from the cached data
            orders = self._mock_merchants[merchant_id]['orders']
            
            # Filter by status if requested
            if status:
                orders = [o for o in orders if o.get('orderStatus') == status]
            
            return orders
        
        # Real API call
        params = {'merchantId': merchant_id}
        
        if start_date:
            params['createdAtStart'] = f"{start_date}T00:00:00Z"
        if end_date:
            params['createdAtEnd'] = f"{end_date}T23:59:59Z"
        if status:
            params['status'] = status
        
        all_orders = []
        page = 1
        per_page = 100
        
        while True:
            params['page'] = page
            params['size'] = per_page
            
            result = self._request('GET', '/order/v1.0/events:polling', params=params)
            
            if not result:
                break
            
            orders = []
            if isinstance(result, list):
                orders = result
            elif isinstance(result, dict):
                if 'data' in result:
                    orders = result['data']
                elif 'orders' in result:
                    orders = result['orders']
            
            if not orders:
                break
            
            all_orders.extend(orders)
            
            if len(orders) < per_page:
                break
            
            page += 1
            time.sleep(0.5)
        
        return all_orders
    
    def get_interruptions(self, merchant_id: str) -> List[Dict]:
        """Get store interruptions/temporary closures"""
        if self.use_mock_data:
            # Generate random interruptions for variety
            # Cache them per merchant so they're consistent
            if not hasattr(self, '_interruptions_cache'):
                self._interruptions_cache = {}
            
            if merchant_id not in self._interruptions_cache:
                import random
                interruptions = []
                today = datetime.now()
                
                # 60% chance of having an interruption
                if random.random() < 0.6:
                    # Random interruption during the day
                    # Pick a random hour between 0-23
                    start_hour = random.randint(0, 22)
                    # Duration between 1-4 hours
                    duration = random.randint(1, 4)
                    end_hour = min(start_hour + duration, 23)
                    
                    reasons = [
                        "Pausa para manutenção",
                        "Falta de energia",
                        "Problema técnico",
                        "Pausa para reabastecimento",
                        "Treinamento de equipe",
                        "Limpeza programada",
                        "Pausa do sistema"
                    ]
                    
                    interruptions.append({
                        "id": f"mock-interruption-{merchant_id}-1",
                        "description": random.choice(reasons),
                        "start": today.replace(hour=start_hour, minute=0, second=0).isoformat(),
                        "end": today.replace(hour=end_hour, minute=0, second=0).isoformat()
                    })
                
                self._interruptions_cache[merchant_id] = interruptions
            
            return self._interruptions_cache[merchant_id]
        
        # Real API call
        return self._request('GET', f'/merchant/v1.0/merchants/{merchant_id}/interruptions')
    
    def create_interruption(self, merchant_id: str, start: str, end: str, description: str = "") -> Optional[Dict]:
        """Create a temporary store closure"""
        if self.use_mock_data:
            return {
                "id": f"mock-interruption-{int(time.time())}",
                "description": description,
                "start": start,
                "end": end
            }
        
        payload = {
            "start": start,
            "end": end,
            "description": description
        }
        return self._request('POST', f'/merchant/v1.0/merchants/{merchant_id}/interruptions', data=payload)
    
    def delete_interruption(self, merchant_id: str, interruption_id: str) -> bool:
        """Remove an interruption/reopen the store"""
        if self.use_mock_data:
            return True
        
        result = self._request('DELETE', f'/merchant/v1.0/merchants/{merchant_id}/interruptions/{interruption_id}')
        return result is not None
    
    def get_merchant_status(self, merchant_id: str) -> Optional[Dict]:
        """Get merchant operational status"""
        if self.use_mock_data:
            return {
                "state": "OK",
                "message": "Loja online e operacional",
                "validations": [
                    {"code": "is-connected", "status": "OK"},
                    {"code": "opening-hours", "status": "OK"}
                ],
                "reopenable": None
            }
        
        return self._request('GET', f'/merchant/v1.0/merchants/{merchant_id}/status')
    
    def get_merchants(self) -> List[Dict]:
        """Get all merchants (real or mock)"""
        if self.use_mock_data:
            return [
                {"id": merchant_id, "name": data['details'].get('name', 'Mock Restaurant')}
                for merchant_id, data in self._mock_merchants.items()
            ]
        
        result = self._request('GET', '/merchant/v1.0/merchants')
        if result and isinstance(result, list):
            return result
        elif result and isinstance(result, dict):
            return result.get('merchants', [])
        return []
    
    def _request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Optional[Dict]:
        """Make API request with error handling"""
        if self.use_mock_data:
            return None
        
        if not self.access_token:
            if not self.authenticate():
                return None
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            if method == 'GET':
                response = self.session.get(url, params=params)
            elif method == 'POST':
                response = self.session.post(url, json=data, params=params)
            elif method == 'PUT':
                response = self.session.put(url, json=data, params=params)
            elif method == 'DELETE':
                response = self.session.delete(url, params=params)
            else:
                return None
            
            if response.status_code in [200, 201, 202]:
                return response.json() if response.content else {}
            elif response.status_code == 401:
                if self.authenticate():
                    return self._request(method, endpoint, params, data)
            else:
                print(f"API Error: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"Request error: {e}")
            return None


class IFoodConfig:
    """Handle iFood configuration"""
    
    @staticmethod
    def load_config(config_path: str) -> Optional[Dict]:
        """Load configuration from JSON file"""
        try:
            path = Path(config_path)
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
        return None
    
    @staticmethod
    def save_config(config: Dict, config_path: str) -> bool:
        """Save configuration to JSON file"""
        try:
            path = Path(config_path)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving config: {e}")
            return False
    
    @staticmethod
    def create_sample_config(config_path: str):
        """Create a sample configuration file"""
        sample_config = {
            "client_id": "MOCK_DATA_MODE",
            "client_secret": "not_needed_for_mock",
            "data_fetch_days": 30,
            "merchants": [
                {
                    "merchant_id": "mock-restaurant-1",
                    "name": "Restaurante Demo",
                    "manager": "Gerente"
                }
            ]
        }
        IFoodConfig.save_config(sample_config, config_path)


if __name__ == "__main__":
    print("iFood API Module - FIXED VERSION")
    print("=" * 60)
    print("\nThis version properly handles mock data structure")
    print("✓ get_merchant_details() returns only merchant details")
    print("✓ get_orders() returns orders from cached data")
    print("✓ Consistent data across both calls")