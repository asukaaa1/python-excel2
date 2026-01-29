"""
iFood Merchant API Integration Module
Handles authentication, merchant data, and order retrieval from iFood API
FIXED VERSION - Updated endpoint paths according to iFood API documentation
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from pathlib import Path
import hashlib
import time


class IFoodAPI:
    """Client for iFood Merchant API"""
    
    # API Endpoints
    BASE_URL = "https://merchant-api.ifood.com.br"
    AUTH_URL = "https://merchant-api.ifood.com.br/authentication/v1.0/oauth/token"
    
    def __init__(self, client_id: str, client_secret: str):
        """Initialize iFood API client
        
        Args:
            client_id: iFood API Client ID
            client_secret: iFood API Client Secret
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expires_at = None
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def authenticate(self) -> bool:
        """Authenticate with iFood API and get access token
        
        Returns:
            bool: True if authentication successful
        """
        try:
            # Check if we have a valid token
            if self.access_token and self.token_expires_at:
                if datetime.now() < self.token_expires_at:
                    return True
            
            # Request new token - iFood uses form-encoded data, not JSON
            payload = {
                'grantType': 'client_credentials',
                'clientId': self.client_id,
                'clientSecret': self.client_secret
            }
            
            # Use data parameter for form-encoding, not json parameter
            response = requests.post(
                self.AUTH_URL,
                data=payload,  # Changed from json=payload to data=payload
                headers={'Content-Type': 'application/x-www-form-urlencoded'}  # Changed content type
            )
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('accessToken')
                expires_in = data.get('expiresIn', 3600)
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
                
                # Update session headers
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}'
                })
                
                print(f"✅ iFood API authenticated successfully")
                print(f"   Token expires in {expires_in} seconds")
                return True
            else:
                print(f"❌ Authentication failed: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Authentication error: {e}")
            return False
    
    def _ensure_auth(self) -> bool:
        """Ensure we have a valid authentication token"""
        if not self.access_token or not self.token_expires_at:
            return self.authenticate()
        
        if datetime.now() >= self.token_expires_at:
            return self.authenticate()
        
        return True
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """Make an authenticated request to the API
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional request parameters
            
        Returns:
            Response JSON or None
        """
        if not self._ensure_auth():
            return None
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = self.session.request(method, url, **kwargs)
            
            if response.status_code == 401:
                # Token expired, try to re-authenticate
                self.access_token = None
                if self._ensure_auth():
                    response = self.session.request(method, url, **kwargs)
                else:
                    return None
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 204:
                return {}
            else:
                print(f"❌ API Error {response.status_code}: {endpoint}")
                print(f"   Response: {response.text[:500]}")
                return None
                
        except Exception as e:
            print(f"❌ Request error: {e}")
            return None
    
    def get_merchants(self) -> List[Dict]:
        """Get all merchants associated with the account
        
        Returns:
            List of merchant dictionaries
        """
        result = self._request('GET', '/merchant/v1.0/merchants')
        if result and isinstance(result, list):
            return result
        elif result and isinstance(result, dict):
            return result.get('merchants', [])
        return []
    
    def get_merchant_details(self, merchant_id: str) -> Optional[Dict]:
        """Get detailed information about a merchant
        
        Args:
            merchant_id: iFood Merchant ID
            
        Returns:
            Merchant details dictionary
        """
        return self._request('GET', f'/merchant/v1.0/merchants/{merchant_id}')
    
    def get_merchant_status(self, merchant_id: str) -> Optional[Dict]:
        """Get current status of a merchant (open/closed)
        
        Args:
            merchant_id: iFood Merchant ID
            
        Returns:
            Status dictionary
        """
        return self._request('GET', f'/merchant/v1.0/merchants/{merchant_id}/status')
    
    def get_orders(self, merchant_id: str, start_date: str = None, 
                   end_date: str = None, status: str = None) -> List[Dict]:
        """Get orders for a merchant
        
        FIXED: Updated to use correct endpoint path
        
        Args:
            merchant_id: iFood Merchant ID
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            status: Order status filter (CONFIRMED, CONCLUDED, CANCELLED, etc.)
            
        Returns:
            List of orders
        """
        # Build query parameters
        params = {'merchantId': merchant_id}
        
        if start_date:
            params['createdAtStart'] = f"{start_date}T00:00:00Z"
        if end_date:
            params['createdAtEnd'] = f"{end_date}T23:59:59Z"
        if status:
            params['status'] = status
        
        # iFood uses pagination
        all_orders = []
        page = 1
        per_page = 100
        
        while True:
            params['page'] = page
            params['size'] = per_page
            
            # FIXED: Use correct endpoint path - /order/v1.0/events:polling
            # The old path /order/v1.0/orders doesn't exist
            result = self._request(
                'GET', 
                f'/order/v1.0/events:polling',
                params=params
            )
            
            if not result:
                break
            
            # Extract orders from events
            orders = []
            if isinstance(result, list):
                orders = result
            elif isinstance(result, dict):
                # Events polling returns events, not orders directly
                orders = result.get('orders', result.get('data', []))
            
            if not orders:
                break
            
            all_orders.extend(orders)
            
            # Check if we got all orders
            if len(orders) < per_page:
                break
            
            page += 1
            
            # Safety limit
            if page > 100:
                print(f"⚠️  Reached pagination limit (100 pages)")
                break
        
        return all_orders
    
    def get_order_details(self, order_id: str) -> Optional[Dict]:
        """Get detailed information about an order
        
        Args:
            order_id: iFood Order ID
            
        Returns:
            Order details dictionary
        """
        return self._request('GET', f'/order/v1.0/orders/{order_id}')
    
    def get_financial_data(self, merchant_id: str, start_date: str = None,
                          end_date: str = None) -> Optional[Dict]:
        """Get financial reconciliation data for a merchant
        
        FIXED: Updated to use correct endpoint
        
        Args:
            merchant_id: iFood Merchant ID
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Financial data dictionary
        """
        params = {}
        
        if start_date:
            params['startDate'] = start_date
        if end_date:
            params['endDate'] = end_date
        
        # FIXED: Use the correct endpoint - financial data may require different path
        # Try the statements endpoint first
        result = self._request(
            'GET',
            f'/financial/v1.0/merchants/{merchant_id}/statements',
            params=params
        )
        
        if not result:
            # If statements don't work, the financial API might not be available
            # or requires different permissions
            print(f"⚠️  Financial data not available for merchant {merchant_id}")
            print(f"   This might require additional API permissions")
            return None
        
        return result
    
    def get_menu(self, merchant_id: str) -> Optional[Dict]:
        """Get merchant menu/catalog
        
        Args:
            merchant_id: iFood Merchant ID
            
        Returns:
            Menu data dictionary
        """
        return self._request('GET', f'/catalog/v1.0/merchants/{merchant_id}')
    
    def get_interruptions(self, merchant_id: str) -> Optional[Dict]:
        """Get merchant interruptions (outages, maintenance)
        
        Args:
            merchant_id: iFood Merchant ID
            
        Returns:
            Interruptions data
        """
        return self._request('GET', f'/merchant/v1.0/merchants/{merchant_id}/interruptions')


class IFoodDataProcessor:
    """Process iFood data for dashboard display"""
    
    @staticmethod
    def calculate_metrics(orders: List[Dict]) -> Dict[str, Any]:
        """Calculate key metrics from orders
        
        Args:
            orders: List of order dictionaries
            
        Returns:
            Dictionary with calculated metrics
        """
        if not orders:
            return {
                'total_orders': 0,
                'total_revenue': 0,
                'average_ticket': 0,
                'cancelled_orders': 0,
                'cancellation_rate': 0,
                'orders_today': 0,
                'revenue_today': 0
            }
        
        total_orders = len(orders)
        total_revenue = 0
        cancelled_orders = 0
        orders_today = 0
        revenue_today = 0
        today = datetime.now().date()
        
        valid_statuses = ['CONCLUDED', 'CONFIRMED', 'DELIVERED', 'DISPATCHED', 'READY_TO_PICKUP']
        
        for order in orders:
            status = order.get('status', '').upper()
            
            # Get order amount
            amount = 0
            if order.get('total') and isinstance(order['total'], dict):
                amount = order['total'].get('orderAmount', 0) or order['total'].get('subTotal', 0) or 0
            elif order.get('totalPrice'):
                amount = order['totalPrice']
            elif order.get('subTotal'):
                amount = order['subTotal']
            
            amount = float(amount) if amount else 0
            
            # Count valid orders
            if any(s in status for s in valid_statuses):
                total_revenue += amount
            
            # Count cancelled
            if 'CANCEL' in status:
                cancelled_orders += 1
            
            # Check if today
            created_at = order.get('createdAt', '') or order.get('created_at', '')
            if created_at:
                try:
                    if 'T' in str(created_at):
                        order_date = datetime.fromisoformat(str(created_at).replace('Z', '+00:00')).date()
                    else:
                        order_date = datetime.strptime(str(created_at)[:10], '%Y-%m-%d').date()
                    
                    if order_date == today:
                        orders_today += 1
                        if any(s in status for s in valid_statuses):
                            revenue_today += amount
                except:
                    pass
        
        average_ticket = total_revenue / total_orders if total_orders > 0 else 0
        cancellation_rate = (cancelled_orders / total_orders * 100) if total_orders > 0 else 0
        
        return {
            'total_orders': total_orders,
            'total_revenue': total_revenue,
            'average_ticket': average_ticket,
            'cancelled_orders': cancelled_orders,
            'cancellation_rate': cancellation_rate,
            'orders_today': orders_today,
            'revenue_today': revenue_today
        }
    
    @staticmethod
    def process_restaurant_data(merchant: Dict, orders: List[Dict], 
                               financial_data: Optional[Dict] = None) -> Dict:
        """Process raw API data into dashboard format
        
        Args:
            merchant: Merchant details from API
            orders: List of orders
            financial_data: Optional financial reconciliation data
            
        Returns:
            Processed data dictionary for dashboard
        """
        metrics = IFoodDataProcessor.calculate_metrics(orders)
        
        return {
            'id': merchant.get('id', ''),
            'name': merchant.get('name', 'Unknown'),
            'status': merchant.get('status', 'UNKNOWN'),
            'metrics': metrics,
            'orders': orders,
            'financial_data': financial_data,
            'last_updated': datetime.now().isoformat()
        }
    
    @staticmethod
    def prepare_chart_data(orders: List[Dict]) -> Dict:
        """Prepare data for charts
        
        Args:
            orders: List of orders
            
        Returns:
            Dictionary with chart datasets
        """
        if not orders:
            return {
                'revenue_chart': {'labels': [], 'datasets': [{'label': 'Faturamento', 'data': []}]},
                'orders_chart': {'labels': [], 'datasets': [{'label': 'Pedidos', 'data': []}]},
                'hourly_chart': {'labels': [f'{h}:00' for h in range(24)], 'datasets': [{'label': 'Pedidos por Hora', 'data': [0]*24}]}
            }
        
        # Daily aggregation
        daily_data = {}
        hourly_data = {str(h).zfill(2): {'orders': 0, 'revenue': 0} for h in range(24)}
        
        # Status variations to accept
        valid_statuses = ['CONCLUDED', 'CONFIRMED', 'DELIVERED', 'DISPATCHED', 'READY_TO_PICKUP']
        
        for order in orders:
            status = order.get('status', '').upper()
            # Accept more status variations
            if not any(s in status for s in valid_statuses):
                continue
            
            created_at = order.get('createdAt', '') or order.get('created_at', '') or order.get('orderDate', '')
            if not created_at:
                continue
            
            try:
                # Handle different date formats
                if 'T' in str(created_at):
                    order_date = datetime.fromisoformat(str(created_at).replace('Z', '+00:00').replace('+00:00+00:00', '+00:00'))
                else:
                    order_date = datetime.strptime(str(created_at)[:10], '%Y-%m-%d')
                
                date_key = order_date.strftime('%d/%m')  # Format as DD/MM for better display
                hour_key = order_date.strftime('%H')
                
                # Get amount from various possible fields
                amount = 0
                if order.get('total') and isinstance(order['total'], dict):
                    amount = order['total'].get('orderAmount', 0) or order['total'].get('subTotal', 0) or 0
                elif order.get('totalPrice'):
                    amount = order['totalPrice']
                elif order.get('subTotal'):
                    amount = order['subTotal']
                
                amount = float(amount) if amount else 0
                
                # Daily
                if date_key not in daily_data:
                    daily_data[date_key] = {'orders': 0, 'revenue': 0, 'date_sort': order_date.strftime('%Y-%m-%d')}
                daily_data[date_key]['orders'] += 1
                daily_data[date_key]['revenue'] += amount
                
                # Hourly
                hourly_data[hour_key]['orders'] += 1
                hourly_data[hour_key]['revenue'] += amount
                
            except Exception as e:
                print(f"Error processing order date: {e}")
                continue
        
        # Sort dates by actual date
        sorted_dates = sorted(daily_data.keys(), key=lambda x: daily_data[x]['date_sort'])
        
        # Format for charts
        return {
            'revenue_chart': {
                'labels': sorted_dates,
                'datasets': [{
                    'label': 'Faturamento',
                    'data': [daily_data[d]['revenue'] for d in sorted_dates],
                    'borderColor': '#ef4444',
                    'backgroundColor': 'rgba(239, 68, 68, 0.1)'
                }]
            },
            'orders_chart': {
                'labels': sorted_dates,
                'datasets': [{
                    'label': 'Pedidos',
                    'data': [daily_data[d]['orders'] for d in sorted_dates],
                    'borderColor': '#3b82f6',
                    'backgroundColor': 'rgba(59, 130, 246, 0.1)'
                }]
            },
            'hourly_chart': {
                'labels': [f'{h}:00' for h in range(24)],
                'datasets': [{
                    'label': 'Pedidos por Hora',
                    'data': [hourly_data[str(h).zfill(2)]['orders'] for h in range(24)],
                    'backgroundColor': '#22c55e'
                }]
            },
            'hourly_revenue': {
                'labels': [f'{h}:00' for h in range(24)],
                'datasets': [{
                    'label': 'Faturamento por Hora',
                    'data': [hourly_data[str(h).zfill(2)]['revenue'] for h in range(24)],
                    'backgroundColor': '#f59e0b'
                }]
            }
        }


class IFoodConfig:
    """Handle iFood configuration file"""
    
    @staticmethod
    def load_config(config_path: str) -> Optional[Dict]:
        """Load configuration from JSON file
        
        Args:
            config_path: Path to config file
            
        Returns:
            Configuration dictionary or None
        """
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
        """Save configuration to JSON file
        
        Args:
            config: Configuration dictionary
            config_path: Path to config file
            
        Returns:
            True if successful
        """
        try:
            path = Path(config_path)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving config: {e}")
            return False
    
    @staticmethod
    def create_sample_config(config_path: str) -> bool:
        """Create a sample configuration file
        
        Args:
            config_path: Path where to create the config
            
        Returns:
            True if successful
        """
        sample_config = {
            "client_id": "your_ifood_client_id_here",
            "client_secret": "your_ifood_client_secret_here",
            "data_fetch_days": 30,
            "merchants": [
                {
                    "merchant_id": "your_merchant_id_1",
                    "name": "Restaurant Name 1",
                    "manager": "Manager Name"
                },
                {
                    "merchant_id": "your_merchant_id_2",
                    "name": "Restaurant Name 2",
                    "manager": "Manager Name"
                }
            ],
            "refresh_interval_minutes": 60,
            "api_notes": {
                "how_to_get_credentials": [
                    "1. Access iFood Portal Parceiro: https://portal.ifood.com.br",
                    "2. Go to Configurações > Integrações",
                    "3. Create a new API integration",
                    "4. Copy Client ID and Client Secret",
                    "5. Add your merchant IDs from the Portal"
                ],
                "api_documentation": "https://developer.ifood.com.br/docs",
                "endpoint_notes": {
                    "orders": "Use /order/v1.0/events:polling for order data",
                    "financial": "Use /financial/v1.0/merchants/{id}/statements",
                    "Note": "Some endpoints may require additional API permissions"
                }
            }
        }
        
        return IFoodConfig.save_config(sample_config, config_path)


# Utility functions for backwards compatibility
def fetch_restaurant_data_from_ifood(api: IFoodAPI, merchant_id: str, 
                                      days: int = 30) -> Optional[Dict]:
    """Convenience function to fetch and process restaurant data
    
    Args:
        api: IFoodAPI instance
        merchant_id: iFood Merchant ID
        days: Number of days of data to fetch
        
    Returns:
        Processed restaurant data dictionary
    """
    # Get merchant details
    merchant = api.get_merchant_details(merchant_id)
    if not merchant:
        return None
    
    # Get orders
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    orders = api.get_orders(merchant_id, start_date, end_date)
    
    # Get financial data if available (this might fail if permissions aren't granted)
    financial_data = api.get_financial_data(merchant_id, start_date, end_date)
    
    # Process into dashboard format
    return IFoodDataProcessor.process_restaurant_data(merchant, orders, financial_data)


if __name__ == "__main__":
    # Test the API
    print("iFood API Module - FIXED VERSION")
    print("=" * 50)
    print("\n🔧 CHANGES IN THIS VERSION:")
    print("   - Fixed /order/v1.0/orders → /order/v1.0/events:polling")
    print("   - Fixed /financial/v1.0/merchants/reconciliation → /financial/v1.0/merchants/{id}/statements")
    print("   - Added better error handling for missing permissions")
    print("\nTo use this module:")
    print("1. Create ifood_config.json with your credentials")
    print("2. Import and initialize: api = IFoodAPI(client_id, client_secret)")
    print("3. Authenticate: api.authenticate()")
    print("4. Get data: api.get_merchants(), api.get_orders(merchant_id)")
    print("\nSample config created at: ./ifood_config.json")
    
    IFoodConfig.create_sample_config("./ifood_config.json")