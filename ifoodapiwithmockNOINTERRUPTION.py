"""
iFood API Module - WITH MOCK DATA SUPPORT
Supports both real iFood API and mock data for testing
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
    """Client for iFood Merchant API with mock data support"""
    
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
        
        # Mock data cache
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
    
    def get_merchants(self) -> List[Dict]:
        """Get all merchants (real or mock)"""
        if self.use_mock_data:
            # Return list of merchant IDs from config
            return [
                {"id": merchant_id, "name": data.get('name', 'Mock Restaurant')}
                for merchant_id, data in self._mock_merchants.items()
            ]
        
        # Real API call
        result = self._request('GET', '/merchant/v1.0/merchants')
        if result and isinstance(result, list):
            return result
        elif result and isinstance(result, dict):
            return result.get('merchants', [])
        return []
    
    def get_merchant_details(self, merchant_id: str) -> Optional[Dict]:
        """Get merchant details (real or mock)"""
        if self.use_mock_data:
            # Generate mock data if not cached
            if merchant_id not in self._mock_merchants:
                self._mock_merchants[merchant_id] = MockIFoodDataGenerator.generate_merchant_data(
                    num_orders=150,
                    days=30
                )
            return self._mock_merchants[merchant_id]
        
        # Real API call
        return self._request('GET', f'/merchant/v1.0/merchants/{merchant_id}')
    
    def get_orders(self, merchant_id: str, start_date: str = None, 
                   end_date: str = None, status: str = None) -> List[Dict]:
        """Get orders (real or mock)"""
        if self.use_mock_data:
            # Get or generate mock merchant data
            if merchant_id not in self._mock_merchants:
                self._mock_merchants[merchant_id] = MockIFoodDataGenerator.generate_merchant_data(
                    num_orders=150,
                    days=30
                )
            
            merchant_data = self._mock_merchants[merchant_id]
            orders = merchant_data.get('orders', [])
            
            # Filter by date if provided
            if start_date or end_date:
                filtered_orders = []
                for order in orders:
                    order_date_str = order.get('orderDate', '')
                    if order_date_str:
                        if start_date and order_date_str < start_date:
                            continue
                        if end_date and order_date_str > end_date:
                            continue
                        filtered_orders.append(order)
                orders = filtered_orders
            
            # Filter by status if provided
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
    
    def get_financial_data(self, merchant_id: str, start_date: str, end_date: str) -> Optional[Dict]:
        """Get financial data (mock or real)"""
        if self.use_mock_data:
            # Calculate from orders
            orders = self.get_orders(merchant_id, start_date, end_date)
            concluded_orders = [o for o in orders if o.get('orderStatus') == 'CONCLUDED']
            
            total_revenue = sum(o.get('totalPrice', 0) for o in concluded_orders)
            total_fees = total_revenue * 0.12  # Simulate 12% commission
            
            return {
                "totalRevenue": total_revenue,
                "totalFees": total_fees,
                "netRevenue": total_revenue - total_fees,
                "orderCount": len(concluded_orders)
            }
        
        # Real API call (might fail if no permission)
        try:
            return self._request(
                'GET',
                f'/financial/v1.0/merchants/{merchant_id}/statements',
                params={'startDate': start_date, 'endDate': end_date}
            )
        except:
            return None
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """Make authenticated request to real API"""
        if self.use_mock_data:
            return None  # Mock mode doesn't make real requests
        
        if not self._ensure_auth():
            return None
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = self.session.request(method, url, **kwargs)
            
            if response.status_code == 401:
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
                return None
                
        except Exception as e:
            print(f"❌ Request error: {e}")
            return None
    
    def _ensure_auth(self) -> bool:
        """Ensure we have valid authentication"""
        if self.use_mock_data:
            return True
        
        if not self.access_token or not self.token_expires_at:
            return self.authenticate()
        
        if datetime.now() >= self.token_expires_at:
            return self.authenticate()
        
        return True


class IFoodDataProcessor:
    """Process iFood data into dashboard format"""
    
    @staticmethod
    def process_restaurant_data(merchant_details: Dict, orders: List[Dict],
                               financial_data: Optional[Dict] = None) -> Dict:
        """Process raw iFood data into dashboard format"""
        
        # Get concluded orders only for metrics
        concluded_orders = [o for o in orders if o.get('orderStatus') == 'CONCLUDED']
        
        # Calculate metrics
        total_orders = len(concluded_orders)
        total_revenue = sum(o.get('totalPrice', 0) or o.get('total', {}).get('orderAmount', 0) for o in concluded_orders)
        avg_ticket = total_revenue / total_orders if total_orders > 0 else 0
        
        # Calculate net revenue (liquido) - typically 70-85% of gross after iFood fees
        # iFood typically takes 12-27% commission depending on plan
        commission_rate = 0.20  # Default 20% commission estimate
        liquido = total_revenue * (1 - commission_rate)
        
        # Calculate trends by comparing recent period vs older period
        trends = IFoodDataProcessor._calculate_trends(concluded_orders)
        
        # Process for dashboard - MUST include 'metrics' object with Portuguese keys
        # for the frontend to display correctly
        return {
            "id": merchant_details.get('id', 'unknown'),
            "name": merchant_details.get('name', merchant_details.get('corporateName', 'Unknown Restaurant')),
            "manager": merchant_details.get('merchantManager', {}).get('name', 'Gerente'),
            "platforms": ["iFood"],  # Add platforms array for frontend
            
            # Metrics object with Portuguese keys that the frontend expects
            "metrics": {
                "vendas": total_orders,
                "ticket_medio": round(avg_ticket, 2),
                "valor_bruto": round(total_revenue, 2),
                "liquido": round(liquido, 2),
                "trends": trends
            },
            
            # Also keep the English keys for backward compatibility
            "total_orders": total_orders,
            "total_revenue": round(total_revenue, 2),
            "average_ticket": round(avg_ticket, 2),
            "cancellation_rate": IFoodDataProcessor._calculate_cancellation_rate(orders),
            "top_items": IFoodDataProcessor._get_top_items(concluded_orders),
            "performance_trend": IFoodDataProcessor._calculate_trend(concluded_orders),
            "raw_orders": orders,  # Keep for detailed analysis
            "concluded_orders": concluded_orders
        }
    
    @staticmethod
    def _calculate_trends(orders: List[Dict]) -> Dict:
        """Calculate percentage trends for all metrics comparing recent vs older periods"""
        if len(orders) < 2:
            return {
                "vendas": 0,
                "ticket_medio": 0,
                "valor_bruto": 0,
                "liquido": 0
            }
        
        # Sort orders by date
        sorted_orders = sorted(orders, key=lambda x: x.get('createdAt', '') or x.get('orderDate', ''))
        
        # Split orders into two halves (recent vs older)
        mid = len(sorted_orders) // 2
        older_half = sorted_orders[:mid]
        recent_half = sorted_orders[mid:]
        
        # Calculate metrics for each half
        def calc_metrics(order_list):
            count = len(order_list)
            revenue = sum(o.get('totalPrice', 0) or o.get('total', {}).get('orderAmount', 0) for o in order_list)
            avg_ticket = revenue / count if count > 0 else 0
            return count, revenue, avg_ticket
        
        older_count, older_revenue, older_ticket = calc_metrics(older_half)
        recent_count, recent_revenue, recent_ticket = calc_metrics(recent_half)
        
        # Calculate percentage change
        def pct_change(new_val, old_val):
            if old_val == 0:
                return 0 if new_val == 0 else 100
            return round(((new_val - old_val) / old_val) * 100, 2)
        
        vendas_trend = pct_change(recent_count, older_count)
        valor_bruto_trend = pct_change(recent_revenue, older_revenue)
        ticket_medio_trend = pct_change(recent_ticket, older_ticket)
        
        return {
            "vendas": vendas_trend,
            "ticket_medio": ticket_medio_trend,
            "valor_bruto": valor_bruto_trend,
            "liquido": valor_bruto_trend  # Liquido trend follows gross revenue trend
        }
    
    @staticmethod
    def _calculate_cancellation_rate(orders: List[Dict]) -> float:
        """Calculate cancellation rate"""
        if not orders:
            return 0.0
        cancelled = len([o for o in orders if o.get('orderStatus') == 'CANCELLED'])
        return round((cancelled / len(orders)) * 100, 2)
    
    @staticmethod
    def _get_top_items(orders: List[Dict], limit: int = 5) -> List[Dict]:
        """Get top selling items"""
        item_counts = {}
        
        for order in orders:
            items = order.get('items', [])
            for item in items:
                name = item.get('name', 'Unknown')
                quantity = item.get('quantity', 1)
                item_counts[name] = item_counts.get(name, 0) + quantity
        
        # Sort and return top items
        sorted_items = sorted(item_counts.items(), key=lambda x: x[1], reverse=True)
        return [{"name": name, "quantity": qty} for name, qty in sorted_items[:limit]]
    
    @staticmethod
    def _calculate_trend(orders: List[Dict]) -> str:
        """Calculate performance trend"""
        if len(orders) < 2:
            return "stable"
        
        # Split orders into two halves
        mid = len(orders) // 2
        recent_half = orders[:mid]
        older_half = orders[mid:]
        
        recent_revenue = sum(o.get('totalPrice', 0) for o in recent_half)
        older_revenue = sum(o.get('totalPrice', 0) for o in older_half)
        
        if recent_revenue > older_revenue * 1.1:
            return "up"
        elif recent_revenue < older_revenue * 0.9:
            return "down"
        return "stable"
    
    @staticmethod
    def generate_charts_data(orders: List[Dict]) -> Dict:
        """Generate chart data from orders"""
        # Similar to existing implementation
        daily_data = {}
        hourly_data = {str(h).zfill(2): {'orders': 0, 'revenue': 0} for h in range(24)}
        
        concluded_orders = [o for o in orders if o.get('orderStatus') == 'CONCLUDED']
        
        for order in concluded_orders:
            created_at = order.get('createdAt', '') or order.get('created_at', '')
            if not created_at:
                continue
            
            try:
                if 'T' in str(created_at):
                    order_date = datetime.fromisoformat(str(created_at).replace('Z', '+00:00'))
                else:
                    order_date = datetime.strptime(str(created_at)[:10], '%Y-%m-%d')
                
                date_key = order_date.strftime('%d/%m')
                hour_key = order_date.strftime('%H')
                
                amount = order.get('totalPrice', 0) or order.get('total', {}).get('orderAmount', 0)
                amount = float(amount) if amount else 0
                
                if date_key not in daily_data:
                    daily_data[date_key] = {'orders': 0, 'revenue': 0, 'date_sort': order_date.strftime('%Y-%m-%d')}
                daily_data[date_key]['orders'] += 1
                daily_data[date_key]['revenue'] += amount
                
                hourly_data[hour_key]['orders'] += 1
                hourly_data[hour_key]['revenue'] += amount
                
            except Exception as e:
                continue
        
        sorted_dates = sorted(daily_data.keys(), key=lambda x: daily_data[x]['date_sort'])
        
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
            }
        }


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
    def create_sample_config(config_path: str) -> bool:
        """Create sample config with mock data option"""
        sample_config = {
            "use_mock_data": True,
            "client_id": "MOCK_DATA_MODE",
            "client_secret": "MOCK_DATA_MODE",
            "data_fetch_days": 30,
            "merchants": [],
            "note": "Set use_mock_data to true to use sample data for testing. Set to false and add real credentials for production."
        }
        
        # Generate mock merchants if available
        if MOCK_AVAILABLE:
            mock_restaurants = MockIFoodDataGenerator.RESTAURANTS[:5]
            sample_config["merchants"] = [
                {
                    "merchant_id": MockIFoodDataGenerator.generate_merchant_id(r['name']),
                    "name": r['name'],
                    "manager": r['manager']
                }
                for r in mock_restaurants
            ]
        
        return IFoodConfig.save_config(sample_config, config_path)


if __name__ == "__main__":
    print("iFood API Module - WITH MOCK DATA SUPPORT")
    print("=" * 60)
    print("\n🎭 This version supports MOCK DATA for testing!")
    print("\nTo use mock data:")
    print("1. Set 'use_mock_data': true in ifood_config.json")
    print("2. Or set client_id to 'MOCK_DATA_MODE'")
    print("\nCreating sample config...")
    IFoodConfig.create_sample_config("./ifood_config.json")
    print("✅ Sample config created!")