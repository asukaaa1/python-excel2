"""
iFood API Module - FIXED VERSION
Fixed mock data structure to properly return merchant details and orders separately
"""

import requests
import json
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from pathlib import Path
import time
from urllib.parse import urlencode
from urllib.request import Request, build_opener, ProxyHandler
from urllib.error import HTTPError, URLError

# Import mock data generator
try:
    from mock_ifood_data import MockIFoodDataGenerator
    MOCK_AVAILABLE = True
except ImportError:
    MOCK_AVAILABLE = False
    print("âš ï¸  Mock data generator not available")


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
        self.last_auth_error = None
        self._http_client = str(os.environ.get('IFOOD_HTTP_CLIENT', 'requests')).strip().lower()
        self._trust_env = str(os.environ.get('IFOOD_TRUST_ENV', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
        self.session = requests.Session()
        self.session.trust_env = self._trust_env
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        if self._trust_env:
            self._urllib_opener = build_opener()
        else:
            # Avoid inheriting proxy env vars unless explicitly enabled.
            self._urllib_opener = build_opener(ProxyHandler({}))
        
        # Mock data cache - FIXED: Store complete merchant data
        self._mock_merchants = {}
        
        if self.use_mock_data:
            print("ðŸŽ­ Running in MOCK DATA mode - using sample data for testing")
    
    def authenticate(self) -> bool:
        """Authenticate with iFood API (or fake it for mock mode)"""
        if self.use_mock_data:
            print("Mock authentication successful")
            self.access_token = "MOCK_TOKEN"
            self.token_expires_at = datetime.now() + timedelta(hours=24)
            self.last_auth_error = None
            return True

        try:
            if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at:
                self.last_auth_error = None
                return True

            if not str(self.client_id or '').strip() or not str(self.client_secret or '').strip():
                self.last_auth_error = "missing_client_credentials"
                print("Authentication failed: missing client credentials")
                return False

            payload = {
                'grantType': 'client_credentials',
                'clientId': self.client_id,
                'clientSecret': self.client_secret
            }
            timeout_seconds = float(os.environ.get('IFOOD_HTTP_TIMEOUT', 20))
            payload_bytes = urlencode(payload).encode('utf-8')
            request_obj = Request(
                self.AUTH_URL,
                data=payload_bytes,
                method='POST',
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept': 'application/json'
                }
            )
            try:
                with self._urllib_opener.open(request_obj, timeout=timeout_seconds) as response:
                    body = response.read().decode('utf-8', errors='replace')
                    data = json.loads(body) if body else {}
                    status_code = int(getattr(response, 'status', 200) or 200)
            except HTTPError as http_err:
                status_code = int(getattr(http_err, 'code', 0) or 0)
                err_body = ''
                try:
                    err_body = http_err.read().decode('utf-8', errors='replace')
                except Exception:
                    pass
                response_snippet = str(err_body or '').strip().replace('\n', ' ')[:200]
                self.last_auth_error = f"http_{status_code}:{response_snippet}" if response_snippet else f"http_{status_code}"
                print(f"Authentication failed: {status_code} {response_snippet}")
                return False
            except URLError as net_err:
                self.last_auth_error = f"url_error:{net_err}"
                print(f"Authentication failed: network error {net_err}")
                return False

            if status_code != 200:
                self.last_auth_error = f"http_{status_code}"
                print(f"Authentication failed: {status_code}")
                return False

            token = data.get('accessToken') or data.get('access_token')
            if not token:
                self.last_auth_error = "missing_access_token_in_response"
                self.access_token = None
                self.token_expires_at = None
                print("Authentication failed: token not present in response")
                return False

            self.access_token = token
            expires_in_raw = data.get('expiresIn', data.get('expires_in', 3600))
            try:
                expires_in = int(expires_in_raw)
            except Exception:
                expires_in = 3600
            self.token_expires_at = datetime.now() + timedelta(seconds=max(60, expires_in - 60))

            self.session.headers.update({'Authorization': f'Bearer {self.access_token}'})
            self.last_auth_error = None
            print("iFood API authenticated successfully")
            return True

        except RecursionError:
            self.last_auth_error = "recursion_error_during_auth_request"
            self.access_token = None
            self.token_expires_at = None
            print("Authentication error: recursion detected while requesting token")
            return False
        except Exception as e:
            self.last_auth_error = str(e)
            print(f"Authentication error: {e}")
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
                wanted_status = self._normalize_order_status(status)
                orders = [o for o in orders if self._normalize_order_status(o.get('orderStatus')) == wanted_status]
            
            return orders
        
        # Real API flow:
        # 1) poll order events, 2) resolve order details by order id.
        polling_headers = {'x-polling-merchants': str(merchant_id)}
        polling_result = self._request('GET', '/order/v1.0/events:polling', headers=polling_headers)
        events = self._extract_polling_events(polling_result)

        # Some providers return order-like payloads directly; keep them too.
        direct_orders = []
        for item in events:
            if isinstance(item, dict) and ('orderStatus' in item or 'totalPrice' in item):
                direct_orders.append(item)

        order_ids = []
        for event in events:
            order_id = self._extract_order_id_from_event(event)
            if order_id:
                order_ids.append(order_id)
        # Preserve ordering while deduplicating.
        dedup_order_ids = list(dict.fromkeys([str(oid) for oid in order_ids if oid]))

        resolved_orders = []
        for order_id in dedup_order_ids:
            details = self.get_order_details(order_id)
            if isinstance(details, dict) and details:
                resolved_orders.append(details)

        candidate_orders = resolved_orders + direct_orders

        # Fallback attempt: some tenants expose list/search endpoint.
        if not candidate_orders:
            fallback_params = {'merchantId': merchant_id}
            if start_date:
                fallback_params['createdAtStart'] = f"{start_date}T00:00:00Z"
            if end_date:
                fallback_params['createdAtEnd'] = f"{end_date}T23:59:59Z"
            if status:
                fallback_params['status'] = status
            fallback_result = self._request('GET', '/order/v1.0/orders', params=fallback_params)
            candidate_orders = self._extract_order_payload_list(fallback_result)

        return self._filter_orders(candidate_orders, merchant_id, start_date, end_date, status)

    def get_order_details(self, order_id: str) -> Optional[Dict]:
        """Resolve full order payload by order id."""
        if not order_id:
            return None

        for endpoint in (
            f'/order/v1.0/orders/{order_id}',
            f'/orders/{order_id}',
        ):
            payload = self._request('GET', endpoint)
            if isinstance(payload, dict) and payload:
                if not payload.get('id'):
                    payload['id'] = order_id
                return payload
        return None

    def _extract_polling_events(self, payload) -> List[Dict]:
        if not payload:
            return []
        if isinstance(payload, list):
            return [p for p in payload if isinstance(p, dict)]
        if isinstance(payload, dict):
            for key in ('events', 'data', 'items', 'orders'):
                value = payload.get(key)
                if isinstance(value, list):
                    return [p for p in value if isinstance(p, dict)]
            return [payload]
        return []

    def _extract_order_payload_list(self, payload) -> List[Dict]:
        if not payload:
            return []
        if isinstance(payload, list):
            return [p for p in payload if isinstance(p, dict)]
        if isinstance(payload, dict):
            for key in ('orders', 'data', 'items'):
                value = payload.get(key)
                if isinstance(value, list):
                    return [p for p in value if isinstance(p, dict)]
            return [payload]
        return []

    def _extract_order_id_from_event(self, event: Dict) -> Optional[str]:
        if not isinstance(event, dict):
            return None
        for key in ('orderId', 'order_id'):
            value = event.get(key)
            if value:
                return str(value)
        metadata = event.get('metadata')
        if isinstance(metadata, dict):
            for key in ('orderId', 'order_id'):
                value = metadata.get(key)
                if value:
                    return str(value)
        return None

    def _parse_order_datetime(self, raw_value):
        if not raw_value:
            return None
        try:
            return datetime.fromisoformat(str(raw_value).replace('Z', '+00:00'))
        except Exception:
            return None

    def _normalize_order_status(self, status_value) -> str:
        if isinstance(status_value, dict):
            status_value = (
                status_value.get('orderStatus')
                or status_value.get('status')
                or status_value.get('state')
                or status_value.get('fullCode')
                or status_value.get('code')
            )

        status = str(status_value or '').strip().upper()
        if not status:
            return 'UNKNOWN'

        status = status.replace('-', '_').replace(' ', '_')
        if status == 'CANCELED':
            status = 'CANCELLED'
        if 'CANCEL' in status or status in ('CAN', 'DECLINED', 'REJECTED'):
            return 'CANCELLED'
        if status in ('CONCLUDED', 'COMPLETED', 'DELIVERED', 'FINISHED'):
            return 'CONCLUDED'
        if status in ('CONFIRMED', 'PLACED', 'CREATED', 'PREPARING', 'READY', 'HANDOFF', 'IN_TRANSIT', 'DISPATCHED', 'PICKED_UP'):
            return 'CONFIRMED'
        return status

    def _get_order_status(self, order: Dict) -> str:
        if not isinstance(order, dict):
            return 'UNKNOWN'

        for key in ('orderStatus', 'status', 'state', 'fullCode', 'code'):
            normalized = self._normalize_order_status(order.get(key))
            if normalized != 'UNKNOWN':
                return normalized

        metadata = order.get('metadata')
        if isinstance(metadata, dict):
            for key in ('orderStatus', 'status', 'state', 'fullCode', 'code'):
                normalized = self._normalize_order_status(metadata.get(key))
                if normalized != 'UNKNOWN':
                    return normalized
        return 'UNKNOWN'

    def _normalize_order_payload(self, order: Dict) -> Dict:
        if not isinstance(order, dict):
            return order

        order['orderStatus'] = self._get_order_status(order)
        if not order.get('createdAt'):
            created_candidate = (
                order.get('created_at')
                or order.get('created')
                or order.get('createdDate')
                or order.get('creationDate')
            )
            if created_candidate:
                order['createdAt'] = created_candidate

        if not order.get('totalPrice'):
            total = order.get('total')
            if isinstance(total, dict):
                amount = total.get('orderAmount')
                if amount is None:
                    try:
                        amount = float(total.get('subTotal', 0) or 0) + float(total.get('deliveryFee', 0) or 0)
                    except Exception:
                        amount = None
                if amount is not None:
                    order['totalPrice'] = amount
        return order

    def _order_matches_merchant(self, order: Dict, merchant_id: str) -> bool:
        if not isinstance(order, dict):
            return False
        merchant_candidates = [
            order.get('merchantId'),
            order.get('merchant_id'),
            (order.get('merchant') or {}).get('id') if isinstance(order.get('merchant'), dict) else None,
            (order.get('merchant') or {}).get('merchantId') if isinstance(order.get('merchant'), dict) else None,
        ]
        merchant_candidates = [str(c) for c in merchant_candidates if c]
        if not merchant_candidates:
            return True  # keep when API omits merchant id in payload
        return str(merchant_id) in merchant_candidates

    def _filter_orders(self, orders: List[Dict], merchant_id: str, start_date: str = None,
                       end_date: str = None, status: str = None) -> List[Dict]:
        start_dt = None
        end_dt = None
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            except Exception:
                start_dt = None
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            except Exception:
                end_dt = None

        filtered = []
        for order in (orders or []):
            if not isinstance(order, dict):
                continue
            order = self._normalize_order_payload(order)
            if not self._order_matches_merchant(order, merchant_id):
                continue
            if status and self._normalize_order_status(order.get('orderStatus')) != self._normalize_order_status(status):
                continue
            created_at = self._parse_order_datetime(order.get('createdAt') or order.get('created_at'))
            if created_at:
                created_date = created_at.date()
                if start_dt and created_date < start_dt:
                    continue
                if end_dt and created_date > end_dt:
                    continue
            filtered.append(order)

        # De-duplicate by order id when possible.
        dedup = {}
        for order in filtered:
            key = str(
                order.get('id')
                or order.get('orderId')
                or order.get('displayId')
                or f"{order.get('createdAt')}:{order.get('orderStatus')}"
            )
            dedup[key] = order
        return list(dedup.values())
    
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
                        "Pausa para manutenÃ§Ã£o",
                        "Falta de energia",
                        "Problema tÃ©cnico",
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

    def _request_via_urllib(self, method: str, endpoint: str, params: Dict = None, data: Dict = None, headers: Dict = None) -> Optional[Dict]:
        """Fallback HTTP path used when requests stack is unstable."""
        timeout_seconds = float(os.environ.get('IFOOD_HTTP_TIMEOUT', 20))
        params = params or {}
        query = urlencode(params, doseq=True) if params else ''
        url = f"{self.BASE_URL}{endpoint}"
        if query:
            url = f"{url}?{query}"

        request_headers = {'Accept': 'application/json'}
        if isinstance(headers, dict):
            request_headers.update(headers)
        if self.access_token and 'Authorization' not in request_headers:
            request_headers['Authorization'] = f'Bearer {self.access_token}'
        body = None
        method_upper = str(method or '').upper()
        if method_upper in ('POST', 'PUT'):
            body = json.dumps(data or {}).encode('utf-8')
            if 'Content-Type' not in request_headers:
                request_headers['Content-Type'] = 'application/json'

        req = Request(url, data=body, method=method_upper, headers=request_headers)
        try:
            with self._urllib_opener.open(req, timeout=timeout_seconds) as resp:
                status = int(getattr(resp, 'status', 200) or 200)
                raw = resp.read()
                if status not in (200, 201, 202):
                    print(f"API Error (urllib): {status} - {endpoint}")
                    return None
                if not raw:
                    return {}
                text = raw.decode('utf-8', errors='replace').strip()
                return json.loads(text) if text else {}
        except HTTPError as http_err:
            if int(getattr(http_err, 'code', 0) or 0) == 401:
                # Signal caller to trigger one token refresh attempt.
                return {'__unauthorized__': True}
            detail = ''
            try:
                detail = http_err.read().decode('utf-8', errors='replace')
            except Exception:
                pass
            print(f"API Error (urllib): {getattr(http_err, 'code', '?')} - {detail}")
            return None
        except Exception as err:
            print(f"Request error (urllib): {err}")
            return None
    
    def _request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None, headers: Dict = None) -> Optional[Dict]:
        """Make API request with bounded retries (no recursion)."""
        if self.use_mock_data:
            return None

        url = f"{self.BASE_URL}{endpoint}"
        timeout_seconds = float(os.environ.get('IFOOD_HTTP_TIMEOUT', 20))
        max_attempts = 2  # initial request + one re-auth retry

        for attempt in range(max_attempts):
            if not self.access_token and not self.authenticate():
                return None

            if self._http_client == 'urllib':
                fallback_result = self._request_via_urllib(method, endpoint, params=params, data=data, headers=headers)
                if isinstance(fallback_result, dict) and fallback_result.get('__unauthorized__'):
                    self.access_token = None
                    self.token_expires_at = None
                    if attempt < (max_attempts - 1):
                        continue
                    return None
                return fallback_result

            try:
                if method == 'GET':
                    response = self.session.get(url, params=params, timeout=timeout_seconds, headers=headers)
                elif method == 'POST':
                    response = self.session.post(url, json=data, params=params, timeout=timeout_seconds, headers=headers)
                elif method == 'PUT':
                    response = self.session.put(url, json=data, params=params, timeout=timeout_seconds, headers=headers)
                elif method == 'DELETE':
                    response = self.session.delete(url, params=params, timeout=timeout_seconds, headers=headers)
                else:
                    return None

                if response.status_code in (200, 201, 202):
                    return response.json() if response.content else {}

                if response.status_code == 401:
                    self.access_token = None
                    self.token_expires_at = None
                    if attempt < (max_attempts - 1):
                        continue
                    print(f"API Error: 401 unauthorized after retry - {endpoint}")
                    return None

                print(f"API Error: {response.status_code} - {response.text}")
                return None

            except RecursionError:
                print(f"Request error: recursion detected for {endpoint}; switching to urllib fallback")
                fallback_result = self._request_via_urllib(method, endpoint, params=params, data=data, headers=headers)
                if isinstance(fallback_result, dict) and fallback_result.get('__unauthorized__'):
                    self.access_token = None
                    self.token_expires_at = None
                    if attempt < (max_attempts - 1):
                        continue
                    return None
                return fallback_result
            except Exception as e:
                print(f"Request error: {e}; trying urllib fallback")
                fallback_result = self._request_via_urllib(method, endpoint, params=params, data=data, headers=headers)
                if isinstance(fallback_result, dict) and fallback_result.get('__unauthorized__'):
                    self.access_token = None
                    self.token_expires_at = None
                    if attempt < (max_attempts - 1):
                        continue
                    return None
                return fallback_result

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
            "client_id": "your_ifood_client_id_here",
            "client_secret": "your_ifood_client_secret_here",
            "data_fetch_days": 30,
            "refresh_interval_minutes": 30,
            "merchants": []
        }
        IFoodConfig.save_config(sample_config, config_path)


if __name__ == "__main__":
    print("iFood API Module - FIXED VERSION")
    print("=" * 60)
    print("\nThis version properly handles mock data structure")
    print("âœ“ get_merchant_details() returns only merchant details")
    print("âœ“ get_orders() returns orders from cached data")
    print("âœ“ Consistent data across both calls")
