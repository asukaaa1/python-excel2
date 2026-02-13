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
    POLLING_ENDPOINTS = (
        "/events/v1.0/events:polling",
        "/order/v1.0/events:polling",  # legacy compatibility
    )
    ACK_ENDPOINTS = (
        "/events/v1.0/events/acknowledgment",
        "/order/v1.0/events/acknowledgment",  # legacy compatibility
    )
    
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
        self._last_http_error = None
        self._order_list_fallback_supported = True
        self._order_list_fallback_disabled_logged = False
        self._mock_orders_per_restaurant = max(
            1,
            int(str(os.environ.get('IFOOD_MOCK_ORDERS_PER_RESTAURANT', '150')).strip() or '150')
        )
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
        self._interruptions_cache = {}
        self._merchant_orders_cache = {}
        
        if self.use_mock_data:
            print("ðŸŽ­ Running in MOCK DATA mode - using sample data for testing")

    def _should_suppress_http_error_log(self, endpoint: str, status_code: int) -> bool:
        endpoint_text = str(endpoint or '').strip()
        try:
            status = int(status_code or 0)
        except Exception:
            status = 0
        # Optional fallback endpoint: unsupported scope is expected for many tenants.
        return endpoint_text == '/order/v1.0/orders' and status in (404, 405)

    def _ensure_mock_merchant(self, merchant_id: str):
        """Create deterministic mock merchant payload when absent."""
        key = str(merchant_id or '').strip()
        if not key:
            return None
        if key not in self._mock_merchants:
            self._mock_merchants[key] = MockIFoodDataGenerator.generate_merchant_data(
                merchant_id=key,
                num_orders=self._mock_orders_per_restaurant,
                days=30
            )
        return self._mock_merchants.get(key)

    def _order_cache_key(self, order: Dict) -> str:
        if not isinstance(order, dict):
            return ''
        return str(
            order.get('id')
            or order.get('orderId')
            or order.get('displayId')
            or f"{order.get('createdAt')}:{order.get('orderStatus')}"
        )

    def _merge_orders_into_local_cache(self, merchant_id: str, orders: List[Dict]) -> List[Dict]:
        merchant_key = str(merchant_id or '').strip()
        if not merchant_key:
            return []

        merged = {}
        for existing in (self._merchant_orders_cache.get(merchant_key) or []):
            if not isinstance(existing, dict):
                continue
            normalized_existing = self._normalize_order_payload(existing)
            existing_key = self._order_cache_key(normalized_existing)
            if existing_key:
                merged[existing_key] = normalized_existing

        for order in (orders or []):
            if not isinstance(order, dict):
                continue
            normalized_order = self._normalize_order_payload(order)
            key = self._order_cache_key(normalized_order)
            if key:
                merged[key] = normalized_order

        merged_list = list(merged.values())
        self._merchant_orders_cache[merchant_key] = merged_list
        return merged_list
    
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
            full_data = self._ensure_mock_merchant(merchant_id)
            return (full_data or {}).get('details')
        
        # Real API call
        return self._request('GET', f'/merchant/v1.0/merchants/{merchant_id}')
    
    def get_orders(self, merchant_id: str, start_date: str = None,
                   end_date: str = None, status: str = None) -> List[Dict]:
        """Get orders (real or mock)
        
        FIXED: Returns the orders from cached mock data
        """
        if self.use_mock_data:
            full_data = self._ensure_mock_merchant(merchant_id) or {}
            orders = full_data.get('orders') or []
            return self._filter_orders(orders, merchant_id, start_date, end_date, status)
        
        # Real API flow:
        # 1) poll order events, 2) resolve order details by order id.
        polling_result = self._poll_events_payload(merchant_id)
        events = self._extract_polling_events(polling_result)
        latest_event_status_by_order = {}
        for event in events:
            order_id = self._extract_order_id_from_event(event)
            if not order_id:
                continue
            event_status = self._extract_order_status_from_event(event)
            normalized_event_status = self._normalize_order_status(event_status)
            if normalized_event_status == 'UNKNOWN':
                continue
            event_created_at = self._parse_order_datetime(
                event.get('createdAt') if isinstance(event, dict) else None
            )
            existing = latest_event_status_by_order.get(str(order_id))
            if not existing:
                latest_event_status_by_order[str(order_id)] = {
                    'status': normalized_event_status,
                    'created_at': event_created_at
                }
                continue
            existing_created = existing.get('created_at')
            if existing_created is None or (event_created_at and event_created_at >= existing_created):
                latest_event_status_by_order[str(order_id)] = {
                    'status': normalized_event_status,
                    'created_at': event_created_at
                }

        # Some providers return order-like payloads directly; keep them too.
        direct_orders = []
        for item in events:
            if not isinstance(item, dict):
                continue
            direct_item = dict(item)
            direct_order_id = self._extract_order_id_from_event(direct_item)
            direct_status = self._extract_order_status_from_event(direct_item)
            has_order_payload = (
                ('orderStatus' in direct_item)
                or ('totalPrice' in direct_item)
                or ('total' in direct_item)
                or bool(direct_status)
            )
            if not has_order_payload or not direct_order_id:
                continue
            # Polling event id is usually the event id, not the order id.
            direct_item['id'] = str(direct_order_id)
            if direct_status and not direct_item.get('orderStatus'):
                direct_item['orderStatus'] = direct_status
            if not direct_item.get('merchantId'):
                direct_item['merchantId'] = str(merchant_id)
            direct_orders.append(self._normalize_order_payload(direct_item))

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
                normalized_current_status = self._normalize_order_status(details.get('orderStatus'))
                if normalized_current_status == 'UNKNOWN':
                    event_info = latest_event_status_by_order.get(str(order_id))
                    if event_info and event_info.get('status'):
                        details['orderStatus'] = event_info['status']
                resolved_orders.append(details)

        candidate_orders = resolved_orders + direct_orders

        # Fallback attempt: some tenants expose list/search endpoint.
        if not candidate_orders and self._order_list_fallback_supported:
            fallback_params = {'merchantId': merchant_id}
            if start_date:
                fallback_params['createdAtStart'] = f"{start_date}T00:00:00Z"
            if end_date:
                fallback_params['createdAtEnd'] = f"{end_date}T23:59:59Z"
            if status:
                fallback_params['status'] = status
            fallback_result = self._request('GET', '/order/v1.0/orders', params=fallback_params)
            candidate_orders = self._extract_order_payload_list(fallback_result)
            last_error = self._last_http_error if isinstance(self._last_http_error, dict) else {}
            if (
                fallback_result is None
                and str(last_error.get('endpoint') or '') == '/order/v1.0/orders'
            ):
                status_code = int(last_error.get('status') or 0)
                if status_code in (404, 405):
                    # Avoid noisy repeated 404 logs for tenants that do not expose this route.
                    self._order_list_fallback_supported = False
                    if not self._order_list_fallback_disabled_logged:
                        print("iFood info: /order/v1.0/orders is not available for this credential scope; using events/details only")
                        self._order_list_fallback_disabled_logged = True

        normalized_candidates = [
            self._normalize_order_payload(order)
            for order in (candidate_orders or [])
            if isinstance(order, dict)
        ]
        if normalized_candidates:
            merged_cache = self._merge_orders_into_local_cache(merchant_id, normalized_candidates)
            filtered = self._filter_orders(merged_cache, merchant_id, start_date, end_date, status)
        else:
            cached_orders = self._merchant_orders_cache.get(str(merchant_id or '').strip()) or []
            filtered = self._filter_orders(cached_orders, merchant_id, start_date, end_date, status)

        if events:
            try:
                self.acknowledge_events(events)
            except Exception:
                pass

        return filtered

    def poll_events(self, merchant_id) -> List[Dict]:
        """Perform lightweight events polling for one merchant.

        Useful for keeping test stores connected/open in iFood sandbox.
        """
        if self.use_mock_data:
            return []
        polling_merchants = self._normalize_polling_merchants(merchant_id)
        if not polling_merchants:
            return []
        polling_result = self._poll_events_payload(polling_merchants)
        return self._extract_polling_events(polling_result)

    def _normalize_polling_merchants(self, merchant_id) -> str:
        if merchant_id is None:
            return ''
        if isinstance(merchant_id, (list, tuple, set)):
            normalized = [str(mid).strip() for mid in merchant_id if str(mid).strip()]
            return ','.join(list(dict.fromkeys(normalized)))
        return str(merchant_id).strip()

    def _poll_events_payload(self, merchant_id):
        """Fetch polling payload using official endpoint, then legacy fallback."""
        polling_merchants = self._normalize_polling_merchants(merchant_id)
        if not polling_merchants:
            return None
        polling_headers = {'x-polling-merchants': polling_merchants}
        for endpoint in self.POLLING_ENDPOINTS:
            payload = self._request('GET', endpoint, headers=polling_headers)
            if payload is not None:
                return payload
        return None

    def _extract_event_id(self, event: Dict) -> Optional[str]:
        if not isinstance(event, dict):
            return None
        for key in ('id', 'eventId', 'event_id'):
            value = event.get(key)
            if value:
                return str(value)
        metadata = event.get('metadata')
        if isinstance(metadata, dict):
            for key in ('id', 'eventId', 'event_id'):
                value = metadata.get(key)
                if value:
                    return str(value)
        return None

    def acknowledge_events(self, events: List[Dict]) -> Dict:
        """Acknowledge polled events after processing to avoid backlog/re-delivery loops."""
        if self.use_mock_data:
            return {'success': True, 'requested': 0, 'acknowledged': 0}

        ack_items = []
        seen_ids = set()
        for event in (events or []):
            event_id = self._extract_event_id(event)
            if not event_id or event_id in seen_ids:
                continue
            seen_ids.add(event_id)
            ack_items.append({'id': event_id})

        requested = len(ack_items)
        if requested == 0:
            return {'success': True, 'requested': 0, 'acknowledged': 0}

        payload_variants = (
            ack_items,
            {'events': ack_items},
            [item['id'] for item in ack_items],
        )
        for endpoint in self.ACK_ENDPOINTS:
            for payload in payload_variants:
                result = self._request('POST', endpoint, data=payload)
                if result is not None:
                    return {'success': True, 'requested': requested, 'acknowledged': requested, 'endpoint': endpoint}

        return {'success': False, 'requested': requested, 'acknowledged': 0}

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

    def _extract_order_status_from_event(self, event: Dict):
        if not isinstance(event, dict):
            return None
        for key in ('orderStatus', 'status', 'state', 'fullCode', 'code'):
            value = event.get(key)
            if value:
                return value
        metadata = event.get('metadata')
        if isinstance(metadata, dict):
            for key in ('orderStatus', 'status', 'state', 'fullCode', 'code'):
                value = metadata.get(key)
                if value:
                    return value
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
        if status in ('CON', 'CONCLUDED', 'COMPLETED', 'DELIVERED', 'FINISHED'):
            return 'CONCLUDED'
        if status in ('CFM', 'CONFIRMED', 'PLACED', 'CREATED', 'PREPARING', 'READY', 'HANDOFF', 'IN_TRANSIT', 'DISPATCHED', 'PICKED_UP'):
            return 'CONFIRMED'
        return status

    def _safe_float_amount(self, value) -> float:
        try:
            return float(value)
        except Exception:
            return 0.0

    def _extract_order_amount(self, order: Dict) -> float:
        if not isinstance(order, dict):
            return 0.0

        direct_total = self._safe_float_amount(order.get('totalPrice'))
        if direct_total > 0:
            return direct_total

        total = order.get('total')
        if isinstance(total, dict):
            for key in ('orderAmount', 'totalPrice', 'amount'):
                amount = self._safe_float_amount(total.get(key))
                if amount > 0:
                    return amount
            sub_total = self._safe_float_amount(total.get('subTotal'))
            delivery_fee = self._safe_float_amount(total.get('deliveryFee'))
            combined = sub_total + delivery_fee
            if combined > 0:
                return combined

        for key in ('orderAmount', 'amount', 'totalAmount', 'value'):
            amount = self._safe_float_amount(order.get(key))
            if amount > 0:
                return amount

        payment = order.get('payment')
        if isinstance(payment, dict):
            for key in ('amount', 'value', 'total', 'paidAmount'):
                amount = self._safe_float_amount(payment.get(key))
                if amount > 0:
                    return amount

        payments = order.get('payments')
        if isinstance(payments, list):
            paid_total = 0.0
            for p in payments:
                if not isinstance(p, dict):
                    continue
                value = 0.0
                for key in ('amount', 'value', 'total', 'paidAmount'):
                    value = self._safe_float_amount(p.get(key))
                    if value > 0:
                        break
                paid_total += value
            if paid_total > 0:
                return paid_total

        items = order.get('items')
        if isinstance(items, list) and items:
            items_total = 0.0
            for item in items:
                if not isinstance(item, dict):
                    continue
                item_total = self._safe_float_amount(item.get('totalPrice'))
                if item_total <= 0:
                    qty = self._safe_float_amount(item.get('quantity') or 1)
                    unit = self._safe_float_amount(item.get('unitPrice'))
                    item_total = qty * unit if qty > 0 and unit > 0 else 0.0
                items_total += item_total
            if items_total > 0:
                return items_total

        return 0.0

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
            amount = self._extract_order_amount(order)
            if amount > 0:
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
            key = str(merchant_id or '').strip()
            current = self._interruptions_cache.get(key, [])
            now = datetime.now()
            still_active = []
            for interruption in current:
                if not isinstance(interruption, dict):
                    continue
                end_raw = interruption.get('end')
                if end_raw:
                    try:
                        end_dt = datetime.fromisoformat(str(end_raw).replace('Z', '+00:00'))
                        if end_dt.tzinfo is not None:
                            end_dt = end_dt.replace(tzinfo=None)
                        if end_dt < now:
                            continue
                    except Exception:
                        pass
                still_active.append(interruption)
            self._interruptions_cache[key] = still_active
            return list(still_active)
        
        # Real API call
        return self._request('GET', f'/merchant/v1.0/merchants/{merchant_id}/interruptions')
    
    def create_interruption(self, merchant_id: str, start: str, end: str, description: str = "") -> Optional[Dict]:
        """Create a temporary store closure"""
        if self.use_mock_data:
            key = str(merchant_id or '').strip()
            interruption = {
                "id": f"mock-interruption-{key}-{int(time.time())}",
                "description": description,
                "start": start,
                "end": end
            }
            self._interruptions_cache.setdefault(key, []).append(interruption)
            return interruption
        
        payload = {
            "start": start,
            "end": end,
            "description": description
        }
        return self._request('POST', f'/merchant/v1.0/merchants/{merchant_id}/interruptions', data=payload)
    
    def delete_interruption(self, merchant_id: str, interruption_id: str) -> bool:
        """Remove an interruption/reopen the store"""
        if self.use_mock_data:
            key = str(merchant_id or '').strip()
            existing = self._interruptions_cache.get(key, [])
            kept = [i for i in existing if str(i.get('id')) != str(interruption_id)]
            self._interruptions_cache[key] = kept
            return len(kept) < len(existing)
        
        result = self._request('DELETE', f'/merchant/v1.0/merchants/{merchant_id}/interruptions/{interruption_id}')
        return result is not None
    
    def get_merchant_status(self, merchant_id: str) -> Optional[Dict]:
        """Get merchant operational status"""
        if self.use_mock_data:
            interruptions = self.get_interruptions(merchant_id)
            now = datetime.now()
            has_active_interruption = False
            for interruption in interruptions:
                if not isinstance(interruption, dict):
                    continue
                start_raw = interruption.get('start')
                end_raw = interruption.get('end')
                try:
                    start_dt = datetime.fromisoformat(str(start_raw).replace('Z', '+00:00')) if start_raw else None
                    end_dt = datetime.fromisoformat(str(end_raw).replace('Z', '+00:00')) if end_raw else None
                    if start_dt and start_dt.tzinfo is not None:
                        start_dt = start_dt.replace(tzinfo=None)
                    if end_dt and end_dt.tzinfo is not None:
                        end_dt = end_dt.replace(tzinfo=None)
                    is_active = False
                    if start_dt and end_dt:
                        is_active = start_dt <= now <= end_dt
                    elif start_dt and not end_dt:
                        is_active = start_dt <= now
                    elif end_dt and not start_dt:
                        is_active = now <= end_dt
                    if is_active:
                        has_active_interruption = True
                        break
                except Exception:
                    continue

            if has_active_interruption:
                return {
                    "state": "CLOSED",
                    "message": "Loja temporariamente fechada",
                    "validations": [
                        {"code": "is-connected", "status": "OK"},
                        {"code": "opening-hours", "status": "CLOSED"}
                    ],
                    "reopenable": {"reopenable": True}
                }
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
                if status not in (200, 201, 202, 204):
                    self._last_http_error = {'status': status, 'endpoint': endpoint, 'detail': ''}
                    if not self._should_suppress_http_error_log(endpoint, status):
                        print(f"API Error (urllib): {status} - {endpoint}")
                    return None
                self._last_http_error = None
                if not raw:
                    return {}
                text = raw.decode('utf-8', errors='replace').strip()
                return json.loads(text) if text else {}
        except HTTPError as http_err:
            status_code = int(getattr(http_err, 'code', 0) or 0)
            self._last_http_error = {'status': status_code, 'endpoint': endpoint, 'detail': ''}
            if status_code == 401:
                # Signal caller to trigger one token refresh attempt.
                return {'__unauthorized__': True}
            detail = ''
            try:
                detail = http_err.read().decode('utf-8', errors='replace')
            except Exception:
                pass
            self._last_http_error = {'status': status_code, 'endpoint': endpoint, 'detail': detail or ''}
            if not self._should_suppress_http_error_log(endpoint, status_code):
                print(f"API Error (urllib): {getattr(http_err, 'code', '?')} - {detail}")
            return None
        except Exception as err:
            self._last_http_error = {'status': 0, 'endpoint': endpoint, 'detail': str(err)}
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

                if response.status_code in (200, 201, 202, 204):
                    self._last_http_error = None
                    if not response.content:
                        return {}
                    try:
                        return response.json()
                    except Exception:
                        return {}

                if response.status_code == 401:
                    self._last_http_error = {'status': 401, 'endpoint': endpoint, 'detail': response.text}
                    self.access_token = None
                    self.token_expires_at = None
                    if attempt < (max_attempts - 1):
                        continue
                    print(f"API Error: 401 unauthorized after retry - {endpoint}")
                    return None

                self._last_http_error = {'status': int(response.status_code or 0), 'endpoint': endpoint, 'detail': response.text}
                if not self._should_suppress_http_error_log(endpoint, response.status_code):
                    print(f"API Error: {response.status_code} - {response.text}")
                return None

            except RecursionError:
                self._last_http_error = {'status': 0, 'endpoint': endpoint, 'detail': 'recursion_error'}
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
                self._last_http_error = {'status': 0, 'endpoint': endpoint, 'detail': str(e)}
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
