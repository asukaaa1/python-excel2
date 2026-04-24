"""Restaurant data and interruption route registrations."""

from flask import Blueprint
from app_services import restaurants_service
from app_routes.dependencies import bind_dependencies
from ifood_homologation_evidence import build_ifood_order_evidence


REQUIRED_DEPS = [
    'IFOOD_API',
    'IFoodDataProcessor',
    'LAST_DATA_REFRESH',
    'ORG_DATA',
    '_extract_status_message_text',
    'admin_required',
    'copy',
    'datetime',
    'db',
    'detect_restaurant_closure',
    'ensure_restaurant_financial_sales_cache',
    'ensure_restaurant_orders_cache',
    'evaluate_restaurant_quality',
    'filter_orders_by_month',
    'find_restaurant_by_identifier',
    'get_cached_restaurants',
    'get_current_org_id',
    'get_current_org_restaurants',
    'get_json_payload',
    'get_order_status',
    'get_resilient_api_client',
    'get_user_allowed_restaurant_ids',
    'internal_error_response',
    'invalidate_cache',
    'jsonify',
    'log_exception',
    'login_required',
    'month_filter_label',
    'normalize_order_payload',
    'normalize_order_status_value',
    'parse_month_filter',
    'request',
    'session',
    'set_cached_restaurants',
]

def register(app, deps):
    bind_dependencies(globals(), deps, REQUIRED_DEPS)
    bp = Blueprint('restaurants_routes', __name__)

    def _is_truthy(value):
        return str(value or '').strip().lower() in ('1', 'true', 'yes', 'on', 'sim')

    def _masked_value(value, *, keep=4):
        text = str(value or '').strip()
        if not text:
            return None
        keep_n = max(1, int(keep or 1))
        if len(text) <= keep_n:
            return '*' * len(text)
        return f"{text[:keep_n]}{'*' * max(3, len(text) - keep_n)}"

    def _parse_ifood_error_status(api, default_status=500):
        err = {}
        getter = getattr(api, 'get_last_http_error', None)
        if callable(getter):
            try:
                err = getter() or {}
            except Exception:
                err = {}
        elif isinstance(getattr(api, '_last_http_error', None), dict):
            err = dict(getattr(api, '_last_http_error', None) or {})
        try:
            status_code = int(err.get('status') or 0)
        except Exception:
            status_code = 0
        if status_code <= 0:
            return default_status
        return status_code

    def _ifood_error_response(api, *, action='operacao iFood', default_status=500):
        raw_error = {}
        getter = getattr(api, 'get_last_http_error', None)
        if callable(getter):
            try:
                raw_error = getter() or {}
            except Exception:
                raw_error = {}
        elif isinstance(getattr(api, '_last_http_error', None), dict):
            raw_error = dict(getattr(api, '_last_http_error') or {})

        status_code = _parse_ifood_error_status(api, default_status=default_status)
        detail_text = str((raw_error or {}).get('detail') or '').strip()
        endpoint_text = str((raw_error or {}).get('endpoint') or '').strip()

        def _error_payload(message):
            payload = {'success': False, 'error': message}
            if status_code:
                payload['ifood_status'] = status_code
            if endpoint_text:
                payload['ifood_endpoint'] = endpoint_text
            if detail_text:
                payload['ifood_detail'] = detail_text[:800]
            return payload

        if status_code == 400:
            return jsonify(_error_payload(
                f'Falha de validacao ao executar {action}. Revise campos obrigatorios e formato dos horarios.'
            )), 400
        if status_code == 401:
            return jsonify(_error_payload(
                'Nao autorizado no iFood. Revise as credenciais da organizacao.'
            )), 401
        if status_code == 403:
            payload = _error_payload(
                'Permissao insuficiente no iFood para a loja informada.'
            )
            payload['permission_gap'] = True
            payload['homologation_impact'] = (
                'A chamada chegou ao iFood, mas o token/app nao tem permissao para esse recurso.'
            )
            if 'virtual-bag' in endpoint_text:
                payload['homologation_note'] = (
                    'GET /orders/{orderId}/virtual-bag foi negado pelo iFood para este app/loja. '
                    'Esse endpoint e usado para pedidos GROCERY; para FOOD, a documentacao usa GET /orders/{orderId}. '
                    'Para evidencia de itens do pedido, use GET /orders/{orderId} e o painel Order Evidence.'
                )
                payload['recommended_action'] = (
                    'Se o avaliador exigir virtual-bag, solicite ao iFood a liberacao desse recurso no escopo do app.'
                )
            return jsonify(payload), 403
        if status_code == 409:
            return jsonify(_error_payload(
                'Conflito detectado (ex.: sobreposicao de interrupcao/horario). Ajuste a janela e tente novamente.'
            )), 409
        if status_code == 429:
            return jsonify(_error_payload(
                'Limite de requisicoes iFood atingido. Tente novamente em instantes.'
            )), 429
        if 500 <= status_code <= 599:
            return jsonify(_error_payload(
                'iFood indisponivel no momento. Tente novamente com backoff.'
            )), 502
        return jsonify(_error_payload(f'Falha ao executar {action}.')), default_status

    def _request_text_arg(*names):
        for name in names:
            value = str(request.args.get(name) or '').strip()
            if value:
                return value
        return None

    def _payload_text_value(payload, *names):
        if not isinstance(payload, dict):
            return None
        for name in names:
            value = str(payload.get(name) or '').strip()
            if value:
                return value
        return None

    def _count_financial_items(payload, nested_key=None):
        if isinstance(payload, list):
            if not nested_key:
                return len(payload)
            nested_total = 0
            for item in payload:
                if not isinstance(item, dict):
                    continue
                nested_value = item.get(nested_key)
                if isinstance(nested_value, list):
                    nested_total += len(nested_value)
            return nested_total or len(payload)
        if isinstance(payload, dict):
            if nested_key:
                nested_value = payload.get(nested_key)
                if isinstance(nested_value, list):
                    return len(nested_value)
            return 1 if payload else 0
        return 0

    HOMOLOGATION_FINANCIAL_HEADERS = {'x-request-homologation': 'true'}
    HOMOLOGATION_ORDER_HEADERS = {'x-request-homologation': 'true'}

    def _snapshot_order_summary(snapshot):
        if not isinstance(snapshot, dict):
            return {}
        payload = snapshot.get('payload') if isinstance(snapshot.get('payload'), dict) else {}
        return {
            'order_id': snapshot.get('order_id') or payload.get('id') or payload.get('orderId'),
            'merchant_id': snapshot.get('merchant_id') or payload.get('merchantId'),
            'display_id': payload.get('displayId') or payload.get('display_id'),
            'status': snapshot.get('status') or payload.get('status') or payload.get('orderStatus'),
            'source': snapshot.get('source'),
            'order_updated_at': snapshot.get('order_updated_at'),
            'snapshot_updated_at': snapshot.get('updated_at'),
        }

    def _order_id_from_payload(payload, fallback=None):
        if not isinstance(payload, dict):
            return fallback
        return (
            payload.get('id')
            or payload.get('orderId')
            or payload.get('order_id')
            or fallback
        )

    def _merchant_id_from_order_payload(payload, fallback=None):
        if not isinstance(payload, dict):
            return fallback
        return (
            payload.get('merchantId')
            or payload.get('merchant_id')
            or payload.get('merchant', {}).get('id')
            or fallback
        )

    def _status_from_order_payload(payload):
        if not isinstance(payload, dict):
            return None
        return payload.get('status') or payload.get('orderStatus')

    def _persist_homologation_order_snapshot(order_payload, *, source='homologation'):
        if not isinstance(order_payload, dict):
            return False
        org_id = get_current_org_id()
        order_id = _order_id_from_payload(order_payload)
        merchant_id = _merchant_id_from_order_payload(order_payload)
        if not org_id or not order_id or not merchant_id:
            return False
        try:
            return bool(db.upsert_ifood_order_snapshot(
                org_id,
                merchant_id,
                order_id,
                order_payload,
                source=source,
                status=_status_from_order_payload(order_payload),
            ))
        except Exception:
            return False

    def _build_order_evidence_for_current_org(order_id, *, allow_live_fetch=True):
        org_id = get_current_org_id()
        snapshot = db.get_ifood_order_snapshot(org_id, order_id) if org_id else None
        events = db.list_ifood_order_events(org_id=org_id, order_id=order_id, limit=80) if org_id else []
        order_payload = snapshot.get('payload') if isinstance(snapshot, dict) else None
        live_error = None

        if allow_live_fetch:
            api = get_resilient_api_client()
            if api:
                details = api.get_order_details(order_id, headers=HOMOLOGATION_ORDER_HEADERS)
                if isinstance(details, dict) and details:
                    order_payload = details
                    _persist_homologation_order_snapshot(details, source='homologation_details')
                    merchant_id = _merchant_id_from_order_payload(details)
                    if merchant_id and not events:
                        events = db.list_ifood_order_events(
                            org_id=org_id,
                            order_id=order_id,
                            merchant_id=merchant_id,
                            limit=80
                        )
                elif details is None:
                    live_error = getattr(api, 'get_last_http_error', lambda: None)()

        if not isinstance(order_payload, dict) or not order_payload:
            return None, live_error

        evidence = build_ifood_order_evidence(
            order_payload,
            snapshot=snapshot or {},
            events=events,
        )
        evidence['raw_available'] = {
            'order_details': bool(order_payload),
            'snapshot': bool(snapshot),
            'events': bool(events),
        }
        return evidence, live_error

    def _parse_reopenable_flag(status_payload):
        if not isinstance(status_payload, dict):
            return None
        reopenable = status_payload.get('reopenable')
        candidate = reopenable
        if isinstance(reopenable, dict):
            candidate = (
                reopenable.get('reopenable')
                if reopenable.get('reopenable') is not None
                else reopenable.get('isReopenable')
                if reopenable.get('isReopenable') is not None
                else reopenable.get('canReopen')
            )
        if isinstance(candidate, bool):
            return candidate
        text = str(candidate or '').strip().lower()
        if text in ('true', '1', 'yes', 'sim'):
            return True
        if text in ('false', '0', 'no', 'nao', 'não'):
            return False
        return None

    def _parse_iso_datetime(raw_value):
        value = str(raw_value or '').strip()
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except Exception:
            return None

    def _parse_optional_int(raw_value, *, minimum=0, maximum=1000, default=None):
        text = str(raw_value or '').strip()
        if not text:
            return default
        try:
            value = int(text)
        except Exception:
            return default
        return max(minimum, min(maximum, value))

    def _get_homologation_financial_filters(payload=None):
        data = payload if isinstance(payload, dict) else {}
        merchant_id = str(
            request.args.get('merchant_id')
            or request.args.get('merchantId')
            or data.get('merchant_id')
            or data.get('merchantId')
            or ''
        ).strip()
        if not merchant_id:
            return None, (jsonify({'success': False, 'error': 'Merchant ID required'}), 400)

        filters = {
            'merchant_id': merchant_id,
            'start_date': str(
                request.args.get('start_date')
                or request.args.get('startDate')
                or data.get('start_date')
                or data.get('startDate')
                or ''
            ).strip() or None,
            'end_date': str(
                request.args.get('end_date')
                or request.args.get('endDate')
                or data.get('end_date')
                or data.get('endDate')
                or ''
            ).strip() or None,
            'competence': str(
                request.args.get('competence')
                or request.args.get('competencia')
                or data.get('competence')
                or data.get('competencia')
                or ''
            ).strip() or None,
            'page': _parse_optional_int(
                request.args.get('page')
                if request.args.get('page') is not None
                else data.get('page'),
                minimum=0,
                maximum=10000,
                default=None
            ),
            'size': _parse_optional_int(
                request.args.get('size')
                if request.args.get('size') is not None
                else data.get('size'),
                minimum=1,
                maximum=500,
                default=None
            ),
        }
        return filters, None

    def _get_homologation_review_filters(payload=None):
        data = payload if isinstance(payload, dict) else {}
        merchant_id = str(
            request.args.get('merchant_id')
            or request.args.get('merchantId')
            or data.get('merchant_id')
            or data.get('merchantId')
            or ''
        ).strip()
        if not merchant_id:
            return None, (jsonify({'success': False, 'error': 'Merchant ID required'}), 400)

        filters = {
            'merchant_id': merchant_id,
            'page': _parse_optional_int(
                request.args.get('page')
                if request.args.get('page') is not None
                else data.get('page'),
                minimum=1,
                maximum=10000,
                default=1
            ),
            'page_size': _parse_optional_int(
                request.args.get('page_size')
                if request.args.get('page_size') is not None
                else request.args.get('pageSize')
                if request.args.get('pageSize') is not None
                else data.get('page_size')
                if data.get('page_size') is not None
                else data.get('pageSize'),
                minimum=1,
                maximum=100,
                default=10
            ),
            'add_count': _is_truthy(
                request.args.get('add_count')
                if request.args.get('add_count') is not None
                else request.args.get('addCount')
                if request.args.get('addCount') is not None
                else data.get('add_count')
                if data.get('add_count') is not None
                else data.get('addCount')
                if data.get('addCount') is not None
                else True
            ),
            'date_from': str(
                request.args.get('date_from')
                or request.args.get('dateFrom')
                or data.get('date_from')
                or data.get('dateFrom')
                or ''
            ).strip() or None,
            'date_to': str(
                request.args.get('date_to')
                or request.args.get('dateTo')
                or data.get('date_to')
                or data.get('dateTo')
                or ''
            ).strip() or None,
            'status': str(
                request.args.get('status')
                or data.get('status')
                or ''
            ).strip() or None,
        }
        return filters, None

    def _normalize_day_of_week(day_raw):
        if isinstance(day_raw, int):
            if 0 <= day_raw <= 6:
                return day_raw
            if 1 <= day_raw <= 7:
                return day_raw - 1
            return None
        text = str(day_raw or '').strip().lower()
        mapping = {
            '0': 0, '1': 0, 'mon': 0, 'monday': 0, 'segunda': 0, 'segunda-feira': 0,
            '2': 1, 'tue': 1, 'tuesday': 1, 'terca': 1, 'terça': 1, 'terca-feira': 1, 'terça-feira': 1,
            '3': 2, 'wed': 2, 'wednesday': 2, 'quarta': 2, 'quarta-feira': 2,
            '4': 3, 'thu': 3, 'thursday': 3, 'quinta': 3, 'quinta-feira': 3,
            '5': 4, 'fri': 4, 'friday': 4, 'sexta': 4, 'sexta-feira': 4,
            '6': 5, 'sat': 5, 'saturday': 5, 'sabado': 5, 'sábado': 5,
            '7': 6, 'sun': 6, 'sunday': 6, 'domingo': 6,
        }
        return mapping.get(text)

    def _hhmmss_to_seconds(text):
        value = str(text or '').strip()
        parts = value.split(':')
        if len(parts) != 3:
            return None
        try:
            hours, minutes, seconds = [int(p) for p in parts]
        except Exception:
            return None
        if not (0 <= hours <= 23 and 0 <= minutes <= 59 and 0 <= seconds <= 59):
            return None
        return hours * 3600 + minutes * 60 + seconds

    def _normalize_opening_hours_entries(raw_entries):
        if not isinstance(raw_entries, list):
            return None, 'opening_hours deve ser uma lista'
        normalized = []
        day_presence = set()
        by_day = {d: [] for d in range(7)}
        for entry in raw_entries:
            if not isinstance(entry, dict):
                return None, 'Cada item de opening_hours deve ser objeto'
            day_raw = (
                entry.get('dayOfWeek')
                if entry.get('dayOfWeek') is not None
                else entry.get('day_of_week')
                if entry.get('day_of_week') is not None
                else entry.get('day')
            )
            day = _normalize_day_of_week(day_raw)
            if day is None:
                return None, 'dayOfWeek invalido; use 0-6 (ou 1-7) e inclua todos os dias'
            day_presence.add(day)
            start = entry.get('start') or entry.get('startTime') or entry.get('from')
            end = entry.get('end') or entry.get('endTime') or entry.get('to')
            start_seconds = _hhmmss_to_seconds(start)
            end_seconds = _hhmmss_to_seconds(end)
            if start_seconds is None or end_seconds is None:
                return None, 'Horario invalido; use formato HH:MM:SS'
            if end_seconds <= start_seconds:
                return None, 'Horario invalido; fim deve ser maior que inicio no mesmo dia'
            duration_minutes = int((end_seconds - start_seconds) / 60)
            if duration_minutes <= 0:
                return None, 'Duracao do turno deve ser maior que zero'
            normalized_item = {
                'dayOfWeek': int(day),
                'start': str(start).strip(),
                'end': str(end).strip(),
            }
            if entry.get('description'):
                normalized_item['description'] = str(entry.get('description')).strip()
            normalized.append(normalized_item)
            by_day[day].append((start_seconds, end_seconds))

        if day_presence != set(range(7)):
            return None, 'Envie configuracao para todos os dias da semana (0-6)'

        for day in range(7):
            slots = sorted(by_day.get(day) or [])
            for idx in range(1, len(slots)):
                prev_start, prev_end = slots[idx - 1]
                cur_start, _ = slots[idx]
                if cur_start < prev_end:
                    return None, 'Turnos sobrepostos detectados no mesmo dia'

        return normalized, None

    @bp.route('/api/restaurants')
    @login_required
    def api_restaurants():
        """Get list of all restaurants with optional month filtering and squad-based access control"""
        try:
            # Get month filter from query parameters
            month_filter = parse_month_filter(request.args.get('month', 'all'))
            if month_filter is None:
                return jsonify({'success': False, 'error': 'Invalid month filter'}), 400
        
            # Check in-memory cache first (avoids re-processing orders every request)
            org_id = get_current_org_id()
            org_last_refresh = ORG_DATA.get(org_id, {}).get('last_refresh') if org_id else LAST_DATA_REFRESH
            org_last_refresh_iso = org_last_refresh.isoformat() if isinstance(org_last_refresh, datetime) else None
            cached = get_cached_restaurants(
                org_id,
                month_filter,
                expected_last_refresh_iso=org_last_refresh_iso,
            )
            if cached:
                cached_restaurants = cached.get('restaurants') if isinstance(cached, dict) else None
                if restaurants_service.cache_has_closure_payload(cached_restaurants):
                    return jsonify(cached)
                # Drop stale cache entries that predate closure indicators.
                invalidate_cache(org_id)
        
            # Get user's allowed restaurants based on squad membership
            user = session.get('user', {})
            allowed_ids = get_user_allowed_restaurant_ids(user.get('id'), user.get('role'))
            org_api = ORG_DATA.get(org_id, {}).get('api') if org_id else IFOOD_API
        
            # Return data without internal caches
            restaurants = []
            for r in get_current_org_restaurants():
                restaurant_id_value = (
                    r.get('id')
                    or r.get('merchant_id')
                    or r.get('merchantId')
                )
                # Skip if user doesn't have access to this restaurant (squad filtering)
                if allowed_ids is not None and (not restaurant_id_value or restaurant_id_value not in allowed_ids):
                    continue
                merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(r)

                existing_orders_cache = [
                    o for o in (r.get('_orders_cache') or [])
                    if isinstance(o, dict)
                ]
                orders_have_identifiable_ids = any(
                    str(o.get('id') or o.get('orderId') or o.get('order_id') or '').strip()
                    for o in existing_orders_cache
                )
                metrics_snapshot = r.get('metrics') if isinstance(r.get('metrics'), dict) else {}
                try:
                    snapshot_orders_total = int(
                        (metrics_snapshot or {}).get('total_pedidos')
                        or r.get('orders')
                        or (metrics_snapshot or {}).get('vendas')
                        or 0
                    )
                except Exception:
                    snapshot_orders_total = 0
                try:
                    snapshot_revenue_total = float(
                        (metrics_snapshot or {}).get('liquido')
                        or (metrics_snapshot or {}).get('valor_bruto')
                        or r.get('revenue')
                        or 0
                    )
                except Exception:
                    snapshot_revenue_total = 0.0

                # Keep list payload aligned with detail endpoint:
                # hydrate when cache is missing/sparse or metrics look stale-zero.
                needs_hydration = (
                    (not existing_orders_cache)
                    or (not orders_have_identifiable_ids)
                    or (snapshot_orders_total <= 0 and snapshot_revenue_total <= 0)
                    # Keepalive polling can temporarily cache sparse event-derived orders
                    # (count present, monetary fields missing). Rehydrate/recompute in list
                    # responses so dashboard cards do not get stuck at zero revenue.
                    or (snapshot_orders_total > 0 and snapshot_revenue_total <= 0)
                )
                if needs_hydration:
                    hydrated_orders = ensure_restaurant_orders_cache(r, merchant_lookup_id)
                    if hydrated_orders:
                        resolved_lookup_id = (
                            r.get('_resolved_merchant_id')
                            or r.get('merchant_id')
                            or r.get('merchantId')
                            or merchant_lookup_id
                        )
                        try:
                            merchant_details = {
                                'id': resolved_lookup_id,
                                'name': r.get('name', 'Unknown Restaurant'),
                                'merchantManager': {'name': r.get('manager', 'Gerente')},
                                'address': {'neighborhood': r.get('neighborhood', 'Centro')},
                                'isSuperRestaurant': restaurants_service.get_super_flag(r),
                            }
                            refreshed = IFoodDataProcessor.process_restaurant_data(
                                merchant_details,
                                hydrated_orders,
                                r.get('_financial_sales_cache')
                            )
                            refreshed['name'] = r.get('name', refreshed.get('name'))
                            refreshed['manager'] = r.get('manager', refreshed.get('manager'))
                            refreshed['merchant_id'] = resolved_lookup_id
                            for key, value in (refreshed or {}).items():
                                if not str(key).startswith('_'):
                                    r[key] = value
                        except Exception:
                            pass
                merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(r, merchant_lookup_id)
                is_super = restaurants_service.get_super_flag(r)
                closure = restaurants_service.normalize_closure(
                    r,
                    api_client=org_api,
                    extract_status_message_text=_extract_status_message_text,
                    detect_restaurant_closure=detect_restaurant_closure,
                )
                # Persist normalized closure fields in-memory for subsequent requests.
                r['is_closed'] = bool(closure.get('is_closed'))
                r['closure_reason'] = closure.get('closure_reason')
                r['closed_until'] = closure.get('closed_until')
                r['active_interruptions_count'] = int(closure.get('active_interruptions_count') or 0)
                r['state'] = closure.get('state')
                r['status_message'] = closure.get('status_message')
                r['reopenable'] = closure.get('reopenable')
            
                # If month filter is specified, reprocess with filtered orders
                if month_filter != 0:
                    # Get cached orders
                    orders = ensure_restaurant_orders_cache(
                        r,
                        merchant_lookup_id
                    )
                
                    # Filter orders by month
                    filtered_orders = filter_orders_by_month(orders, month_filter)
                
                    # Reprocess restaurant data with filtered orders
                    if filtered_orders or month_filter != 0:
                        restaurant_name = r.get('name', 'Unknown Restaurant')
                        restaurant_manager = r.get('manager', 'Gerente')
                        # Get merchant details (reconstruct basic structure)
                        merchant_details = {
                            'id': merchant_lookup_id or restaurant_id_value,
                            'name': restaurant_name,
                            'merchantManager': {'name': restaurant_manager},
                            'address': {'neighborhood': r.get('neighborhood', 'Centro')},
                            'isSuperRestaurant': is_super,
                        }
                    
                        # Reprocess with filtered orders
                        restaurant_data = IFoodDataProcessor.process_restaurant_data(
                            merchant_details,
                            filtered_orders,
                            r.get('_financial_sales_cache')
                        )
                    
                        # Keep original name and manager
                        restaurant_data['name'] = restaurant_name
                        restaurant_data['manager'] = restaurant_manager
                        restaurant_data['isSuperRestaurant'] = is_super
                        restaurant_data['isSuper'] = is_super
                        restaurant_data['super'] = is_super
                        restaurant_data['is_closed'] = bool(closure.get('is_closed'))
                        restaurant_data['closure_reason'] = closure.get('closure_reason')
                        restaurant_data['closed_until'] = closure.get('closed_until')
                        restaurant_data['active_interruptions_count'] = int(closure.get('active_interruptions_count') or 0)
                        restaurant_data['state'] = closure.get('state')
                        restaurant_data['status_message'] = closure.get('status_message')
                        restaurant_data['reopenable'] = closure.get('reopenable')
                    
                        # Remove internal caches before sending
                        restaurant = {k: v for k, v in restaurant_data.items() if not k.startswith('_')}
                        restaurant['quality'] = evaluate_restaurant_quality(r, reference_last_refresh=org_last_refresh)
                        restaurants.append(restaurant)
                    else:
                        # No orders for this month, return empty metrics
                        restaurant = {k: v for k, v in r.items() if not k.startswith('_')}
                        restaurant['isSuperRestaurant'] = is_super
                        restaurant['isSuper'] = is_super
                        restaurant['super'] = is_super
                        if isinstance(restaurant.get('metrics'), dict):
                            # Avoid mutating the shared in-memory metrics dict.
                            restaurant['metrics'] = copy.deepcopy(restaurant.get('metrics') or {})
                        # Reset metrics to zero
                        if 'metrics' in restaurant:
                            restaurants_service.zero_numeric_metrics(restaurant['metrics'])
                        restaurant['revenue'] = 0
                        restaurant['orders'] = 0
                        restaurant['ticket'] = 0
                        restaurant['trend'] = 0
                        restaurant['quality'] = evaluate_restaurant_quality(r, reference_last_refresh=org_last_refresh)
                        restaurants.append(restaurant)
                else:
                    # No filter, return all data
                    orders_snapshot = [o for o in (r.get('_orders_cache') or []) if isinstance(o, dict)]
                    metrics_snapshot = r.get('metrics') if isinstance(r.get('metrics'), dict) else {}
                    try:
                        metrics_total_orders = int(
                            (metrics_snapshot or {}).get('total_pedidos')
                            or r.get('orders')
                            or (metrics_snapshot or {}).get('vendas')
                            or 0
                        )
                    except Exception:
                        metrics_total_orders = 0
                    try:
                        metrics_total_revenue = float(
                            (metrics_snapshot or {}).get('liquido')
                            or (metrics_snapshot or {}).get('valor_bruto')
                            or r.get('revenue')
                            or 0
                        )
                    except Exception:
                        metrics_total_revenue = 0.0
                    # Guard against stale metrics staying at zero while raw orders exist.
                    if orders_snapshot and (
                        metrics_total_orders <= 0
                        or (metrics_total_orders > 0 and metrics_total_revenue <= 0)
                    ):
                        try:
                            refreshed = IFoodDataProcessor.process_restaurant_data(
                                {
                                    'id': merchant_lookup_id or restaurant_id_value,
                                    'name': r.get('name', 'Restaurante'),
                                    'merchantManager': {'name': r.get('manager', 'Gerente')},
                                    'address': {'neighborhood': r.get('neighborhood', 'Centro')},
                                    'isSuperRestaurant': is_super,
                                },
                                orders_snapshot,
                                r.get('_financial_sales_cache')
                            )
                            for key, value in (refreshed or {}).items():
                                if not str(key).startswith('_'):
                                    r[key] = value
                        except Exception:
                            pass

                    restaurant = {k: v for k, v in r.items() if not k.startswith('_')}
                    restaurant['isSuperRestaurant'] = is_super
                    restaurant['isSuper'] = is_super
                    restaurant['super'] = is_super
                    restaurant['quality'] = evaluate_restaurant_quality(r, reference_last_refresh=org_last_refresh)
                    restaurants.append(restaurant)
        
            org_id = get_current_org_id()
            org_refresh = ORG_DATA.get(org_id, {}).get('last_refresh') if org_id else None
            quality_summary = restaurants_service.summarize_quality(restaurants)
        
            result = {
                'success': True,
                'restaurants': restaurants,
                'last_refresh': (org_refresh or LAST_DATA_REFRESH).isoformat() if (org_refresh or LAST_DATA_REFRESH) else None,
                'month_filter': month_filter_label(month_filter),
                'data_quality': quality_summary
            }
        
            # Cache the processed result
            set_cached_restaurants(org_id, month_filter, result)
        
            return jsonify(result)
        except Exception as e:
            print(f"Error getting restaurants: {e}")
            import traceback
            log_exception("request_exception", e)
            return internal_error_response()



    @bp.route('/api/restaurant/<restaurant_id>')
    @login_required
    def api_restaurant_detail(restaurant_id):
        """Get detailed data for a specific restaurant with optional date filtering"""
        try:
            # Get date filter parameters
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
        
            # Find restaurant in org data (supports alias IDs).
            restaurant = find_restaurant_by_identifier(restaurant_id)
        
            if not restaurant:
                return jsonify({'success': False, 'error': 'Restaurant not found'}), 404

            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant, restaurant_id)
        
            # Ensure orders cache is present even when loaded from DB snapshots.
            all_orders = ensure_restaurant_orders_cache(
                restaurant,
                merchant_lookup_id,
                force_remote_sync=True
            )
            financial_sales = ensure_restaurant_financial_sales_cache(
                restaurant,
                merchant_lookup_id,
                force_remote_sync=True,
            )
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant, merchant_lookup_id)
        
            # Filter orders by date range if provided
            filtered_orders = all_orders
            if start_date or end_date:
                filtered_orders = restaurants_service.filter_orders_by_date_range(
                    all_orders,
                    start_date,
                    end_date,
                    datetime_mod=datetime,
                    normalize_order_payload=normalize_order_payload,
                )
        
            metrics_snapshot = restaurant.get('metrics') if isinstance(restaurant.get('metrics'), dict) else {}
            try:
                snapshot_orders_total = int(
                    (metrics_snapshot or {}).get('total_pedidos')
                    or restaurant.get('orders')
                    or (metrics_snapshot or {}).get('vendas')
                    or 0
                )
            except Exception:
                snapshot_orders_total = 0
            try:
                snapshot_revenue_total = float(
                    (metrics_snapshot or {}).get('liquido')
                    or (metrics_snapshot or {}).get('valor_bruto')
                    or restaurant.get('revenue')
                    or 0
                )
            except Exception:
                snapshot_revenue_total = 0.0
            has_snapshot_totals = (snapshot_orders_total > 0) or (snapshot_revenue_total > 0)

            # Reprocess restaurant data with filtered orders if date filtering is applied.
            # If raw order cache is unavailable but snapshot metrics exist, keep snapshot metrics
            # instead of forcing a misleading all-zero payload.
            if start_date or end_date:
                if not filtered_orders and not all_orders and has_snapshot_totals:
                    response_data = {k: v for k, v in restaurant.items() if not k.startswith('_')}
                else:
                    # Get merchant details
                    merchant_details = {
                        'id': merchant_lookup_id,
                        'name': restaurant.get('name', 'Unknown'),
                        'merchantManager': {'name': restaurant.get('manager', 'Gerente')}
                    }
                
                    # Reprocess with filtered orders
                    response_data = IFoodDataProcessor.process_restaurant_data(
                        merchant_details,
                        filtered_orders,
                        financial_sales
                    )
                
                    # Keep original name and manager
                    response_data['name'] = restaurant['name']
                    response_data['manager'] = restaurant['manager']
            else:
                # No explicit filter: prefer recalculating from cached orders when available.
                if all_orders:
                    merchant_details = {
                        'id': merchant_lookup_id,
                        'name': restaurant.get('name', 'Unknown'),
                        'merchantManager': {'name': restaurant.get('manager', 'Gerente')}
                    }
                    response_data = IFoodDataProcessor.process_restaurant_data(
                        merchant_details,
                        all_orders,
                        financial_sales
                    )
                    response_data['name'] = restaurant.get('name', response_data.get('name'))
                    response_data['manager'] = restaurant.get('manager', response_data.get('manager'))
                    for closure_key in (
                        'is_closed',
                        'closure_reason',
                        'closed_until',
                        'active_interruptions_count',
                        'state',
                        'status_message',
                        'reopenable',
                    ):
                        if closure_key in restaurant:
                            response_data[closure_key] = restaurant.get(closure_key)
                else:
                    # Clean data for response (no date filtering)
                    response_data = {k: v for k, v in restaurant.items() if not k.startswith('_')}
        
            # Generate chart data from filtered orders
            chart_data = {}
            interruptions = []
        
            api = get_resilient_api_client()
            if api:
                # Get interruptions
                try:
                    interruptions = api.get_interruptions(merchant_lookup_id) or []
                except:
                    pass
        
            # Generate charts from filtered orders
            orders_for_charts = filtered_orders if (start_date or end_date) else all_orders
            top_n = request.args.get('top_n', default=10, type=int)
            top_n = max(1, min(top_n or 10, 50))
            menu_performance = IFoodDataProcessor.calculate_menu_item_performance(orders_for_charts, top_n=top_n)

            if orders_for_charts:
                if hasattr(IFoodDataProcessor, 'generate_charts_data_with_interruptions'):
                    chart_data = IFoodDataProcessor.generate_charts_data_with_interruptions(
                        orders_for_charts,
                        interruptions
                    )
                else:
                    chart_data = IFoodDataProcessor.generate_charts_data(orders_for_charts)
                    chart_data['interruptions'] = []
        
            # Extract reviews from orders
            reviews_payload = restaurants_service.build_reviews_payload(orders_for_charts)

            return jsonify({
                'success': True,
                'restaurant': response_data,
                'charts': chart_data,
                'menu_performance': menu_performance,
                'interruptions': interruptions,
                'reviews': reviews_payload,
                'filter': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'total_orders_filtered': len(filtered_orders) if (start_date or end_date) else len(all_orders)
                }
            })

        except Exception as e:
            print(f"Error getting restaurant detail: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/restaurant/<restaurant_id>/orders')
    @login_required
    def api_restaurant_orders(restaurant_id):
        """Get orders for a specific restaurant"""
        try:
            # Find restaurant (supports alias IDs).
            restaurant = find_restaurant_by_identifier(restaurant_id)
        
            if not restaurant:
                return jsonify({'success': False, 'error': 'Restaurant not found'}), 404

            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant, restaurant_id)
        
            # Get parameters
            try:
                per_page = int(request.args.get('per_page', 100))
                page = int(request.args.get('page', 1))
            except ValueError:
                return jsonify({'success': False, 'error': 'Invalid pagination parameters'}), 400

            per_page = max(1, min(per_page, 500))
            page = max(1, page)
            status = request.args.get('status')
        
            # Ensure order cache is present (DB cache snapshots may not include raw orders).
            orders = ensure_restaurant_orders_cache(
                restaurant,
                merchant_lookup_id,
                force_remote_sync=True
            )
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant, merchant_lookup_id)
        
            # Filter by status if provided
            if status:
                wanted_status = normalize_order_status_value(status)
                orders = [o for o in orders if get_order_status(o) == wanted_status]
        
            # Paginate
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            paginated_orders = orders[start_idx:end_idx]
        
            return jsonify({
                'success': True,
                'orders': paginated_orders,
                'total': len(orders),
                'page': page,
                'per_page': per_page,
                'total_pages': (len(orders) + per_page - 1) // per_page
            })
        
        except Exception as e:
            print(f"Error getting restaurant orders: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    @bp.route('/api/restaurant/<restaurant_id>/menu-performance')
    @login_required
    def api_restaurant_menu_performance(restaurant_id):
        """Get menu item performance for a specific restaurant."""
        try:
            restaurant = find_restaurant_by_identifier(restaurant_id)

            if not restaurant:
                return jsonify({'success': False, 'error': 'Restaurant not found'}), 404

            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant, restaurant_id)

            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            top_n = request.args.get('top_n', default=10, type=int)
            top_n = max(1, min(top_n or 10, 50))

            orders = ensure_restaurant_orders_cache(
                restaurant,
                merchant_lookup_id,
                force_remote_sync=True
            )
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant, merchant_lookup_id)
            if start_date or end_date:
                orders = restaurants_service.filter_orders_by_date_range(
                    orders,
                    start_date,
                    end_date,
                    datetime_mod=datetime,
                )

            performance = IFoodDataProcessor.calculate_menu_item_performance(orders, top_n=top_n)
            return jsonify({
                'success': True,
                'restaurant_id': merchant_lookup_id,
                'menu_performance': performance,
                'filter': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'top_n': top_n,
                    'orders_considered': len(orders)
                }
            })
        except Exception as e:
            print(f"Error getting menu performance: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    # ============================================================================
    # API ROUTES - RESTAURANT INTERRUPTIONS
    # ============================================================================

    @bp.route('/api/restaurant/<restaurant_id>/interruptions')
    @login_required
    def api_restaurant_interruptions(restaurant_id):
        """Get interruptions for a specific restaurant"""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)
        
            # Get interruptions
            interruptions = api.get_interruptions(merchant_lookup_id)
            if interruptions is None:
                return _ifood_error_response(api, action='consulta de interrupcoes', default_status=502)
        
            return jsonify({
                'success': True,
                'interruptions': interruptions or []
            })
        
        except Exception as e:
            print(f"Error getting interruptions: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/merchants')
    @login_required
    def api_ifood_homologation_merchants():
        """Live proxy for iFood GET /merchants used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            merchants = api.get_merchants()
            if merchants is None:
                return _ifood_error_response(api, action='listagem de lojas (GET /merchants)', default_status=502)

            if not isinstance(merchants, list):
                merchants = []

            return jsonify({
                'success': True,
                'merchants': merchants,
                'count': len(merchants),
            })
        except Exception as e:
            print(f"Error listing iFood merchants: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/merchants/<merchant_id>')
    @login_required
    def api_ifood_homologation_merchant_details(merchant_id):
        """Live proxy for iFood GET /merchants/{merchantId} used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            details = api.get_merchant_details(merchant_id)
            if details is None:
                return _ifood_error_response(
                    api,
                    action='detalhes da loja (GET /merchants/{merchantId})',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'merchant': details if isinstance(details, dict) else {},
            })
        except Exception as e:
            print(f"Error getting iFood merchant details: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/merchants/<merchant_id>/status')
    @login_required
    def api_ifood_homologation_merchant_status(merchant_id):
        """Live proxy for iFood GET /merchants/{merchantId}/status."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            status_payload = api.get_merchant_status(merchant_id)
            if status_payload is None:
                return _ifood_error_response(
                    api,
                    action='status da loja (GET /merchants/{merchantId}/status)',
                    default_status=502
                )
            return jsonify({
                'success': True,
                'module': 'Merchant',
                'api': 'Merchant Status',
                'merchant_id': merchant_id,
                'status': status_payload,
            })
        except Exception as e:
            print(f"Error getting iFood merchant status: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/merchants/<merchant_id>/interruptions', methods=['GET', 'POST'])
    @login_required
    def api_ifood_homologation_merchant_interruptions(merchant_id):
        """Live proxy for iFood GET/POST /merchants/{merchantId}/interruptions."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if request.method == 'GET':
                payload = api.get_interruptions(merchant_id)
                if payload is None:
                    return _ifood_error_response(
                        api,
                        action='interrupcoes da loja (GET /merchants/{merchantId}/interruptions)',
                        default_status=502
                    )
                return jsonify({
                    'success': True,
                    'module': 'Merchant',
                    'api': 'Merchant Interruptions',
                    'merchant_id': merchant_id,
                    'interruptions': payload if isinstance(payload, list) else [],
                })

            data = get_json_payload()
            if not isinstance(data, dict):
                data = {}
            start = str(data.get('start') or '').strip()
            end = str(data.get('end') or '').strip()
            description = str(data.get('description') or '').strip()
            if not start or not end:
                return jsonify({'success': False, 'error': 'start and end are required'}), 400
            if len(description) < 4:
                return jsonify({'success': False, 'error': 'description is required and must be clear'}), 400
            created = api.create_interruption(merchant_id, start, end, description)
            if created is None:
                return _ifood_error_response(
                    api,
                    action='criacao de interrupcao (POST /merchants/{merchantId}/interruptions)',
                    default_status=502
                )
            return jsonify({
                'success': True,
                'module': 'Merchant',
                'api': 'Create Merchant Interruption',
                'merchant_id': merchant_id,
                'interruption': created,
            }), 201
        except Exception as e:
            print(f"Error handling iFood merchant interruptions: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/merchants/<merchant_id>/interruptions/<interruption_id>', methods=['DELETE'])
    @login_required
    def api_ifood_homologation_merchant_delete_interruption(merchant_id, interruption_id):
        """Live proxy for iFood DELETE /merchants/{merchantId}/interruptions/{id}."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            deleted = api.delete_interruption(merchant_id, interruption_id)
            if not deleted:
                return _ifood_error_response(
                    api,
                    action='remocao de interrupcao (DELETE /merchants/{merchantId}/interruptions/{id})',
                    default_status=502
                )
            return jsonify({
                'success': True,
                'module': 'Merchant',
                'api': 'Delete Merchant Interruption',
                'merchant_id': merchant_id,
                'interruption_id': interruption_id,
            })
        except Exception as e:
            print(f"Error deleting iFood merchant interruption: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/merchants/<merchant_id>/opening-hours', methods=['GET', 'PUT'])
    @login_required
    def api_ifood_homologation_merchant_opening_hours(merchant_id):
        """Live proxy for iFood GET/PUT /merchants/{merchantId}/opening-hours."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if request.method == 'GET':
                opening_hours = api.get_opening_hours(merchant_id)
                if opening_hours is None:
                    return _ifood_error_response(
                        api,
                        action='horario de funcionamento (GET /merchants/{merchantId}/opening-hours)',
                        default_status=502
                    )
                return jsonify({
                    'success': True,
                    'module': 'Merchant',
                    'api': 'Merchant Opening Hours',
                    'merchant_id': merchant_id,
                    'opening_hours': opening_hours,
                })

            data = get_json_payload()
            if not isinstance(data, dict):
                data = {}
            opening_hours_raw = (
                data.get('opening_hours')
                if data.get('opening_hours') is not None
                else data.get('openingHours')
                if data.get('openingHours') is not None
                else data.get('hours')
            )
            timezone_name = str(data.get('timezone') or '').strip() or None
            normalized_hours, validation_error = _normalize_opening_hours_entries(opening_hours_raw)
            if validation_error:
                return jsonify({'success': False, 'error': validation_error}), 400
            updated = api.update_opening_hours(merchant_id, normalized_hours, timezone_name=timezone_name)
            if updated is None:
                return _ifood_error_response(
                    api,
                    action='atualizacao de horario (PUT /merchants/{merchantId}/opening-hours)',
                    default_status=502
                )
            return jsonify({
                'success': True,
                'module': 'Merchant',
                'api': 'Update Merchant Opening Hours',
                'merchant_id': merchant_id,
                'opening_hours': updated,
            }), 201
        except Exception as e:
            print(f"Error handling iFood merchant opening hours: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/authentication', methods=['POST'])
    @login_required
    def api_ifood_homologation_authentication():
        """Run a safe authentication probe without exposing the bearer token."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            authenticated = bool(api.authenticate())
            token_expires_at = getattr(api, 'token_expires_at', None)
            last_auth_error = str(getattr(api, 'last_auth_error', '') or '').strip() or None
            status_code = 200 if authenticated else 502
            if last_auth_error and last_auth_error.startswith('http_401'):
                status_code = 401
            elif last_auth_error and last_auth_error.startswith('http_403'):
                status_code = 403

            return jsonify({
                'success': authenticated,
                'module': 'Authentication',
                'grant_type': 'client_credentials',
                'authenticated': authenticated,
                'token_present': bool(getattr(api, 'access_token', None)),
                'token_expires_at': token_expires_at.isoformat() if token_expires_at else None,
                'client_id_hint': _masked_value(getattr(api, 'client_id', None), keep=6),
                'error': last_auth_error,
            }), status_code
        except Exception as e:
            print(f"Error authenticating against iFood: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/orders')
    @login_required
    def api_ifood_homologation_orders():
        """Live proxy for iFood order listing used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            merchant_id = str(
                request.args.get('merchant_id')
                or request.args.get('merchantId')
                or ''
            ).strip()
            if not merchant_id:
                return jsonify({'success': False, 'error': 'Merchant ID required'}), 400

            start_date = str(request.args.get('start_date') or request.args.get('startDate') or '').strip() or None
            end_date = str(request.args.get('end_date') or request.args.get('endDate') or '').strip() or None
            status = str(request.args.get('status') or '').strip() or None

            orders = api.get_orders(
                merchant_id,
                start_date,
                end_date,
                status,
                headers=HOMOLOGATION_ORDER_HEADERS,
            )
            if orders is None:
                return _ifood_error_response(api, action='listagem de pedidos (GET /orders)', default_status=502)
            if not isinstance(orders, list):
                orders = []
            for order_payload in orders[:50]:
                _persist_homologation_order_snapshot(order_payload, source='homologation_list')

            return jsonify({
                'success': True,
                'module': 'Order',
                'merchant_id': merchant_id,
                'filters': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'status': status,
                },
                'orders': orders,
                'count': len(orders),
            })
        except Exception as e:
            print(f"Error listing iFood orders: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/orders/recent')
    @login_required
    def api_ifood_homologation_recent_orders():
        """Recent local order candidates from webhook/polling snapshots."""
        try:
            org_id = get_current_org_id()
            merchant_id = str(
                request.args.get('merchant_id')
                or request.args.get('merchantId')
                or ''
            ).strip() or None
            limit = _parse_optional_int(request.args.get('limit'), minimum=1, maximum=100, default=20)
            snapshots = db.list_ifood_order_snapshots(org_id=org_id, merchant_id=merchant_id, limit=limit)
            return jsonify({
                'success': True,
                'module': 'Order',
                'source': 'local_snapshots',
                'merchant_id': merchant_id,
                'orders': [_snapshot_order_summary(snapshot) for snapshot in snapshots],
                'count': len(snapshots),
            })
        except Exception as e:
            print(f"Error listing local iFood order snapshots: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/orders/<order_id>')
    @login_required
    def api_ifood_homologation_order_details(order_id):
        """Live proxy for iFood GET /orders/{orderId} used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            details = api.get_order_details(order_id, headers=HOMOLOGATION_ORDER_HEADERS)
            if details is None:
                return _ifood_error_response(
                    api,
                    action='detalhes do pedido (GET /orders/{orderId})',
                    default_status=502
                )
            if isinstance(details, dict):
                _persist_homologation_order_snapshot(details, source='homologation_details')

            return jsonify({
                'success': True,
                'module': 'Order',
                'order': details if isinstance(details, dict) else {},
            })
        except Exception as e:
            print(f"Error getting iFood order details: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/orders/<order_id>/evidence')
    @login_required
    def api_ifood_homologation_order_evidence(order_id):
        """Reviewer-friendly, redacted field evidence for an iFood order."""
        try:
            refresh = _is_truthy(request.args.get('refresh', '1'))
            evidence, live_error = _build_order_evidence_for_current_org(order_id, allow_live_fetch=refresh)
            if not evidence:
                payload = {
                    'success': False,
                    'error': 'No local or live order evidence found for this order',
                    'order_id': order_id,
                }
                if live_error:
                    payload['live_error'] = live_error
                return jsonify(payload), 404
            if live_error:
                evidence['live_fetch_warning'] = live_error
            return jsonify({
                'success': True,
                'module': 'Order',
                'evidence': evidence,
            })
        except Exception as e:
            print(f"Error building iFood order evidence: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/events/polling', methods=['POST'])
    @login_required
    def api_ifood_homologation_events_polling():
        """Run a live polling request, optionally acknowledging returned events."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            data = get_json_payload()
            if not isinstance(data, dict):
                data = {}

            merchant_scope_raw = (
                data.get('merchant_ids')
                if data.get('merchant_ids') is not None
                else data.get('merchantId')
                if data.get('merchantId') is not None
                else data.get('merchant_id')
            )
            if isinstance(merchant_scope_raw, str):
                merchant_scope = [part.strip() for part in merchant_scope_raw.split(',') if part.strip()]
            elif isinstance(merchant_scope_raw, (list, tuple, set)):
                merchant_scope = [str(part).strip() for part in merchant_scope_raw if str(part).strip()]
            else:
                merchant_scope = []

            if not merchant_scope:
                return jsonify({'success': False, 'error': 'Merchant ID required for polling'}), 400

            ack_requested = _is_truthy(data.get('ack'))
            events = api.poll_events(merchant_scope if len(merchant_scope) > 1 else merchant_scope[0]) or []
            if not isinstance(events, list):
                events = []

            ack_result = None
            if ack_requested and events and hasattr(api, 'acknowledge_events'):
                ack_result = api.acknowledge_events(events)

            return jsonify({
                'success': True,
                'module': 'Events',
                'merchant_scope': merchant_scope,
                'events': events,
                'count': len(events),
                'ack_requested': ack_requested,
                'ack_result': ack_result,
            })
        except Exception as e:
            print(f"Error polling iFood events: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/financial/sales')
    @login_required
    def api_ifood_homologation_financial_sales():
        """Live proxy for Financial API Sales used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            merchant_id = _request_text_arg('merchant_id', 'merchantId')
            if not merchant_id:
                return jsonify({'success': False, 'error': 'Merchant ID required'}), 400

            begin_sales_date = _request_text_arg('beginSalesDate', 'begin_sales_date', 'start_date', 'startDate')
            end_sales_date = _request_text_arg('endSalesDate', 'end_sales_date', 'end_date', 'endDate')
            page = _request_text_arg('page')
            if not begin_sales_date or not end_sales_date:
                return jsonify({
                    'success': False,
                    'error': 'beginSalesDate and endSalesDate are required for Financial Sales'
                }), 400

            payload = api.get_financial_sales(
                merchant_id,
                begin_sales_date=begin_sales_date,
                end_sales_date=end_sales_date,
                page=page,
                headers=HOMOLOGATION_FINANCIAL_HEADERS,
            )
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='listagem financeira de vendas (GET /financial/v3.0/merchants/{merchantId}/sales)',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Financial',
                'api': 'Sales',
                'merchant_id': merchant_id,
                'filters': {
                    'beginSalesDate': begin_sales_date,
                    'endSalesDate': end_sales_date,
                    'page': page,
                },
                'homologation_header': 'x-request-homologation=true',
                'count': _count_financial_items(payload, 'sales'),
                'payload': payload,
            })
        except Exception as e:
            print(f"Error listing iFood financial sales: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/financial/events')
    @login_required
    def api_ifood_homologation_financial_events():
        """Live proxy for Financial API Financial Events used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            merchant_id = _request_text_arg('merchant_id', 'merchantId')
            if not merchant_id:
                return jsonify({'success': False, 'error': 'Merchant ID required'}), 400

            begin_date = _request_text_arg('beginDate', 'begin_date', 'start_date', 'startDate')
            end_date = _request_text_arg('endDate', 'end_date', 'endDate')
            page = _request_text_arg('page')
            size = _request_text_arg('size')
            if not begin_date or not end_date:
                return jsonify({
                    'success': False,
                    'error': 'beginDate and endDate are required for Financial Events'
                }), 400

            payload = api.get_financial_events(
                merchant_id,
                begin_date=begin_date,
                end_date_filter=end_date,
                page=page,
                size=size,
                headers=HOMOLOGATION_FINANCIAL_HEADERS,
            )
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='listagem de eventos financeiros (GET /financial/v3.0/merchants/{merchantId}/financial-events)',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Financial',
                'api': 'Financial Events',
                'merchant_id': merchant_id,
                'filters': {
                    'beginDate': begin_date,
                    'endDate': end_date,
                    'page': page,
                    'size': size,
                },
                'homologation_header': 'x-request-homologation=true',
                'count': _count_financial_items(payload, 'financialEvents'),
                'payload': payload,
            })
        except Exception as e:
            print(f"Error listing iFood financial events: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/financial/reconciliation')
    @login_required
    def api_ifood_homologation_financial_reconciliation():
        """Live proxy for Financial API Reconciliation used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            merchant_id = _request_text_arg('merchant_id', 'merchantId')
            competence = _request_text_arg('competence')
            if not merchant_id:
                return jsonify({'success': False, 'error': 'Merchant ID required'}), 400
            if not competence:
                return jsonify({'success': False, 'error': 'Competence required (YYYY-MM)'}), 400

            payload = api.get_financial_reconciliation(
                merchant_id,
                competence=competence,
                headers=HOMOLOGATION_FINANCIAL_HEADERS,
            )
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='consulta de reconciliacao financeira (GET /financial/v3.0/merchants/{merchantId}/reconciliation)',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Financial',
                'api': 'Reconciliation',
                'merchant_id': merchant_id,
                'filters': {'competence': competence},
                'homologation_header': 'x-request-homologation=true',
                'payload': payload,
            })
        except Exception as e:
            print(f"Error getting iFood financial reconciliation: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/financial/reconciliation/on-demand', methods=['POST'])
    @login_required
    def api_ifood_homologation_financial_reconciliation_on_demand():
        """Live proxy for Financial API Reconciliation On Demand creation."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            data = get_json_payload()
            if not isinstance(data, dict):
                data = {}

            merchant_id = _payload_text_value(data, 'merchant_id', 'merchantId') or _request_text_arg('merchant_id', 'merchantId')
            competence = _payload_text_value(data, 'competence')
            if not merchant_id:
                return jsonify({'success': False, 'error': 'Merchant ID required'}), 400
            if not competence:
                return jsonify({'success': False, 'error': 'Competence required (YYYY-MM)'}), 400

            payload = api.request_financial_reconciliation_on_demand(
                merchant_id,
                competence=competence,
                headers=HOMOLOGATION_FINANCIAL_HEADERS,
            )
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='solicitacao de reconciliacao on demand (POST /financial/v3.0/merchants/{merchantId}/reconciliation/on-demand)',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Financial',
                'api': 'Reconciliation On Demand',
                'merchant_id': merchant_id,
                'filters': {'competence': competence},
                'homologation_header': 'x-request-homologation=true',
                'payload': payload,
            })
        except Exception as e:
            print(f"Error requesting iFood reconciliation on demand: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/financial/reconciliation/on-demand/<request_id>')
    @login_required
    def api_ifood_homologation_financial_reconciliation_on_demand_status(request_id):
        """Live proxy for Financial API Reconciliation On Demand status."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            merchant_id = _request_text_arg('merchant_id', 'merchantId')
            if not merchant_id:
                return jsonify({'success': False, 'error': 'Merchant ID required'}), 400

            payload = api.get_financial_reconciliation_on_demand_status(
                merchant_id,
                request_id,
                headers=HOMOLOGATION_FINANCIAL_HEADERS,
            )
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='status da reconciliacao on demand (GET /financial/v3.0/merchants/{merchantId}/reconciliation/on-demand/{requestId})',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Financial',
                'api': 'Reconciliation On Demand Status',
                'merchant_id': merchant_id,
                'request_id': request_id,
                'homologation_header': 'x-request-homologation=true',
                'payload': payload,
            })
        except Exception as e:
            print(f"Error getting iFood reconciliation on demand status: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/financial/settlements')
    @login_required
    def api_ifood_homologation_financial_settlements():
        """Live proxy for Financial API Settlements used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            merchant_id = _request_text_arg('merchant_id', 'merchantId')
            if not merchant_id:
                return jsonify({'success': False, 'error': 'Merchant ID required'}), 400

            begin_payment_date = _request_text_arg('beginPaymentDate', 'begin_payment_date')
            end_payment_date = _request_text_arg('endPaymentDate', 'end_payment_date')
            begin_calculation_date = _request_text_arg('beginCalculationDate', 'begin_calculation_date')
            end_calculation_date = _request_text_arg('endCalculationDate', 'end_calculation_date')
            has_payment_pair = bool(begin_payment_date and end_payment_date)
            has_calculation_pair = bool(begin_calculation_date and end_calculation_date)
            if (begin_payment_date and not end_payment_date) or (end_payment_date and not begin_payment_date):
                return jsonify({
                    'success': False,
                    'error': 'Inform both beginPaymentDate and endPaymentDate for Settlements'
                }), 400
            if (begin_calculation_date and not end_calculation_date) or (end_calculation_date and not begin_calculation_date):
                return jsonify({
                    'success': False,
                    'error': 'Inform both beginCalculationDate and endCalculationDate for Settlements'
                }), 400
            if not has_payment_pair and not has_calculation_pair:
                return jsonify({
                    'success': False,
                    'error': 'Provide begin/end payment dates or begin/end calculation dates for Settlements'
                }), 400

            payload = api.get_financial_settlements(
                merchant_id,
                begin_payment_date=begin_payment_date,
                end_payment_date=end_payment_date,
                begin_calculation_date=begin_calculation_date,
                end_calculation_date=end_calculation_date,
                headers=HOMOLOGATION_FINANCIAL_HEADERS,
            )
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='consulta de liquidacoes financeiras (GET /financial/v3.0/merchants/{merchantId}/settlements)',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Financial',
                'api': 'Settlements',
                'merchant_id': merchant_id,
                'filters': {
                    'beginPaymentDate': begin_payment_date,
                    'endPaymentDate': end_payment_date,
                    'beginCalculationDate': begin_calculation_date,
                    'endCalculationDate': end_calculation_date,
                },
                'homologation_header': 'x-request-homologation=true',
                'count': _count_financial_items(payload, 'settlements'),
                'payload': payload,
            })
        except Exception as e:
            print(f"Error listing iFood financial settlements: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/financial/anticipations')
    @login_required
    def api_ifood_homologation_financial_anticipations():
        """Live proxy for Financial API Anticipations used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            merchant_id = _request_text_arg('merchant_id', 'merchantId')
            if not merchant_id:
                return jsonify({'success': False, 'error': 'Merchant ID required'}), 400

            begin_calculation_date = _request_text_arg('beginCalculationDate', 'begin_calculation_date')
            end_calculation_date = _request_text_arg('endCalculationDate', 'end_calculation_date')
            begin_anticipated_payment_date = _request_text_arg('beginAnticipatedPaymentDate', 'begin_anticipated_payment_date')
            end_anticipated_payment_date = _request_text_arg('endAnticipatedPaymentDate', 'end_anticipated_payment_date')
            has_calculation_pair = bool(begin_calculation_date and end_calculation_date)
            has_payment_pair = bool(begin_anticipated_payment_date and end_anticipated_payment_date)
            if (begin_calculation_date and not end_calculation_date) or (end_calculation_date and not begin_calculation_date):
                return jsonify({
                    'success': False,
                    'error': 'Inform both beginCalculationDate and endCalculationDate for Anticipations'
                }), 400
            if (begin_anticipated_payment_date and not end_anticipated_payment_date) or (end_anticipated_payment_date and not begin_anticipated_payment_date):
                return jsonify({
                    'success': False,
                    'error': 'Inform both beginAnticipatedPaymentDate and endAnticipatedPaymentDate for Anticipations'
                }), 400
            if not has_calculation_pair and not has_payment_pair:
                return jsonify({
                    'success': False,
                    'error': 'Provide begin/end calculation dates or begin/end anticipated payment dates for Anticipations'
                }), 400

            payload = api.get_financial_anticipations(
                merchant_id,
                begin_calculation_date=begin_calculation_date,
                end_calculation_date=end_calculation_date,
                begin_anticipated_payment_date=begin_anticipated_payment_date,
                end_anticipated_payment_date=end_anticipated_payment_date,
                headers=HOMOLOGATION_FINANCIAL_HEADERS,
            )
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='consulta de antecipacoes financeiras (GET /financial/v3.0/merchants/{merchantId}/anticipations)',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Financial',
                'api': 'Anticipations',
                'merchant_id': merchant_id,
                'filters': {
                    'beginCalculationDate': begin_calculation_date,
                    'endCalculationDate': end_calculation_date,
                    'beginAnticipatedPaymentDate': begin_anticipated_payment_date,
                    'endAnticipatedPaymentDate': end_anticipated_payment_date,
                },
                'homologation_header': 'x-request-homologation=true',
                'count': _count_financial_items(payload, 'settlements'),
                'payload': payload,
            })
        except Exception as e:
            print(f"Error listing iFood financial anticipations: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    # ========================================================================
    # Homologation: Reviews
    # ========================================================================

    @bp.route('/api/ifood/homologation/reviews')
    @login_required
    def api_ifood_homologation_reviews():
        """Live proxy for Review API list used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            filters, error_response = _get_homologation_review_filters()
            if error_response:
                return error_response

            payload = api.list_reviews(
                filters['merchant_id'],
                page=filters.get('page'),
                page_size=filters.get('page_size'),
                add_count=filters.get('add_count'),
                date_from=filters.get('date_from'),
                date_to=filters.get('date_to'),
                status=filters.get('status'),
            )
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='listagem de avaliacoes (GET /review/v2.0/merchants/{merchantId}/reviews)',
                    default_status=502
                )

            count = 0
            if isinstance(payload, dict) and isinstance(payload.get('reviews'), list):
                count = len(payload.get('reviews') or [])
            elif isinstance(payload, list):
                count = len(payload)

            return jsonify({
                'success': True,
                'module': 'Review',
                'api': 'Reviews',
                'merchant_id': filters['merchant_id'],
                'filters': {
                    'page': filters.get('page'),
                    'page_size': filters.get('page_size'),
                    'add_count': filters.get('add_count'),
                    'dateFrom': filters.get('date_from'),
                    'dateTo': filters.get('date_to'),
                    'status': filters.get('status'),
                },
                'count': count,
                'payload': payload,
            })
        except Exception as e:
            print(f"Error listing iFood reviews: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/reviews/summary')
    @login_required
    def api_ifood_homologation_review_summary():
        """Live proxy for Review API summary used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            filters, error_response = _get_homologation_review_filters()
            if error_response:
                return error_response

            payload = api.get_review_summary(filters['merchant_id'])
            if payload is None:
                status_code = _parse_ifood_error_status(api, default_status=502)
                if status_code == 404:
                    return jsonify({
                        'success': True,
                        'module': 'Review',
                        'api': 'Review Summary',
                        'merchant_id': filters['merchant_id'],
                        'payload': None,
                        'empty': True,
                        'ifood_status': 404,
                        'homologation_note': (
                            'iFood retornou 404 "Summary not found". Esta loja ainda nao possui resumo '
                            'de avaliacoes (nenhum review agregado). Estado valido para homologacao.'
                        ),
                    })
                return _ifood_error_response(
                    api,
                    action='resumo de avaliacoes (GET /review/v2.0/merchants/{merchantId}/summary)',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Review',
                'api': 'Review Summary',
                'merchant_id': filters['merchant_id'],
                'payload': payload,
            })
        except Exception as e:
            print(f"Error getting iFood review summary: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/reviews/<review_id>')
    @login_required
    def api_ifood_homologation_review_details(review_id):
        """Live proxy for Review API detail used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            filters, error_response = _get_homologation_review_filters()
            if error_response:
                return error_response

            payload = api.get_review_details(filters['merchant_id'], review_id)
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='detalhe de avaliacao (GET /review/v2.0/merchants/{merchantId}/reviews/{reviewId})',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Review',
                'api': 'Review Details',
                'merchant_id': filters['merchant_id'],
                'review_id': review_id,
                'payload': payload,
            })
        except Exception as e:
            print(f"Error getting iFood review details: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/reviews/<review_id>/answers', methods=['POST'])
    @login_required
    def api_ifood_homologation_review_answer(review_id):
        """Live proxy for Review API answer creation used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            data = get_json_payload()
            if not isinstance(data, dict):
                data = {}
            filters, error_response = _get_homologation_review_filters(data)
            if error_response:
                return error_response

            text = _payload_text_value(data, 'text', 'answer', 'message')
            if not text:
                return jsonify({'success': False, 'error': 'Review answer text required'}), 400

            payload = api.answer_review(filters['merchant_id'], review_id, text)
            if payload is None:
                return _ifood_error_response(
                    api,
                    action='resposta de avaliacao (POST /review/v2.0/merchants/{merchantId}/reviews/{reviewId}/answers)',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Review',
                'api': 'Review Answer',
                'merchant_id': filters['merchant_id'],
                'review_id': review_id,
                'payload': payload,
            })
        except Exception as e:
            print(f"Error answering iFood review: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    @bp.route('/api/restaurant/<restaurant_id>/status')
    @login_required
    def api_restaurant_status(restaurant_id):
        """Get operational status for a specific restaurant"""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)
        
            # Get status
            status = api.get_merchant_status(merchant_lookup_id)
            if status is None:
                return _ifood_error_response(api, action='consulta de status', default_status=502)
        
            return jsonify({
                'success': True,
                'status': status or {'state': 'UNKNOWN', 'message': 'Unable to fetch status'}
            })
        
        except Exception as e:
            print(f"Error getting status: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    @bp.route('/api/restaurant/<restaurant_id>/interruptions', methods=['POST'])
    @admin_required
    def api_create_interruption(restaurant_id):
        """Create a new interruption (close store temporarily)"""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)
        
            data = get_json_payload()
            if not isinstance(data, dict):
                return jsonify({'success': False, 'error': 'Invalid payload'}), 400
            start = data.get('start')
            end = data.get('end')
            description = data.get('description', '')
        
            if not start or not end:
                return jsonify({'success': False, 'error': 'Start and end times required'}), 400
            if len(str(description or '').strip()) < 4:
                return jsonify({'success': False, 'error': 'Description is required and must be clear'}), 400

            start_dt = _parse_iso_datetime(start)
            end_dt = _parse_iso_datetime(end)
            if not start_dt or not end_dt:
                return jsonify({'success': False, 'error': 'Invalid datetime format; use ISO-8601'}), 400
            if end_dt <= start_dt:
                return jsonify({'success': False, 'error': 'End must be greater than start'}), 400
            max_window_seconds = 7 * 24 * 60 * 60
            if (end_dt - start_dt).total_seconds() > max_window_seconds:
                return jsonify({'success': False, 'error': 'Interruption duration cannot exceed 7 days'}), 400
        
            # Create interruption
            result = api.create_interruption(merchant_lookup_id, start, end, description)
        
            if result:
                return jsonify({
                    'success': True,
                    'interruption': result,
                    'message': 'Interruption created successfully'
                })
            return _ifood_error_response(api, action='criacao de interrupcao', default_status=502)
        
        except Exception as e:
            print(f"Error creating interruption: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    @bp.route('/api/restaurant/<restaurant_id>/interruptions/<interruption_id>', methods=['DELETE'])
    @admin_required
    def api_delete_interruption(restaurant_id, interruption_id):
        """Delete an interruption (reopen store)"""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)

            # Reopen should be blocked when iFood marks merchant as not reopenable.
            status_payload = api.get_merchant_status(merchant_lookup_id)
            if status_payload is None:
                return _ifood_error_response(api, action='consulta de status antes de reabertura', default_status=502)
            reopenable = _parse_reopenable_flag(status_payload)
            if reopenable is False:
                return jsonify({
                    'success': False,
                    'error': 'Loja bloqueada para reabertura no iFood (reopenable=false).'
                }), 409
        
            # Delete interruption
            success = api.delete_interruption(merchant_lookup_id, interruption_id)
        
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Interruption removed successfully'
                })
            return _ifood_error_response(api, action='remocao de interrupcao', default_status=502)
        
        except Exception as e:
            print(f"Error deleting interruption: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/restaurant/<restaurant_id>/opening-hours')
    @login_required
    def api_restaurant_opening_hours(restaurant_id):
        """Get opening-hours for a specific restaurant."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)

            if not hasattr(api, 'get_opening_hours'):
                return jsonify({'success': False, 'error': 'Opening-hours API not supported by client'}), 501

            opening_hours = api.get_opening_hours(merchant_lookup_id)
            if opening_hours is None:
                return _ifood_error_response(api, action='consulta de horario de funcionamento', default_status=502)

            return jsonify({
                'success': True,
                'opening_hours': opening_hours
            })
        except Exception as e:
            print(f"Error getting opening hours: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/restaurant/<restaurant_id>/opening-hours', methods=['PUT'])
    @admin_required
    def api_update_restaurant_opening_hours(restaurant_id):
        """Replace opening-hours for a specific restaurant."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if not hasattr(api, 'update_opening_hours'):
                return jsonify({'success': False, 'error': 'Opening-hours API not supported by client'}), 501

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)

            data = get_json_payload()
            if not isinstance(data, dict):
                return jsonify({'success': False, 'error': 'Invalid payload'}), 400

            opening_hours_raw = (
                data.get('opening_hours')
                if data.get('opening_hours') is not None
                else data.get('openingHours')
                if data.get('openingHours') is not None
                else data.get('hours')
            )
            timezone_name = str(data.get('timezone') or '').strip() or None

            normalized_hours, validation_error = _normalize_opening_hours_entries(opening_hours_raw)
            if validation_error:
                return jsonify({'success': False, 'error': validation_error}), 400

            updated = api.update_opening_hours(
                merchant_lookup_id,
                opening_hours=normalized_hours,
                timezone_name=timezone_name
            )
            if updated is None:
                return _ifood_error_response(api, action='atualizacao de horario de funcionamento', default_status=502)

            return jsonify({
                'success': True,
                'opening_hours': updated,
                'message': 'Opening-hours updated successfully'
            })
        except Exception as e:
            print(f"Error updating opening hours: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    # ========================================================================
    # Homologation: Order Action Routes
    # ========================================================================

    def _homolog_order_action(order_id, action_name, api_method, extra_payload=None):
        """Generic handler for homologation order action endpoints."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            method_fn = getattr(api, api_method, None)
            if not method_fn:
                return jsonify({'success': False, 'error': f'Method {api_method} not available'}), 501
            if extra_payload is not None:
                result = method_fn(order_id, **extra_payload, headers=HOMOLOGATION_ORDER_HEADERS)
            else:
                result = method_fn(order_id, headers=HOMOLOGATION_ORDER_HEADERS)
            if result is None:
                return _ifood_error_response(api, action=action_name, default_status=502)
            return jsonify({'success': True, 'module': 'Order', 'action': action_name, 'result': result})
        except Exception as e:
            print(f"Error in homologation {action_name}: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/orders/<order_id>/confirm', methods=['POST'])
    @login_required
    def api_ifood_homologation_order_confirm(order_id):
        return _homolog_order_action(order_id, 'confirm', 'confirm_order')

    @bp.route('/api/ifood/homologation/orders/<order_id>/start-preparation', methods=['POST'])
    @login_required
    def api_ifood_homologation_order_start_preparation(order_id):
        return _homolog_order_action(order_id, 'startPreparation', 'start_order_preparation')

    @bp.route('/api/ifood/homologation/orders/<order_id>/dispatch', methods=['POST'])
    @login_required
    def api_ifood_homologation_order_dispatch(order_id):
        return _homolog_order_action(order_id, 'dispatch', 'dispatch_order')

    @bp.route('/api/ifood/homologation/orders/<order_id>/ready-to-pickup', methods=['POST'])
    @login_required
    def api_ifood_homologation_order_ready_to_pickup(order_id):
        return _homolog_order_action(order_id, 'readyToPickup', 'ready_order_for_pickup')

    @bp.route('/api/ifood/homologation/orders/<order_id>/cancellation-reasons')
    @login_required
    def api_ifood_homologation_order_cancellation_reasons(order_id):
        """GET cancellation reasons for an order - must be called BEFORE requesting cancellation."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            reasons = api.get_order_cancellation_reasons(order_id, headers=HOMOLOGATION_ORDER_HEADERS)
            if reasons is None:
                return _ifood_error_response(api, action='cancellationReasons', default_status=502)
            return jsonify({
                'success': True,
                'module': 'Order',
                'action': 'cancellationReasons',
                'order_id': order_id,
                'reasons': reasons if isinstance(reasons, list) else [],
            })
        except Exception as e:
            print(f"Error getting cancellation reasons: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/orders/<order_id>/request-cancellation', methods=['POST'])
    @login_required
    def api_ifood_homologation_order_request_cancellation(order_id):
        """POST request cancellation - requires cancellation_code from cancellationReasons."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            data = get_json_payload()
            cancellation_code = str(data.get('cancellation_code') or data.get('cancellationCode') or '').strip()
            reason = str(data.get('reason') or '').strip() or None
            if not cancellation_code:
                return jsonify({'success': False, 'error': 'cancellation_code is required (get from /cancellationReasons first)'}), 400
            result = api.request_order_cancellation(
                order_id,
                cancellation_code,
                reason,
                headers=HOMOLOGATION_ORDER_HEADERS,
            )
            if result is None:
                return _ifood_error_response(api, action='requestCancellation', default_status=502)
            return jsonify({'success': True, 'module': 'Order', 'action': 'requestCancellation', 'result': result})
        except Exception as e:
            print(f"Error requesting cancellation: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/orders/<order_id>/tracking', methods=['POST'])
    @login_required
    def api_ifood_homologation_order_tracking(order_id):
        return _homolog_order_action(order_id, 'tracking', 'get_order_tracking')

    @bp.route('/api/ifood/homologation/orders/<order_id>/virtual-bag')
    @login_required
    def api_ifood_homologation_order_virtual_bag(order_id):
        return _homolog_order_action(order_id, 'virtual-bag', 'get_order_virtual_bag')

    @bp.route('/api/ifood/homologation/orders/<order_id>/validate-pickup-code', methods=['POST'])
    @login_required
    def api_ifood_homologation_order_validate_pickup_code(order_id):
        data = get_json_payload()
        code = str(data.get('code') or '').strip()
        if not code:
            return jsonify({'success': False, 'error': 'code is required'}), 400
        return _homolog_order_action(order_id, 'validatePickupCode', 'validate_order_pickup_code', {'code': code})

    @bp.route('/api/ifood/homologation/orders/<order_id>/verify-delivery-code', methods=['POST'])
    @login_required
    def api_ifood_homologation_order_verify_delivery_code(order_id):
        data = get_json_payload()
        code = str(data.get('code') or '').strip()
        if not code:
            return jsonify({'success': False, 'error': 'code is required'}), 400
        return _homolog_order_action(order_id, 'verifyDeliveryCode', 'verify_order_delivery_code', {'code': code})

    # ========================================================================
    # Homologation: Dispute / Negotiation Platform Routes
    # ========================================================================

    @bp.route('/api/ifood/homologation/disputes/<dispute_id>/accept', methods=['POST'])
    @login_required
    def api_ifood_homologation_dispute_accept(dispute_id):
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            result = api.accept_dispute(dispute_id)
            if result is None:
                return _ifood_error_response(api, action='accept dispute', default_status=502)
            return jsonify({'success': True, 'module': 'Negotiation', 'action': 'acceptDispute', 'result': result})
        except Exception as e:
            print(f"Error accepting dispute: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/disputes/<dispute_id>/reject', methods=['POST'])
    @login_required
    def api_ifood_homologation_dispute_reject(dispute_id):
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            data = get_json_payload()
            reason = str(data.get('reason') or '').strip() or None
            result = api.reject_dispute(dispute_id, reason)
            if result is None:
                return _ifood_error_response(api, action='reject dispute', default_status=502)
            return jsonify({'success': True, 'module': 'Negotiation', 'action': 'rejectDispute', 'result': result})
        except Exception as e:
            print(f"Error rejecting dispute: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/homologation/disputes/<dispute_id>/alternatives/<alternative_id>', methods=['POST'])
    @login_required
    def api_ifood_homologation_dispute_alternative(dispute_id, alternative_id):
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            data = get_json_payload()
            result = api.submit_dispute_alternative(dispute_id, alternative_id, data if isinstance(data, dict) else None)
            if result is None:
                return _ifood_error_response(api, action='submit dispute alternative', default_status=502)
            return jsonify({'success': True, 'module': 'Negotiation', 'action': 'submitDisputeAlternative', 'result': result})
        except Exception as e:
            print(f"Error submitting dispute alternative: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    app.register_blueprint(bp)
