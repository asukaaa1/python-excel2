"""Restaurant data and interruption route registrations."""

from flask import Blueprint
from app_services import restaurants_service
from app_routes.dependencies import bind_dependencies


REQUIRED_DEPS = [
    'IFOOD_API',
    'IFoodDataProcessor',
    'LAST_DATA_REFRESH',
    'ORG_DATA',
    '_extract_status_message_text',
    'admin_required',
    'copy',
    'datetime',
    'detect_restaurant_closure',
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
        status_code = _parse_ifood_error_status(api, default_status=default_status)
        if status_code == 400:
            return jsonify({
                'success': False,
                'error': f'Falha de validacao ao executar {action}. Revise campos obrigatorios e formato dos horarios.'
            }), 400
        if status_code == 401:
            return jsonify({
                'success': False,
                'error': 'Nao autorizado no iFood. Revise as credenciais da organizacao.'
            }), 401
        if status_code == 403:
            return jsonify({
                'success': False,
                'error': 'Permissao insuficiente no iFood para a loja informada.'
            }), 403
        if status_code == 409:
            return jsonify({
                'success': False,
                'error': 'Conflito detectado (ex.: sobreposicao de interrupcao/horario). Ajuste a janela e tente novamente.'
            }), 409
        if status_code == 429:
            return jsonify({
                'success': False,
                'error': 'Limite de requisicoes iFood atingido. Tente novamente em instantes.'
            }), 429
        if 500 <= status_code <= 599:
            return jsonify({
                'success': False,
                'error': 'iFood indisponivel no momento. Tente novamente com backoff.'
            }), 502
        return jsonify({'success': False, 'error': f'Falha ao executar {action}.'}), default_status

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
                                None
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
                            None
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
                                None
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
                        None
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
                        None
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

            orders = api.get_orders(merchant_id, start_date, end_date, status)
            if orders is None:
                return _ifood_error_response(api, action='listagem de pedidos (GET /orders)', default_status=502)
            if not isinstance(orders, list):
                orders = []

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

    @bp.route('/api/ifood/homologation/orders/<order_id>')
    @login_required
    def api_ifood_homologation_order_details(order_id):
        """Live proxy for iFood GET /orders/{orderId} used in homologation demos."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            details = api.get_order_details(order_id)
            if details is None:
                return _ifood_error_response(
                    api,
                    action='detalhes do pedido (GET /orders/{orderId})',
                    default_status=502
                )

            return jsonify({
                'success': True,
                'module': 'Order',
                'order': details if isinstance(details, dict) else {},
            })
        except Exception as e:
            print(f"Error getting iFood order details: {e}")
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


    app.register_blueprint(bp)
