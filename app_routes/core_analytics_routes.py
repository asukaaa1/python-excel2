"""Core route registrations by domain."""

from app_services import core_analytics_service
from app_routes.route_context import RouteContext


REQUIRED_DEPS = [
    'CANCELLED_RESTAURANTS',
    'RESTAURANTS_DATA',
    '_GLOBAL_STATE_LOCK',
    '_aggregate_daily',
    '_calculate_period_metrics',
    '_filter_orders_by_date',
    '_init_org_ifood',
    '_load_org_restaurants',
    'admin_required',
    'datetime',
    'db',
    'get_current_org_id',
    'get_current_org_restaurants',
    'get_json_payload',
    'get_org_data',
    'get_resilient_api_client',
    'internal_error_response',
    'json',
    'jsonify',
    'log_exception',
    'login_required',
    'normalize_merchant_id',
    'platform_admin_required',
    'request',
    'require_feature',
    'sanitize_merchant_name',
    'timedelta',
]


def register_routes(bp, ctx: RouteContext):
    deps = ctx.require(REQUIRED_DEPS)
    # Keep mutable shared collections on module globals so global assignments
    # inside handlers stay in sync across requests.
    globals()['CANCELLED_RESTAURANTS'] = deps['CANCELLED_RESTAURANTS']
    globals()['RESTAURANTS_DATA'] = deps['RESTAURANTS_DATA']
    # Explicit aliases keep IDE/static analysis happy.
    _GLOBAL_STATE_LOCK = deps['_GLOBAL_STATE_LOCK']
    _aggregate_daily = deps['_aggregate_daily']
    _calculate_period_metrics = deps['_calculate_period_metrics']
    _filter_orders_by_date = deps['_filter_orders_by_date']
    _init_org_ifood = deps['_init_org_ifood']
    _load_org_restaurants = deps['_load_org_restaurants']
    admin_required = deps['admin_required']
    datetime = deps['datetime']
    db = deps['db']
    get_current_org_id = deps['get_current_org_id']
    get_current_org_restaurants = deps['get_current_org_restaurants']
    get_json_payload = deps['get_json_payload']
    get_org_data = deps['get_org_data']
    get_resilient_api_client = deps['get_resilient_api_client']
    internal_error_response = deps['internal_error_response']
    json = deps['json']
    jsonify = deps['jsonify']
    log_exception = deps['log_exception']
    login_required = deps['login_required']
    normalize_merchant_id = deps['normalize_merchant_id']
    platform_admin_required = deps['platform_admin_required']
    request = deps['request']
    require_feature = deps['require_feature']
    sanitize_merchant_name = deps['sanitize_merchant_name']
    timedelta = deps['timedelta']

    @bp.route('/api/analytics/compare')
    @login_required
    @require_feature('analytics')
    def api_compare_periods():
        """Compare restaurant metrics between two time periods.
    
        Query params:
            restaurant_id: specific restaurant or 'all'
            period_a_start, period_a_end: first period (ISO dates)
            period_b_start, period_b_end: second period (ISO dates)
            preset: optional shortcut - 'week', 'month', 'quarter', 'yoy'
        """
        try:
            restaurant_id = request.args.get('restaurant_id', 'all')
            preset = request.args.get('preset')

            period_a_start, period_a_end, period_b_start, period_b_end = core_analytics_service.resolve_compare_periods(
                now=datetime.now(),
                preset=preset,
                args=request.args,
                datetime_mod=datetime,
            )

            targets = core_analytics_service.select_restaurants(get_current_org_restaurants(), restaurant_id)
            if not targets:
                return jsonify({'success': False, 'error': 'Restaurant not found'}), 404

            comparisons, totals_a, totals_b, overall_deltas = core_analytics_service.build_period_comparison(
                targets=targets,
                period_a_start=period_a_start,
                period_a_end=period_a_end,
                period_b_start=period_b_start,
                period_b_end=period_b_end,
                filter_orders_by_date=_filter_orders_by_date,
                calculate_period_metrics=_calculate_period_metrics,
            )
        
            return jsonify({
                'success': True,
                'period_a': {'start': period_a_start.strftime('%Y-%m-%d'), 'end': period_a_end.strftime('%Y-%m-%d')},
                'period_b': {'start': period_b_start.strftime('%Y-%m-%d'), 'end': period_b_end.strftime('%Y-%m-%d')},
                'restaurants': comparisons,
                'totals': {'period_a': totals_a, 'period_b': totals_b, 'deltas': overall_deltas},
                'preset': preset
            })
        
        except ValueError as e:
            return jsonify({'success': False, 'error': f'Invalid date format: {e}'}), 400
        except Exception as e:
            print(f"Error in compare: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/analytics/daily-comparison')
    @login_required
    @require_feature('analytics')
    def api_daily_comparison():
        """Get day-by-day data for two periods for chart overlay.
        Returns arrays aligned by day offset for easy chart rendering."""
        try:
            restaurant_id = request.args.get('restaurant_id', 'all')
            preset = request.args.get('preset', 'week')

            period_a_start, period_a_end, period_b_start, period_b_end = core_analytics_service.resolve_daily_periods(
                now=datetime.now(),
                preset=preset,
                args=request.args,
                datetime_mod=datetime,
            )

            all_orders = core_analytics_service.collect_orders(
                restaurants=get_current_org_restaurants(),
                restaurant_id=restaurant_id,
            )

            orders_a = _filter_orders_by_date(all_orders, period_a_start, period_a_end)
            orders_b = _filter_orders_by_date(all_orders, period_b_start, period_b_end)

            daily_a = _aggregate_daily(orders_a, period_a_start, period_a_end)
            daily_b = _aggregate_daily(orders_b, period_b_start, period_b_end)
        
            return jsonify({
                'success': True,
                'period_a': {'start': period_a_start.strftime('%Y-%m-%d'), 'end': period_a_end.strftime('%Y-%m-%d'), 'daily': daily_a},
                'period_b': {'start': period_b_start.strftime('%Y-%m-%d'), 'end': period_b_end.strftime('%Y-%m-%d'), 'daily': daily_b}
            })
        
        except ValueError as e:
            return jsonify({'success': False, 'error': f'Invalid date format: {e}'}), 400
        except Exception as e:
            print(f"Error in daily comparison: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/ifood/config')
    @platform_admin_required
    def api_ifood_config():
        """Get iFood configuration (without secrets)"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'Organization context required'}), 403

            org = get_org_data(org_id)
            org_cfg = org.get('config') or db.get_org_ifood_config(org_id) or {}
            merchants = org_cfg.get('merchants') or []
            if isinstance(merchants, str):
                try:
                    merchants = json.loads(merchants)
                except Exception:
                    merchants = []
            settings = org_cfg.get('settings') if isinstance(org_cfg, dict) else {}
            api = org.get('api') or _init_org_ifood(org_id)
            last_refresh = org.get('last_refresh')
            config = {
                'configured': bool(api),
                'merchant_count': len(merchants),
                'merchants': [
                    (
                        {
                            'merchant_id': normalize_merchant_id(m.get('merchant_id') or m.get('id')),
                            'name': sanitize_merchant_name(m.get('name')),
                            'manager': sanitize_merchant_name(m.get('manager'))
                        }
                        if isinstance(m, dict)
                        else {
                            'merchant_id': normalize_merchant_id(m),
                            'name': '',
                            'manager': ''
                        }
                    )
                    for m in merchants
                ],
                'data_fetch_days': int(
                    (settings or {}).get('data_fetch_days')
                    or org_cfg.get('data_fetch_days')
                    or 30
                ),
                'refresh_interval_minutes': int(
                    (settings or {}).get('refresh_interval_minutes')
                    or org_cfg.get('refresh_interval_minutes')
                    or 60
                ),
                'last_refresh': last_refresh.isoformat() if isinstance(last_refresh, datetime) else None
            }
        
            return jsonify({'success': True, 'config': config})
        
        except Exception as e:
            print(f"Error getting config: {e}")
            return jsonify({'success': False, 'error': 'Internal server error'}), 500

    @bp.route('/api/ifood/merchants', methods=['POST'])
    @admin_required
    def api_add_merchant():
        """Add a merchant to current org config."""
        try:
            data = get_json_payload()
        
            merchant_id = normalize_merchant_id(data.get('merchant_id'))
            name = sanitize_merchant_name(data.get('name'))
            manager = sanitize_merchant_name(data.get('manager')) or 'Gerente'
        
            if not merchant_id:
                return jsonify({'success': False, 'error': 'Merchant ID required'}), 400

            merchant_payload = {
                'merchant_id': merchant_id,
                'name': name or f'Restaurant {merchant_id[:8]}',
                'manager': manager
            }

            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'Organization context required'}), 403
            org_cfg = db.get_org_ifood_config(org_id) or {}
            merchants = org_cfg.get('merchants') or []
            if isinstance(merchants, str):
                try:
                    merchants = json.loads(merchants)
                except Exception:
                    merchants = []
            for m in merchants:
                existing_id = normalize_merchant_id(
                    m.get('merchant_id') or m.get('id')
                    if isinstance(m, dict)
                    else m
                )
                if existing_id == merchant_id:
                    return jsonify({'success': False, 'error': 'Merchant already exists'}), 400
            merchants.append(merchant_payload)
            db.update_org_ifood_config(org_id, merchants=merchants)
            api = _init_org_ifood(org_id)
            if api:
                _load_org_restaurants(org_id)
            return jsonify({
                'success': True,
                'message': 'Merchant added successfully',
                'restaurant_count': len(get_current_org_restaurants())
            })
        
        except Exception as e:
            print(f"Error adding merchant: {e}")
            return jsonify({'success': False, 'error': 'Internal server error'}), 500

    @bp.route('/api/ifood/merchants/<merchant_id>', methods=['DELETE'])
    @admin_required
    def api_remove_merchant(merchant_id):
        """Remove a merchant from current org config."""
        try:
            target_merchant_id = normalize_merchant_id(merchant_id)
            if not target_merchant_id:
                return jsonify({'success': False, 'error': 'Merchant not found'}), 404

            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'Organization context required'}), 403
            org_cfg = db.get_org_ifood_config(org_id) or {}
            merchants = org_cfg.get('merchants') or []
            if isinstance(merchants, str):
                try:
                    merchants = json.loads(merchants)
                except Exception:
                    merchants = []
            original_count = len(merchants)
            merchants = [
                m for m in merchants
                if normalize_merchant_id(
                    m.get('merchant_id') or m.get('id')
                    if isinstance(m, dict)
                    else m
                ) != target_merchant_id
            ]
            if len(merchants) == original_count:
                return jsonify({'success': False, 'error': 'Merchant not found'}), 404
            db.update_org_ifood_config(org_id, merchants=merchants)
            api = _init_org_ifood(org_id)
            if api:
                _load_org_restaurants(org_id)
            return jsonify({
                'success': True,
                'message': 'Merchant removed successfully',
                'restaurant_count': len(get_current_org_restaurants())
            })
        
        except Exception as e:
            print(f"Error removing merchant: {e}")
            return jsonify({'success': False, 'error': 'Internal server error'}), 500

    @bp.route('/api/ifood/test')
    @platform_admin_required
    def api_test_ifood():
        """Test iFood API connection"""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({
                    'success': False,
                    'error': 'iFood API not configured',
                    'configured': False
                })
        
            # Try to authenticate
            if api.authenticate():
                # Try to fetch merchants
                merchants = api.get_merchants()
            
                return jsonify({
                    'success': True,
                    'message': 'iFood API connection successful',
                    'configured': True,
                    'merchant_count': len(merchants) if merchants else 0
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Authentication failed',
                    'configured': True
                })
            
        except Exception as e:
            print(f"Error testing iFood API: {e}")
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'configured': bool(get_resilient_api_client())
            })

    @bp.route('/api/comparativo/stats')
    @admin_required
    @require_feature('comparativo')
    def api_comparativo_stats():
        """Get consolidated stats for comparativo page"""
        try:
            stats = core_analytics_service.build_comparativo_stats(
                restaurants_data=RESTAURANTS_DATA,
                org_restaurants=get_current_org_restaurants(),
                cancelled_restaurants=CANCELLED_RESTAURANTS,
            )

            return jsonify({
                'success': True,
                'stats': stats
            })
        
        except Exception as e:
            print(f"Error getting comparativo stats: {e}")
            return internal_error_response()

    @bp.route('/api/comparativo/managers')
    @admin_required
    @require_feature('comparativo')
    def api_comparativo_managers():
        """Get data grouped by manager"""
        try:
            managers = core_analytics_service.build_managers_payload(RESTAURANTS_DATA)
        
            return jsonify({
                'success': True,
                'managers': managers
            })
        
        except Exception as e:
            print(f"Error getting managers data: {e}")
            return internal_error_response()

    @bp.route('/api/comparativo/cancelled')
    @admin_required
    @require_feature('comparativo')
    def api_comparativo_cancelled():
        """Get cancelled restaurants"""
        return jsonify({
            'success': True,
            'cancelled': CANCELLED_RESTAURANTS
        })

    @bp.route('/api/comparativo/cancelled', methods=['POST'])
    @admin_required
    @require_feature('comparativo')
    def api_cancel_restaurant():
        """Mark a restaurant as cancelled"""
        global RESTAURANTS_DATA
        try:
            data = get_json_payload()
            restaurant_id = data.get('restaurant_id')
            reason = data.get('reason', '')
        
            if not restaurant_id:
                return jsonify({'success': False, 'error': 'Restaurant ID required'}), 400
        
            # Find restaurant
            restaurant = None
            for r in get_current_org_restaurants():
                if str((r or {}).get('id') or '') == str(restaurant_id or ''):
                    restaurant = r
                    break
        
            if not restaurant:
                return jsonify({'success': False, 'error': 'Restaurant not found'}), 404
        
            # Add to cancelled list
            cancelled_entry = {
                'id': restaurant_id,
                'name': restaurant.get('name'),
                'manager': restaurant.get('manager'),
                'reason': reason,
                'cancelled_at': datetime.now().isoformat()
            }
        
            # Check if already cancelled
            for c in CANCELLED_RESTAURANTS:
                if c['id'] == restaurant_id:
                    return jsonify({'success': False, 'error': 'Restaurant already cancelled'}), 400
        
            CANCELLED_RESTAURANTS.append(cancelled_entry)
        
            # Remove from active restaurants
            with _GLOBAL_STATE_LOCK:
                RESTAURANTS_DATA = [
                    r for r in get_current_org_restaurants()
                    if str((r or {}).get('id') or '') != str(restaurant_id or '')
                ]
        
            return jsonify({
                'success': True,
                'message': f'Restaurant {restaurant.get("name")} cancelled',
                'cancelled': cancelled_entry
            })
        
        except Exception as e:
            print(f"Error cancelling restaurant: {e}")
            return internal_error_response()

    @bp.route('/api/comparativo/cancelled/<restaurant_id>', methods=['DELETE'])
    @admin_required
    @require_feature('comparativo')
    def api_restore_restaurant(restaurant_id):
        """Restore a cancelled restaurant"""
        try:
            global CANCELLED_RESTAURANTS
        
            # Find in cancelled list
            cancelled = None
            for c in CANCELLED_RESTAURANTS:
                if c['id'] == restaurant_id:
                    cancelled = c
                    break
        
            if not cancelled:
                return jsonify({'success': False, 'error': 'Cancelled restaurant not found'}), 404
        
            # Remove from cancelled list
            CANCELLED_RESTAURANTS = [c for c in CANCELLED_RESTAURANTS if c['id'] != restaurant_id]
        
            # Reload current org data to get the restaurant back
            org_id = get_current_org_id()
            if org_id:
                api = _init_org_ifood(org_id)
                if api:
                    _load_org_restaurants(org_id)
        
            return jsonify({
                'success': True,
                'message': 'Restaurant restored successfully'
            })
        
        except Exception as e:
            print(f"Error restoring restaurant: {e}")
            return internal_error_response()

