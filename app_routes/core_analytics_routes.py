"""Core route registrations by domain."""

from app_routes.dependencies import bind_dependencies


REQUIRED_DEPS = [
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


def register_routes(bp, deps):
    bind_dependencies(globals(), deps, REQUIRED_DEPS)
    # Explicit aliases keep IDE/static analysis happy.
    _GLOBAL_STATE_LOCK = globals()['_GLOBAL_STATE_LOCK']
    _aggregate_daily = globals()['_aggregate_daily']
    _calculate_period_metrics = globals()['_calculate_period_metrics']
    _filter_orders_by_date = globals()['_filter_orders_by_date']
    _init_org_ifood = globals()['_init_org_ifood']
    _load_org_restaurants = globals()['_load_org_restaurants']
    admin_required = globals()['admin_required']
    datetime = globals()['datetime']
    db = globals()['db']
    get_current_org_id = globals()['get_current_org_id']
    get_current_org_restaurants = globals()['get_current_org_restaurants']
    get_json_payload = globals()['get_json_payload']
    get_org_data = globals()['get_org_data']
    get_resilient_api_client = globals()['get_resilient_api_client']
    internal_error_response = globals()['internal_error_response']
    json = globals()['json']
    jsonify = globals()['jsonify']
    log_exception = globals()['log_exception']
    login_required = globals()['login_required']
    normalize_merchant_id = globals()['normalize_merchant_id']
    platform_admin_required = globals()['platform_admin_required']
    request = globals()['request']
    require_feature = globals()['require_feature']
    sanitize_merchant_name = globals()['sanitize_merchant_name']
    timedelta = globals()['timedelta']

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
        
            now = datetime.now()
        
            if preset == 'week':
                # This week vs last week
                period_b_end = now
                period_b_start = now - timedelta(days=7)
                period_a_end = period_b_start - timedelta(days=1)
                period_a_start = period_a_end - timedelta(days=6)
            elif preset == 'month':
                # This month vs last month
                period_b_start = now.replace(day=1)
                period_b_end = now
                last_month_end = period_b_start - timedelta(days=1)
                period_a_start = last_month_end.replace(day=1)
                period_a_end = last_month_end
            elif preset == 'quarter':
                # This quarter vs last quarter
                current_q_start_month = ((now.month - 1) // 3) * 3 + 1
                period_b_start = now.replace(month=current_q_start_month, day=1)
                period_b_end = now
                period_a_end = period_b_start - timedelta(days=1)
                prev_q_start_month = ((period_a_end.month - 1) // 3) * 3 + 1
                period_a_start = period_a_end.replace(month=prev_q_start_month, day=1)
            elif preset == 'yoy':
                # Last 30 days vs same 30 days last year
                period_b_end = now
                period_b_start = now - timedelta(days=30)
                period_a_start = period_b_start.replace(year=now.year - 1)
                period_a_end = period_b_end.replace(year=now.year - 1)
            else:
                # Custom dates
                period_a_start = datetime.strptime(request.args.get('period_a_start', ''), '%Y-%m-%d')
                period_a_end = datetime.strptime(request.args.get('period_a_end', ''), '%Y-%m-%d')
                period_b_start = datetime.strptime(request.args.get('period_b_start', ''), '%Y-%m-%d')
                period_b_end = datetime.strptime(request.args.get('period_b_end', ''), '%Y-%m-%d')
        
            # Collect restaurants to compare
            if restaurant_id == 'all':
                targets = get_current_org_restaurants()
            else:
                targets = [
                    r for r in get_current_org_restaurants()
                    if str((r or {}).get('id') or '') == str(restaurant_id or '')
                ]
                if not targets:
                    return jsonify({'success': False, 'error': 'Restaurant not found'}), 404
        
            comparisons = []
            totals_a = {'revenue': 0, 'orders': 0, 'cancelled': 0, 'new_customers': 0, 'ticket_sum': 0}
            totals_b = {'revenue': 0, 'orders': 0, 'cancelled': 0, 'new_customers': 0, 'ticket_sum': 0}
        
            for restaurant in targets:
                orders = restaurant.get('_orders_cache', [])
            
                # Split orders into period A and B
                orders_a = _filter_orders_by_date(orders, period_a_start, period_a_end)
                orders_b = _filter_orders_by_date(orders, period_b_start, period_b_end)
            
                metrics_a = _calculate_period_metrics(orders_a)
                metrics_b = _calculate_period_metrics(orders_b)
            
                # Calculate deltas
                deltas = {}
                for key in metrics_a:
                    if isinstance(metrics_a[key], (int, float)) and isinstance(metrics_b[key], (int, float)):
                        old_val = metrics_a[key]
                        new_val = metrics_b[key]
                        deltas[key] = {
                            'absolute': round(new_val - old_val, 2),
                            'percent': round(((new_val - old_val) / old_val * 100) if old_val != 0 else (100 if new_val > 0 else 0), 1)
                        }
            
                comparisons.append({
                    'restaurant_id': restaurant.get('id'),
                    'restaurant_name': restaurant.get('name', 'Unknown'),
                    'period_a': metrics_a,
                    'period_b': metrics_b,
                    'deltas': deltas
                })
            
                # Accumulate totals
                for key in totals_a:
                    totals_a[key] += metrics_a.get(key, 0)
                    totals_b[key] += metrics_b.get(key, 0)
        
            # Calculate overall deltas
            overall_deltas = {}
            for key in totals_a:
                old_val = totals_a[key]
                new_val = totals_b[key]
                overall_deltas[key] = {
                    'absolute': round(new_val - old_val, 2),
                    'percent': round(((new_val - old_val) / old_val * 100) if old_val != 0 else (100 if new_val > 0 else 0), 1)
                }
        
            # Calculate averages
            totals_a['ticket'] = round(totals_a['revenue'] / totals_a['orders'], 2) if totals_a['orders'] > 0 else 0
            totals_b['ticket'] = round(totals_b['revenue'] / totals_b['orders'], 2) if totals_b['orders'] > 0 else 0
        
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
        
            now = datetime.now()
        
            if preset == 'week':
                period_b_start = now - timedelta(days=6)
                period_b_end = now
                period_a_start = period_b_start - timedelta(days=7)
                period_a_end = period_b_start - timedelta(days=1)
            elif preset == 'month':
                period_b_start = now.replace(day=1)
                period_b_end = now
                last_month_end = period_b_start - timedelta(days=1)
                period_a_start = last_month_end.replace(day=1)
                period_a_end = last_month_end
            else:
                period_a_start = datetime.strptime(request.args.get('period_a_start', ''), '%Y-%m-%d')
                period_a_end = datetime.strptime(request.args.get('period_a_end', ''), '%Y-%m-%d')
                period_b_start = datetime.strptime(request.args.get('period_b_start', ''), '%Y-%m-%d')
                period_b_end = datetime.strptime(request.args.get('period_b_end', ''), '%Y-%m-%d')
        
            # Collect orders
            all_orders = []
            if restaurant_id == 'all':
                for r in get_current_org_restaurants():
                    all_orders.extend(r.get('_orders_cache', []))
            else:
                for r in get_current_org_restaurants():
                    if str((r or {}).get('id') or '') == str(restaurant_id or ''):
                        all_orders = r.get('_orders_cache', [])
                        break
        
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
            total_stores = len(RESTAURANTS_DATA)
            stores_with_history = sum(1 for r in get_current_org_restaurants() if (r.get('metrics', {}).get('vendas') or r.get('metrics', {}).get('total_pedidos') or 0) > 0)
        
            total_revenue = 0
            positive_count = 0
            negative_count = 0
            previous_revenue = 0
        
            for r in get_current_org_restaurants():
                metrics = r.get('metrics', {})
                valor_bruto = metrics.get('valor_bruto') or 0
                total_revenue += valor_bruto
            
                trend = (metrics.get('trends') or {}).get('vendas') or 0
                if trend > 0:
                    positive_count += 1
                elif trend < 0:
                    negative_count += 1
            
                # Estimate previous revenue from trend
                if valor_bruto and trend != 0:
                    previous_revenue += valor_bruto / (1 + trend / 100)
                else:
                    previous_revenue += valor_bruto
        
            revenue_trend = ((total_revenue - previous_revenue) / previous_revenue * 100) if previous_revenue > 0 else 0
        
            return jsonify({
                'success': True,
                'stats': {
                    'total_stores': total_stores,
                    'stores_with_history': stores_with_history,
                    'total_revenue': total_revenue,
                    'revenue_trend': revenue_trend,
                    'positive_count': positive_count,
                    'negative_count': negative_count,
                    'cancelled_count': len(CANCELLED_RESTAURANTS)
                }
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
            manager_map = {}
        
            for restaurant in RESTAURANTS_DATA:
                manager = restaurant.get('manager') or 'Sem Gestor'
            
                if manager not in manager_map:
                    manager_map[manager] = {
                        'name': manager,
                        'restaurants': [],
                        'total_revenue': 0,
                        'total_orders': 0,
                        'positive_count': 0,
                        'negative_count': 0,
                        'services': set()
                    }
            
                manager_data = manager_map[manager]
                manager_data['restaurants'].append({
                    'id': restaurant.get('id'),
                    'name': restaurant.get('name'),
                    'metrics': restaurant.get('metrics', {})
                })
            
                metrics = restaurant.get('metrics', {})
                manager_data['total_revenue'] += metrics.get('valor_bruto') or 0
                manager_data['total_orders'] += metrics.get('total_pedidos') or 0
            
                trend = (metrics.get('trends') or {}).get('vendas') or 0
                if trend > 0:
                    manager_data['positive_count'] += 1
                elif trend < 0:
                    manager_data['negative_count'] += 1
            
                # Add services based on platforms
                platforms = restaurant.get('platforms') or []
                for p in platforms:
                    pl = p.lower()
                    if 'ifood' in pl:
                        manager_data['services'].add('ifood')
                    elif '99' in pl:
                        manager_data['services'].add('99food')
                    elif 'keeta' in pl:
                        manager_data['services'].add('keeta')
        
            # Convert sets to lists for JSON serialization
            managers = []
            for m in manager_map.values():
                m['services'] = list(m['services'])
                m['restaurant_count'] = len(m['restaurants'])
                managers.append(m)
        
            # Sort by revenue
            managers.sort(key=lambda x: x['total_revenue'], reverse=True)
        
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

