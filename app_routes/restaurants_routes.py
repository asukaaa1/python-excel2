"""Restaurant data and interruption route registrations."""

from flask import Blueprint
from app_services import restaurants_service

def register(app, deps):
    globals().update(deps)
    bp = Blueprint('restaurants_routes', __name__)

    def _is_truthy(value):
        return str(value or '').strip().lower() in ('1', 'true', 'yes', 'on', 'sim')

    def _collect_financial_query_params():
        params = {}
        # Keep only known filters used by iFood financial APIs.
        passthrough_keys = (
            'beginDate',
            'endDate',
            'competence',
            'periodType',
            'cursor',
            'requestId',
            'status',
            'referenceDate',
            'startDateCalculation',
            'endDateCalculation',
            'from',
            'to',
        )
        for key in passthrough_keys:
            value = request.args.get(key)
            if value is not None and str(value).strip():
                params[key] = str(value).strip()

        # Common aliases used by frontend forms.
        start_alias = request.args.get('start_date')
        end_alias = request.args.get('end_date')
        if start_alias and 'beginDate' not in params:
            params['beginDate'] = str(start_alias).strip()
        if end_alias and 'endDate' not in params:
            params['endDate'] = str(end_alias).strip()

        try:
            page = request.args.get('page', type=int)
        except Exception:
            page = None
        if page is not None:
            params['page'] = max(1, int(page))

        try:
            size = request.args.get('size', type=int)
        except Exception:
            size = None
        if size is not None:
            params['size'] = max(1, min(int(size), 500))

        return params

    def _api_error_response(message, api=None):
        payload = {'success': False, 'error': message}
        last_error = getattr(api, '_last_http_error', None)
        if isinstance(last_error, dict):
            status_code = last_error.get('status')
            endpoint = last_error.get('endpoint')
            detail = str(last_error.get('detail') or '').strip()
            attempts = last_error.get('attempts')
            if status_code:
                payload['ifood_status'] = status_code
            if endpoint:
                payload['ifood_endpoint'] = endpoint
            if detail:
                payload['ifood_detail'] = detail[:400]
            if isinstance(attempts, list) and attempts:
                payload['ifood_attempts'] = attempts[-8:]
        return jsonify(payload), 502

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
            cached = get_cached_restaurants(org_id, month_filter)
            if cached:
                cached_restaurants = cached.get('restaurants') if isinstance(cached, dict) else None
                if restaurants_service.cache_has_closure_payload(cached_restaurants):
                    return jsonify(cached)
                # Drop stale cache entries that predate closure indicators.
                invalidate_cache(org_id)
        
            # Get user's allowed restaurants based on squad membership
            user = session.get('user', {})
            allowed_ids = get_user_allowed_restaurant_ids(user.get('id'), user.get('role'))
            org_last_refresh = ORG_DATA.get(org_id, {}).get('last_refresh') if org_id else LAST_DATA_REFRESH
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

                # Always hydrate raw orders when cache is missing.
                # This prevents "all months" from serving stale snapshot-only metrics.
                if not (r.get('_orders_cache') or []):
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
                            or (metrics_snapshot or {}).get('vendas')
                            or r.get('orders')
                            or 0
                        )
                    except Exception:
                        metrics_total_orders = 0
                    # Guard against stale metrics staying at zero while raw orders exist.
                    if orders_snapshot and metrics_total_orders <= 0:
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
            all_orders = ensure_restaurant_orders_cache(restaurant, merchant_lookup_id)
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
                    or (metrics_snapshot or {}).get('vendas')
                    or restaurant.get('orders')
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
                    for closure_key in ('is_closed', 'closure_reason', 'closed_until', 'active_interruptions_count'):
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
            orders = ensure_restaurant_orders_cache(restaurant, merchant_lookup_id)
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

            orders = ensure_restaurant_orders_cache(restaurant, merchant_lookup_id)
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
    # API ROUTES - MERCHANT OPENING HOURS
    # ============================================================================

    @bp.route('/api/restaurant/<restaurant_id>/opening-hours')
    @login_required
    def api_restaurant_opening_hours(restaurant_id):
        """Get opening-hours schedule for a specific restaurant."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)

            opening_hours = api.get_opening_hours(merchant_lookup_id) if hasattr(api, 'get_opening_hours') else None
            if opening_hours is None:
                return _api_error_response('Failed to fetch opening hours', api=api)

            return jsonify({
                'success': True,
                'merchant_id': merchant_lookup_id,
                'opening_hours': opening_hours
            })
        except Exception as e:
            print(f"Error getting opening hours: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/restaurant/<restaurant_id>/opening-hours', methods=['PUT'])
    @admin_required
    def api_update_restaurant_opening_hours(restaurant_id):
        """Update opening-hours schedule for a specific restaurant."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if not hasattr(api, 'update_opening_hours'):
                return jsonify({'success': False, 'error': 'Opening hours update not supported by API client'}), 501

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)

            payload = get_json_payload()
            opening_hours_payload = payload.get('opening_hours') if isinstance(payload, dict) else payload
            if opening_hours_payload is None:
                return jsonify({'success': False, 'error': 'opening_hours payload is required'}), 400

            update_result = api.update_opening_hours(merchant_lookup_id, opening_hours_payload)
            if update_result is None:
                return _api_error_response('Failed to update opening hours', api=api)

            return jsonify({
                'success': True,
                'merchant_id': merchant_lookup_id,
                'opening_hours': update_result
            })
        except Exception as e:
            print(f"Error updating opening hours: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    # ============================================================================
    # API ROUTES - FINANCIAL
    # ============================================================================

    @bp.route('/api/restaurant/<restaurant_id>/financial/sales')
    @login_required
    def api_restaurant_financial_sales(restaurant_id):
        """Get financial sales data for reconciliation."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if not hasattr(api, 'get_financial_sales'):
                return jsonify({'success': False, 'error': 'Financial sales not supported by API client'}), 501

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)
            params = _collect_financial_query_params()
            homologation = _is_truthy(request.args.get('homologation')) or _is_truthy(
                request.headers.get('x-request-homologation')
            )

            payload = api.get_financial_sales(merchant_lookup_id, params=params, homologation=homologation)
            if payload is None:
                return _api_error_response('Failed to fetch financial sales', api=api)

            return jsonify({
                'success': True,
                'merchant_id': merchant_lookup_id,
                'homologation': homologation,
                'sales': payload
            })
        except Exception as e:
            print(f"Error getting financial sales: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/restaurant/<restaurant_id>/financial/events')
    @login_required
    def api_restaurant_financial_events(restaurant_id):
        """Get financial events ledger."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if not hasattr(api, 'get_financial_events'):
                return jsonify({'success': False, 'error': 'Financial events not supported by API client'}), 501

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)
            params = _collect_financial_query_params()
            homologation = _is_truthy(request.args.get('homologation')) or _is_truthy(
                request.headers.get('x-request-homologation')
            )

            payload = api.get_financial_events(merchant_lookup_id, params=params, homologation=homologation)
            if payload is None:
                return _api_error_response('Failed to fetch financial events', api=api)

            return jsonify({
                'success': True,
                'merchant_id': merchant_lookup_id,
                'homologation': homologation,
                'financial_events': payload
            })
        except Exception as e:
            print(f"Error getting financial events: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/restaurant/<restaurant_id>/financial/reconciliation')
    @login_required
    def api_restaurant_financial_reconciliation(restaurant_id):
        """Get reconciliation data/status payload."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if not hasattr(api, 'get_financial_reconciliation'):
                return jsonify({'success': False, 'error': 'Financial reconciliation not supported by API client'}), 501

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)
            params = _collect_financial_query_params()
            homologation = _is_truthy(request.args.get('homologation')) or _is_truthy(
                request.headers.get('x-request-homologation')
            )

            payload = api.get_financial_reconciliation(merchant_lookup_id, params=params, homologation=homologation)
            if payload is None:
                return _api_error_response('Failed to fetch reconciliation data', api=api)

            return jsonify({
                'success': True,
                'merchant_id': merchant_lookup_id,
                'homologation': homologation,
                'reconciliation': payload
            })
        except Exception as e:
            print(f"Error getting reconciliation data: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/restaurant/<restaurant_id>/financial/reconciliation-on-demand', methods=['POST'])
    @admin_required
    def api_restaurant_financial_reconciliation_on_demand(restaurant_id):
        """Request reconciliation generation on demand."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if not hasattr(api, 'request_financial_reconciliation_on_demand'):
                return jsonify({'success': False, 'error': 'Reconciliation on-demand not supported by API client'}), 501

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)
            payload = get_json_payload() or {}
            if not isinstance(payload, dict):
                return jsonify({'success': False, 'error': 'Invalid payload'}), 400

            # Accept either raw financial payload keys or simple aliases.
            if payload.get('start_date') and not payload.get('beginDate'):
                payload['beginDate'] = payload.get('start_date')
            if payload.get('end_date') and not payload.get('endDate'):
                payload['endDate'] = payload.get('end_date')

            homologation = _is_truthy(payload.get('homologation')) or _is_truthy(
                request.headers.get('x-request-homologation')
            )
            payload.pop('homologation', None)

            result = api.request_financial_reconciliation_on_demand(
                merchant_lookup_id,
                payload=payload,
                homologation=homologation
            )
            if result is None:
                return _api_error_response('Failed to request reconciliation on-demand', api=api)

            return jsonify({
                'success': True,
                'merchant_id': merchant_lookup_id,
                'homologation': homologation,
                'result': result
            })
        except Exception as e:
            print(f"Error requesting reconciliation on-demand: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/restaurant/<restaurant_id>/financial/settlements')
    @login_required
    def api_restaurant_financial_settlements(restaurant_id):
        """Get settlement entries."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if not hasattr(api, 'get_financial_settlements'):
                return jsonify({'success': False, 'error': 'Financial settlements not supported by API client'}), 501

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)
            params = _collect_financial_query_params()
            homologation = _is_truthy(request.args.get('homologation')) or _is_truthy(
                request.headers.get('x-request-homologation')
            )

            payload = api.get_financial_settlements(merchant_lookup_id, params=params, homologation=homologation)
            if payload is None:
                return _api_error_response('Failed to fetch settlements', api=api)

            return jsonify({
                'success': True,
                'merchant_id': merchant_lookup_id,
                'homologation': homologation,
                'settlements': payload
            })
        except Exception as e:
            print(f"Error getting settlements: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/restaurant/<restaurant_id>/financial/anticipations')
    @login_required
    def api_restaurant_financial_anticipations(restaurant_id):
        """Get anticipation entries."""
        try:
            api = get_resilient_api_client()
            if not api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400
            if not hasattr(api, 'get_financial_anticipations'):
                return jsonify({'success': False, 'error': 'Financial anticipations not supported by API client'}), 501

            restaurant = find_restaurant_by_identifier(restaurant_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant or {}, restaurant_id)
            params = _collect_financial_query_params()
            homologation = _is_truthy(request.args.get('homologation')) or _is_truthy(
                request.headers.get('x-request-homologation')
            )

            payload = api.get_financial_anticipations(merchant_lookup_id, params=params, homologation=homologation)
            if payload is None:
                return _api_error_response('Failed to fetch anticipations', api=api)

            return jsonify({
                'success': True,
                'merchant_id': merchant_lookup_id,
                'homologation': homologation,
                'anticipations': payload
            })
        except Exception as e:
            print(f"Error getting anticipations: {e}")
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
        
            return jsonify({
                'success': True,
                'interruptions': interruptions or []
            })
        
        except Exception as e:
            print(f"Error getting interruptions: {e}")
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
            start = data.get('start')
            end = data.get('end')
            description = data.get('description', '')
        
            if not start or not end:
                return jsonify({'success': False, 'error': 'Start and end times required'}), 400
        
            # Create interruption
            result = api.create_interruption(merchant_lookup_id, start, end, description)
        
            if result:
                return jsonify({
                    'success': True,
                    'interruption': result,
                    'message': 'Interruption created successfully'
                })
            else:
                return jsonify({'success': False, 'error': 'Failed to create interruption'}), 500
        
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
        
            # Delete interruption
            success = api.delete_interruption(merchant_lookup_id, interruption_id)
        
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Interruption removed successfully'
                })
            else:
                return jsonify({'success': False, 'error': 'Failed to remove interruption'}), 500
        
        except Exception as e:
            print(f"Error deleting interruption: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    app.register_blueprint(bp)
