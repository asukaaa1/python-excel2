"""Shared business logic for analytics/comparativo routes."""

from datetime import timedelta


def resolve_compare_periods(now, preset, args, datetime_mod):
    """Resolve period A/B boundaries for comparison endpoint."""
    if preset == 'week':
        period_b_end = now
        period_b_start = now - timedelta(days=7)
        period_a_end = period_b_start - timedelta(days=1)
        period_a_start = period_a_end - timedelta(days=6)
    elif preset == 'month':
        period_b_start = now.replace(day=1)
        period_b_end = now
        last_month_end = period_b_start - timedelta(days=1)
        period_a_start = last_month_end.replace(day=1)
        period_a_end = last_month_end
    elif preset == 'quarter':
        current_q_start_month = ((now.month - 1) // 3) * 3 + 1
        period_b_start = now.replace(month=current_q_start_month, day=1)
        period_b_end = now
        period_a_end = period_b_start - timedelta(days=1)
        prev_q_start_month = ((period_a_end.month - 1) // 3) * 3 + 1
        period_a_start = period_a_end.replace(month=prev_q_start_month, day=1)
    elif preset == 'yoy':
        period_b_end = now
        period_b_start = now - timedelta(days=30)
        period_a_start = period_b_start.replace(year=now.year - 1)
        period_a_end = period_b_end.replace(year=now.year - 1)
    else:
        period_a_start = datetime_mod.strptime(args.get('period_a_start', ''), '%Y-%m-%d')
        period_a_end = datetime_mod.strptime(args.get('period_a_end', ''), '%Y-%m-%d')
        period_b_start = datetime_mod.strptime(args.get('period_b_start', ''), '%Y-%m-%d')
        period_b_end = datetime_mod.strptime(args.get('period_b_end', ''), '%Y-%m-%d')

    return period_a_start, period_a_end, period_b_start, period_b_end


def resolve_daily_periods(now, preset, args, datetime_mod):
    """Resolve period A/B boundaries for daily overlay endpoint."""
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
        period_a_start = datetime_mod.strptime(args.get('period_a_start', ''), '%Y-%m-%d')
        period_a_end = datetime_mod.strptime(args.get('period_a_end', ''), '%Y-%m-%d')
        period_b_start = datetime_mod.strptime(args.get('period_b_start', ''), '%Y-%m-%d')
        period_b_end = datetime_mod.strptime(args.get('period_b_end', ''), '%Y-%m-%d')

    return period_a_start, period_a_end, period_b_start, period_b_end


def select_restaurants(restaurants, restaurant_id):
    """Select one or many restaurants for analytics requests."""
    if restaurant_id == 'all':
        return list(restaurants)

    selected = [
        r for r in restaurants
        if str((r or {}).get('id') or '') == str(restaurant_id or '')
    ]
    return selected


def _compute_delta(old_val, new_val):
    return {
        'absolute': round(new_val - old_val, 2),
        'percent': round(((new_val - old_val) / old_val * 100) if old_val != 0 else (100 if new_val > 0 else 0), 1),
    }


def build_period_comparison(targets, period_a_start, period_a_end, period_b_start, period_b_end,
                            filter_orders_by_date, calculate_period_metrics):
    """Build per-restaurant and aggregate period comparison payloads."""
    comparisons = []
    totals_a = {'revenue': 0, 'orders': 0, 'cancelled': 0, 'new_customers': 0, 'ticket_sum': 0}
    totals_b = {'revenue': 0, 'orders': 0, 'cancelled': 0, 'new_customers': 0, 'ticket_sum': 0}

    for restaurant in targets:
        orders = restaurant.get('_orders_cache', [])
        orders_a = filter_orders_by_date(orders, period_a_start, period_a_end)
        orders_b = filter_orders_by_date(orders, period_b_start, period_b_end)

        metrics_a = calculate_period_metrics(orders_a)
        metrics_b = calculate_period_metrics(orders_b)

        deltas = {}
        for key in metrics_a:
            if isinstance(metrics_a[key], (int, float)) and isinstance(metrics_b[key], (int, float)):
                deltas[key] = _compute_delta(metrics_a[key], metrics_b[key])

        comparisons.append({
            'restaurant_id': restaurant.get('id'),
            'restaurant_name': restaurant.get('name', 'Unknown'),
            'period_a': metrics_a,
            'period_b': metrics_b,
            'deltas': deltas,
        })

        for key in totals_a:
            totals_a[key] += metrics_a.get(key, 0)
            totals_b[key] += metrics_b.get(key, 0)

    overall_deltas = {key: _compute_delta(totals_a[key], totals_b[key]) for key in totals_a}

    totals_a['ticket'] = round(totals_a['revenue'] / totals_a['orders'], 2) if totals_a['orders'] > 0 else 0
    totals_b['ticket'] = round(totals_b['revenue'] / totals_b['orders'], 2) if totals_b['orders'] > 0 else 0

    return comparisons, totals_a, totals_b, overall_deltas


def collect_orders(restaurants, restaurant_id):
    """Collect cached orders from one or many restaurants."""
    if restaurant_id == 'all':
        all_orders = []
        for restaurant in restaurants:
            all_orders.extend(restaurant.get('_orders_cache', []))
        return all_orders

    for restaurant in restaurants:
        if str((restaurant or {}).get('id') or '') == str(restaurant_id or ''):
            return restaurant.get('_orders_cache', [])
    return []


def build_comparativo_stats(restaurants_data, org_restaurants, cancelled_restaurants):
    """Compute comparativo summary metrics."""
    total_stores = len(restaurants_data)
    stores_with_history = sum(
        1 for r in org_restaurants
        if (r.get('metrics', {}).get('vendas') or r.get('metrics', {}).get('total_pedidos') or 0) > 0
    )

    total_revenue = 0
    positive_count = 0
    negative_count = 0
    previous_revenue = 0

    for restaurant in org_restaurants:
        metrics = restaurant.get('metrics', {})
        valor_bruto = metrics.get('valor_bruto') or 0
        total_revenue += valor_bruto

        trend = (metrics.get('trends') or {}).get('vendas') or 0
        if trend > 0:
            positive_count += 1
        elif trend < 0:
            negative_count += 1

        if valor_bruto and trend != 0:
            previous_revenue += valor_bruto / (1 + trend / 100)
        else:
            previous_revenue += valor_bruto

    revenue_trend = ((total_revenue - previous_revenue) / previous_revenue * 100) if previous_revenue > 0 else 0

    return {
        'total_stores': total_stores,
        'stores_with_history': stores_with_history,
        'total_revenue': total_revenue,
        'revenue_trend': revenue_trend,
        'positive_count': positive_count,
        'negative_count': negative_count,
        'cancelled_count': len(cancelled_restaurants),
    }


def build_managers_payload(restaurants_data):
    """Group comparativo data by manager."""
    manager_map = {}
    for restaurant in restaurants_data:
        manager = restaurant.get('manager') or 'Sem Gestor'
        if manager not in manager_map:
            manager_map[manager] = {
                'name': manager,
                'restaurants': [],
                'total_revenue': 0,
                'total_orders': 0,
                'positive_count': 0,
                'negative_count': 0,
                'services': set(),
            }

        manager_data = manager_map[manager]
        manager_data['restaurants'].append({
            'id': restaurant.get('id'),
            'name': restaurant.get('name'),
            'metrics': restaurant.get('metrics', {}),
        })

        metrics = restaurant.get('metrics', {})
        manager_data['total_revenue'] += metrics.get('valor_bruto') or 0
        manager_data['total_orders'] += metrics.get('total_pedidos') or 0

        trend = (metrics.get('trends') or {}).get('vendas') or 0
        if trend > 0:
            manager_data['positive_count'] += 1
        elif trend < 0:
            manager_data['negative_count'] += 1

        for platform_name in (restaurant.get('platforms') or []):
            platform_text = str(platform_name).lower()
            if 'ifood' in platform_text:
                manager_data['services'].add('ifood')
            elif '99' in platform_text:
                manager_data['services'].add('99food')
            elif 'keeta' in platform_text:
                manager_data['services'].add('keeta')

    managers = []
    for manager_data in manager_map.values():
        manager_data['services'] = list(manager_data['services'])
        manager_data['restaurant_count'] = len(manager_data['restaurants'])
        managers.append(manager_data)
    managers.sort(key=lambda item: item['total_revenue'], reverse=True)
    return managers
