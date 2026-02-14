"""Restaurant domain service helpers."""

CLOSED_KEYWORDS = ('closed', 'offline', 'unavailable', 'paused', 'stopped', 'fechad', 'indispon')


def to_bool_flag(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'y', 'sim')
    return False


def resolve_merchant_lookup_id(record, fallback=None):
    if not isinstance(record, dict):
        return fallback
    return (
        record.get('_resolved_merchant_id')
        or record.get('merchant_id')
        or record.get('merchantId')
        or record.get('id')
        or fallback
    )


def get_super_flag(record):
    if not isinstance(record, dict):
        return False
    return (
        to_bool_flag(record.get('isSuperRestaurant'))
        or to_bool_flag(record.get('isSuper'))
        or to_bool_flag(record.get('super'))
    )


def normalize_closure(record, *, api_client=None, extract_status_message_text=None, detect_restaurant_closure=None):
    if not isinstance(record, dict):
        return {
            'is_closed': False,
            'closure_reason': None,
            'closed_until': None,
            'active_interruptions_count': 0,
        }

    is_closed = to_bool_flag(record.get('is_closed')) or to_bool_flag(record.get('isClosed'))
    reason = record.get('closure_reason') or record.get('closureReason')
    closed_until = record.get('closed_until') or record.get('closedUntil')
    try:
        active_interruptions = int(record.get('active_interruptions_count') or record.get('activeInterruptionsCount') or 0)
    except Exception:
        active_interruptions = 0

    has_explicit = any(
        key in record
        for key in (
            'is_closed',
            'isClosed',
            'closure_reason',
            'closureReason',
            'closed_until',
            'closedUntil',
            'active_interruptions_count',
            'activeInterruptionsCount',
        )
    )

    status_field = record.get('status')
    state_candidates = [record.get('state'), record.get('operational_status')]
    status_message = ''
    if isinstance(status_field, dict):
        state_candidates.append(status_field.get('state') or status_field.get('status'))
        if callable(extract_status_message_text):
            status_message = extract_status_message_text(status_field.get('message') or status_field.get('description'))
        if not reason:
            reason = status_message or reason
    elif isinstance(status_field, str):
        state_candidates.append(status_field)

    state_raw = ' '.join(str(v or '') for v in state_candidates).strip().lower()
    if not has_explicit and not is_closed and state_raw:
        if any(token in state_raw for token in CLOSED_KEYWORDS):
            is_closed = True

    message_text = str(status_message or '').strip().lower()
    if not has_explicit and not is_closed and message_text:
        if any(token in message_text for token in CLOSED_KEYWORDS):
            is_closed = True

    if active_interruptions > 0:
        is_closed = True

    if (not has_explicit) and api_client and record.get('id') and callable(detect_restaurant_closure):
        fetched = detect_restaurant_closure(api_client, record.get('id'))
        if isinstance(fetched, dict):
            is_closed = to_bool_flag(fetched.get('is_closed')) or is_closed
            reason = fetched.get('closure_reason') or reason
            closed_until = fetched.get('closed_until') or closed_until
            try:
                active_interruptions = int(fetched.get('active_interruptions_count') or active_interruptions or 0)
            except Exception:
                pass
            if active_interruptions > 0:
                is_closed = True

    if not is_closed:
        reason = None
        closed_until = None

    return {
        'is_closed': bool(is_closed),
        'closure_reason': reason,
        'closed_until': closed_until,
        'active_interruptions_count': int(active_interruptions or 0),
    }


def cache_has_closure_payload(cached_restaurants):
    if not isinstance(cached_restaurants, list):
        return True
    for store in cached_restaurants:
        if not isinstance(store, dict):
            continue
        if (
            'is_closed' not in store
            and 'isClosed' not in store
            and 'closure_reason' not in store
            and 'active_interruptions_count' not in store
            and 'activeInterruptionsCount' not in store
        ):
            return False
    return True


def zero_numeric_metrics(metrics):
    if not isinstance(metrics, dict):
        return metrics
    for key, value in metrics.items():
        if isinstance(value, (int, float)):
            metrics[key] = 0
        elif isinstance(value, dict):
            for subkey, subvalue in value.items():
                if isinstance(subvalue, (int, float)):
                    value[subkey] = 0
    return metrics


def summarize_quality(restaurants):
    summary = {
        'store_count': len(restaurants),
        'average_score': 100.0,
        'poor_count': 0,
        'warning_count': 0,
        'good_count': 0,
        'issue_buckets': {},
    }
    if not restaurants:
        return summary

    score_sum = 0.0
    for store in restaurants:
        quality = (store or {}).get('quality') or {}
        score_sum += float(quality.get('score') or 0)
        status = quality.get('status')
        if status == 'poor':
            summary['poor_count'] += 1
        elif status == 'warning':
            summary['warning_count'] += 1
        else:
            summary['good_count'] += 1

        for issue in (quality.get('issues') or []):
            code = issue.get('code', 'unknown')
            summary['issue_buckets'][code] = summary['issue_buckets'].get(code, 0) + 1

    summary['average_score'] = round(score_sum / len(restaurants), 1)
    return summary


def filter_orders_by_date_range(orders, start_date, end_date, *, datetime_mod, normalize_order_payload=None):
    filtered = []
    for order in orders or []:
        try:
            if callable(normalize_order_payload):
                created_at = normalize_order_payload(order).get('createdAt', '')
            else:
                created_at = (order or {}).get('createdAt', '')

            if not created_at:
                continue

            order_date = datetime_mod.fromisoformat(str(created_at).replace('Z', '+00:00')).date()
            include = True
            if start_date:
                start = datetime_mod.strptime(start_date, '%Y-%m-%d').date()
                if order_date < start:
                    include = False
            if end_date:
                end = datetime_mod.strptime(end_date, '%Y-%m-%d').date()
                if order_date > end:
                    include = False
            if include:
                filtered.append(order)
        except Exception:
            continue
    return filtered


def build_reviews_payload(orders):
    reviews_list = []
    rating_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}

    for order in orders or []:
        feedback = (order or {}).get('feedback')
        if not feedback or not feedback.get('rating'):
            continue

        rating = feedback['rating']
        if rating in rating_counts:
            rating_counts[rating] += 1

        reviews_list.append({
            'rating': rating,
            'comment': feedback.get('comment'),
            'compliments': feedback.get('compliments', []),
            'complaints': feedback.get('complaints', []),
            'customer_name': (order.get('customer') or {}).get('name', 'Cliente'),
            'date': order.get('createdAt'),
            'order_id': order.get('displayId', order.get('id', '')),
        })

    total_reviews = sum(rating_counts.values())
    avg_review_rating = round(sum(k * v for k, v in rating_counts.items()) / total_reviews, 1) if total_reviews else 0
    return {
        'average_rating': avg_review_rating,
        'total_reviews': total_reviews,
        'rating_distribution': rating_counts,
        'items': sorted(reviews_list, key=lambda x: x['date'] or '', reverse=True),
    }
