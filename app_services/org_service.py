"""Organization service helpers."""


def _parse_features(features_raw, json_mod):
    features = features_raw or []
    if isinstance(features, str):
        try:
            features = json_mod.loads(features)
        except Exception:
            features = []
    if not isinstance(features, list):
        return []
    return features


def build_org_capabilities_payload(*, org_id, db, json_mod):
    """Build payload for /api/org/capabilities."""
    details = db.get_org_details(org_id) or {}
    features = _parse_features(details.get('features'), json_mod)

    restaurant_limit = db.check_restaurant_limit(org_id)
    user_count = len(db.get_org_users(org_id))
    max_users = int(details.get('max_users') or 0)
    restaurant_current = int(restaurant_limit.get('current') or 0)
    restaurant_max = int(restaurant_limit.get('max') or 0)

    users_pct = (user_count / max_users * 100) if max_users > 0 else 0
    restaurants_pct = (restaurant_current / restaurant_max * 100) if restaurant_max > 0 else 0
    near_limit = users_pct >= 80 or restaurants_pct >= 80

    return {
        'success': True,
        'plan': details.get('plan', 'free'),
        'plan_display': details.get('plan_display', 'Gratuito'),
        'subscription': db.get_org_subscription(org_id),
        'features': features,
        'limits': {
            'users': {
                'current': user_count,
                'max': max_users,
                'usage_pct': round(users_pct, 1)
            },
            'restaurants': {
                'current': restaurant_current,
                'max': restaurant_max,
                'usage_pct': round(restaurants_pct, 1)
            }
        },
        'health': {
            'near_limit': near_limit,
            'users_near_limit': users_pct >= 80,
            'restaurants_near_limit': restaurants_pct >= 80
        }
    }
