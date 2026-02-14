"""Operations service helpers."""


def build_ops_summary(*,
                      org_id,
                      db,
                      get_refresh_status,
                      get_redis_client,
                      redis_refresh_queue,
                      redis_refresh_lock_key,
                      org_data,
                      last_data_refresh,
                      get_current_org_restaurants,
                      build_data_quality_payload,
                      instance_id,
                      app_started_at,
                      datetime_mod,
                      bg_refresher,
                      use_redis_queue,
                      use_redis_cache,
                      use_redis_pubsub,
                      api_cache,
                      sse_manager):
    """Build response payload for /api/ops/summary."""
    refresh_payload = get_refresh_status()
    redis_client = get_redis_client()
    queue_depth = 0
    lock_present = False
    redis_ok = bool(redis_client)
    if redis_client:
        try:
            queue_depth = int(redis_client.llen(redis_refresh_queue) or 0)
            lock_present = bool(redis_client.get(redis_refresh_lock_key))
        except Exception:
            redis_ok = False
            queue_depth = 0
            lock_present = False

    org_details = db.get_org_details(org_id) or {}
    last_refresh = org_data.get(org_id, {}).get('last_refresh') or last_data_refresh
    restaurants = get_current_org_restaurants()
    quality = build_data_quality_payload(restaurants, reference_last_refresh=last_refresh).get('summary', {})

    return {
        'success': True,
        'ops': {
            'instance_id': instance_id,
            'uptime_seconds': int((datetime_mod.utcnow() - app_started_at).total_seconds()),
            'started_at': app_started_at.isoformat(),
            'org': {
                'id': org_id,
                'name': org_details.get('name'),
                'plan': org_details.get('plan'),
                'plan_display': org_details.get('plan_display')
            },
            'refresh': {
                'status': refresh_payload.get('status'),
                'payload': refresh_payload,
                'last_refresh': last_refresh.isoformat() if isinstance(last_refresh, datetime_mod) else None,
                'is_refreshing_local': bool(bg_refresher.is_refreshing)
            },
            'queue': {
                'enabled': bool(use_redis_queue),
                'redis_connected': redis_ok,
                'pending_jobs': queue_depth,
                'lock_present': lock_present
            },
            'cache': {
                'redis_cache_enabled': bool(use_redis_cache),
                'local_keys': len(api_cache)
            },
            'realtime': {
                'redis_pubsub_enabled': bool(use_redis_pubsub),
                'connected_clients': sse_manager.client_count
            },
            'stores': {
                'count': len(restaurants),
                'quality': quality
            }
        }
    }
