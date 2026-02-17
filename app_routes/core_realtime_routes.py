"""Core route registrations by domain."""

from app_routes.dependencies import bind_dependencies


REQUIRED_DEPS = [
    'APP_STARTED_AT',
    'IFOOD_API',
    'IFOOD_CONFIG',
    'IFOOD_EVIDENCE_LOG_FILE',
    'IFOOD_EVIDENCE_MAX_ENTRIES',
    'IFOOD_KEEPALIVE_POLLING',
    'IFOOD_POLL_INTERVAL_SECONDS',
    'IFOOD_STRICT_30S_POLLING',
    'IFOOD_WEBHOOK_ALLOW_UNSIGNED',
    'IFOOD_WEBHOOK_SECRET',
    'IFOOD_WEBHOOK_TOKEN',
    'IFoodDataProcessor',
    'LAST_DATA_REFRESH',
    'RESTAURANTS_DATA',
    'Response',
    'USE_REDIS_QUEUE',
    '_extract_ifood_events_from_payload',
    '_extract_merchant_id_from_poll_event',
    '_extract_org_merchant_ids',
    '_find_org_ids_for_merchant_id',
    '_get_org_api_client_for_ingestion',
    '_group_events_by_merchant',
    '_init_org_ifood',
    '_iso_utc_now',
    '_org_data_items_snapshot',
    '_org_data_values_snapshot',
    '_persist_org_restaurants_cache',
    '_process_ifood_events_for_merchant',
    '_save_data_snapshot',
    '_snapshot_ifood_evidence_entries',
    '_snapshot_ifood_ingestion_metrics',
    '_update_ifood_ingestion_metrics',
    '_verify_ifood_webhook_request',
    'admin_required',
    'aggregate_dashboard_summary',
    'app',
    'bg_refresher',
    'datetime',
    'db',
    'enqueue_refresh_job',
    'filter_orders_by_month',
    'get_cached_dashboard_summary',
    'get_current_org_id',
    'get_current_org_last_refresh',
    'get_current_org_restaurants',
    'get_org_data',
    'get_redis_client',
    'get_refresh_status',
    'internal_error_response',
    'invalidate_cache',
    'json',
    'jsonify',
    'log_exception',
    'login_required',
    'month_filter_label',
    'normalize_merchant_id',
    'os',
    'parse_month_filter',
    'platform_admin_required',
    'queue',
    'rate_limit',
    'request',
    'session',
    'set_cached_dashboard_summary',
    'sse_manager',
    'stream_with_context',
    'threading',
]


def register_routes(bp, deps):
    bind_dependencies(globals(), deps, REQUIRED_DEPS)
    # Explicit aliases keep IDE/static analysis happy.
    APP_STARTED_AT = globals()['APP_STARTED_AT']
    IFOOD_API = globals()['IFOOD_API']
    IFOOD_CONFIG = globals()['IFOOD_CONFIG']
    IFOOD_EVIDENCE_LOG_FILE = globals()['IFOOD_EVIDENCE_LOG_FILE']
    IFOOD_EVIDENCE_MAX_ENTRIES = globals()['IFOOD_EVIDENCE_MAX_ENTRIES']
    IFOOD_KEEPALIVE_POLLING = globals()['IFOOD_KEEPALIVE_POLLING']
    IFOOD_POLL_INTERVAL_SECONDS = globals()['IFOOD_POLL_INTERVAL_SECONDS']
    IFOOD_STRICT_30S_POLLING = globals()['IFOOD_STRICT_30S_POLLING']
    IFOOD_WEBHOOK_ALLOW_UNSIGNED = globals()['IFOOD_WEBHOOK_ALLOW_UNSIGNED']
    IFOOD_WEBHOOK_SECRET = globals()['IFOOD_WEBHOOK_SECRET']
    IFOOD_WEBHOOK_TOKEN = globals()['IFOOD_WEBHOOK_TOKEN']
    IFoodDataProcessor = globals()['IFoodDataProcessor']
    LAST_DATA_REFRESH = globals()['LAST_DATA_REFRESH']
    RESTAURANTS_DATA = globals()['RESTAURANTS_DATA']
    Response = globals()['Response']
    USE_REDIS_QUEUE = globals()['USE_REDIS_QUEUE']
    _extract_ifood_events_from_payload = globals()['_extract_ifood_events_from_payload']
    _extract_merchant_id_from_poll_event = globals()['_extract_merchant_id_from_poll_event']
    _extract_org_merchant_ids = globals()['_extract_org_merchant_ids']
    _find_org_ids_for_merchant_id = globals()['_find_org_ids_for_merchant_id']
    _get_org_api_client_for_ingestion = globals()['_get_org_api_client_for_ingestion']
    _group_events_by_merchant = globals()['_group_events_by_merchant']
    _init_org_ifood = globals()['_init_org_ifood']
    _iso_utc_now = globals()['_iso_utc_now']
    _org_data_items_snapshot = globals()['_org_data_items_snapshot']
    _org_data_values_snapshot = globals()['_org_data_values_snapshot']
    _persist_org_restaurants_cache = globals()['_persist_org_restaurants_cache']
    _process_ifood_events_for_merchant = globals()['_process_ifood_events_for_merchant']
    _save_data_snapshot = globals()['_save_data_snapshot']
    _snapshot_ifood_evidence_entries = globals()['_snapshot_ifood_evidence_entries']
    _snapshot_ifood_ingestion_metrics = globals()['_snapshot_ifood_ingestion_metrics']
    _update_ifood_ingestion_metrics = globals()['_update_ifood_ingestion_metrics']
    _verify_ifood_webhook_request = globals()['_verify_ifood_webhook_request']
    admin_required = globals()['admin_required']
    aggregate_dashboard_summary = globals()['aggregate_dashboard_summary']
    app = globals()['app']
    bg_refresher = globals()['bg_refresher']
    datetime = globals()['datetime']
    db = globals()['db']
    enqueue_refresh_job = globals()['enqueue_refresh_job']
    filter_orders_by_month = globals()['filter_orders_by_month']
    get_cached_dashboard_summary = globals()['get_cached_dashboard_summary']
    get_current_org_id = globals()['get_current_org_id']
    get_current_org_last_refresh = globals()['get_current_org_last_refresh']
    get_current_org_restaurants = globals()['get_current_org_restaurants']
    get_org_data = globals()['get_org_data']
    get_redis_client = globals()['get_redis_client']
    get_refresh_status = globals()['get_refresh_status']
    internal_error_response = globals()['internal_error_response']
    invalidate_cache = globals()['invalidate_cache']
    json = globals()['json']
    jsonify = globals()['jsonify']
    log_exception = globals()['log_exception']
    login_required = globals()['login_required']
    month_filter_label = globals()['month_filter_label']
    normalize_merchant_id = globals()['normalize_merchant_id']
    os = globals()['os']
    parse_month_filter = globals()['parse_month_filter']
    platform_admin_required = globals()['platform_admin_required']
    queue = globals()['queue']
    rate_limit = globals()['rate_limit']
    request = globals()['request']
    session = globals()['session']
    set_cached_dashboard_summary = globals()['set_cached_dashboard_summary']
    sse_manager = globals()['sse_manager']
    stream_with_context = globals()['stream_with_context']
    threading = globals()['threading']

    @bp.route('/api/refresh-data', methods=['POST'])
    @admin_required
    @rate_limit(limit=20, window_seconds=60, scope='refresh_data')
    def api_refresh_data():
        """Refresh restaurant data from iFood API."""
        try:
            org_id = get_current_org_id()
            if org_id:
                current_org = get_org_data(org_id)
                if not current_org.get('api'):
                    _init_org_ifood(org_id)

            has_org_api = any(od.get('api') for od in _org_data_values_snapshot())
            if not IFOOD_API and not has_org_api:
                return jsonify({'success': False, 'error': 'iFood API not configured'}), 400

            if USE_REDIS_QUEUE and get_redis_client():
                job_id = enqueue_refresh_job(trigger='api')
                if not job_id:
                    return jsonify({'success': False, 'error': 'Failed to enqueue refresh job'}), 500
                return jsonify({
                    'success': True,
                    'message': 'Refresh job queued',
                    'status': 'queued',
                    'job_id': job_id,
                    'last_refresh': LAST_DATA_REFRESH.isoformat() if LAST_DATA_REFRESH else None
                })

            if bg_refresher.is_refreshing:
                return jsonify({'success': True, 'message': 'Refresh already in progress', 'status': 'refreshing'})

            # Fallback: trigger in-process refresh.
            threading.Thread(target=bg_refresher.refresh_now, daemon=True).start()

            return jsonify({
                'success': True,
                'message': 'Refresh started in background',
                'status': 'started',
                'last_refresh': LAST_DATA_REFRESH.isoformat() if LAST_DATA_REFRESH else None
            })
        
        except Exception as e:
            print(f"Error refreshing data: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/reload', methods=['POST'])
    @login_required
    def api_reload():
        """Alias for refresh-data"""
        return api_refresh_data()

    @bp.route('/api/ifood/webhook', methods=['POST'])
    @bp.route('/ifood/webhook', methods=['POST'])
    @rate_limit(limit=240, window_seconds=60, scope='ifood_webhook')
    def api_ifood_webhook():
        """Receive iFood real-time events and feed the same pipeline used by polling."""
        _update_ifood_ingestion_metrics(webhook_requests=1)
        raw_body = request.get_data(cache=True) or b''
        is_valid, auth_reason = _verify_ifood_webhook_request(raw_body)
        if not is_valid:
            _update_ifood_ingestion_metrics(webhook_last_error_at=datetime.now().isoformat())
            status_code = 401 if auth_reason in ('missing_signature', 'invalid_signature', 'invalid_token') else 503
            return jsonify({
                'success': False,
                'error': auth_reason,
                'webhook_configured': bool(IFOOD_WEBHOOK_SECRET or IFOOD_WEBHOOK_TOKEN or IFOOD_WEBHOOK_ALLOW_UNSIGNED)
            }), status_code

        payload = request.get_json(silent=True)
        if payload is None:
            try:
                payload = json.loads(raw_body.decode('utf-8', errors='replace'))
            except Exception:
                payload = {}
        events = _extract_ifood_events_from_payload(payload)
        if not events:
            return jsonify({'success': True, 'received': 0, 'processed': 0, 'message': 'no_events'}), 202
        payload_merchant_hint_raw = None
        if isinstance(payload, dict):
            payload_merchant_hint_raw = payload.get('merchantId') or payload.get('merchant_id')
            merchant_obj = payload.get('merchant')
            if not payload_merchant_hint_raw and isinstance(merchant_obj, dict):
                payload_merchant_hint_raw = merchant_obj.get('id') or merchant_obj.get('merchantId')
        payload_merchant_hint = normalize_merchant_id(payload_merchant_hint_raw)
        if payload_merchant_hint:
            for event in events:
                if not isinstance(event, dict):
                    continue
                if not _extract_merchant_id_from_poll_event(event):
                    event['merchantId'] = payload_merchant_hint

        received = len(events)
        processed = 0
        deduplicated = 0
        persisted = 0
        cached = 0
        updated = 0
        errors = 0
        unmatched_events = 0
        changed_org_ids = set()

        grouped_by_merchant, orphan_events = _group_events_by_merchant(events)
        for merchant_id, merchant_events in grouped_by_merchant.items():
            org_ids = _find_org_ids_for_merchant_id(merchant_id)
            if not org_ids:
                unmatched_events += len(merchant_events)
                continue
            for org_id in org_ids:
                try:
                    api_client = _get_org_api_client_for_ingestion(org_id)
                    if not api_client:
                        errors += len(merchant_events)
                        continue
                    org_data = get_org_data(org_id)
                    ingest_result = _process_ifood_events_for_merchant(
                        org_id=org_id,
                        org_data=org_data,
                        api_client=api_client,
                        merchant_id=merchant_id,
                        merchant_events=merchant_events,
                        source='webhook'
                    )
                    processed += int(ingest_result.get('events_new') or 0)
                    deduplicated += int(ingest_result.get('events_deduplicated') or 0)
                    persisted += int(ingest_result.get('orders_persisted') or 0)
                    cached += int(ingest_result.get('orders_cached') or 0)
                    updated += int(ingest_result.get('orders_updated') or 0)
                    errors += int(ingest_result.get('errors') or 0)
                    if ingest_result.get('org_data_changed'):
                        changed_org_ids.add(org_id)
                except Exception:
                    errors += len(merchant_events)

        if orphan_events:
            single_target = None
            for org_id, org_data in _org_data_items_snapshot():
                if org_id is None:
                    continue
                config = org_data.get('config') if isinstance(org_data, dict) else {}
                if not isinstance(config, dict) or not config:
                    config = db.get_org_ifood_config(org_id) or {}
                    if isinstance(org_data, dict) and isinstance(config, dict):
                        org_data['config'] = config
                org_merchant_ids = _extract_org_merchant_ids(config if isinstance(config, dict) else {})
                if len(org_merchant_ids) == 1:
                    if single_target is not None:
                        single_target = None
                        break
                    single_target = (org_id, org_merchant_ids[0])
            if single_target is None:
                unmatched_events += len(orphan_events)
            else:
                org_id, merchant_id = single_target
                try:
                    api_client = _get_org_api_client_for_ingestion(org_id)
                    if api_client:
                        org_data = get_org_data(org_id)
                        ingest_result = _process_ifood_events_for_merchant(
                            org_id=org_id,
                            org_data=org_data,
                            api_client=api_client,
                            merchant_id=merchant_id,
                            merchant_events=orphan_events,
                            source='webhook'
                        )
                        processed += int(ingest_result.get('events_new') or 0)
                        deduplicated += int(ingest_result.get('events_deduplicated') or 0)
                        persisted += int(ingest_result.get('orders_persisted') or 0)
                        cached += int(ingest_result.get('orders_cached') or 0)
                        updated += int(ingest_result.get('orders_updated') or 0)
                        errors += int(ingest_result.get('errors') or 0)
                        if ingest_result.get('org_data_changed'):
                            changed_org_ids.add(org_id)
                    else:
                        errors += len(orphan_events)
                except Exception:
                    errors += len(orphan_events)

        for org_id in list(changed_org_ids):
            try:
                _persist_org_restaurants_cache(org_id, get_org_data(org_id))
                invalidate_cache(org_id)
            except Exception:
                errors += 1

        if changed_org_ids:
            try:
                _save_data_snapshot()
            except Exception:
                pass

        _update_ifood_ingestion_metrics(
            events_received=received,
            events_deduplicated=deduplicated,
            events_processed=processed,
            orders_persisted=persisted,
            orders_cached=cached,
            orders_updated=updated
        )
        if errors > 0:
            _update_ifood_ingestion_metrics(webhook_last_error_at=datetime.now().isoformat())

        return jsonify({
            'success': True,
            'received': received,
            'processed': processed,
            'deduplicated': deduplicated,
            'orders_persisted': persisted,
            'orders_cached': cached,
            'orders_updated': updated,
            'unmatched_events': unmatched_events,
            'orgs_changed': len(changed_org_ids),
            'errors': errors
        }), 202

    @bp.route('/api/events')
    @login_required
    def sse_stream():
        """SSE endpoint for real-time order tracking and data updates"""
        def event_stream(client_queue):
            # Send initial connection event
            yield f"event: connected\ndata: {json.dumps({'timestamp': datetime.now().isoformat(), 'restaurants': len(RESTAURANTS_DATA)})}\n\n"
        
            try:
                while True:
                    try:
                        message = client_queue.get(timeout=30)
                        yield message
                    except queue.Empty:
                        # Send keepalive
                        yield f": keepalive {datetime.now().isoformat()}\n\n"
            except GeneratorExit:
                sse_manager.unregister(client_queue)
    
        client_queue = sse_manager.register()
        response = Response(
            stream_with_context(event_stream(client_queue)),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'X-Accel-Buffering': 'no',  # Disable nginx buffering
                'Connection': 'keep-alive'
            }
        )
        return response

    @bp.route('/api/refresh-status')
    @login_required
    def api_refresh_status():
        """Get current refresh status and system info"""
        refresh_payload = get_refresh_status()
        refresh_status = refresh_payload.get('status')
        last_refresh = get_current_org_last_refresh()
        return jsonify({
            'success': True,
            'is_refreshing': refresh_status in ('refreshing', 'queued'),
            'refresh_status': refresh_payload,
            'last_refresh': last_refresh.isoformat() if last_refresh else None,
            'restaurant_count': len(get_current_org_restaurants()),
            'connected_clients': sse_manager.client_count,
            'refresh_interval_minutes': IFOOD_CONFIG.get('refresh_interval_minutes', 30),
            'ifood_keepalive_polling': bool(IFOOD_KEEPALIVE_POLLING),
            'ifood_poll_interval_seconds': int(IFOOD_POLL_INTERVAL_SECONDS or 30),
            'ifood_strict_30s': bool(IFOOD_STRICT_30S_POLLING),
        })

    @bp.route('/api/ifood/evidence-pack')
    @admin_required
    def api_ifood_evidence_pack():
        """Export homologation evidence pack with polling/ack request traces."""
        try:
            limit = request.args.get('limit', default=300, type=int)
        except Exception:
            limit = 300
        limit = max(1, min(limit or 300, IFOOD_EVIDENCE_MAX_ENTRIES))

        org_id_filter = request.args.get('org_id')
        include_metrics = str(request.args.get('include_metrics', '1')).strip().lower() in ('1', 'true', 'yes', 'on')

        entries = _snapshot_ifood_evidence_entries(limit=limit, org_id=org_id_filter)
        generated_at = _iso_utc_now()

        pack = {
            'success': True,
            'generated_at': generated_at,
            'pack_type': 'ifood_homologation_events_evidence',
            'filters': {
                'limit': limit,
                'org_id': org_id_filter if str(org_id_filter or '').strip() else None,
            },
            'poller_config': {
                'enabled': bool(IFOOD_KEEPALIVE_POLLING),
                'strict_30s': bool(IFOOD_STRICT_30S_POLLING),
                'interval_seconds': int(IFOOD_POLL_INTERVAL_SECONDS or 30),
            },
            'entries_count': len(entries),
            'entries': entries,
            'evidence_log_file': IFOOD_EVIDENCE_LOG_FILE or None,
        }

        if include_metrics:
            pack['ingestion_metrics'] = _snapshot_ifood_ingestion_metrics()
            pack['db_ingestion_last_24h'] = db.get_ifood_ingestion_summary(org_id=None, since_hours=24)

        filename = f"ifood-evidence-pack-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
        response_text = json.dumps(pack, ensure_ascii=False, indent=2)
        return Response(
            response_text,
            mimetype='application/json',
            headers={'Content-Disposition': f'attachment; filename=\"{filename}\"'}
        )

    @bp.route('/api/dashboard/summary')
    @login_required
    def api_dashboard_summary():
        """Return aggregate KPI summary for current org and month filter."""
        month_filter = parse_month_filter(request.args.get('month', 'all'))
        if month_filter is None:
            return jsonify({'success': False, 'error': 'Invalid month filter'}), 400
        org_id = get_current_org_id()
        last_refresh = get_current_org_last_refresh()
        last_refresh_iso = last_refresh.isoformat() if last_refresh else None
        cached_payload = get_cached_dashboard_summary(org_id, month_filter, last_refresh_iso)
        if cached_payload:
            return jsonify(cached_payload)

        restaurants = []
        for r in get_current_org_restaurants():
            orders = r.get('_orders_cache', [])
            if month_filter != 0:
                orders = filter_orders_by_month(orders, month_filter)
            if not orders:
                continue
            restaurant_data = IFoodDataProcessor.process_restaurant_data(
                {'id': r.get('id'), 'name': r.get('name', 'Restaurante'), 'merchantManager': {'name': r.get('manager', 'Gerente')}},
                orders,
                None
            )
            restaurant_data['name'] = r.get('name', 'Restaurante')
            restaurant_data['manager'] = r.get('manager', 'Gerente')
            restaurants.append(restaurant_data)
        summary = aggregate_dashboard_summary(restaurants)
        summary['last_refresh'] = last_refresh_iso
        payload = {'success': True, 'summary': summary, 'month_filter': month_filter_label(month_filter)}
        set_cached_dashboard_summary(org_id, month_filter, last_refresh_iso, payload)
        return jsonify(payload)

    @bp.route('/api/health')
    def api_health():
        """Health probe."""
        conn = db.get_connection()
        ok = bool(conn)
        if conn:
            conn.close()
        last_refresh = get_current_org_last_refresh()
        ingestion_metrics = _snapshot_ifood_ingestion_metrics()
        db_ingestion = db.get_ifood_ingestion_summary(org_id=None, since_hours=24)
        return jsonify({
            'success': ok,
            'status': 'ok' if ok else 'degraded',
            'uptime_seconds': int((datetime.utcnow() - APP_STARTED_AT).total_seconds()),
            'restaurants_loaded': len(get_current_org_restaurants()),
            'last_refresh': last_refresh.isoformat() if last_refresh else None,
            'ifood_ingestion': {
                'polling_enabled': bool(IFOOD_KEEPALIVE_POLLING),
                'poll_interval_seconds': int(IFOOD_POLL_INTERVAL_SECONDS or 30),
                'webhook_auth_mode': (
                    'hmac_sha256' if IFOOD_WEBHOOK_SECRET
                    else 'token' if IFOOD_WEBHOOK_TOKEN
                    else 'unsigned' if IFOOD_WEBHOOK_ALLOW_UNSIGNED
                    else 'disabled'
                ),
                'metrics': ingestion_metrics,
                'db_24h': db_ingestion
            }
        }), (200 if ok else 503)

    @bp.route('/api/debug/session')
    @platform_admin_required
    def api_debug_session():
        """Debug route for session cookie visibility and server cookie flags."""
        if not os.environ.get('ENABLE_SESSION_DEBUG'):
            return jsonify({'success': False, 'error': 'disabled'}), 404

        cookie_name = app.config.get('SESSION_COOKIE_NAME', 'session')
        cookie_value = request.cookies.get(cookie_name)
        return jsonify({
            'success': True,
            'has_session_cookie': cookie_value is not None,
            'session_cookie_name': cookie_name,
            'session_cookie_length': len(cookie_value) if cookie_value else 0,
            'cookie_flags': {
                'secure': app.config.get('SESSION_COOKIE_SECURE'),
                'httponly': app.config.get('SESSION_COOKIE_HTTPONLY'),
                'samesite': app.config.get('SESSION_COOKIE_SAMESITE'),
                'domain': app.config.get('SESSION_COOKIE_DOMAIN'),
                'path': app.config.get('SESSION_COOKIE_PATH'),
            },
            'session': {
                'keys': list(session.keys()),
                'permanent': session.permanent,
                'modified': session.modified,
                'new': session.new,
            },
            'request': {
                'is_secure': request.is_secure,
                'scheme': request.scheme,
                'host': request.host,
                'remote_addr': request.remote_addr,
                'forwarded_proto': request.headers.get('X-Forwarded-Proto'),
                'forwarded_host': request.headers.get('X-Forwarded-Host'),
                'forwarded_for': request.headers.get('X-Forwarded-For'),
            }
        })

    @bp.route('/cdn-cgi/<path:path>')
    def cdn_cgi_fallback(path):
        """Fallback for Cloudflare CDN requests - prevents 404 errors"""
        return '', 204

