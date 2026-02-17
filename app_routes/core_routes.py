"""Core page and API route registrations extracted from dashboardserver."""

from flask import Blueprint

from app_routes.dependencies import bind_dependencies

REQUIRED_DEPS = [
    'APP_STARTED_AT',
    'DASHBOARD_OUTPUT',
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
    'ORG_DATA',
    'Response',
    'USE_REDIS_QUEUE',
    '_GLOBAL_STATE_LOCK',
    '_aggregate_daily',
    '_calculate_period_metrics',
    '_extract_ifood_events_from_payload',
    '_extract_merchant_id_from_poll_event',
    '_extract_org_merchant_ids',
    '_filter_orders_by_date',
    '_find_org_ids_for_merchant_id',
    '_get_org_api_client_for_ingestion',
    '_get_squads_schema_flags',
    '_group_events_by_merchant',
    '_init_org_ifood',
    '_iso_utc_now',
    '_load_org_restaurants',
    '_org_data_items_snapshot',
    '_org_data_values_snapshot',
    '_persist_org_restaurants_cache',
    '_process_ifood_events_for_merchant',
    '_save_data_snapshot',
    '_snapshot_ifood_evidence_entries',
    '_snapshot_ifood_ingestion_metrics',
    '_squad_belongs_to_org',
    '_table_has_org_id',
    '_update_ifood_ingestion_metrics',
    '_verify_ifood_webhook_request',
    'admin_page_required',
    'admin_required',
    'aggregate_dashboard_summary',
    'bg_refresher',
    'build_data_quality_payload',
    'build_onboarding_state',
    'datetime',
    'db',
    'enqueue_refresh_job',
    'ensure_csrf_token',
    'ensure_restaurant_orders_cache',
    'escape_html_text',
    'filter_orders_by_month',
    'find_restaurant_by_identifier',
    'get_cached_dashboard_summary',
    'get_current_org_id',
    'get_current_org_last_refresh',
    'get_current_org_restaurants',
    'get_json_payload',
    'get_org_data',
    'get_public_base_url',
    'get_redis_client',
    'get_refresh_status',
    'get_resilient_api_client',
    'get_user_allowed_restaurant_ids',
    'internal_error_response',
    'invalidate_cache',
    'is_platform_admin_user',
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
    'redirect',
    'request',
    'require_feature',
    'safe_json_for_script',
    'sanitize_merchant_name',
    'send_file',
    'session',
    'set_cached_dashboard_summary',
    'sse_manager',
    'stream_with_context',
    'threading',
    'timedelta',
    'url_for',
    'uuid',
]


def register(app, deps):
    bind_dependencies(globals(), deps, REQUIRED_DEPS)
    bp = Blueprint('core_routes', __name__)

    # BEGIN: explicit linter-friendly dependency bindings
    # These aliases are runtime no-ops, but keep IDE static analysis accurate.
    APP_STARTED_AT = globals()['APP_STARTED_AT']
    DASHBOARD_OUTPUT = globals()['DASHBOARD_OUTPUT']
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
    ORG_DATA = globals()['ORG_DATA']
    Response = globals()['Response']
    USE_REDIS_QUEUE = globals()['USE_REDIS_QUEUE']
    _GLOBAL_STATE_LOCK = globals()['_GLOBAL_STATE_LOCK']
    _aggregate_daily = globals()['_aggregate_daily']
    _calculate_period_metrics = globals()['_calculate_period_metrics']
    _extract_ifood_events_from_payload = globals()['_extract_ifood_events_from_payload']
    _extract_merchant_id_from_poll_event = globals()['_extract_merchant_id_from_poll_event']
    _extract_org_merchant_ids = globals()['_extract_org_merchant_ids']
    _filter_orders_by_date = globals()['_filter_orders_by_date']
    _find_org_ids_for_merchant_id = globals()['_find_org_ids_for_merchant_id']
    _get_org_api_client_for_ingestion = globals()['_get_org_api_client_for_ingestion']
    _get_squads_schema_flags = globals()['_get_squads_schema_flags']
    _group_events_by_merchant = globals()['_group_events_by_merchant']
    _init_org_ifood = globals()['_init_org_ifood']
    _iso_utc_now = globals()['_iso_utc_now']
    _load_org_restaurants = globals()['_load_org_restaurants']
    _org_data_items_snapshot = globals()['_org_data_items_snapshot']
    _org_data_values_snapshot = globals()['_org_data_values_snapshot']
    _persist_org_restaurants_cache = globals()['_persist_org_restaurants_cache']
    _process_ifood_events_for_merchant = globals()['_process_ifood_events_for_merchant']
    _save_data_snapshot = globals()['_save_data_snapshot']
    _snapshot_ifood_evidence_entries = globals()['_snapshot_ifood_evidence_entries']
    _snapshot_ifood_ingestion_metrics = globals()['_snapshot_ifood_ingestion_metrics']
    _squad_belongs_to_org = globals()['_squad_belongs_to_org']
    _table_has_org_id = globals()['_table_has_org_id']
    _update_ifood_ingestion_metrics = globals()['_update_ifood_ingestion_metrics']
    _verify_ifood_webhook_request = globals()['_verify_ifood_webhook_request']
    admin_page_required = globals()['admin_page_required']
    admin_required = globals()['admin_required']
    aggregate_dashboard_summary = globals()['aggregate_dashboard_summary']
    bg_refresher = globals()['bg_refresher']
    build_data_quality_payload = globals()['build_data_quality_payload']
    build_onboarding_state = globals()['build_onboarding_state']
    datetime = globals()['datetime']
    db = globals()['db']
    enqueue_refresh_job = globals()['enqueue_refresh_job']
    ensure_csrf_token = globals()['ensure_csrf_token']
    ensure_restaurant_orders_cache = globals()['ensure_restaurant_orders_cache']
    escape_html_text = globals()['escape_html_text']
    filter_orders_by_month = globals()['filter_orders_by_month']
    find_restaurant_by_identifier = globals()['find_restaurant_by_identifier']
    get_cached_dashboard_summary = globals()['get_cached_dashboard_summary']
    get_current_org_id = globals()['get_current_org_id']
    get_current_org_last_refresh = globals()['get_current_org_last_refresh']
    get_current_org_restaurants = globals()['get_current_org_restaurants']
    get_json_payload = globals()['get_json_payload']
    get_org_data = globals()['get_org_data']
    get_public_base_url = globals()['get_public_base_url']
    get_redis_client = globals()['get_redis_client']
    get_refresh_status = globals()['get_refresh_status']
    get_resilient_api_client = globals()['get_resilient_api_client']
    get_user_allowed_restaurant_ids = globals()['get_user_allowed_restaurant_ids']
    internal_error_response = globals()['internal_error_response']
    invalidate_cache = globals()['invalidate_cache']
    is_platform_admin_user = globals()['is_platform_admin_user']
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
    redirect = globals()['redirect']
    request = globals()['request']
    require_feature = globals()['require_feature']
    safe_json_for_script = globals()['safe_json_for_script']
    sanitize_merchant_name = globals()['sanitize_merchant_name']
    send_file = globals()['send_file']
    session = globals()['session']
    set_cached_dashboard_summary = globals()['set_cached_dashboard_summary']
    sse_manager = globals()['sse_manager']
    stream_with_context = globals()['stream_with_context']
    threading = globals()['threading']
    timedelta = globals()['timedelta']
    url_for = globals()['url_for']
    uuid = globals()['uuid']
    # END: explicit linter-friendly dependency bindings

    @bp.route('/')
    def index():
        """Redirect to login or dashboard based on session"""
        if 'user' in session:
            return redirect(url_for('dashboard'))
        return redirect(url_for('login_page'))

    @bp.route('/login')
    def login_page():
        """Serve login page"""
        login_file = DASHBOARD_OUTPUT / 'login.html'
        if login_file.exists():
            return send_file(login_file)
        return "Login page not found. Please check dashboard_output directory.", 404

    @bp.route('/invite/<token>')
    def invite_page(token):
        """Invitation landing route used in invite URLs."""
        invite_token = (token or '').strip()
        if not invite_token:
            return "Invite not found", 404

        if 'user' in session:
            result = db.accept_invite(invite_token, session['user']['id'])
            if result and result.get('success'):
                session['org_id'] = result['org_id']
                return redirect(url_for('dashboard'))
            return redirect(url_for('login_page', invite=invite_token, invite_error=1))

        return redirect(url_for('login_page', invite=invite_token))

    @bp.route('/dashboard')
    @login_required
    def dashboard():
        """Serve main dashboard page (restaurants list)"""
        # Always serve index.html for the main dashboard view
        dashboard_file = DASHBOARD_OUTPUT / 'index.html'
    
        if dashboard_file.exists():
            return send_file(dashboard_file)
        return f"Dashboard page not found: {dashboard_file}", 404

    @bp.route('/admin')
    @admin_page_required
    def admin_page():
        """Serve admin page"""
        admin_file = DASHBOARD_OUTPUT / 'admin.html'
        if admin_file.exists():
            return send_file(admin_file)
        return "Admin page not found", 404

    @bp.route('/ops')
    @platform_admin_required
    def ops_page():
        """Serve operations panel page."""
        ops_file = DASHBOARD_OUTPUT / 'ops.html'
        if ops_file.exists():
            return send_file(ops_file)
        return "Ops page not found", 404

    @bp.route('/comparativo')
    @admin_page_required
    @require_feature('comparativo')
    def comparativo_page():
        """Serve comparativo por gestor page"""
        comparativo_file = DASHBOARD_OUTPUT / 'comparativo.html'
        if comparativo_file.exists():
            return send_file(comparativo_file)
        return "Comparativo page not found", 404

    @bp.route('/hidden-stores')
    @admin_page_required
    def hidden_stores_page():
        """Serve hidden stores management page"""
        hidden_stores_file = DASHBOARD_OUTPUT / 'hidden_stores.html'
        if hidden_stores_file.exists():
            return send_file(hidden_stores_file)
        return "Hidden stores page not found", 404

    @bp.route('/squads')
    @admin_page_required
    def squads_page():
        """Serve squads management page"""
        squads_file = DASHBOARD_OUTPUT / 'squads.html'
        if squads_file.exists():
            return send_file(squads_file)
        return "Squads page not found", 404

    @bp.route('/restaurant/<restaurant_id>')
    @login_required
    def restaurant_page(restaurant_id):
        """Serve individual restaurant dashboard"""
        # Find restaurant in org data (supports alias IDs).
        restaurant = find_restaurant_by_identifier(restaurant_id)
    
        if not restaurant:
            return "Restaurant not found", 404

        # Ensure canonical merchant id is resolved before rendering template JS.
        try:
            ensure_restaurant_orders_cache(
                restaurant,
                restaurant.get('merchant_id') or restaurant.get('merchantId') or restaurant_id
            )
        except Exception:
            pass
    
        # Check if we have a template
        template_file = DASHBOARD_OUTPUT / 'restaurant_template.html'
        if template_file.exists():
            with open(template_file, 'r', encoding='utf-8') as f:
                template = f.read()
        
            resolved_id = (
                restaurant.get('_resolved_merchant_id')
                or restaurant.get('merchant_id')
                or restaurant.get('merchantId')
                or restaurant.get('id')
                or restaurant_id
            )
            # Replace placeholders with actual data
            rendered = template.replace('{{restaurant_name}}', escape_html_text(restaurant.get('name', 'Restaurante')))
            rendered = rendered.replace('{{restaurant_id}}', escape_html_text(resolved_id))
            rendered = rendered.replace('{{restaurant_manager}}', escape_html_text(restaurant.get('manager', 'Gerente')))
            rendered = rendered.replace('{{restaurant_data}}', safe_json_for_script(restaurant))
        
            return Response(rendered, mimetype='text/html')
    
        return "Restaurant template not found", 404

    @bp.route('/api/register', methods=['POST'])
    @rate_limit(limit=5, window_seconds=3600, scope='register')
    def api_register():
        """Self-service signup: create account + organization"""
        try:
            data = get_json_payload()
            if not data:
                return jsonify({'success': False, 'error': 'Payload invalido'}), 400
            email = (data.get('email') or '').strip().lower()
            password = data.get('password', '')
            full_name = (data.get('full_name') or '').strip()
            org_name = (data.get('org_name') or '').strip()
            if not all([email, password, full_name, org_name]):
                return jsonify({'success': False, 'error': 'Todos os campos sao obrigatorios'}), 400
            if len(password) < 8:
                return jsonify({'success': False, 'error': 'Senha deve ter no minimo 8 caracteres'}), 400
            result = db.register_user_and_org(email, password, full_name, org_name)
            if not result:
                return jsonify({'success': False, 'error': 'Email ja cadastrado'}), 409
            session['user'] = {
                'id': result['user_id'],
                'username': result['username'],
                'name': full_name,
                'email': email,
                'role': 'user',
                'is_platform_admin': False,
                'primary_org_id': result['org_id']
            }
            session['org_id'] = result['org_id']
            session.permanent = True
            ensure_csrf_token()
            db.log_action('user.registered', org_id=result['org_id'], user_id=result['user_id'], details={'email': email, 'org_name': org_name}, ip_address=request.remote_addr)
            return jsonify({'success': True, 'user_id': result['user_id'], 'org_id': result['org_id'], 'redirect': url_for('dashboard')})
        except Exception as e:
            print(f"Registration error: {e}")
            return jsonify({'success': False, 'error': 'Internal server error'}), 500

    @bp.route('/api/onboarding', methods=['GET'])
    @admin_required
    def api_onboarding_state():
        """Get onboarding checklist state for current organization."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403
        return jsonify({'success': True, 'onboarding': build_onboarding_state(org_id)})

    @bp.route('/api/onboarding', methods=['PATCH'])
    @admin_required
    def api_onboarding_update():
        """Patch onboarding settings for current organization."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        onboarding = (db.get_org_settings(org_id) or {}).get('onboarding') or {}
        completed_steps = set(onboarding.get('completed_steps') or [])

        step_id = (data.get('complete_step') or '').strip()
        if step_id:
            completed_steps.add(step_id)

        if bool(data.get('reset_completed')):
            completed_steps = set()

        if 'dismissed' in data:
            onboarding['dismissed'] = bool(data.get('dismissed'))

        onboarding['completed_steps'] = sorted(completed_steps)
        onboarding['updated_at'] = datetime.utcnow().isoformat()

        ok = db.update_org_settings(org_id, {'onboarding': onboarding})
        if not ok:
            return jsonify({'success': False, 'error': 'Unable to save onboarding state'}), 500

        return jsonify({'success': True, 'onboarding': build_onboarding_state(org_id)})

    @bp.route('/api/data-quality')
    @login_required
    def api_data_quality():
        """Get data-quality overview for currently visible stores."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        user = session.get('user', {})
        allowed_ids = get_user_allowed_restaurant_ids(user.get('id'), user.get('role'))
        stores = []
        for r in get_current_org_restaurants():
            if allowed_ids is not None and r.get('id') not in allowed_ids:
                continue
            stores.append(r)
        last_refresh = ORG_DATA.get(org_id, {}).get('last_refresh') or LAST_DATA_REFRESH
        payload = build_data_quality_payload(stores, reference_last_refresh=last_refresh)
        payload['last_refresh'] = last_refresh.isoformat() if isinstance(last_refresh, datetime) else None
        return jsonify({'success': True, 'quality': payload})

    @bp.route('/api/saved-views', methods=['GET'])
    @login_required
    def api_saved_views_list():
        """List saved views for the current user/org"""
        org_id = get_current_org_id()
        user = session.get('user', {})
        if not org_id or not user:
            return jsonify({'success': False}), 403

        view_type = request.args.get('view_type')
        scope_id = request.args.get('scope_id')
        if not view_type:
            return jsonify({'success': False, 'error': 'view_type is required'}), 400

        views = db.list_saved_views(org_id, user.get('id'), view_type, scope_id)
        return jsonify({'success': True, 'views': views})

    @bp.route('/api/saved-views', methods=['POST'])
    @login_required
    def api_saved_views_create():
        """Create a saved view for the current user/org"""
        org_id = get_current_org_id()
        user = session.get('user', {})
        if not org_id or not user:
            return jsonify({'success': False}), 403

        data = get_json_payload() or {}
        view_type = (data.get('view_type') or '').strip()
        name = (data.get('name') or '').strip()
        payload = data.get('payload') or {}
        scope_id = data.get('scope_id')
        is_default = bool(data.get('is_default'))

        if not view_type or not name:
            return jsonify({'success': False, 'error': 'view_type and name are required'}), 400

        new_id = db.create_saved_view(org_id, user.get('id'), view_type, name, payload, scope_id, is_default)
        if not new_id:
            return jsonify({'success': False, 'error': 'Unable to create view'}), 500

        db.log_action('saved_view.created', org_id=org_id, user_id=user.get('id'),
                      details={'view_type': view_type, 'name': name, 'scope_id': scope_id},
                      ip_address=request.remote_addr)
        return jsonify({'success': True, 'id': new_id})

    @bp.route('/api/saved-views/<int:view_id>', methods=['DELETE'])
    @login_required
    def api_saved_views_delete(view_id):
        """Delete a saved view"""
        org_id = get_current_org_id()
        user = session.get('user', {})
        if not org_id or not user:
            return jsonify({'success': False}), 403

        ok = db.delete_saved_view(org_id, user.get('id'), view_id)
        if not ok:
            return jsonify({'success': False, 'error': 'View not found'}), 404

        db.log_action('saved_view.deleted', org_id=org_id, user_id=user.get('id'),
                      details={'view_id': view_id}, ip_address=request.remote_addr)
        return jsonify({'success': True})

    @bp.route('/api/saved-views/<int:view_id>/default', methods=['POST'])
    @login_required
    def api_saved_views_set_default(view_id):
        """Set a saved view as default"""
        org_id = get_current_org_id()
        user = session.get('user', {})
        if not org_id or not user:
            return jsonify({'success': False}), 403

        ok = db.set_default_saved_view(org_id, user.get('id'), view_id)
        if not ok:
            return jsonify({'success': False, 'error': 'View not found'}), 404

        db.log_action('saved_view.set_default', org_id=org_id, user_id=user.get('id'),
                      details={'view_id': view_id}, ip_address=request.remote_addr)
        return jsonify({'success': True})

    @bp.route('/api/saved-views/<int:view_id>/share', methods=['POST'])
    @login_required
    def api_saved_view_share_create(view_id):
        """Create or rotate share link for a saved view."""
        org_id = get_current_org_id()
        user = session.get('user', {})
        if not org_id or not user:
            return jsonify({'success': False}), 403

        data = get_json_payload()
        try:
            expires_hours = int(data.get('expires_hours', 24 * 7))
        except Exception:
            expires_hours = 24 * 7
        expires_hours = max(1, min(expires_hours, 24 * 90))

        shared = db.create_saved_view_share_link(org_id, user.get('id'), view_id, expires_hours=expires_hours)
        if not shared:
            return jsonify({'success': False, 'error': 'View not found'}), 404

        share_url = f"{get_public_base_url()}/dashboard?shared_view={shared['token']}"
        return jsonify({'success': True, 'share_url': share_url, 'token': shared['token'], 'expires_at': shared['expires_at']})

    @bp.route('/api/saved-views/<int:view_id>/share', methods=['DELETE'])
    @login_required
    def api_saved_view_share_revoke(view_id):
        """Revoke share link for a saved view."""
        org_id = get_current_org_id()
        user = session.get('user', {})
        if not org_id or not user:
            return jsonify({'success': False}), 403
        ok = db.revoke_saved_view_share_link(org_id, user.get('id'), view_id)
        if not ok:
            return jsonify({'success': False, 'error': 'View not found'}), 404
        return jsonify({'success': True})

    @bp.route('/api/saved-views/share/<token>')
    @login_required
    def api_saved_view_share_resolve(token):
        """Resolve a shared saved-view token into payload."""
        shared = db.get_saved_view_by_share_token((token or '').strip())
        if not shared:
            return jsonify({'success': False, 'error': 'Shared view not found'}), 404
        return jsonify({'success': True, 'view': shared})

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

    @bp.route('/api/users')
    @admin_required
    def api_users():
        """Get users visible to current admin context."""
        try:
            current_user = session.get('user', {})
            if is_platform_admin_user(current_user):
                users = db.get_all_users()
                return jsonify({
                    'success': True,
                    'users': users
                })

            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'Organization context required'}), 403

            users = db.get_org_users(org_id)
            return jsonify({
                'success': True,
                'users': users
            })
        except Exception as e:
            print(f"Error getting users: {e}")
            return jsonify({'success': False, 'error': 'Internal server error'}), 500

    @bp.route('/api/users', methods=['POST'])
    @admin_required
    @rate_limit(limit=30, window_seconds=3600, scope='create_user')
    def api_create_user():
        """Create user; org admins create tenant users, platform admins may create global admins."""
        try:
            data = get_json_payload()
        
            current_user = session.get('user', {})
            platform_admin = is_platform_admin_user(current_user)
            username = data.get('username')
            password = data.get('password')
            full_name = data.get('full_name')
            email = data.get('email')
            role = (data.get('role') or 'user').strip().lower()
            if role not in ('user', 'admin', 'site_admin'):
                return jsonify({'success': False, 'error': 'Invalid global role'}), 400
            org_role = (data.get('org_role') or ('admin' if role in ('admin', 'site_admin') else 'viewer')).strip().lower()
            org_id = get_current_org_id()
        
            if not all([username, password, full_name]):
                return jsonify({
                    'success': False,
                    'error': 'Username, password, and full name required'
                }), 400
            if len(str(password)) < 8:
                return jsonify({
                    'success': False,
                    'error': 'Password must have at least 8 characters'
                }), 400

            if org_role not in ('owner', 'admin', 'viewer'):
                return jsonify({'success': False, 'error': 'Invalid org role'}), 400
            if org_role == 'owner':
                return jsonify({'success': False, 'error': 'Owner role cannot be assigned at creation'}), 400

            if not platform_admin:
                if not org_id:
                    return jsonify({'success': False, 'error': 'Organization context required'}), 403
                # Tenant admins cannot create global platform admins.
                role = 'user'

            if org_id:
                user_limit = db.check_user_limit(org_id)
                if not user_limit.get('allowed'):
                    return jsonify({
                        'success': False,
                        'error': 'User limit reached for current organization',
                        'code': 'user_limit_exceeded',
                        'current_users': user_limit.get('current'),
                        'max_users': user_limit.get('max')
                    }), 409
        
            user_id = db.create_user(username, password, full_name, email, role)
        
            if user_id:
                assigned_to_org = False
                assigned_role = None
                if org_id:
                    assign_result = db.assign_user_to_org(org_id, user_id, org_role)
                    if not assign_result.get('success'):
                        # Best-effort cleanup to avoid orphan account if org assignment fails.
                        cleanup_conn = db.get_connection()
                        if cleanup_conn:
                            try:
                                cleanup_cursor = cleanup_conn.cursor()
                                cleanup_cursor.execute("DELETE FROM dashboard_users WHERE id=%s", (user_id,))
                                cleanup_conn.commit()
                                cleanup_cursor.close()
                            except Exception:
                                cleanup_conn.rollback()
                            finally:
                                cleanup_conn.close()
                        return jsonify({
                            'success': False,
                            'error': assign_result.get('error', 'Failed to assign user to organization')
                        }), 400
                    assigned_to_org = True
                    assigned_role = assign_result.get('org_role')

                return jsonify({
                    'success': True,
                    'message': 'User created successfully',
                    'user_id': user_id,
                    'assigned_to_org': assigned_to_org,
                    'org_role': assigned_role
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Username already exists'
                }), 400
            
        except Exception as e:
            print(f"Error creating user: {e}")
            return jsonify({'success': False, 'error': 'Internal server error'}), 500

    @bp.route('/api/users/<int:user_id>/role', methods=['PATCH'])
    @admin_required
    def api_update_user_global_role(user_id):
        """Update a user's global role (platform/site admin only)."""
        try:
            current_user = session.get('user', {})
            if not is_platform_admin_user(current_user):
                return jsonify({'success': False, 'error': 'Platform admin access required'}), 403

            data = get_json_payload()
            role = (data.get('role') or '').strip().lower()
            if not role:
                return jsonify({'success': False, 'error': 'role is required'}), 400

            result = db.update_user_global_role(
                user_id,
                role,
                acting_user_id=current_user.get('id')
            )
            if not result.get('success'):
                code = str(result.get('error') or '')
                if code == 'user_not_found':
                    return jsonify({'success': False, 'error': 'User not found'}), 404
                if code == 'invalid_role':
                    return jsonify({'success': False, 'error': 'Invalid global role'}), 400
                if code == 'cannot_demote_last_site_admin':
                    return jsonify({'success': False, 'error': 'Cannot demote the last site admin'}), 409
                if code == 'cannot_update_own_role':
                    return jsonify({'success': False, 'error': 'You cannot change your own global role'}), 400
                return jsonify({'success': False, 'error': code or 'Failed to update global role'}), 400

            db.log_action(
                'user.global_role_updated',
                org_id=get_current_org_id(),
                user_id=current_user.get('id'),
                details={'target_user_id': user_id, 'role': result.get('role')},
                ip_address=request.remote_addr
            )

            return jsonify({
                'success': True,
                'user_id': user_id,
                'role': result.get('role'),
                'changed': bool(result.get('changed', True))
            })
        except Exception as e:
            print(f"Error updating user global role: {e}")
            return jsonify({'success': False, 'error': 'Internal server error'}), 500

    @bp.route('/api/users/<int:user_id>', methods=['DELETE'])
    @admin_required
    def api_delete_user(user_id):
        """Delete a user account (platform admin only)."""
        try:
            if not is_platform_admin_user(session.get('user', {})):
                return jsonify({'success': False, 'error': 'Platform admin access required'}), 403

            # Prevent self-deletion
            current_user_id = (session.get('user') or {}).get('id')
            if str(current_user_id or '').strip() == str(user_id):
                return jsonify({
                    'success': False,
                    'error': 'Cannot delete your own account'
                }), 400
        
            conn = db.get_connection()
            if not conn:
                return jsonify({
                    'success': False,
                    'error': 'Database connection failed'
                }), 500
        
            cursor = conn.cursor()
        
            # Check if user exists
            cursor.execute("SELECT username FROM dashboard_users WHERE id = %s", (user_id,))
            user = cursor.fetchone()
        
            if not user:
                cursor.close()
                conn.close()
                return jsonify({
                    'success': False,
                    'error': 'User not found'
                }), 404
        
            # Delete user
            cursor.execute("DELETE FROM dashboard_users WHERE id = %s", (user_id,))
            conn.commit()
            cursor.close()
            conn.close()
        
            return jsonify({
                'success': True,
                'message': f'User {user[0]} deleted successfully'
            })
        
        except Exception as e:
            print(f"Error deleting user: {e}")
            return jsonify({'success': False, 'error': 'Internal server error'}), 500

    @bp.route('/api/hidden-stores', methods=['GET'])
    @login_required
    def get_hidden_stores():
        """Get list of all hidden stores"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            conn = db.get_connection()
            cursor = conn.cursor()

            if _table_has_org_id(cursor, 'hidden_stores'):
                cursor.execute("""
                    SELECT store_id, store_name, hidden_at, hidden_by
                    FROM hidden_stores
                    WHERE org_id = %s
                    ORDER BY hidden_at DESC
                """, (org_id,))
            else:
                cursor.execute("""
                    SELECT store_id, store_name, hidden_at, hidden_by
                    FROM hidden_stores
                    ORDER BY hidden_at DESC
                """)
            hidden = cursor.fetchall()
            cursor.close()
            conn.close()
        
            hidden_list = [{
                'id': h[0],
                'name': h[1],
                'hidden_at': h[2].isoformat() if h[2] else None,
                'hidden_by': h[3]
            } for h in hidden]
        
            return jsonify({
                'success': True,
                'hidden_stores': hidden_list
            })
        except Exception as e:
            print(f"Error getting hidden stores: {e}")
            return internal_error_response()

    @bp.route('/api/stores/<store_id>/hide', methods=['POST'])
    @admin_required
    def hide_store(store_id):
        """Hide a store from the main dashboard"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            data = get_json_payload() or {}
            store_name = data.get('name', 'Unknown Store')
            hidden_by = session.get('user', {}).get('username', 'Unknown')
        
            conn = db.get_connection()
            cursor = conn.cursor()
        
            has_org_id = _table_has_org_id(cursor, 'hidden_stores')

            # Check if already hidden
            if has_org_id:
                cursor.execute(
                    "SELECT store_id FROM hidden_stores WHERE store_id = %s AND org_id = %s",
                    (store_id, org_id)
                )
            else:
                cursor.execute("SELECT store_id FROM hidden_stores WHERE store_id = %s", (store_id,))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Store already hidden'}), 400
        
            # Insert into hidden stores
            if has_org_id:
                cursor.execute("""
                    INSERT INTO hidden_stores (store_id, store_name, hidden_by, org_id)
                    VALUES (%s, %s, %s, %s)
                """, (store_id, store_name, hidden_by, org_id))
            else:
                cursor.execute("""
                    INSERT INTO hidden_stores (store_id, store_name, hidden_by)
                    VALUES (%s, %s, %s)
                """, (store_id, store_name, hidden_by))
            conn.commit()
            cursor.close()
            conn.close()
        
            return jsonify({
                'success': True,
                'message': f'Store "{store_name}" hidden successfully'
            })
        except Exception as e:
            print(f"Error hiding store: {e}")
            return internal_error_response()

    @bp.route('/api/stores/<store_id>/unhide', methods=['POST'])
    @admin_required
    def unhide_store(store_id):
        """Unhide a store and show it on the main dashboard"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            conn = db.get_connection()
            cursor = conn.cursor()
            has_org_id = _table_has_org_id(cursor, 'hidden_stores')
        
            # Get store name before deleting
            if has_org_id:
                cursor.execute(
                    "SELECT store_name FROM hidden_stores WHERE store_id = %s AND org_id = %s",
                    (store_id, org_id)
                )
            else:
                cursor.execute("SELECT store_name FROM hidden_stores WHERE store_id = %s", (store_id,))
            result = cursor.fetchone()
        
            if not result:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Store not found in hidden list'}), 404
        
            store_name = result[0]
        
            # Remove from hidden stores
            if has_org_id:
                cursor.execute(
                    "DELETE FROM hidden_stores WHERE store_id = %s AND org_id = %s",
                    (store_id, org_id)
                )
            else:
                cursor.execute("DELETE FROM hidden_stores WHERE store_id = %s", (store_id,))
            conn.commit()
            cursor.close()
            conn.close()
        
            return jsonify({
                'success': True,
                'message': f'Store "{store_name}" is now visible'
            })
        except Exception as e:
            print(f"Error unhiding store: {e}")
            return internal_error_response()

    @bp.route('/api/squads', methods=['GET'])
    @login_required
    def api_get_squads():
        """Get all squads with their members and restaurants"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            conn = db.get_connection()
            cursor = conn.cursor()

            # Check which schema we have (cached; avoids repeated information_schema hits)
            has_old_schema, has_org_id = _get_squads_schema_flags(cursor)

            if has_old_schema:
                # Old schema: id, squad_id, name, leader, members, restaurants, active, created_at
                if has_org_id:
                    cursor.execute("""
                        SELECT id, squad_id, name, leader, members, restaurants, active, created_at
                        FROM squads
                        WHERE active = true AND org_id = %s
                        ORDER BY name
                    """, (org_id,))
                else:
                    cursor.execute("""
                        SELECT id, squad_id, name, leader, members, restaurants, active, created_at
                        FROM squads
                        WHERE active = true
                        ORDER BY name
                    """)
            else:
                # New schema: id, name, description, created_at, created_by
                if has_org_id:
                    cursor.execute("""
                        SELECT id, NULL as squad_id, name, created_by as leader,
                               NULL as members, NULL as restaurants, true as active, created_at
                        FROM squads
                        WHERE org_id = %s
                        ORDER BY name
                    """, (org_id,))
                else:
                    cursor.execute("""
                        SELECT id, NULL as squad_id, name, created_by as leader,
                               NULL as members, NULL as restaurants, true as active, created_at
                        FROM squads
                        ORDER BY name
                    """)

            squads_raw = cursor.fetchall()

            squads = []
            for squad in squads_raw:
                squad_id = squad[0]

                # Parse members and restaurants from JSON text fields (old schema)
                try:
                    members_list = json.loads(squad[4]) if squad[4] else []
                except Exception:
                    members_list = []

                try:
                    restaurants_list = json.loads(squad[5]) if squad[5] else []
                except Exception:
                    restaurants_list = []

                # Get members for this squad from squad_members table
                cursor.execute("""
                    SELECT u.id, u.full_name, u.username, u.role
                    FROM squad_members sm
                    JOIN dashboard_users u ON sm.user_id = u.id
                    WHERE sm.squad_id = %s
                    ORDER BY u.full_name
                """, (squad_id,))
                members_from_table = cursor.fetchall()

                # Get restaurants for this squad from squad_restaurants table
                cursor.execute("""
                    SELECT restaurant_id, restaurant_name
                    FROM squad_restaurants
                    WHERE squad_id = %s
                    ORDER BY restaurant_name
                """, (squad_id,))
                restaurants_from_table = cursor.fetchall()

                squads.append({
                    'id': squad_id,
                    'squad_id': squad[1] or str(squad_id),
                    'name': squad[2],
                    'leader': squad[3] or '',
                    'description': '',
                    'created_at': squad[7].isoformat() if squad[7] else None,
                    'created_by': squad[3] or '',
                    'active': squad[6] if squad[6] is not None else True,
                    'members': [
                        {'id': m[0], 'name': m[1] or m[2], 'username': m[2], 'role': m[3]}
                        for m in members_from_table
                    ] if members_from_table else members_list,
                    'restaurants': [
                        {'id': r[0], 'name': r[1]}
                        for r in restaurants_from_table
                    ] if restaurants_from_table else restaurants_list
                })

            cursor.close()
            conn.close()

            return jsonify({
                'success': True,
                'squads': squads
            })

        except Exception as e:
            print(f"Error getting squads: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/squads', methods=['POST'])
    @admin_required
    def api_create_squad():
        """Create a new squad"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            data = get_json_payload()
            name = data.get('name', '').strip()
            description = data.get('description', '').strip()

            if not name:
                return jsonify({'success': False, 'error': 'Nome obrigatorio'}), 400

            created_by = session.get('user', {}).get('username', 'Unknown')

            conn = db.get_connection()
            cursor = conn.cursor()
            has_org_id = _table_has_org_id(cursor, 'squads')

            # Check if squad with same name exists
            if has_org_id:
                cursor.execute("SELECT id FROM squads WHERE name = %s AND org_id = %s", (name, org_id))
            else:
                cursor.execute("SELECT id FROM squads WHERE name = %s", (name,))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Ja existe um squad com este nome'}), 400

            # Create squad - check which schema we have (cached metadata)
            has_old_schema, _ = _get_squads_schema_flags(cursor)

            if has_old_schema:
                squad_uid = str(uuid.uuid4())[:8]
                if has_org_id:
                    cursor.execute("""
                        INSERT INTO squads (squad_id, name, leader, members, restaurants, org_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (squad_uid, name, created_by, '[]', '[]', org_id))
                else:
                    cursor.execute("""
                        INSERT INTO squads (squad_id, name, leader, members, restaurants)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id
                    """, (squad_uid, name, created_by, '[]', '[]'))
            else:
                if has_org_id:
                    cursor.execute("""
                        INSERT INTO squads (name, description, created_by, org_id)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id
                    """, (name, description, created_by, org_id))
                else:
                    cursor.execute("""
                        INSERT INTO squads (name, description, created_by)
                        VALUES (%s, %s, %s)
                        RETURNING id
                    """, (name, description, created_by))
            squad_id = cursor.fetchone()[0]
            conn.commit()

            cursor.close()
            conn.close()

            return jsonify({
                'success': True,
                'message': 'Squad criado com sucesso',
                'squad_id': squad_id
            })

        except Exception as e:
            print(f"Error creating squad: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/squads/<int:squad_id>', methods=['PUT'])
    @admin_required
    def api_update_squad(squad_id):
        """Update a squad"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            data = get_json_payload()
            name = data.get('name', '').strip()
            _description = data.get('description', '').strip()

            if not name:
                return jsonify({'success': False, 'error': 'Nome obrigatorio'}), 400

            conn = db.get_connection()
            cursor = conn.cursor()
            has_org_id = _table_has_org_id(cursor, 'squads')

            # Check if squad exists in current org
            if has_org_id:
                cursor.execute("SELECT id FROM squads WHERE id = %s AND org_id = %s", (squad_id, org_id))
            else:
                cursor.execute("SELECT id FROM squads WHERE id = %s", (squad_id,))
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

            # Check for duplicate name (excluding current squad)
            if has_org_id:
                cursor.execute(
                    "SELECT id FROM squads WHERE name = %s AND id != %s AND org_id = %s",
                    (name, squad_id, org_id)
                )
            else:
                cursor.execute("SELECT id FROM squads WHERE name = %s AND id != %s", (name, squad_id))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Ja existe outro squad com este nome'}), 400

            # Update squad - only update name since description may not exist in old schema
            if has_org_id:
                cursor.execute("UPDATE squads SET name = %s WHERE id = %s AND org_id = %s", (name, squad_id, org_id))
            else:
                cursor.execute("UPDATE squads SET name = %s WHERE id = %s", (name, squad_id))
            conn.commit()

            cursor.close()
            conn.close()

            return jsonify({
                'success': True,
                'message': 'Squad atualizado com sucesso'
            })

        except Exception as e:
            print(f"Error updating squad: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/squads/<int:squad_id>', methods=['DELETE'])
    @admin_required
    def api_delete_squad(squad_id):
        """Delete a squad"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            conn = db.get_connection()
            cursor = conn.cursor()
            has_org_id = _table_has_org_id(cursor, 'squads')

            # Check if squad exists in current org
            if has_org_id:
                cursor.execute("SELECT name FROM squads WHERE id = %s AND org_id = %s", (squad_id, org_id))
            else:
                cursor.execute("SELECT name FROM squads WHERE id = %s", (squad_id,))
            result = cursor.fetchone()
            if not result:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

            squad_name = result[0]

            # Delete squad (cascade will delete members and restaurants)
            if has_org_id:
                cursor.execute("DELETE FROM squads WHERE id = %s AND org_id = %s", (squad_id, org_id))
            else:
                cursor.execute("DELETE FROM squads WHERE id = %s", (squad_id,))
            conn.commit()

            cursor.close()
            conn.close()

            return jsonify({
                'success': True,
                'message': f'Squad "{squad_name}" excluido com sucesso'
            })

        except Exception as e:
            print(f"Error deleting squad: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/squads/<int:squad_id>/members', methods=['POST'])
    @admin_required
    def api_add_squad_members(squad_id):
        """Add members to a squad"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            data = get_json_payload()
            user_ids = data.get('user_ids', [])

            if not user_ids:
                return jsonify({'success': False, 'error': 'Nenhum usuario selecionado'}), 400

            conn = db.get_connection()
            cursor = conn.cursor()

            # Check if squad exists in current org
            if not _squad_belongs_to_org(cursor, squad_id, org_id):
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

            added_count = 0
            for user_id in user_ids:
                try:
                    cursor.execute("""
                        INSERT INTO squad_members (squad_id, user_id)
                        VALUES (%s, %s)
                        ON CONFLICT (squad_id, user_id) DO NOTHING
                    """, (squad_id, user_id))
                    if cursor.rowcount > 0:
                        added_count += 1
                except Exception as e:
                    print(f"Error adding member {user_id}: {e}")

            conn.commit()
            cursor.close()
            conn.close()

            return jsonify({
                'success': True,
                'message': f'{added_count} membro(s) adicionado(s)',
                'added_count': added_count
            })

        except Exception as e:
            print(f"Error adding squad members: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/squads/<int:squad_id>/members/<int:user_id>', methods=['DELETE'])
    @admin_required
    def api_remove_squad_member(squad_id, user_id):
        """Remove a member from a squad"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            conn = db.get_connection()
            cursor = conn.cursor()

            if not _squad_belongs_to_org(cursor, squad_id, org_id):
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

            cursor.execute("""
                DELETE FROM squad_members
                WHERE squad_id = %s AND user_id = %s
            """, (squad_id, user_id))

            if cursor.rowcount == 0:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Membro nao encontrado no squad'}), 404

            conn.commit()
            cursor.close()
            conn.close()

            return jsonify({
                'success': True,
                'message': 'Membro removido do squad'
            })

        except Exception as e:
            print(f"Error removing squad member: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/squads/<int:squad_id>/restaurants', methods=['POST'])
    @admin_required
    def api_add_squad_restaurants(squad_id):
        """Add restaurants to a squad"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            data = get_json_payload()
            restaurant_ids = data.get('restaurant_ids', [])

            if not restaurant_ids:
                return jsonify({'success': False, 'error': 'Nenhum restaurante selecionado'}), 400

            conn = db.get_connection()
            cursor = conn.cursor()

            # Check if squad exists in current org
            if not _squad_belongs_to_org(cursor, squad_id, org_id):
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

            restaurants_index = {}
            for restaurant in get_current_org_restaurants():
                if not isinstance(restaurant, dict):
                    continue
                rid = str(restaurant.get('id') or '').strip()
                if rid:
                    restaurants_index[rid] = restaurant.get('name', 'Unknown')

            added_count = 0
            seen_restaurant_ids = set()
            for restaurant_id in restaurant_ids:
                rid = str(restaurant_id or '').strip()
                if not rid or rid in seen_restaurant_ids:
                    continue
                seen_restaurant_ids.add(rid)
                restaurant_name = restaurants_index.get(rid, 'Unknown')

                try:
                    cursor.execute("""
                        INSERT INTO squad_restaurants (squad_id, restaurant_id, restaurant_name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (squad_id, restaurant_id) DO NOTHING
                    """, (squad_id, rid, restaurant_name))
                    if cursor.rowcount > 0:
                        added_count += 1
                except Exception as e:
                    print(f"Error adding restaurant {rid}: {e}")

            conn.commit()
            cursor.close()
            conn.close()

            return jsonify({
                'success': True,
                'message': f'{added_count} restaurante(s) adicionado(s)',
                'added_count': added_count
            })

        except Exception as e:
            print(f"Error adding squad restaurants: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/squads/<int:squad_id>/restaurants/<restaurant_id>', methods=['DELETE'])
    @admin_required
    def api_remove_squad_restaurant(squad_id, restaurant_id):
        """Remove a restaurant from a squad"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            conn = db.get_connection()
            cursor = conn.cursor()

            if not _squad_belongs_to_org(cursor, squad_id, org_id):
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Squad nao encontrado'}), 404

            cursor.execute("""
                DELETE FROM squad_restaurants
                WHERE squad_id = %s AND restaurant_id = %s
            """, (squad_id, restaurant_id))

            if cursor.rowcount == 0:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Restaurante nao encontrado no squad'}), 404

            conn.commit()
            cursor.close()
            conn.close()

            return jsonify({
                'success': True,
                'message': 'Restaurante removido do squad'
            })

        except Exception as e:
            print(f"Error removing squad restaurant: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/api/user/allowed-restaurants')
    @login_required
    def api_user_allowed_restaurants():
        """Get list of restaurant IDs the current user can access based on squad membership"""
        try:
            user = session.get('user', {})
            user_id = user.get('id')
            user_role = user.get('role')
        
            # Admins/site-admins see all restaurants
            if user_role in ('admin', 'site_admin'):
                return jsonify({
                    'success': True,
                    'allowed_all': True,
                    'restaurant_ids': []
                })
        
            allowed_ids = get_user_allowed_restaurant_ids(user_id, user_role)
            restaurant_ids = allowed_ids or []
        
            # If user is not in any squad, they see all restaurants (default behavior)
            if not restaurant_ids:
                return jsonify({
                    'success': True,
                    'allowed_all': True,
                    'restaurant_ids': []
                })
        
            return jsonify({
                'success': True,
                'allowed_all': False,
                'restaurant_ids': restaurant_ids
            })
        
        except Exception as e:
            print(f"Error getting allowed restaurants: {e}")
            log_exception("request_exception", e)
            return internal_error_response()

    @bp.route('/cdn-cgi/<path:path>')
    def cdn_cgi_fallback(path):
        """Fallback for Cloudflare CDN requests - prevents 404 errors"""
        return '', 204


    # Register with an empty blueprint-name override so historical endpoint
    # names (e.g. "dashboard", "login_page") remain unchanged.
    app.register_blueprint(bp, name='')
