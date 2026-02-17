"""Core route registrations by domain."""

from app_routes.dependencies import bind_dependencies


REQUIRED_DEPS = [
    'DASHBOARD_OUTPUT',
    'LAST_DATA_REFRESH',
    'ORG_DATA',
    'Response',
    'admin_page_required',
    'admin_required',
    'build_data_quality_payload',
    'build_onboarding_state',
    'datetime',
    'db',
    'ensure_csrf_token',
    'ensure_restaurant_orders_cache',
    'escape_html_text',
    'find_restaurant_by_identifier',
    'get_current_org_id',
    'get_current_org_restaurants',
    'get_json_payload',
    'get_public_base_url',
    'get_user_allowed_restaurant_ids',
    'jsonify',
    'login_required',
    'platform_admin_required',
    'rate_limit',
    'redirect',
    'request',
    'require_feature',
    'safe_json_for_script',
    'send_file',
    'session',
    'url_for',
]


def register_routes(bp, deps):
    bind_dependencies(globals(), deps, REQUIRED_DEPS)
    # Explicit aliases keep IDE/static analysis happy.
    DASHBOARD_OUTPUT = globals()['DASHBOARD_OUTPUT']
    LAST_DATA_REFRESH = globals()['LAST_DATA_REFRESH']
    ORG_DATA = globals()['ORG_DATA']
    Response = globals()['Response']
    admin_page_required = globals()['admin_page_required']
    admin_required = globals()['admin_required']
    build_data_quality_payload = globals()['build_data_quality_payload']
    build_onboarding_state = globals()['build_onboarding_state']
    datetime = globals()['datetime']
    db = globals()['db']
    ensure_csrf_token = globals()['ensure_csrf_token']
    ensure_restaurant_orders_cache = globals()['ensure_restaurant_orders_cache']
    escape_html_text = globals()['escape_html_text']
    find_restaurant_by_identifier = globals()['find_restaurant_by_identifier']
    get_current_org_id = globals()['get_current_org_id']
    get_current_org_restaurants = globals()['get_current_org_restaurants']
    get_json_payload = globals()['get_json_payload']
    get_public_base_url = globals()['get_public_base_url']
    get_user_allowed_restaurant_ids = globals()['get_user_allowed_restaurant_ids']
    jsonify = globals()['jsonify']
    login_required = globals()['login_required']
    platform_admin_required = globals()['platform_admin_required']
    rate_limit = globals()['rate_limit']
    redirect = globals()['redirect']
    request = globals()['request']
    require_feature = globals()['require_feature']
    safe_json_for_script = globals()['safe_json_for_script']
    send_file = globals()['send_file']
    session = globals()['session']
    url_for = globals()['url_for']

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

