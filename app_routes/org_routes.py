"""Organization and SaaS route registrations."""

from flask import Blueprint
from app_services.org_service import build_org_capabilities_payload

def register(app, deps):
    globals().update(deps)
    bp = Blueprint('org_routes', __name__)

    @bp.route('/api/orgs', methods=['GET', 'POST'])
    @login_required
    def api_user_orgs():
        """Get or create organizations for the current user."""
        user = session.get('user') or {}
        user_id = user.get('id')
        if not user_id:
            return jsonify({'success': False, 'error': 'Not authenticated'}), 401

        platform_admin = is_platform_admin_user(user)

        if request.method == 'GET':
            if platform_admin:
                orgs = db.list_all_organizations()
            else:
                orgs = db.get_user_orgs(user_id)
            return jsonify({'success': True, 'organizations': orgs})

        data = get_json_payload()
        if not data:
            return jsonify({'success': False, 'error': 'Invalid payload'}), 400

        org_name = (data.get('org_name') or data.get('name') or '').strip()
        if not org_name:
            return jsonify({'success': False, 'error': 'Organization name is required'}), 400

        current_org_id = get_current_org_id()
        copy_users = bool(data.get('copy_users', True))
        current_org = db.get_org_details(current_org_id) if current_org_id else None

        plan = (data.get('plan') or '').strip().lower()
        if not plan:
            plan = (current_org or {}).get('plan') or 'free'

        source_members = []
        if copy_users and current_org_id:
            if not platform_admin:
                current_member_role = db.get_org_member_role(current_org_id, user_id)
                if current_member_role not in ('owner', 'admin'):
                    return jsonify({'success': False, 'error': 'Owner/admin role required to copy users'}), 403
            source_members = db.get_org_users(current_org_id)

        created = db.create_organization(
            org_name,
            user_id,
            plan=plan,
            billing_email=(user.get('email') or None)
        )
        if not created:
            return jsonify({'success': False, 'error': 'Unable to create organization'}), 400

        # Ensure in-memory org bucket exists for the new org context.
        get_org_data(created['id'])

        copied_users = 0
        copy_errors = []
        if copy_users and source_members:
            for member in source_members:
                member_id = member.get('id')
                if not member_id or member_id == user_id:
                    continue
                member_org_role = str(member.get('org_role') or 'viewer').strip().lower()
                if member_org_role not in ('owner', 'admin', 'viewer'):
                    member_org_role = 'viewer'

                assign_result = db.assign_user_to_org(created['id'], member_id, member_org_role)
                if assign_result.get('success'):
                    copied_users += 1
                    continue

                if assign_result.get('error') == 'already_member':
                    continue

                copy_errors.append({
                    'user_id': member_id,
                    'error': assign_result.get('error') or 'assign_failed'
                })

        # Switch active organization so the sidebar/current page updates immediately.
        session['org_id'] = created['id']
        session['org_name'] = created['name']
        session['org_plan'] = created['plan']
        if session.get('user'):
            session['user']['primary_org_id'] = created['id']

        db.log_action(
            'org.created',
            org_id=created['id'],
            user_id=user_id,
            details={
                'name': created['name'],
                'plan': created.get('plan'),
                'copy_users': copy_users,
                'copied_users': copied_users,
                'copy_errors': copy_errors[:10],
                'source_org_id': current_org_id
            },
            ip_address=request.remote_addr
        )

        organizations_payload = db.list_all_organizations() if platform_admin else db.get_user_orgs(user_id)

        return jsonify({
            'success': True,
            'organization': created,
            'org_id': created['id'],
            'copied_users': copied_users,
            'copy_errors': copy_errors,
            'organizations': organizations_payload
        }), 201


    @bp.route('/api/orgs/switch', methods=['POST'])
    @login_required
    def api_switch_org():
        """Switch active organization"""
        data = get_json_payload()
        if not data:
            return jsonify({'success': False, 'error': 'Payload invalido'}), 400
        org_id = data.get('org_id')
        # Normalize org_id to int if it's a numeric string
        if isinstance(org_id, str):
            org_id_str = org_id.strip()
            if org_id_str.isdigit():
                org_id = int(org_id_str)
        current_user = session.get('user', {})
        if is_platform_admin_user(current_user):
            details = db.get_org_details(org_id)
            if not details:
                return jsonify({'success': False, 'error': 'Organization not found'}), 404
            session['org_id'] = org_id
            session['org_name'] = details.get('name')
            session['org_plan'] = details.get('plan')
            return jsonify({'success': True, 'org_id': org_id})

        orgs = db.get_user_orgs(current_user['id'])
        if not any(o['id'] == org_id for o in orgs):
            return jsonify({'success': False, 'error': 'Not a member'}), 403
        session['org_id'] = org_id
        for o in orgs:
            if o['id'] == org_id:
                session['org_name'] = o['name']
                session['org_plan'] = o['plan']
        return jsonify({'success': True, 'org_id': org_id})


    @bp.route('/api/org/details')
    @login_required
    def api_org_details():
        """Get current org details"""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403
        details = db.get_org_details(org_id)
        return jsonify({'success': True, 'organization': details})


    @bp.route('/api/org/ifood-config', methods=['GET', 'POST'])
    @login_required
    @org_owner_required
    def api_org_ifood_config():
        """Get or update iFood credentials for current org"""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization'}), 403
        if request.method == 'GET':
            config = db.get_org_ifood_config(org_id) or {}
            client_id = config.get('client_id')
            client_secret = config.get('client_secret')
            merchants = config.get('merchants', []) or []

            org_has_credentials = bool((client_id or '').strip() and (client_secret or '').strip())
            org_mode = 'none'
            if org_has_credentials:
                org_mode = 'mock' if str(client_id).strip().upper() == 'MOCK_DATA_MODE' else 'live'

            # Current org API instance (if initialized for this tenant)
            org_api = None
            if org_id in ORG_DATA:
                org_api = ORG_DATA[org_id].get('api')
            org_connected = bool(org_api)

            # Legacy global fallback (single-tenant/mock mode)
            legacy_client_id = (IFOOD_CONFIG or {}).get('client_id') if isinstance(IFOOD_CONFIG, dict) else None
            legacy_mode = 'none'
            if legacy_client_id:
                legacy_mode = 'mock' if str(legacy_client_id).strip().upper() == 'MOCK_DATA_MODE' else 'live'
            legacy_available = bool(IFOOD_API and legacy_client_id)
            env_credentials_available = bool(
                str(os.environ.get('IFOOD_CLIENT_ID') or '').strip() and
                str(os.environ.get('IFOOD_CLIENT_SECRET') or '').strip()
            )

            using_legacy_fallback = (not org_has_credentials) and legacy_available
            using_env_fallback = (not org_has_credentials) and env_credentials_available
            credentials_available = bool(org_has_credentials or using_env_fallback or using_legacy_fallback)
            if not org_connected and credentials_available:
                # Opportunistic init so status reflects real connectivity (not only env presence).
                org_api = _init_org_ifood(org_id)
                org_connected = bool(org_api)

            connection_active = bool(org_connected)
            effective_mode = org_mode if org_mode != 'none' else legacy_mode
            source = 'org'
            if not org_has_credentials:
                if using_env_fallback:
                    source = 'env'
                elif using_legacy_fallback:
                    source = 'legacy'
                else:
                    source = 'none'

            masked = None
            if client_secret:
                s = client_secret
                masked = s[:4] + '****' + s[-4:] if len(s) > 8 else '****'
            return jsonify({
                'success': True,
                'config': {
                    'client_id': client_id,
                    'client_secret_masked': masked,
                    'merchants': merchants,
                    'has_credentials': org_has_credentials,
                    'credentials_available': credentials_available,
                    'connection_active': bool(connection_active),
                    'mode': effective_mode,
                    'source': source,
                    'using_env_fallback': bool(using_env_fallback),
                    'using_legacy_fallback': bool(using_legacy_fallback),
                    'use_mock_data': effective_mode == 'mock'
                }
            })
        # POST
        data = get_json_payload()
        if not data:
            return jsonify({'success': False, 'error': 'Payload invalido'}), 400
        client_id_payload = data.get('client_id') if 'client_id' in data else None
        client_secret_payload = data.get('client_secret') if 'client_secret' in data else None
        merchants_payload = data.get('merchants') if 'merchants' in data else None

        client_id_update = client_id_payload if isinstance(client_id_payload, str) and client_id_payload.strip() else None
        client_secret_update = None
        if isinstance(client_secret_payload, str) and client_secret_payload.strip() and client_secret_payload != '****':
            client_secret_update = client_secret_payload.strip()

        db.update_org_ifood_config(
            org_id,
            client_id=client_id_update,
            client_secret=client_secret_update,
            merchants=merchants_payload
        )
        db.log_action('org.ifood_config_updated', org_id=org_id, user_id=session['user']['id'], ip_address=request.remote_addr)
        # Reinitialize this org's API connection
        api = _init_org_ifood(org_id)
        if api:
            _load_org_restaurants(org_id)
        return jsonify({'success': True, 'connection_active': bool(api), 'restaurant_count': len(ORG_DATA.get(org_id, {}).get('restaurants') or [])})


    @bp.route('/api/org/invite', methods=['POST'])
    @login_required
    @org_owner_required
    @rate_limit(limit=20, window_seconds=3600, scope='org_invite')
    def api_create_invite():
        """Create a team invite"""
        org_id = get_current_org_id()
        if not org_id: return jsonify({'success': False}), 403
        data = get_json_payload()
        if not data:
            return jsonify({'success': False, 'error': 'Payload invalido'}), 400
        email = (data.get('email') or '').strip().lower()
        role = (data.get('role') or 'viewer').strip().lower()
        if role not in ('viewer', 'admin'):
            return jsonify({'success': False, 'error': 'Invalid role'}), 400
        if not email: return jsonify({'success': False, 'error': 'Email obrigatorio'}), 400
        token = db.create_invite(org_id, email, role, session['user']['id'])
        if not token: return jsonify({'success': False, 'error': 'Limite de membros atingido'}), 400
        invite_url = f"{get_public_base_url()}/invite/{token}"
        db.log_action('org.member_invited', org_id=org_id, user_id=session['user']['id'], details={'email': email, 'role': role}, ip_address=request.remote_addr)
        return jsonify({'success': True, 'invite_url': invite_url, 'token': token})


    @bp.route('/api/invite/<token>/accept', methods=['POST'])
    @login_required
    @rate_limit(limit=20, window_seconds=3600, scope='invite_accept')
    def api_accept_invite(token):
        """Accept a team invite"""
        result = db.accept_invite(token, session['user']['id'])
        if not result or not result.get('success'):
            code = (result or {}).get('error')
            if code == 'invite_email_mismatch':
                return jsonify({'success': False, 'error': 'Este convite pertence a outro e-mail'}), 403
            if code == 'invite_already_accepted':
                return jsonify({'success': False, 'error': 'Convite já foi utilizado'}), 409
            return jsonify({'success': False, 'error': 'Convite inválido ou expirado'}), 400
        session['org_id'] = result['org_id']
        return jsonify({'success': True, 'org_id': result['org_id'], 'redirect': url_for('dashboard')})


    @bp.route('/api/plans')
    def api_get_plans():
        """Get available subscription plans"""
        include_free = str(request.args.get('include_free', '')).lower() in ('1', 'true', 'yes')
        plans = db.list_active_plans(include_free=include_free)
        if not plans:
            return jsonify({'success': True, 'plans': []})
        return jsonify({'success': True, 'plans': [enrich_plan_payload(p) for p in plans]})


    @bp.route('/api/org/subscription')
    @login_required
    def api_get_org_subscription():
        """Return current org subscription details, usage and available plans."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        current_subscription = db.get_org_subscription(org_id)
        plan_options = []

        user_limit = db.check_user_limit(org_id)
        restaurant_limit = db.check_restaurant_limit(org_id)

        for plan in db.list_active_plans(include_free=False):
            plan_payload = enrich_plan_payload(plan)
            users_ok = int(user_limit.get('current') or 0) <= int(plan_payload.get('max_users') or 0)
            restaurants_ok = int(restaurant_limit.get('current') or 0) <= int(plan_payload.get('max_restaurants') or 0)
            plan_payload['eligible'] = bool(users_ok and restaurants_ok)
            plan_payload['eligibility'] = {
                'users_ok': users_ok,
                'restaurants_ok': restaurants_ok
            }
            plan_options.append(plan_payload)

        history = db.list_org_subscription_history(org_id, limit=12)

        return jsonify({
            'success': True,
            'subscription': current_subscription,
            'plans': plan_options,
            'usage': {
                'users': user_limit,
                'restaurants': restaurant_limit
            },
            'history': history
        })


    @bp.route('/api/org/subscription', methods=['POST'])
    @login_required
    @org_owner_required
    def api_change_org_subscription():
        """Change current organization plan and persist a subscription record."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        target_plan = (data.get('plan') or '').strip().lower()
        if not target_plan:
            return jsonify({'success': False, 'error': 'plan is required'}), 400

        if target_plan not in PLAN_CATALOG_UI:
            return jsonify({'success': False, 'error': 'Unsupported plan'}), 400

        reason = (data.get('reason') or 'admin_portal').strip()[:120]
        change = db.change_org_plan(org_id, target_plan, changed_by=session['user']['id'], reason=reason)
        if not change.get('success'):
            status = 409 if str(change.get('error', '')).endswith('_limit_exceeded') else 400
            return jsonify(change), status

        session['org_plan'] = target_plan
        db.log_action(
            'org.plan_changed',
            org_id=org_id,
            user_id=session['user']['id'],
            details={
                'target_plan': target_plan,
                'previous_plan': change.get('previous_plan'),
                'reason': reason,
                'usage': change.get('usage', {})
            },
            ip_address=request.remote_addr
        )

        return jsonify({
            'success': True,
            'change': change,
            'subscription': db.get_org_subscription(org_id),
            'organization': db.get_org_details(org_id)
        })


    @bp.route('/api/org/limits')
    @login_required
    def api_org_limits():
        """Get current org usage vs limits"""
        org_id = get_current_org_id()
        if not org_id: return jsonify({'success': False}), 403
        limits = db.check_restaurant_limit(org_id)
        details = db.get_org_details(org_id)
        return jsonify({'success': True, 'restaurants': limits, 'plan': details.get('plan') if details else 'free', 'plan_display': details.get('plan_display') if details else 'Gratuito'})


    @bp.route('/api/org/users')
    @login_required
    def api_org_users():
        """Get users in current org"""
        org_id = get_current_org_id()
        if not org_id: return jsonify({'success': False}), 403
        users = db.get_org_users(org_id)
        return jsonify({'success': True, 'users': users})


    @bp.route('/api/org/users/candidates')
    @login_required
    @org_owner_required
    def api_org_user_candidates():
        """List users that are not yet members of the current org."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403
        users = db.list_users_not_in_org(org_id)
        return jsonify({'success': True, 'users': users})


    @bp.route('/api/org/users/assign', methods=['POST'])
    @login_required
    @org_owner_required
    def api_org_user_assign():
        """Assign an existing user to the current organization."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        user_id = data.get('user_id')
        if isinstance(user_id, str) and user_id.strip().isdigit():
            user_id = int(user_id.strip())
        org_role = (data.get('org_role') or 'viewer').strip().lower()

        if not isinstance(user_id, int):
            return jsonify({'success': False, 'error': 'user_id is required'}), 400

        result = db.assign_user_to_org(org_id, user_id, org_role)
        if not result.get('success'):
            code = str(result.get('error') or '')
            if code in ('user_not_found', 'org_not_found'):
                status = 404
            elif code in ('already_member', 'user_limit_exceeded'):
                status = 409
            else:
                status = 400
            return jsonify(result), status

        db.log_action(
            'org.member_assigned',
            org_id=org_id,
            user_id=session['user']['id'],
            details={'assigned_user_id': user_id, 'org_role': result.get('org_role')},
            ip_address=request.remote_addr
        )
        return jsonify({'success': True, 'user_id': user_id, 'org_role': result.get('org_role')})


    @bp.route('/api/org/users/<int:user_id>/role', methods=['PATCH'])
    @login_required
    @org_owner_required
    def api_org_user_role_update(user_id):
        """Update a member role inside current organization."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        org_role = (data.get('org_role') or '').strip().lower()
        if not org_role:
            return jsonify({'success': False, 'error': 'org_role is required'}), 400

        result = db.update_org_member_role(
            org_id,
            user_id,
            org_role,
            acting_user_id=session.get('user', {}).get('id')
        )
        if not result.get('success'):
            code = str(result.get('error') or '')
            if code == 'admin_cannot_self_promote_to_owner':
                return jsonify({
                    'success': False,
                    'error': 'Admins cannot promote themselves to owner'
                }), 403
            status = 404 if code == 'member_not_found' else 400
            return jsonify(result), status

        db.log_action(
            'org.member_role_updated',
            org_id=org_id,
            user_id=session['user']['id'],
            details={'target_user_id': user_id, 'org_role': result.get('org_role')},
            ip_address=request.remote_addr
        )
        return jsonify({'success': True, 'user_id': user_id, 'org_role': result.get('org_role')})


    @bp.route('/api/org/users/<int:user_id>', methods=['DELETE'])
    @login_required
    @org_owner_required
    def api_org_user_remove(user_id):
        """Remove a user from current organization (keeps account intact)."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        if session.get('user', {}).get('id') == user_id:
            return jsonify({'success': False, 'error': 'Cannot remove your own membership from active org'}), 400

        result = db.remove_user_from_org(org_id, user_id)
        if not result.get('success'):
            code = str(result.get('error') or '')
            status = 404 if code == 'member_not_found' else 400
            return jsonify(result), status

        db.log_action(
            'org.member_removed',
            org_id=org_id,
            user_id=session['user']['id'],
            details={'removed_user_id': user_id},
            ip_address=request.remote_addr
        )
        return jsonify({'success': True, 'user_id': user_id})


    @bp.route('/api/org/capabilities')
    @login_required
    def api_org_capabilities():
        """Get tenant plan, enabled features and usage health."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False}), 403

        return jsonify(build_org_capabilities_payload(org_id=org_id, db=db, json_mod=json))


    app.register_blueprint(bp)
