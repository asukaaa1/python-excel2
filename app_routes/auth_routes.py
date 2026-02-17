"""Authentication route registrations."""

from flask import Blueprint
from app_routes.dependencies import bind_dependencies


REQUIRED_DEPS = [
    'db',
    'ensure_csrf_token',
    'get_current_org_id',
    'get_json_payload',
    'is_platform_admin_user',
    'jsonify',
    'log_exception',
    'login_required',
    'rate_limit',
    'session',
    'url_for',
]

def register(app, deps):
    bind_dependencies(globals(), deps, REQUIRED_DEPS)
    bp = Blueprint('auth_routes', __name__)

    @bp.route('/api/login', methods=['POST'])
    @rate_limit(limit=10, window_seconds=60, scope='login')
    def api_login():
        """Handle login requests"""
        try:
            data = get_json_payload()
            if not data:
                return jsonify({'success': False, 'error': 'Invalid JSON payload'}), 400
            email = data.get('email', '').strip()
            password = data.get('password', '')
        
            if not email or not password:
                return jsonify({
                    'success': False,
                    'error': 'Email and password required'
                }), 400
        
            # Authenticate using email
            user = db.authenticate_user_by_email(email, password)
        
            if user:
                user['is_platform_admin'] = bool(db.is_platform_admin(user.get('id')))
                session['user'] = user
                session.permanent = True
                ensure_csrf_token()
            
                # Set org context
                orgs = db.get_user_orgs(user['id'])
                if orgs:
                    session['org_id'] = orgs[0]['id']
                    session['org_name'] = orgs[0]['name']
                    session['org_plan'] = orgs[0]['plan']
                elif user.get('is_platform_admin'):
                    all_orgs = db.list_all_organizations()
                    if all_orgs:
                        first_org = all_orgs[0]
                        session['org_id'] = first_org.get('id')
                        session['org_name'] = first_org.get('name')
                        session['org_plan'] = first_org.get('plan')
            
                redirect_url = url_for('dashboard')
            
                return jsonify({
                    'success': True,
                    'user': user,
                    'redirect': redirect_url
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Invalid email or password'
                }), 401
            
        except Exception as e:
            print(f"Login error: {e}")
            log_exception("request_exception", e)
            return jsonify({
                'success': False,
                'error': 'Server error during login'
            }), 500


    @bp.route('/api/logout', methods=['POST'])
    def api_logout():
        """Logout user and clear session"""
        session.clear()
        return jsonify({'success': True, 'message': 'Logged out successfully'})


    @bp.route('/api/me')
    @login_required
    def api_me():
        """Get current user info with org context"""
        user = session.get('user') or {}
        org_id = get_current_org_id()
        org_info = None
        org_role = None
        if org_id:
            org_info = db.get_org_details(org_id)
            org_role = db.get_org_member_role(org_id, user.get('id'))

        user_payload = dict(user)
        user_payload['is_platform_admin'] = bool(is_platform_admin_user(user))
        if user_payload.get('is_platform_admin'):
            # Keep legacy frontend role checks working for global admins.
            user_payload['role'] = 'admin'
        user_payload['org_role'] = org_role
        if (not user_payload.get('is_platform_admin')) and org_role in ('owner', 'admin'):
            user_payload['role'] = 'admin'

        return jsonify({
            'success': True,
            'user': user_payload,
            'org_id': org_id,
            'org': org_info,
            'csrf_token': ensure_csrf_token()
        })


    # ============================================================================
    # SaaS API ROUTES
    # ============================================================================


    app.register_blueprint(bp)
