"""Core route registrations by domain."""

from app_routes.dependencies import bind_dependencies


REQUIRED_DEPS = [
    '_get_squads_schema_flags',
    '_squad_belongs_to_org',
    '_table_has_org_id',
    'admin_required',
    'db',
    'get_current_org_id',
    'get_current_org_restaurants',
    'get_json_payload',
    'get_user_allowed_restaurant_ids',
    'internal_error_response',
    'is_platform_admin_user',
    'json',
    'jsonify',
    'log_exception',
    'login_required',
    'rate_limit',
    'request',
    'session',
    'uuid',
]


def register_routes(bp, deps):
    bind_dependencies(globals(), deps, REQUIRED_DEPS)
    # Explicit aliases keep IDE/static analysis happy.
    _get_squads_schema_flags = globals()['_get_squads_schema_flags']
    _squad_belongs_to_org = globals()['_squad_belongs_to_org']
    _table_has_org_id = globals()['_table_has_org_id']
    admin_required = globals()['admin_required']
    db = globals()['db']
    get_current_org_id = globals()['get_current_org_id']
    get_current_org_restaurants = globals()['get_current_org_restaurants']
    get_json_payload = globals()['get_json_payload']
    get_user_allowed_restaurant_ids = globals()['get_user_allowed_restaurant_ids']
    internal_error_response = globals()['internal_error_response']
    is_platform_admin_user = globals()['is_platform_admin_user']
    json = globals()['json']
    jsonify = globals()['jsonify']
    log_exception = globals()['log_exception']
    login_required = globals()['login_required']
    rate_limit = globals()['rate_limit']
    request = globals()['request']
    session = globals()['session']
    uuid = globals()['uuid']

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

