"""Group and public sharing route registrations."""

from flask import Blueprint
from app_services import groups_service, restaurants_service

def register(app, deps):
    globals().update(deps)
    bp = Blueprint('groups_routes', __name__)

    # Page route for grupos
    @bp.route('/grupos')
    @login_required
    def grupos_page():
        """Serve client groups management page"""
        grupos_file = DASHBOARD_OUTPUT / 'grupos.html'
        if grupos_file.exists():
            return send_file(grupos_file)
        return "Grupos page not found", 404


    @bp.route('/grupos/comparativo')
    @login_required
    def grupos_comparativo_page():
        """Serve multi-store comparison page for groups."""
        comp_file = DASHBOARD_OUTPUT / 'grupos_comparativo.html'
        if comp_file.exists():
            return send_file(comp_file)
        return "Grupos comparativo page not found", 404


    # Public group page
    @bp.route('/grupo/<slug>')
    def public_group_page(slug):
        """Serve group dashboard with token-gated public access."""
        try:
            group_token = (request.args.get('token') or '').strip()
            shared = None
            if group_token:
                shared = db.get_group_by_share_token(group_token)
                if not shared:
                    return "Shared group link not found or expired", 404
                if str(shared.get('group_slug') or '') != str(slug):
                    return "Shared group link not found or expired", 404
                if not shared.get('group_active'):
                    return "Group is inactive", 404

            conn = db.get_connection()
            cursor = conn.cursor()

            # Get group by slug
            has_group_org = _table_has_org_id(cursor, 'client_groups')
            if has_group_org:
                cursor.execute("""
                    SELECT id, name, slug, active, org_id
                    FROM client_groups
                    WHERE slug = %s AND active = true
                """, (slug,))
            else:
                cursor.execute("""
                    SELECT id, name, slug, active, NULL::INTEGER as org_id
                    FROM client_groups
                    WHERE slug = %s AND active = true
                """, (slug,))

            group = cursor.fetchone()
            if not group:
                cursor.close()
                conn.close()
                return "Group not found", 404

            group_id = group[0]
            group_name = group[1]
            group_org_id = group[4]

            # Without token, only authenticated members of the same org (or platform admins) can view.
            if not shared:
                if 'user' not in session:
                    cursor.close()
                    conn.close()
                    return "Shared group link not found or expired", 404
                current_user = session.get('user', {})
                if not is_platform_admin_user(current_user):
                    current_org_id = get_current_org_id()
                    if group_org_id and current_org_id != group_org_id:
                        cursor.close()
                        conn.close()
                        return "Access denied", 403

            # Get stores in this group
            cursor.execute("""
                SELECT store_id, store_name
                FROM group_stores
                WHERE group_id = %s
                ORDER BY store_name
            """, (group_id,))

            store_rows = cursor.fetchall()
            cursor.close()
            conn.close()

            # Resolve store data using the group's organization when available.
            source_restaurants = get_current_org_restaurants()
            if group_org_id:
                source_restaurants = ORG_DATA.get(group_org_id, {}).get('restaurants') or []
                if not source_restaurants:
                    cached_org_data = db.load_org_data_cache(group_org_id, 'restaurants', max_age_hours=12)
                    if isinstance(cached_org_data, list):
                        source_restaurants = cached_org_data

            # Get store data from the resolved org source
            stores_data = []
            for store_row in store_rows:
                store_id = store_row[0]
                store_name = store_row[1]

                # Find in resolved org data
                for r in source_restaurants:
                    if r['id'] == store_id:
                        # Clean data (remove internal caches)
                        store_data = {k: v for k, v in r.items() if not k.startswith('_')}
                        stores_data.append(store_data)
                        break
                else:
                    # Store not found in data, add placeholder
                    stores_data.append({
                        'id': store_id,
                        'name': store_name,
                        'metrics': {}
                    })

            # Prepare group data
            group_data = {
                'id': group_id,
                'name': group_name,
                'slug': slug,
                'stores': stores_data
            }

            # Load template
            template_file = DASHBOARD_OUTPUT / 'grupo_public.html'
            if template_file.exists():
                with open(template_file, 'r', encoding='utf-8') as f:
                    template = f.read()

                # Replace placeholders
                rendered = template.replace('{{group_name}}', escape_html_text(group_name))
                rendered = rendered.replace('{{group_initial}}', escape_html_text(group_name[0].upper() if group_name else 'G'))
                rendered = rendered.replace('{{group_data}}', safe_json_for_script(group_data))

                return Response(rendered, mimetype='text/html')

            return "Template not found", 404

        except Exception as e:
            log_exception("public_group_page_failed", e)
            return "Error loading group", 500


    @bp.route('/grupo/share/<token>')
    def public_group_share_page(token):
        """Resolve expirable token and redirect to public group page."""
        shared = db.get_group_by_share_token((token or '').strip())
        if not shared:
            return "Shared group link not found or expired", 404
        if not shared.get('group_active'):
            return "Group is inactive", 404
        return redirect(f"/grupo/{shared.get('group_slug')}?token={token}")


    # ============================================================================
    # PUBLIC RESTAURANT SHARE LINKS
    # ============================================================================

    @bp.route('/r/<token>')
    @rate_limit(limit=30, window_seconds=60, scope='public_restaurant')
    def public_restaurant_share_page(token):
        """Serve restaurant dashboard via share token -- no login required."""
        try:
            shared = db.get_restaurant_by_share_token((token or '').strip())
            if not shared:
                return "Restaurant link not found or expired", 404

            org_id = shared['org_id']
            restaurant_id = shared['restaurant_id']

            restaurant = find_restaurant_in_org(restaurant_id, org_id)
            if not restaurant:
                return "Restaurant data not available", 404

            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant, restaurant_id)
            try:
                ensure_restaurant_orders_cache(restaurant, merchant_lookup_id, org_id_override=org_id)
            except Exception:
                pass

            template_file = DASHBOARD_OUTPUT / 'restaurant_template.html'
            if not template_file.exists():
                return "Restaurant template not found", 404

            with open(template_file, 'r', encoding='utf-8') as f:
                template = f.read()

            resolved_id = restaurants_service.resolve_merchant_lookup_id(restaurant, restaurant_id)

            rendered = template.replace('{{restaurant_name}}', escape_html_text(restaurant.get('name', 'Restaurante')))
            rendered = rendered.replace('{{restaurant_id}}', escape_html_text(resolved_id))
            rendered = rendered.replace('{{restaurant_manager}}', escape_html_text(restaurant.get('manager', 'Gerente')))
            rendered = rendered.replace('{{restaurant_data}}', safe_json_for_script(restaurant))

            public_script = f"""
    <script>
        window.__PUBLIC_MODE__ = true;
        window.__PUBLIC_TOKEN__ = '{escape_html_text(token)}';
        window.__PUBLIC_API_BASE__ = '/api/public/restaurant/{escape_html_text(token)}';
    </script>
    <style>
        .nav-back {{ display: none !important; }}
        #exportPdfBtn {{ display: none !important; }}
        #shareLinkBtn {{ display: none !important; }}
    </style>
    """
            rendered = rendered.replace('</head>', public_script + '</head>')

            return Response(rendered, mimetype='text/html')

        except Exception as e:
            log_exception("public_restaurant_share_page_failed", e)
            return "Error loading restaurant", 500


    @bp.route('/api/public/restaurant/<token>')
    @rate_limit(limit=60, window_seconds=60, scope='public_restaurant_api')
    def api_public_restaurant_detail(token):
        """Public API: get restaurant data via share token (no auth required)."""
        try:
            shared = db.get_restaurant_by_share_token((token or '').strip())
            if not shared:
                return jsonify({'success': False, 'error': 'Link not found or expired'}), 404

            org_id = shared['org_id']
            restaurant_id = shared['restaurant_id']

            restaurant = find_restaurant_in_org(restaurant_id, org_id)
            if not restaurant:
                return jsonify({'success': False, 'error': 'Restaurant data not available'}), 404

            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant, restaurant_id)

            all_orders = ensure_restaurant_orders_cache(restaurant, merchant_lookup_id, org_id_override=org_id)
            merchant_lookup_id = restaurants_service.resolve_merchant_lookup_id(restaurant, merchant_lookup_id)

            # Date filtering
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')

            filtered_orders = all_orders
            if start_date or end_date:
                filtered_orders = restaurants_service.filter_orders_by_date_range(
                    all_orders,
                    start_date,
                    end_date,
                    datetime_mod=datetime,
                    normalize_order_payload=normalize_order_payload,
                )

            # Process restaurant data
            if start_date or end_date:
                merchant_details = {
                    'id': merchant_lookup_id,
                    'name': restaurant.get('name', 'Unknown'),
                    'merchantManager': {'name': restaurant.get('manager', 'Gerente')}
                }
                response_data = IFoodDataProcessor.process_restaurant_data(merchant_details, filtered_orders, None)
                response_data['name'] = restaurant['name']
                response_data['manager'] = restaurant['manager']
            else:
                if all_orders:
                    merchant_details = {
                        'id': merchant_lookup_id,
                        'name': restaurant.get('name', 'Unknown'),
                        'merchantManager': {'name': restaurant.get('manager', 'Gerente')}
                    }
                    response_data = IFoodDataProcessor.process_restaurant_data(merchant_details, all_orders, None)
                    response_data['name'] = restaurant.get('name', response_data.get('name'))
                    response_data['manager'] = restaurant.get('manager', response_data.get('manager'))
                else:
                    response_data = {k: v for k, v in restaurant.items() if not k.startswith('_')}

            # Chart data
            chart_data = {}
            orders_for_charts = filtered_orders if (start_date or end_date) else all_orders
            top_n = request.args.get('top_n', default=10, type=int)
            top_n = max(1, min(top_n or 10, 50))
            menu_performance = IFoodDataProcessor.calculate_menu_item_performance(orders_for_charts, top_n=top_n)

            if orders_for_charts:
                if hasattr(IFoodDataProcessor, 'generate_charts_data_with_interruptions'):
                    chart_data = IFoodDataProcessor.generate_charts_data_with_interruptions(orders_for_charts, [])
                else:
                    chart_data = IFoodDataProcessor.generate_charts_data(orders_for_charts)
                    chart_data['interruptions'] = []

            reviews_payload = restaurants_service.build_reviews_payload(orders_for_charts)

            return jsonify({
                'success': True,
                'restaurant': response_data,
                'charts': chart_data,
                'menu_performance': menu_performance,
                'interruptions': [],
                'reviews': reviews_payload,
                'filter': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'total_orders_filtered': len(filtered_orders) if (start_date or end_date) else len(all_orders)
                }
            })

        except Exception as e:
            log_exception("api_public_restaurant_detail_failed", e)
            return internal_error_response()


    # API: Get all groups
    @bp.route('/api/groups', methods=['GET'])
    @login_required
    def api_get_groups():
        """Get all client groups with their stores"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            conn = db.get_connection()
            cursor = conn.cursor()
            has_org_id = _table_has_org_id(cursor, 'client_groups')
        
            # Get all groups
            if has_org_id:
                cursor.execute("""
                    SELECT id, name, slug, active, created_by, created_at
                    FROM client_groups
                    WHERE org_id = %s
                    ORDER BY name
                """, (org_id,))
            else:
                cursor.execute("""
                    SELECT id, name, slug, active, created_by, created_at
                    FROM client_groups
                    ORDER BY name
                """)
        
            groups_raw = cursor.fetchall()
            groups = []
        
            for g in groups_raw:
                group_id = g[0]
            
                # Get stores for this group
                cursor.execute("""
                    SELECT store_id, store_name
                    FROM group_stores
                    WHERE group_id = %s
                    ORDER BY store_name
                """, (group_id,))
            
                stores = [{'id': s[0], 'name': s[1]} for s in cursor.fetchall()]
            
                groups.append({
                    'id': group_id,
                    'name': g[1],
                    'slug': g[2],
                    'active': g[3],
                    'created_by': g[4],
                    'created_at': g[5].isoformat() if g[5] else None,
                    'stores': stores
                })
        
            cursor.close()
            conn.close()
        
            return jsonify({
                'success': True,
                'groups': groups
            })
        
        except Exception as e:
            print(f"Error getting groups: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    @bp.route('/api/groups/templates', methods=['GET'])
    @login_required
    def api_group_templates_list():
        """List saved group templates for current organization."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403
        templates = db.list_group_templates(org_id)
        return jsonify({'success': True, 'templates': templates})


    @bp.route('/api/groups/templates', methods=['POST'])
    @admin_required
    def api_group_templates_create():
        """Create reusable group template from selected stores."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        name = (data.get('name') or '').strip()
        description = (data.get('description') or '').strip()
        store_ids = data.get('store_ids') or []
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not isinstance(store_ids, list):
            return jsonify({'success': False, 'error': 'store_ids must be a list'}), 400
        store_ids = [str(s).strip() for s in store_ids if str(s).strip()]
        if not store_ids:
            return jsonify({'success': False, 'error': 'At least one store is required'}), 400

        created_by = session.get('user', {}).get('username')
        template_id = db.create_group_template(org_id, name, store_ids, created_by=created_by, description=description)
        if not template_id:
            return jsonify({'success': False, 'error': 'Unable to create template'}), 400
        return jsonify({'success': True, 'template_id': template_id})


    @bp.route('/api/groups/templates/<int:template_id>', methods=['DELETE'])
    @admin_required
    def api_group_templates_delete(template_id):
        """Delete a group template."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403
        ok = db.delete_group_template(org_id, template_id)
        if not ok:
            return jsonify({'success': False, 'error': 'Template not found'}), 404
        return jsonify({'success': True})


    @bp.route('/api/groups/from-template', methods=['POST'])
    @admin_required
    def api_create_group_from_template():
        """Create a group quickly from template stores."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        template_id = data.get('template_id')
        name = (data.get('name') or '').strip()
        slug = (data.get('slug') or '').strip()
        if isinstance(template_id, str) and template_id.isdigit():
            template_id = int(template_id)
        if not isinstance(template_id, int):
            return jsonify({'success': False, 'error': 'template_id is required'}), 400
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400

        template = db.get_group_template(org_id, template_id)
        if not template:
            return jsonify({'success': False, 'error': 'Template not found'}), 404

        slug = groups_service.normalize_group_slug(name, slug)
        if not slug:
            slug = f"group-{int(time.time())}"

        conn = db.get_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database unavailable'}), 500
        cursor = conn.cursor()
        try:
            has_org_id = _table_has_org_id(cursor, 'client_groups')
            slug = groups_service.ensure_unique_group_slug(cursor, slug, org_id=org_id, has_org_id=has_org_id)
            if not slug:
                slug = f"group-{int(time.time())}"

            created_by = session.get('user', {}).get('username', 'Unknown')
            if has_org_id:
                cursor.execute("""
                    INSERT INTO client_groups (name, slug, created_by, org_id)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (name, slug, created_by, org_id))
            else:
                cursor.execute("""
                    INSERT INTO client_groups (name, slug, created_by)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (name, slug, created_by))
            group_id = cursor.fetchone()[0]

            stores_by_id = groups_service.build_store_name_lookup(get_current_org_restaurants())
            groups_service.insert_group_stores(
                cursor,
                group_id,
                template.get('store_ids') or [],
                stores_by_id,
                ignore_conflict=True,
            )

            conn.commit()
            return jsonify({'success': True, 'group_id': group_id, 'slug': slug})
        except Exception as e:
            conn.rollback()
            print(f"Error creating group from template: {e}")
            log_exception("request_exception", e)
            return internal_error_response()
        finally:
            cursor.close()
            conn.close()


    @bp.route('/api/groups/<int:group_id>/share-links', methods=['GET'])
    @admin_required
    def api_group_share_links_list(group_id):
        """List expirable share links for a group."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403
        conn = db.get_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database unavailable'}), 500
        cursor = conn.cursor()
        try:
            if not groups_service.group_belongs_to_org(cursor, group_id, org_id, _table_has_org_id):
                return jsonify({'success': False, 'error': 'Group not found'}), 404
        finally:
            cursor.close()
            conn.close()
        links = db.list_group_share_links(org_id, group_id)
        return jsonify({'success': True, 'links': links})


    @bp.route('/api/groups/<int:group_id>/share-links', methods=['POST'])
    @admin_required
    def api_group_share_links_create(group_id):
        """Create expirable share link for group."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        data = get_json_payload()
        expires_hours = groups_service.parse_expires_hours(data)

        conn = db.get_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database unavailable'}), 500
        cursor = conn.cursor()
        try:
            if not groups_service.group_belongs_to_org(cursor, group_id, org_id, _table_has_org_id):
                return jsonify({'success': False, 'error': 'Group not found'}), 404
        finally:
            cursor.close()
            conn.close()

        link = db.create_group_share_link(org_id, group_id, created_by=session.get('user', {}).get('id'), expires_hours=expires_hours)
        if not link:
            return jsonify({'success': False, 'error': 'Unable to create share link'}), 400
        share_url = f"{get_public_base_url()}/grupo/share/{link['token']}"
        return jsonify({'success': True, 'link': {**link, 'url': share_url}})


    @bp.route('/api/groups/<int:group_id>/share-links/<int:link_id>', methods=['DELETE'])
    @admin_required
    def api_group_share_links_revoke(group_id, link_id):
        """Disable an existing group share link."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403
        conn = db.get_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database unavailable'}), 500
        cursor = conn.cursor()
        try:
            if not groups_service.group_belongs_to_org(cursor, group_id, org_id, _table_has_org_id):
                return jsonify({'success': False, 'error': 'Group not found'}), 404
        finally:
            cursor.close()
            conn.close()
        ok = db.revoke_group_share_link(org_id, group_id, link_id)
        if not ok:
            return jsonify({'success': False, 'error': 'Share link not found'}), 404
        return jsonify({'success': True})


    # ============================================================================
    # API ROUTES - RESTAURANT SHARE LINKS (ADMIN)
    # ============================================================================

    @bp.route('/api/restaurant/<restaurant_id>/share-links', methods=['GET'])
    @admin_required
    @require_feature('public_links')
    def api_restaurant_share_links_list(restaurant_id):
        """List share links for a restaurant."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        restaurant, resolved_id = groups_service.resolve_restaurant_id(find_restaurant_by_identifier, restaurant_id)
        if not restaurant:
            return jsonify({'success': False, 'error': 'Restaurant not found'}), 404

        links = db.list_restaurant_share_links(org_id, resolved_id)
        base_url = get_public_base_url()
        for link in links:
            link['url'] = f"{base_url}/r/{link['token']}"
        return jsonify({'success': True, 'links': links})


    @bp.route('/api/restaurant/<restaurant_id>/share-links', methods=['POST'])
    @admin_required
    @require_feature('public_links')
    def api_restaurant_share_links_create(restaurant_id):
        """Create share link for a restaurant."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        restaurant, resolved_id = groups_service.resolve_restaurant_id(find_restaurant_by_identifier, restaurant_id)
        if not restaurant:
            return jsonify({'success': False, 'error': 'Restaurant not found'}), 404

        data = get_json_payload()
        expires_hours = groups_service.parse_expires_hours(data)

        link = db.create_restaurant_share_link(
            org_id, resolved_id,
            created_by=session.get('user', {}).get('id'),
            expires_hours=expires_hours
        )
        if not link:
            return jsonify({'success': False, 'error': 'Unable to create share link'}), 400

        share_url = f"{get_public_base_url()}/r/{link['token']}"
        return jsonify({'success': True, 'link': {**link, 'url': share_url}})


    @bp.route('/api/restaurant/<restaurant_id>/share-links/<int:link_id>', methods=['DELETE'])
    @admin_required
    @require_feature('public_links')
    def api_restaurant_share_links_revoke(restaurant_id, link_id):
        """Revoke a restaurant share link."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        restaurant, resolved_id = groups_service.resolve_restaurant_id(find_restaurant_by_identifier, restaurant_id)
        if not restaurant:
            return jsonify({'success': False, 'error': 'Restaurant not found'}), 404

        ok = db.revoke_restaurant_share_link(org_id, resolved_id, link_id)
        if not ok:
            return jsonify({'success': False, 'error': 'Share link not found'}), 404
        return jsonify({'success': True})


    @bp.route('/api/groups/<int:group_id>/comparison', methods=['GET'])
    @login_required
    def api_group_comparison(group_id):
        """Compare stores inside a client group over a date range."""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            start_str = request.args.get('start_date')
            end_str = request.args.get('end_date')
            sort_by = request.args.get('sort_by', 'revenue')
            sort_dir = request.args.get('sort_dir', 'desc')

            end_dt = datetime.strptime(end_str, '%Y-%m-%d') if end_str else datetime.now()
            start_dt = datetime.strptime(start_str, '%Y-%m-%d') if start_str else (end_dt - timedelta(days=30))
            if start_dt > end_dt:
                return jsonify({'success': False, 'error': 'start_date must be before end_date'}), 400

            conn = db.get_connection()
            cursor = conn.cursor()
            has_org_id = _table_has_org_id(cursor, 'client_groups')

            if has_org_id:
                cursor.execute("""
                    SELECT id, name, slug, active
                    FROM client_groups
                    WHERE id = %s AND org_id = %s
                """, (group_id, org_id))
            else:
                cursor.execute("""
                    SELECT id, name, slug, active
                    FROM client_groups
                    WHERE id = %s
                """, (group_id,))
            group_row = cursor.fetchone()
            if not group_row:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Grupo nao encontrado'}), 404

            cursor.execute("""
                SELECT store_id, store_name
                FROM group_stores
                WHERE group_id = %s
                ORDER BY store_name
            """, (group_id,))
            store_rows = cursor.fetchall()
            cursor.close()
            conn.close()

            user = session.get('user', {})
            allowed_ids = get_user_allowed_restaurant_ids(user.get('id'), user.get('role'))

            restaurants_map = {r.get('id'): r for r in get_current_org_restaurants()}
            comparison_rows = []
            group_orders = []

            for store_id, store_name in store_rows:
                if allowed_ids is not None and store_id not in allowed_ids:
                    continue

                r = restaurants_map.get(store_id)
                if not r:
                    comparison_rows.append({
                        'store_id': store_id,
                        'store_name': store_name,
                        'manager': None,
                        'available': False,
                        'metrics': _calculate_period_metrics([])
                    })
                    continue

                orders = r.get('_orders_cache', [])
                filtered_orders = _filter_orders_by_date(orders, start_dt, end_dt)
                metrics = _calculate_period_metrics(filtered_orders)
                group_orders.extend(filtered_orders)

                comparison_rows.append({
                    'store_id': store_id,
                    'store_name': r.get('name', store_name),
                    'manager': r.get('manager'),
                    'available': True,
                    'metrics': metrics
                })

            if not comparison_rows:
                return jsonify({
                    'success': True,
                    'group': {
                        'id': group_row[0],
                        'name': group_row[1],
                        'slug': group_row[2],
                        'active': group_row[3]
                    },
                    'period': {
                        'start_date': start_dt.strftime('%Y-%m-%d'),
                        'end_date': end_dt.strftime('%Y-%m-%d')
                    },
                    'summary': _calculate_period_metrics([]),
                    'stores': []
                })

            key_map = {
                'revenue': lambda x: x['metrics'].get('revenue', 0),
                'orders': lambda x: x['metrics'].get('orders', 0),
                'ticket': lambda x: x['metrics'].get('ticket', 0),
                'cancel_rate': lambda x: x['metrics'].get('cancel_rate', 0),
                'avg_rating': lambda x: x['metrics'].get('avg_rating', 0)
            }
            sort_fn = key_map.get(sort_by, key_map['revenue'])
            reverse = sort_dir != 'asc'
            comparison_rows.sort(key=sort_fn, reverse=reverse)

            total_revenue = sum(s['metrics'].get('revenue', 0) for s in comparison_rows)
            for i, row in enumerate(comparison_rows, start=1):
                row['rank'] = i
                rev = row['metrics'].get('revenue', 0)
                row['metrics']['revenue_share'] = round((rev / total_revenue * 100) if total_revenue > 0 else 0, 2)

            summary = _calculate_period_metrics(group_orders)
            best_revenue = max(comparison_rows, key=lambda x: x['metrics'].get('revenue', 0))
            best_orders = max(comparison_rows, key=lambda x: x['metrics'].get('orders', 0))
            lowest_cancel = min(comparison_rows, key=lambda x: x['metrics'].get('cancel_rate', 100))

            return jsonify({
                'success': True,
                'group': {
                    'id': group_row[0],
                    'name': group_row[1],
                    'slug': group_row[2],
                    'active': group_row[3]
                },
                'period': {
                    'start_date': start_dt.strftime('%Y-%m-%d'),
                    'end_date': end_dt.strftime('%Y-%m-%d')
                },
                'sort': {'by': sort_by, 'dir': sort_dir},
                'summary': summary,
                'benchmarks': {
                    'best_revenue_store': {'id': best_revenue['store_id'], 'name': best_revenue['store_name']},
                    'best_orders_store': {'id': best_orders['store_id'], 'name': best_orders['store_name']},
                    'lowest_cancel_store': {'id': lowest_cancel['store_id'], 'name': lowest_cancel['store_name']}
                },
                'stores': comparison_rows
            })

        except ValueError:
            return jsonify({'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        except Exception as e:
            print(f"Error group comparison: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    # API: Create group
    @bp.route('/api/groups', methods=['POST'])
    @admin_required
    def api_create_group():
        """Create a new client group"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            data = get_json_payload()
            name = data.get('name', '').strip()
            slug = data.get('slug', '').strip()
            store_ids = data.get('store_ids', [])
        
            if not name:
                return jsonify({'success': False, 'error': 'Nome e obrigatorio'}), 400
            slug = groups_service.normalize_group_slug(name, slug)
        
            created_by = session.get('user', {}).get('username', 'Unknown')
        
            conn = db.get_connection()
            cursor = conn.cursor()
            has_org_id = _table_has_org_id(cursor, 'client_groups')
            slug = groups_service.ensure_unique_group_slug(cursor, slug, org_id=org_id, has_org_id=has_org_id)
        
            # Create group
            if has_org_id:
                cursor.execute("""
                    INSERT INTO client_groups (name, slug, created_by, org_id)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (name, slug, created_by, org_id))
            else:
                cursor.execute("""
                    INSERT INTO client_groups (name, slug, created_by)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (name, slug, created_by))
        
            group_id = cursor.fetchone()[0]
            store_lookup = groups_service.build_store_name_lookup(get_current_org_restaurants())
            groups_service.insert_group_stores(
                cursor,
                group_id,
                store_ids,
                store_lookup,
                ignore_conflict=True,
            )
        
            conn.commit()
            cursor.close()
            conn.close()
        
            return jsonify({
                'success': True,
                'message': 'Grupo criado com sucesso',
                'group_id': group_id,
                'slug': slug
            })
        
        except Exception as e:
            print(f"Error creating group: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    # API: Update group
    @bp.route('/api/groups/<int:group_id>', methods=['PUT'])
    @admin_required
    def api_update_group(group_id):
        """Update a client group"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            data = get_json_payload()
            name = data.get('name', '').strip()
            slug = data.get('slug', '').strip()
            store_ids = data.get('store_ids', [])
            active = data.get('active', True)
        
            if not name:
                return jsonify({'success': False, 'error': 'Nome e obrigatorio'}), 400
        
            conn = db.get_connection()
            cursor = conn.cursor()
            has_org_id = _table_has_org_id(cursor, 'client_groups')
        
            # Check if group exists
            if has_org_id:
                cursor.execute("SELECT id, slug FROM client_groups WHERE id = %s AND org_id = %s", (group_id, org_id))
            else:
                cursor.execute("SELECT id, slug FROM client_groups WHERE id = %s", (group_id,))
            existing = cursor.fetchone()
            if not existing:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Grupo nao encontrado'}), 404
        
            # If slug changed, validate it
            if slug and slug != existing[1]:
                slug = groups_service.normalize_group_slug(name, slug)
                slug = groups_service.ensure_unique_group_slug(
                    cursor,
                    slug,
                    org_id=org_id,
                    group_id=group_id,
                    has_org_id=has_org_id,
                )
                if not slug:
                    cursor.close()
                    conn.close()
                    return jsonify({'success': False, 'error': 'Slug ja existe'}), 400
            else:
                slug = existing[1]
        
            # Update group
            if has_org_id:
                cursor.execute("""
                    UPDATE client_groups
                    SET name = %s, slug = %s, active = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND org_id = %s
                """, (name, slug, active, group_id, org_id))
            else:
                cursor.execute("""
                    UPDATE client_groups
                    SET name = %s, slug = %s, active = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (name, slug, active, group_id))
        
            # Update stores - remove all and re-add
            cursor.execute("DELETE FROM group_stores WHERE group_id = %s", (group_id,))
            store_lookup = groups_service.build_store_name_lookup(get_current_org_restaurants())
            groups_service.insert_group_stores(cursor, group_id, store_ids, store_lookup, ignore_conflict=False)
        
            conn.commit()
            cursor.close()
            conn.close()
        
            return jsonify({
                'success': True,
                'message': 'Grupo atualizado com sucesso'
            })
        
        except Exception as e:
            print(f"Error updating group: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    # API: Delete group
    @bp.route('/api/groups/<int:group_id>', methods=['DELETE'])
    @admin_required
    def api_delete_group(group_id):
        """Delete a client group"""
        try:
            org_id = get_current_org_id()
            if not org_id:
                return jsonify({'success': False, 'error': 'No organization selected'}), 403

            conn = db.get_connection()
            cursor = conn.cursor()
            has_org_id = _table_has_org_id(cursor, 'client_groups')
        
            # Check if group exists
            if has_org_id:
                cursor.execute("SELECT name FROM client_groups WHERE id = %s AND org_id = %s", (group_id, org_id))
            else:
                cursor.execute("SELECT name FROM client_groups WHERE id = %s", (group_id,))
            result = cursor.fetchone()
            if not result:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Grupo nao encontrado'}), 404
        
            group_name = result[0]
        
            # Delete group (cascade will delete stores)
            if has_org_id:
                cursor.execute("DELETE FROM client_groups WHERE id = %s AND org_id = %s", (group_id, org_id))
            else:
                cursor.execute("DELETE FROM client_groups WHERE id = %s", (group_id,))
            conn.commit()
        
            cursor.close()
            conn.close()
        
            return jsonify({
                'success': True,
                'message': f'Grupo "{group_name}" excluido com sucesso'
            })
        
        except Exception as e:
            print(f"Error deleting group: {e}")
            log_exception("request_exception", e)
            return internal_error_response()


    app.register_blueprint(bp)
