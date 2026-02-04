"""
Groups Feature - Backend Additions
Add these to your existing dashboardserver.py and dashboarddb.py files
"""

# ============================================================================
# ADD TO dashboarddb.py - In the setup_tables() method, add this table creation:
# ============================================================================

"""
# Create client_groups table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS client_groups (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        slug VARCHAR(100) UNIQUE NOT NULL,
        active BOOLEAN DEFAULT true,
        created_by VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
''')

# Create group_stores junction table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS group_stores (
        id SERIAL PRIMARY KEY,
        group_id INTEGER REFERENCES client_groups(id) ON DELETE CASCADE,
        store_id VARCHAR(100) NOT NULL,
        store_name VARCHAR(200),
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(group_id, store_id)
    )
''')
"""


# ============================================================================
# ADD TO dashboardserver.py - Add these routes (paste after squads routes):
# ============================================================================

# --- START OF ROUTES TO ADD ---

# Page route for grupos
@app.route('/grupos')
@login_required
def grupos_page():
    """Serve client groups management page"""
    grupos_file = DASHBOARD_OUTPUT / 'grupos.html'
    if grupos_file.exists():
        return send_file(grupos_file)
    return "Grupos page not found", 404


# Public group page (no auth required)
@app.route('/grupo/<slug>')
def public_group_page(slug):
    """Serve public group dashboard - NO AUTH REQUIRED"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get group by slug
        cursor.execute("""
            SELECT id, name, slug, active
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
        
        # Get store data from RESTAURANTS_DATA
        stores_data = []
        for store_row in store_rows:
            store_id = store_row[0]
            store_name = store_row[1]
            
            # Find in global data
            for r in RESTAURANTS_DATA:
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
            rendered = template.replace('{{group_name}}', group_name)
            rendered = rendered.replace('{{group_initial}}', group_name[0].upper() if group_name else 'G')
            rendered = rendered.replace('{{group_data}}', json.dumps(group_data, ensure_ascii=False))
            
            return render_template_string(rendered)
        
        return "Template not found", 404
        
    except Exception as e:
        print(f"Error loading public group: {e}")
        traceback.print_exc()
        return "Error loading group", 500


# API: Get all groups
@app.route('/api/groups', methods=['GET'])
@login_required
def api_get_groups():
    """Get all client groups with their stores"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get all groups
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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# API: Create group
@app.route('/api/groups', methods=['POST'])
@admin_required
def api_create_group():
    """Create a new client group"""
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        slug = data.get('slug', '').strip()
        store_ids = data.get('store_ids', [])
        
        if not name:
            return jsonify({'success': False, 'error': 'Nome é obrigatório'}), 400
        
        # Generate slug if not provided
        if not slug:
            import re
            slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
        
        # Ensure slug is valid
        import re
        slug = re.sub(r'[^a-z0-9-]', '', slug.lower())
        
        created_by = session.get('user', {}).get('username', 'Unknown')
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if slug exists
        cursor.execute("SELECT id FROM client_groups WHERE slug = %s", (slug,))
        if cursor.fetchone():
            # Add random suffix
            import random
            slug = f"{slug}-{random.randint(100, 999)}"
        
        # Create group
        cursor.execute("""
            INSERT INTO client_groups (name, slug, created_by)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (name, slug, created_by))
        
        group_id = cursor.fetchone()[0]
        
        # Add stores
        for store_id in store_ids:
            # Get store name from RESTAURANTS_DATA
            store_name = store_id
            for r in RESTAURANTS_DATA:
                if r['id'] == store_id:
                    store_name = r.get('name', store_id)
                    break
            
            cursor.execute("""
                INSERT INTO group_stores (group_id, store_id, store_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (group_id, store_id) DO NOTHING
            """, (group_id, store_id, store_name))
        
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
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# API: Update group
@app.route('/api/groups/<int:group_id>', methods=['PUT'])
@admin_required
def api_update_group(group_id):
    """Update a client group"""
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        slug = data.get('slug', '').strip()
        store_ids = data.get('store_ids', [])
        active = data.get('active', True)
        
        if not name:
            return jsonify({'success': False, 'error': 'Nome é obrigatório'}), 400
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if group exists
        cursor.execute("SELECT id, slug FROM client_groups WHERE id = %s", (group_id,))
        existing = cursor.fetchone()
        if not existing:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Grupo não encontrado'}), 404
        
        # If slug changed, validate it
        if slug and slug != existing[1]:
            import re
            slug = re.sub(r'[^a-z0-9-]', '', slug.lower())
            
            cursor.execute("SELECT id FROM client_groups WHERE slug = %s AND id != %s", (slug, group_id))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Slug já existe'}), 400
        else:
            slug = existing[1]
        
        # Update group
        cursor.execute("""
            UPDATE client_groups 
            SET name = %s, slug = %s, active = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (name, slug, active, group_id))
        
        # Update stores - remove all and re-add
        cursor.execute("DELETE FROM group_stores WHERE group_id = %s", (group_id,))
        
        for store_id in store_ids:
            store_name = store_id
            for r in RESTAURANTS_DATA:
                if r['id'] == store_id:
                    store_name = r.get('name', store_id)
                    break
            
            cursor.execute("""
                INSERT INTO group_stores (group_id, store_id, store_name)
                VALUES (%s, %s, %s)
            """, (group_id, store_id, store_name))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Grupo atualizado com sucesso'
        })
        
    except Exception as e:
        print(f"Error updating group: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# API: Delete group
@app.route('/api/groups/<int:group_id>', methods=['DELETE'])
@admin_required
def api_delete_group(group_id):
    """Delete a client group"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check if group exists
        cursor.execute("SELECT name FROM client_groups WHERE id = %s", (group_id,))
        result = cursor.fetchone()
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Grupo não encontrado'}), 404
        
        group_name = result[0]
        
        # Delete group (cascade will delete stores)
        cursor.execute("DELETE FROM client_groups WHERE id = %s", (group_id,))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Grupo "{group_name}" excluído com sucesso'
        })
        
    except Exception as e:
        print(f"Error deleting group: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

# --- END OF ROUTES TO ADD ---
