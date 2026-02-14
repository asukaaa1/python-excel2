"""Group domain service helpers."""

import random
import re


def group_belongs_to_org(cursor, group_id, org_id, table_has_org_id):
    if table_has_org_id(cursor, 'client_groups'):
        cursor.execute('SELECT 1 FROM client_groups WHERE id=%s AND org_id=%s', (group_id, org_id))
    else:
        cursor.execute('SELECT 1 FROM client_groups WHERE id=%s', (group_id,))
    return cursor.fetchone() is not None


def resolve_restaurant_id(find_restaurant_by_identifier, restaurant_id):
    restaurant = find_restaurant_by_identifier(restaurant_id)
    if not restaurant:
        return None, None
    resolved_id = (
        restaurant.get('_resolved_merchant_id')
        or restaurant.get('merchant_id')
        or restaurant.get('merchantId')
        or restaurant.get('id')
        or restaurant_id
    )
    return restaurant, resolved_id


def parse_expires_hours(data, default_hours=24 * 7, min_hours=1, max_hours=24 * 90):
    try:
        hours = int((data or {}).get('expires_hours', default_hours))
    except Exception:
        hours = default_hours
    return max(min_hours, min(hours, max_hours))


def normalize_group_slug(name, slug=''):
    clean_name = str(name or '').strip()
    raw_slug = str(slug or '').strip().lower()
    if not raw_slug:
        raw_slug = re.sub(r'[^a-z0-9]+', '-', clean_name.lower()).strip('-')
    raw_slug = re.sub(r'[^a-z0-9-]', '', raw_slug.lower())
    return raw_slug


def ensure_unique_group_slug(cursor, slug, *, org_id=None, group_id=None, has_org_id=False):
    if not slug:
        slug = f'grupo-{random.randint(100, 999)}'

    if group_id is None:
        cursor.execute('SELECT id FROM client_groups WHERE slug = %s', (slug,))
        if cursor.fetchone():
            return f'{slug}-{random.randint(100, 999)}'
        return slug

    if has_org_id and org_id is not None:
        cursor.execute(
            'SELECT id FROM client_groups WHERE slug = %s AND id != %s AND org_id = %s',
            (slug, group_id, org_id),
        )
    else:
        cursor.execute('SELECT id FROM client_groups WHERE slug = %s AND id != %s', (slug, group_id))

    if cursor.fetchone():
        return None
    return slug


def sanitize_store_ids(store_ids):
    if not isinstance(store_ids, list):
        return []
    cleaned = []
    for store_id in store_ids:
        sid = str(store_id or '').strip()
        if sid:
            cleaned.append(sid)
    return cleaned


def build_store_name_lookup(restaurants):
    lookup = {}
    for restaurant in restaurants or []:
        rid = str((restaurant or {}).get('id') or '').strip()
        if not rid:
            continue
        lookup[rid] = restaurant.get('name', rid)
    return lookup


def insert_group_stores(cursor, group_id, store_ids, store_name_lookup, *, ignore_conflict=False):
    if ignore_conflict:
        query = (
            'INSERT INTO group_stores (group_id, store_id, store_name) '
            'VALUES (%s, %s, %s) '
            'ON CONFLICT (group_id, store_id) DO NOTHING'
        )
    else:
        query = 'INSERT INTO group_stores (group_id, store_id, store_name) VALUES (%s, %s, %s)'

    for store_id in sanitize_store_ids(store_ids):
        store_name = store_name_lookup.get(store_id, store_id)
        cursor.execute(query, (group_id, store_id, store_name))
