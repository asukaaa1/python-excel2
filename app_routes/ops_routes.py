"""Operations diagnostics route registrations."""

from flask import Blueprint
from app_services.ops_service import build_ops_summary
from app_routes.dependencies import bind_dependencies


REQUIRED_DEPS = [
    'APP_STARTED_AT',
    'LAST_DATA_REFRESH',
    'ORG_DATA',
    'REDIS_INSTANCE_ID',
    'REDIS_REFRESH_LOCK_KEY',
    'REDIS_REFRESH_QUEUE',
    'USE_REDIS_CACHE',
    'USE_REDIS_PUBSUB',
    'USE_REDIS_QUEUE',
    '_api_cache',
    'bg_refresher',
    'build_data_quality_payload',
    'datetime',
    'db',
    'get_current_org_id',
    'get_current_org_restaurants',
    'get_redis_client',
    'get_refresh_status',
    'jsonify',
    'platform_admin_required',
    'sse_manager',
]

def register(app, deps):
    bind_dependencies(globals(), deps, REQUIRED_DEPS)
    bp = Blueprint('ops_routes', __name__)

    @bp.route('/api/ops/summary')
    @platform_admin_required
    def api_ops_summary():
        """Operations summary for Railway/production diagnostics."""
        org_id = get_current_org_id()
        if not org_id:
            return jsonify({'success': False, 'error': 'No organization selected'}), 403

        payload = build_ops_summary(
            org_id=org_id,
            db=db,
            get_refresh_status=get_refresh_status,
            get_redis_client=get_redis_client,
            redis_refresh_queue=REDIS_REFRESH_QUEUE,
            redis_refresh_lock_key=REDIS_REFRESH_LOCK_KEY,
            org_data=ORG_DATA,
            last_data_refresh=LAST_DATA_REFRESH,
            get_current_org_restaurants=get_current_org_restaurants,
            build_data_quality_payload=build_data_quality_payload,
            instance_id=REDIS_INSTANCE_ID,
            app_started_at=APP_STARTED_AT,
            datetime_mod=datetime,
            bg_refresher=bg_refresher,
            use_redis_queue=USE_REDIS_QUEUE,
            use_redis_cache=USE_REDIS_CACHE,
            use_redis_pubsub=USE_REDIS_PUBSUB,
            api_cache=_api_cache,
            sse_manager=sse_manager,
        )
        return jsonify(payload)


    # ============================================================================
    # API ROUTES - SAVED VIEWS
    # ============================================================================


    app.register_blueprint(bp)
