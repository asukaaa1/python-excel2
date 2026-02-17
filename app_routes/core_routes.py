"""Core route registration orchestrator."""

from flask import Blueprint

from app_routes.core_admin_routes import register_routes as register_admin_routes
from app_routes.core_analytics_routes import register_routes as register_analytics_routes
from app_routes.core_pages_routes import register_routes as register_pages_routes
from app_routes.core_realtime_routes import register_routes as register_realtime_routes
from app_routes.route_context import RouteContext


def register(app, deps):
    bp = Blueprint('core_routes', __name__)
    ctx = RouteContext.from_deps(deps)

    register_pages_routes(bp, ctx)
    register_realtime_routes(bp, ctx)
    register_analytics_routes(bp, ctx)
    register_admin_routes(bp, ctx)

    # Keep historical endpoint names (no blueprint prefix).
    app.register_blueprint(bp, name='')
