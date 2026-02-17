"""Core route registration orchestrator."""

from flask import Blueprint

from app_routes.core_admin_routes import register_routes as register_admin_routes
from app_routes.core_analytics_routes import register_routes as register_analytics_routes
from app_routes.core_pages_routes import register_routes as register_pages_routes
from app_routes.core_realtime_routes import register_routes as register_realtime_routes


def register(app, deps):
    bp = Blueprint('core_routes', __name__)

    register_pages_routes(bp, deps)
    register_realtime_routes(bp, deps)
    register_analytics_routes(bp, deps)
    register_admin_routes(bp, deps)

    # Keep historical endpoint names (no blueprint prefix).
    app.register_blueprint(bp, name='')
