web: gunicorn wsgi:app --preload --bind 0.0.0.0:${PORT:-5000} --workers ${WEB_CONCURRENCY:-2} --worker-class gevent --worker-connections ${GUNICORN_WORKER_CONNECTIONS:-1000} --timeout 120 --keep-alive 5 --log-level info
worker: python dashboardserver.py --worker
