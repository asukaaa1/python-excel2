
## Railway Deployment Notes

- Web service start command:
  - `gunicorn dashboardserver:app --bind 0.0.0.0:$PORT --workers ${WEB_CONCURRENCY:-2} --worker-class gevent --worker-connections ${GUNICORN_WORKER_CONNECTIONS:-1000} --timeout 120 --keep-alive 5 --log-level info`
- Worker service start command:
  - `python dashboardserver.py --worker`

### Recommended env vars

- `FLASK_ENV=production`
- `FLASK_SECRET_KEY=<long-random-secret>`
- `DATABASE_URL=<railway-postgres-url>`
- `REDIS_URL=<railway-redis-url>`
- `USE_REDIS_QUEUE=true`
- `USE_REDIS_CACHE=true`
- `USE_REDIS_PUBSUB=true`
- `RUN_REFRESH_WORKER=true` (worker service only)
- `ENABLE_LEGACY_FALLBACK=false`
