
## Railway Deployment Notes

- Web service start command:
  - `gunicorn wsgi:app --preload --bind 0.0.0.0:$PORT --workers ${WEB_CONCURRENCY:-2} --worker-class gevent --worker-connections ${GUNICORN_WORKER_CONNECTIONS:-1000} --timeout 120 --keep-alive 5 --log-level info`
- Worker service start command:
  - `python dashboardserver.py --worker`

### Recommended env vars

- `FLASK_ENV=production`
- `FLASK_SECRET_KEY=<long-random-secret>`
- `PUBLIC_BASE_URL=https://your-app.up.railway.app`
- `DATABASE_URL=<railway-postgres-url>`
- `REDIS_URL=<railway-redis-url>`
- `USE_REDIS_QUEUE=true`
- `USE_REDIS_CACHE=true`
- `USE_REDIS_PUBSUB=true`
- `RUN_REFRESH_WORKER=true` (worker service only)
- `IFOOD_KEEPALIVE_POLLING=true` (worker service; keeps test stores connected/open)
- `IFOOD_POLL_INTERVAL_SECONDS=30` (worker service)
- `ENABLE_LEGACY_FALLBACK=false`
- `IFOOD_CLIENT_ID=<optional env fallback>`
- `IFOOD_CLIENT_SECRET=<optional env fallback>`
