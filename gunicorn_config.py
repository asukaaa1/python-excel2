# Gunicorn configuration file
import multiprocessing

# Server socket
bind = "0.0.0.0:5000"
backlog = 2048

# Worker processes
# IMPORTANT: Using 'gevent' async workers because the app uses SSE (Server-Sent Events).
# Sync workers get permanently blocked by each SSE connection (/api/events),
# starving all other requests and causing "loads forever" on page navigation.
# Gevent uses green threads so one worker can handle thousands of concurrent
# connections (including long-lived SSE streams) without blocking.
#
# Install: pip install gevent
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = 'gevent'
worker_connections = 1000
timeout = 30
keepalive = 2

# Logging
accesslog = '-'  # Log to stdout
errorlog = '-'   # Log to stderr
loglevel = 'info'

# Process naming
proc_name = 'restaurant-dashboard'

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# SSL (uncomment if using HTTPS)
# keyfile = '/path/to/keyfile'
# certfile = '/path/to/certfile'