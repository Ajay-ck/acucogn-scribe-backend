#!/bin/bash
# Azure App Service startup script for FastAPI application
# Uses gunicorn with uvicorn workers (ASGI) for FastAPI

echo "=== Starting FastAPI Application ==="
echo "Python version: $(python --version)"

# Azure App Service sets the PORT environment variable automatically
# Use it if available, otherwise default to 8000
PORT="${PORT:-8000}"

echo "Starting server on port: $PORT"

# Start gunicorn with uvicorn workers for FastAPI (ASGI)
# This is production-ready with multiple workers
exec gunicorn app:app \
    --bind 0.0.0.0:$PORT \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 600 \
    --access-logfile - \
    --error-logfile - \
    --log-level info