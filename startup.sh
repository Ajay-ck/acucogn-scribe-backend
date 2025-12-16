#!/bin/bash
# Azure App Service startup script for FastAPI application
# Uses gunicorn with uvicorn workers (ASGI) for FastAPI

# Azure App Service sets the PORT environment variable automatically
# Use it if available, otherwise default to 8000
PORT="${PORT:-8000}"

# Start gunicorn with uvicorn workers for FastAPI (ASGI)
# This is required because FastAPI is ASGI, not WSGI
exec gunicorn app:app \
    --bind 0.0.0.0:$PORT \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 600 \
    --access-logfile - \
    --error-logfile -

