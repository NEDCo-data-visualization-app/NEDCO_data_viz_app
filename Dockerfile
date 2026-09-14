# Production image for the VoltaV dashboard (used by the Render Blueprint).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000 \
    DB_PATH=/app/data/warehouse_new.duckdb \
    UPLOADS_DIR=/app/data/uploads

WORKDIR /app

# libgomp is required by LightGBM at import time.
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt gunicorn

COPY . .

# The DuckDB warehouse lives on a persistent volume mounted here.
VOLUME ["/app/data"]
EXPOSE 5000

# One worker: DuckDB allows a single writer per file. Threads are fine because
# DataStore serialises access to the shared connection.
CMD ["sh", "-c", "gunicorn --workers 1 --threads 8 --timeout 300 --bind 0.0.0.0:${PORT} run:app"]
