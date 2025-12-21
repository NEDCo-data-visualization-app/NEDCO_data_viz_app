# Use official Python base image
FROM python:3.11-slim

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=run.py
ENV FLASK_ENV=development
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt requirements-forecast.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -r requirements-forecast.txt

# Copy the rest of the app
COPY . .

# Expose port
EXPOSE 5000

# Make data accessible
VOLUME ["/app/data"]

# Run Flask server
CMD ["flask", "run"]
