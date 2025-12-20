# Use official Python slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY volta/ volta/
COPY run.py .
COPY models/ models/
COPY .env .env

# Expose port (Cloud Run expects PORT environment variable)
ENV PORT 8080
EXPOSE 8080

# Set Flask production environment
ENV FLASK_APP=run.py
ENV FLASK_ENV=production

# Run the app with Flask built-in server
CMD ["flask", "run", "--host=0.0.0.0", "--port=8080"]




