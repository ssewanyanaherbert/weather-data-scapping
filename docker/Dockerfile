# docker/Dockerfile
# Build context is the project root (set via docker-compose context: ..)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies first (better layer caching — only re-runs when requirements change)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project
COPY . /app

# Default command (overridden per-service in docker-compose.yml)
CMD ["python", "main.py"]