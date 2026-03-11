# --- Stage 1: Build scraper ---
FROM golang:1.25-bookworm AS builder

WORKDIR /build

# Copy Go source code
COPY scraper .

# Build the binary with CLI tags to skip GUI/Wails deps
# We disable CGO to ensure the binary is portable, although bookworm->bookworm should be fine
ENV CGO_ENABLED=0
RUN go mod download && \
    go build -tags cli -o scraper .

# --- Stage 2: Setup Python App ---
FROM python:3.11-slim-bookworm

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    IS_DOCKER=1 \
    OUTPUT_PATH=/music

WORKDIR /app

# Install system dependencies
# We ONLY need runtime dependencies now.
# - ffmpeg: for audio conversion
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copy scraper binary from builder
COPY --from=builder /build/scraper /usr/local/bin/scraper

# Copy source code and scripts
COPY src /app/src
COPY pyproject.toml /app/
COPY run_pipeline.py /app/

# Install Python dependencies (after copying src so the package is present)
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir .

# Define volumes
VOLUME /music
VOLUME /config

# Expose Web UI port
EXPOSE 5000

# Entrypoint
CMD ["python", "run_pipeline.py"]
