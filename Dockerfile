# ==============================================================================
# MULTI-STAGE BUILD FOR PYTHON E-COMMERCE PIPELINE
# ==============================================================================

# Stage 1: Builder (Dependencies & Tests)
FROM python:3.12-slim AS builder

# Build-time operations
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Copy dependency files
COPY requirements.txt .

# Install runtime dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy source code
COPY src/ /app/src/

# Set ownership
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Healthcheck
# HEALTHCHECK CMD python -c "import src.config.settings" || exit 1
