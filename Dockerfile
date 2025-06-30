# FHIR4DS Analytics Server Dockerfile
# Multi-stage build for optimized production image

FROM python:3.11-slim as builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster package management
RUN pip install uv

# Set work directory
WORKDIR /app

# Copy project files
COPY pyproject.toml ./
COPY fhir4ds/ ./fhir4ds/

# Install dependencies using uv
RUN uv venv /opt/venv && \
    . /opt/venv/bin/activate && \
    uv pip install --no-deps . && \
    uv pip install ".[server,postgresql,helpers]"

# Production stage
FROM python:3.11-slim as production

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user
RUN groupadd -r fhir4ds && useradd -r -g fhir4ds fhir4ds

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create application directory
WORKDIR /app

# Copy application code
COPY --chown=fhir4ds:fhir4ds fhir4ds/ ./fhir4ds/
COPY --chown=fhir4ds:fhir4ds pyproject.toml ./

# Create directories for data and configuration
RUN mkdir -p /app/data /app/config /app/views && \
    chown -R fhir4ds:fhir4ds /app

# Switch to non-root user
USER fhir4ds

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command (can be overridden)
CMD ["python", "-m", "fhir4ds.server", "--host", "0.0.0.0", "--port", "8000"]