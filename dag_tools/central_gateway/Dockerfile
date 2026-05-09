FROM python:3.12-slim

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install system dependencies (if any needed, e.g. curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies directly to system python using uv
RUN uv pip install --system \
    fastapi \
    hypercorn \
    httpx \
    pydantic \
    redis \
    pyjwt

# Copy application code
COPY . .

# Default port for Central Gateway
EXPOSE 8090

# Use Hypercorn as requested by the user
CMD ["hypercorn", "main:app", "--bind", "0.0.0.0:8090"]
