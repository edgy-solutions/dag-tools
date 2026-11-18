FROM python:3.12-slim

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install system dependencies (if any needed, e.g. curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies directly to system python using uv.
#
# This list IS the image's dependency manifest: the build context is this
# directory alone, so nothing here reads the project file and the extras
# declared there govern `pip install` only.
#
# `pyjwt[crypto]`, never bare `pyjwt`. The signature algorithms are in the
# optional `crypto` extra (PyJWT declares `cryptography>=3.4.0; extra ==
# "crypto"`), and without it `import jwt` still succeeds, the container still
# starts, and only `HS256/384/512` register -- so a gateway configured for
# RS256 comes up clean and refuses every token as invalid. That is a
# deployment fault wearing a token fault's clothes, which is why the check
# in scripts/check_gateway_signing.py runs against the built image.
RUN uv pip install --system \
    fastapi \
    hypercorn \
    httpx \
    pydantic \
    redis \
    'pyjwt[crypto]'

# Copy application code
COPY . .

# Default port for Central Gateway
EXPOSE 8090

# Use Hypercorn as requested by the user
CMD ["hypercorn", "main:app", "--bind", "0.0.0.0:8090"]
