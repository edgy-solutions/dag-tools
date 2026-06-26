# Dag-tools user-deployment image.
#
# Two run modes from the same image (the iagent helm chart picks the
# mode via the container's ``command`` override):
#
#   1. Dagster code-location (default CMD):
#        dagster api grpc -m dag_tools.user_deployment.definitions
#      This is the gRPC server iagent-dagster's webserver/daemon
#      connects to. Asset definitions are scanned at startup via
#      definitions.py, which reads DAG_TOOLS_DEMO_MODE to pick which
#      components subdirectory to register.
#
#   2. Domain broker (helm overrides command):
#        hypercorn dag_tools.domain_broker.main:app --bind 0.0.0.0:8000
#      Same image, hypercorn entrypoint for the dag-tools domain
#      broker that advertises this deployment's URN→physical-coordinates
#      map to the central gateway. Mirror of pub-tools' broker shape.
#
# Image name: ``ghcr.io/edgy-solutions/dag-tools/user-deployment:latest``
# (per the build-containers.yml matrix entry).
#
# Build context: the repo root (``..``), so the image has the entire
# dag_tools package available and can resolve cross-module imports
# (the user-deployment uses dag_tools.io_managers, dag_tools.domain_broker,
# etc.).

# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Logs flush directly (no buffering) and no .pyc droppings.
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# uv for fast dep install.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# git is needed for any `pip install git+https://...` deps the
# dag-tools wheel pulls in transitively.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Copy the whole dag-tools source tree first (we install editable so
# the Dagster components scanner can find the component.yaml files
# at runtime).
COPY . /app

# Resolve and install dependencies from the pyproject.toml at the
# repo root. Cache mount keeps repeat builds fast.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system .

# Default port for the Dagster gRPC code-location.
EXPOSE 4000

# Default mode: Dagster gRPC code-location for the iagent webserver
# to connect to. The broker pod overrides this via container.command
# in the iagent helm chart's templates/user-deployments.yaml.
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "dag_tools.user_deployment.definitions"]
