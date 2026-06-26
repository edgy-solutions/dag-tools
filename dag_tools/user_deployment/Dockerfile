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

# Install only the dagster + S3 + DataHub bits the user-deployment
# actually uses. The full ``orchestrator`` extras group pulls in
# dagster-dlt + dagster-dbt + dagster-embedded-elt + dlt with five
# DB backends — none of which the user-deployment touches. Importing
# them at code-location startup ballooned ``import
# dag_tools.user_deployment.definitions`` to 43.4 seconds, which
# overflowed Dagster's 60-second gRPC launch_run timeout: the
# daemon's subprocess-spawn RPC consistently failed with
# ``DagsterUserCodeUnreachableError`` even though the gRPC server
# was reachable. The 43.4s + subprocess handshake exceeded the
# budget; trimming brings it well under the deadline.
#
# The minimum set:
#   * Base dag-tools deps via ``.`` install (polars, pyarrow, etc.)
#   * ``dagster`` + ``dagster-aws`` — @asset decorator + the
#     CortexPolarsIOManager's transitive dagster_aws.s3 import.
#   * ``dagster-postgres`` — REQUIRED by the daemon's launch-run
#     subprocess. The iagent-dagster instance's dagster.yaml
#     configures DagsterPostgresStorage; when the daemon spawns a
#     run subprocess on the code-location pod, that subprocess
#     reads the instance config and instantiates the storage class.
#     Without dagster-postgres installed, the subprocess fails to
#     import the storage backend and the daemon reports a
#     misleading ``DagsterUserCodeUnreachableError: ... timed out
#     after 60 seconds`` (because it never gets the success ack
#     within the budget). The actual error is in the daemon log:
#     ``Couldn't import module dagster_postgres when attempting to
#     load the configurable class dagster_postgres.DagsterPostgresStorage``.
#     Pub-tools includes this transitively via ``.[orchestrator]``;
#     dag-tools' trimmed install dropped that group, so it must be
#     listed explicitly here.
#   * ``acryl-datahub`` — CortexPolarsIOManager.handle_output emits
#     URNs to DataHub on materialization.
#   * ``boto3`` + ``s3fs`` — S3 writes.
#   * ``.[broker]`` extras — fastapi, hypercorn, pydantic, httpx,
#     boto3 for the broker pod (same image, helm picks the
#     entrypoint).
#
# Future singleton modules added by the owner may need additional
# bits (e.g. a Snowflake source-singleton would want
# ``dagster-snowflake`` or similar). Add those individually here
# as needed — but resist re-introducing the full ``orchestrator``
# extras group unless every transitive package is genuinely used.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system \
        ".[broker]" \
        dagster \
        dagster-aws \
        dagster-postgres \
        acryl-datahub \
        boto3 \
        s3fs

# Default port for the Dagster gRPC code-location.
EXPOSE 4000

# Default mode: Dagster gRPC code-location for the iagent webserver
# to connect to. The broker pod overrides this via container.command
# in the iagent helm chart's templates/user-deployments.yaml.
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "dag_tools.user_deployment.definitions"]
