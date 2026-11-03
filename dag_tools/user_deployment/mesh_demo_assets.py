"""Mesh-aware demo asset — synthetic Customer 360 / mesh_demo_customers data.

Relocated from pub-tools (which is reserved for real customer-domain
data-plane pipelines like PUB LOG). This module is loaded by
``definitions.py`` only when ``DAG_TOOLS_DEMO_MODE`` is on; otherwise
it's not imported and the synthetic asset is structurally absent
from the running deployment.

Why a flat module instead of a dagster ``Component`` + ``component.yaml``:

* ``build_component_defs`` is deprecated (breaking_version 0.2.0) —
  even pub-tools' pattern is going away.
* The Component-discovery API derives a module path from the
  components directory's ``parent.name + name``, which assumes a
  shallow two-level structure (``<package>/components``). The
  dag-tools user-deployment lives at
  ``dag_tools/user_deployment/`` — a deeper nesting — so the
  derived import path lands on a non-existent module and fails.
* A flat ``Definitions(assets=..., resources=...)`` is what most
  Dagster code looks like anyway, so dropping the Component
  scaffolding is also a simplification, not just a workaround.

When the owner adds the real source-singleton code, the same
flat-Definitions pattern works — each singleton module exports an
``asset`` (or ``assets``) constant plus its IO manager, and
``definitions.py`` merges them.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

from dagster import (
    Definitions,
    asset,
)

from dag_tools.io_managers.arrow import (
    ConfigurableArrowIOManager,
    S3FSCommonConfig,
    S3FSConfig,
)


# Lazy ``polars`` import inside the asset compute keeps definitions-
# load fast. Importing polars at module level was part of what pushed
# ``dag_tools.user_deployment.definitions`` import time past Dagster's
# 60-second gRPC launch_run budget; see
# ``[[dag-tools-grpc-import-timeout]]`` for the full diagnosis. The
# IO manager applies the same discipline at
# ``dag_tools.io_managers.cortex_io_manager``.
#
# Note: ``context`` is intentionally left unannotated. The
# @asset decorator's validator rejected the explicit
# ``AssetExecutionContext`` annotation in this build of dagster
# even though it's in the documented accept-list — likely a version
# skew between the dagster the validator runs and the type imported
# here. Leaving the annotation blank is documented as acceptable and
# avoids the validator entirely. Type-checkers still infer the right
# type from the @asset decorator's signature.
@asset(
    name="mesh_demo_customers",
    group_name="mesh_demo",
    io_manager_key="mesh_demo_io",
)
def mesh_demo_customers(context):
    """Synthetic customer dataset.

    Real-shaped rows (id, name, region, signup_date, plan) generated
    locally so the materialization has no external dependencies — fast,
    deterministic, repeatable. The IO manager handles the actual S3
    write and attaches the URN + physical_uri metadata for the broker
    and downstream consumers to pick up.
    """
    # Lazy import — see module-level note.
    import polars as pl

    df = pl.DataFrame(
        {
            "id": list(range(1, 11)),
            "name": [
                "Avery Stone", "Blair Tate", "Casey Quinn", "Drew Pine",
                "Emery Lane", "Finley Cross", "Gray Marsh", "Hollis Reed",
                "Indigo Vale", "Jules Banner",
            ],
            "region": [
                "US-East", "US-West", "EU-North", "US-East", "EU-South",
                "APAC", "US-West", "EU-North", "APAC", "US-East",
            ],
            "signup_date": [
                "2024-01-15", "2024-02-03", "2024-02-19", "2024-03-04",
                "2024-03-22", "2024-04-08", "2024-04-29", "2024-05-11",
                "2024-05-28", "2024-06-12",
            ],
            "plan": [
                "pro", "starter", "enterprise", "pro", "starter",
                "pro", "enterprise", "starter", "pro", "enterprise",
            ],
        }
    )
    context.log.info(
        "mesh_demo_customers materialized: %d rows, %d columns",
        df.height,
        df.width,
    )
    return df


# Synthetic earned-value rows: one row per (work_package, period), period
# amounts and never cumulative. Everything that names a program lives in the
# CSV, so nothing in code does. The strings are declared rather than inferred:
# a control account such as ``1.1`` would otherwise arrive as a float. The
# amounts keep the seed's integer and float split, because a reader that prints
# its inputs prints ``12000000.0`` for a float budget and the audit strings would
# then differ from the seed's though no figure does.
EV_FIXTURE_CSV = Path(__file__).parent / "demo_data" / "ev_fixture.csv"
EV_FIXTURE_STRING_COLUMNS: Tuple[str, ...] = (
    "program_id",
    "program_name",
    "control_account",
    "work_package",
    "period",
)
EV_FIXTURE_INTEGER_COLUMNS: Tuple[str, ...] = (
    "program_bac",
    "bcws",
)
EV_FIXTURE_FLOAT_COLUMNS: Tuple[str, ...] = (
    "bcwp",
    "acwp",
)


@asset(
    name="ev_fixture",
    group_name="mesh_demo",
    io_manager_key="mesh_demo_io",
    description=(
        "Synthetic earned-value fixture for exercising a governed read. "
        "Not a real program's figures."
    ),
)
def ev_fixture(context):
    """Materialize the synthetic earned-value rows as parquet."""
    import polars as pl

    df = pl.read_csv(
        EV_FIXTURE_CSV,
        schema_overrides={
            **{c: pl.String for c in EV_FIXTURE_STRING_COLUMNS},
            **{c: pl.Int64 for c in EV_FIXTURE_INTEGER_COLUMNS},
            **{c: pl.Float64 for c in EV_FIXTURE_FLOAT_COLUMNS},
        },
    )
    context.log.info(
        "ev_fixture materialized: %d rows, %d columns",
        df.height,
        df.width,
    )
    return df


def build_demo_defs() -> Definitions:
    """Build a Definitions containing the synthetic demo assets and their
    IO manager.

    The demo asset PRODUCES data, so it uses a producer IO manager —
    ``ConfigurableArrowIOManager`` (parquet on S3). That manager
    implements ``physical_coordinates``, so the domain broker can
    truthfully advertise this asset's location to the central gateway
    and mesh consumers can read it back.

    (It deliberately does NOT use ``CortexPolarsIOManager``: that one is
    the mesh's read-only facade — it's bound to assets you *consume*,
    which may be owned by another deployment, so it must never announce
    ownership. See that module's docstring.)

    All config comes from env vars set by the helm chart's
    ``userDeployments.dag-tools.codeLocation.env`` block:

    * ``DAG_TOOLS_DEMO_S3_BUCKET`` (default ``dag-lake``) — the MinIO
      bucket the parquet lands in. **Must exist in the cluster's MinIO
      before the first materialization** (the chart's
      ``minio-bucket-init`` Helm hook creates it).
    * ``MESH_DEMO_PREFIX`` (default ``mesh_demo``) — path prefix
      under the bucket.
    * ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` /
      ``AWS_ENDPOINT_URL`` / ``AWS_DEFAULT_REGION`` — MinIO credentials
      and endpoint. These are the same values the broker pod carries, so
      the advertised routing ticket resolves for consumers too.
    """
    s3_bucket = os.getenv("DAG_TOOLS_DEMO_S3_BUCKET", "dag-lake")
    prefix = os.getenv("MESH_DEMO_PREFIX", "mesh_demo")
    endpoint = os.getenv("AWS_ENDPOINT_URL", "")

    io_manager = ConfigurableArrowIOManager(
        uri_base=f"s3://{s3_bucket}/{prefix}",
        fs=S3FSConfig(
            common=S3FSCommonConfig(
                access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
                secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
                end_point=endpoint,
                region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
                # MinIO in-cluster is plain HTTP; real AWS/S3 is HTTPS.
                allow_http=endpoint.startswith("http://"),
            ),
        ),
    )

    return Definitions(
        assets=[mesh_demo_customers, ev_fixture],
        resources={"mesh_demo_io": io_manager},
    )
