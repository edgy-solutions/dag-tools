"""Dag-tools user-deployment package.

This is a Dagster code-location image that any cluster can spin up as
a `userDeployments` entry via the iagent helm chart. Same shape as
``pub_tools`` but lives inside the dag-tools repo so it can host
**generic source-asset surfaces** (Snowflake all-assets, etc.) that
are not tied to a single customer-domain pipeline.

Two modes, switched by ``DAG_TOOLS_DEMO_MODE``:

* **demo mode on** (sandbox / dev) — registers the synthetic
  ``mesh_demo_customers`` dataset under ``components/demo/`` so the
  bar-chart demo path has somewhere to live. The mock data lands in
  MinIO via ``CortexPolarsIOManager`` at the same path it always
  used; only the deployment that owns the asset has moved (pub-tools
  → dag-tools).
* **demo mode off** (production) — only the ``components/singletons/``
  surface registers. That directory is currently empty; future code
  (provided by the owner) populates it with basic singleton source
  assets that are globally available in the mesh — e.g. "list all
  Snowflake assets in this account" — and they materialize through
  the same ``CortexPolarsIOManager`` so consumers reach them via the
  central gateway exactly like any other mesh asset.

The split keeps demo content out of customer-domain pub-tools (which
exists to demonstrate the mesh-publishing protocol against real
customer-domain pipelines like PUB LOG) and gives the singleton
source surfaces a clean home that doesn't muddle with customer
domains.
"""
