"""Dagster Upgrade Regression & Qualification System.

This subpackage contains the desktop CLI (`dagtools`) and the MinIO/S3
registry that backs it. The continuous CI survey publishes per-build
structural inventories to the registry; the operator-driven qualification
workflow reads them to drive baseline-vs-candidate comparisons.

See ``docs/RECIPE.md`` for the full system spec and architectural decisions.

Modules:
  * ``registry`` — MinIO/S3 layout contract, client, and staleness reporter.
  * ``cli``      — the ``dagtools`` Typer application.

Phase rollout (see RECIPE):

  * Phase 1 step 1 — shared inventory contract (``dag_tools.inventory``).
  * Phase 1 step 2 — registry + ``dagtools registry status``.  ← here
  * Phase 1 step 3 — survey + ``dagtools survey``.
  * Phase 2 — qualification (Q0..Q6).
"""
