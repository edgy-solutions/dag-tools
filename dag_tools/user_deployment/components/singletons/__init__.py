"""Singleton source assets — global surfaces (Snowflake, etc.).

This directory is the home for ``DAG_TOOLS_DEMO_MODE`` off-mode
content: basic singleton source assets that are global in the mesh
(e.g. "list all Snowflake assets in this account"). It is currently
empty and will be filled in by the owner. The empty state is
intentional — when demo mode is off and no singletons exist yet, the
user-deployment registers no assets, which is the honest production
state for a mesh that has no globally-available source surfaces wired
yet.

When code is added here, the parent ``definitions.py`` discovers it
via the Dagster components scanner; no registration boilerplate
needed beyond the component module itself.
"""
