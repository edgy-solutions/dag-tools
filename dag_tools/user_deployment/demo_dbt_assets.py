"""Demo dbt surface for the dag-tools user-deployment.

Wired into ``definitions.py`` only when ``DAG_TOOLS_DEMO_MODE`` is on, the
same switch that governs ``mesh_demo_assets``. Production clusters set the
flag off and get none of this.

It exists because the ``CustomDbtProjectComponent`` -> DataHub publishing
path had no deployed exercise anywhere: the user-deployment built zero dbt
assets, and ``examples/real_declarative_dbt`` pointed at dbt projects that
did not exist in the repo. Two bugs (artifacts scattered across per-
invocation target directories, and no PAT sent to an authenticated GMS)
therefore shipped and survived. A demo surface that actually runs the
component end to end in a cluster is what makes the next one visible.

Two deliberate choices:

**Seeds, not sources.** ``execute()`` runs ``dbt source snapshot-freshness``
*before* ``dbt build``, so a source pointing at a raw table would fail the
run before the build could create that table. Seeds ship their data inside
the image and build first, so the project is self-contained against an
empty database. The cost is that ``sources.json`` comes out with empty
results -- dbt still writes the file, which is what the ingest needs.

**Public dagster-dbt API only.** The assets are built with ``@dbt_assets``
delegating to ``CustomDbtProjectComponent.execute`` -- exactly what
``DbtProjectComponent``'s own generated asset function does. Going through
``build_defs`` instead would mean depending on ``StateBackedComponent``
state plumbing and the private ``_project_manager``; this deployment has
already been taken down once by a private dagster-dbt hook moving
(``_get_op_spec``), and a code location that fails to load stops every
materialization in the deployment, not just this surface.

Everything here is failure-tolerant for the same reason: any problem
building the surface is logged and yields empty ``Definitions``.
"""

import logging
import os
from pathlib import Path
from typing import Any, Mapping

from dagster import AssetExecutionContext, Definitions

logger = logging.getLogger(__name__)

# The dbt project shipped alongside this module (packaged via
# pyproject's package-data glob, so it survives the wheel build).
DEMO_DBT_PROJECT_DIR = Path(__file__).parent / "demo_dbt"

# dbt writes target/ and logs/ into the project dir. Site-packages is
# usually read-only in a container, so both are redirected somewhere
# writable. DAGSTER_HOME is the natural home; /tmp is the last resort.
_TARGET_ROOT_ENV = "DAG_TOOLS_DEMO_DBT_TARGET_DIR"


def _writable_target_root() -> Path:
    explicit = os.getenv(_TARGET_ROOT_ENV)
    if explicit:
        return Path(explicit)
    dagster_home = os.getenv("DAGSTER_HOME")
    if dagster_home:
        return Path(dagster_home) / "demo_dbt_target"
    return Path(os.getenv("TMPDIR", "/tmp")) / "dag_tools_demo_dbt_target"


def _build_translator():
    """A translator applying the same key normalization as the component.

    ``CustomDbtProjectComponent.get_asset_spec`` routes every node through
    ``AssetNormalizationRegistry``; the ``@dbt_assets`` path does not call
    that method, so without this the demo surface would produce different
    asset keys than the YAML-configured component for the same project.
    """
    from dagster_dbt import DagsterDbtTranslator

    from dag_tools.utils.translation_registry import AssetNormalizationRegistry

    class _NormalizingTranslator(DagsterDbtTranslator):
        def get_asset_key(self, dbt_resource_props: Mapping[str, Any]):
            return AssetNormalizationRegistry.apply(dbt_resource_props)

    return _NormalizingTranslator()


def _demo_component_cls():
    """``CustomDbtProjectComponent`` with cli-arg resolution made optional.

    ``get_cli_args`` resolves templates through a ContextVar that only the
    YAML component loader sets, and that ContextVar has no default -- so
    reading it from this hand-assembled surface raises ``LookupError`` at
    materialization time. Fall back to the literal args when it is unset,
    and keep the real resolution when it is available so a YAML-loaded
    instance of this class behaves exactly as before.
    """
    from dag_tools.components.dbt_project.component import (
        CustomDbtProjectComponent,
    )

    class _DemoDbtComponent(CustomDbtProjectComponent):
        def get_cli_args(self, context):
            try:
                return super().get_cli_args(context)
            except LookupError:
                return [arg for arg in self.cli_args if isinstance(arg, str)]

    return _DemoDbtComponent


def build_demo_dbt_defs() -> Definitions:
    """Build the demo dbt assets, or empty Definitions if anything is off.

    Never raises: this is merged into the deployment's Definitions, and an
    exception here would take the whole code location down.
    """
    try:
        from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
    except ImportError as exc:
        logger.warning(
            "demo dbt surface skipped: dagster-dbt is not installed (%s). "
            "Install the `orchestrator` extra to enable it.", exc,
        )
        return Definitions()

    if not DEMO_DBT_PROJECT_DIR.is_dir():
        logger.warning(
            "demo dbt surface skipped: %s is missing from the image; the "
            "project is package data and may have been dropped by the build.",
            DEMO_DBT_PROJECT_DIR,
        )
        return Definitions()

    try:
        _DemoDbtComponent = _demo_component_cls()

        target_root = _writable_target_root()
        target_root.mkdir(parents=True, exist_ok=True)

        project = DbtProject(
            project_dir=DEMO_DBT_PROJECT_DIR,
            profiles_dir=DEMO_DBT_PROJECT_DIR,
            target_path=target_root,
        )
        # Parse at load time so the manifest exists without a separate
        # image-build step. `dbt parse` does not open a warehouse
        # connection, so this works even when postgres is unreachable --
        # the failure then surfaces at materialization, where an operator
        # can see it, rather than as an unloadable code location.
        project.preparer.prepare(project)

        datahub_server = os.getenv("DATAHUB_SERVER")
        # Constructed through the dataclass, NOT __new__: only `project` is
        # required and every inherited field then gets its declared default.
        # Bypassing __init__ leaves those unset, and the base class reads
        # them at run time -- with DATAHUB_SERVER unset, execute() delegates
        # to super().execute(), which touches `include_metadata` and dies
        # with AttributeError mid-materialization.
        component = _DemoDbtComponent(
            project=project,
            cli_args=["build"],
            datahub_config=(
                {"server": datahub_server} if datahub_server else None
            ),
            k8s_resource_env_prefix=os.getenv("DAG_TOOLS_DEMO_DBT_K8S_PREFIX"),
        )

        @dbt_assets(
            manifest=project.manifest_path,
            project=project,
            dagster_dbt_translator=_build_translator(),
            name="dag_tools_demo_dbt",
        )
        def _demo_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
            yield from component.execute(context=context, dbt=dbt)

        logger.info(
            "demo dbt surface registered from %s (datahub=%s)",
            DEMO_DBT_PROJECT_DIR,
            datahub_server or "disabled",
        )
        return Definitions(
            assets=[_demo_dbt_assets],
            resources={
                "dbt": DbtCliResource(
                    project_dir=DEMO_DBT_PROJECT_DIR,
                    profiles_dir=str(DEMO_DBT_PROJECT_DIR),
                    target_path=target_root,
                )
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "demo dbt surface failed to build (%s); continuing without it so "
            "the rest of the code location still loads.", exc,
        )
        return Definitions()
