import logging
import os
import shutil
import yaml
from pathlib import Path
from dataclasses import dataclass
from subprocess import Popen, PIPE, STDOUT
from typing import Annotated, Any, Dict, Iterator, Mapping, Optional

import dagster as dg
from dagster import AssetExecutionContext, EnvVar
from dagster.components.resolved.model import Resolver
from dagster_dbt import DbtCliResource, DbtProjectComponent

from dag_tools.utils.translation_registry import AssetNormalizationRegistry

logger = logging.getLogger(__name__)


@dataclass
class CustomDbtProjectComponent(DbtProjectComponent):
    """A custom DbtProjectComponent that executes Datahub lineage ingestion
    and applies standardized prefix stripping."""

    datahub_config: Annotated[
        Optional[Dict[str, Any]],
        Resolver.default(description="Datahub ingest configuration (requires 'server').")
    ] = None

    k8s_resource_env_prefix: Annotated[
        Optional[str],
        Resolver.default(description=(
            "Env-prefix convention for the dbt run's k8s resources (matches "
            "the deployment pattern used elsewhere in the fleet). When set, "
            "resolve <PREFIX>_CPU_REQUEST / _MEM_REQUEST / _CPU_LIMIT / "
            "_MEM_LIMIT from the code-location environment into the generated "
            "@dbt_assets op's `dagster-k8s/config` tag at defs-load time. The "
            "deployment sets those four env vars (Helm `env:`) and the YAML "
            "just names the prefix. Any explicit `op.tags` are deep-merged on "
            "top. All dbt models run in one op, so this sizes the whole dbt run."
        ))
    ] = None
    k8s_default_cpu: Annotated[
        str, Resolver.default(description="Fallback CPU request when the prefix's "
                                          "<PREFIX>_CPU_REQUEST env var is unset.")
    ] = "500m"
    k8s_default_mem: Annotated[
        str, Resolver.default(description="Fallback memory request when the prefix's "
                                          "<PREFIX>_MEM_REQUEST env var is unset.")
    ] = "1Gi"

    def _get_op_spec(self, project: Any) -> Any:
        """Inject env-prefix-resolved k8s resources into the @dbt_assets op
        tags, deep-merging any explicit `op.tags` on top.

        ``_get_op_spec`` is a PRIVATE hook on ``DbtProjectComponent`` and
        does not exist in every dagster-dbt: on 0.26.19 (the release that
        pairs with Dagster 1.10.19) the base class has no such method, and
        calling ``super()`` there raises ``AttributeError: 'super' object
        has no attribute '_get_op_spec'`` at definition-load time -- so the
        whole code location fails to load, not just this component.

        Overriding a private hook means accepting that it can move. When
        it is absent the k8s tag injection is skipped and the component
        still builds; losing a resource hint is a far better outcome than
        an unloadable location. Caught by the CI job that pins Dagster to
        the 1.10.19 floor.
        """
        base = getattr(super(), "_get_op_spec", None)
        if base is None:
            logger.warning(
                "dagster-dbt's DbtProjectComponent has no _get_op_spec on this "
                "version; skipping k8s resource-tag injection for "
                "env_prefix=%r. Upgrade dagster-dbt to restore it.",
                self.k8s_resource_env_prefix,
            )
            return None
        op_spec = base(project)
        if not self.k8s_resource_env_prefix:
            return op_spec
        from dag_tools.utils.k8s import resolve_op_tags_with_env_prefix
        merged = resolve_op_tags_with_env_prefix(
            env_prefix=self.k8s_resource_env_prefix,
            explicit_tags=getattr(op_spec, "tags", None),
            default_cpu=self.k8s_default_cpu,
            default_mem=self.k8s_default_mem,
        )
        return op_spec.model_copy(update={"tags": merged})

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: Any
    ) -> dg.AssetSpec:
        """Override to inject our custom AssetKey translation rules natively into Dagster."""
        base_spec = super().get_asset_spec(manifest, unique_id, project)
        node_info = self.get_resource_props(manifest, unique_id)
        
        # Apply the centralized registration normalizations
        new_key = AssetNormalizationRegistry.apply(node_info)
        
        return base_spec.replace_attributes(key=new_key)

    def execute(self, context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator:
        """Override to inject the datahub publishing step during the dbt build."""
        
        if not self.datahub_config:
            # Fall back to standard native execution if not integrated
            yield from super().execute(context, dbt)
            return

        # 1. dbt source snapshot-freshness -> sources.json
        #
        # DataHub ingests a *directory* of dbt artifacts: the recipe built in
        # _publish_to_datahub names manifest.json / catalog.json /
        # sources.json / run_results relative to a single cwd. But
        # `DbtCliResource.cli()` mints a FRESH `target/<op>-<run>-<uuid>`
        # directory for every invocation unless handed an explicit
        # `target_path` (dagster_dbt's `_get_unique_target_path`). Left alone,
        # the three dbt calls below scatter their artifacts across three
        # directories and `datahub ingest` dies on the first file it cannot
        # find -- in practice sources.json, which only `source
        # snapshot-freshness` writes. So pin the first invocation's directory
        # and thread it through the rest; then every artifact lands together.
        freshness_invocation = dbt.cli(["source", "snapshot-freshness"], context=context)
        yield from freshness_invocation.stream()

        target_path = Path(freshness_invocation.target_path)

        # extract platform
        target_platform = "postgres" # fallback
        try:
            target_platform = freshness_invocation.manifest['metadata'].get('adapter_type', 'postgres')
        except Exception:
            pass

        # 2. dbt build (applies partition/cli-args resolution from base class)
        build_invocation = dbt.cli(
            self.get_cli_args(context), context=context, target_path=target_path
        )
        yield from build_invocation.stream()

        # 3. Handle datahub docs
        # copy run results since documentation build will clobber them
        shutil.copyfile(
            target_path.joinpath("run_results.json"),
            target_path.joinpath("run_results_build.json")
        )

        # Deliberately NO `context=` here. dagster-dbt appends the context's
        # selection (`--select fqn:*`) to any invocation it is given a
        # context for, and that selection does not match sources -- so the
        # catalog came out with models only and DataHub warned "Node missing
        # from catalog: source.<project>.<table>", losing source column
        # schema. A catalog is not a run: it should describe the whole
        # project regardless of which assets this run materialized. Without
        # a context there is also no chance of re-emitting materialization
        # events the build already yielded, so `.wait()` replaces `.stream()`
        # (docs generate produces no node results to stream anyway).
        dbt.cli(["docs", "generate"], target_path=target_path).wait()

        # 4. Publish to datahub
        self._publish_to_datahub(target_path, context, target_platform)

    def _publish_to_datahub(self, run_dir: Path, context: AssetExecutionContext, target_platform: str) -> None:
        """Constructs a transient yaml recipe and executes the external DataHub cli tool."""
        try:
            def _resolve(value: Any) -> Any:
                return value.get_value() if isinstance(value, EnvVar) else value

            datahub_url = _resolve(
                self.datahub_config.get("server", "http://localhost:8080")
            )

            # A PAT is required when the metadata service runs with
            # METADATA_SERVICE_AUTH_ENABLED=true (the correct posture, and
            # what the sandbox cluster runs). Same resolution order as
            # DatahubLineageComponent: explicit config, else the standard
            # DATAHUB_TOKEN env var. Without it every request 401s -- and
            # because `infer_dbt_schemas` reads schemaMetadata back out of
            # GMS *before* emitting, the failure surfaces as a source-side
            # HTTPError with zero events produced, not as a sink error.
            token = _resolve(self.datahub_config.get("token")) or os.environ.get(
                "DATAHUB_TOKEN"
            )

            sink_config: Dict[str, Any] = {'server': datahub_url}
            if token:
                sink_config['token'] = token

            recipe = {
                'source': {
                    'type': 'dbt',
                    'config': {
                        'manifest_path': "./manifest.json",
                        'catalog_path': "./catalog.json",
                        'sources_path': "./sources.json",
                        'run_results_paths': ["./run_results_build.json"],
                        "include_column_lineage": True,
                        "infer_dbt_schemas": True,
                        'target_platform': target_platform
                    }
                },
                'sink': {
                    'type': 'datahub-rest',
                    'config': sink_config
                }
            }
            
            recipe_path = run_dir / 'recipe.yaml'
            with open(recipe_path, 'w') as file:
                yaml.dump(recipe, file)

            cmd = [shutil.which("datahub") or "datahub", 'ingest', '-c', str(recipe_path)]
            
            process = Popen(cmd, cwd=run_dir, env={ **os.environ }, stdout=PIPE, stderr=STDOUT)
            output, _ = process.communicate()
            
            if output:
                context.log.info(output.decode("utf-8") if isinstance(output, bytes) else str(output))
            
            if process.returncode != 0:
                context.log.error(f"DataHub ingestion failed with return code {process.returncode}")
                
        except Exception as e:
            context.log.error(f"Failed to publish to DataHub: {e}")
