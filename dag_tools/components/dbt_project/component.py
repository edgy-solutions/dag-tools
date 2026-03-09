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

@dataclass
class CustomDbtProjectComponent(DbtProjectComponent):
    """A custom DbtProjectComponent that executes Datahub lineage ingestion 
    and applies standardized prefix stripping."""
    
    datahub_config: Annotated[
        Optional[Dict[str, Any]],
        Resolver.default(description="Datahub ingest configuration (requires 'server').")
    ] = None

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

        # 1. dbt source snapshot-freshness
        freshness_invocation = dbt.cli(["source", "snapshot-freshness"], context=context)
        yield from freshness_invocation.stream()
            
        # extract platform
        target_platform = "postgres" # fallback
        try:
            target_platform = freshness_invocation.manifest['metadata'].get('adapter_type', 'postgres')
        except Exception:
            pass

        # 2. dbt build (applies partition/cli-args resolution from base class)
        build_invocation = dbt.cli(self.get_cli_args(context), context=context)
        yield from build_invocation.stream()
        
        target_path = Path(build_invocation.target_path)
            
        # 3. Handle datahub docs 
        # copy run results since documentation build will clobber them
        shutil.copyfile(
            target_path.joinpath("run_results.json"), 
            target_path.joinpath("run_results_build.json")
        )
            
        yield from dbt.cli(["docs", "generate"], context=context, target_path=target_path).stream()
            
        # 4. Publish to datahub
        self._publish_to_datahub(target_path, context, target_platform)

    def _publish_to_datahub(self, run_dir: Path, context: AssetExecutionContext, target_platform: str) -> None:
        """Constructs a transient yaml recipe and executes the external DataHub cli tool."""
        try:
            url = self.datahub_config.get("server", "http://localhost:8080")
            datahub_url = url.get_value() if isinstance(url, EnvVar) else url

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
                    'config': {
                        'server': datahub_url
                    }
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
