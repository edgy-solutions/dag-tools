# dbt project (CO-DBT-PROJECT)

The dbt project component extends Dagster's own dbt project component. It does two things the base does not. It turns each dbt node's name into an asset key through one shared normalization registry. When a DataHub setting is present, it also runs freshness, build and docs, then hands the artifacts to the DataHub command-line tool.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **With no DataHub setting it is Dagster's dbt component with different asset keys.** When `datahub_config` is unset, `execute` hands the run to the base class unchanged. The asset spec is still rewritten: each node's key comes from the shared normalization registry. A model's key starts with `dbt`. A source's key starts with a connector's path when its identifier begins with a known prefix, and that prefix is removed from the identifier. Otherwise it starts with a connector name when the source's name begins with one, and otherwise with nothing. The database, the schema and the identifier follow. `built`
2. **With a DataHub setting it runs three dbt commands into one directory.** It runs source freshness first, then the build with the component's resolved arguments, then docs generation. All three write to the directory the first command chose, so the DataHub tool finds every artifact in one place. The build's results are copied aside before docs generation, which overwrites them. Docs generation is given no run context, so the catalog covers the whole project rather than only this run's selection. The reasons are in AGENTS.md §4i. `built`
3. **Only the build receives the operator's dbt arguments.** Freshness and docs generation are fixed commands, so an argument such as `--empty` reaches the build alone. Under `--empty`, dbt tests run against empty tables and pass without checking data. AGENTS.md §4i names this hazard. `built`
4. **The DataHub token is written in plain text to a file in the dbt target directory, and the file is not removed.** The token comes from `datahub_config.token`, else from the `DATAHUB_TOKEN` variable. It is placed in the sink section of `recipe.yaml`, which is written into the run's target directory. Nothing deletes that file afterwards. No token key is written when there is none. `built`
5. **The DataHub tool runs as a child process with the whole environment of the Dagster process.** It is started with the run directory as its working directory. Its full output is copied into the run log. `built`
6. **A failed publish never fails the run.** A non-zero exit is logged as an error. Any exception in the publish step is caught and logged. The dbt materializations have already been reported by then. This is deliberate: a catalog outage must not cost the tables. `built`
7. **The normalization registry can be changed by any code in the process.** `register_prefix` writes into a class-level map, so a later registration changes keys for every dbt component loaded after it. No code at the pin calls it. The DataHub lineage component reads the same registry's prefixes. Building a key can also change the map. A source whose identifier begins with a mapped prefix takes the map's own list as its key path, and its database and schema, when set, and its identifier are appended to that list. Within one process, each later source matching the same prefix therefore gets a key that carries every earlier one's parts; a new process starts again from the mapped list. Sources matching no prefix, and models, are not affected. `built`
8. **It can size the dbt run from environment variables.** When `k8s_resource_env_prefix` is set, four variables under that prefix are read at load into the op's Kubernetes tags, with defaults for the requests. All dbt models run in one op, so this sizes the whole run. If the base class offers no op spec hook, a warning is logged and nothing is applied. `built`
9. **The chart deploys no dbt project; the shipped code location adds one only in demo mode.** The user code location registers a demo dbt project only in demo mode. The demo uses seeds, not sources, and any failure building it yields empty definitions. `built` No chart template deploys a dbt project, turns demo mode on, or sets a DataHub address or token for it. `not built`
10. **It does not read through the gateway or the broker.** dbt reads and writes its warehouse with the profile the deployment supplies. The component issues no ticket and applies no classification label. `not built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| the base component's settings (`project`, `cli_args`, `op` and others) | the base component's | passed to Dagster's dbt project component |
| `datahub_config` | unset | turns on the freshness, build, docs and publish sequence; the keys read are `server` and `token` |
| `datahub_config.server` | a local development address | the DataHub address written into the recipe |
| `datahub_config.token` | unset, then the `DATAHUB_TOKEN` variable | the DataHub token written into the recipe |
| `k8s_resource_env_prefix` | unset | the prefix of four variables that size the dbt op |
| `k8s_default_cpu` | `500m` | the CPU request when the prefix's variable is unset |
| `k8s_default_mem` | `1Gi` | the memory request when the prefix's variable is unset |

## Gaps

- **The DataHub token is left in a file on disk** (finding 4). Anyone who can read the dbt target directory can read the token. Remedy: not on the roadmap.
- **The DataHub tool receives every variable in the process environment** (finding 5). That includes credentials it does not need. Remedy: not on the roadmap.
- **A failed publish is visible only in the run log** (finding 6). A run whose catalog publish failed shows as successful. Remedy: not on the roadmap.
- **Asset keys depend on a process-wide registry, and a source matching a prefix changes it** (finding 7). The second such source in a process gets a key that includes the first source's parts. No test covers it. Remedy: not on the roadmap.

## Not established from code

- Whether a deployment's dbt target directory, and so the `recipe.yaml` holding the token, outlives the run.
- What happens to the run when source freshness reports an error. The dagster-dbt library's defaults decide that, not this repo.
- Which warehouse a deployment's dbt profile points at, and with what credential.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-DAGSTER | is called by | in-process | none | asset definitions and run context | `dag_tools/components/dbt_project/component.py:90-100`, `:105-108`; `test_demo_dbt_surface.py`: `test_demo_asset_keys_go_through_the_normalization_registry` | `built` |
| CO-DBT | calls | child process of the dbt command line | none | three commands into one target directory | `dag_tools/components/dbt_project/component.py:123-158`; `test_dbt_datahub_artifacts.py`: `test_dbt_artifacts_are_not_scattered_across_target_directories` | `built` |
| CO-DATAHUB | calls | the catalog's ingestion command run as a child process | token from config or the environment, none when absent | build artifacts, recipe with the token | `dag_tools/components/dbt_project/component.py:181-218`; `test_dbt_datahub_artifacts.py`: `test_recipe_carries_the_datahub_token_from_the_environment`, `test_recipe_omits_the_token_when_there_is_none` | `built` |
| CO-DATAHUB-LINEAGE | writes | in-process | none | key prefixes in a shared process-wide map | `dag_tools/utils/translation_registry.py:12-21`, `:41-44`, `:56-62`; `dag_tools/components/datahub_lineage/component.py:63`; no test | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | builds dbt models as Dagster assets, and optionally publishes their lineage and schema to the catalog | — |
| How the mesh wires it | a Dagster component in a code location; the shipped code location loads one only in demo mode | — |
| Sign-in | the dbt profile's credential to the warehouse; a DataHub token from config or the environment | [NATO R7](../alignment/nato-dcra.md) |
| Data path | dbt reads and writes the warehouse directly; the DataHub tool posts to the catalog; the gateway and broker are not on it | — |
| Plane | `data-management`: transformation and catalog metadata. The curated models it builds are read in the analytical-data plane | [NATO L3](../alignment/nato-dcra.md) |
| Security role | none | — |
| Services served | catalog entries, and lineage from sources to models with column lineage | [DoD S3](../alignment/dod-dmra.md), [DoD S4](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/dbt_project/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Base execution with no DataHub setting; keys from the registry | `component.py:90-100`, `:105-108`; `dag_tools/utils/translation_registry.py:33-64` | not stated | `test_demo_dbt_surface.py`: `test_demo_asset_keys_go_through_the_normalization_registry` | `built` |
| Three commands, one directory; results copied; docs without context | `component.py:123-158` | AGENTS.md §4i | `test_dbt_datahub_artifacts.py`: `test_dbt_artifacts_are_not_scattered_across_target_directories`, `test_every_artifact_the_datahub_recipe_names_is_in_the_ingest_cwd`, `test_run_results_from_build_survive_docs_generate`, `test_docs_generate_is_not_given_a_context` | `built` |
| Operator arguments reach the build only; `--empty` hazard | `component.py:136-139` | AGENTS.md §4i | `test_dbt_empty_staging.py`: `test_resolved_cli_args_reach_only_the_build_invocation`, `test_the_non_build_invocations_are_hardcoded_and_unparameterized`, `test_empty_makes_dbt_tests_vacuous_is_documented` | `built` |
| Token in `recipe.yaml`, not removed; no key without a token | `component.py:181-210` | AGENTS.md §4i | `test_dbt_datahub_artifacts.py`: `test_recipe_carries_the_datahub_token_from_the_environment`, `test_an_explicit_token_beats_the_environment`, `test_recipe_omits_the_token_when_there_is_none`; no test that the file is removed | `built` |
| Child process with the full environment; output to the log | `component.py:212-218` | not stated | no test | `built` |
| A failed publish is logged, not raised | `component.py:220-224` | AGENTS.md §4i | no test of the swallowing itself | `built` |
| Process-wide registry; no caller of `register_prefix`; a matching source appends to the map's list | `dag_tools/utils/translation_registry.py:12-21`, `:41-44`, `:56-62`; `dag_tools/components/datahub_lineage/component.py:63` | not stated | no test | `built` |
| Op sizing from a variable prefix | `component.py:30-50`, `:52-88` | not stated | `test_k8s_env_prefix.py`: `test_dbt_component_get_op_spec_injects_env_prefix`, `test_dbt_component_survives_a_missing_private_hook` | `built` |
| Demo-only wiring; no chart wiring | `dag_tools/user_deployment/definitions.py:61-67`, `:232`, `:246`; `dag_tools/user_deployment/demo_dbt_assets.py:143-234` | AGENTS.md §4i | `test_demo_dbt_surface.py`: `test_no_dbt_assets_when_demo_mode_is_off`, `test_demo_mode_registers_the_dbt_surface`, `test_the_demo_project_declares_no_sources` | `built`; the absence `not built` |
| No ticket, no label | `component.py:102-224` | not stated | — | `not built` |
