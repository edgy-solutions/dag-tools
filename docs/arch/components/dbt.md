# dbt (CO-DBT)

dbt is the SQL transformation tool the mesh uses to build tables from other tables inside a warehouse. dag-tools reaches it in four ways: a component that runs a dbt project as Dagster assets, a shipped dbt package for one curated slice, a SQL route in contract mapping that renders dbt models and tests, and a demo project. No chart deploys a dbt project. This record says what each of the four provides. The [dbt project record](dbt-project.md) has the detail of the component.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **The dbt project component runs a deployment's own dbt project.** It runs three dbt commands into one target directory, gives only the build the operator's arguments, and can publish the result to the catalog (see [the dbt project record](dbt-project.md), findings 2, 3 and 6). Its dbt runs are not reached through the gateway or the broker (that record, finding 10). `built`
2. **A dbt package for the curated slice ships inside the library.** It holds three curated models, their schema file, one hand-written macro, the tests generated from the slice's grain and reference rules, and one more generated test that counts bounds whose comparison operator the deployment's table does not cover; that one warns rather than fails. It declares a default for none of its four required variables, so an unset variable fails at parse rather than taking a guess. Its SQL is generated from the same rule table the Python route reads, and tests hold the shipped files to the generator byte for byte and the two routes to the same answers. The reasons are in AGENTS.md §4r. `built`
3. **Contract mapping renders dbt models and tests from a mapping.** Its SQL route writes a model, a schema file and singular tests into a directory a deployment names. The reasons are in AGENTS.md §4k, and the [contract mapping record](contract-mapping.md) has the detail. `built`
4. **The demo project builds from seeds, is demo-gated, and by default writes into the Dagster database.** It is registered only in demo mode. It uses seeds rather than sources, so it needs no raw table first. Its connection comes from its own variables and falls back to the Dagster instance's Postgres variables, and it writes into a schema of its own. With the invincible-agent chart's defaults, demo mode is on and only the Dagster variables are set, so the demo models land in the Dagster instance's database. Any failure to build it leaves the code location without it rather than failing the load. `built`
5. **A dbt model's asset key starts with `dbt`; a source's may start with a connector name or with nothing.** The asset-key normalization registry maps three identifier prefixes to connector paths, for Airbyte's raw tables, other Airbyte tables and dlt, and reports `dbt` plus those connectors as its known top-level prefixes. Code may register more prefixes at run time. A source whose identifier matches a mapped prefix takes that connector's path; one that does not, but whose source name begins with a connector name, takes that name; any other source gets a key of its database and schema, when set, and its identifier, with no leading prefix. The demo's keys pass through the registry. `built`
6. **A custom dbt translator puts an asset in its own qualification class.** The class key adds the names of any custom translator classes, for assets that use the dbt integration only. A survey therefore never treats two dbt assets as equivalent when either uses a custom translator. The reasons are in AGENTS.md §4e. `built`
7. **The dbt integration is part of the orchestrator extra.** It requires the Dagster dbt integration and the Postgres adapter. No other adapter is a dependency; a deployment adds its own. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| dbt project component `project`, `cli_args` | required; the integration's default, a plain build | the project to run and the build's arguments (see [the dbt project record](dbt-project.md)) |
| curated package variables | none; all four required | the two input relations, the numeric type and the comparator table |
| contract mapping target directory | set by the deployment | where the rendered models and tests are written |
| demo mode | off in code; `"true"` in the invincible-agent chart | registers the demo project |
| demo connection variables | fall back to the Dagster instance's Postgres variables | where the demo project writes |
| demo dbt target directory | unset; then under `DAGSTER_HOME`; then a temporary directory | where the demo project's dbt artifacts go |

## Gaps

- **The demo project writes into the Dagster instance's database by default** (finding 4). Remedy: not on the roadmap.
- **Only the Postgres adapter ships** (finding 7). A deployment on another warehouse supplies its own adapter. Remedy: not on the roadmap.

## Not established from code

- Which dbt projects a deployment runs, and against which warehouse.
- Whether a deployment installs the curated package, and with which variables.
- Whether a deployment runs the contract-mapping SQL route.
- Whether the demo project's target directory survives a pod restart; that depends on what a deployment mounts at `DAGSTER_HOME`.
- What a failed source freshness command does to the run that follows it.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-DBT-PROJECT | is called by | child process of the dbt command line | none | build, freshness and docs commands | `dag_tools/components/dbt_project/component.py:123-158`; `test_dbt_empty_staging.py`: `test_resolved_cli_args_reach_only_the_build_invocation` | `built` |
| XE-SQL-WAREHOUSE | reads | SQL through the dbt adapter | the warehouse credential in the project profile, from the environment | models and tests over tables | `dag_tools/user_deployment/demo_dbt/profiles.yml:11-18`; `test_demo_dbt_surface.py`: `test_demo_mode_registers_the_dbt_surface` | `built` |
| XE-SQL-WAREHOUSE | writes | SQL through the dbt adapter | the warehouse credential in the project profile, from the environment | built models | `dag_tools/user_deployment/demo_dbt/profiles.yml:11-18`; `test_demo_dbt_surface.py`: `test_demo_mode_registers_the_dbt_surface` | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | builds tables from tables inside a warehouse, and tests them | — |
| How the mesh wires it | run by a component in a Dagster code location; no chart deploys a project | — |
| Sign-in | the warehouse credential in the project's profile, from the environment | [NATO R7](../alignment/nato-dcra.md) |
| Data path | inside the warehouse; no data leaves it through dbt | — |
| Plane | `analytical-data`: the shipped dbt package builds curated models that are read for decisions, which is what the DCRA's L4 row corresponds to. Running a transformation is a data-management service, and CO-DBT-PROJECT is placed there | [NATO L4](../alignment/nato-dcra.md) |
| Security role | none built | — |
| Services served | data quality tests, mediation between source and contract shapes, and lineage from its manifest | [DoD S13](../alignment/dod-dmra.md), [DoD S14](../alignment/dod-dmra.md), [DoD S4](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| The dbt project component | `dag_tools/components/dbt_project/component.py` | AGENTS.md §4i | see [the dbt project record](dbt-project.md) | `built` |
| Curated package; no variable default; byte identity; the uncovered-operator test warns | `dag_tools/dbt_project_packages/mfg_trace_curated/dbt_project.yml:15-22`; `dag_tools/systemlink/sql.py:723`, `:734` | AGENTS.md §4r | `test_systemlink_curated_sql.py`: `test_every_generated_file_is_shipped_byte_for_byte`, `test_every_measurement_case_agrees_between_the_two_routes`, `test_the_shipped_package_is_on_a_path_no_vcs_ignore_rule_matches`, `test_the_package_declares_a_default_for_no_required_variable`, `test_the_uncovered_comparator_test_warns_rather_than_failing` | `built` |
| Contract mapping SQL route | `dag_tools/contract_mapping/emit.py:903` | AGENTS.md §4k | `test_contract_mapping_component.py`: `test_nothing_is_written_when_the_render_fails`; `test_contract_mapping_proof.py`: `test_the_scaled_columns_carry_a_cast_so_the_contract_can_be_enforced` | `built` |
| Demo project: seeds, gated, falls back to the Dagster database | `dag_tools/user_deployment/demo_dbt/profiles.yml:11-18`; `dag_tools/user_deployment/demo_dbt_assets.py:54`, `:60-67`, `:143-158`; invincible-agent `helm/invincible-agent/values.yaml:1305`; `helm/invincible-agent/templates/user-deployments.yaml:119-128` | AGENTS.md §4i | `test_demo_dbt_surface.py`: `test_no_dbt_assets_when_demo_mode_is_off`, `test_demo_mode_registers_the_dbt_surface`, `test_the_demo_project_declares_no_sources`, `test_a_missing_project_directory_degrades_instead_of_raising` | `built` |
| Three mapped prefixes; `dbt` for models; no prefix for an unmatched source | `dag_tools/utils/translation_registry.py:12-15`, `:19-21`, `:24-30`, `:35`, `:40-53` | not stated | `test_demo_dbt_surface.py`: `test_demo_asset_keys_go_through_the_normalization_registry` | `built` |
| Custom translator, own class | `dag_tools/qual/classes/key.py:9-14`, `:102-106` | AGENTS.md §4e | — | `built` |
| Orchestrator extra | `pyproject.toml:94-95` | not stated | — | `built` |
