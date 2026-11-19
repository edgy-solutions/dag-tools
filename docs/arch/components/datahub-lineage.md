# DataHub lineage (CO-DATAHUB-LINEAGE)

The DataHub lineage component registers what Dagster produces in the DataHub catalog. It builds one sensor, from the DataHub Dagster plugin. After a run, the sensor reads that run's asset materializations from the Dagster instance and writes a dataset for each one to DataHub, with its properties, its upstream lineage and, when the asset published one, its column schema. It adds no assets, checks or jobs.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It builds one sensor and nothing else, and the component's sensor starts stopped.** `build_defs` returns one sensor made by the plugin's `make_datahub_sensor`. The component's `default_status` is `STOPPED` unless set to `RUNNING`; any other value is read as `STOPPED`. The name and minimum interval can be set. A test builds the component and checks that one sensor comes back, and checks both status values. `built`
2. **The deployed code location turns the sensor on, and only when a server address is set.** The code location's helper returns no definitions when `DATAHUB_SERVER` is unset, and when the plugin is not installed. Otherwise it builds the component with that address and a status read from `DATAHUB_SENSOR_STATUS`, which defaults to `RUNNING`. It passes no token, so the token comes from the environment (finding 7). `built`
3. **It reads the run's materializations, the parents' latest materializations and the asset graph.** For the run that triggered it, it reads the log for asset materialization events. For each one it reads the metadata keys `destination_name` (the platform the producer wrote to), `datahub_urn` (an explicit dataset address), `datahub.inputs` (extra upstream addresses) and a table schema. Every other metadata value that has a `value` is written to the dataset as a string property. For each upstream it also reads the parent keys from the asset graph and the parent's latest materialization. `built`
4. **By default it writes whole aspects to the physical dataset, not patches.** For an asset with a resolved platform it always sends three aspects: dataset properties (description, an external link taken from the first property that starts with `http`, and the properties), status, and sub-type `Table`. Upstream lineage is sent when the asset has upstreams, so a root asset gets none. A column schema is sent when the asset published one. Each is a proposal carrying a whole aspect, not a patch. A failed send is logged as a warning and the next one is tried. `built`
5. **Datasets are named on the physical platform, with two exceptions that use the Dagster platform.** The producer's declared platform is translated to DataHub's name (an `s3_delta` table becomes `delta-lake`, for example), and a name nothing recognises passes through as given. By default no Dagster-platform dataset is written alongside it. An asset that declares no platform gets no physical dataset and is written by the plugin as a Dagster-platform dataset. A parent with no physical location is named on the Dagster platform in the upstream list. The component's `emit_dagster_assets` switch, off by default, writes both. AGENTS.md §4l rules the physical URN as the asset of record for the components that build their own URNs; the code here follows that rule except in the no-platform case, which is a gap. No test covers the default physical-only path. `built`
6. **A platform the producer did not declare is catalogued without a physical dataset, never as `unknown`.** The code logs that it is skipping the physical half. A comment in the code says an `unknown`-platform dataset used to be created again on every run. A test pins that an absent name resolves to `unknown`, and the component then declines to use it. `built`
7. **The token comes from config, then `DATAHUB_TOKEN`, and an empty one is never sent.** An explicit `token` in `datahub_config` is used if it is non-empty; a Dagster `EnvVar` is resolved. Otherwise the environment variable is read. The token is added to the client config only when it is truthy, so an empty string is omitted. It is read once, when the definitions are built. The connection goes straight to DataHub and not through the central gateway or the domain broker. A test builds the component with the environment variable set, but does not check that the token reaches the client. The dbt publisher's matching rule (AGENTS.md §4i) has its own tests. `built`
8. **A catalog failure never takes the code location down; what happens inside a sensor tick was not read.** If building the component raises, the code location's helper logs an error and returns no definitions. A test pins this. The per-aspect sends in finding 4 are guarded. The plugin's own `emit_asset` call, used on the other path, is not guarded in this component. Because the sensor runs after a run, a failure there is not a failure of the run, but the plugin's sensor code was not read. `built`
9. **Upstream lineage is the union of the asset graph and `datahub.inputs`.** Assets that declare dependencies normally publish no `datahub.inputs`, so the graph is read too. The graph read is best-effort. A failure to read it gives no graph lineage and does not stop the emit. A test pins the dependency edge, the root asset and the degrade-without-abort case. `built`
10. **The platform and layout tables are code defaults with a config override.** `platform_mappings` is checked before the built-in tables. `filesystem_platforms` decides whether a name is laid out as a path or a dotted identifier. The tests pin that every mapped filesystem target is listed and that mapping does not change a dataset's name. One test checks that the broker's address for an asset equals the catalog's for the same asset. `built`
11. **A missing plugin or missing server is not refused by the component.** The plugin imports sit in a `try` that ignores `ImportError`, so without the plugin building fails later on an unbound name. The code location's helper checks for the plugin first for this reason. `server` defaults to an empty string when `datahub_config` is absent or has no server, and the component raises no error of its own for that. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `datahub_config` | none; `server` is described as required, `token` optional | DataHub address and token |
| `environments` | a list of five environment names | key segments read as the dataset's fabric |
| `platforms` | a list of three database platform names | key segments read as the platform |
| `filesystem_platforms` | the module's list of path-style platforms | platforms whose dataset names are a path |
| `log_platform_mappings` | one metadata key mapped to one platform | metadata keys that imply a platform when `destination_name` is absent |
| `platform_mappings` | empty | overrides for producer platform to DataHub platform |
| `emit_dagster_assets` | `false` | also write a Dagster-platform dataset |
| `default_status` | `STOPPED` | whether the sensor starts running |
| `sensor_name` | the plugin's name | name of the sensor |
| `minimum_interval_seconds` | not set | minimum time between evaluations |
| `DATAHUB_TOKEN` (env) | unset | token when none is in `datahub_config` |
| `DATAHUB_SERVER`, `DATAHUB_SENSOR_STATUS` (env, code location helper) | unset / `RUNNING` | turn the sensor on, and its status |

## Gaps

- **An asset with no declared platform is registered on the Dagster platform** (finding 5). AGENTS.md §4l says the asset of record is the physical platform's dataset and never a Dagster-platform one. This component does not meet it for these assets, or for parents with no physical location. Remedy: not on the roadmap.
- **No test pins the default write path or the token reaching the client** (findings 4, 5 and 7). Tests cover the platform tables, the sensor build and status, the code location's wiring and the helpers. None runs the extractor, and none checks what is sent to DataHub. Remedy: not on the roadmap.

## Not established from code

- Whether the plugin's sensor is triggered by run status or by materialization events, and what it does when its own sends fail. The plugin source was not read.
- Whether a whole aspect sent here replaces what DataHub held for that aspect, and whether the schema aspect overwrites one a DataHub source crawler wrote for the same dataset.
- What a DataHub with authentication on returns to a request with no token, or with a wrong one.
- Which deployments set `DATAHUB_SERVER` and `DATAHUB_TOKEN`, and whether the token is delivered as a plain value or from a secret.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-DAGSTER | is called by | in-process | none | a sensor tick after a run | `dag_tools/components/datahub_lineage/component.py:507-545`; no test | `built` |
| CO-DAGSTER | reads | in-process Dagster instance API | none | run events, parent materializations and the asset graph | `dag_tools/components/datahub_lineage/component.py:507-545`; no test | `built` |
| CO-DATAHUB | calls | HTTP | token from config, else from the environment; none sent when empty | whole aspects per dataset | `dag_tools/components/datahub_lineage/component.py:615-630, :715-731`; no test | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | registers each materialized asset and its lineage in the catalog under the physical dataset's address | [DoD S3](../alignment/dod-dmra.md), [S4](../alignment/dod-dmra.md) |
| How the mesh wires it | a sensor in the Dagster code location, turned on by environment variables in the deployed location, or by a component file in a deployment's own | — |
| Sign-in | a bearer token from config or the environment, sent to DataHub; none is sent if none is set | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct from the Dagster process to DataHub; the gateway and broker are not on it | — |
| Plane | data management, where shared lifecycle services keep metadata | [NATO L3](../alignment/nato-dcra.md) |
| Security role | none; it writes metadata and decides no access | — |
| Services served | dataset identity and lineage records for the catalog | [DoD S1](../alignment/dod-dmra.md), [S11](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/datahub_lineage/`; tests under `dag_tools_tests/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| One sensor, stopped by default, status settable | `component.py:437-446`, `:749-763` | not stated | `test_datahub_catalog_sensor.py`: `test_component_builds_a_sensor`, `test_sensor_can_start_running` | `built` |
| Code location enables it from environment | `dag_tools/user_deployment/definitions.py:169-228`, `:270` | not stated | `test_datahub_catalog_sensor.py`: `test_no_sensor_when_datahub_server_unset`, `test_sensor_registered_and_running_when_configured`, `test_sensor_status_is_overridable` | `built` |
| Reads run log, parent materializations, asset graph | `component.py:507-510`, `:521-545`, `:574`, `:273-286`, `:320-325` | not stated | `test_datahub_catalog_sensor.py`: `test_table_schema_is_extracted_for_the_catalog`, `test_extract_upstream_urns_handles_every_metadata_shape`, `test_datahub_urn_override_path_is_executable` | `built` |
| Whole aspects written to the physical dataset | `component.py:170-237` (upstream lineage conditional at `:211`, schema at `:223`), `:615-630` | not stated | `test_datahub_catalog_sensor.py`: `test_upstream_url_becomes_a_clickable_external_url`, `test_non_http_locations_are_not_offered_as_links`; no test of the send | `built` |
| Physical platform by default; Dagster platform for no-platform assets and parents | `component.py:256-291`, `:338-340`, `:414-424`, `:530-568`, `:632-644` | AGENTS.md §4l | `test_broker_physical_urn_identity.py`: `test_broker_and_catalog_agree_on_identity`; no test of the default path | `built` |
| No physical dataset when platform undeclared | `component.py:548-566`; `platforms.py:106-110` | not stated | `test_datahub_platforms.py`: `test_absent_name_is_unknown` | `built` |
| Token order; empty never sent; direct connection | `component.py:715-731`; `definitions.py:216-219` | AGENTS.md §4i | `test_datahub_catalog_sensor.py`: `test_token_is_picked_up_from_env` (does not check the client) | `built` |
| Build failure does not fail the code location; sends guarded | `definitions.py:211-228`; `component.py:233-237`, `:632-644` | not stated | `test_datahub_catalog_sensor.py`: `test_catalog_failure_does_not_break_the_code_location` | `built` |
| Graph plus `datahub.inputs` upstreams, best-effort | `component.py:294-343`, `:582-594` | not stated | `test_datahub_catalog_sensor.py`: `test_graph_upstreams_capture_a_deps_edge`, `test_graph_upstreams_empty_for_a_root_asset`, `test_graph_upstreams_degrade_rather_than_abort` | `built` |
| Platform and layout tables with override | `platforms.py:36-84`, `:87-115`; `component.py:398-435` | not stated | `test_datahub_platforms.py`: `test_overrides_win`, `test_mapping_preserves_the_dataset_name_layout`, `test_every_mapped_filesystem_target_is_listed` | `built` |
| Plugin and server not checked by the component | `component.py:36-44`, `:713-718`; `definitions.py:198-209` | not stated | `test_datahub_catalog_sensor.py`: `test_optional_datahub_imports_do_not_swallow_dagster_symbols` | `built` |
