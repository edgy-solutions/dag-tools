# Grist ingest (CO-GRIST-INGEST)

The Grist ingest component copies tables from a Grist instance into a SQL database. A sensor lists the Grist documents and their tables and starts one run per table. Each run downloads the document, reads one table and writes it to the destination as a whole-table replace. The destination is written by the SQL IO manager, with a pandas frame.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **The component builds one partitioned asset, one job, one sensor and two resources.** The asset is `<name>_ingest`, with `<name>` defaulting to `grist`. It has one dynamic partition set, named `<name>_tables` unless set. The job selects that asset. The resources are a Grist resource and a SQL IO manager. No asset check is defined. Tests assert the asset, sensor, job and both resource keys, and the partition set name. `built`
2. **The sensor is stopped by default and polls every sixty seconds.** `default_status` is `STOPPED`, and any value other than `RUNNING` (any case) also gives stopped. `minimum_interval_seconds` is 60. No test checks the default status or the interval. `built`
3. **Discovery reads the Grist REST API, and only reads.** Three routes are called, all with GET: the org's workspaces, a document's tables, and a document's download. The workspace listing carries each document's update time, and a document is kept only if that time is later than the sensor's cursor. The component never writes to Grist. `built`
4. **Each table becomes a friendly partition key that is also the destination table name.** The key joins the workspace name (optional), the document name and the table id, each lower-cased to letters, digits and underscores, joined by `__` and cut to 63 characters. A collision within one sweep gets a short suffix from the table id. The opaque Grist document and table ids travel in run config, not in the key. Tests cover the normalisation, the length limit, the collision suffix and the run config. `built`
5. **The cursor moves before the runs are known to succeed.** The sensor takes the newest update time among the documents it saw and stores it as the cursor in the same tick that requests the runs. A document with no tables still advances it. The run key is the update time with the document and table ids, so one document revision requests one run per table. A run that fails is not requested again for that revision. A test checks that the cursor advances. No test covers a failed run. `built`
6. **The asset downloads the whole document and reads one table.** It fetches the document as a SQLite file into a temporary file, opens it read-only and runs one select on the table. It downloads the document twice per table. The first request is a plain request with the per-request timeout; it follows any redirect, reads the whole document and discards it, and its status is not checked. The second downloads the same document again, streamed, with the timeout doubled, and raises an error when the status is not HTTP 200, so the data is not written. A run over N tables of one revision downloads the whole document 2N times. The asset itself is not tested, and neither is the download. `built`
7. **The data lands through the SQL IO manager, which replaces the table.** The destination protocol, host, database, user and password are required YAML; port and schema are optional. `write_style` defaults to replace, so each run replaces the table named by the partition key. The IO manager also emits the column schema as output metadata, and the destination platform name when the protocol is one the mesh can route; the catalog sensor can read both. No test runs a write. `built`
8. **The Grist token and the database password come from the component YAML, and nothing is required of an environment variable.** The Grist host, org and token are required, with a scheme (default `https`) and a timeout (default 30 seconds). The token is sent as a bearer header on every request. The deployed code location resolves `{{ env.NAME }}` references in the file against the container's environment, so secrets can stay out of the file. An unset variable becomes an empty string with a warning. `built`
9. **It connects directly, and presents a static credential.** The Grist calls use `requests` straight to the configured host with the token. The SQL write opens its own connection with the database user and password. The [central gateway](central-gateway.md) and the [domain broker](domain-broker.md) are not on either path, so no ticket is issued. `built`
10. **The deployed code location loads it only when a config file is named, and off means nothing is imported.** With `DAG_TOOLS_GRIST_CONFIG` unset, the file missing, unparseable, empty or `enabled: false`, the location gets empty definitions and the Grist code is not imported. It is independent of the demo switch and is merged with the base and catalog surfaces. Tests cover unset, missing file, `enabled: false`, the build with env resolution, and the merge. No test covers an unparseable or empty file. `built`
11. **A bad config fails the code location load, while a bad token or download fails a run or a tick.** A config that parses but is invalid makes the component constructor raise, and the code location re-raises it. A failed workspace listing raises inside the sensor, so that tick errors. A failed table load fails that run. Missing required fields (`grist`, `postgres`) fail at construction. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `host` in the `grist` block | required | Grist host, no scheme |
| `org` in the `grist` block | required | organisation whose workspaces are listed |
| `token` in the `grist` block | required | bearer token for the Grist API |
| `scheme` in the `grist` block | `https` | URL scheme |
| `timeout_seconds` in the `grist` block | `30` | per-request timeout; the snapshot download waits twice this |
| `postgres.protocol`, `.host`, `.database`, `.username`, `.password` | required | destination for the SQL IO manager |
| `postgres.port` | none | left out of the connection address when absent |
| `postgres.schema` | none | default schema for the tables |
| `postgres.write_style` | `replace` | how the table is written; also `append` or `fail` |
| `postgres.get_source_fn` | none | name of a resolver for the destination table; falls back to the default |
| `name` | `grist` | base name for the asset, sensor, job and resources |
| `partition_name` | `<name>_tables` | dynamic partition set name |
| `include_workspace_in_name` | `true` | put the workspace first in the partition key |
| `minimum_interval_seconds` | `60` | minimum time between sensor ticks |
| `default_status` | `STOPPED` | `RUNNING` or `STOPPED` for the sensor |
| `DAG_TOOLS_GRIST_CONFIG` (code location) | unset | path to the config file; unset turns the surface off |
| `enabled` (config file) | `true` when the file exists | `false` turns the surface off |

## Gaps

- **A failed table run is not retried by the sensor** (finding 5). The cursor has passed that document revision, and the run key for it has been used. The table is read again only when the document changes. Remedy: not on the roadmap. Status: `built`.
- **The download path has no test** (findings 6 and 7). The tests replace the client, so nothing covers the download, the redirect or the SQLite read. Remedy: not on the roadmap. Status: `built`.

## Not established from code

- Which Grist instance, organisation and database any deployment points at.
- How the config file is mounted into the pod. A search of `helm/` at the pin found no Grist template or value.
- Whether the Grist token is a read-only token. The code only reads, but it cannot show what the token permits.
- Whether the destination tables are read back through the mesh. Whether the catalog sensor registers them was not read for this record.
- Whether the download endpoint's redirect is followed with the token in the header, to the address the redirect names. The code does that, and the behaviour of any particular server was not checked.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-DAGSTER | is called by | in-process | none | a sensor, a job and a partitioned asset | `dag_tools/components/grist_ingest/component.py:107-150`; no test | `built` |
| CO-GRIST | calls | HTTP | bearer token from the component file | GET routes for workspaces, tables and a document download | `dag_tools/resources/grist.py:61-125`; no test | `built` |
| XE-SQL-DATABASE | writes | SQL through a database driver | user and password from the component file | one table per partition key | `dag_tools/io_managers/sql.py:175-178, :232-234`; no test | `built` |
| CO-DATAHUB-LINEAGE | writes | in-process output metadata | none | destination name and column schema | `dag_tools/io_managers/sql.py:271-299`; no test | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | copies tables from a Grist instance into a SQL database, one asset partition per table | — |
| How the mesh wires it | a Dagster component, loaded by the code location only when a config file is named; the sensor starts stopped | — |
| Sign-in | a bearer token to Grist and a database user and password to the destination, both from the component's YAML (which may refer to environment variables) | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct from Grist to the pod and from the pod to the destination database; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | `operational-data`: where a producer's data enters the mesh | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none; it reads with one token and writes with one database account | — |
| Services served | the destination tables advertise their platform and columns for the catalog sensor | [DoD S3](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/grist_ingest/`; the resource is `dag_tools/resources/grist.py`; the deployed wiring is `dag_tools/user_deployment/definitions.py`; the IO manager is `dag_tools/io_managers/sql.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Asset, job, sensor, two resources; partition set | `component.py:107-150`, `:208-218` | not stated | `test_grist_component.py`: `test_build_defs_produces_asset_sensor_job_resources`, `test_ingest_asset_is_dynamic_partitioned` | `built` |
| Sensor stopped by default; 60 seconds | `component.py:97-105`, `:152-163` | not stated | no test | `built` |
| Reads only, three GET routes | `resources/grist.py:61-125` | not stated | no test of the resource | `built` |
| Friendly key, collision suffix, ids in run config | `discovery.py:21-60`, `:75-130`; `component.py:184-195` | not stated | `test_grist_discovery.py`: `test_normalize_lowercases_and_replaces_nonalnum`, `test_friendly_name_is_valid_pg_identifier_length`, `test_discover_disambiguates_friendly_name_collisions`; `test_grist_component.py`: `test_sensor_emits_friendly_partitions_and_run_config` | `built` |
| Cursor advanced in the requesting tick | `component.py:170-182`, `:198-206`; `discovery.py:126` | not stated | `test_grist_component.py`: `test_sensor_emits_friendly_partitions_and_run_config`; `test_sensor_skips_when_no_new_docs`; no test of a failed run | `built` |
| Two whole-document downloads per table, one table read, error on failure of the second | `resources/grist.py:91-125`; `component.py:134-146` | not stated | no test | `built` |
| Lands through the SQL IO manager, replace | `component.py:214-216`; `io_managers/sql.py:141-151`, `:219-234`, `:256-262`, `:271-299` | not stated | no test of a write | `built` |
| Token and password from YAML; env references resolved | `resources/grist.py:29-36`, `:56-57`; `definitions.py:55-58`, `:70-93`, `:153` | AGENTS.md §5 | `test_user_deployment_grist.py`: `test_resolve_env_templates_whole_string`, `test_resolve_env_templates_unset_becomes_empty`, `test_grist_builds_and_resolves_env` | `built` |
| Direct connections, no gateway or broker | `resources/grist.py:71`, `:100-106`; `io_managers/sql.py:175-178`, `:232-234` | not stated | no test | `built` |
| Loaded only when a config file is named | `definitions.py:109-166`, `:266-270` | not stated | `test_user_deployment_grist.py`: `test_grist_disabled_by_default`, `test_grist_missing_file_is_disabled`, `test_grist_enabled_false_is_disabled`, `test_grist_merged_into_combined_defs`; no test of an unparseable file | `built` |
| Failure points | `definitions.py:161-166`; `resources/grist.py:72`; `component.py:141-146`, `:64-68` | not stated | no test | `built` |
