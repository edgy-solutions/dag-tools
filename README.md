# `dag-tools`

This repository serves as the central hub for common Dagster utilities, resources, IO managers, sensors, and asset patterns used across all of our data tooling projects.

## Project Purpose
Rather than duplicating infrastructure logic (such as configuring connection strings, handling file formats, or defining generic S3 bucket sensors) across multiple repositories, `dag-tools` provides a unified, typed, and easily importable library of standard Dagster components. 

Other projects (e.g., `pub-tools`) rely on this repository for their core pipeline scaffolding.

## Design Philosophy
This library follows a **Dagster-first** configuration approach. 
1. **Config Normalization**: Components MUST wrap native settings of underlying tools (like `dlt` or `dbt`) into standardized Dagster configuration schemas. 
2. **Internal Translation**: The component's `build_defs()` is responsible for translating these standardized Dagster inputs into the format required by the external tool. 
3. **Consistency**: Downstream users should interact with a consistent Dagster-centric experience regardless of the specific integration being used.

## Structure
- **[Changelog: CHANGELOG.md](CHANGELOG.md)** — what changed in each release, and whether it changes an existing signature, export or default. Starts at 0.11.0; earlier releases are in the git history between the `v*` tags.
- **[Surveying deployed code locations: docs/surveying-deployed-code-locations.md](docs/surveying-deployed-code-locations.md)** — running `survey` where the secrets are: why CI cannot do it, why you survey the BASELINE containers only, and the minimal install that does not perturb the container you are measuring.
- **[Private/self-signed TLS endpoints: docs/restate-worker-ca-certs.md](docs/restate-worker-ca-certs.md)** — `restateWorker.caCerts` / `centralGateway.caCerts` in the Helm chart: injecting a CA bundle, why it takes two env vars and a merge rather than one, and how to confirm it took.
- **[Local dev loops: docs/local-dev-loops.md](docs/local-dev-loops.md)** — iterating on dbt and dlt without the Dagster UI: `dagtools dlt run --destination ./dev.duckdb`, `--extract-only`, `dagtools dlt env`, and how to do it without copying credentials onto a laptop.
- `dag_tools/components/`: Dagster 1.12 GA Declarative Components using the `Component, Resolvable, Model` pattern (e.g., `DltPipelineComponent`, `CustomDbtProjectComponent`, `GristIngestComponent`) that allow users to deploy complex workloads via YAML.
- `dag_tools/audit/`: The **read audit** — a pure (Dagster-free) engine that turns a warehouse's query history into a report of which asset read which object, and which reads nothing explains. Builds the SELECT and returns SQL plus bound parameters; a resource runs it. Names no relation, column or identity, not even as a default. Surfaced as `ReadAuditComponent` (YAML) and `dagtools audit` (CLI).
- `dag_tools/contract_mapping/`: The **contract mapping** engine — pure (no Dagster, no dbt) — that reconciles a data contract against a source schema and renders the dbt model that satisfies it plus the tests that prove it still does. The same plan also runs over Python rows, which is how a mapping is proven with no warehouse. Refuses at code-location load anything that would emit a wrong artifact, notes anything it merely could not test, and guesses no warehouse type, cast or predicate. Surfaced as `ContractMappingComponent`.
- `dag_tools/warehouse_source/`: The **warehouse source** engine — a pure (Dagster-free, DataHub-free) package that reads the deployment's mounted classification dimensions document, validates each column's `{dimension: values}` label against it, plans an observation that moves no data, and builds the catalog payload. Surfaced as `WarehouseSourceComponent`, which publishes one source dataset as an observable source asset under the PHYSICAL platform's dataset URN and attaches the Dagster asset as lineage. Names no database, schema, table, column, dimension or value, not even as a default.
- `dag_tools/io_managers/`: Custom Dagster IO Managers.
- `dag_tools/resources/`: Reusable resources and API/Database clients, including the **tagged warehouse** resource (`TaggedWarehouseComponent`) — every query it sends carries which asset the read was for, and a query it cannot attribute is not sent.
- `dag_tools/sensors/`: Common sensors (S3, file system, etc.).
- `dag_tools/utils/`: Assorted helper functions, centralized `AssetNormalizationRegistry`, and logging utilities.
- `dag_tools/restate_handlers/`: Durable Data Plane services (Restate) for SAP and Database synchronization.
- `dag_tools/inventory/`: The **shared structural-inventory contract** for Dagster assets — a versioned `AssetRecord` schema, an FQN-based IO manager classifier with MRO walking, and a soft-failing extractor that walks a `Definitions`. Used both by the runtime Domain Broker (for IO manager classification) and by the `dagtools survey` CLI (for per-build inventory published to MinIO). Evolution is additive-only; bump `SCHEMA_VERSION` on every change. See `dag_tools/inventory/schema.py` for the rules.
- `dag_tools/qual/`: The **Dagster Upgrade Regression & Qualification System** — the `dagtools` console-script (Typer-based) and its MinIO/S3 registry. Now shipped (Phase 1 steps 1–3):
  - `dagtools qual init` — Q0 of Phase 2: pin the registry's current inventory snapshot, record the baseline/candidate version pair (with explicit pin sets), diff non-Dagster pins into `co_upgrade_risks[]` so a hidden dbt-core bump can't masquerade as a Dagster regression, and write the immutable manifest to both the registry and `~/.dagtools/quals/<id>/manifest.yaml`. Pass `--graphql-url` (the test deployment's Dagster GraphQL endpoint) and `--location-name` (the code-location name that deployment exposes, e.g. the user-deployment / gRPC server name) so Q2/Q4 launches target the right place — the launcher falls back to `"default"` otherwise, which no real deployment uses.
  - `dagtools qual classes` — Q1: read the manifest's pinned inventories, group every fleet asset into an equivalence class by the recipe key (compute kind, IO manager FQN, partitioning, resources, integration libs, asset checks, automation condition, plus custom dbt translator FQNs), pick representatives per class (preferring `regression: "true"` tagged, spanning ≥2 repos), label each rep `RUNNABLE` / `SYNTHETIC_REQUIRED` / `OBSERVE_ONLY`, report each rep's staging need (`none_needed` / `required` / `unknown`, from the inventory's `upstream_asset_keys`) plus a `launchable_now_count` of reps Q2 can launch with no staged upstream data (report-only; it gates nothing), and publish both `equivalence_classes.json` and a human-readable `.md` companion. The class hash is deterministic so the same fleet shape always produces the same matrix.
  - `dagtools qual init --probe-config PATH` supplies constructor config for the synthetic probes' IO managers (YAML, keyed by IO manager FQN; secrets as `{env: VAR}`, rendered as `os.environ[...]` and never baked into probe source); a `resource_config` block in the same file opts individual non-IO-manager resources into the probe (by resource class FQN). See [docs/RECIPE.md](./docs/RECIPE.md), Q5.
  - Staging data for Q2 is documented in [docs/RECIPE.md](./docs/RECIPE.md) ("Staging data for Q2"): dbt representatives build schema-only via `cli_args: ["build", "--empty"]` or `DBT_EMPTY=true` (dbt tests then pass vacuously, so parity there is not evidence of data correctness); all other representatives use `qual init --staging-overrides` against an operator-staged schema or bucket. Size the work with `qual classes`.
  - `dagtools qual stage --side baseline|candidate` — materialize the computed upstreams Q2's `required` representatives will load as inputs, in runs of their own (one per partition group, in dependency order), using the manifest's `staging_overrides`. `--plan` prints the selection and launches nothing; `--max-keys N` refuses an oversized one; `--force` restarts. State lives in its own registry slot; Q6 records it per side but never gates on it. Run it per side, before `qual run`. See [docs/RECIPE.md](./docs/RECIPE.md), "Staging data for Q2".
  - `dagtools qual run --side baseline|candidate` — Q2 (and Q4 once the test deployment is bumped): launch each RUNNABLE representative through the test deployment's Dagster GraphQL, poll to a terminal status, pull the event log, persist a per-rep `RunRecord` (materializations, asset-check results, metadata-key union, failure step keys), maintain `~/.dagtools/quals/<id>/<side>-state.json` and mirror it to the registry so a desktop crash is recoverable. Re-invocation skips PASSED reps and reconciles LAUNCHED reps via run-id lookup rather than re-launching. `--retry-failed` / `--only-class` for surgical re-runs.
  - `dagtools qual preflight --side baseline|candidate` — Q3: the gate operators run after upgrading the test deployment. Checks via GraphQL: the deployment reports the expected version (manifest's baseline/candidate; wildcard `1.12.x` accepts `1.12.5`); every code location is in `LOADED` state with the per-location error surfaced on failure; each `inventory_pins` entry is matched by a code location stamped with that repo and git sha in `workspace.yaml` `additional_metadata` (keys configurable via `qual init --repo-metadata-key/--sha-metadata-key`; exact or >=7-char prefix; unstamped locations are informational); on the candidate side, a deterministic sample of PASSED baseline runs still renders via `pipelineRunOrError` (event-log back-compat spot check). Publishes `preflight.json` immutably; exits non-zero on any failed check.
  - `dagtools qual synthetic` — Q5 (generation): for every `SYNTHETIC_REQUIRED` equivalence class, emit a self-contained Dagster module that imports the real IO manager FQN (with an `InMemoryIOManager` fallback so the code location always loads), defines a deterministic upstream/downstream asset pair under a class-unique `io_manager_<short>` resource key (so N probes merge cleanly with no collision), and asserts the payload survives the IO manager round-trip. Publishes one source per class plus `probe_manifest.json` to `qualifications/<qual_id>/probes/` (manifest written last) AND a local copy under `~/.dagtools/quals/<id>/probes/`. `--skip-publish` / `--skip-local` pick one side.
  - `dag_tools.probes_location.definitions` — the deployable `dag-tools-probes` Dagster code location. Operators point their test deployment's workspace at it and set `DAGTOOLS_PROBES_DIR` to the bundle dir; the location dynamically loads every `<class_hash>.py`, merges into one `Definitions`, and soft-fails per probe so one broken file doesn't block the whole location. Loads as an empty `Definitions` when no probes are deployed, so the operator can deploy the location once and add bundles over time.
  - Round-trip probes — `dagtools qual synthetic` also generates one probe per RUNNABLE class that loads the representative's Q2 output through the class's real IO manager (writing nothing); `dagtools qual run` launches it after the Q2 pass on `dag-tools-probes`. Q6 compares loaded type and columns across sides; `dagtools qual report --accept-roundtrip-missing` excuses missing coverage only.
  - `dagtools qual probes run --side baseline|candidate` — Q5c: launches each probe's upstream AND downstream assets together against the test deployment's `dag-tools-probes` location (`deps` does not co-materialize the upstream, so a downstream-only run dies loading its input), polls to a terminal status, persists per-probe RunRecords under `<side>/probes/runs/<class_hash>/<run_id>.json`, and mirrors per-side state for resumability. PASSED is sacred; LAUNCHED probes reconcile via GraphQL poll rather than relaunching. Q6 then counts a synthetic class as **covered green** when its probe PASSED on both sides (no `--accept-synthetic-coverage-missing` needed), and lists any ran-and-failed probes in `synthetic_classes_red` — which blocks GO regardless of acceptance flags (the synthetic-accept flag excuses *missing* coverage, not actively failing probes).
  - `dagtools qual probes status` — Q5d: GraphQL cross-reference of the probe manifest against the test deployment's `dag-tools-probes` location. Reports per probe whether both `<module_name>_upstream` and `<module_name>_downstream` are loaded (fully_loaded), one of two is missing (partially_loaded, usually a probe-side import error), or neither is loaded (missing — operator hasn't redeployed yet, `DAGTOOLS_PROBES_DIR` points elsewhere, or the location is ERROR). Also flags `unexpected_probe_asset_keys` — probe-shaped assets loaded but absent from the current manifest, usually stale files from a prior bundle. `--exit-nonzero-on-gap` lets operator shell scripts gate on a clean deploy.
  - `dagtools qual report` — Q6: the operator payoff. Diffs every representative's baseline vs candidate `RunRecord` for success / materialization count + asset-key set / metadata KEY-set (values may differ) / asset-check parity. Rolls up to per-class verdicts (any failing rep = class red). Applies the recipe's GO criteria: candidate preflight passed + every RUNNABLE class green + (op-in) synthetic-class probe coverage + (op-in) orchestration snapshots clean + (op-in) co_upgrade_risks validated. **Strict by default** — operators explicitly accept each known gap (`--accept-orchestration-deferred`, `--accept-synthetic-coverage-missing`, `--accept-co-upgrade-risks`). Publishes both `verdict.json` and the human-readable `UPGRADE_VERDICT.md` immutably; exits non-zero on NO_GO.
  - `dagtools survey` — load every code location in a workspace.yaml / module spec with `-W all` warning capture; if **any** load fails, refuse to publish and exit non-zero; otherwise introspect assets / sensors / schedules / asset checks / IO managers / dbt projects (custom translator flagged) and publish per-build artifacts via the registry.
  - `dagtools registry status` — fleet-wide staleness report (fresh / stale / missing / unreadable per repo).
  - `S3Storage` + `InventoryRegistry` with immutable per-build keys and a **write-last** `latest.json` pointer so readers never observe a partial publish. See `dag_tools/qual/registry/layout.py` for the bucket layout contract.
  - `templates/Jenkinsfile.survey` — drop-in Jenkins stage for adding a repo to the survey fleet.

  Install via the `qual` extras: `pip install "edgy-dag-tools[qual]"` (the pip/distribution name is `edgy-dag-tools`; the import package stays `dag_tools`). **Full system spec, ADRs, and implementation status:** [docs/RECIPE.md](./docs/RECIPE.md).

## Control Plane vs. Data Plane
To ensure scalability and security, `dag-tools` enforces a strict separation between:
1. **Control Plane (Dagster)**: Orchestrates data movement, manages schedules, and handles metadata.
2. **Data Plane (Restate)**: Executes high-volume, row-level API and database mutations durably.

Data Plane workers run the shared `restate-worker` image (built from the repo-root `Dockerfile.restate-worker` and published by CI). Its env-driven entrypoint `dag_tools.restate_handlers.serve` selects which handlers to host via the `RESTATE_SERVICES` environment variable and self-registers with Restate on startup (`RESTATE_ADMIN_URL` / `RESTATE_ADVERTISED_URI`). Workers use `Hypercorn` for the mandatory HTTP/2 support required by modern Restate SDKs — no bespoke per-project entrypoint or Dockerfile is needed.

When a handler's endpoint — an API, the SAP OData host, or Restate's own admin URL — is served with a private or self-signed certificate, set `restateWorker.caCerts` in the Helm chart. It mounts your PEM files, merges them with the image's public roots in an init container, and points every TLS client at the result; `centralGateway.caCerts` is the same block for the gateway (Topaz is commonly self-signed). Both are off by default. See [docs/restate-worker-ca-certs.md](docs/restate-worker-ca-certs.md) — including why one environment variable is not enough and why the bundle is merged rather than replaced.

## Component Configuration Examples

### 1. DLT Pipeline Component
Deploy declarative full `dlt` extraction pipelines from YAML definitions natively mapped to `dag-tools/components/dlt_pipeline`. Includes IO Manager and incremental hints mappings:

```yaml
type: dag_tools.components.dlt_pipeline.DltPipelineComponent

attributes:
  source_config:
    drivername: "mssql+pyodbc"
    database: "mydatabase"
    schema: "dbo"
  dest_config:
    drivername: "snowflake"
    database: "analytics"
  pipelines:
    fast_refresh:
      io_manager_key: "snowflake_io_manager"
      sources:
        - "production"
        - "consumption"
    heavy_ingest:
      io_manager_key: "snowflake_io_manager"
      sources:
        - "big_fact_table"
      # Per-pipeline k8s resources via op_tags → dagster-k8s/config.
      # The k8s executor / run launcher reads this at run submit time.
      pool: "heavy-ingest"          # optional Dagster concurrency pool
      op_tags:
        dagster-k8s/config:
          container_config:
            resources:
              requests: {cpu: "2000m", memory: "8Gi"}
              limits:   {cpu: "4000m", memory: "16Gi"}
    env_sized_ingest:
      io_manager_key: "snowflake_io_manager"
      sources:
        - "another_table"
      # Env-prefix convention (matches the deployment pattern used
      # elsewhere in the fleet): the deployment sets ENV_SIZED_CPU_REQUEST
      # / _MEM_REQUEST / _CPU_LIMIT / _MEM_LIMIT (Helm `env:`), and the
      # YAML just names the prefix — no need to template four values.
      k8s_resource_env_prefix: "ENV_SIZED"
```

There are **three** ways to size a pipeline's k8s resources, all landing on the same `dagster-k8s/config` op_tag the launcher reads at run submit time:

1. **Literal `op_tags`** in YAML (the `heavy_ingest` example) — explicit values.
2. **`{{ env.VAR }}` templating** inside `op_tags` — Dagster's template resolver fills each value from the environment (verified to resolve at any nesting depth).
3. **`k8s_resource_env_prefix`** (the `env_sized_ingest` example) — name a single prefix; the component resolves `<PREFIX>_CPU_REQUEST` / `_MEM_REQUEST` / `_CPU_LIMIT` / `_MEM_LIMIT` from the code-location environment at defs-load time (limits default to requests). This mirrors the `resolve_k8s_resource_tags(prefix=...)` pattern used on plain `@asset`s elsewhere and is the least verbose for the common case. Explicit `op_tags` are deep-merged **on top**, so you can name a prefix for resources *and* add node selectors / tolerations (or override one value) in the same block.

For Python callers of `create_dlt_assets`, the same helper is available directly: `dag_tools.utils.k8s.resolve_k8s_resource_tags("<PREFIX>")`.

### 2. DBT Project Component
Expose fully compiled DBT projects directly to Dagster with automatic Datahub integration native to the project component:

```yaml
type: dag_tools.components.dbt_project.CustomDbtProjectComponent

attributes:
  project: "../../dbt_projects/project_one"
  datahub_config:
    server: "{{ env.DATAHUB_URL }}"
  # dbt runs all models in one op, so this sizes the whole dbt run.
  # Same env-prefix convention as the dlt component: the deployment sets
  # DBT_BUILD_CPU_REQUEST / _MEM_REQUEST / _CPU_LIMIT / _MEM_LIMIT and the
  # resolved dagster-k8s/config lands on the generated @dbt_assets op's
  # tags (explicit `op.tags` deep-merge on top).
  k8s_resource_env_prefix: "DBT_BUILD"
```

### 3. Datahub Global Lineage Tracking
To enable instance-wide asset materialization tracking for DataHub, downstream projects should define the `DatahubLineageComponent` in their `components/` directory (e.g. `components/datahub_lineage/component.yaml`):

```yaml
type: dag_tools.components.datahub_lineage.DatahubLineageComponent

attributes:
  datahub_config:
    server: "{{ env.DATAHUB_URL }}"
    
  # (Optional) Override known environment prefixes 
  environments:
    - prod
    - uat
    - sandbox
    - dev
    - test
    
  # (Optional) Override standard database platforms
  platforms:
    - clickhouse
    - snowflake
    - postgres
    
  # (Optional) Override which schemas act as filesystems vs databases (impacts dot notation)
  filesystem_platforms:
    - s3
    - abs
    - filesystem
    
  # (Optional) Dynamic mappings from dict metadata keys out of the dagster log into datahub labels
  log_platform_mappings:
    "Databricks Job Run ID": "databricks"
```

### Grist Ingest Component
Publish [Grist](https://www.getgrist.com/) documents/tables into Postgres so the pipeline can consume them. A single component wires a Grist resource, a SQL IO manager, a dynamic-partitioned ingest asset, and a discovery sensor. Each discovered table becomes a **human-friendly** dynamic partition — `<workspace>__<doc>__<table>`, normalized — which is also the destination Postgres table name; the opaque Grist doc/table ids travel in run config, not the key.

```yaml
type: dag_tools.components.grist_ingest.GristIngestComponent

attributes:
  name: crm                       # base name for the asset/sensor/job/resources
  grist:
    host: "{{ env.GRIST_HOST }}"     # e.g. grist.example.com (no scheme)
    org: "{{ env.GRIST_ORG }}"
    token: "{{ env.GRIST_TOKEN }}"
  postgres:                          # SQL IO manager destination
    protocol: postgresql
    host: "{{ env.PG_HOST }}"
    port: 5432
    database: analytics
    schema: grist
    username: "{{ env.PG_USER }}"
    password: "{{ env.PG_PASSWORD }}"
  # (Optional)
  include_workspace_in_name: true    # prefix friendly names with the workspace
  minimum_interval_seconds: 60       # sensor poll interval
  default_status: STOPPED            # or RUNNING
```

The sensor polls Grist for updated documents (cursor = the max `updatedAt` seen), registers a dynamic partition per changed table, and fires a run that loads the table into a DataFrame and writes it to `<schema>.<friendly_name>`. Rename a Grist doc and the friendly name follows it (a new table); friendly-name collisions within one sweep are disambiguated automatically so two tables never clobber one Postgres table.

**In the user-deployment container** the surface is **off by default** — no default Grist/Postgres connection can be guessed. The code-location (`dag_tools.user_deployment.definitions`) enables it only when `DAG_TOOLS_GRIST_CONFIG` points at a mounted YAML holding the `attributes` above (optionally wrapped as `{enabled: true, attributes: {...}}`). `{{ env.VAR }}` references inside that YAML are resolved against the container environment at load time, so tokens/passwords stay in k8s Secrets rather than the ConfigMap.

### 3. S3 to Arrow Storage Component
This component tracks an S3 Bucket and registers dynamic partitions for new incoming files chronologically. It triggers a PyArrow job that converts the raw bytes natively through your specified `io_manager`.

```yaml
type: dag_tools.components.s3_sensor.S3ToArrowComponent

attributes:
  partition_name: "daily_ingestion_logs"
  bucket: "my-production-lake"
  prefix: "raw_data/logs/2026"
  io_manager_key: "parquet_io_manager"
  delimiter: ","
```

### 4. S3 Sensor Component (Standalone)
A standalone sensor that monitors an S3 bucket and triggers any Dagster job with file-level `RunRequests`. It supports modern Dagster 1.12 resource configuration, allowing for custom S3 endpoints (e.g. Minio) and regex-based key filtering.

```yaml
type: dag_tools.components.s3_sensor.S3SensorComponent

attributes:
  bucket: "my-raw-data"
  prefix: "incoming/"
  target_job: "raw_ingestion_job"
  target_op: "ingest_op"
  partition_name: "landed_files"
  
  # Connect to local Minio
  s3_resource:
    endpoint_url: "http://minio:9000"
    aws_access_key_id: "admin"
    aws_secret_access_key: "password"
    
  # Only trigger for parquet files
  s3_filter: ".*\\.parquet"

  default_status: "RUNNING"
```

### 5. PyArrow DataFrame IO Manager
The `ConfigurableArrowIOManager` connects Python's memory to Datalake storage using optimized `pyarrow.fs` clients. It abstracts S3 and Local mounts seamlessly while transparently coercing results into `pa.Table`, `pa.dataset.Dataset`, or `pd.DataFrame` directly into your downstream assets.

```python
from dag_tools.io_managers import ConfigurableArrowIOManager

# Define in your Definitions resources dictionary
resources = {
    "parquet_io_manager": ConfigurableArrowIOManager(
        uri_base="s3://my-datalake/gold-tier",
        fs={
            "type_": "s3",
            "common": {
                "access_key_id": {"env": "AWS_ACCESS_KEY_ID"},
                "secret_access_key": {"env": "AWS_SECRET_ACCESS_KEY"},
                "end_point": "s3.amazonaws.com"
            }
        }
    )
}
```

### 5. Restate DLT Data Sync Component
Instantiate generic Oracle-to-Postgres syncing and auto-chunked Restate acking by writing a single YAML component definition. A pipeline may also declare a `cycle_sensor:` block — the component then emits, alongside the dlt + ack-dispatch assets, an asset job binding them and a sensor that polls the source for unprocessed rows and re-runs the job, driving the read → ack → cycle loop hands-off:

```yaml
type: dag_tools.components.restate_dlt_sync.RestateDltSyncComponent

attributes:
  restate_endpoint: "http://restate-server:8080/GenericOracleAckService/mark_as_processed/send"

  source_config:
    drivername: "oracle+oracledb"
    credentials: "{{ env.ORACLE_DSN_URL }}"
    database: "MY_COMPANY_DB"
    schema: "HR"

  dest_config:
    drivername: "postgres"
    schema: "ingested_hr"

  pipelines:
    hr_employee_data:
      primary_key: "EMP_ID"
      sources:
        - "EMPLOYEE_MASTER"
        - "DEPARTMENT_MASTER"
      # Optional: the Restate handler writes one summary row here per ack batch.
      stats_table: "HR_SYNC_STATS"
      # Optional: emit a cycle job + polling sensor for hands-off operation.
      cycle_sensor:
        enabled: true
        interval_seconds: 60
        backlog_query: "SELECT COUNT(*) FROM employee_master WHERE processed_flag = 'N'"
```

A complete, runnable stateful cycle — Oracle → dlt → Postgres → Restate ack → Oracle — with a Docker Compose stack, init SQL, and end-to-end integration tests, is in [examples/pdm_oracle_ingestion](./examples/pdm_oracle_ingestion).

### 6. Restate DLT API Sync Component
Instantiate generic SQL Server-to-External REST API syncing using stateful row-level Restate acks by defining a single YAML configuration:

```yaml
type: dag_tools.components.restate_api_sync.RestateApiSyncComponent

attributes:
  restate_endpoint: "http://restate-server:8080/GenericApiSyncService/process_record/send"
  
  source_config:
    drivername: "mssql+pyodbc"
    database: "INTERNAL_ERP"
    schema: "dbo"
    
  # Staging configuration holding new rows temporarily for API fanning
  dest_config:
    drivername: "postgres"
    schema: "api_staging_buffer"
    
  pipelines:
    sap_api_dispatch:
      primary_key: "PO_NUMBER"
      api_path: "/v1/orders"
      sources:
        - "PURCHASE_ORDERS"
```

### 7. OpenTelemetry → API Sync Component
Push **any** OpenTelemetry publication in ClickHouse to **any** ordered set of API endpoints, defined entirely in YAML. Where the two components above dispatch one payload per row, this one groups telemetry into *execution groups* and renders a whole ordered **call plan** per group — mixed batched and per-record calls, with fallbacks.

The mapping file is the domain model; the engine has no idea how many endpoints there are. Adding one is another entry under `steps:`.

```yaml
type: dag_tools.OtelApiSyncComponent

attributes:
  restate_endpoint: "{{ env.RESTATE_INGRESS_URL }}"
  source_config:
    drivername: clickhouse
    host: "{{ env.CLICKHOUSE_HOST }}"
    database: otel
  dest_config:                     # same shape as every other component
    drivername: postgresql
    host: "{{ env.POSTGRES_HOST }}"
    username: "{{ env.POSTGRES_USER }}"
    password: "{{ env.POSTGRES_PASSWORD }}"
    database: telemetry
    schema: otel_staging
  pipelines:
    ci_results:
      staged: true                 # dlt → warehouse → dispatch (false = read ClickHouse directly)
      mapping_file: mapping.yaml
      sources:
        - name: execution_spans
          query: "SELECT * FROM otel.otel_traces WHERE SpanName = 'execution.event'"
          cursor_column: Timestamp
          lookback_seconds: 600    # re-read window for late-arriving spans
          primary_key: [TraceId, SpanId]
      api:
        base_url: "{{ env.TARGET_API_BASE_URL }}"   # non-secret, per-deployment; merged over mapping.yaml's api: block
        # plan_carried_headers:                     # rare: rendered into the plan itself (see below)
        #   X-Trace-Source: dag-tools
      completion_check:
        blocking: true                              # fail the run if a dispatched plan actually failed
        timeout_seconds: 300
```

`mapping.yaml` — grouping, derived collections, then ordered steps:

```yaml
api:
  header_env:
    Authorization: "Bearer ${TARGET_API_TOKEN}"   # expanded on the WORKER, never in the plan
  # No base_url here — component.yaml's api.base_url supplies it, keeping
  # this file portable across environments.

group_by: "{{ attr(row, 'execution.group_id') }}"

readiness:
  quiet_period_seconds: 300                        # don't dispatch a group that is still filling
  complete_when: "{{ filter_rows(rows, attr('execution.terminal'), 'true') | length > 0 }}"
  max_age_seconds: 86400

derive:
  entities: "{{ distinct(rows, attr('entity.id')) }}"
  entities_by_item: "{{ group_map(rows, attr('item.name'), attr('entity.id')) }}"

steps:
  - id: entity_artifacts                           # once per entity
    for_each: "{{ entities }}"
    method: PATCH
    path: "/api/EntityMaintenance/{{ item }}"
    payload: {artifacts: "{{ join(unique(artifacts_by_entity[item]), ',') }}"}
    on_status:
      404:
        mode: aggregate                            # ONE bulk POST after the fan-out,
        path: /api/EntityMaintenance               # carrying only the items that 404'd
        collect_into: entities
        payload: {deleteMissingEntities: false, entities: []}
        fragment: {entityIdentifier: "{{ item }}"}
  - id: record_execution                           # once per event record
    for_each: "{{ rows }}"
    item_key: "{{ attr(item, 'SpanId') }}"
    path: /api/RecordExecution
    payload:
      eventDateTime: "{{ to_iso(attr(item, 'Timestamp')) }}"
      metrics: "{{ metrics_from_prefix(item, 'metric.') }}"
```

Four design points worth knowing before you write a mapping:

- **Plans render in Dagster, execute in Restate.** Materialize the dispatch asset with `dry_run: true` (a `dagster.Config` knob, alongside `limit`, `only_group`, `max_groups`, `ignore_readiness`, `ignore_ledger`) to see the exact URLs and bodies in asset metadata without sending anything. Mapping edits never require a worker redeploy.
- **Fallbacks are scoped to what actually failed.** `mode: item` retries one item elsewhere; `mode: aggregate` banks a pre-rendered fragment per failed call and issues one bulk request afterwards. Against replace-semantics bulk endpoints, a group-wide fallback would overwrite state for items whose call had just succeeded.
- **Status is data, not an exception.** The handler returns the HTTP status from inside `ctx.run` and classifies outside it: 2xx done, a status with a fallback runs the fallback, 5xx/429 raise so Restate retries, other 4xx is terminal. Raising on every non-2xx would retry a 404 forever and never reach the fallback.
- **Types survive.** Mapping expressions render through a combined native + sandboxed Jinja environment, so a single-expression template returns a real `int`/`bool`/`list`. OTel attributes are `Map(String, String)`; use `as_int`/`as_float`/`as_bool`/`split`/`metrics_from_prefix` for non-string fields.

**Non-secret API config lives in component.yaml, not the worker's environment.** component.yaml's `api:` block (`base_url`, static `headers`, `timeout_seconds`) is resolved by Dagster's own `{{ env.X }}` substitution and merged over `mapping.yaml`'s `api:` block before the mapping is validated — so `mapping.yaml` stays portable across environments and never needs its own `base_url`. Setting `api.base_url` also clears any `api.base_url_env` the mapping declared, so a worker-side env var can't silently keep winning. Secrets stay on the mapping side, as `api.header_env` — expanded from the Restate worker's environment at call time, so only the variable name ever reaches the rendered plan. The exception is `api.plan_carried_headers`, which renders straight into the plan and is therefore persisted in the Restate invocation journal; use it only when that exposure is an acceptable trade-off for a header that must travel with the plan.

**Dispatch succeeding is not the same as the plan succeeding.** `/send` is one-way — HTTP 200 means Restate accepted the plan into its journal, nothing about whether the calls inside it actually landed. The `restate_plans_completed` asset check (`completion_check:` in component.yaml) closes that gap: it reads the group/plan hashes the dispatch asset recorded in its materialization metadata and polls each group's `get_status` handler until every plan resolves or `timeout_seconds` elapses. Classification checks `last_result` before `completed_plan_hashes`, deliberately — `COMPLETED_WITH_ERRORS` plans do get their hash recorded in `completed_plan_hashes`, so checking that list first would report a partially-failed plan as a pass.

Duplicate dispatch is suppressed twice: a Dagster-side ledger of `(group, plan hash)` pairs, and the group-keyed Restate `VirtualObject`, which refuses a plan hash it has already completed. The handler ships in the shared worker image as `RESTATE_SERVICES=api_call_plan`. A failed plan is recorded in that object's state and returned normally, never raised as `restate.TerminalError` — `/send` dispatch means nothing would ever observe the raise, and a terminal failure would roll back the very state the failure record lives in.

A complete runnable stack — ClickHouse + Postgres + Restate + a mock API that reproduces the 404-then-bulk-create behaviour — is in [examples/otel_to_api](./examples/otel_to_api).

### 8. SAP Induction Orchestrator ("The Holy Trinity")
The professional standard for complex SAP integrations. This example demonstrates the full orchestration lifecycle:
- **`dlt`**: Extracting from read-only SQL Server views.
- **`dbt`**: Transforming into a stateful Postgres outbox.
- **`Restate`**: Durably triggering the `SapInductionService` with exactly-once semantics.

See the full implementation and Docker demo in [examples/sap_induction_orchestrator](./examples/sap_induction_orchestrator).

### 9. SAP OData Induction Service
Deploy a durable SAP OData 2.0 induction workflow. This service handles material resolution, quotation lookups, and serial number fan-out with a built-in state machine (NEW -> PENDING -> SUCCESS/ERROR) and callback webhook support.

```yaml
# Used via Restate components in downstream projects
restate_endpoint: "http://restate-server:8080/SapInductionService/execute_induction/send"
```

The induction service is fully configuration-driven via `SapInductionSettings`, mapping generic field names to technical SAP OData properties.

### 10. Federated Zero-Trust Data Mesh
The Data Mesh architecture perfectly decouples the Control Plane from the Data Plane, enabling seamless, zero-trust data access across Dagster jobs, AI Agents, and Jupyter users using DataHub URNs.

- **Domain Broker (`dag_tools.domain_broker`)**: A Dagster sidecar that maps DataHub URNs to physical storage paths and mints temporary AWS STS credentials or database tickets. → **[Deployment guide: docs/domain-broker-deployment.md](docs/domain-broker-deployment.md)** — `DAGSTER_DEFS_MODULE`, and the probe setup (liveness on `/health`, readiness on `/ready`; a Deployment copied from the Dagster user-deployment chart inherits a gRPC health check that can never pass against hypercorn).
- **Central Gateway (`dag_tools.central_gateway`)**: The highly available traffic cop that verifies Keycloak JWTs against the Topaz AuthZ engine before routing requests to the appropriate Domain Broker.
- **Published route list (`dag_tools/central_gateway/gateway_routes.json`)**: The gateway's route ids, shipped as data in the wheel so a consumer in another repository reads them from the installed package instead of keeping a copy that goes stale. Every route carries `delegate_rows_may_name` — the published answer to whether a delegate row may name that route (`assets.authorize` is the only one; `broker.resolve` and `gateway.register` are internal doors with their own caller lists), so the consumer reads that field rather than deciding it itself. Read it with `dag_tools.central_gateway.routes.load_routes()` or straight from the file; the loader imports only the standard library, so a policy validator needs neither Dagster nor FastAPI installed. `dag_tools_tests/test_gateway_routes_json.py` fails if the json and the `ROUTE_*` constants ever disagree.
- **Cortex Data Client (`dag_tools.cortex_data`)**: The Universal Data Plane client. It fetches routing tickets from the Central Gateway and uses Polars to lazily load data (`pl.scan_parquet`, `pl.read_database`) directly from S3 or Databases. → **[Usage guide: docs/cortex-data-client.md](docs/cortex-data-client.md)** — construction, the URN contract, all five source types, reading on behalf of a user, and where laziness and row/column security are *not* uniform.
- **Cortex Polars IO Manager (`dag_tools.io_managers.CortexPolarsIOManager`)**: Forces Dagster to use the `CortexDataClient` with M2M OAuth2 authentication for `load_input`, ensuring 100% uniformity. Data Engineers can copy-paste Polars code from Jupyter directly into production `@asset` definitions! **Read-only by design** — `handle_output` raises. Dagster loads an input using the IO manager of the asset that *produced* it, so this manager is bound to assets you consume (including external stubs for data another deployment owns); it must never announce ownership. To **publish** an asset to the mesh, use a producer IO manager that implements `physical_coordinates` truthfully: `ConfigurableArrowIOManager` (parquet on S3), `ConfigurableSQLIOManager` (postgres/clickhouse), or the Delta IO manager. Catalog registration is handled globally by `DatahubLineageComponent`, not per-IO-manager.

### 11. Utilities
The `dag_tools.utils` namespace provides foundational helpers used across the fleet.

- **Dynamic K8s Resource Tags (`dag_tools.utils.k8s.resolve_k8s_resource_tags`)**: A resilient utility to resolve Kubernetes pod resource requests and limits from environment variables. It enforces a 1:1 request/limit ratio by default to ensure predictable scheduling and provides whitespace cleaning for K8s API safety.

```python
from dag_tools.utils.k8s import resolve_k8s_resource_tags

# Returns a dagster-k8s/config compliant tag dictionary (nested dict)
k8s_tags = resolve_k8s_resource_tags(prefix="INGEST_JOB", default_cpu="1000m", default_mem="2Gi")

# IMPORTANT: Use 'op_tags' for K8s config to bypass Dagster's strict UI label string validation
@asset(op_tags={**k8s_tags}, tags={"owner": "data-eng"})
    ...
```

- **Multi-Environment dbt Compiler & Validator (`scripts/compile_and_validate_dbt.py`)**: A robust utility for container assembly pipelines. It dynamically loads environment configurations from a `dbt_compile_config.yaml` file in the caller's repository.

```yaml
# dbt_compile_config.yaml
dbt_assets_file: "mylib/assets/dbt_assets.py"
manifest_path: "target/manifest.json"

environments:
  - name: "DEV"
    env_vars:
      DBT_TARGET_PROD: "target_dev"
      SOME_DATABASE: "ENGINEERING_DEV"
  - name: "PROD"
    env_vars:
      DBT_TARGET_PROD: "target"
      SSOME_DATABASE: "ENGINEERING"
```

```bash
# Usage in a container build/assemble script
# Ensure PyYAML is installed: pip install PyYAML
python3 scripts/compile_and_validate_dbt.py
```

### 12. Read Audit (`ReadAuditComponent` / `dagtools audit`)

Read-only. Reports which assets read which warehouse objects, and — the part
that matters — which reads **no asset explains**. That gap is what sizes a
classification registry, and it is the number a query tag exists to drive to
zero.

```yaml
# components/read_audit/component.yaml
type: dag_tools.ReadAuditComponent
attributes:
  history:
    # Either an export you already have...
    rows_file: history_rows.json
    lookback_hours: 168
    # ...or a resource that runs the read, in which case `relation` and all
    # five `columns` are required. None of them has a default: this library
    # does not know how your account spells its query history and will not
    # guess.
  runs:
    from_instance: true
  group_name: audit
```

Each read is attributed by query tag (asset key, or a run id resolving to a
single-asset run), else by correlation against exactly one single-asset run
window, else it is reported as `ambiguous`, `unattributed` or
`not_a_pipeline`. Those three stay in the headline gap *and* are broken out
separately, because they go to different people. `coverage` is omitted rather
than reported as 1.0 when nothing was read, and a malformed history row is
counted in `row_failures` rather than dropped.

The component builds the SELECT and hands it to a resource as SQL plus bound
parameters (`fetch_rows(statement, parameters)`) — no driver dependency, no
credential handling, nothing executed in this library. Runnable example with
no warehouse required: [`examples/read_audit`](examples/read_audit).

`dagtools audit statement` prints the SQL and parameters for an operator to
run themselves; `dagtools audit reads` turns an export into the same report.

### 13. Tagged Warehouse (`TaggedWarehouseComponent`)

The resource that closes the gap section 12 measures. Every query it sends
carries a query tag naming who read and what for — and **a query it cannot
attribute is not sent**.

```yaml
# components/warehouse/component.yaml
type: dag_tools.TaggedWarehouseComponent
attributes:
  tag:
    placement: comment            # the tag rides in the statement text
    # placement: session          # ...or your own statement runs first:
    # statement: ALTER SESSION SET QUERY_TAG = :tag
  principal:
    pipeline_name: orders_etl     # tags as `pipeline:orders_etl`
  connection:
    dsn: "{{ env('WAREHOUSE_DSN') }}"
  resource_key: warehouse
```

The tag is `{principal, asset_key, run_id}`. `principal` is
`pipeline:<name>`, or the authz_id the gateway verified for a read made on
someone's behalf. A resource cannot see which asset is reading it, so
`fetch_rows` tags the run — which the audit resolves to an asset only when the
run selected exactly one — and an asset that wants a sharper tag names itself:

```python
rows = context.resources.warehouse.for_asset(context).fetch_rows(statement, params)
rows = context.resources.warehouse.for_user(authz_id).fetch_rows(statement, params)
```

This library ships **no tag dialect**. `comment` placement needs nothing and
is what a connection pool wants. `session` placement runs *your* statement,
which must bind the tag as `:tag`; one that does not fails the code-location
load rather than silently attributing every later read to nothing. The
connection has no environment fallback — name a `dsn`, or a `drivername` with
host/user/password/database — and it is resolved during validation, so a typo
fails the load while importing no driver.

It drops in under section 12 unchanged: same `fetch_rows(statement,
parameters)` seam, named by `resource_key`. Runnable example, sqlite, no
credentials: [`examples/tagged_warehouse`](examples/tagged_warehouse).

### 14. Contract Mapping (`ContractMappingComponent`)

A data contract on one side, a raw source table on the other, and a YAML file
saying which source column feeds which contract column. The component renders
the dbt model that satisfies the contract, plus the tests that prove it still
does.

```yaml
# components/contract_mapping/component.yaml
type: dag_tools.ContractMappingComponent
attributes:
  mapping_file: mapping.yaml
  contract:
    path: ../../../contracts/my_contract.yaml   # reconciled at LOAD
    expect_contract_id: my_contract
    # url: "{{ env('CONTRACT_URL') }}"          # ...or served, fetched at run
  source_schema:
    from_mapping: true          # the mapping declares the source's columns
    datahub: false              # ...or read the real schema from the catalog
  emit:
    dbt_project_dir: ../../../dbt_project
    models_subdir: models/contracts
    tests_subdir: tests/contracts
    # dry_run: true             # render into metadata, write nothing
  asset_name: my_contract_mapping
  group_name: contracts
```

The mapping file names the source's platform URN and table, the period key,
the write cadence, and one entry per contract column — `copy`, `trim`,
`upper`, `lower`, `concat`, `coalesce`, `constant` or `scale`. It also carries
the three things this library will not guess: the contract type → warehouse
type map, a cast template per type that needs one, and the predicate that is
true for a non-finite value.

**With a file contract the whole plan is built at the code location load.** A
mapping that reads a column the source does not have, drops a column a
governed read selects, or disagrees with the contract about write cadence
fails there — not in a run an hour later. A served contract is fetched at
materialization instead, because a code location must not fail to load because
a service is down.

The split between a refusal and a note is whether the emitted artifact would
be **wrong**. A test that could not be written — the contract names its period
calendar rather than listing it, or no warehouse type is declared — is a note
on the materialization. Nothing is guessed to fill a gap: a cast this library
invented, or a non-finite predicate that silently matches nothing, is worse
than a note saying the test was not written.

The **asset of record is the physical platform's dataset URN**, from the
contract — never a `dagster` platform URN. That is the object a governed read
is served from and the object a broker authorizes; the raw table's URN and the
Dagster asset attach as lineage.

Runnable example — no warehouse, no dbt project, no credentials, and it proves
the mapping reproduces a shipped fixture exactly, types included:
[`examples/contract_mapping`](examples/contract_mapping).
### 15. Warehouse Source (`WarehouseSourceComponent`)

One labelled source dataset, **observed rather than moved**. The component
records what is there (row count, schema hash, as-of watermark) and publishes
what a catalog needs to describe the object a broker authorizes reads of: the
dataset itself, a classification label per column, the owner, the access mode,
and where the labels came from.

```yaml
# components/source/component.yaml
type: dag_tools.WarehouseSourceComponent
attributes:
  asset_name: party_extract
  source:
    # The asset of record: the PHYSICAL platform's dataset URN, never a
    # `dagster` one -- that is refused at the load. The Dagster asset is
    # attached to this dataset as lineage.
    dataset_urn: "urn:li:dataset:(urn:li:dataPlatform:sqlite,party_extract,DEV)"
    table: party_extract            # as your warehouse spells it
    access: brokered                # brokered | passthrough
    owner: example-domain-team
    label_origin: board             # board | catalog_tag
    columns:
      - name: party_id
        labels: { example_ladder: rung_internal }
      - name: external_ref
        labels:
          example_ladder: rung_restricted      # the hierarchical dimension,
          example_facet: [facet_x, facet_y]    # one value; others conjunctive
      - name: exposure
        labels: { example_ladder: rung_restricted }
        cell_predicate: "state = 'open'"       # published, never interpreted
  observation:
    count_rows: true
    as_of_statement: "SELECT max(loaded_at) FROM {table}"
  resource_key: warehouse           # the tagged warehouse from section 13
  group_name: classified_sources
```

Every dimension id and value above belongs to the **deployment's** mounted
classification document (env `DAGTOOLS_CLASSIFICATION_LEVELS`, default
`/etc/dagtools/classification/levels.yaml`, top-level key
`classification_dimensions`). This library ships no such file and names no
dimension and no value, not even as a default — the hierarchical dimension is
discovered from the document's own `hierarchical` flag. A label the document
does not declare fails the code-location load; a column with **no** label is
published most restrictive in every dimension, counted as
`defaulted_columns`, and warned about. Deny by default means an incomplete
list under-serves rather than over-serves.

The document is read again **at materialization**, because the mount is the
authority at the moment of the read. If it is absent then, the run fails and
names the path — but the code location still loads, so one missing ConfigMap
cannot take the location's other assets down with it.

The asset is an **observable source asset**: it moves no data and it has no
run config (Dagster calls an observe function with resources only, so every
knob is a YAML attribute). It needs the tagged warehouse resource named by
`resource_key`, and it reads through `fetch_columns` — a seam of its own,
because an empty table yields no rows to read column names off. Note that
source assets are **excluded from `AssetSelection.all()`**: name the keys in
any job or schedule that observes it, or it will run zero steps and report
success.

Point it at a catalog with `catalog: { server: ..., token: ... }` (or
`DATAHUB_TOKEN`); leave the server out, or set `dry_run: true`, and the
payload is recorded in the observation metadata and sent nowhere. Unlike the
dbt publisher, **a catalog failure fails the materialization** — here the
publish is the deliverable, and a source whose labels never arrived is one a
broker will answer unlabelled.

Runnable example, sqlite, no credentials, no catalog:
[`examples/warehouse_source`](examples/warehouse_source).

### 16. Classification Propagation (`ClassificationPropagationComponent`)

Section 15 labels a source, where a human decided what the labels **are**.
This labels everything downstream of it, where nobody decides anything: the
label of a derived column is a consequence of what it read.

The component builds **one sensor** and no assets. For each derived dataset it
watches, every new materialization is read for its `dagster/column_lineage`,
each output column's label is composed from the source columns that column
came from, and the result is published against the dataset's own URN and
recorded against the asset. What *builds* the derived table — dbt, dlt, a
Python asset — is the deployment's business and this component neither owns
nor materializes it.

```yaml
# components/propagation/component.yaml
type: dag_tools.ClassificationPropagationComponent
attributes:
  datasets:
    - asset_key: marts/party_exposure         # key parts joined by '/'
      # Where the DERIVED data lives: the physical platform's URN, never a
      # `dagster` one -- refused at the load, same as section 15.
      dataset_urn: "urn:li:dataset:(urn:li:dataPlatform:sqlite,party_exposure,DEV)"
      # DECLARED, not read off the asset graph: which upstreams carry labels
      # is a policy fact. A lineage dep naming an unlisted source is refused.
      sources: [party_extract]
      downgrades:
        - id: state_total_is_not_a_party
          owner: example-board                 # all three are required
          reason: "A state-level sum does not identify a party."
          columns: [exposure_total]
          label:
            example_ladder: rung_internal      # lower on the ladder
            example_facet: []                  # and the set is CLEARED
  classification:
    require_ratified: false
  catalog:
    server: "{{ env('DATAHUB_GMS_URL') }}"
    token: "{{ env('DATAHUB_TOKEN') }}"
  default_status: STOPPED                      # the default, deliberately
```

**Composition only ever tightens, and the directions differ.** The
hierarchical dimension takes the highest rung of its inputs. A restrictive or
exclusionary set **unions**. A permissive set **intersects** — a wider
permissive set admits more readers, so composing two of them keeps only what
both allow. The dimension types come off the mounted document (`type:
restrictive_set | permissive_set | exclusionary`, defaulting to
`restrictive_set`), and this library still names no dimension and no value.

**A column with no lineage entry publishes the table's ceiling** — the
composition of every labelled column of every source the asset read, which
includes any column published deny-by-default. The decision records which
basis it used (`column_lineage` or `table_max`) and the exact source columns,
so a coarse label is visible as a coarse label rather than as a wrong one.
Composing *no* labels at all is refused: a source that has published no label
set fails the dataset rather than contributing nothing, because an unlabelled
source is not an unrestricted one.

**Nothing is ever widened except by a declared rule.** A `downgrades` entry is
the only thing that can publish a column wider than it derives, so `id`,
`owner` and `reason` are required and the id travels on the published
decision. Two rules reaching one column are refused rather than ordered —
which one the record credits decides who is accountable — and a rule that
would make any dimension *more* restrictive is refused as a re-label. A
dimension the rule omits keeps what the data derived; an empty set **clears**
that dimension, because an empty set is a value and not an omission.

Published as **two patch aspects**, never a replace: `datasetProperties`
(`classification`, `classification_origin: propagated`, the sources, the
document's digest) and `editableSchemaMetadata` (a glossary term per column
per label value, *plus a remove for every other term in the vocabulary* — add-
only would leave a stricter term behind and void the downgrade). A patch
rather than a full aspect so a crawler's own metadata survives.

The sensor is **stopped by default**, the document is re-read **every tick**
(a ConfigMap can move between a deploy and a tick), one dataset's failure
never stops the others, and a failing dataset is retried on later ticks
without re-materializing it — so a fixed rule publishes on the next tick. Set
`dry_run: true` to compose and record the whole decision and emit nothing.

Limitation: a derived dataset cannot yet be another's source. The component
records its decisions as observations and reads its sources' published label
sets, so chained propagation is not supported.

The same runnable example carries it, with one derived table whose three
columns show the two bases and one downgrade:
[`examples/warehouse_source`](examples/warehouse_source).

## Setup & Development

This project targets **Dagster 1.12+ (core)** / **0.28+ (libraries)**. We use `uv` for all dependency management.

```bash
uv sync
```

### Running the local test environment

To verify that the shared components load correctly, we provide example Definitions entry points in `examples/`.

```bash
uv run dagster dev
```

### Component API

All custom components use the Dagster 1.12 GA `Component, Resolvable, Model` triple-inheritance pattern:

```python
from dagster import Definitions
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

class MyComponent(Component, Resolvable, Model):
    my_field: str

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        ...
```

## AI Agent & Developer Guidelines
If you are an AI or human developer modifying this repository:
1. **[llms.txt](./llms.txt)**: High-level architectural context for AI tools.
2. **[.cursorrules](./.cursorrules)**: Strict enforcement of our coding styles, `uv` stack, and type-hinting requirements.
3. **[AGENTS.md](./AGENTS.md)**: Safety boundaries and operational guidelines for agentic modifications (ensuring generic, non-breaking reusability).
