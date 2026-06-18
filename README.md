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
- `dag_tools/components/`: Dagster 1.12 GA Declarative Components using the `Component, Resolvable, Model` pattern (e.g., `DltPipelineComponent`, `CustomDbtProjectComponent`) that allow users to deploy complex workloads via YAML.
- `dag_tools/io_managers/`: Custom Dagster IO Managers.
- `dag_tools/resources/`: Reusable resources and API/Database clients.
- `dag_tools/sensors/`: Common sensors (S3, file system, etc.).
- `dag_tools/utils/`: Assorted helper functions, centralized `AssetNormalizationRegistry`, and logging utilities.
- `dag_tools/restate_handlers/`: Durable Data Plane services (Restate) for SAP and Database synchronization.
- `dag_tools/inventory/`: The **shared structural-inventory contract** for Dagster assets — a versioned `AssetRecord` schema, an FQN-based IO manager classifier with MRO walking, and a soft-failing extractor that walks a `Definitions`. Used both by the runtime Domain Broker (for IO manager classification) and by the `dagtools survey` CLI (for per-build inventory published to MinIO). Evolution is additive-only; bump `SCHEMA_VERSION` on every change. See `dag_tools/inventory/schema.py` for the rules.
- `dag_tools/qual/`: The **Dagster Upgrade Regression & Qualification System** — the `dagtools` console-script (Typer-based) and its MinIO/S3 registry. Now shipped (Phase 1 steps 1–3):
  - `dagtools qual init` — Q0 of Phase 2: pin the registry's current inventory snapshot, record the baseline/candidate version pair (with explicit pin sets), diff non-Dagster pins into `co_upgrade_risks[]` so a hidden dbt-core bump can't masquerade as a Dagster regression, and write the immutable manifest to both the registry and `~/.dagtools/quals/<id>/manifest.yaml`.
  - `dagtools qual classes` — Q1: read the manifest's pinned inventories, group every fleet asset into an equivalence class by the recipe key (compute kind, IO manager FQN, partitioning, resources, integration libs, asset checks, automation condition, plus custom dbt translator FQNs), pick representatives per class (preferring `regression: "true"` tagged, spanning ≥2 repos), label each rep `RUNNABLE` / `SYNTHETIC_REQUIRED` / `OBSERVE_ONLY`, and publish both `equivalence_classes.json` and a human-readable `.md` companion. The class hash is deterministic so the same fleet shape always produces the same matrix.
  - `dagtools qual run --side baseline|candidate` — Q2 (and Q4 once the test deployment is bumped): launch each RUNNABLE representative through the test deployment's Dagster GraphQL, poll to a terminal status, pull the event log, persist a per-rep `RunRecord` (materializations, asset-check results, metadata-key union, failure step keys), maintain `~/.dagtools/quals/<id>/<side>-state.json` and mirror it to the registry so a desktop crash is recoverable. Re-invocation skips PASSED reps and reconciles LAUNCHED reps via run-id lookup rather than re-launching. `--retry-failed` / `--only-class` for surgical re-runs.
  - `dagtools qual preflight --side baseline|candidate` — Q3: the gate operators run after upgrading the test deployment. Three checks via GraphQL: the deployment reports the expected version (manifest's baseline/candidate; wildcard `1.12.x` accepts `1.12.5`); every code location is in `LOADED` state with the per-location error surfaced on failure; on the candidate side, a deterministic sample of PASSED baseline runs still renders via `pipelineRunOrError` (event-log back-compat spot check). Publishes `preflight.json` immutably; exits non-zero on any failed check.
  - `dagtools qual report` — Q6: the operator payoff. Diffs every representative's baseline vs candidate `RunRecord` for success / materialization count + asset-key set / metadata KEY-set (values may differ) / asset-check parity. Rolls up to per-class verdicts (any failing rep = class red). Applies the recipe's GO criteria: candidate preflight passed + every RUNNABLE class green + (op-in) synthetic-class probe coverage + (op-in) orchestration snapshots clean + (op-in) co_upgrade_risks validated. **Strict by default** — operators explicitly accept each known gap (`--accept-orchestration-deferred`, `--accept-synthetic-coverage-missing`, `--accept-co-upgrade-risks`). Publishes both `verdict.json` and the human-readable `UPGRADE_VERDICT.md` immutably; exits non-zero on NO_GO.
  - `dagtools survey` — load every code location in a workspace.yaml / module spec with `-W all` warning capture; if **any** load fails, refuse to publish and exit non-zero; otherwise introspect assets / sensors / schedules / asset checks / IO managers / dbt projects (custom translator flagged) and publish per-build artifacts via the registry.
  - `dagtools registry status` — fleet-wide staleness report (fresh / stale / missing / unreadable per repo).
  - `S3Storage` + `InventoryRegistry` with immutable per-build keys and a **write-last** `latest.json` pointer so readers never observe a partial publish. See `dag_tools/qual/registry/layout.py` for the bucket layout contract.
  - `templates/Jenkinsfile.survey` — drop-in Jenkins stage for adding a repo to the survey fleet.

  Install via the `qual` extras: `pip install dag_tools[qual]`. **Full system spec, ADRs, and implementation status:** [docs/RECIPE.md](./docs/RECIPE.md).

## Control Plane vs. Data Plane
To ensure scalability and security, `dag-tools` enforces a strict separation between:
1. **Control Plane (Dagster)**: Orchestrates data movement, manages schedules, and handles metadata.
2. **Data Plane (Restate)**: Executes high-volume, row-level API and database mutations durably.

Data Plane workers run the shared `restate-worker` image (built from the repo-root `Dockerfile.restate-worker` and published by CI). Its env-driven entrypoint `dag_tools.restate_handlers.serve` selects which handlers to host via the `RESTATE_SERVICES` environment variable and self-registers with Restate on startup (`RESTATE_ADMIN_URL` / `RESTATE_ADVERTISED_URI`). Workers use `Hypercorn` for the mandatory HTTP/2 support required by modern Restate SDKs — no bespoke per-project entrypoint or Dockerfile is needed.

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
```

### 2. DBT Project Component
Expose fully compiled DBT projects directly to Dagster with automatic Datahub integration native to the project component:

```yaml
type: dag_tools.components.dbt_project.CustomDbtProjectComponent

attributes:
  project: "../../dbt_projects/project_one"
  datahub_config:
    server: "{{ env.DATAHUB_URL }}"
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

### 7. SAP Induction Orchestrator ("The Holy Trinity")
The professional standard for complex SAP integrations. This example demonstrates the full orchestration lifecycle:
- **`dlt`**: Extracting from read-only SQL Server views.
- **`dbt`**: Transforming into a stateful Postgres outbox.
- **`Restate`**: Durably triggering the `SapInductionService` with exactly-once semantics.

See the full implementation and Docker demo in [examples/sap_induction_orchestrator](./examples/sap_induction_orchestrator).

### 8. SAP OData Induction Service
Deploy a durable SAP OData 2.0 induction workflow. This service handles material resolution, quotation lookups, and serial number fan-out with a built-in state machine (NEW -> PENDING -> SUCCESS/ERROR) and callback webhook support.

```yaml
# Used via Restate components in downstream projects
restate_endpoint: "http://restate-server:8080/SapInductionService/execute_induction/send"
```

The induction service is fully configuration-driven via `SapInductionSettings`, mapping generic field names to technical SAP OData properties.

### 9. Federated Zero-Trust Data Mesh
The Data Mesh architecture perfectly decouples the Control Plane from the Data Plane, enabling seamless, zero-trust data access across Dagster jobs, AI Agents, and Jupyter users using DataHub URNs.

- **Domain Broker (`dag_tools.domain_broker`)**: A Dagster sidecar that maps DataHub URNs to physical storage paths and mints temporary AWS STS credentials or database tickets.
- **Central Gateway (`dag_tools.central_gateway`)**: The highly available traffic cop that verifies Keycloak JWTs against the Topaz AuthZ engine before routing requests to the appropriate Domain Broker.
- **Cortex Data Client (`dag_tools.cortex_data`)**: The Universal Data Plane client. It fetches routing tickets from the Central Gateway and uses Polars to lazily load data (`pl.scan_parquet`, `pl.read_database`) directly from S3 or Databases.
- **Cortex Polars IO Manager (`dag_tools.io_managers.CortexPolarsIOManager`)**: Forces Dagster to use the `CortexDataClient` with M2M OAuth2 authentication for `load_input`, ensuring 100% uniformity. Data Engineers can copy-paste Polars code from Jupyter directly into production `@asset` definitions!

### 10. Utilities
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
