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

## Control Plane vs. Data Plane
To ensure scalability and security, `dag-tools` enforces a strict separation between:
1. **Control Plane (Dagster)**: Orchestrates data movement, manages schedules, and handles metadata.
2. **Data Plane (Restate)**: Executes high-volume, row-level API and database mutations durably.

Data Plane workers are built using `Dockerfile.worker` and utilize `Hypercorn` for mandatory HTTP/2 support required by modern Restate SDKs.

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
Instantiate generic Oracle-to-Postgres syncing and auto-chunked Restate acking by writing a single YAML component definition:

```yaml
type: dag_tools.components.restate_dlt_sync.RestateDltSyncComponent

attributes:
  restate_endpoint: "http://restate-server:8080/GenericOracleAckService/mark_as_processed/send"
  
  source_config:
    drivername: "oracle+oracledb"
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
```

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
def my_kubernetes_asset():
    ...
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
