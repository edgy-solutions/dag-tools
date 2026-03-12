# `dag-tools`

This repository serves as the central hub for common Dagster utilities, resources, IO managers, sensors, and asset patterns used across all of our data tooling projects.

## Project Purpose
Rather than duplicating infrastructure logic (such as configuring connection strings, handling file formats, or defining generic S3 bucket sensors) across multiple repositories, `dag-tools` provides a unified, typed, and easily importable library of standard Dagster components. 

Other projects (e.g., `pub-tools`) rely on this repository for their core pipeline scaffolding.

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

### 4. PyArrow DataFrame IO Manager
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

### 7. SAP OData Induction Service
Deploy a durable SAP OData 2.0 induction workflow. This service handles material resolution, quotation lookups, and serial number fan-out with exactly-once semantics using Restate:

```yaml
# Used via Restate components in downstream projects
restate_endpoint: "http://restate-server:8080/SapInductionService/execute_induction/send"
```

The induction service is fully configuration-driven via `SapInductionSettings`, mapping generic field names to technical SAP OData properties.
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
