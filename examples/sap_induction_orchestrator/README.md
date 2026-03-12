# SAP Induction Orchestrator ("The Holy Trinity")

This example demonstrates the professional standard for SAP integration using the **"Holy Trinity"** of modern data engineering:

1.  **dlt**: Extracting data from a read-only SQL Server view.
2.  **dbt**: Transforming raw data into a stateful **SAP Outbox** table in Postgres.
3.  **Restate**: Durably executing the SAP induction workflow with exactly-once semantics.

## 🏗️ The Declarative Architecture

The pipeline leverages the established `dag-tools` components for maximum reusability and clean separation of concerns:

### 1. `DltPipelineComponent` (Extraction)
Configured via `components/extraction/component.yaml`. It surgically extracts records from a read-only View in SQL Server (`vw_sap_source_data`) and loads them into a `raw_sap_data` schema in the local Postgres instance.

### 2. `DbtProjectComponent` (Transformation)
Configured via `components/transformation/component.yaml`. It executes the `dbt` project located in `dbt_project/` which:
- Cleans the raw data.
- Maps internal fields to the logical structure required by SAP.
- Initializes the state machine columns: `status = 'NEW'`, `retry_count = 0`, and `status_timestamp`.

### 3. `trigger_restate_induction` (Execution)
Queries the `SAP_OUTBOX` table for records requiring attention (`NEW`, `PENDING`, `ERROR`). For each record, it durably dispatches a task to the `SapInductionService` running on Restate.

## 🚀 How to Run

1.  **Setup Environment**:
    Ensure you have `POSTGRES_DSN`, `SQLSERVER_DSN`, and `RESTATE_INGRESS_URL` set in your environment.

2.  **Initialize dbt**:
    Navigate to `dbt_project/` and ensure you have a `profiles.yml` pointing to your Postgres instance.

3.  **Launch Dagster**:
    ```bash
    cd examples/sap_induction_orchestrator
    uv run dagster dev
    ```

4.  **Execute**:
    Materialize the `trigger_restate_induction` asset. Dagster will automatically execute the upstream extraction and transformation steps first.

## 🔒 Durable Security
By utilizing Restate for the final API push, we ensure that:
- **SAP Mutations are Idempotent**: Retries happen at the row level, not the batch level.
- **Failures are Documented**: The `SapInductionService` will use its Callback/Webhook logic to update the `SAP_OUTBOX` status if the induction fails or succeeds.
- **Zero Data Loss**: The state is held in Restate's persistent journal until the transaction is successfully acknowledged by both SAP and the Outbox.
