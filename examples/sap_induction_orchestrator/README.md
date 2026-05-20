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

## 🚀 How to Run (End-to-End Demo)

This example includes a complete Docker Compose environment to simulate the entire hardware/software stack.

### 1. Start the Infrastructure
```bash
cd examples/sap_induction_orchestrator
# Builds and starts SQL Server, Postgres, Restate, and the Mock SAP API
docker compose up -d --build
```

### 2. Configure Environment
The provided `.env` file contains the local DSNs and URLs required for Dagster (running on your host) to connect to the Docker containers.

### 3. Load Dagster
```bash
# Launch Dagster (host-side) using the local Python environment
uv run dagster dev
```

### 4. Service Registration
The `restate-handlers` worker self-registers with the Restate coordinator on
startup (`RESTATE_ADMIN_URL` / `RESTATE_ADVERTISED_URI` are set in the compose
file), so no manual step is needed. To re-register manually if required:
```bash
curl -X POST http://localhost:9070/deployments -H "Content-Type: application/json" -d '{"uri": "http://restate-handlers:9080"}'
```

### 5. Execute Pipeline
Launch the `trigger_restate_induction` asset in the Dagster UI. This will:
1. **Extract** POs from the Dockerized SQL Server.
2. **Transform** them into a Postgres Outbox.
3. **Dispatch** them to the Restate Ingress.
4. **Verify** success via the logs of the `restate-handlers` container!

## 🔒 Durable Security
By utilizing Restate for the final API push, we ensure that:
- **SAP Mutations are Idempotent**: Retries happen at the row level, not the batch level.
- **Failures are Documented**: The `SapInductionService` will use its Callback/Webhook logic to update the `SAP_OUTBOX` status if the induction fails or succeeds.
- **Zero Data Loss**: The state is held in Restate's persistent journal until the transaction is successfully acknowledged by both SAP and the Outbox.
