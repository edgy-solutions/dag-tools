# SQL Server to SAP (Restate + DLT) Demo

This directory contains a fully self-contained, end-to-end example demonstrating how `dag-tools` orchestrates stateful, row-level data synchronization from an on-premise SQL Server to an external REST API (like SAP) using **Dagster Components**, **dlt**, and **Restate**.

## 🏗️ Architecture Matrix

This demo spins up 5 sandbox containers to simulate an enterprise environment:

1. **SQL Server 2022 (Source)**
   - Initialized with a dummy `PURCHASE_ORDERS` table on port `1433`.
   - **Change Tracking (CT)** is natively enabled.
   - 3 dummy purchase orders are inserted on boot.
2. **PostgreSQL (Staging Buffer)**
   - Acts as the `dlt` pipeline staging destination on port `5433` (mapped locally to avoid native Postgres conflicts).
3. **Restate Server (Durable Execution Engine)**
   - The battle-tested durable execution framework running on port `8083`.
   - Ensures that row-level API dispatch networks never fail silently.
4. **Mock SAP API (Target Endpoint)**
   - A lightweight FastAPI server simulating a corporate ERP ingress endpoint on port `5000`.
   - Accepts JSON `POST` requests and logs them.
5. **Restate Handlers (dag-tools worker)**
   - A dedicated Python worker that hosts the `dag-tools` Restate Services (`GenericApiSyncService` and `GenericSqlAckService`).
   - Acts as the bridge between the Restate cluster and your custom Python sync logic.

## 🚀 How to Run

### 1. Boot the Distributed Infrastructure
Start by launching the background Docker containers. This will spin up the databases, initialize the dummy data, and start the Restate cluster.
```bash
docker compose up -d
```
> *Note: Initial boot takes ~15 seconds as the `restate_handlers` container installs the Microsoft ODBC drivers natively.*

### 2. Launch the Dagster Orchestrator
Once the containers are running securely in the background, use `uv` to boot the Dagster UI in your local environment. 

Ensure you run this command from within this `sql_to_sap` directory (or use the `--workspace` flag) so Dagster picks up the `workspace.yaml`.
```bash
uv run dagster dev
```

### 3. Open the UI & Execute
Navigate to **http://localhost:3000** in your browser.
1. You will see a pre-built pipeline labeled `sap_api_dispatch`.
2. This pipeline was dynamically generated purely from the declarative `dagster_home/components/sap_sync/component.yaml` file.
3. Click **Materialize** on the pipeline.

## 🔍 What Actually Happens?

When you materialize the pipeline in Dagster, the `restate_api_sync` component executes a highly resilient 3-step orchestration:

1. **Change Data Capture (`dlt_assets`)**
   - Dagster connects to SQL Server via ODBC.
   - It leverages `dlt` and Change Tracking metadata to surgically extract only the new inserts/updates from `PURCHASE_ORDERS`.
   - The data is staged temporarily in the Postgres buffer.
2. **Restate Invocation (`restate_dispatch_task`)**
   - Dagster submits a durable task directly to the Restate Server block.
   - Restate accepts the batch and securely maps over every individual row.
3. **Row-Level Execution (Restate Handlers)**
   - For every row, Restate fires off an HTTP `POST` to the Mock SAP API.
   - If the API succeeds (Returns HTTP 200), Restate utilizes the nested `GenericSqlAckService` to connect back to SQL Server and natively mark the row's `sync_status` as `'SYNCED'`.
   - If the API goes down, Restate holds the state in its persistent log and automatically retries indefinitely.

## 👀 Verifying the Results

You can verify that the data successfully traversed the network in two places:

**1. The SAP Mock API received the payloads:**
```bash
docker logs sql_to_sap-mock_sap-1
```
*(You will see the JSON payloads for `PO-1001`, `PO-1002`, and `PO-1003` logged to the console).*

**2. SQL Server processed the async Acknowledgments:**
```bash
# Exec into the database and check the sync_status
docker exec sql_to_sap-sqlserver-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Password123!" -d INTERNAL_ERP -Q "SELECT * FROM dbo.PURCHASE_ORDERS;"
```
*(All 3 rows will now show a `sync_status` of `'SYNCED'`).*

## 🛑 Clean Up

When you are done testing the platform, gracefully spin everything down and wipe the state:
```bash
docker compose down -v
```
