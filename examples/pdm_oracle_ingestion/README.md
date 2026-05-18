# PDM Oracle Ingestion (Stateful Cycle)

Stateful ingestion of a **Parts Master (PDM)** staging table from Oracle, with
durable acknowledgment back to an Oracle stats table via Restate. The flow
cycles: read unprocessed rows → dispatch → ack → cycle again.

## Flow

```
+----------------------+      +-----------------+      +---------------------------+
|  Oracle              |      |  dlt incremental|      |  Dagster trigger asset    |
|  PDM_STAGING         | ---> |  extract (only  | ---> |  reads PKs from           |
|  (processed_flag,    |      |  WHERE          |      |  destination, chunks      |
|   sync_date,         |      |  processed_flag |      |  them, POSTs to Restate   |
|   business cols)     |      |  = 'N')         |      |  /send ingress            |
+----------------------+      +-----------------+      +---------------------------+
          ^                                                          |
          |                                                          v
+----------------------+                                  +---------------------------+
|  Oracle              |                                  |  GenericOracleAckService  |
|  PDM_STATS           | <------- durable ack ----------- |  (ctx.run -> oracledb     |
|  (cycle_id,          |                                  |   UPDATE processed_flag,  |
|   rows_acked, ts)    |                                  |   sync_date)              |
+----------------------+                                  +---------------------------+
```

Next cycle sees zero rows for whatever was acked, because the dlt incremental
read filters on `processed_flag = 'N'`. Restate gives us exactly-once ack
semantics — if the worker dies mid-batch, the journal replays and finishes.

## Pieces

| File | Purpose |
| --- | --- |
| [init_oracle.sql](init_oracle.sql) | Creates `PDM_STAGING` and `PDM_STATS` tables and seeds 5 unprocessed rows |
| [docker-compose.yaml](docker-compose.yaml) | Oracle Free 23c + Restate + worker |
| [Dockerfile.worker](Dockerfile.worker) | Builds the `restate-handlers` worker image |
| [restate_entrypoint.py](restate_entrypoint.py) | ASGI app binding `GenericOracleAckService` |
| [dagster_home/components/extraction/component.yaml](dagster_home/components/extraction/component.yaml) | `RestateDltSyncComponent` configured against Oracle as both source and ack target |
| [dagster_home/definitions.py](dagster_home/definitions.py) | Dagster `Definitions` |
| [.env.example](.env.example) | Required environment variables |

## How to run

```bash
cd examples/pdm_oracle_ingestion
cp .env.example .env
docker compose up -d --build
curl -X POST http://localhost:9070/deployments -H "Content-Type: application/json" \
  -d '{"uri": "http://restate-handlers:9080"}'
uv run dagster dev
```

Materialize the extraction asset twice in the Dagster UI. The first run picks
up 5 rows; the second run picks up 0 because they've been acked.

## Tests

The cycle semantics are validated by
[dag_tools_tests/test_pdm_oracle_cycle.py](../../dag_tools_tests/test_pdm_oracle_cycle.py),
which uses an in-memory sqlite database as a stand-in for Oracle and patches
`oracledb.connect` to exercise the round-trip without requiring a live Oracle
instance. See that test for chunk-size verification and the two-cycle
read/ack/read assertion.
