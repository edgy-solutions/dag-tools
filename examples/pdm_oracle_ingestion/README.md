# PDM Oracle Ingestion (request / response cycle)

Stateful ingestion of Parts Master (PDM) tables from Oracle, driven by a
two-way conversation over Oracle tables and made durable with Restate.

We ask for a set of top-level Major End Items; PDM explodes them and fills
its staging tables; we extract, acknowledge, and tell PDM we are done.

## Flow

```
  us                          Oracle (PDM)                        us
  --                          ------------                        --

  mei_request  ─────────►  PDM_MEI_REQUEST
                           (MEI_NUMBER)
                                 │
                                 │  PDM explodes the MEIs
                                 ▼
                           PDM_STAGING / PDM_BOM / PDM_ROUTING
                           (processed_flag='N')
                                 │
                           PDM_CONTROL  ◄── STARTED, then COMPLETED (FULL|DELTA)
                                 │
                                 └──────────►  cycle sensor sees COMPLETED
                                                       │
                           PDM_CONTROL  ◄────── EXTRACT_STARTED   (the gate)
                                                       │
                                                       ▼
                                              dlt incremental extract
                                                       │
                                                       ▼
                           PDM_CONTROL  ◄────── CONSUMED
                                    or ◄────── EXTRACT_ABORTED    (on failure)
```

The `CONSUMED` row is the **only** acknowledgment sent. There is no
per-row receipt — see "Row-level acknowledgment" below.

**COMPLETED is the handshake, and it is the reason this cycle no longer
polls for a row count.** A count cannot distinguish "PDM finished" from
"PDM is a third of the way through committing", so a count-driven cycle
extracts a partial load and acknowledges it as whole. The sensor fires
only when the newest COMPLETED is newer than the newest CONSUMED, which
also makes the cycle self-settling: our own row closes it.

Restate gives exactly-once semantics on every Oracle write. That matters
most for the two new ones — a duplicated MEI request could make PDM redo
a full load, and a duplicated completion row could make our sensor
believe a cycle finished that did not.

## Pieces

| File | Purpose |
| --- | --- |
| [init_oracle.sql](init_oracle.sql) | Request, control, three data tables; seeds a completed full load |
| [mei_overlay/meis.yaml](mei_overlay/meis.yaml) | The MEI list, stood in for the git overlay |
| [docker-compose.yaml](docker-compose.yaml) | Oracle Free 23c + Restate + worker |
| [component.yaml](dagster_home/components/extraction/component.yaml) | The whole pipeline, declaratively |
| [.env.example](.env.example) | Required environment variables |

The worker runs the shared `restate-worker` image and hosts both handler
services via `RESTATE_SERVICES=oracle_ack,oracle_control`.

## Configuration

Every name and every magic string is a config field, because none of them
can be guessed from one site to the next.

**Per-table index and cursor.** PDM does one full load and then only
deltas, so each table needs both — a key to merge on and a cursor to
advance. A table missing its cursor silently reverts to a full extract
every cycle, so they are declared per table rather than one key for the
whole pipeline:

```yaml
table_config:
  PDM_STAGING: {primary_key: PART_ID,  cursor: LAST_MODIFIED}
  PDM_BOM:     {primary_key: BOM_ID,   cursor: LAST_MODIFIED}
```

**The control table.** Table, status column, the strings that column
holds, the load-type column and its strings, the timestamp column:

```yaml
control_table:
  name: PDM_CONTROL
  status_column: LOAD_STATUS
  started_value: STARTED
  completed_value: COMPLETED
  consumer_done_value: CONSUMED     # what WE write
  load_type_column: LOAD_TYPE
  full_value: FULL
  delta_value: DELTA
  timestamp_column: LOAD_TS
```

`timestamp_column` is required: it is how a cycle decides whether a
COMPLETED load has already been consumed. `consumer_done_value` must
differ from `completed_value`, or our own closing row looks like PDM
announcing a new load and the sensor never settles — the config refuses
to load if they match.

**The MEI table**, and where the list comes from:

```yaml
mei_table:
  name: PDM_MEI_REQUEST
  mei_column: MEI_NUMBER
  source_file: /overlay/meis.yaml
```

The file is read at materialization time, not at definitions load, so
re-pointing the overlay takes effect on the next run rather than the next
redeploy. A YAML list, a JSON list, or one MEI per line all work. A
sensor watches it and re-requests when the *set* of MEIs changes —
hashed, so reformatting or reordering does not re-trigger.

### Row-level acknowledgment

`row_ack` defaults to **true** and this example sets it **false**.

When on, each table gets an ack asset that reads every ingested primary
key back out of the destination and POSTs them to Restate, which flips
`processed_flag` on the *source* table — one UPDATE per 1000 keys, back
into the system we read from. On a million-row table that is a million
keys marshalled through JSON and ~1000 UPDATEs against PDM's Oracle, per
cycle, plus UPDATE grants on their tables.

Nothing on this side reads that flag. dlt's cursor decides what to pull
next, the control table carries the cycle handshake PDM dequeues on, and
the audit trail lives in dlt's own load history. So the flag would be
written by us and read by nobody.

Turn it on only when the source genuinely consumes it — to purge staged
rows, or as a transfer audit. `stats_table` is written by this ack, so
the config refuses to load with both `row_ack: false` and a
`stats_table`.

With it off, `load_complete` depends on the extraction assets directly
rather than on the acks, so the completion row still cannot be written
before every table has landed.

### The gate

Setting `consumer_started_value` turns the control table into a lock: the
source polls for our start marker and holds off updating the data until a
terminal row from us releases it. Two consequences, both correctness:

* the marker is injected as a **dependency of every extraction asset**, so
  it lands before a single row is read. A separate job would leave exactly
  the window the gate exists to close.
* `consumer_aborted_value` is **required** alongside it. Only the success
  path writes `consumer_done_value`, and the source clears nothing, so a
  failed run would leave them blocked forever. A run-failure sensor writes
  the aborted status for every other ending. Half a lock is a deadlock,
  and the config refuses to load with only one of the two.

> **Enable `run_monitoring` on the Dagster instance wherever the gate is
> used.** The abort sensor fires when Dagster marks a run FAILED. If a run
> pod dies hard and run monitoring is off, the run can sit in `STARTED`
> indefinitely, no failure event is emitted, and the gate is never
> released from this side. Keep a staleness rule on the source as the
> backstop — no arrangement of code here covers a process that never
> reports anything.

Leave `consumer_started_value` unset and no marker is written at all.

### Request rows with more than one column

A request row often needs several columns. Three sources cover it without
the handler knowing what any column means:

```yaml
mei_table:
  name: <table>
  mei_column: <column a bare list entry lands in>
  constants: {<column>: <value>}   # every row, NOT overridable
  defaults:  {<column>: <value>}   # every row, overridable by an entry
```

The overlay stays a flat list for the simple case, or becomes a list of
mappings when a row needs its own values:

```yaml
- <identifier>                     # uses mei_column + constants + defaults
- {<column>: <value>, <column>: <value>}
```

Every row is normalised to the same column set before insert, because
`executemany` binds one statement across the batch — a row missing a
column another row has would bind the wrong parameters. Missing values
become NULL rather than being dropped.

### MEI-scoped vs unscoped tables

Only some tables are MEI-driven. Nothing in the config says which:
an unscoped table populates regardless, and a scoped one stays empty
until the MEI list is written. Treating them uniformly costs nothing and
avoids mirroring a distinction that lives in PDM and would rot here.

## How to run

```bash
cd examples/pdm_oracle_ingestion
cp .env.example .env
docker compose up -d --build
uv run dagster dev
```

1. Materialize `pdm_extraction_mei_request` — writes the MEIs.
2. In a real deployment PDM now fills its tables and appends COMPLETED.
   The seed data already contains a COMPLETED row, so the cycle sensor
   fires against `PDM_STAGING` immediately.
3. The cycle job extracts, acks, and appends CONSUMED.
4. The sensor goes quiet until the next COMPLETED.

## Tests

| File | Covers |
| --- | --- |
| [test_pdm_oracle_cycle.py](../../dag_tools_tests/test_pdm_oracle_cycle.py) | The ack handler, chunking, two-cycle drain |
| [test_pdm_control_cycle.py](../../dag_tools_tests/test_pdm_control_cycle.py) | MEI request, completion signal, the handshake rule, per-table hints, overlay parsing |
| [test_pdm_component_build.py](../../dag_tools_tests/test_pdm_component_build.py) | What the component actually generates, and the config errors it refuses |

All use sqlite as an Oracle stand-in with `oracledb.connect` patched, so
the real handler bodies run without a live Oracle.
