# Restate dlt sync (CO-RESTATE-DLT-SYNC)

The Restate dlt sync component turns a YAML block into a source-to-destination dlt copy that is acknowledged back to the source through Restate. For each pipeline it builds the dlt extraction assets and, per table, an asset that reads the ingested keys back from the destination and posts them to a Restate ingress. Optional blocks add a request asset that writes a list of top-level items to the source, a control-table handshake with a start marker and a completion row, and a job with sensors that start a cycle when the source says its load is whole.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It builds the dlt assets, one acknowledgement asset per table, and optional request, start-marker and completion assets.** The extraction comes from the same `create_dlt_assets` as the [dlt pipeline](dlt-pipeline.md). Each table gets `<pipeline>_<table>_ack_dispatch` and depends only on its own table's extraction asset, with a fallback to all of them when no unique match is found. With `row_ack` set to false, no acknowledgement assets are built and the completion asset hangs off the extraction itself. `built`
2. **A pipeline has at most three sensors, and none sets a default status.** When `cycle_sensor` is set, a cycle job selects the start marker, the extraction, the acknowledgements and the completion asset, and exactly one sensor starts it: a control-table sensor when the pipeline has a control table, otherwise a backlog-query sensor. An abort sensor, which runs when that job fails, is added only when the control table sets `consumer_aborted_value`. Separately from `cycle_sensor`, a pipeline with `mei_table` gets a request job, and one whose `mei_table` has a `source_file` also gets an overlay sensor that starts that job and is evaluated at most once every 60 seconds; that interval is fixed. No sensor passes `default_status`, so Dagster's own default applies. No schedule is built. The component's own tests check that the sensors exist; none checks their default status. `built`
3. **`source_config`, `dest_config` and `restate_endpoint` are required; two further endpoints are required only when their block is used.** `mei_request_endpoint` is required when a pipeline declares `mei_table`, and `control_status_endpoint` when it declares `control_table`; both default to empty and the load fails if they are needed and empty. Every table needs a primary key, from `table_config` or a pipeline-level `primary_key`. The component carries no default source, destination, credential or endpoint. The package also ships a `schema.py` with a root and a pipeline model, which nothing at the pin imports; the component validates through its own fields and `config.py`. `built`
4. **It reads the source and the destination directly, and the credentials come from config and from a process environment variable.** The dlt extraction uses `source_config` and `dest_config`. The acknowledgement read-back connects to the destination through a URL read from an environment variable named after the destination driver, falling back to a Postgres-named variable; if neither is set the step raises. The control and backlog sensors connect to the source with `source_config.credentials`, which a `cycle_sensor` requires. The component does not go through the [central gateway](central-gateway.md), the [domain broker](domain-broker.md) or a ticket. `built`
5. **It calls Restate's request/response form, not the one-way `/send` form.** `await_endpoint` strips a trailing `/send` from every configured endpoint, so the call waits for the handler to finish and its failure is visible. Request, control-row and abort calls wait at most 120 seconds, a value no setting changes, and raise on any error status. The acknowledgement chunks are posted from the dispatch asset in chunks of 10,000 keys with a 300-second timeout. No credential or identity is sent on any of these calls. The calls are plain HTTP posts to the endpoint in config; the Restate ingress is a [server](restate-server.md) the [worker](restate-worker.md) registers with. `built`
6. **The payloads match the two Oracle handlers in the shared worker, and the component does not name them.** The acknowledgement payload carries the table, the key column, the key list and an optional stats table, which are the fields the acknowledgement handler reads. The request and control payloads carry the fields the control handler reads. Which service each endpoint reaches is set by the endpoint in config; the handler wrappers' bounds and refusals are described in the [Restate worker](restate-worker.md) record. `built`
7. **A failed acknowledgement chunk does not fail the run, and it holds the high-water mark.** Each chunk failure is logged and counted. If any chunk failed, the mark stays at its old value so the next cycle re-sends those keys, and the materialization records the failed count. The mark is the largest dlt load id seen, read back from the asset's last materialization, and a first run acknowledges the whole table. When the scoped read-back raises, for example because the destination has no dlt load-id column, the asset logs a warning and acknowledges the whole table instead. Rows read that way carry no load id, so the mark is not advanced and the whole table is acknowledged again on every cycle for as long as the scoped read keeps raising. Control-row calls behave the opposite way and raise. `built`
8. **A control-table sensor fires on the source's completion row, and its retries are counted per marker.** The sensor skips while a run of the job is open, skips when the latest completion is not newer than the latest row this side wrote, and counts launches per marker in its cursor, three by default. A new marker resets the count. A cycle that fails is retried up to the bound rather than blocked forever by a run key. A start marker, when configured, is a dependency of every extraction asset, and the abort sensor writes a terminal row when the job fails. `built`
9. **Bad config is refused at load; a missing list or credential is refused at the run.** At load: a `table_config` entry for a table not in `sources`, `row_ack: false` together with `stats_table`, an overlapping or incomplete set of control statuses, a start value with no aborted value, a missing endpoint, a missing primary key, a composite key on an acknowledged table, a `cycle_sensor` without source credentials, a `cycle_sensor` with neither a control table nor a backlog query, and `max_attempts` below one. At the run: a missing destination credential variable, an overlay file that does not exist, and a request list that resolves to zero entries, which would otherwise clear the request table. `built`
10. **The request list is read at the run, and the overlay sensor keys on a hash of the list.** The list can be inline or a file in YAML, JSON or one item per line. The overlay sensor re-requests only when the resolved list differs from the last list a run succeeded for, so a failed request is retried and a reformatted file is not a new request. The control-row writer takes both status values and extra columns from config; they travel in the payload to the handler. `built`
11. **It makes no catalog call and writes no label.** The component builds assets only; it reads and writes source and destination tables and posts to Restate. `built`

## Settings and defaults

Component attributes:

| Setting | Default | What it does |
|---|---|---|
| `source_config` | required | source kind and connection; `credentials` is needed for any `cycle_sensor` |
| `dest_config` | required | destination kind and credential; its driver name picks the read-back environment variable |
| `restate_endpoint` | required | Restate endpoint for acknowledgement chunks; a trailing `/send` is stripped |
| `mei_request_endpoint` | empty | endpoint for the request write; required when a pipeline has `mei_table` |
| `control_status_endpoint` | empty | endpoint for control-table rows; required when a pipeline has `control_table` |
| `staging_config` | empty | optional staging destination |
| `pipelines` | empty | named pipelines, with the per-pipeline settings below |

Per-pipeline settings (popped from the pipeline block; the rest goes to the shared dlt group model):

| Setting | Default | What it does |
|---|---|---|
| `sources` | empty | tables to extract |
| `primary_key` | none | default key for tables with no `table_config` entry; one is needed per table |
| `table_config` | none | per-table key and cursor; refused for a table not in `sources` |
| `row_ack` | `true` | whether to read keys back and post acknowledgements |
| `stats_table` | none | table that receives a stats row; refused when `row_ack` is false |
| `control_table` | none | the control table, its status column and values, and the timestamp column |
| `mei_table` | none | the request table, its column, constants, defaults, `source_file`, inline list and `replace` (default `true`) |
| `cycle_sensor.enabled` | `true` when the block is present | builds the cycle job and sensors |
| `cycle_sensor.interval_seconds` | 60 | minimum interval of the control-table or backlog sensor; the overlay sensor's 60 seconds is fixed |
| `cycle_sensor.max_attempts` | 3 | launches per completion marker |
| `cycle_sensor.backlog_query` | none | scalar count used when there is no control table |

Fixed in code:

| Setting | Default | What it does |
|---|---|---|
| Restate call timeout | 120 seconds | how long a request, control-row or abort call waits; no setting changes it |
| acknowledgement chunk timeout | 300 seconds | how long one chunk post waits; no setting changes it |

## Gaps

- **An acknowledgement run is green when chunks failed** (finding 7). The failure is a log line and a metadata count, and the run succeeds, so an operator watching run status alone sees nothing. The mark is held, so the keys are re-sent next cycle. `test_a_failed_dispatch_holds_the_mark` pins the held mark, not an alert. Remedy: not on the roadmap. Status: `built`.
- **A destination without dlt's load-id column is acknowledged in full on every cycle** (finding 7). The fallback logs a warning and the run is green, so the repeat is visible only in the log. No test covers the fallback. Remedy: not on the roadmap. Status: `built`.
- **No credential or identity is sent to the Restate ingress** (finding 5). Every post is a plain request with a JSON body. Whether the ingress checks a caller is set outside this component. Remedy: [roadmap row 13](../ROADMAP.md) (not filed). Status: `built`.
- **The shipped schema file is not used by the component** (finding 3). `schema.py` defines a root and a pipeline model, and nothing at the pin imports them; the component validates through its own fields and `config.py`. A reader who takes `schema.py` as the contract would read a stricter and different one. No test. Remedy: not on the roadmap. Status: `built`.

## Not established from code

- Which Restate services the three endpoints reach in any deployment, and whether the handlers are hosted in the same worker.
- The default status of the sensors in a running instance; the code sets none, and the instance's own setting was not read.
- What dlt writes when the destination falls back to a bare name; that is described in the [dlt pipeline](dlt-pipeline.md) record.
- Whether the source accounts are read-only. The sensors and the extraction only read, but the handlers write back to the source with the worker's own credentials.
- Whether run monitoring is enabled on an instance. The abort sensor does not fire for a run that never reports a failure.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-RESTATE | calls | HTTP | none | acknowledgement chunks, request lists and control rows, each awaited | `dag_tools/components/restate_dlt_sync/component.py:290-345`, `:326`, `:601-622`; `test_the_send_suffix_is_stripped`, `test_the_ack_is_awaited_not_fired_and_forgotten` | `built` |
| CO-DLT-PIPELINE | calls | in-process | none | the shared extraction asset factory | `dag_tools/components/restate_dlt_sync/component.py:410-532`; `test_every_source_table_gets_an_ack_dispatch` | `built` |
| XE-SOURCE-DB | reads | SQL through a database driver | not established | the tables to copy; control and backlog queries | `dag_tools/components/restate_dlt_sync/component.py:551-560`, `:692-697`, `:868`, `:939`; no test | `built` |
| XE-DLT-DESTINATION | writes | dlt destination connection | not established | the copied tables | `dag_tools/components/restate_dlt_sync/component.py:410-532`; `test_every_source_table_gets_an_ack_dispatch` | `built` |
| XE-DLT-DESTINATION | reads | SQL through a database driver | explicit config, else a process environment URL | the keys loaded since the last acknowledged mark | `dag_tools/components/restate_dlt_sync/component.py:500-507`, `:551-560`; no test | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | copies source tables into a destination as Dagster assets, then acknowledges them back to the source and drives a request and completion handshake through Restate | — |
| How the mesh wires it | a YAML component in a code location, sharing the dlt resource with the other dlt-based components; it posts to Restate ingress endpoints that a deployment names | — |
| Sign-in | to the source and destination with credentials from config or environment; to Restate, nothing is presented | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct from the source to the destination, and direct to the Restate ingress; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | `operational-data`: where producers' tables are read out and their systems written back to | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none for mesh reads; the refusals at load are config validation, not access control | — |
| Services served | moves producer data into a store a consumer reads and records consumption back at the source | [DoD S7](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/restate_dlt_sync/`; handler paths under `dag_tools/restate_handlers/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Assets per pipeline and per table; own-table dependency; `row_ack` off | `component.py:410-532`, `:526-658`, `:53-89`, `:677-688` | AGENTS.md §4 | `test_pdm_component_build.py`: `test_every_source_table_gets_an_ack_dispatch`, `test_each_ack_dispatch_depends_only_on_its_own_table`, `test_row_ack_false_generates_no_dispatch_assets`, `test_completion_falls_back_to_the_extraction_for_its_ordering` | `built` |
| Cycle job and sensors; at most three; no default status | `component.py:690-745` (abort at `:729-733`, control or backlog at `:735-745`), `:661-674`, `:863`, `:934`, `:971`, `:997` | not stated | `test_pdm_component_build.py`: `test_the_cycle_sensor_is_generated`, `test_overlay_file_adds_a_watcher`; `test_pdm_gate.py`: `test_an_abort_sensor_is_generated`; no test of default status | `built` |
| Required config | `component.py:387-408`, `:457-478` | AGENTS.md §5 | `test_pdm_component_build.py`: `test_mei_table_without_an_endpoint_is_refused`, `test_control_table_without_an_endpoint_is_refused`, `test_a_table_with_no_primary_key_anywhere_is_refused` | `built` |
| Direct connections and credential sources | `component.py:551-560`, `:692-697`, `:868`, `:939`; `:500-507` | AGENTS.md §4 | no test of the environment fallback; no test of absence of the gateway | `built` |
| Request/response calls; fixed timeouts; no credential sent | `component.py:290-345`, `:326`, `:601-622` | AGENTS.md §4 | `test_restate_await.py`: `test_the_send_suffix_is_stripped`, `test_the_request_that_starts_a_cycle_cannot_fail_silently`; `test_pdm_ack_scoping.py`: `test_the_ack_is_awaited_not_fired_and_forgotten` | `built` |
| Payload fields match the handlers | `component.py:603-609`, `:771-778`, `:795-801`; `oracle_ack.py:9`, `:49`, `:77`; `oracle_control.py:35`, `:141-146`, `:179-182`, `:299`, `:362` | AGENTS.md §4 | `test_pdm_gate.py`: `test_one_endpoint_serves_every_control_status` | `built` |
| Failed chunks hold the mark; scoped read-back; whole-table fallback | `component.py:192-241`, `:203-221`, `:564-643` (fallback at `:573-589`) | AGENTS.md §4 | `test_pdm_ack_scoping.py`: `test_second_cycle_acks_only_the_new_load`, `test_a_failed_dispatch_holds_the_mark`, `test_the_next_cycle_retries_what_the_failure_stranded` | `built` |
| Control sensor, attempts, start marker, abort | `component.py:253-287`, `:458-466`, `:496-515`, `:858-992`; `config.py:95-141` | AGENTS.md §4 | `test_pdm_cycle_retry.py`: `test_the_same_marker_retries_after_a_failure`, `test_retries_are_bounded`, `test_a_new_marker_resets_the_count`; `test_pdm_gate.py`: `test_the_gate_is_wired_as_a_real_op_input`, `test_a_gate_without_a_release_is_refused`, `test_our_status_cannot_collide_with_the_sources` | `built` |
| Refusals at load and at run | `component.py:435-478`, `:532-541`, `:691-716`, `:557-558`, `:763-769`, `:111-116` | AGENTS.md §4 | `test_pdm_component_build.py`: `test_table_config_for_an_unlisted_table_is_refused`, `test_a_composite_key_cannot_be_acked`, `test_stats_table_without_the_ack_is_refused`; `test_pdm_cycle_retry.py`: `test_max_attempts_below_one_is_refused`; no test of the run-time refusals | `built` |
| Request list read at run; overlay sensor keys on a hash | `component.py:92-144`, `:994-1041` | AGENTS.md §4 | `test_pdm_cycle_retry.py`: `test_a_failed_request_is_retried`, `test_a_succeeded_request_is_not_repeated`, `test_a_success_for_a_DIFFERENT_list_does_not_count` | `built` |
| No catalog call | `component.py:1-36`, `:747-752` | not stated | no test of absence | `built` |
| Gap: green run on failed chunks; no credential to ingress; unused schema file | `component.py:623-636`; `:342`, `:617-621`; `schema.py:5-48` | not stated | `test_a_failed_dispatch_holds_the_mark` pins the held mark only | `built` |
