# Manufacturing traceability data pipes (`mfg_trace`) — spec

Status: **APPROVED with amendments** (architect via Chris, 2026-11-11; that
block replaces the 11:59 one). D1-D10, D7b, vendor field names in the
adapter, the HMAC operator default and stopped schedules are approved as
written. The amendments are folded in: the template stance (Q1, §6.2),
`all_terminal` as the code default (Q2, §4.1), `interval_origin` on
`as_built_edge` (§4.2), the three Q7 contract types (D10), and source ids
in `source_system` / `cadence.set_by` (§4.1). Order: the spike, then PR A
(§12). Dispatch 1, deliverable 1.

Second ruling (2026-11-11, on `dispatch-mfg-trace.md`). The template's
grains stand, and the dispatch now defers to them. `ingest_status` stays
one row per run. The ClickHouse as-of reader is approved (§12, PR D). The
sandbox and refusal fixture is ours to propose (§12, PR E). PR A starts
with the seam and the schedule; the SystemLink paging is final after the
spike.

Third ruling (2026-11-12, quick turnaround on SystemLink). A1 merges on
green (dag-tools#39), and its same-job-name limitation (AGENTS §4p) is
accepted for now and noted on the board. **A2 starts without waiting for
the spike.** Every value the spike would confirm is a setting with a
conservative default: `take` 100, the server time filter off, and the steps
key `(resultId, stepId)` per Q3. The orderBy spelling and the step-batch
sizes are settings too. The fake server covers both plausible variants
wherever the spike would decide. When the spike JSON lands, the defaults
are set from it in a final commit before A2 merges. After A2, a **thin PR
D** follows: only `test_result`, `test_measurement` and `test_catalog` over
the SystemLink raw tables, with their contract checks. The rest of D, then
E, B and C, follow it. A minor release is tagged after A2 merges, so other
work can pin it.

Every name in this document — part numbers, serials, programs, tables,
columns, hosts — is synthetic (`PN-1000-A`, `SN-0001`, `prog_functional`).
The deployment's real names live in its own repo's YAML.

---

## 0. Where this spec departs from the dispatch, and why

The dispatch tells us to follow repo convention where the two conflict and
to record each conflict. These are the conflicts. Each one is a review
point, listed first because the rest of the spec is built on them.

| # | Dispatch says | Repo convention (wins) | Effect |
|---|---|---|---|
| D1 | A new `SystemLinkTestMonitorComponent` wrapping its own dlt source | **Every dlt ingest goes through the dlt component** (`DltPipelineComponent` with `dlt_assets_parsing`), so translator, asset keys, source-spec lineage, `dest_config`, `op_tags`/`pool` and dry-run `extract()` exist once. Engines are **pure packages** with no Dagster or dlt imports (`dag_tools/otel_api_sync/`, `dag_tools/contract_mapping/`); dlt sources live in `dag_tools/asset_wrappers/sources/` (`clickhouse_query.py`, `sql_ct_database.py`). | **No new component.** SystemLink is a new **source kind** of `DltPipelineComponent` (`source_config.type: systemlink_test_monitor`), with the engine in `dag_tools/systemlink/` and the dlt source in `dag_tools/asset_wrappers/sources/systemlink.py`. The dlt component gains a source-kind seam (§4.1); the SQL and filesystem kinds go behind it unchanged. |
| D2 | `destination.dataset: mfg_raw` / curated default `mfg_trace` | **No database, schema or table name is a default in dag-tools** (standing rule, Chris 2026-11-04/05). A default is a name in the repo, and a wrong default writes to the wrong object without anyone noticing. | Every dataset, table prefix and database name is a **required** attribute. `mfg_raw` / `mfg_trace` appear only in the synthetic example. |
| D3 | `destination: {dataset, table_prefix}` | Every component takes the destination as **`dest_config`** in the one shared shape, resolved by `config_to_credentials` / `get_destination` (AGENTS §4a "dest_config takes the same shape here as in every other component"). | SystemLink is a `DltPipelineComponent` source kind, so it takes `dest_config` and `dest_schema` in exactly that component's shape; nothing new to learn or resolve. |
| D4 | YAML spells env vars `{{ env('X') }}` | Same. The attribute form `{{ env.X }}` does not resolve on the 1.10.19 floor (§4j/§4k). | No change. Noted because older examples in the repo still use `env.X`. |
| D5 | Schedules built in Python from a component attribute | AGENTS §4a: OTel ships no schedule, and the operator attaches one through YAML `post_processing`. But that **cannot work on 1.10.19**, the version production runs, because the floor rejects `post_processing` and fails the whole component load. `grist_ingest` is the precedent for a component that builds its own sensor in `build_defs`. | The dispatch wins, on the floor argument. The component builds a `ScheduleDefinition` from `schedule:` attributes, **created STOPPED unless `schedule.enabled: true`**, matching the repo's "the default sensor starts stopped" stance. No automation condition is set in YAML. |
| D6 | `contracts/mfg_trace.yaml` at the repo root | Contracts are one document per dataset (`load_contract` takes one mapping). Samples live under `examples/<name>/contracts/` (`examples/contract_mapping/contracts/ev_fixture.yaml`). | `examples/mfg_trace/contracts/mfg_trace.yaml` is a **multi-document** YAML stream: one `---` document per curated table, each loadable by `load_contract` unchanged. |
| D7 | Compat floor: component tests must *run*, not skip, on 1.10.19 | The floor CI job (`test.yml` `dagster-floor`) does **not install dlt**. Every dlt-importing test currently *skips* there through `importorskip`. | Two changes, both in PR A. (a) The engine is pure, so its tests (client, paging, split-retry, parsing, normalizer, SQL rendering) run on the floor with no dlt. (b) Add `dlt[clickhouse]` and `dagster-dlt==0.26.19` to the floor install, so the component tests run there too. (b) is the one CI change this work needs. It is called out because it widens what the floor job resolves. |
| D8 | Classification "marked in the contract" (e.g. `operator_ref`) | **No classification level is named in dag-tools** (4l). The levels are the board's, read from the deployment's dimensions file. | The contract records a *fact* (`personal_data: true`), never a level. The deployment labels the column through `warehouse_source` (4m) against its own dimensions file. |
| D9 | `contract_mapping` "if it has shipped" | It has shipped (4k). It maps **one source table onto one contract table, column by column**: `copy/constant/trim/upper/lower/concat/coalesce/scale`. It has no row filter, no regex, no joins and no window functions. | The curated layer is built in two stages (§6.2). `contract_mapping` does every source-name-to-contract-name step. A small dag-tools dbt package does the derivations (attempts, closures, status), written only against **contract** column names. `contract_mapping` gains two additive transforms (§6.3). |
| D10 | Contract column types include timestamps and arrays | `CONTRACT_TYPES` is `string/numeric/integer/float/boolean`. `load_contract` accepts an unknown type and reports it through `unknown_types` (a note, not a refusal). | **Q7 answered: all three are added.** `timestamp` (always UTC), `date` and `array<string>` join `CONTRACT_TYPES` in PR B, with the plan, the row route and the emitted tests handling each. iagent's reader gains the same three in the same change (Lane 1 packet), so neither side ever accepts a type the other refuses. Pairing approved: PR B merges and is released first, then iagent's reader moves its pin in one PR. `array<string>` is one token, spelled exactly so; there is no general `array<T>`. |

---

## 1. Purpose and non-goals

**Purpose.** Land three manufacturing sources in ClickHouse and keep a
contract-governed curated layer current, so that a separate iagent engine
can serve, in under a second, three linked views for any part number:
as-designed BOM, as-built serial tree, and test results with SPC
populations.

**Non-goals.**
- No UI and no query engine. iagent builds those (dispatch 2).
- **No writes to any source.** PDM, SAP/MES, Snowflake and SystemLink are
  read-only. The SystemLink client exposes no verb other than the three
  `POST query-*` routes and `GET results/{id}`, and a test asserts it.
- **No server-side aggregation.** No `groupBy` and no `*-summary`
  endpoints. All aggregation happens in ClickHouse.
- No customer shape in the package: no part formats, programs, tables or
  hosts. dag-tools ships component types, validators, generic derivations
  and synthetic fixtures.

## 2. Sources and their public semantics

**NI SystemLink Test Monitor v2** (`/nitestmonitor/v2`, base path
configurable). Auth uses the `x-ni-api-key` header, with workspace-scoped
keys. A `200` with an empty list means "not visible to this key", not "does
not exist". The routes used are `POST query-results`, `POST query-steps`,
`POST query-products` and `GET results/{id}` (diagnostics only). Query
bodies take `filter` (Dynamic LINQ with `@n` placeholders), `substitutions`,
`orderBy`, `descending`, `take`, `continuationToken`, `returnCount` and
`projection`. A result has an `id` and a status (`status.statusType`). A
step is keyed `stepId` within `resultId`, and its `data.parameters[]`
carries `measurement / lowLimit / highLimit / units / comparisonType /
status`, usually as strings.

The SystemLink field names (`partNumber`, `status.statusType`, `stepId`, ...)
are named in the engine, because they are the vendor's **public wire
protocol** that the client has to speak to build a filter. That is the same
standing as the Dagster GraphQL field names in `dag_tools.qual.graphql`.
They are not a customer's shape. Nothing *downstream* of the raw layer
depends on them, because raw-to-curated goes through `contract_mapping` like
every other source. **Review point:** this is my reading of the "nothing
below is named" rule as it applies to a vendor API. The alternative is a
YAML field map for the client, which buys nothing unless NI renames fields
between server versions. If it does, that belongs under §3 "Server
deviations".

**SAP IBase via BW, in Snowflake.** Expect BW-shaped tables: DSO/aDSO
transactional data, master-data attribute and text tables (with `OBJVERS`,
active = `A`; time-dependent rows with `DATEFROM`/`DATETO` and a
`99991231`-style open end; a blank-key row in every master-data table),
possibly with a `fact_*`/`dim_*` star on top. The as-built structure is one
row per installed component instance: parent instance, part, serial,
position, validity. The underlying SAP objects (IBase header, component,
structure) are context only. **Every relation and column name is YAML.**

**Windchill PDMLink BOMs.** These are already landed by the existing dlt PDM
ingest, which is a deployment instance of `RestateDltSyncComponent` (the
synthetic shape is `examples/pdm_oracle_ingestion`: Oracle to **Postgres**,
`merge` on per-table `primary_key` + `cursor`). The relevant semantics: part
number + revision + view; quantity and UoM; find/line number; reference
designators; effectivity (date, serial or lot range); substitutes and
alternates as distinct links.

## 3. Server deviations found

*Empty until the spike against the target server's Swagger document (PR A,
step 1).* The spike records, for the target server version:
- the `orderBy` enum spelling (`UPDATED_AT`? `updatedAt`?) and whether
  `descending` is honoured on query-results;
- whether `projection` is accepted on query-results and query-steps;
- whether `stepId` is globally unique or only unique per `resultId` (this
  decides the steps primary key; Q3);
- whether `continuationToken` survives a concurrent insert (stable paging);
- the maximum `take`;
- whether `returnCount` with `take: 0` is honoured on every query route;
- whether query-steps accepts `resultId in @0` with a list substitution, or
  needs `resultId == @0 || resultId == @1 ...` (this decides the batch
  filter builder; both use substitutions).

The spike output is a JSON file, committed under
`docs/plans/mfg-trace-spike/<server-version>.json`, generated by a
read-only probe utility that takes the same `connection:` block. It records
the target server's routes and enums, never any test data.

## 4. Component designs

### 4.1 SystemLink as a source kind of the dlt component (PR A)

**No new component (D1).** SystemLink is ingested by `DltPipelineComponent`,
like every other dlt source, so it inherits the translator, the
`["dlt", <db>, <schema>, <table>]` asset keys, the external source
`AssetSpec` with `kinds` (the lineage icon), `dest_config` resolution
(D3), `op_tags` / `pool` / `k8s_resource_env_prefix`, and the dry-run
`extract()` runnable. What SystemLink needs and the SQL path does not have
arrives through one seam.

**The source-kind seam (`dag_tools/asset_wrappers/source_kinds.py`).**
`build_dlt_source` / `_prepare_units` currently branch on
`creds.drivername` (`filesystem` vs SQL). That branch becomes a registry
keyed by `source_config.type`. The key already exists in YAML (the PDM
example sets `type: sql_database`), but nothing reads it today. Each kind
provides:

| Hook | SQL / filesystem kinds | `systemlink_test_monitor` kind |
|---|---|---|
| `validate(source_config)` | today's behaviour (no model) | pydantic, `extra="forbid"`; a bad block fails the **load** |
| `prepare_units(...)` | today's `_prepare_units` body, moved verbatim | one unit per pipeline; the source spec is `["systemlink", <host>]`, `kinds={"systemlink"}` |
| `build_source(unit, config)` | today's `build_dlt_source` body, moved verbatim | the dlt source from `sources/systemlink.py` |
| `extra_defs(unit, config)` | nothing | backfill job, schedule, asset checks (§8, §9) |

Only an **exact registered kind** selects a kind. Anything else, a missing
`type` or one the registry does not know, resolves to today's drivername
dispatch, so **every existing YAML loads unchanged**. That matters because
`type` was never read: the repo's own YAMLs and tests set `sql_database`
and `duckdb`, and a downstream repo (`pub-tools`) may have written anything
there. Failing the load on an unknown value would break YAML that works
today, which AGENTS §6 forbids. The existing dlt test modules are the
regression, run unmodified. Two guards keep the fallback from hiding a
mistake:

- An unregistered `type` logs one WARNING naming the registered kinds.
- An unregistered `type` that is a near miss of a registered kind
  (`difflib`, cutoff 0.8, so `systemlink_test_monitr` matches) **fails the
  load** and names the kind it resembles. Otherwise a typo would fall into
  the SQL path and fail later on a missing `creds`, which names the wrong
  problem.

**Layout.**

```
dag_tools/systemlink/                 pure engine: no dagster, no dlt
  client.py       HTTP: auth header, request IDs, retry/backoff, 504 split, taxonomy events
  queries.py      filter + substitutions builders (never interpolates a value)
  paging.py       continuation-token pager, lookback-page stop rule
  incremental.py  new / changed / in-progress classification (known_ids, cursor)
  measurements.py measurement + limit string parsing -> (num, text, flags)
  events.py       the §5.3 taxonomy as typed JSON events
  spec.py         the source_config pydantic models
dag_tools/asset_wrappers/source_kinds.py         the registry above
dag_tools/asset_wrappers/sources/systemlink.py   the dlt source (results, steps, products, ingest_runs)
dag_tools_tests/systemlink_fake.py    the synthetic server (stdlib ThreadingHTTPServer, as the repo's
                                      other HTTP fakes are; no respx / pytest-httpserver dependency)
```

**YAML.** The SystemLink-specific knobs live in `source_config`, which is
where the dlt component already puts "what to read and how". Per-pipeline
knobs live in the existing `pipelines:` block, with two additive fields
(`schedule`, `table_prefix`). Plain dicts and scalars inside `type:` +
`attributes:`. No `from __future__ import annotations` in the component
module.

```yaml
type: dag_tools.DltPipelineComponent
attributes:
  source_config:
    type: systemlink_test_monitor                   # selects the kind
    source_id: test_repo                            # required; written to source_system (see below)
    connection:
      base_url: "{{ env('SYSTEMLINK_BASE_URL') }}"  # required
      api_key_env: SYSTEMLINK_API_KEY               # required; the NAME of the env var
      api_version_path: /nitestmonitor/v2           # default; v1 servers override
      workspace_id: null                            # optional workspace filter (a substitution)
      verify_tls: true
      # Private CA: the client is httpx, which honours SSL_CERT_FILE. The
      # merged-bundle rule (AGENTS §4) applies: never point it at the bare cert.
    seed:                                           # required; exactly one kind
      kind: file                                    # file | clickhouse_query | inline
      path: seeds/part_numbers.csv                  # resolved against the component dir
      column: part_number
      # kind: clickhouse_query -> query: "<SELECT returning one column>", connection: {...}
      # kind: inline           -> values: [PN-1000-A, PN-1000-B]
    filters:
      terminal_statuses: [PASSED, FAILED, ERRORED, TERMINATED, TIMED_OUT, DONE]
      step_detail_for: all_terminal                 # all_terminal (the default) | failed_only
      failed_statuses: [FAILED, ERRORED, TERMINATED, TIMED_OUT]   # what failed_only means
      program_include: []
      program_exclude: []
    limits:
      page_size: 100
      step_batch_default: 15
      step_batch_max: 40        # hard ceiling; validation refuses default > max
      max_concurrency: 1        # validation caps at 4
      request_timeout_s: 30
      max_retries: 5
      not_visible_warn_threshold: 0   # asset check threshold (§9)
    incremental:
      strategy: known_ids       # known_ids | cursor
      order_by: UPDATED_AT      # spelling verified by the spike
      lookback_pages: 3
      use_server_time_filter: false
    keep_json_columns: [properties, keywords, inputs, outputs]   # §5.4 casing hazard
    operator:
      store: pseudonym          # pseudonym | name | drop
      pseudonym_key_env: SYSTEMLINK_OPERATOR_PSEUDONYM_KEY   # HMAC key; required when store=pseudonym
    products: {enabled: false}
  dest_config: {...}            # required; the shared dest_config shape (D3)
  pipelines:
    systemlink:
      name: systemlink_test_monitor
      dest_schema: mfg_raw      # required for this kind (D2); synthetic value
      table_prefix: systemlink_ # required for this kind (D2)
      sources: [results, steps] # resource names; `products` / `ingest_runs` added by the kind
      pool: null                # existing field; Dagster concurrency pool (§8.3)
      schedule:                 # new, additive; built in Python (D5)
        enabled: false          # created STOPPED unless true
        cron: "*/15 * * * *"
```

**Assets.** For a `systemlink_test_monitor` pipeline, the dlt component
builds, through the shared code path:

1. the external source `AssetSpec` `["systemlink", <host>]` with
   `kinds={"systemlink"}`, a dep of every raw table;
2. one dlt `@multi_asset` whose specs are `results`, `steps`,
   `steps__data__parameters` (the dlt child table, see below), `products`
   when enabled, and `ingest_runs`;
3. through `extra_defs`: jobs `<name>_incremental` and `<name>_backfill`, the
   schedule (D5), and the SystemLink asset checks (§9).

**dlt resources.**

| Resource | Disposition | Key | Notes |
|---|---|---|---|
| `results` | `merge` | `id` | Every fetched result, terminal or not, with `_source_system` (the configured `source_id`), `_extracted_at`, `_request_id`. `operator` is replaced per `operator.store` **before yield**, so the name never reaches dlt. |
| `steps` | `merge` | `stepId`, or `(resultId, stepId)` per Q3 | A `@dlt.transformer` over `results`. It gets **only** results classified *fetch-steps* (§5). |
| `steps__data__parameters` | follows its parent | dlt `_dlt_parent_id` / `_dlt_list_idx` | The `data.parameters[]` child table that dlt unpacks. It becomes `test_measurement`. Parsing runs in the curated layer from the raw text, so raw stays source-faithful. |
| `products` | `merge` | product `id` | Optional. |
| `ingest_runs` | `append` | `(run_id, source_system)` | One row per run: counts and taxonomy event counts. Feeds `ingest_status` (§6). |

**Source id versus vendor name.** `source_id` is the *role* the source
plays in the curated contract (`test_repo` in the example), and it is what
`source_system` and `cadence.set_by` carry, alongside `pdm` and `mes`. The
vendor name stays where the vendor is the subject: the kind
(`systemlink_test_monitor`), the engine package, the raw table prefix, the
source spec `["systemlink", <host>]` and the DataHub platform. So replacing
the test repository changes an adapter and leaves every curated row's
`source_system` alone. `source_id` is required with no default, because a
default would be a value from below written into every row (D2).

**`step_detail_for` defaults to `all_terminal`** in code, not only in the
example (Q2, answered). The population and out-of-family views need the
passing measurements too. `failed_only` is the opt-out for a server that
cannot carry the load, and the backfill sizing pass is what shows whether
one needs it.

**The API key never reaches dlt.** The client reads `os.environ[api_key_env]`
when the run starts and holds it only in the httpx client's headers. The
dlt source is constructed with the env var *name*, never the value, so the
key cannot land in dlt state, the pipeline's config dump or a resource
argument that dlt persists. A regression test greps the pipeline working
directory and the destination for the key after a run.

**Modes.** The shared dlt `@multi_asset` takes no SystemLink-specific run
config, so the mode is selected by **job**: `extra_defs` builds
`<name>_incremental` and `<name>_backfill` over the same asset selection, and
the backfill job sets the run tag `dagtools/systemlink_mode=backfill`, which
the kind reads from `context.run.tags` when it builds the source. Absent
means `incremental`. The schedule only ever targets `<name>_incremental`.
- **backfill**: a separate dlt **pipeline name** (`<name>_backfill`), so it
  never touches incremental state. A `returnCount` sizing pass runs first and
  logs per-part counts as one JSON event. Then there is **one dlt load per
  part number**, and the completed part is recorded in that pipeline's
  resource state. dlt commits state only with a completed load, so the
  resume granularity is exactly one part number. An interrupted part is
  re-fetched whole, and `merge` on the primary key makes that duplicate-free.
  The run tag `dagtools/systemlink_restart=true` clears the completed set.
- **incremental**: for each seeded part number, page newest-first in
  `order_by` and stop under the §5 rule.

### 4.2 MES as-built from Snowflake (reuse; PR B)

**Reuse `DltPipelineComponent`** (`sql_database` with a `snowflake://`
`source_config`; `dlt[snowflake]` is already a dependency). That gives
merge on keys, `select_columns`, `op_tags`/`pool` and the source-spec
lineage for free. It needs two **additive, optional** fields beyond PR A's `schedule`. They are
generic, not MES-specific, and every other dlt_pipeline user gains them:

1. **`table_config: {TABLE: {primary_key, cursor}}`**. This already exists
   on `RestateDltSyncComponent`, with its hint translation verified against
   dlt 1.26. It gets lifted into a shared helper in `dlt_assets_factory.py`
   and exposed on `DltPipelineComponent`. When a table has no cursor, the
   table runs `replace` on its own schedule, with a row-count asset check.
2. **`row_filters: {TABLE: [{column, op, value}]}`**, ops
   `= != in not_in is_null not_null`. Rendered as a SQLAlchemy `where` with
   **bound parameters** through the existing `query_adapter_callback` seam,
   which `build_dlt_source` already has and the component never wires. This
   is how `OBJVERS = 'A'`, a language filter and a plant filter are expressed
   with no SQL text in YAML. A filter naming a column that reflection does
   not find fails the run, naming the table and column.
The per-pipeline **`schedule: {cron, enabled}`** that PR A adds (D5) is the
same field for every kind, so MES and the bridge schedule the same way.

The blank-key master-data row is filtered **in the curated layer**
(dispatch §4), not at extraction, so raw remains a faithful copy.

**Removal history the MES does not keep (`interval_origin`).** Some MES
tables keep validity intervals. Others keep only the current installation,
so a replaced child disappears with no record that it was ever fitted (Q6).
For the second kind, dag-tools records what it can see: **a removal seen
between two loads becomes a closed interval, from the first load onward,
marked `observed`.** The deployment declares which kind its table is with
one var, `as_built_history: source | observed`. There is no default,
because guessing wrong either invents removals or hides them.

- **Where the history lives: raw, through dlt's `scd2` merge strategy.** In
  `observed` mode the as-built table's `table_config` entry sets
  `write_disposition: {disposition: merge, strategy: scd2}`. Each load is a
  full snapshot of the table; dlt closes a row's `_dlt_valid_to` at the load
  that no longer returns it and opens a new row for one that appears. The
  observation history is therefore raw data, and the curated table stays
  rebuildable from raw with a full refresh. A history kept only in an
  incremental dbt model would be destroyed by the first `--full-refresh`.
- **Snapshot loads, never cursor loads, in `observed` mode.** `scd2` reads
  "absent from this load" as "removed", so a cursor that fetches only
  changed rows would close every unchanged edge. The component refuses an
  `scd2` table that also declares a `cursor`, at load.
- **Attribute changes are not removals.** `scd2` versions a row when any
  column changes. The derive model merges consecutive versions that share
  the edge key (parent, child, position), so a changed non-key attribute
  does not split one installation into two intervals.
- **What each interval says.** `valid_from` is the MES install date. The
  conform mapping must supply one, because a table with no install date at
  all cannot answer "as built at delivery", and the companion check refuses
  that configuration rather than substituting the load time. `valid_to` is
  the `_dlt_valid_to` of the first load that no longer saw the edge, and
  **its precision is the load interval**: the removal happened at some
  point between the previous load and that one. `valid_to_precision_s`
  records that gap in seconds, so a reader can see how coarse each
  interval is (approved 2026-11-11). It is set for `observed` intervals
  and null for `source` ones.
- **Before the first load, nothing can be recovered.** A child replaced
  before observation began has no row, and `is_as_built` is then wrong for
  that unit in exactly the way Q6 describes. The as-built hazard in the
  contract doc says so, and a check reports the observation start per
  source.
- **Gate (PR B):** dlt's `scd2` on the ClickHouse destination is not yet
  verified here. PR B proves it on duckdb in the unit suite and on
  ClickHouse under `DAGTOOLS_E2E_CLICKHOUSE=1` before the mode is
  documented. If ClickHouse refuses it, the fallback is a raw
  `as_built_observations` table appended per load by a dlt resource, with
  the intervals computed in the derive package. Raw is still the record,
  so the full-refresh property holds either way.

### 4.3 PDM to ClickHouse bridge (PR C)

The existing PDM ingest is a deployment's `RestateDltSyncComponent`, landing
in Postgres in the synthetic example. **It is not modified.** If the
deployment's PDM lands somewhere other than ClickHouse, the bridge is a
second `DltPipelineComponent` instance: `source_config` is the PDM
destination (Postgres through `sql_database`), `dest_config` is ClickHouse,
`table_config` carries the same keys and cursors, and `select_columns`
limits it to the fields the curated layer needs. The bridge's source spec
has the PDM tables' asset keys as deps, so lineage runs straight through.
If PDM already lands in ClickHouse, there is no bridge; Q8 asks which.

## 5. Incremental, dedup and failure modes

### 5.1 Classification (pure, `dag_tools/systemlink/incremental.py`)

At run start there is **one** ClickHouse query (through
`clickhouse_query._client`, already a base dependency):
`SELECT id, updatedAt, status_type FROM <raw results> WHERE part_number IN (<seed>)`,
with the seed passed as a bound parameter. It builds an in-memory map. No
ID list is ever written to dlt state. Each fetched result is then classified:

| Class | Rule | Loaded to `results` | Steps fetched |
|---|---|---|---|
| `new_terminal` | id absent, status terminal | yes | yes, subject to `step_detail_for` |
| `changed_terminal` | id present; `updatedAt` or status differs; now terminal | yes | yes (re-fetch; `merge` replaces) |
| `in_progress` | status not terminal | yes | **no**; not marked processed |
| `known_unchanged` | id present, same `updatedAt` and status | no | no |

An `in_progress` result stays absent-or-non-terminal in raw, so the next
poll classifies it `changed_terminal` once it finishes. That is what makes
"picked up next poll" automatic, with no separate pending list. A
`changed_terminal` result's steps are replaced through `merge`. Steps that
existed before and are no longer returned are **not** deleted (Q3 covers
whether that can happen).

**Stop rule.** Page newest-first. Stop after `lookback_pages` *consecutive*
pages in which every result is `known_unchanged`. Then stop, also at the end
of the data. Correctness does not depend on a server time filter: with
`use_server_time_filter: false` the stop rule alone bounds the scan. With it
`true`, `updatedAt > @n` is added (as a substitution) as an optimization
only. The known-ID classification still runs, so a lying filter can cost a
re-fetch but never a gap.

**Known blind spot, stated rather than hidden:** a result whose `updatedAt`
changes while it sits deeper than `lookback_pages` pages behind
`known_unchanged` pages is missed until a backfill. That is inherent to any
newest-first scan with an early stop. A raised `lookback_pages` and a
periodic backfill (a schedule the deployment may attach) cover it.

**`cursor` strategy.** `dlt.sources.incremental("updatedAt",
row_order="desc")` applied client-side. It is correct **only** if the server
bumps `updatedAt` on every change. Kept as an option; `known_ids` stays the
default.

**Re-runs on the same serial** are distinct results with distinct IDs. All
are kept. Attempt order, latest and first are derived in curated (§6),
never by overwriting.

### 5.2 Request discipline (`client.py`)

- **Step batches** start at `step_batch_default`. The batcher **cannot**
  produce more than `step_batch_max` IDs: it is a constructor invariant, and
  the request log is asserted against it in tests and in an asset check.
- **504 / client timeout** split the ID list into halves and retry each half,
  recursively down to 1. A single ID that still times out is recorded as
  `gateway_timeout_split` with `exhausted: true`, and the run continues. An
  oversized batch is **never resent**: the split replaces it.
- **429 / 503** use exponential backoff with full jitter, honour
  `Retry-After` (seconds or HTTP date), and give up after `max_retries`.
- **401 / 403** raise immediately (`auth_failed`) and fail the run.
- **200 with an expected ID missing** logs `not_visible` with the ID and the
  endpoint, then continues.
- **Concurrency** is a semaphore of size `max_concurrency` (default 1,
  capped at 4) around every request.
- **Every request** gets a client request ID (uuid4), sent as
  `x-request-id`, and one JSON log line: `{request_id, endpoint, body_sha256,
  status, latency_ms, records}`. The body is hashed, never logged, because
  filters carry part numbers and the hash is enough to match a report to the
  server admins against a replay.
- **The API key** goes into the header and nowhere else. It is read from
  `os.environ[api_key_env]` inside the client, at materialization. It is
  never an attribute value, never in run config, never in dlt state, never
  in a log line, never in an event. A test greps captured logs, the dlt
  pipeline directory and the loaded tables for the key.

### 5.3 Error taxonomy (`events.py`)

Each event is a JSON object `{event, request_id, endpoint, ...}`. Events are
logged and counted into `ingest_runs.errors_json`.

| Event | Trigger | Effect |
|---|---|---|
| `auth_failed` | 401/403 | raise; run fails |
| `not_visible` | 200 empty for an expected ID | warn, continue |
| `gateway_timeout_split` | 504 / timeout | split and retry |
| `throttled` | 429/503 | backoff and retry |
| `schema_drift` | a field the engine reads is missing, or a top-level field it has never seen appears | warn, load anyway (dlt schema evolution stays on for raw; curated contracts are pinned, §6) |
| `parse_failed` | measurement not numeric while a limit is numeric | keep the text, set the flag (raised in curated, counted by a check) |

### 5.4 Two hazards carried over from existing components

- **Casing.** dlt lower-cases and flattens nested dicts, which is how OTel
  lost `metric.NUM_ERROR` (§4a). SystemLink `properties` keys are
  operator-defined and case-meaningful, so `keep_json_columns` pins them to
  JSON on the way in, the same way `DEFAULT_MAP_COLUMNS` does. Default:
  `properties, keywords, inputs, outputs`. `data.parameters` is deliberately
  **not** pinned, because unpacking it into a child table is what produces
  `test_measurement`.
- **Exact numbers.** No measurement is parsed through a path that rounds.
  The raw text is always kept. `measurement_num` is `Float64`, as the
  dispatch specifies, and a parameter whose text has more significant digits
  than a double holds is counted under `parse_failed` with
  `reason: precision`, not silently rounded.

## 6. Curated contract

The tables, keys, `ORDER BY` and dedup views are in
**`mfg-trace-contract.md`**. The machine-readable template is
`examples/mfg_trace/contracts/mfg_trace.yaml` (D6). This section covers how
the tables are built.

### 6.1 Engines

Every curated table is `ReplacingMergeTree(<version column>)`, with
`loaded_at` as the version, and `ORDER BY` set to the table's access path.
A table that can hold rows not yet merged gets a `<table>_current` view
using `FINAL`. The engine reads only the `_current` views and never needs
to know. `test_measurement` and `as_built_edge` carry their second access
path as a ClickHouse **projection**: one table, consistent by construction,
with no second copy to drift. This needs ClickHouse 21.6 or later.
Recursive closures need 24.4 or later; see §6.4.

### 6.2 Two stages

```
raw (dlt)  --contract_mapping-->  conformed (one model per source table, contract column names)
           --dag-tools dbt package-->  curated (derivations over conformed names only)
```

- **Conform (deployment YAML).** One `contract_mapping` instance per source
  table, mapping that source's columns onto a *conformed* contract (for
  example `conf_installed_component`, `conf_bom_line`,
  `conf_systemlink_step_parameter`). This stage is where every name from
  below is spelled, so it lives entirely in deployment YAML. It also carries
  the normalizer and `*_raw` passthrough columns.
- **Derive (dag-tools).** A dbt package shipped in dag-tools
  (`dag_tools/mfg_trace/dbt/`), included by the deployment's dbt project as a
  local package, materialized through the existing
  `CustomDbtProjectComponent` (so the DataHub publishing and `cli_args`
  rules of §4i apply unchanged). Its models read **only** conformed contract
  columns. Its inputs are declared as dbt vars that point at the conformed
  relations, so the package names no relation.

**The template is dag-tools' published interface (Q1, answered yes).** The
derive stage names the *contract's* column names in dag-tools code
(`part_number`, `serial_number`, ...). That is allowed because the curated
contract is dag-tools' published interface, in the same way `AssetRecord`
is, not a name from below. The two repos split it this way:

- **dag-tools owns the template**
  (`examples/mfg_trace/contracts/mfg_trace.yaml`): the columns the derive
  package reads, their types and their grain. Changing one is a dag-tools
  release, and the template stays in this repo.
- **invincible-agent registers the deployment's contract as a superset** of
  it. It may add columns and never drop or rename one. Its CI checks the
  registered contract against this template **at a pinned dag-tools sha**,
  so a template change reaches iagent only when it moves the pin.
- At load, the package's companion check refuses a deployment contract
  that lacks a column the derivations need. A rename fails loudly instead
  of producing empty joins.

### 6.3 Additive changes to `contract_mapping` (PR B)

1. **`normalize` transform**: `{transform: normalize, rules: <normalizer
   id>, part: value|revision}`. Rules come from a normalizer document
   (§7). It renders to SQL (`trim`, `upper`, `replaceRegexpAll`,
   `extract`), and the row route mirrors it, so the
   `test_contract_mapping_proof` inversion property still holds: a
   normalize with a `strip` rule is **not invertible**, and the plan
   declares that, the same way `scale` declares its factor.
2. **`where`** on the mapping's `source:` block: structured
   `[{column, op, value}]`, the same grammar as dlt `row_filters`, rendered
   with literals escaped by the operator's `cast_templates` dialect. Needed
   for the blank-key row and for validity sentinels.
3. **`nullif` transform** (`{transform: nullif, value: "99991231"}`), so a
   BW open-ended `DATETO` becomes `NULL` (open interval) rather than a fake
   date.

### 6.4 Derivations (the dag-tools dbt package)

| Model | Derivation |
|---|---|
| `test_result` | `attempt_seq = row_number() over (partition by part_number, serial_number, program_name order by started_at, result_id)`; `is_latest`, `is_first` from it. Only terminal results count toward `is_latest`; an in-progress newer attempt is still a row (`is_terminal = false`, `status_type` verbatim) but never `is_latest`, so a running retest cannot hide the last verdict. |
| `test_measurement` | Joins conformed step parameters to `test_result` for the denormalized columns; parses numbers with the §6.5 grammar; `is_latest_result` from `test_result`. |
| `test_catalog` | `group by part_number, program_name, step_path, parameter_name, units` over `test_measurement`. |
| `dim_part` | From conformed PDM parts; `excluded` from the exclusion list (§7.3); `serialized` from YAML rule or MES presence (Q9). |
| `bom_edge` | From conformed BOM lines; `edge_type` from a conformed `link_kind` (the mapping decides how PDM spells substitute and alternate). |
| `bom_closure` | Recursive CTE over current-effective `bom_edge` (`edge_type = 'child'`, not excluded), carrying `path` and a cycle guard (`has(path, child)`), bounded by `max_depth` (a var, required). If the server is older than 24.4, the fallback is an unrolled self-join to `max_depth`, chosen by a var, and a check reports any path that reached `max_depth` (a truncated tree must be visible). |
| `serial_unit` | From conformed MES units; `derived_test_status` and `first_pass` per §6.6; `top_level_*` from `as_built_closure` (scope `current`). |
| `as_built_edge` | One row per validity interval, with `interval_origin` from the `as_built_history` var (§4.2): `source` intervals come from the MES as mapped; `observed` intervals come from raw `scd2` versions, merged across attribute-only changes, with `valid_to_precision_s` from the gap between that load and the previous one. `is_current = valid_to IS NULL OR valid_to > now()`. `is_as_built = valid_from <= parent.build_completed_at AND (valid_to IS NULL OR valid_to > parent.build_completed_at)`. The as-built reference date is the build-complete date by default and the ship date if configured (var `as_built_date`: `build_completed_at` \| `shipped_at`). |
| `as_built_closure` | Same recursion as `bom_closure`, run twice: over `is_current` edges (`scope = current`) and over `is_as_built` edges (`scope = as_built`). |
| `ingest_status` | Union of `<prefix>ingest_runs` from each source. dlt_pipeline gains an optional `ingest_runs: true` that yields the same row shape from `_dlt_loads` plus extract counts. |

### 6.5 Measurement parsing grammar

Table-driven in `dag_tools/systemlink/measurements.py`, which is the
reference implementation and the Python route the tests run. The SQL
rendering in the dbt package is generated from the **same rule table**, and
a test runs both routes over the same cases and asserts they agree, so the
two can never drift.

| Input text | `*_num` | Notes |
|---|---|---|
| `4.95`, `-1e-3`, ` 12 ` | parsed | |
| `4.95 V`, `120mA` | `4.95`, `120` | unit suffix captured into `units_parsed`; `units` column wins when present |
| `< 50` | high = 50 | as a limit string; `comparison_type` inferred `LT` when absent |
| `> 4.2` | low = 4.2 | `GT` |
| `1.20 ± 0.02`, `1.20 +/- 0.02` | low 1.18, high 1.22 | computed in decimal, emitted as Float64 |
| `PASS`, `""`, `NaN`, `inf` | null | text kept; `parse_failed` only when a limit is numeric |

### 6.6 `derived_test_status` (default rule; overridable by YAML vars)

Taken per serial, over the programs listed in the required-programs var
(Q4: **no default**; empty means every program seen for that part is
required):
1. MES status in `rework_statuses` → `in_rework`.
2. Any required program whose latest result is non-terminal → `testing`.
3. All required programs have a terminal latest result, and every one is
   `PASSED` (or in `pass_statuses`) → `passed`.
4. Any required program's latest terminal result is in `failed_statuses` →
   `failed`.
5. No results at all → `untested`.

`first_pass` = every required program's `is_first` result passed. It is
null when any required program has no result.

## 7. Normalization and the unmatched-key report

### 7.1 Normalizer document (deployment-owned YAML)

```yaml
normalizers:
  part_number:
    - {op: trim}
    - {op: upper}
    - {op: strip, pattern: "[\\s-]"}
    - {op: split_revision, pattern: "^(?P<value>.+?)(?:/(?P<revision>[A-Z]{1,2}))?$"}
  serial_number:
    - {op: trim}
    - {op: upper}
    - {op: lstrip_zeros}
```

Ops: `trim`, `upper`, `lower`, `strip` (regex delete), `replace`,
`lstrip_zeros`, `split_revision` (named groups `value` / `revision`). Rules
are ordered and applied identically everywhere, because every source's
conform mapping references the **same normalizer id**. That single
reference is what makes "identical across sources" a property of the
configuration and not of discipline. Original values go to `*_raw`
columns. Python and SQL routes are cross-checked as in §6.5. A regex that
compiles in Python but not in ClickHouse's RE2 (for example a lookbehind)
is refused at load.

### 7.2 Unmatched-key check

Asset check `mfg_trace_unmatched_keys`. Metadata carries counts plus up to
50 sample keys per direction:
- serials with test results and no MES `serial_unit`;
- MES units of `serialized` parts with no test result;
- `dim_part` parts (not excluded) with no MES unit.

It **warns**, never fails. A healthy deployment has untested units, so a
hard gate would be permanently red. Severity is a YAML threshold per
direction.

### 7.3 Exclusion list

`exclude: {categories: [...], patterns: [...]}` as dbt vars. It applies in
curated only: `dim_part.excluded = true`, and excluded parts are dropped
from closure traversal (their own subtree is dropped with them; Q10 asks
whether a non-excluded child below an excluded parent should re-attach).
Raw is never filtered.

## 8. Orchestration

### 8.1 Code location
One deployment code location holds three `DltPipelineComponent`s (SystemLink,
MES, PDM bridge), the conform `ContractMappingComponent`s, the
`CustomDbtProjectComponent` running the derive package, and the
`datahub_lineage` component. That is the repo's established pattern; no new
location type is needed.

### 8.2 Jobs and schedules
- `<systemlink>_backfill` (manual, from the kind's `extra_defs`): SystemLink
  under its backfill pipeline name (§4.1 Modes). The MES and bridge pipelines
  and the curated rebuild are launched by selecting their assets; no combined
  job is built, because one job spanning three dlt pipelines would tie their
  failures together.
- `<systemlink>_incremental`: the per-pipeline `schedule.cron` (the example
  uses every 15 minutes), created STOPPED (D5).
- MES and bridge: their own `schedule:` cron (daily by default in the
  example only; the attribute is required where `enabled: true`).
- Curated: an asset sensor in the derive component that requests the
  affected curated models after any raw source materializes. It is built in
  Python, so it works on the floor. `AssetSelection` names keys explicitly,
  never `all()` (AGENTS 4m: `all()` excludes source assets and passes
  vacuously).

### 8.3 Concurrency
The SystemLink job carries a dedicated Dagster concurrency key (tag
`dagster/concurrency_key`, plus `pool` when set). **And** the schedule's
evaluation function skips with a `SkipReason` while a run of the same job is
queued or in progress. The second guard works whatever the instance's
concurrency config is, which the library cannot see. Two overlapping polls
would not corrupt data (`merge` plus known IDs), but they would double the
load on a shared service, and that is the reason for the limit.

### 8.4 Rebuild strategy and worst-case cost
- `test_result` / `test_measurement`: dbt-clickhouse `incremental`,
  `delete+insert` keyed on `part_number`, over parts touched since the last
  build (from `_extracted_at`). Worst case is all measurements of every
  touched part, which is one part's population, not the table.
- `test_catalog`: rebuilt for touched parts.
- `bom_closure`: per affected root. Any edge change under a part rebuilds
  the closure rows of every ancestor of that part (ancestors found from the
  existing closure). Worst case is a change in a widely shared leaf, which
  rebuilds nearly all of it: O(edges × depth). Documented, not optimized,
  in v1.
- `as_built_closure`: per affected top-level unit, the same shape.
- `serial_unit`: touched serials.

## 9. Checks and observability

On every curated table, generated by `contract_mapping` for the conform
stage and by the derive package's schema tests: column presence and types,
non-null keys, key uniqueness on `_current`.

Also:
- referential: `test_measurement.result_id ⊂ test_result.result_id`;
  `as_built_edge` parent and child ⊂ `serial_unit`;
- validity non-overlap per `(child_pn, child_serial)`;
- for `observed` sources: the observation start (first load) per source,
  and a count of units built before it, whose as-built tree cannot be
  trusted;
- freshness per source, against the source's configured cadence;
- parse-failure rate per parameter (warn above a threshold);
- `bom_closure` / `as_built_closure` reached `max_depth`;
- unmatched keys (§7.2).

SystemLink-specific checks read `ingest_runs` for the latest run: no
`auth_failed`; `not_visible` ≤ threshold; max step batch sent ≤
`step_batch_max` (from the request log counts recorded in the run row).

Lineage: the existing `datahub_lineage` component covers raw and curated,
with the SystemLink source spec's `kinds` giving its platform. Platform
mapping for `systemlink` is added to `datahub_lineage/platforms.py` (Q11:
which platform URN the deployment's DataHub uses for it).

## 10. Test plan

Every case has a control that differs in exactly one thing, and each is
confirmed to fail with its behaviour removed (stated in the PR). The fake
server is `dag_tools_tests/systemlink_fake.py` (stdlib HTTP server; it
records every request).

| Case | Runs on floor? |
|---|---|
| backfill pages every result for every seeded part; an interruption after part k resumes at k+1 without duplicates | component (needs D7b) |
| incremental fetches steps only for new/changed terminal results; stops after `lookback_pages` known pages | engine |
| `RUNNING` result loaded, steps not fetched, picked up next poll once terminal | engine + component |
| 504 on a batch over N → split and retry succeeds; the oversized batch is never resent (request log) | engine |
| no request carries more than `step_batch_max` IDs, including after splits | engine |
| 401 fails the run; empty-200 logs `not_visible` and continues | engine |
| 429 with `Retry-After` honoured (clock injected; no real sleep) | engine |
| API key absent from captured logs, the dlt pipeline dir and loaded tables (duckdb destination in tests) | component |
| every filter value travels in `substitutions`; the rendered `filter` contains only `@n` placeholders | engine |
| normalizer: Python and SQL routes agree; same id gives the same result for all three sources | engine |
| validity: removal and replacement give two edges; `is_as_built` and `is_current` are correct | SQL (ClickHouse integration, CI-marked) |
| observed history: a child absent from load 3 gets `valid_to` = load 3, `interval_origin = observed`, `valid_to_precision_s` = load 3 − load 2; control: present in every load, stays open | component (duckdb `scd2`); SQL half CI-marked |
| observed history: an attribute-only change between loads is one interval, not two | component |
| an `scd2` table that also declares a `cursor` fails the load | component |
| measurement parsing, table-driven, Python ↔ SQL agreement | engine; SQL half CI-marked |
| orphan `test_measurement` caught by the referential check | SQL |
| `table_config` / `row_filters` on dlt_pipeline: filter uses bound params; unknown column fails | component |
| component YAML loads on 1.10.19 | floor |

ClickHouse SQL tests run against a containerized ClickHouse under an
opt-in env var (`DAGTOOLS_E2E_CLICKHOUSE=1`, the `DAGTOOLS_E2E_DBT` pattern),
as a CI job like `dbt-datahub-e2e`. Without the variable they **skip with
a reason**, never pass vacuously.

## 11. Open questions

Status after the architect's approval (2026-11-11). **Answered** questions
are settled and built as stated. **Standing** defaults are approved as
they are. **Deployment** questions are Chris's to answer for the
deployment; the code takes the stated default, or has none, until then.

- **Q1** *Answered: yes.* The template is dag-tools' published interface;
  iagent registers a superset and checks it against the template at a
  pinned sha (§6.2).
- **Q2** *Answered: `all_terminal` is the code default*, not only the
  example's value (§4.1). The backfill sizing pass still produces per-part
  `returnCount` numbers to take to the SystemLink admins.
- **Q3** *Standing default.* Are `stepId`s globally unique or unique per
  result? Can a result's step set shrink on update? *Key
  `(resultId, stepId)`; no delete of vanished steps.* The spike records
  what the server does.
- **Q4** *Deployment.* Which programs are required for `passed`, and which MES statuses
  mean rework or hold? *Default: no required list (every program seen is
  required); no rework statuses (so `in_rework` never fires) until set.*
- **Q5** *Deployment.* Is a service-account API key available for the scheduled
  pipeline? *Default: the pipeline needs one; a personal key works
  technically but the run fails loudly the day it expires.*
- **Q6** Does the replicated as-built table keep removal history (validity
  intervals), or only current state? *Answered by design:* both are
  supported through `as_built_history` (§4.2), and `as_built_edge` carries
  `interval_origin`. Which one this MES is remains a **deployment answer**:
  the var has no default.
- **Q7** *Answered: all three.* `timestamp` (UTC), `date` and
  `array<string>` join `CONTRACT_TYPES` in PR B, and iagent's reader gains
  them in the same change, coordinated with Lane 1 (D10). Order approved:
  PR B merges and is released first; then one iagent PR adds the types to
  its reader and moves its pin. `array<string>` is one token.
- **Q8** *Deployment.* Where does the deployment's PDM land today: Postgres (bridge
  needed) or ClickHouse? *Default: bridge.*
- **Q9** *Deployment.* How is `serialized` known: a PDM attribute, or "the part has MES
  units"? *Default: the mapping supplies it if PDM has it; otherwise MES
  presence.*
- **Q10** *Standing default.* Below an excluded part, should a non-excluded descendant
  re-attach to the nearest included ancestor? *Default: no, the subtree is
  dropped.*
- **Q11** *Deployment.* Which DataHub platform name for SystemLink? *Default:
  `systemlink`, as a custom platform.*
- **Q12** *Deployment.* Which part-number and serial normalization rules join the three
  sources? *Default: none. The example ships only `trim` + `upper`, and the
  unmatched-key check shows what is missing.*
- **Q13** *Standing default.* ClickHouse server version (recursive CTEs
  need 24.4 or later).
  *Default: the recursive path, with an unrolled fallback var.*

## 12. Order of work (after approval)

1. **Spike** (read-only probe of the target server's Swagger document, §3).
   Its output is committed.
2. **PR A**, in two steps.
   - **A1, now:** the source-kind seam in `DltPipelineComponent`, with the
     existing kinds moved behind it verbatim and the existing dlt tests
     unmodified as the regression. Plus the per-pipeline `schedule` field.
   - **A2, after the spike's JSON is in:** the SystemLink engine, kind and
     dlt source (with the required `source_id` and the `all_terminal`
     default), the fake server, the D7b floor change, and the README with a
     synthetic YAML. The paging is fixed by what the spike records.
3. **PR B**: `table_config` / `row_filters` / `ingest_runs` on
   `DltPipelineComponent`, with the `scd2` disposition proven on duckdb and
   ClickHouse (§4.2); `normalize` / `where` / `nullif` on
   `contract_mapping`; the three Q7 types in `CONTRACT_TYPES`, merged and
   released before iagent's reader moves its pin to them (Lane 1); a synthetic BW fixture (an `OBJVERS` mix,
   `DATEFROM`/`DATETO`, a blank-key row, a removal/replacement).
4. **PR C**: the PDM bridge example, exclusion, `bom_closure`.
5. **PR D**: the derive package, normalizer, checks, sensor and jobs,
   lineage platform, `ingest_status`.
   - It also ships the ClickHouse as-of reader for the broker
     (`AS_OF_READERS["clickhouse"]`), approved 2026-11-11.
   - `as_of` is the time the **producer records when it materializes** a
     curated dataset, never `max(loaded_at)` over the rows. The derive
     package records it, and the reader reads that record exactly as it
     was written. Where it is recorded is settled in this PR, and nothing
     in code names the place: the deployment declares it.
   - The reader ships with its e2e test under `DAGTOOLS_E2E_CLICKHOUSE`,
     which meets §4o's rule that no reader ships until it can be tested.
6. **PR E**: the sandbox fixture and the refusal assets, demo-gated, on the
   `demo_ev_refusal` pattern (AGENTS §4k-1).
   - Volume: about 2k units, 10k results and 2M measurements, generated
     deterministically from a seed.
   - It includes one removal and replacement, so as-built and current
     differ, and one unit installed in a parent the sample viewer cannot
     see.
   - One refusal asset per refusal kind (`wrong_type`, `duplicate_key`,
     `not_determined_by`, a missing declared column), each carrying one
     defect, with a clean control.
   - The PR proposes the synthetic part-to-program rows, the `determined_by`
     rules and the refusal targets. Lane 1 ratifies them in ADR review.
7. `examples/mfg_trace/`: three component YAMLs, a seed CSV, the
   normalizer, the conform mappings, and a README a deployment copies.

Each PR reports targeted and full-suite counts, the floor result, and
open questions. AGENTS.md, `.cursorrules`, `llms.txt` and README are
updated in the PR that introduces each new surface (§7 trifecta).
