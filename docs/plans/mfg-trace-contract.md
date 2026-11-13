# Manufacturing traceability: the curated contract

*Companion to [`mfg-trace.md`](mfg-trace.md). This document is for people;
the machine-readable source of record is
[`examples/mfg_trace/contracts/mfg_trace.yaml`](../../examples/mfg_trace/contracts/mfg_trace.yaml),
one YAML document per table, each loadable by
`dag_tools.contract_mapping.contract.load_contract` unchanged. Where the two
disagree, the YAML wins and this file is the bug.*

Every value here is generic or synthetic. dag-tools ships the template, not
a customer's shape.

**Status.** The spec and this contract are approved (2026-11-11), with the
amendments folded in below: the template stance, `interval_origin`, the
three new contract types and source ids. Every document is still
`status: sample`, because nothing produces it yet.

## 1. Conventions every table follows

**Who owns what.** The template is dag-tools' published interface (spec
§6.2, Q1 answered). The columns the derive package reads are dag-tools'
to define, and the template stays in this repo.

- invincible-agent registers the deployment's contract as a **superset** of
  the template. It may **add** columns and never drops or renames one.
- iagent's CI checks the registered contract against this template **at a
  pinned dag-tools sha**. A template change reaches iagent only when the pin
  moves.
- At load, the derive package's check refuses a contract that lacks a column
  it reads.

All naming of *source* tables and columns happens in the deployment's
conform YAML, never here.

**Physical layout.**

- Engine: `ReplacingMergeTree(loaded_at)` for every table. The newest
  `loaded_at` for a grain wins.
- `ORDER BY` is the table's main access path (`tags.order_by`), not its grain.
  The grain is enforced by the `key_not_unique` rule over the `_current` view.
- **Read through `<table>_current`** (`tags.current_view`). It is a `FINAL`
  view, so a reader never sees a row that a later load has already superseded
  but the merge has not yet removed. Reading the base table directly can
  double-count; that is a reader bug, not a data bug.
- A second access path is a **projection** (`tags.projections`), never a
  second table, so the two can never disagree. Projections need ClickHouse
  21.6 or later; the recursive closures need 24.4 or later (Q13).

**Values.**

- **Types.** `timestamp` is always UTC. `timestamp`, `date` and
  `array<string>` are contract types (Q7 answered). They join
  `CONTRACT_TYPES` in spec PR B, and iagent's reader gains them in the same
  change: PR B is released first, then one iagent PR adds them to its reader
  and moves its pin. `array<string>` is one token. Until PR B lands, `load_contract` reports them as `unknown_types`
  notes; that is expected, not a defect.
- **`''` versus null is deliberate.** A column that is part of a grain
  (`revision`, `find_number`, `units`, `effectivity_from`) uses `''` for
  "absent", because a null in a key cannot be compared or merged. Elsewhere,
  null means "the source did not say".
- `*_raw` columns carry the value exactly as the source wrote it, beside the
  normalized key. They are how a mismatch between sources is diagnosed
  (spec §7.2).
- `source_system`, `source_record_id`, `extracted_at` and `loaded_at` are on
  every row that came from a source, so any curated row can be traced back to
  the raw record it came from.
- **`source_system` carries a source id, not a vendor:** `pdm`, `mes` or
  `test_repo`, the role the source plays here. `cadence.set_by` uses the same
  ids. Adapters, raw tables and the DataHub platform keep the vendor name.
  So replacing a vendor changes an adapter, not a curated row.

**Status and cadence.** Every document is `status: sample`,
`ratified_by: null`, and every column `produced_since: null`: the shape is
declared and no dag-tools release produces it yet. `cadence.set_by` names
what drives a rewrite; `cadence.write` is one of:

| `write` | Meaning |
|---|---|
| `incremental_by_key` | New or changed rows are inserted; the engine collapses them by grain. Nothing is deleted. |
| `rebuild_per_root` | The rows for one root (a part, or a serial) are recomputed whenever any edge under it changes. |
| `rebuild_per_part` | The rows for one part number are recomputed from that part's measurements. |
| `append` | One row per run; rows are never revised. |

**Personal data.** `personal_data: true` records a **fact** about a column.
The classification *level* is the deployment's, applied through
`warehouse_source` against its own dimensions file (D8); no level appears
here. The only such column is `test_result.operator_ref`.

**Rules.** Every table carries `key_not_unique` and `missing_column`. Two
further rules appear where they apply:

| Rule | Fails when |
|---|---|
| `key_not_unique` | Two rows in `_current` share the grain. |
| `missing_column` | A declared required column is absent from the relation. |
| `interval_overlap` | Two `as_built_edge` intervals for the same child serial overlap (a part installed in two places at once). |
| `orphan_reference` | On `as_built_edge`, the parent or child is not a `serial_unit`; on `test_measurement`, the `result_id` is not a `test_result`. |

## 2. Product structure (from PDM)

### `dim_part`

One row per part revision.

| | |
|---|---|
| Grain | `part_number`, `revision` |
| Scope | `part_number` |
| `ORDER BY` | `part_number`, `revision` |
| Cadence | set by `pdm`; `incremental_by_key` |

- `revision` is `''` when the source carries no revision.
- `serialized`: whether units of the part carry serial numbers. Where it comes
  from is open (Q9).
- `excluded`: the part matched the deployment's exclusion list (spec §7.3).
  An excluded part is **kept and flagged, never deleted**, so a reader can
  tell "excluded" from "missing".

### `bom_edge`

One row per designed parent-to-child link: what the design says goes in.

| | |
|---|---|
| Grain | `parent_pn`, `parent_rev`, `child_pn`, `child_rev`, `find_number`, `edge_type`, `effectivity_from` |
| Scope | `parent_pn` |
| `ORDER BY` | `parent_pn`, `parent_rev` |
| Cadence | set by `pdm`; `incremental_by_key` |

- `edge_type` is `child`, `substitute` or `alternate`. Only `child` edges are
  walked by `bom_closure`.
- `quantity` is `numeric`. It keeps the source's written form and is never
  routed through a float (AGENTS 4k).
- Effectivity: `effectivity_kind` is `date`, `serial`, `lot` or null (always
  effective). `effectivity_from` is `''` when open and is read according to
  the kind; `effectivity_to` null means open-ended.
- `ref_designators` is an empty array when absent, never null.

### `bom_closure`

Every ancestor/descendant pair of the designed structure, with the route
between them.

| | |
|---|---|
| Grain | `ancestor_pn`, `ancestor_rev`, `descendant_pn`, `descendant_rev`, `path` |
| Scope | `ancestor_pn` |
| `ORDER BY` | `ancestor_pn`, `ancestor_rev`, `depth` |
| Cadence | set by `bom_edge`; `rebuild_per_root` |

- `depth` 1 is a direct child. Self rows are not stored.
- `path` lists part numbers from the ancestor to the descendant, inclusive. A
  part reached by two routes has **two rows**. Count distinct descendants for
  "what is under this", and count rows for "how many ways".
- `truncated = true` means the walk stopped at `max_depth` below this row. A
  truncated tree is always visible; it is never silently shortened.
- Excluded parts and their subtrees are not walked (Q10).

## 3. Units as built (from MES)

### `serial_unit`

One row per serialized unit.

| | |
|---|---|
| Grain | `part_number`, `serial_number` |
| Scope | `part_number` |
| `ORDER BY` | `part_number`, `serial_number` |
| Cadence | set by `mes`; `incremental_by_key` |

- `build_completed_at` is the default **as-built reference date**. A
  deployment can switch it to `shipped_at` (spec §6.4).
- `derived_test_status` is one of `passed`, `failed`, `in_rework`, `testing`
  or `untested`, computed by the rule in spec §6.6 from the required programs
  (Q4). `mes_status` is MES's own word, verbatim; the two are separate on
  purpose.
- `first_pass` is null when any required program has no result. It is never
  `false`-by-default.
- `top_level_pn` / `top_level_serial`: the outermost unit this one is
  currently installed in, from `as_built_closure` (`scope = current`). A unit
  installed nowhere is its own top level.

### `as_built_edge`

One row per **installation interval**: this child serial was in that parent
serial from `valid_from` to `valid_to`.

| | |
|---|---|
| Grain | `parent_pn`, `parent_serial`, `child_pn`, `child_serial`, `valid_from` |
| Scope | `parent_pn` |
| `ORDER BY` | `parent_pn`, `parent_serial` |
| Projection | `by_child` on (`child_pn`, `child_serial`): "where is this serial" |
| Cadence | set by `mes`; `incremental_by_key` |
| Extra rules | `interval_overlap`, `orphan_reference` |

- `valid_to` null means the child is still installed.
- `is_current`: the interval is open now.
- `is_as_built`: the interval covers the parent's as-built reference date.
  This is the column that answers "what was in it when it was delivered".
- `interval_origin` says where the interval came from:
  - `source`: the MES keeps removal history, and both ends are the MES's
    own.
  - `observed`: the MES keeps only current state. dag-tools compares each
    load with the previous one, and a child that disappears gets a closed
    interval. `valid_from` is still the MES install date; `valid_to` is the
    first load that no longer saw the child.
- `valid_to_precision_s` (observed only): the gap, in seconds, between the
  last load that saw the child and the first that did not. The removal
  happened somewhere in that window, so **an observed `valid_to` is only as
  precise as the load interval**. Set for `observed` intervals, null for
  `source` ones (approved 2026-11-11); also null while an interval is
  open, since there is no `valid_to` yet.
- **Hazard (Q6): history begins at the first load.** For an `observed`
  source, a replacement made before the first load left no trace. For a
  unit reworked before then, `is_as_built` describes what is in it now, not
  what was delivered. A check reports each observed source's first load and
  how many units were built before it. Which kind the deployment's MES is
  is a deployment answer (spec §4.2, `as_built_history`); there is no
  default.

### `as_built_closure`

Every ancestor/descendant pair of the physical structure, computed twice.

| | |
|---|---|
| Grain | `scope`, `ancestor_pn`, `ancestor_serial`, `descendant_pn`, `descendant_serial` |
| Scope | `ancestor_pn` |
| `ORDER BY` | `ancestor_pn`, `ancestor_serial` |
| Projection | `by_descendant` on (`descendant_pn`, `descendant_serial`): "which top-level units contain this serial" |
| Cadence | set by `as_built_edge`; `rebuild_per_root` |

- The `scope` column is `current` (walks `is_current` edges) or `as_built`
  (walks `is_as_built` edges). **Always filter on it.** The union of the two
  is not a meaningful structure.
- `depth` and `truncated` mean what they mean in `bom_closure`.

## 4. Test results (from SystemLink)

### `test_result`

One row per test result: one execution of one program against one unit.

| | |
|---|---|
| Grain | `result_id` |
| Scope | `part_number` |
| `ORDER BY` | `part_number`, `serial_number`, `started_at` |
| Cadence | set by `test_repo`; `incremental_by_key` |

- `status_type` is the source's status, verbatim. `is_terminal` says whether
  that status can still change.
- A result first seen while running is updated in place (same `result_id`)
  when it finishes. Results the source stops returning are **not** deleted
  (spec §5.1).
- `attempt_seq` numbers the attempts per (`part_number`, `serial_number`,
  `program_name`) by `started_at`, then `result_id`. `is_first` is attempt 1.
- `is_latest` is the latest **terminal** attempt. A retest that is still
  running is a row with `is_terminal = false` but is never `is_latest`, so it
  cannot hide the last verdict.
- `operator_ref` is `personal_data`. It holds an HMAC pseudonym by default,
  the name only where the deployment's `operator.store` allows it, or null
  when the deployment drops it. The name never reaches raw storage in the
  pseudonym or drop modes (spec §4.1).

### `test_measurement`

One row per measured parameter.

| | |
|---|---|
| Grain | `result_id`, `step_id`, `parameter_index` |
| Scope | `part_number` |
| `ORDER BY` | `part_number`, `parameter_name`, `started_at`: trend one parameter across units |
| Projection | `by_serial` on (`serial_number`, `result_id`): everything measured on one unit |
| Cadence | set by `test_repo`; `incremental_by_key` |
| Extra rules | `orphan_reference` |

- `parameter_index` is the position in the step's parameter list. Parameter
  **names can repeat** within a step, so the name is not part of the grain.
- `measurement_text` and `limits_text` are verbatim; `measurement_text` is
  `''` when absent. The `*_num` columns are parsed by the grammar in spec §6.5
  and are null when the text is not a number (`PASS`, `NaN`, `inf`).
  Non-finite values are never stored as numbers.
- `parse_failed` is true only when a limit was numeric but the measurement
  could not be parsed against it. A text-only measurement is not a failure.
- `part_number`, `serial_number`, `program_name`, `started_at` and
  `is_latest_result` are denormalized from `test_result`, so the common
  queries need no join.

### `test_catalog`

What is measured for each part: one row per distinct parameter.

| | |
|---|---|
| Grain | `part_number`, `program_name`, `step_path`, `parameter_name`, `units` |
| Scope | `part_number` |
| `ORDER BY` | `part_number`, `parameter_name` |
| Cadence | set by `test_measurement`; `rebuild_per_part` |

- `units` is `''` when absent. Because it is part of the grain, a parameter
  whose units changed shows up as **two rows**, which is what makes that
  change visible.
- `n_results`, `first_seen` and `last_seen` summarize the measurements behind
  each row.

## 5. Operations

### `ingest_status`

One row per source run, for every source.

| | |
|---|---|
| Grain | `source_system`, `run_id` |
| Scope | `source_system` |
| `ORDER BY` | `source_system`, `started_at` |
| Cadence | every source run; `append` |

- `run_id` is the Dagster run id. `mode` is `incremental`, `backfill` or
  `full`.
- `finished_at` null means the run is still going or died. Use it, together
  with `started_at`, to spot a stuck source.
- `records_in`, `records_new` and `records_changed` come from the
  classification in spec §5.1.
- `errors_json` maps each taxonomy event name (spec §5.3) to a count, plus up
  to 20 sample events. It **never** contains a credential.

## 6. Questions that change this contract

The questions from spec §11 that touch a table. Q1, Q2 and Q7 are answered
and built in above. (Q3, on `stepId` uniqueness, changes only the raw
`steps` merge key; the `test_measurement` grain already includes
`result_id`.)

| Question | Status | Tables affected |
|---|---|---|
| Q4: required programs, rework statuses | deployment | `serial_unit.derived_test_status`, `first_pass` |
| Q6: does the MES keep removal history? | deployment; both handled through `interval_origin` | `as_built_edge`, `as_built_closure` (`scope = as_built`) |
| Q9: how `serialized` is known | deployment | `dim_part.serialized` |
| Q10: re-attach below an excluded part? | standing default: no | `bom_closure` |
| Q13: ClickHouse version | standing default: recursive, unrolled fallback | both closures |
