---
id:         qual-stage
status:     approved; D1–D5 as proposed, D2 measured and resolved to grouping, D4 extended
owner:      agent
blocked-on:
closed-by:
repo:       dag-tools
summary:    `dagtools qual stage --side <s>`. For each side, it materializes the upstreams Q2's representatives will load as inputs. It launches one run per partition group, which is usually exactly one. The runs and the registry slot are its own. Q6 never compares it and records only whether it ran. Q2's one-asset selection is unchanged. Rides 0.12.0.
---

# `dagtools qual stage --side <s>`

## Problem

Q2 launches each representative **alone** (AGENTS 4f). Dagster treats a
*computed* upstream as a loaded input, so its output must already exist in
the IO manager. If it does not, the representative dies at input loading
with `DagsterExecutionLoadInputError`.

`qual classes` already reports this per representative (`staging_need:
required`) and in total (`launchable_now_count`). The only remedy is
manual: operator-staged data, dbt `--empty`, or `staging_overrides`
pointing at pre-populated storage. Consumers read "Q2 doesn't build
upstreams" as a defect and stall.

`qual stage` closes the gap without changing what Q2 compares. Before Q2,
it materializes the upstreams **once per side, in separate runs**. It banks
those runs where Q6 never compares them.

## Non-goals

- **No change to Q2's selection.** `plan_asset_selection` still selects
  only the representative. Stage is a different run. Folding the upstreams
  into the representative's launch would turn a one-asset comparison into a
  subgraph build (AGENTS 4f). That stays forbidden, and a regression test
  pins it.
- **Not compared by Q6.** There is no new verdict gate, gap or blocker. A
  failed stage shows up exactly as it would without stage: the affected
  representatives fail in Q2. Q6 reads the stage **state** only to write
  one informational line per side (see "Interplay with Q6"). It never reads
  stage records, and stage never influences `status`.
- **No generated seeder assets.** Stage runs the real upstream code.
  RECIPE's "seeder assets are deliberately NOT built" stays true.
- **No gating.** `Runnability` and `staging_need` are untouched. `qual run`
  never refuses to start because stage has not run.

## Command

```
dagtools qual stage --id <qual_id> --side <baseline|candidate>
                    [--plan] [--force] [--max-keys N]
                    [--poll-interval S] [--timeout S]
                    [--registry-uri ...] [--format json|table]
```

- `--plan` computes and prints the selection and groups. It launches
  nothing and writes nothing.
- `--force` launches again even when this side's stage state is `PASSED`
  for the same selection hash.
- `--max-keys N` refuses to launch when the selection exceeds N keys. The
  default is no limit. It guards against a closure much larger than the
  operator expected.
- Output is JSON by default, like every `dagtools` subcommand (AGENTS 4b).

## What is selected

The input is the class matrix (`classes/equivalence_classes.json`) for the
`qual_id`. That is the manifest-pinned truth (AGENTS 4d/4e). Stage never
reads `latest.json`.

1. **Seeds.** Take every representative with `runnability == RUNNABLE` and
   `staging_need == REQUIRED`. Find its `ClassMember` (match on `repo` and
   `asset_key`) and take that member's `upstream_asset_keys`.
2. **Executability.** Build the index with `build_executability_index` over
   all members. It reads only `asset_key` and `is_executable`, so a member
   can stand in for an `AssetRecord`. This keeps the rule "computed in ANY
   pinned repo means computed".
3. **Closure.** Walk the upstreams transitively from the seeds and keep
   every key the index marks executable (D1). The walk stops at
   external/source keys. The walk is transitive because staging U alone
   fails the same way if U has a computed upstream W that is not in the run.
4. **Deployment cross-check.** Fetch `client.get_asset_launch_info()` once.
   Drop any key the deployment reports as `is_executable is False`. Record
   any key the deployment does not know at all as **unstageable**: this
   location (`manifest.deployment.location_name`) cannot materialize it.
5. **Representatives may appear.** Rep A can be an upstream of rep B. A is
   then staged here, and Q2 still launches A alone afterwards. This is
   intended.

The selection is sorted and hashed into `selection_hash`, a 12-char SHA-256
prefix like the class hash. The hash ties a `PASSED` state to *what* was
staged. When the matrix changes, the hash changes, and the next invocation
launches again without `--force`.

**Reported, not launched:**

| Bucket | Meaning |
|---|---|
| `unknown_reps` | `staging_need == UNKNOWN`. The upstream graph is unknown (pre-v3 inventory), so it cannot be planned. Re-survey. |
| `unknown_closure_keys` | A key in the closure has `upstream_asset_keys is None`. It is staged, but its own upstreams could not be walked. |
| `unstageable_keys` | The deployment does not know the key. Each key is listed with the representatives that depend on it. |
| `uncovered_cross_group_keys` | A key on a cycle between partition groups, which v1 cannot order (see "Partition groups"). Each key is listed with the representatives that depend on it. |

## Partition groups (D2, measured)

Measured on Dagster 1.13.1, both through the real `LAUNCH_MUTATION`
against an in-process workspace and through an in-process run. The
selection mixed a daily and a static partitions definition:

| Selection, partition tag | Launch | Run |
|---|---|---|
| daily + static, a date | `LaunchRunSuccess` | the daily step materializes, then the static step fails |
| daily + static, `a` | `PythonError` (`time data 'a' does not match format`) | none |
| daily + an equal daily definition | accepted | succeeds |
| daily + unpartitioned | accepted | succeeds; the unpartitioned asset ignores the tag |

1.13 does not reject the mix consistently. Depending on the key, it either
errors at launch or fails partway through after partial materialization.
Either way, a mixed run cannot succeed. **v1 therefore groups by partition
definition and launches one run per group.** Report-only is not used.

- **Group identity** is the deployment's `partition_keys` list from
  `launch_info`, taken as a tuple. `AssetNode` exposes no definition
  identity. What makes a run valid is that its one tag is a key of every
  partitioned asset in it. Equal definitions give equal key lists. Two
  different definitions with identical key lists are still compatible,
  because each step checks only its own definition.
- **The partition tag per group** is the LAST key, which is the rule
  `plan_asset_selection` uses. The staged partition is then exactly the one
  Q2 will request for a representative sharing that definition. Factor the
  last-key choice out of `plan_asset_selection` into one helper that both
  call. Do not duplicate it.
- **Unpartitioned keys** ride in the partitioned run when there is at most
  one partitioned group. The measurements show that works. This is the
  common case, and it gives **one run per side**. With two or more
  partitioned groups, the unpartitioned keys form their own group.
- **Order.** Build the dependency graph between groups from the closure's
  edges. Launch the groups in topological order, deterministically, breaking
  ties by sorted group key. A group launches only after every group it
  depends on reaches a terminal status.
- **Cross-group cycles** cannot be ordered. An example is unpartitioned ->
  daily -> unpartitioned while a second partitioned group exists. Every key
  on such a cycle goes to `uncovered_cross_group_keys` and is not launched.
  The rest proceed.
- **A failed group** marks every group that depends on it, transitively, as
  `SKIPPED`, and the reason names the failed group. Independent groups
  still run.
- **Known v1 limit, documented rather than solved.** Some downstreams load
  several partitions of an upstream: an unpartitioned asset over a
  partitioned one, or a non-identity partition mapping. Those need more
  than the last partition staged. This surfaces as a failed step in the
  group's record, never as a silent success.

## How it launches

- **One run per group.** Each run targets `manifest.deployment.location_name`
  and `job_name`, as `run_side` does.
- **Staging overrides are mandatory when declared.** This is the safety
  invariant. Stage writes data, so it must write where Q2 will read. It
  therefore uses the same `resolve_staging_overrides(...)` document,
  resolved once per side and merged with `merge_run_config`. A declared but
  unresolved pointer raises `StagingOverridesNotResolved`, exactly as the
  launcher does. Without the overrides, stage would write staged outputs
  into the deployment's default resources, and Q2 would never find them.
- **Tags:** `dagtools/qual`, `dagtools/side`, `dagtools/purpose: stage`,
  `dagtools/stage_selection_hash`, `dagtools/stage_group`, and
  `dagster/partition` for a partitioned group. There is deliberately no
  `dagtools/class_hash`, so a stage run is never mistaken for a
  representative run on the deployment.
- **Non-subsettable multi_asset.** On `DagsterSubsetNotSupportedError`
  (matched by typename, as in 4f), widen that group's keys to their
  op-mates with `expand_to_op_siblings` and retry that group once.
  - Op-mates share an op, and therefore a partitions definition, so
    widening never moves a key across groups.
  - The retry does not add an op-mate's own computed upstreams.
    Re-planning mid-side would invalidate the selection hash. If those
    upstreams are missing, the step fails and the record says so.
  - Other launch errors are not retried.
- **An empty `launch_info` refuses to launch (D3).** This is deliberately
  unlike Q2, which degrades to a single-key selection because one rep
  failing costs one rep. In stage, a single unlaunchable key fails a whole
  run, and without `launch_info` that key cannot be detected.

## State and records (its own slot)

Layout helpers go in `registry/layout.py` (4b), with no literal key strings
anywhere else. The registry gets `put_stage_state` / `read_stage_state` /
`put_stage_run_record` / `read_stage_run_record`.

```
qualifications/<qual_id>/<side>/stage/state.json          mutable, mirrored after every transition
qualifications/<qual_id>/<side>/stage/runs/<run_id>.json  immutable StageRecord, one per group run
local: ~/.dagtools/quals/<qual_id>/<side>-stage-state.json
```

- `StageState`, `StageGroupState` and `StageStatus` (`PENDING | LAUNCHED |
  PASSED | FAILED | SKIPPED`) are **distinct types** from `RepState` and
  `ProbeRepState`. That makes cross-wiring a type error (same reasoning as
  4g0).
- `StageState` holds the plan: `selection_hash`, the ordered groups and the
  reported buckets. It holds one `StageGroupState` per group, with
  `group_key`, `partition_key`, `selection`, `status`, `run_id`, `error`,
  `attempts` and `finished_at`. The side is `PASSED` only when every group
  is `PASSED`.
- The invariants are the same as Q2's:
  - State is saved after every transition.
  - **PASSED is sacred.** The same selection hash with no `--force` is a
    no-op.
  - **LAUNCHED reconciles** through `get_run_status`. It is never
    relaunched. A poll that fails or times out leaves the group `LAUNCHED`
    (with the error noted), because the run may still be materializing.
    Q2 marks the same case `FAILED`, and stage deliberately does not.
- `StageRecord`, one per group run, holds `run_id`, `group_key`,
  `partition_key`, `status`, `selection` and `selection_hash`. It also
  holds a per-key outcome from the event log (`materialized` / `failed` /
  `not_reached`), plus `error` and timestamps. It is **not** a `RunRecord`.
  It describes no class, and Q6's diff must never be able to consume one.
- **A failed run is still useful.** Keys that materialized are banked in
  the record. The command's output lists which representatives have their
  closure fully materialized and which still do not.

## Interplay with Q6 (`qual report`)

A GO must never be read without knowing whether
staging happened. `Verdict` gains a field, additively. Bump the verdict
`SCHEMA_VERSION` by 1 in the same commit.

```python
stage_by_side: Dict[str, StageSideSummary]   # keys "baseline", "candidate"
# StageSideSummary: status ("not_run" | "passed" | "failed" | "incomplete"),
#   finished_at: Optional[datetime], selection_hash: Optional[str],
#   groups_total: int, groups_passed: int
```

- It is built from `registry.read_stage_state(qual_id, side)` only.
  - A missing state is `not_run`.
  - A state with any group not yet terminal is `incomplete`.
  - When the status is `passed`, `finished_at` is the latest group's finish
    time.
- `UPGRADE_VERDICT.md` renders one line per side under **Criteria**, for
  example `- Staging (baseline): passed at 2026-11-04T22:40Z (1/1 groups)`
  or `- Staging (candidate): not run`. The line is marked informational and
  carries no ✅/🛑 gate mark.
- It never reaches `blocking_issues` and never changes `status`. A side
  whose reps needed no staging still shows `not run`, which is the honest
  statement.

## Interplay with Q2 (`qual run`)

At the start of `run_side`, Q2 logs a WARNING that names `dagtools qual
stage` when both of these hold:
- any RUNNABLE rep is `REQUIRED`;
- this side's stage state is not `PASSED` for the current selection hash.

It never refuses to start, and its state, records and selection are
unchanged.

Operator order per side:
`preflight -> stage -> run -> (redeploy candidate) -> preflight -> stage -> run -> report`

Stage must be run **again on the candidate side**. IO-manager storage
survives a redeploy, so skipping candidate stage means the candidate reps
silently read baseline-written outputs.

## Decisions (approved)

- **D1. Transitive closure, not one level.** "The union of the
  representatives' upstreams", read literally, is one level. That fails
  whenever an upstream has a computed upstream of its own. Stage takes the
  transitive closure of computed ancestors, bounded by `--max-keys`.
- **D2. Partitioned keys.** Measured: a mixed run never succeeds (see
  "Partition groups"). **Resolved:** one run per partition group, in
  dependency order. Usually there is one group.
- **D3. An empty `launch_info` refuses to launch**, where Q2 degrades.
- **D4. Q2 warns and does not gate** when stage is missing. `qual report`
  records the stage status per side (see "Interplay with Q6").
- **D5. dbt `--empty` composes.** If the deployment sets `DBT_EMPTY`, or
  `cli_args: [build, --empty]`, the staged dbt upstreams are built empty
  too. That is consistent, and it carries RECIPE's vacuous-test-parity
  hazard. This decision is documentation only.

## Tests (regressions to add)

`dag_tools_tests/test_qual_stage.py`, test names grouped by area:

- **Selection:**
  - `test_selection_is_the_transitive_closure_of_computed_upstreams`
  - `test_closure_stops_at_external_sources`
  - `test_computed_in_any_pinned_repo_is_staged`
  - `test_unknown_reps_are_reported_not_planned`
  - `test_key_absent_from_the_deployment_is_unstageable_and_names_its_reps`
- **Staging overrides:**
  - `test_staging_overrides_reach_the_stage_run_config`
  - `test_unresolved_staging_pointer_raises_instead_of_launching`
- **Launching:**
  - `test_stage_run_tags_carry_purpose_and_no_class_hash`
  - `test_refused_subset_widens_that_group_to_op_mates_once`
  - `test_empty_launch_info_refuses_to_launch`
- **Partition groups:**
  - `test_one_partitioned_group_and_unpartitioned_keys_is_one_run`
  - `test_two_partition_definitions_launch_as_two_runs_in_dependency_order`
  - `test_each_group_is_tagged_with_its_last_partition_key`
  - `test_a_failed_group_skips_only_the_groups_that_depend_on_it`
  - `test_cross_group_cycle_keys_are_reported_and_not_launched`
- **State:**
  - `test_passed_is_sacred_for_the_same_selection_hash`
  - `test_changed_selection_hash_launches_again`
  - `test_launched_reconciles_via_poll_not_relaunch`
  - `test_state_is_mirrored_after_each_transition`
- **CLI options:**
  - `test_plan_launches_and_writes_nothing`
  - `test_max_keys_refuses_an_oversized_selection`
- **Q6 isolation:**
  - `test_q6_status_is_identical_with_and_without_stage_artifacts` (only
    `stage_by_side` may differ)
  - `test_report_records_stage_not_run_and_passed_at_time_per_side`
  - `test_stage_status_never_reaches_blocking_issues`
- **Q2 isolation:**
  - `test_q2_selection_is_unchanged_by_stage` (the representative is still
    launched alone)
  - `test_q2_warns_when_required_reps_exist_and_stage_has_not_passed`

## Docs (AGENTS 7)

- RECIPE "Staging data for Q2": `qual stage` becomes the first mechanism,
  and dbt `--empty` and `staging_overrides` compose with it. Remove
  "nothing stages data" from Known limitations and update the status table.
  Keep "generated seeder assets are not built", because stage runs real
  upstream code. Add `stage` to the per-side operator sequence.
- AGENTS: add a 4f bullet with the invariants above:
  - own slot;
  - overrides mandatory;
  - Q2 selection unchanged;
  - Q6 records the status but never compares;
  - one run per partition group;
  - re-run per side.
- README: add the command to the `dagtools qual` list.

## Size

The estimate is about 450 lines in `dag_tools/qual/stage/` (`plan.py` pure,
`runner.py`, `state.py`). On top of that come layout and registry helpers,
a CLI command, a verdict field and renderer line, and about 26 tests. There
is no schema bump to `AssetRecord` or the class matrix. Only the verdict's
`SCHEMA_VERSION` changes.
