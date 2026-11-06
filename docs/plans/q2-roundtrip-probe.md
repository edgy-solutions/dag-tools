---
id:         q2-roundtrip-probe
status:     approved 2026-11-05 (D4 and D5 with conditions, recorded inline); floor measured
owner:      agent
blocked-on:
closed-by:
repo:       dag-tools
summary:    Q2 recipe item 4. For each RUNNABLE class, a generated probe in the `dag-tools-probes` location loads the output the side's Q2 run just wrote, through the class's real IO manager and Dagster's own input-loading path. It asserts the value is not None and records its type and columns. `qual run` launches it as a second pass after the representatives. It has its own state slot. Q6 gates on parity, with a new strict-by-default gap. Rides 0.12.0.
---

# Q2 IO round-trip probe

## Problem

Q2 launches each representative alone and records what `handle_output`
did. Nothing ever *reads* that output back, so a candidate that still
writes but can no longer load passes Q2. Examples: an IO manager whose
`load_input` changed its path or partition resolution, a type its loader
no longer returns, or an input-context field the executor stopped
populating. The synthetic probes (Q5) do read back, but they read their
own dict payload, and only for SYNTHETIC_REQUIRED classes. RUNNABLE
classes have no `load_input` coverage at all.

Recipe (Q2 item 4): "for each class, also launch the class's probe asset
(a tiny downstream defined in a dag-tools probe code location, or a
designated real downstream) that loads the representative's output via the
same IO manager and asserts non-null/shape."

## Non-goals

- **No "designated real downstream".** A real downstream has upstreams of
  its own that would also have to exist. That is the staging problem again,
  for an asset nobody chose as a representative. This is deferred and
  recorded in RECIPE known limitations.
- **No data-correctness check.** The probe asserts the load works and that
  the shape is the same on both sides. It does not check values.
- **No new code location and no new deploy step.** Round-trip modules ship
  in the same `dag-tools-probes` bundle that `qual synthetic` already
  produces.
- **Q2's selection is unchanged.** The probe is a separate run in a
  separate location. It never widens a representative's launch.

## What was measured (dagster 1.13.1, `dagster dev`, two code locations)

The test workspace had a user location writing `sales/orders`
(unpartitioned) and `sales/daily` (daily-partitioned) through
`FilesystemIOManager`, plus a `dag-tools-probes` location. Every launch
below went through `DagsterGraphQLClient.launch_asset_run`, the path
Q2 uses.

| Probe location declares | Probe launch | Result |
|---|---|---|
| `AssetSpec(key).with_io_manager_key(k)` + downstream `AssetIn(key)` | downstream alone | loads `[1, 2, 3]` written by the user run |
| same, `partitions_def=StaticPartitionsDefinition([pk])` | downstream, `dagster/partition=pk` | loads exactly partition `pk` |
| same, `partitions_def=DynamicPartitionsDefinition(name=…)`, key added via `addDynamicPartition` | downstream, `dagster/partition=pk` | loads exactly partition `pk` (`{'pk': '2026-10-03'}`) |
| upstream file deleted before the probe launch | downstream | `ExecutionStepFailureEvent`, run FAILURE |

Other measured facts:

- **`get_asset_launch_info` is unchanged.** The deployment reports one
  node per key, and it prefers the materializable one. The probe's external
  stand-in, with its different partitions def, did not change the
  representative's `is_executable`, `partition_keys` or `op_names`. Q2 and
  `qual stage` read this, so it matters.
- **A second `addDynamicPartition` of the same key returns
  `DuplicateDynamicPartitionError`.** That is a typed union member, so it
  can be treated as idempotent success by typename.
- **A downstream that returns `MaterializeResult` writes nothing to the
  store.** `handle_output` is never called for it. A downstream that
  returns a value would write its own object into the deployment's real
  storage, and with a database IO manager it would fail on a dict (see D6).

**The compat floor (dagster 1.10.19) was measured too, with the same
workspace shape and the same `DagsterGraphQLClient` path.** It matched on
every point:
- `AssetSpec.with_io_manager_key` exists, and both locations load.
- The unpartitioned load returned `builtins.list`.
- `addDynamicPartition` returned `AddDynamicPartitionSuccess`, and a
  second add returned `DuplicateDynamicPartitionError`.
- The dynamic-partition load read exactly `2026-10-03`.
- `get_asset_launch_info` was unaffected.
- `MaterializeResult` wrote nothing to the store.
- A deleted upstream produced an `ExecutionStepFailureEvent`.
- The D8 freshness query works there:
  `assetMaterializations(partitions:[pk], limit:1) { runId }`.

The generated-source loads test still runs on the compat job (see
Tests), so a future drift is caught in CI.

## What is generated

`qual synthetic` gains a second output. For each class whose first
RUNNABLE representative is round-trip applicable (D2, D7), it writes
`roundtrip_<short>.py` next to the synthetic `<class_hash>.py` files, where
`<short>` is the first 8 characters of the class hash.

```python
_IO_MANAGER_KEY = "io_manager_rt_<short>"
_PARTITIONS = DynamicPartitionsDefinition(name="dagtools_rt_<short>")   # only if the rep is partitioned

_UPSTREAM = AssetSpec(<rep asset_key>, partitions_def=_PARTITIONS).with_io_manager_key(_IO_MANAGER_KEY)

@asset(key=["dagtools_roundtrip_<short>"], ins={"up": AssetIn(key=<rep asset_key>)},
       partitions_def=_PARTITIONS)
def dagtools_roundtrip_<short>(up) -> MaterializeResult:
    if _IO_MANAGER_ERROR is not None: raise RuntimeError(...)
    assert up is not None
    return MaterializeResult(metadata={
        "dagtools/loaded_type": f"{type(up).__module__}.{type(up).__qualname__}",
        "dagtools/loaded_columns": <json list, only if `up.columns` iterates to strings>,
        "dagtools/loaded_len": <int, only if len(up) succeeds>,
    })

defs = Definitions(assets=[_UPSTREAM, dagtools_roundtrip_<short>],
                   resources={_IO_MANAGER_KEY: <real IO manager, manifest probes config>})
```

- **The IO manager is constructed as in synthetic probes:** imported by FQN
  inside `try`, configured from `ProbeSettings.config_for(class_hash, fqn)`,
  with env references rendered as `os.environ[...]` and never resolved.
- **The fallback is not `InMemoryIOManager`.** Input loading happens
  *before* the body runs, so an in-memory fallback would fail with a
  misleading missing-key error and the import error would never surface.
  The fallback is a generated `_ImportFailedIOManager` whose `load_input`
  raises the import error.
- The module must not import `dag_tools.qual.*` (AGENTS 4g1).
- `ProbeManifest` gains `roundtrip_probes: List[RoundTripProbe]` (additive;
  bump `SCHEMA_VERSION_PROBE_MANIFEST`). Each entry records `class_hash`,
  `rep_id`, `asset_key`, `module_name`, `probe_asset_key`,
  `partitions_def_name` (or None), `io_manager_class`, config key *names*,
  `notes`. The manifest is still written last.
- **The probe status check counts round-trip assets.** `qual probes status`
  counts `dagtools_roundtrip_*` as expected assets, never as unexpected.
  The rep's external spec is not a probe asset and is not counted.

## How it runs (D1)

`qual run --side <s>` gains a second pass after the representative pass,
`run_roundtrip_side`, in a new `dag_tools/qual/runs/roundtrip.py`. For each
`roundtrip_probes` entry:

1. **Gate on the rep.** If its Q2 RepState on this side is not PASSED, the
   probe is `SKIPPED` with reason `representative not passed`. Q6 already
   reports that rep red.
2. **Check freshness (D8).** Read the rep's latest materialization from
   the deployment (for the partition, if partitioned). If its `runId` is not
   the rep's Q2 `run_id`, the probe is `SKIPPED` with reason
   `upstream rematerialized since the Q2 run`. This keeps the probe from
   loading another run's output, for example the other side's on shared
   storage.
3. **Register the partition, if partitioned.** The key is the partition
   the Q2 run actually used (D3). Call `addDynamicPartition` on
   `dag-tools-probes`. `AddDynamicPartitionSuccess` and
   `DuplicateDynamicPartitionError` both proceed. Any other typename is
   FAILED with the error.
4. **Launch.** Launch `dagtools_roundtrip_<short>` alone on
   `dag-tools-probes` / `__repository__` / `__ASSET_JOB`, with the
   `dagster/partition` tag set when partitioned, and tags `dagtools/qual`,
   `dagtools/side`, `dagtools/roundtrip=true`, `dagtools/class_hash`.
   Resolved staging overrides are applied as for probes.
5. **Poll and record.** Then build the record. Status rules match PR #19:
   PASSED is sacred, LAUNCHED reconciles, a failed poll stays LAUNCHED, and
   only `DagsterRunNotFoundError` makes it FAILED.

If `dag-tools-probes` is absent from the workspace, or the probe manifest
has no `roundtrip_probes`, the pass records nothing. It prints one line
saying so and `qual run`'s exit status is unchanged. Q6 then sees the gap
(D5).

## State and records (their own slot)

- **Layout:** `qualifications/<qual_id>/<side>/roundtrip/state.json` and
  `<side>/roundtrip/runs/<class_hash>/<run_id>.json`. Keys come from new
  `layout.py` helpers; no literal keys.
- **Types:** `RoundTripStatus` (PENDING / LAUNCHED / PASSED / FAILED /
  SKIPPED) and `RoundTripState` (`class_hash`, `rep_id`, `status`,
  `run_id`, `partition_key`, `rep_run_id`, `error`, `attempts`,
  `last_updated`). They are distinct from `RepState` and `ProbeRepState`, so
  cross-wiring fails type-check (the AGENTS 4g0 rule).
- **Records reuse `RunRecord`** (AGENTS 4g0), with two additive fields:
  - `partition_key: Optional[str]`, which Q2 now also fills (D3);
  - `probe_metadata: Dict[str, str]`, which holds the *values* of
    `dagtools/*` metadata entries only.

  Bump `runs.records.SCHEMA_VERSION`.
- **`LOGS_QUERY` must return entry values.** It currently selects only
  `metadataEntries { label }`. It gains
  `... on TextMetadataEntry { text }`, `... on IntMetadataEntry { intValue }`
  and `... on JsonMetadataEntry { jsonString }`. The schema-validation test
  already covers every `*_QUERY` document.

## Q6 (`qual report`) (D4, D5)

Each class is put in one bucket:

| Bucket | When | Effect |
|---|---|---|
| **covered** | PASSED both sides, `loaded_type` equal, `loaded_columns` equal (or absent on both) | counts toward coverage |
| **red** | one side PASSED and the other FAILED (a FAILED side opposite one that never ran is uncovered); **or** both PASSED and `loaded_type` / `loaded_columns` differ | **blocks GO; no opt-out** |
| **uncovered** | not generated, not deployed, not run, SKIPPED, LAUNCHED, or FAILED on **both** sides | blocks GO unless `--accept-roundtrip-missing` |
| **not applicable** | no IO manager class, or a dbt class (D7) | listed; never blocks |

- `loaded_len` is informational, like durations.
- `Verdict` gains `roundtrip_classes_covered`, `roundtrip_classes_red`,
  `roundtrip_classes_uncovered` and `roundtrip_not_applicable` (additive;
  bump the verdict SCHEMA_VERSION).
- `ClassVerdict` gains `roundtrip_diff`.
- `UPGRADE_VERDICT.md` gets a "Failing round-trip probes" section.
- The CLI gains `--accept-roundtrip-missing`.

## Decisions (proposed)

- **D1. A second pass inside `qual run`, with its own state slot.** The
  recipe makes this Q2 item 4. The probe is only meaningful right after this
  side's rep run, and keeping it in `qual run` means the operator workflow
  gets no new step.
  - *Alternative:* a separate `qual probes roundtrip --side <s>`. That is
    one more command to forget, and the gap between the two commands is
    when D8's staleness creeps in.
- **D2. One probe per class, not per representative.** This follows the
  recipe's "for each class". IO behaviour is a class property (the IO
  manager class is a class-key component), and this halves the extra
  launches. The probe uses the class's first RUNNABLE representative.
  - *Alternative:* per rep. That doubles Q2 launch count for coverage the
    class key says is redundant.
- **D3. The partition is the one Q2 actually used.** It travels as a new
  `RunRecord.partition_key` that Q2 fills from the tag it already sets. For
  records written before this field existed, the partition is read from the
  rep run's `dagster/partition` tag via GraphQL. It is **not** recomputed
  with `last_partition_key`: a time-based partition list grows, so the
  recomputed key could name a partition the Q2 run never wrote, and a
  never-written partition fails like a real load failure.
  - The dynamic stand-in is necessary because generation is offline and
    cannot know that key.
  - **Limit:** an IO manager whose loader reads
    `context.asset_partitions_time_window` will fail on a dynamic stand-in.
    Multi-dimensional keys are not mirrored either. Both get generation
    notes. They fail on both sides, so they land in *uncovered*, not red.
- **D4. Shape parity compares two values, an exception to "metadata
  parity is key-set only" (AGENTS 4h).** The exception is scoped to
  `dagtools/loaded_type` and `dagtools/loaded_columns`. The probe emits
  those itself and they are stable by construction. User metadata stays
  key-set only. A loader that starts returning a different type, or drops
  or adds a column, is precisely what this probe exists to catch, and a
  key-set comparison would call that a pass.
  - **Approved**, on condition that the exception is written into the
    parity rule's own text (AGENTS §4h and the `diff_rep` docstring), with
    the reason: these two values are the probe's own output, not asset
    metadata.
- **D5. Strict by default: a new `GapAcceptance.roundtrip_missing`.** It
  follows the 4h pattern. **Consequence:** every qualification that is GO
  today becomes NO_GO until round-trips are generated, deployed and run, or
  the operator passes `--accept-roundtrip-missing`. Failing on *both* sides
  counts as uncovered rather than red. It means the probe environment
  cannot load (config, or a D3 limit), not that the candidate regressed. The
  errors are printed so this cannot pass silently.
  - **Approved** (deny by default is the house rule), on three
    conditions:
    1. 0.12.0's CHANGELOG lists it under a **Behaviour change** heading,
       not under Added.
    2. The NO_GO blocker line names the flag, like the existing gaps do
       (verdict.py ~292–307): `N class(es) have no round-trip coverage
       (generate via \`dagtools qual synthetic\`, deploy, re-run \`dagtools
       qual run --side baseline|candidate\`); pass
       --accept-roundtrip-missing to ignore`.
    3. Rollout (see below): the team re-runs its current qualification
       once *with* the flag, so they see what else changed before the
       strict run.
- **D6. The probe writes nothing.** It returns `MaterializeResult` and has
  no output for an IO manager to handle (measured). A value-returning probe
  would write `dagtools_roundtrip_*` objects into the deployment's real
  storage, and it would fail on any IO manager that cannot store a dict.
- **D7. Not applicable means no IO manager class, or dbt.** dbt assets
  yield no value for an IO manager to store, so there is no `load_input` to
  test. Detection is the same as the class key's (`dagster_dbt` in
  `integration_libs` or `compute_kind == "dbt"`). These classes are listed
  in the verdict so the gap is visible, but they never block.
- **D8. The freshness guard before launch.** The probe must read what this
  side's Q2 run wrote. If anything rematerialized the rep since (the other
  side on shared storage, a manual run), the probe would load that output
  and could pass on it. The guard costs one GraphQL query per probe.
  Without it, a red candidate load could hide behind a stale baseline
  object.

## Tests (regressions to add)

**Generator** (`test_qual_roundtrip_generate.py`):
- `test_roundtrip_source_parses_and_loads_as_definitions`. This test also
  runs on the compat job.
- `test_roundtrip_probe_returns_materialize_result_and_writes_nothing`
  (in-process `materialize` against `FilesystemIOManager`; the store gains
  no probe object).
- `test_roundtrip_loads_the_partition_it_was_given`
  (`DynamicPartitionsDefinition` via the instance).
- `test_import_failure_surfaces_the_import_error_not_a_missing_key`.
- `test_roundtrip_source_does_not_import_dag_tools_qual`.
- `test_env_reference_renders_os_environ_not_the_literal_value`
  (round-trip copy).
- `test_one_probe_per_class_from_its_first_runnable_rep`.
- `test_dbt_and_io_managerless_classes_get_no_probe_and_are_listed`.
- `test_time_window_and_multi_partition_classes_get_notes`.

**Runner** (`test_qual_roundtrip_runner.py`, fake client):
- `test_a_rep_that_did_not_pass_skips_its_roundtrip`.
- `test_rematerialized_upstream_skips_instead_of_loading_stale_output`.
- `test_partition_comes_from_the_q2_record_not_last_partition_key`.
- `test_old_record_without_partition_reads_the_run_tag`.
- `test_duplicate_dynamic_partition_is_idempotent`.
- `test_roundtrip_launches_the_probe_alone_on_the_probes_location`.
- `test_passed_roundtrip_is_never_relaunched`.
- `test_a_roundtrip_poll_timeout_stays_launched`.
- `test_absent_probes_location_records_nothing_and_does_not_fail_qual_run`.
- `test_state_is_mirrored_after_each_transition`.

**Q6** (`test_verdict_roundtrip.py`):
- `test_type_divergence_is_red_even_with_accept_flag`.
- `test_one_side_failed_is_red_even_with_accept_flag`.
- `test_both_sides_failed_is_uncovered_not_red`.
- `test_missing_roundtrip_blocks_go_without_accept_flag`.
- `test_not_applicable_classes_never_block`.
- `test_loaded_len_difference_does_not_block`.

**Q2:** `test_run_record_carries_the_partition_key_it_launched`.

## Docs (AGENTS 7)

- RECIPE: Q2 item 4 marked implemented, plus the D3 limits. In the
  status table, the "Q2 IO round-trip" row becomes Done and the
  orchestration half stays. In known limitations, the "designated real
  downstream" deferral and the time-window/multi-partition limit.
- AGENTS: update the §4f bullet (now built). New bullets: the round-trip
  slot and types are separate; the D4 value exception is scoped to
  `dagtools/*`; the D8 freshness guard; D6 (write nothing). §4h names the
  new gap.
- README: one line under the qual commands.
- CHANGELOG, Unreleased (0.12.0):
  - an **Added** entry for the probe;
  - a separate **Behaviour change** entry for D5. That entry says
    qualifications that are GO today become NO_GO until round-trips run,
    names `--accept-roundtrip-missing`, and gives the rollout step below.

## Rollout (the team, not this PR)

Before relying on the strict verdict under 0.12.0, re-run the current
qualification once with the flag:
`dagtools qual report --qual-id <id> ... --accept-roundtrip-missing`.
That shows any round-trip *red* classes (which the flag never hides) and
any other change, separated from the expected new "missing" gap. Then
generate, deploy and run the round-trips, and run the strict report. The
flagged run is a one-off comparison, not a standing setting.

## Size

Roughly:
- generator: 150 lines;
- runner: 280 lines;
- verdict and report: 150 lines;
- client and layout: 40 lines;
- tests: around 28.

One PR, implementation through the implementer, with judgment and review
kept in the main session.
