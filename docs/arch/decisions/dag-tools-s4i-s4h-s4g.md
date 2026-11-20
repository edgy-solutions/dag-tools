# dag-tools decisions: §4i, §4h, §4g-1, §4g0, §4g0a, §4g2

This record covers the decisions in `AGENTS.md` sections 4i, 4h, 4g-1, 4g0, 4g0a and 4g2 at the component-records pin (`add5bc4`): publishing a dbt build to DataHub, the qualification verdict, the probe status report, the probe runner, the synthetic coverage rollup in the verdict, and the IO round-trip probe. The dbt component is described in the [dbt project record](../components/dbt-project.md), and the tools it wires in the [dbt record](../components/dbt.md) and the [DataHub record](../components/datahub.md). The qualification sections run against Dagster, described in the [Dagster record](../components/dagster.md); there is no component record for the qualification system, and its procedure is in the [recipe](../../RECIPE.md). [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §4i dbt to DataHub publishing

The custom dbt project component runs a dbt build, then publishes the build's artifacts to DataHub through a DataHub ingestion recipe. Three dbt invocations run in order: source freshness, the build, and docs generation. The recipe reads files those invocations write.

### §4i.1 Every dbt invocation in one execution shares one target directory

- **Decision.** The freshness invocation's target directory is taken and passed to the build and to docs generation, so all three write into one directory.
- **Why.** The dbt resource creates a new target directory for each invocation unless it is given one. The ingestion recipe names its files relative to one working directory, so an artifact written elsewhere is never read. The census reports the failure this prevented: ingestion could not find the sources file, because only the freshness invocation writes it and that invocation was the one left with its own directory.
- **Status.** `built`.

### §4i.2 The build's run results are kept under their own name before docs generation

- **Decision.** The build's run results file is copied to a second name before docs generation runs, and the recipe reads the copy.
- **Why.** Docs generation overwrites the run results file. The build's results are what carry materialization lineage into DataHub.
- **Status.** `built`.

### §4i.3 A publishing failure never fails the dbt build

- **Decision.** Every failure inside the publishing step is caught and logged. A test of this path asserts on the artifacts and on what DataHub received, never only on the absence of an exception.
- **Why.** A DataHub outage must not cost the tables the build produced. Because every failure is swallowed, a test that only checks that execution did not raise cannot see ingestion break; the census reports that this is how the scattered-artifacts defect in §4i.1 shipped.
- **Status.** `built`.

### §4i.4 The DataHub token comes from configuration or the environment, and an absent token is omitted

- **Decision.** The token is the explicit configured value, else the standard DataHub token environment variable, resolved the same way the DataHub lineage component resolves it. When there is no token, the recipe carries no token key at all.
- **Why.** A DataHub server with authentication on rejects an unauthenticated request. The failure is misleading: schema inference reads metadata back from the server before emitting, so the rejection shows as a source-side error with zero events produced, not as a sink authentication error. An empty token is sent as a credential and rejected. The census reports a live comparison against a server with authentication on (failures and no events without the token; no failures and events emitted with it); no recorded run of that is cited.
- **Status.** `built`.

### §4i.5 The worked example must resolve

- **Decision.** A test fails when a dbt example names a project path that does not exist in the repository.
- **Why.** The census reports that the one worked example of this component once pointed at two projects absent from the repository, so the path could never load and had never run anywhere. At the pin, the deployment's definitions wire dbt only through the demo-mode surface in §4i.7; outside demo mode the deployment builds no dbt assets. The census statement that the definitions carry no dbt reference no longer holds at the pin.
- **Status.** `built`.

### §4i.6 Docs generation is not given a Dagster context

- **Decision.** Docs generation runs without a Dagster context and is waited on rather than streamed.
- **Why.** Given a context, the dbt integration appends the run's selection to the invocation, and that selection does not match sources. The catalog then covers models only and DataHub drops source column schemas. A catalog describes the project, not the run. Waiting rather than streaming also means the build's materializations are not emitted a second time.
- **Status.** `built`.

### §4i.7 The demo dbt surface exists only in demo mode and never takes the code location down

- **Decision.** A seed-to-model-to-model dbt project is registered only in demo mode. It uses seeds, not sources. It builds through the public dbt asset decorator and the component's own execution, constructs the component through its dataclass, and drops fields the installed component does not support. Every failure degrades to empty definitions.
- **Why.** A production deployment must not grow synthetic tables or publish demo lineage to a real catalog. Freshness runs before the build, so a source would need a raw table nothing has created yet, and the demo would fail on every clean cluster. Bypassing the constructor leaves inherited fields unset and execution then fails mid-materialization. An exception at registration would take the whole code location down.
- **Status.** `built`.

### §4i.8 Only the build invocation takes configured arguments, which is what makes an empty build safe

- **Decision.** The component's configured CLI arguments reach the build invocation alone. The freshness and docs invocations are fixed. An empty build is set through those configured arguments or through dbt's environment variable for it, never as a global flag.
- **Why.** The empty-build flag exists on the build, compile and run commands only; docs generation has a different flag of a similar name. Passing configured arguments to every invocation would hand that flag to commands that reject it. Under an empty build, dbt tests pass on empty relations, so asset-check parity in a qualification is not evidence of data correctness; the recipe documents this hazard.
- **Status.** `built`.

### §4i.9 The path is trusted only under an end-to-end run

- **Decision.** An end-to-end test runs a real database, the real dbt command line and the real DataHub command line against an in-process stand-in for the DataHub server, behind an environment gate. The stand-in answers "not found" when asked for an aspect.
- **Why.** A real DataHub server needs several backing services. The artifact handoff fails on the ingestion source side, long before the sink, so a stand-in server still exercises the part that broke. Schema inference looks up each target dataset's schema aspect, and an empty success answer makes the client raise instead of treating the aspect as absent.
- **Status.** `built`.

## §4h Verdict

The verdict is the last qualification phase. It compares the baseline and candidate sides and answers go or no-go, with the reasons as blocking issues.

### §4h.1 Metadata parity compares the set of keys, not their values

- **Decision.** A run's metadata on each side is compared as a set of keys. Values are not compared.
- **Why.** Metadata values legitimately differ between the two sides, for example timestamps and run identifiers. Comparing values would make every qualification fail.
- **Where else.** §4g2.4 is the one scoped exception, for two round-trip probe keys.
- **Status.** `built`.

### §4h.2 Duration differences are reported and never gate

- **Decision.** The duration difference between the two sides is computed for each representative and does not enter the pass decision.
- **Why.** A slower candidate is a matter for the operator's judgement, not a verdict gate.
- **Status.** `built`.

### §4h.3 Strict by default; each known gap needs its own acceptance

- **Decision.** Every gap the verdict cannot evaluate defaults to blocking. The operator accepts each one with its own flag: co-upgrade risks, missing synthetic coverage, deferred orchestration checks and missing round-trip coverage. A new gap follows the same pattern.
- **Why.** Treating an absent check as a passing one would report a qualification that nobody checked as a pass.
- **Status.** `built`.

### §4h.4 A failed candidate preflight blocks with no override

- **Decision.** A failed candidate preflight, or one that has not been run, blocks the verdict. No acceptance flag excuses it.
- **Why.** If the candidate deployment failed preflight, the fix is to the deployment, not to the verdict.
- **Status.** `built`.

## §4g-1 Probe status

The probe status command reports whether the synthetic probes are loaded in the probes code location, before anyone launches them.

### §4g-1.1 The probes location name is one constant shared with the runner

- **Decision.** The location name is fixed, and the status module takes it from the same constant the runner uses. A configurable name would come from the qualification manifest, not from a per-call override.
- **Why.** The deployment contract is fixed for the life of one qualification.
- **Status.** `built`.

### §4g-1.2 An absent location is its own state

- **Decision.** When the workspace has no probes location at all, the report says it is absent. That is distinct from a location that is loaded and holds no probes.
- **Why.** An absent location means the operator did not register it. An empty one means no probes are deployed. The two have different fixes.
- **Status.** `built`.

### §4g-1.3 A partial load is reported, not bundled into "missing"

- **Decision.** Each probe reports its upstream and downstream asset separately, and a probe with one loaded and the other not is reported as partial.
- **Why.** A partial load usually means an edit broke one asset, or that the fallback for a failed IO manager import is hiding a class import problem. Both need the operator's attention, and calling them "missing" would hide the asymmetry.
- **Status.** `built`.

### §4g-1.4 Unexpected probe-shaped assets are informational

- **Decision.** Probe-shaped assets the current bundle does not expect are listed. They do not change whether every expected probe is reported as loaded.
- **Why.** Stale files from an earlier bundle are the usual cause. Failing on them would make the operator clear the probe directory on every iteration.
- **Status.** `built`.

## §4g0 Probe runner

The probe runner launches each deployed synthetic probe on one side of a qualification and records the result.

### §4g0.1 Probe state is a separate slot with separate types

- **Decision.** Probe state and probe run records live under their own registry keys for each side, apart from the representatives' state and records. The probe state types are distinct from the representative state types.
- **Why.** One class can be covered both by a runnable representative and by a probe, and neither result may overwrite the other. Distinct types make accidental cross-wiring fail at type-check time.
- **Status.** `built`.

### §4g0.2 The runner launches the upstream and the downstream asset in one run

- **Decision.** Each probe run selects both of its assets.
- **Why.** Launching only the downstream does not materialize the upstream. Dagster treats the upstream as a loaded input that must already exist, and the run fails loading it. The probe exists to exercise the write and the read back through the real IO manager in one run. The census reports that an earlier version launched only the downstream and that a live run disproved its assumption; no recorded run of that is cited.
- **Status.** `built`.

### §4g0.3 The location name is fixed, not overridden per call

- **Decision.** Probes launch against the fixed probes location. A configurable name would come through the qualification manifest's deployment settings.
- **Why.** The same as §4g-1.1: the deployment contract is fixed for one qualification, and it is the contract with the probes location's definitions.
- **Status.** `built`.

### §4g0.4 Probe runs reuse the representative run record schema

- **Decision.** A probe run writes the same run record schema the representatives use. A probe-specific field would be added to that schema with a default.
- **Why.** The verdict can then compare probe runs the same way it compares representative runs, and both paths stay forward-compatible.
- **Status.** `built`.

### §4g0.5 A passed probe is never relaunched, and a launched one is reconciled by polling

- **Decision.** A re-invocation skips probes that passed. A probe left launched is reconciled by polling its run, not by launching it again. State is saved after every transition.
- **Why.** The same rules as the representative runner. Relaunching would spend compute and leave two records for one probe on one side.
- **Status.** `built`.

## §4g0a Synthetic coverage in the verdict

For classes that cannot run on real data, the verdict counts coverage from the probes instead.

### §4g0a.1 A class is covered only when its probes pass on both sides and the records agree

- **Decision.** A class counts as covered when its probes passed on both sides and, where run records exist, those records compare cleanly. A pass on one side only does not count.
- **Why.** Coverage means the candidate behaves as the baseline did. A one-sided pass says nothing about that.
- **Status.** `built`.

### §4g0a.2 A failing or divergent probe blocks with no opt-out

- **Decision.** A class whose probe failed on either side, or whose probes passed on both sides with records that diverge, blocks the verdict. The missing-coverage acceptance does not excuse it.
- **Why.** The acceptance excuses coverage that is missing. A probe that fails or diverges is a regression signal, which is what the verdict exists to catch.
- **Status.** `built`.

### §4g0a.3 Uncovered classes are what the missing-coverage acceptance excuses

- **Decision.** A class that is neither covered nor failing (pending, launched or not deployed) is uncovered. Uncovered classes block unless the operator accepts missing synthetic coverage.
- **Why.** This separates "not yet known" from "known bad", so the acceptance cannot be used to wave through a failure.
- **Status.** `built`.

### §4g0a.4 Missing records degrade to the status check

- **Decision.** When probe state shows a pass on both sides but a run record cannot be read, the class still counts as covered.
- **Why.** The record comparison adds to the status check; it does not replace it. The census names probe runs that predate the records and a lost record write as the cases.
- **Status.** `built`.

### §4g0a.5 The per-class probe comparison is carried for synthetic classes only

- **Decision.** Each class verdict carries the probe record comparison only for classes that require synthetic probes. The rendered verdict lists failing synthetic probes in a section of their own.
- **Why.** The comparison only exists for those classes, and a failing probe needs to be visible on its own in the verdict document.
- **Status.** `built`.

## §4g2 IO round-trip probe

The round-trip probe loads, through the real IO manager, the output a representative's qualification run wrote, and reports the type and columns it got back.

### §4g2.1 Its state slot and types are separate again

- **Decision.** Round-trip state and records have their own types and their own registry keys for each side, apart from the representatives' and the probes'. A passed round trip is never relaunched, a launched one is reconciled by polling, and only a run the server no longer knows marks it failed; any other poll error leaves it launched. State is saved after every transition.
- **Why.** The same reasons as §4g0.1 and §4g0.5. A failed poll says nothing about the run.
- **Status.** `built`.

### §4g2.2 The probe writes nothing

- **Decision.** The probe returns a materialization result and no value, so nothing reaches an IO manager and nothing is written to the deployment's storage. The upstream is declared as an external asset bound to the real IO manager, so loading the input takes Dagster's own path. When the IO manager cannot be imported, a stand-in whose load raises the import error is bound instead.
- **Why.** A probe that wrote would change the deployment it is measuring. An in-memory stand-in would fail the input load first with a misleading missing-key error, hiding the import error.
- **Status.** `built`.

### §4g2.3 The probe loads only what the qualification run wrote, for the partition that run used

- **Decision.** The pass is skipped, and re-evaluated on the next invocation, when the representative's latest materialization is not the qualification run, and when the representative's qualification run did not pass. The partition is the one recorded for that run, falling back to the run's partition tag, and is never recomputed from the latest partition. If the partition cannot be determined, the pass is skipped, not failed.
- **Why.** Loading a newer output, or a different partition, would compare the wrong data. A partition that has moved since the run is the case recomputation gets wrong.
- **Status.** `built`.

### §4g2.4 Shape parity compares two values, as a scoped exception to §4h.1

- **Decision.** For the round-trip probe only, the verdict compares the values of the loaded type and the loaded columns between the two sides. The loaded length is reported and never gates. Value comparison does not extend beyond those two keys.
- **Why.** A different loaded type or column set is exactly the regression the probe exists to catch. Everywhere else, values legitimately differ between sides.
- **Status.** `built`.

### §4g2.5 Covered, red, uncovered and not applicable

- **Decision.** A class is covered when it passed on both sides with equal type and columns. It is red when it passed on one side and failed on the other, or when the shape differs; red blocks with no opt-out. Anything else, including a failure on both sides, is uncovered and is excused by the missing round-trip acceptance. A class with no IO manager, such as a dbt class, is listed as not applicable and never blocks. A runnable class with no round-trip coverage blocks by default. The round-trip pass never changes the exit status of the qualification run command.
- **Why.** A failure on both sides is not a regression between them. A difference between the sides is. Blocking on missing coverage by default follows §4h.3; the census records it as a behaviour change.
- **Status.** `built`.

### §4g2.6 Regressions for §4g2.1 to §4g2.5

This bullet lists the test modules that pin the five decisions above. It states no decision of its own. The tests are in the Sources table.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Test files are under `dag_tools_tests/`. `dpc` is `dag_tools/components/dbt_project/component.py`, `dda` is `dag_tools/user_deployment/demo_dbt_assets.py`, `udd` is `dag_tools/user_deployment/definitions.py`, `rcp` is `docs/RECIPE.md`, `vdf` is `dag_tools/qual/verdict/diff.py`, `vvd` is `dag_tools/qual/verdict/verdict.py`, `loc` is `dag_tools/qual/synthetic/location.py`, `pst` is `dag_tools/qual/probes/status.py`, `prn` is `dag_tools/qual/probes/runner.py`, `pss` is `dag_tools/qual/probes/state.py`, `lay` is `dag_tools/qual/registry/layout.py`, `rrt` is `dag_tools/qual/runs/roundtrip.py`, `srt` is `dag_tools/qual/synthetic/roundtrip.py`, `qcl` is `dag_tools/qual/cli.py`. Test abbreviations: `tda` is `test_dbt_datahub_artifacts.py`, `tde` is `test_dbt_empty_staging.py`, `tds` is `test_demo_dbt_surface.py`, `tex` is `test_example_dbt_projects_resolve.py`, `te2` is `test_dbt_datahub_e2e.py`, `tvd` is `test_verdict_diff.py`, `tvr` is `test_verdict.py`, `tvt` is `test_verdict_roundtrip.py`, `tps` is `test_probes_status.py`, `tpc` is `test_dagtools_qual_probes_status_cli.py`, `tpr` is `test_probes_runner.py`, `trr` is `test_qual_roundtrip_runner.py`, `trg` is `test_qual_roundtrip_generate.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4i.1 one target directory | dpc `:123-126`, `:136-139`, `:158` | AGENTS.md §4i, bullet 1 | tda: `test_dbt_artifacts_are_not_scattered_across_target_directories` (`:160`), `test_every_artifact_the_datahub_recipe_names_is_in_the_ingest_cwd` (`:132`) | `built` |
| §4i.2 build results copied before docs | dpc `:144-145`, `:194-196` | bullet 2 | tda: `test_run_results_from_build_survive_docs_generate` (`:193`) | `built` |
| §4i.3 publishing failure swallowed | dpc `:161`, `:223-224` | bullet 3 | tda: `test_every_artifact_the_datahub_recipe_names_is_in_the_ingest_cwd` (`:132`); te2: `test_dbt_build_publishes_to_datahub_end_to_end` (`:294`), behind an environment gate | `built` |
| §4i.4 token from config or environment; omitted when absent | dpc `:181-182`, `:186-187` | bullet 4 | tda: `test_recipe_carries_the_datahub_token_from_the_environment` (`:241`), `test_an_explicit_token_beats_the_environment` (`:247`), `test_recipe_omits_the_token_when_there_is_none` (`:255`); the census reports a live comparison; no recorded run cited | `built` |
| §4i.5 example resolves; deployment wires dbt in demo mode only | udd `:239-259` | bullet 5 | tex: `test_there_is_at_least_one_dbt_example` (`:55`), `test_example_dbt_project_exists` (`:70`) | `built` |
| §4i.6 docs generate without context | dpc `:158` | bullet 6 | tda: `test_docs_generate_is_not_given_a_context` (`:264`) | `built` |
| §4i.7 demo surface gated; degrades to empty | dda `:124-133`, `:143`, `:151-164`, `:185`, `:211`, `:229-234`; udd `:239-259` | bullet 7 | tds: `test_no_dbt_assets_when_demo_mode_is_off` (`:88`), `test_demo_mode_registers_the_dbt_surface` (`:103`), `test_a_missing_project_directory_degrades_instead_of_raising` (`:148`), `test_an_unparseable_project_degrades_instead_of_raising` (`:156`), `test_the_demo_project_declares_no_sources` (`:182`), `test_unsupported_component_fields_are_dropped_not_raised` (`:199`) | `built` |
| §4i.8 configured arguments reach the build only | dpc `:123-124`, `:136-139`, `:158`; rcp `:408`, `:452-460` | bullet 8 | tde: `test_resolved_cli_args_reach_only_the_build_invocation` (`:61`), `test_the_non_build_invocations_are_hardcoded_and_unparameterized` (`:76`), `test_empty_is_not_a_global_config_flag` (`:89`), `test_empty_makes_dbt_tests_vacuous_is_documented` (`:103`); te2: `test_dbt_accepts_empty_on_build_and_not_on_docs_generate` (`:387`), `test_dbt_empty_env_var_is_the_documented_name` (`:396`), behind an environment gate | `built` |
| §4i.9 end-to-end run under a gate | te2 `:42-44`, `:112-115` | bullet 9 | te2: `test_dbt_build_publishes_to_datahub_end_to_end` (`:294`); no recorded run cited | `built` |
| §4h.1 key-set parity | vdf `:59`, `:165-167` | AGENTS.md §4h, bullet 1 | tvd: `test_diff_compares_metadata_KEY_set_not_values` (`:123`) | `built` |
| §4h.2 duration informational | vdf `:67`, `:132`, `:205` | bullet 2 | tvd: `test_diff_duration_is_informational_not_gating` (`:167`) | `built` |
| §4h.3 strict by default; gaps accepted one by one | vvd `:60-80` | bullet 3 | tvr: `test_verdict_no_go_by_default_when_orchestration_not_accepted` (`:208`), `test_verdict_go_when_all_gates_pass_and_known_gaps_accepted` (`:222`), `test_verdict_no_go_when_co_upgrade_risks_unaccepted` (`:329`), `test_verdict_go_when_co_upgrade_risks_accepted` (`:348`) | `built` |
| §4h.4 candidate preflight absolute | vvd `:305-308` | bullet 4 | tvr: `test_verdict_no_go_when_candidate_preflight_failed` (`:247`) | `built` |
| §4g-1.1 one location constant | loc `:9`; pst `:45` | AGENTS.md §4g-1, bullet 1 | tpr: `test_run_probes_side_launches_against_dag_tools_probes_location` (`:178`) | `built` |
| §4g-1.2 absent is its own state | pst `:83-84`, `:185-194` | bullet 2 | tps: `test_build_report_location_absent` (`:144`), `test_build_report_location_error_state` (`:158`), `test_e2e_status_handles_absent_location` (`:285`) | `built` |
| §4g-1.3 partial load surfaced | pst `:65-70`, `:221-248` | bullet 3 | tps: `test_build_report_partially_loaded` (`:100`), `test_build_report_missing_when_no_assets_loaded` (`:88`); tpc: `test_cli_status_exit_nonzero_on_gap` (`:128`) | `built` |
| §4g-1.4 unexpected assets informational | pst `:97`, `:110-112`, `:285` | bullet 4 | tps: `test_build_report_unexpected_asset_keys` (`:118`), `test_build_report_fully_loaded` (`:74`) | `built` |
| §4g0.1 separate probe slot and types | pss `:30`, `:46`; lay `:150-159`, `:165-170` | AGENTS.md §4g0, bullet 1 | tpr: `test_run_probes_side_persists_run_record` (`:207`) | `built` |
| §4g0.2 upstream and downstream in one run | prn `:372-373` | bullet 2 | tpr: `test_run_probes_side_launches_against_dag_tools_probes_location` (`:178`), `test_run_probes_side_launches_and_passes_one_probe` (`:159`) | `built` |
| §4g0.3 fixed location | prn `:58`, `:391`; loc `:9` | bullet 3 | tpr: `test_run_probes_side_launches_against_dag_tools_probes_location` (`:178`) | `built` |
| §4g0.4 reuses the run record schema | prn `:48` | bullet 4 | tpr: `test_run_probes_side_persists_run_record` (`:207`), `test_run_probes_side_failed_run_records_record` (`:354`) | `built` |
| §4g0.5 passed sacred; launched reconciled | prn `:247-261`, `:291`, `:308-345`; pss `:111` | bullet 5 | tpr: `test_run_probes_side_skips_passed_probes_on_re_invocation` (`:249`), `test_run_probes_side_reconciles_launched_via_poll_not_relaunch` (`:269`), `test_run_probes_side_mirrors_state_to_registry_after_each_transition` (`:312`), `test_a_probe_poll_timeout_stays_launched_and_reconciles` (`:418`), `test_a_probe_run_that_no_longer_exists_fails` (`:439`) | `built` |
| §4g0a.1 covered needs both sides and agreeing records | vvd `:225-260` | AGENTS.md §4g0a, bullet 1 | tvr: `test_verdict_matching_probe_records_count_as_covered` (`:581`), `test_verdict_partial_probe_coverage_still_blocks` (`:474`) | `built` |
| §4g0a.2 failing or divergent probe blocks | vvd `:240-260`, `:313-325` | bullet 2 | tvr: `test_verdict_no_go_when_probe_failed_even_with_synthetic_accept` (`:444`), `test_verdict_diverged_probes_blocks_go_even_with_passing_status` (`:531`) | `built` |
| §4g0a.3 uncovered excused by the flag | vvd `:261-263`, `:326-331` | bullet 3 | tvr: `test_verdict_no_go_when_synthetic_classes_without_probe_coverage` (`:366`) | `built` |
| §4g0a.4 missing records degrade | vvd `:225-238` | bullet 4 | tvr: `test_verdict_go_when_probes_pass_on_both_sides` (`:417`) | `built` |
| §4g0a.5 probe comparison for synthetic classes only | vdf `:255`, `:329-333`; vvd `:452`, `:566` | bullet 5 | tvr: `test_publish_verdict_writes_both_json_and_md` (`:624`), `test_render_markdown_uses_go_or_nogo_headline` (`:662`) | `built` |
| §4g2.1 separate slot; passed sacred; poll | rrt `:79`, `:87`, `:312-334`, `:380`, `:390`, `:516`, `:531`; lay `:183`, `:191-196` | AGENTS.md §4g2, bullet 1 | trr: `test_passed_roundtrip_is_never_relaunched` (`:220`), `test_a_roundtrip_poll_timeout_stays_launched` (`:229`), `test_state_is_mirrored_after_each_transition` (`:257`) | `built` |
| §4g2.2 probe writes nothing | srt `:178`, `:190`, `:234`, `:254`, `:263-283` | bullet 2 | trg: `test_roundtrip_probe_returns_materialize_result_and_writes_nothing` (`:69`), `test_import_failure_surfaces_the_import_error_not_a_missing_key` (`:96`), `test_roundtrip_source_parses_and_loads_as_definitions` (`:59`), `test_roundtrip_source_does_not_import_dag_tools_qual` (`:109`) | `built` |
| §4g2.3 freshness and partition | rrt `:66`, `:84`, `:397-426`, `:456-480` | bullets 3-4 | trr: `test_a_rep_that_did_not_pass_skips_its_roundtrip` (`:129`), `test_rematerialized_upstream_skips_instead_of_loading_stale_output` (`:139`), `test_partition_comes_from_the_q2_record_not_last_partition_key` (`:149`), `test_old_record_without_partition_reads_the_run_tag` (`:164`), `test_undeterminable_partition_skips_it_does_not_fail` (`:173`); trg: `test_roundtrip_loads_the_partition_it_was_given` (`:83`) | `built` |
| §4g2.4 two values compared | vdf `:212` | bullet 5 | tvt: `test_type_divergence_is_red_even_with_accept_flag` (`:97`), `test_columns_divergence_is_red` (`:108`), `test_loaded_len_difference_does_not_block` (`:182`) | `built` |
| §4g2.5 buckets; exit status unchanged | vvd `:333-345`, `:723-780`; qcl `:905-946`, `:1196-1199` | bullet 6 | tvt: `test_one_side_failed_is_red_even_with_accept_flag` (`:116`), `test_both_sides_failed_is_uncovered_not_red` (`:125`), `test_failed_opposite_a_side_that_never_ran_is_uncovered_not_red` (`:139`), `test_missing_roundtrip_blocks_go_without_accept_flag` (`:156`), `test_not_applicable_classes_never_block` (`:168`); trr: `test_absent_probes_location_records_nothing_and_does_not_fail_qual_run` (`:247`), `test_roundtrip_launches_the_probe_alone_on_the_probes_location` (`:201`); trg: `test_dbt_and_io_managerless_classes_get_no_probe_and_are_listed` (`:163`), `test_time_window_and_multi_partition_classes_get_notes` (`:182`); no test of the exit status | `built` |

### Rows that carry no status

These rows count or point at records, or name a decision that is superseded or decides nothing here. None is a claim about the mesh, so none carries a status.

| Claim | Source | Decision | Evidence |
|---|---|---|---|
| §4g2.6 regression list | trr, trg, tvt | bullet 7 | the round-trip tests above |
