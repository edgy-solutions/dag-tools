# A sustainment case: qualifying a Dagster upgrade (WT-SUSTAIN)

A team that sustains the mesh wants to move Dagster to a newer version. Before the upgrade is allowed, they need evidence that every repository's pipelines still load, still run and still read and write their data the same way. dag-tools' qualification tooling produces that evidence. This walkthrough follows one qualification from survey to verdict. It is framed by elements of the NATO *Data Centric Reference Architecture for the Alliance*, version 2:

- the infrastructure plane (Appendix 14, paragraphs 2 to 4), where Dagster and its images run;
- the operational data plane (Appendix 14, paragraphs 5 to 7), where the producers' pipelines that must keep working sit.

Each step names the part of the tooling that does it and the records it relies on. A step adds no claim of its own: its status is the status of the records it cites, and its evidence is theirs.

**This record describes; it proposes nothing.** Where a gap has a remedy, it is a row in [the roadmap note](../ROADMAP.md), marked `proposed`; none of the gaps below has such a row yet. Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **Every step is `built` with a test, and no qualification has a recorded run.** The tooling runs against a live Dagster through its GraphQL API. No run of it against a running deployment is recorded, so nothing here is `measured`. `built`
2. **What is qualified is frozen when the qualification starts.** The manifest pins each repository's inventory at a commit and is written once. Every later step reads the manifest, never the registry's moving pointer. `built`
3. **A deployment is identified by the bytes of its image.** Continuous integration records each build's image digests per commit, and a release renames that build rather than building again. Preflight matches a running code location by digest first. `built`
4. **The verdict is strict by default, with one gate nothing can waive.** Each known gap blocks a pass until the operator accepts it by name. A failed preflight of the candidate blocks with no override. A failing or diverging probe blocks with no opt-out. `built`
5. **The probes need a code location that neither chart deploys.** The probe location is built in code and loads whatever a directory holds. A deployment adds it. `built`

## The walkthrough, step by step

| # | Step | What happens | Component | Status | Evidence | Records relied on |
|---|---|---|---|---|---|---|
| 1 | Survey each repository | Each repository's code locations are loaded at a commit and inventoried. If any location fails to load, nothing is published. Warnings raised during load are kept in the record. | qualification survey | `built` | `test_run_survey_refuses_to_publish_when_load_fails` | [§4c.1, §4c.2](../decisions/dag-tools-s4e-s4a-s7.md) |
| 2 | Publish to the registry | Per-build artifacts are written once and never overwritten. The pointer to the latest build is written last, so a reader never sees a partial publish. | qualification registry | `built` | `test_storage_put_immutable_refuses_overwrite`; `test_pointer_not_updated_when_artifact_write_fails` | [§4b.2, §4b.3](../decisions/dag-tools-s4e-s4a-s7.md) |
| 3 | Record the image digests | Each main-branch build's digests are written as one record per commit, on a records branch, by a writer that opens no storage. A record is complete for the whole fleet or not written, and is never rewritten. A release renames the build of its commit rather than building again, then checks the record and attaches it. | continuous integration; image record writer | `built` | `test_a_tag_ref_does_not_build`; `test_the_writer_needs_no_registry_and_opens_no_storage`; `test_completeness_is_measured_against_the_declared_fleet`; `test_an_existing_record_is_never_rewritten`; `test_a_moved_digest_fails_the_release_naming_both` | [§8.1, §8.6 to §8.9](../decisions/dag-tools-s4o2-s8.md) |
| 4 | Start the qualification | A manifest pins each repository's inventory at a commit, under one identifier written once. It lists dependencies outside the Dagster family that the upgrade would also move. | qualification manifest | `built` | `test_re_init_same_qual_id_raises`; `test_dagster_family_is_filtered_out`; `test_dbt_core_major_bump_is_major` | [§4d.1 to §4d.3](../decisions/dag-tools-s4e-s4a-s7.md) |
| 5 | Group the assets | Assets are grouped into classes read from the manifest. Whether a class runs, needs a synthetic probe or is only observed comes from tags. Whether a class's upstream data must be staged is reported, and an unknown need is never read as none. | class matrix | `built` | `test_classify_synthetic_required_via_tag`; `test_pre_v3_inventory_classifies_unknown_not_none_needed` | [§4e.1, §4e.4, §4e.5](../decisions/dag-tools-s4e-s4a-s7.md) |
| 6 | Preflight both sides | Each check becomes an entry in a report, and none raises. Each code location is matched to its pin by digest, then by commit tag, then by a stamp, and an unidentified location fails. A pin that names an image record has it fetched and checked again. The candidate side also checks that sampled past runs still render. | preflight | `built` | `test_run_preflight_graphql_failure_becomes_failed_check`; `test_the_digest_decides_over_a_tag_that_claims_the_pin`; `test_an_unverified_location_fails_the_check`; `test_changed_bytes_fail_even_when_the_location_matches_by_digest`; `test_candidate_preflight_samples_baseline_runs` | [§4g.1 to §4g.4](../decisions/dag-tools-s4g1-s4g-s4f.md) |
| 7 | Stage and run | A staging pointer is resolved once per side, and an unresolved one stops the launch. The staging pass refuses to launch with no launch information. One representative per class is launched alone, widened only if the deployment refuses the subset. State is saved after each transition. A passed representative is never relaunched, and a failed poll leaves a run launched rather than failed. | qualification runner | `built` | `test_the_pointer_itself_never_reaches_run_config`; `test_an_unresolved_pointer_raises_instead_of_launching`; `test_empty_launch_info_refuses_to_launch`; `test_a_subsettable_dbt_op_does_not_drag_in_the_whole_project`; `test_run_side_state_is_mirrored_to_registry_after_each_transition`; `test_a_poll_timeout_leaves_the_rep_launched_and_is_never_relaunched`; `test_a_run_that_no_longer_exists_fails_so_the_next_pass_relaunches` | [§4f.1 to §4f.3, §4f.7, §4f.9, §4f.11](../decisions/dag-tools-s4g1-s4g-s4f.md) |
| 8 | Probe what cannot run | For classes that need one, a probe is generated whose source always parses. The probe launches the upstream and the downstream asset in one run, so the real input/output manager writes and reads back. A missing probe location is reported as absent, not as an empty one. | probe generator; probe runner; probe location | `built` | `test_generated_source_parses_as_python`; `test_run_probes_side_launches_against_dag_tools_probes_location`; `test_run_probes_side_skips_passed_probes_on_re_invocation`; `test_build_report_location_absent` | [§4g1.1](../decisions/dag-tools-s4g1-s4g-s4f.md), [§4g0.2, §4g0.5, §4g-1.2](../decisions/dag-tools-s4i-s4h-s4g.md); [Dagster](../components/dagster.md), finding 10 |
| 9 | Check the round trip | For a class that runs, a probe loads what the run wrote, for the partition that run used, and writes nothing. If the output was materialized again since, the probe skips rather than loading the wrong data. | round-trip probe | `built` | `test_roundtrip_probe_returns_materialize_result_and_writes_nothing`; `test_rematerialized_upstream_skips_instead_of_loading_stale_output` | [§4g2.2, §4g2.3](../decisions/dag-tools-s4i-s4h-s4g.md) |
| 10 | Decide | The two sides are compared. Metadata parity compares the set of keys, not their values, and duration differences never gate. A class is covered only when its probes pass on both sides and the records agree. A red probe or round trip blocks with no opt-out. Each other gap blocks until accepted by name, and a failed candidate preflight blocks with no override. | verdict | `built` | `test_diff_compares_metadata_KEY_set_not_values`; `test_verdict_matching_probe_records_count_as_covered`; `test_verdict_diverged_probes_blocks_go_even_with_passing_status`; `test_one_side_failed_is_red_even_with_accept_flag`; `test_missing_roundtrip_blocks_go_without_accept_flag`; `test_verdict_no_go_by_default_when_orchestration_not_accepted`; `test_verdict_no_go_when_candidate_preflight_failed` | [§4h.1 to §4h.4, §4g0a.1, §4g0a.2, §4g2.5](../decisions/dag-tools-s4i-s4h-s4g.md) |

## Roles in the NATO frame

| Element | Mesh component | What it does here | Status | Evidence | Gap |
|---|---|---|---|---|---|
| Infrastructure plane (Appendix 14, ¶2 to ¶4) | Dagster; the image builds and their digest records | Runs the pipelines being qualified. The digest records let preflight say which bytes each location runs. | `built` | steps 3 and 6 above; [Dagster](../components/dagster.md) | The supported Dagster floor is tested in a job that does not block a merge ([Dagster](../components/dagster.md), finding 7) |
| Operational data plane (Appendix 14, ¶5 to ¶7) | the producers' code locations, through the survey, runs, probes and round trip | Shows that each producer's pipelines load, run, and read and write their data the same way on both versions | `built` | steps 1, 5 and 7 to 10 above | Staging data for a run is the operator's to provide (step 7) |

## Gaps

- **No recorded qualification.** Every step is tested in isolation, and no qualification against a running deployment is recorded, so none of this is `measured`. The tests that could make it so are listed in [the built-to-measured list](../built-to-measured.md).
- **The probe location is deployed by neither chart.** A deployment must add it, or every class that needs a probe stays uncovered ([Dagster](../components/dagster.md), finding 10).
- **Staging is the operator's.** The tooling reports which classes need upstream data and resolves the operator's staging pointer. It does not produce staging data on its own (§4e.5, §4f.11).
- **Metadata values are not compared.** Parity is on keys only, except for the two round-trip shape keys (§4h.1, §4g2.5). A change in a metadata value passes.

## Not established from code

- Whether any qualification has been run on a deployment, and with what verdict.
- Which deployments run the probe location, and with which probe configuration.
- Whether the downstream repositories surveyed tag their assets for runnability as the class matrix expects.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Survey publishes nothing for code that does not load | dag-tools decision §4c.1 | [§4c.1](../decisions/dag-tools-s4e-s4a-s7.md) | `test_run_survey_refuses_to_publish_when_load_fails` | `built` |
| Write-once artifacts; pointer last | dag-tools decisions §4b.2, §4b.3 | [§4b.2](../decisions/dag-tools-s4e-s4a-s7.md) | as step 2 | `built` |
| Build once, rename on release; image digest records | dag-tools decisions §8.1, §8.6 to §8.9 | [§8.1](../decisions/dag-tools-s4o2-s8.md) | as step 3 | `built` |
| Manifest frozen and written once | dag-tools decisions §4d.1 to §4d.3, §4e.1 | [§4d.1](../decisions/dag-tools-s4e-s4a-s7.md) | as step 4 | `built` |
| Classes, runnability and staging need | dag-tools decisions §4e.4, §4e.5 | [§4e.4](../decisions/dag-tools-s4e-s4a-s7.md) | as step 5 | `built` |
| Preflight | dag-tools decisions §4g.1 to §4g.4 | [§4g.1](../decisions/dag-tools-s4g1-s4g-s4f.md) | as step 6 | `built` |
| Staging and runs | dag-tools decisions §4f.1 to §4f.3, §4f.7, §4f.9, §4f.11 | [§4f.1](../decisions/dag-tools-s4g1-s4g-s4f.md) | as step 7 | `built` |
| Probes and the probe location | dag-tools decisions §4g1.1, §4g0.2, §4g0.5, §4g-1.2; [Dagster](../components/dagster.md), finding 10 | [§4g0.2](../decisions/dag-tools-s4i-s4h-s4g.md) | as step 8 | `built` |
| Round trip | dag-tools decisions §4g2.2, §4g2.3, §4g2.5 | [§4g2.2](../decisions/dag-tools-s4i-s4h-s4g.md) | as step 9 | `built` |
| Verdict | dag-tools decisions §4h.1 to §4h.4, §4g0a.1, §4g0a.2, §4g2.5 | [§4h.1](../decisions/dag-tools-s4i-s4h-s4g.md) | as step 10 | `built` |
| No qualification is `measured` | the status columns of the records cited | — | none found | — |
