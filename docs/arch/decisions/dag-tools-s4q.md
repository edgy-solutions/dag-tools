# dag-tools decisions: §4q

This record covers the decisions in `AGENTS.md` section 4q at the dag-tools pin (`503607d`): the SystemLink source kind. The source is described in the [SystemLink source record](../components/systemlink-source.md), and the seam it plugs into in the [dlt pipeline record](../components/dlt-pipeline.md) and [the §4p decisions](dag-tools-s4p.md). [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §4q The SystemLink source kind

The kind has two parts. The engine, under `dag_tools/systemlink/`, imports no Dagster, dlt or network library. The kind, `dag_tools/asset_wrappers/sources/systemlink.py`, is the dlt and Dagster surface over it.

### §4q.1 Read-only, and the key stays in the request header

- **Decision.** Only the three query routes are called, each a request that changes nothing on the server. The configuration names the environment variable that holds the API key, never the key. A missing variable fails the run, naming the variable. That check comes before the step that records the run, so no run row is written. The key reaches no table, no pipeline working directory and no log.
- **Why.** The source reads from a system it does not own, and a key written anywhere but the request header is a key someone else can read.
- **Status.** `built`. The leak scan has a control that plants a key and checks that the scan finds it.

### §4q.2 How the operator's name is stored is a required decision

- **Decision.** The `operator` block is required. Storing the operator as a pseudonym without naming the variable that holds the pseudonym key fails the load.
- **Why.** Any default would put a person's name somewhere nobody chose to put it.
- **Status.** `built`.

### §4q.3 Settings the server decides are settings, and a refusal names the setting

- **Decision.** Each value that depends on the server has a conservative default in the engine's settings module, and a deployment that has checked its own server overrides it in YAML. The value is never pinned into the library from one server. The settings are the page size, whether the server-side time filter is used, the steps key, the incremental ordering, the step filter form, and the default and maximum step batch sizes. An HTTP 400 raises an error naming the setting instead of retrying.
- **Why.** A value pinned from one server becomes every deployment's answer.
- **Status.** `built`.

### §4q.4 No bound on a setting is invented in code

- **Decision.** The page size is checked only for being positive. It used to carry an upper bound of one thousand.
- **Why.** The server declares no maximum and accepted a far larger page, so the bound made the load fail against a number the library invented. Positivity is a property of the request, not a reading of any server. The default is unchanged.
- **Status.** `built`.

### §4q.5 The step batch maximum defaults to the longest chain the census reports a server answering

- **Decision.** The maximum step batch defaults to 20, the longest or-chain the census says a server answered. A deployment that tests its own server raises it in YAML. The investigation script has an opt-in ladder that tries longer chains on the steps and products routes. Each rung asks for no rows, a rung wider than the result ids seeded for the test is skipped rather than padded, and the whole ladder is walked past a refusal so that a limit that varies shows.
- **Why.** The two routes share the batch setting and are not guaranteed to share a limit. The ladder is opt-in because it is the only probe whose cost grows with its length.
- **Where else.** No server result is recorded by this lane. The census states the reading of 20; this record carries it as the census's.
- **Status.** `built`.

### §4q.6 A 400 on a batch of several ids is diagnosed, never split

- **Decision.** When a steps request carrying several result ids is refused with a 400, the batch is sent once more with a single id. If that is accepted, the length was the cause, and the error names the two batch-size settings and the size that failed. If it is also refused, the form was the cause, and the error names the step filter setting. Both outcomes raise. A batch of one id is not probed. A probe that fails for any other reason leaves the original error standing. The probe does not count as a batch sent.
- **Why.** The error used to name the step filter whatever the cause, which sent an operator to change the filter form when the length was at fault. A timeout says the batch is too big, so a timed-out batch is halved; a 400 says nothing about size, and halving it would send about twice as many requests as ids and end in the same message. Counting the probe would inflate the largest batch that the batch-size check reads.
- **Status.** `built`.

### §4q.7 The sizing pass moves no row and names the flag it sends

- **Decision.** The backfill sizing request asks for no rows, only the total count. If it is refused, the error names the count flag, not the page size.
- **Why.** It used to ask for one row and discard it, which pulled one row of real data per part. It also named the page size, a setting that request never carries.
- **Status.** `built`.

### §4q.8 The terminal statuses are required, and an empty list has its own refusal

- **Decision.** The list of statuses that mark a result complete has no default, and the `filters` block that carries it is required. The refusal names the missing list, not just the block. An empty list is refused with a different message. The list of failure statuses is required only when steps are fetched for failures alone. The engine's settings module names no status value; the sample list lives in the example's own document.
- **Why.** The status vocabulary decides when a result is complete and its steps are fetched, so one server's spelling as a default would be every deployment's answer. A server that spelt a status differently would load cleanly and collect no steps. An empty list passes a required-field check, then every result reads as in progress, no steps are ever fetched, every result is read again each run, and nothing reports it. Requiring a list nobody reads teaches an operator to write one without meaning it.
- **Status.** `built`. The test that the settings module names no status has a control that checks the example still carries the sample list.

### §4q.9 Incremental reads compare known ids, not a cursor

- **Decision.** The ids and versions already stored are read back from the destination. A result is fetched again only when it is new or its terminal state changed. A result still running is stored without steps and gets them once it is terminal. A first run, with no table yet, is a first run, not an error.
- **Status.** `built`.

### §4q.10 Backfill is its own job and pipeline, resumable per seed part

- **Decision.** A job tag selects backfill mode. It runs under its own pipeline name, counts each part first, records completed parts in resource state and skips them next time. A restart tag reads every part again. Each asset key gets one materialization, not one per part.
- **Status.** `built`.

### §4q.11 The schedule targets the incremental job

- **Decision.** This kind's schedule targets the incremental job, and no job with the plain pipeline name exists for it. The fallback kind keeps that plain name. A table prefix is required for this kind and refused on the fallback kind, which would ignore it.
- **Status.** `built`.

### §4q.12 Every run is recorded, failed or not

- **Decision.** The run table gets one row per run: the status, the counts, the root error's type and a digest of its message, never the message. A second dlt run writes the row after the data run, and the original failure is then raised again. The row names the engine's own error from the cause chain, because dlt wraps what a resource raised. Three checks sit on the table: no authorization failure, no result that stays without steps beyond a threshold, and no step batch above the maximum.
- **Where else.** [§4p.16](dag-tools-s4p.md) records the same rule for the other kinds; this kind overrides the base run method and writes its own row.
- **Status.** `built`.

### §4q.13 The digest is of the error the row names, chosen by one function

- **Decision.** One function chooses the error that both the error-type column and the digest column describe. A separate helper digests an error the caller has already chosen.
- **Why.** The digest used to be taken from dlt's wrapper, whose message carries a per-run load id, so two runs of one failure digested differently and the column could not group repeats. Pointing it at the shared digest helper would have digested the innermost error, while the type column names the first engine error in the chain, so the two columns would describe different errors. The new helper is additive, so the other kinds are unchanged.
- **Where else.** The census accepts one boundary: a digest written before this change does not match one written after it for the same failure.
- **Status.** `built`.

### §4q.14 The steps' nested parameters are not a separate asset

- **Decision.** dlt unnests the step parameters into a child table of the steps table. The kind reports that child table as metadata on the steps materialization instead of making an asset key nothing selects. The census marks this as a deviation.
- **Status.** `built`.

### §4q.15 Tests inject DuckDB; the shipped code does not

- **Decision.** The destination factory knows five destinations and otherwise returns the bare string `filesystem`, so the component tests replace it with a DuckDB destination.
- **Where else.** The census says widening the factory is a separate change. No proposal for it is recorded.
- **Status.** `built`.

## Sources

All paths are at `503607dd321cd6627509bfc93955357b8566597f`. Test files are under `dag_tools_tests/`. `kind` is `dag_tools/asset_wrappers/sources/systemlink.py` and engine paths are under `dag_tools/systemlink/`. `comp` is `test_systemlink_component.py`, `eng` is `test_systemlink_engine.py` and `spike` is `test_systemlink_spike.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4q.1 read-only; key by name, header only | `queries.py:10`, `:33-90`; `client.py:103-112`, `:134-139`; kind `:367-371`, `:545`, `:565` | AGENTS.md §4q, bullet 1 | comp: `test_neither_secret_appears_in_the_pipeline_dir_any_table_or_any_log`, `test_control_the_leak_scan_finds_a_planted_key`, `test_the_key_is_sent_in_the_header_only`, `test_a_missing_key_variable_fails_the_run_naming_the_variable` | `built` |
| §4q.2 operator policy required | `spec.py:134-142`, `:159`; `incremental.py:66-78`; kind `:546-548` | bullet 2 | comp: `test_pseudonym_without_a_key_env_fails_the_load`, `test_the_operator_is_pseudonymised_before_it_reaches_any_table`, `test_control_the_name_policy_keeps_the_operator`, `test_the_drop_policy_leaves_no_operator_column` | `built` |
| §4q.3 server-decided settings; a 400 names the setting | `spec.py:15-23`, `:102-123`; `client.py:166-170` | bullet 3 | comp: `test_a_server_that_rejects_the_configured_spelling_fails_naming_the_setting`, `test_control_the_matching_spelling_settings_run_clean` | `built` |
| §4q.4 page size checked only for positivity | `spec.py:16`, `:103` | bullet 4 | eng: `test_a_page_size_above_a_thousand_is_a_deployments_decision_not_a_refusal`, `test_a_page_size_of_zero_is_still_refused` | `built` |
| §4q.5 batch maximum default; opt-in ladder | `spec.py:21-22`, `:104-105`, `:114-118`; `spike.py:30`, `:43`, `:466-504`, `:534` | bullet 5 | eng: `test_the_default_step_batch_max_is_the_conservative_reading`, `test_a_deployment_raises_the_chain_limit_from_its_own_ladder`; spike: `test_the_ladder_is_off_unless_it_is_asked_for`, `test_the_whole_ladder_is_walked_past_a_rejection_so_a_flaky_limit_shows`, `test_a_rung_wider_than_the_seeded_results_is_skipped_and_says_why`, `test_every_rung_asks_for_no_row_so_the_ladder_moves_no_data`, `test_the_ladder_without_a_part_number_exits_2_rather_than_being_ignored` | `built` |
| §4q.6 a 400 is diagnosed once, never split | `paging.py:70-110`, `:130-160` (batch counted at `:133`, timeout halved at `:148-153`, single id not probed at `:155-156`) | bullet 6 | eng: `test_a_400_on_a_long_chain_is_re_issued_once_with_one_id_and_names_the_length`, `test_a_400_for_one_id_too_names_the_form_not_the_length`, `test_a_400_is_never_split_the_way_a_timeout_is`, `test_a_single_id_batch_that_400s_is_not_probed_because_nothing_is_left_to_tell_apart`, `test_a_probe_that_answers_nothing_leaves_the_original_diagnosis_standing`, `test_the_probe_does_not_count_as_the_largest_batch_sent` | `built` |
| §4q.7 sizing pass asks for no row; names the count flag | `queries.py:51-59`; kind `:640-647` | bullet 7 | eng: `test_the_sizing_pass_asks_for_no_row_at_all`; comp: `test_a_refused_sizing_pass_names_the_count_flag_not_a_page_size`, `test_the_sizing_pass_moves_no_row` | `built` |
| §4q.8 terminal statuses required; empty refused separately | `spec.py:70-99`, `:154`, `:162-172` | bullet 8 | eng: `test_the_filters_block_is_required_because_terminal_statuses_has_no_default`, `test_an_empty_terminal_statuses_gets_ITS_OWN_refusal`, `test_failed_statuses_is_required_UNDER_failed_only_and_not_otherwise`, `test_the_spec_names_no_status_value`, `test_control_the_examples_own_document_carries_the_sample_list`; comp: `test_an_absent_filters_block_fails_the_LOAD_naming_terminal_statuses`, `test_an_empty_terminal_statuses_fails_the_LOAD_too` | `built` |
| §4q.9 known ids, not a cursor | `incremental.py:36-47`, `:84-139`; kind `:348-364`, `:601` | bullet 9 | comp: `test_the_first_run_stores_everything_and_the_second_stores_nothing_new`, `test_a_result_whose_version_moved_is_stored_once_and_its_steps_refetched`, `test_a_running_result_is_stored_without_steps_and_gets_them_once_terminal` | `built` |
| §4q.10 backfill job, resumable per part | kind `:45-46`, `:488-491`, `:542-543`, `:570`, `:624-678` | bullet 10 | comp: `test_a_multi_part_backfill_yields_one_materialization_per_key`, `test_a_repeated_backfill_resumes_past_the_parts_already_done`, `test_control_the_restart_tag_reads_every_part_again`, `test_a_backfill_interrupted_after_one_part_resumes_with_the_other`, `test_the_incremental_job_is_never_a_backfill` | `built` |
| §4q.11 schedule targets incremental; prefix rules | kind `:398-406`, `:473-474`, `:488`; `dag_tools/asset_wrappers/source_kinds.py:103-106` | bullet 11 | comp: `test_the_schedule_targets_incremental_and_no_name_job_exists`, `test_the_legacy_schedule_still_targets_name_job`, `test_a_legacy_kind_with_table_prefix_fails_the_load`, `test_the_legacy_body_still_issues_one_run_with_the_same_kwargs` | `built` |
| §4q.12 every run recorded; three checks | kind `:498-534`, `:565-599`, `:680-712` | bullet 12 | comp: `test_the_ingest_run_row_counts_the_run`, `test_a_refused_key_fails_the_run_and_is_recorded_without_the_key`, `test_the_asset_checks_pass_on_a_clean_run`, `test_the_not_visible_check_fails_when_a_result_has_no_steps` | `built` |
| §4q.13 one function chooses the recorded error | kind `:26`, `:374-390`, `:686`; `dag_tools/asset_wrappers/ingest_runs.py:41-77` | bullet 13 | comp: `test_two_runs_of_one_failure_carry_the_SAME_digest`, `test_the_digest_is_of_the_error_the_row_NAMES` | `built` |
| §4q.14 child table reported, not an asset | kind `:611-622` | bullet 14 | comp: `test_the_steps_child_table_is_reported_not_made_an_asset` | `built` |
| §4q.15 tests inject DuckDB | `dag_tools/asset_wrappers/dlt_assets_parsing.py:85-125`; `dag_tools_tests/_systemlink_helpers.py:30-34` | bullet 15 | the helper is used by the comp tests; no test of its own | `built` |
