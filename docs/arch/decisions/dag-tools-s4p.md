# dag-tools decisions: §4p

This record covers the decisions in `AGENTS.md` section 4p at the component-records pin (`add5bc4`): dlt source kinds and per-pipeline schedules. The component is described in the [dlt pipeline record](../components/dlt-pipeline.md). [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §4p dlt source kinds and per-pipeline schedules

### §4p.1 Only an exact registered type selects a kind

- **Decision.** A source kind is chosen only when the source's `type` setting names a registered kind exactly. Any other value falls back to the original dispatch by driver name. An absent `type`, and the three spellings that the repository's own YAML already uses, resolve to that fallback silently. Any other unregistered value also falls back, with one warning per value per process.
- **Why.** The `type` setting was never read before source kinds existed, so existing YAML may hold anything there. Failing the load on an unknown value would break working configuration (census §6).
- **Status.** `built`.

### §4p.2 A near miss of a registered kind is refused

- **Decision.** A value that closely resembles a registered kind, other than the fallback spellings, fails the load and names the kind it resembles.
- **Why.** A misspelt kind would otherwise take the SQL path and fail later on a missing credential, naming the wrong problem. The fallback spellings are never candidates, so no existing YAML newly fails.
- **Status.** `built`.

### §4p.3 The fallback kind delegates to the existing functions

- **Decision.** The fallback kind calls the existing source-building and asset-building functions. Those functions stay where they are, with their signatures.
- **Why.** Downstream repositories import them.
- **Status.** `built`.

### §4p.4 A kind's validation runs outside the loop that swallows failures

- **Decision.** A kind's own validation runs before the loop that builds each pipeline's assets.
- **Why.** That loop prints an instantiation failure and carries on. A refusal raised inside it would load as a pipeline with no assets.
- **Status.** `built`. The loop still swallows every other instantiation failure ([dlt pipeline record](../components/dlt-pipeline.md)).

### §4p.5 The deployed asset and the development loop build the source the same way

- **Decision.** Each unit of work carries its kind, and both the asset builder and the development runner build the source through that kind.
- **Why.** The development loop then runs the same function as the deployed asset.
- **Status.** `built`.

### §4p.6 The development loader keeps an absolute path absolute

- **Decision.** When a development run names a DuckDB file by URL, the loader drops the leading slash only before a drive letter. Otherwise the slash starts an absolute path and is kept.
- **Why.** Stripping the slash in every case turned an absolute path, including the spelling the loader's own error message suggests, into a relative one. On a POSIX system the run then wrote a file in the current directory and looked like a success. Windows could not show the fault, because there the strip is correct. The census says CI found it, and that the only earlier test asserted only that a destination was returned.
- **Where else.** Path resolution returns a string, so the tests assert on the string rather than building a destination. Census §4a (OTel) makes the same split.
- **Status.** `built`.

### §4p.7 The schedule block is removed before the settings model is built

- **Decision.** The component removes `schedule` from each pipeline's settings before it builds the pipeline settings model.
- **Why.** That model ignores unknown keys, so a schedule left in place would load with no schedule and no message. The schedule block's own model refuses unknown keys, so a malformed block fails the load.
- **Status.** `built`.

### §4p.8 The schedule is built in Python, starts stopped and selects its own assets

- **Decision.** The schedule is built in Python, not attached through the component file's post-processing. It starts stopped unless the block enables it. Its job selects exactly that pipeline's assets by key. A schedule on a pipeline that produced no assets fails the load. No time zone is assumed.
- **Why.** The oldest supported Dagster release refuses a post-processing key and fails the whole load (census §4a, OTel). A selection of every asset would not include external source assets and would pass with nothing run (census §4m). A job over no assets would also pass with nothing run.
- **Status.** `built`.

### §4p.9 A tick is skipped while a run of its job is queued or in progress

- **Decision.** The schedule's tick looks for a run of its own job that is queued, not started or in progress, and skips if one is found.
- **Why.** This works whatever concurrency limits the Dagster instance has, which the library cannot see.
- **Where else.** The filter is by job name, so two code locations that each define a pipeline with the same name would hold each other's ticks. The census says to name pipelines distinctly. The tests need a temporary local instance, because an in-memory one cannot be reached from a schedule's context.
- **Status.** `built`.

### §4p.10 Table settings are per table

- **Decision.** `table_config` sets the key and the cursor for each table, not once for a pipeline. Its model is shared by the two components that read it. It is removed from the settings before the settings model is built. An entry naming a table outside the pipeline's sources fails the load. An explicit hint still wins. A table with no entry loads as before.
- **Why.** One pipeline usually reads tables with different keys and cursors. A second copy of the model would drift from the first. The settings model ignores unknown keys, as in §4p.7.
- **Status.** `built`.

### §4p.11 Kept history uses the key as a merge key

- **Decision.** A table with `history: scd2` sends its key to dlt as a merge key, never as a primary key. Declaring both history and a cursor fails the load, naming both fields. Only the strategy is sent; no validity or hash column is named. A table without history loads as before.
- **Why.** With no merge key, dlt closes every current row that a load does not carry. A filtered extraction, a partial read or a narrowed row filter would then retire rows that never changed, and the table would look like a correct record of a mass change. A cursor means a load carries only what moved, which is exactly the case that rule cannot be applied to. The column names are dlt's, and a deployment may set them.
- **Status.** `built`.

### §4p.12 Row filters are structured data on one shared grammar

- **Decision.** `row_filters` is extraction configuration in YAML with six operators. Not-equal and not-in also exclude nulls. The operators, shapes and literal rules come from the contract mapping's filter check, which a mapping's own `where` also uses. Values are bound as parameters. Floats, mixed lists, nulls and booleans are refused, as in a mapping.
- **Why.** A filter then means one thing in this repository. The excluded null makes a predicate read the same here as in a mapping. Binding a value does not remove the float-equality and coercion questions, so the same values are refused.
- **Where else.** The gateway and the data client carry a field with the same name. That one is a broker's per-read answer, written as SQL text. The two are not unified and do not import each other, and this one must never accept an expression string.
- **Status.** `built`.

### §4p.13 An unknown filter column fails the run, never silently

- **Decision.** A filter column is checked at extraction time, not at load. A column the table does not have fails the run, naming the table and the column.
- **Why.** The check needs a live connection, and a code location must not fail to load because a warehouse is down. A filter matching no column would otherwise read as a correct extraction of an empty table.
- **Status.** `built`.

### §4p.14 A row-count check covers tables with no cursor, and a missing count fails it

- **Decision.** Each table with a `table_config` entry and no cursor gets a check that the latest load recorded a row count. A missing count fails the check, and the message says no count was recorded rather than "zero rows". The check warns and never blocks. It reads the latest materialization's metadata and opens no connection. A table with no entry gets no check.
- **Why.** An incremental table may load zero rows; a table loaded by replacement that loads zero has emptied its destination. The Dagster dlt integration records the count only when it is non-zero, so a missing count cannot be told from zero, and claiming zero would send an operator to debug the source. The check runs where no database driver is installed. Adding checks to tables with no entry would change a deployment's asset graph as a side effect of a library upgrade.
- **Status.** `built`.

### §4p.15 A table is matched by the source's spelling and by dlt's

- **Decision.** The row-count check finds a table's asset by the name as written, then by the name dlt normalizes it to. A refusal names both spellings. The normalizer is taken from dlt; if dlt cannot be imported, only the exact match is tried. Two spellings of one table are refused.
- **Why.** Asset keys carry dlt's normalized names, which are lower-cased and snake-cased. A source table with an upper-case name therefore could not get a check, and the refusal named an asset key when the table name was correct. A deployment can configure another naming convention, so the normalizer is not reimplemented. Two checks with one name on one asset would be refused later by Dagster, less clearly.
- **Status.** `built`.

### §4p.16 With `ingest_runs` on, every run is recorded, failed or not

- **Decision.** With `ingest_runs: true`, a second dlt run writes one row per run to the pipeline's own dataset, after the data run. The row carries the root error's class name and a digest of its message, never the message. A failure of the data run is then raised again. If the row cannot be written after a failed run, the write error is logged and the original failure is raised. If it cannot be written after a successful run, the write error is raised. The setting is off by default, and then no table is created.
- **Why.** An exception's text often carries a connection string, a row of source data or a token, and the table is as readable as any other in the warehouse. A run that failed must still be reported as failing for its own reason: a record step may never replace the failure it records. After a successful run there is no other cause to hide, and a green run with no row would break the rule that every run is recorded. The recording is in the base kind's run method, because the SystemLink kind overrides that method and writes its own row (census §4q).
- **Status.** `built`.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Test files are under `dag_tools_tests/`. `wrappers` is `dag_tools/asset_wrappers/` and `component` is `dag_tools/components/dlt_pipeline/`. `kinds` is `test_dlt_source_kinds.py`, `dev` is `test_dlt_dev_loader.py`, `sched` is `test_dlt_pipeline_schedule.py`, `read` is `test_dlt_pipeline_read_config.py`, `scd2` is `test_dlt_pipeline_scd2.py` and `ingest` is `test_dlt_pipeline_ingest_runs.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4p.1 exact type; fallback; one warning | wrappers `source_kinds.py:23`, `:134-157` | AGENTS.md §4p, bullet 1 | kinds: `test_a_legacy_alias_is_legacy_and_silent`, `test_an_unknown_type_is_legacy_with_exactly_one_warning` | `built` |
| §4p.2 near miss refused | wrappers `source_kinds.py:141-148` | bullet 2 | kinds: `test_a_near_miss_of_a_registered_kind_fails_naming_it`, `test_a_near_miss_of_a_legacy_alias_does_not_fail` | `built` |
| §4p.3 fallback delegates | wrappers `source_kinds.py:108-122` | bullet 3 | kinds: `test_the_legacy_entry_points_keep_their_signatures` | `built` |
| §4p.4 validation outside the loop | wrappers `dlt_assets_parsing.py:575`, `:581`, `:590-603` | bullet 4 | kinds: `test_validate_raising_fails_create_dlt_assets_and_is_not_swallowed` | `built` |
| §4p.5 one source builder on both paths | wrappers `dlt_assets_parsing.py:326`, `:531-540`, `:596` | bullet 5 | kinds: `test_the_kinds_build_source_is_used_on_both_paths` | `built` |
| §4p.6 absolute path kept | `dag_tools/dlt_dev/loader.py:224`, `:241-245` | bullet 6 | dev: `test_a_posix_absolute_spec_resolves_to_an_absolute_path`, `test_the_spelling_the_error_message_offers_resolves_absolute`, `test_a_drive_letter_loses_the_url_separator` | `built` |
| §4p.7 schedule removed first | component `component.py:57`, `:94` | bullet 7 | sched: `test_no_schedule_means_no_schedule_and_no_job`, `test_with_a_schedule_there_is_exactly_one_job_and_one_schedule`; neither is named by the census | `built` |
| §4p.8 built in Python, stopped, own keys | component `schedule.py:27-76` | bullet 8 | sched: `test_the_schedule_is_stopped_by_default`, `test_each_job_selects_only_its_own_pipelines_keys`, `test_a_schedule_on_a_zero_asset_pipeline_fails_the_load_naming_it` | `built` |
| §4p.9 skip guard | component `schedule.py:19-24`, `:55-61` | bullet 9 | sched: `test_a_started_run_of_the_job_skips_the_tick`, `test_a_run_of_a_different_job_does_not_skip`, `test_a_finished_run_does_not_skip` | `built` |
| §4p.10 per-table settings | wrappers `dlt_assets_factory.py:43-87`, `:90-137`; component `component.py:61`, `:66-77` | bullet 10 | read: `test_an_unpopped_table_config_would_be_silently_ignored`, `test_a_table_config_naming_a_table_outside_sources_fails_the_load`, `test_an_explicit_hint_still_wins_over_table_config`, `test_a_table_with_no_entry_loads_exactly_as_before` | `built` |
| §4p.11 merge key under history | wrappers `dlt_assets_factory.py:78-86`, `:109-127` | bullet 11 | scd2: `test_a_key_absent_from_the_second_load_is_not_retired`, `test_the_primary_key_is_emitted_as_a_merge_key_under_scd2`, `test_scd2_with_a_cursor_fails_the_load_naming_both_fields`, `test_only_the_strategy_is_emitted_and_no_column_name_is_named`, `test_a_table_without_history_is_loaded_exactly_as_before` | `built` |
| §4p.12 one filter grammar; bound values | wrappers `row_filters.py:43`, `:58-62`, `:89-101`, `:133`; component `component.py:64` | bullet 12 | read: `test_values_are_bound_and_no_literal_is_formatted_into_sql`, `test_not_equal_and_not_in_exclude_nulls`, `test_every_operator_renders`, `test_an_unpopped_row_filters_would_be_silently_ignored`, `test_row_filters_on_a_filesystem_source_fail_the_load` | `built` |
| §4p.13 unknown column fails the run | wrappers `row_filters.py:121-131` | bullet 13 | read: `test_an_unknown_column_fails_the_run_naming_the_table_and_the_column`, `test_an_unknown_column_does_not_fail_the_load` | `built` |
| §4p.14 row-count check | component `row_count_check.py:36`, `:56-75`, `:122-127` | bullet 14 | read: `test_a_table_with_no_entry_at_all_gets_no_check_either`, `test_an_absent_rows_loaded_fails_the_check`, `test_the_failure_message_states_no_count_rather_than_zero`, `test_control_a_zero_row_load_omits_rows_loaded_entirely`, `test_the_check_opens_no_connection`, `test_the_check_is_not_blocking` | `built` |
| §4p.15 both spellings | component `row_count_check.py:40-51`, `:104-118` | bullet 15 | read: `test_an_uppercase_source_table_still_gets_its_row_count_check`, `test_a_table_matching_no_asset_key_names_BOTH_spellings`, `test_two_spellings_of_one_table_are_refused_rather_than_colliding` | `built` |
| §4p.16 every run recorded; which error is raised | wrappers `source_kinds.py:73-97`; `ingest_runs.py:41-77`, `:95-104`, `:129-150` | bullet 16 | ingest: `test_a_failed_run_is_recorded_and_the_failure_is_re_raised`, `test_the_row_names_the_root_error_not_the_dlt_wrapper`, `test_no_exception_message_reaches_the_row`, `test_control_the_scan_finds_a_planted_value`, `test_a_failure_writing_the_row_re_raises_the_original_failure`, `test_two_runs_of_one_root_failure_carry_the_SAME_digest`, `test_ingest_runs_is_off_by_default_and_no_table_is_created` | `built` |
