# dlt pipeline (CO-DLT-PIPELINE)

The dlt pipeline component turns a YAML block into Dagster assets that copy tables from a source into a destination with dlt. One instance holds one source config, one destination config and any number of named pipelines. It builds the assets for each pipeline, and optionally a schedule and row-count checks. A source kind is chosen by name, and the SQL path is the default.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It builds the dlt assets for each pipeline, an optional schedule and optional row-count checks, and no sensor of its own.** `build_defs` reads the source kind, builds the assets and returns them with the shared dlt resource. A kind may contribute sensors, and none at the pin does. Row-count checks attach only to tables with a `table_config` entry and no cursor. Each asset carries catalog metadata naming its source when the source platform is known, which the separate lineage component reads; this component makes no catalog call. `built`
2. **`source_config` and `dest_config` are required, and everything else is optional.** `staging_config` defaults to none and `pipelines` to an empty mapping. The component carries no default source, destination or credential. `built`
3. **Only an exact registered `type` selects a source kind; anything else is the legacy SQL dispatch.** Three legacy spellings resolve silently. Any other unregistered value resolves to legacy with one warning per value per process. A near miss of a registered non-legacy kind fails the load and names it. The legacy kind delegates to the existing functions, so their signatures hold. The [SystemLink source](systemlink-source.md) plugs into this seam. `built`
4. **Credentials come from the config, with environment references resolved when the units are prepared.** `{env: NAME}` references inside the config are replaced from the environment. Credentials are built from explicit parts or a connection string. Under `dagster dev` only, the destination variables a build derived are written to a dump file by name, with every value redacted; real values are written only to a path named by `DAGTOOLS_DLT_ENV_FILE`. The component does not reject a source or destination that carries no credential. `built`
5. **The destination is built by one function that knows five kinds and otherwise returns the bare name `filesystem`.** The five are a Postgres family, a Snowflake family, a filesystem, a ClickHouse family and a Databricks family. If building the named destination raises, or the driver name is none of these, it prints a message and returns `filesystem`. `built`
6. **It connects directly to the source and the destination. It does not use the central gateway, the domain broker or a ticket.** The source connection is made with the credential in config; a SQL Server source goes through a change-tracking reader. No ticket is requested and no identity is presented to the mesh. `built`
7. **Bad config is refused at load, and an unknown filter column at the run.** At load it refuses a near-miss kind, a `table_config` entry for a table not in `sources`, `history` together with a cursor, `row_filters` on a filesystem source, `table_prefix` on the legacy kind, and a schedule on a pipeline with no assets. A kind's own validation runs outside the loop that swallows instantiation failures (gap 1), so its refusal is not lost. A filter naming an unknown column fails the run, because the column is resolved against the live table. `built`
8. **`table_config` sets key and cursor per table, and `history: scd2` emits the key as a merge key.** Without a merge key, dlt would close every active row a load does not carry, so a partial read would retire history that never changed. A table with no entry loads as before. An explicit hint wins over `table_config`. The block is popped before the attribute model is built, since that model would otherwise ignore it silently. `built`
9. **`row_filters` is structured and bound, and is not the gateway's field of the same name.** Six operators are allowed: equal, not equal, in, not in, is null and not null. Values are bound as parameters through the table's columns, and not-equal and not-in carry a null guard. An expression string is not accepted. `built`
10. **The schedule is built in Python, starts stopped, selects explicit keys, and skips a tick while a run is open.** The `schedule` block is popped before the group model is built, and the block forbids extra keys. It starts stopped unless `enabled` is true. No time zone is invented. The job selects exactly that pipeline's asset keys. A tick skips while a run of the job is queued or in progress. `built`
11. **The row-count check is non-blocking, reads recorded metadata, and fails on an absent count.** It reads the latest materialization's `rows_loaded` at warning severity and opens no connection. A zero-row load carries no such key, so absence fails, and the message says no count was recorded. A table with no `table_config` entry gets no check. The source's table spelling is matched first, then dlt's normalized spelling. `built`
12. **`ingest_runs` is off by default; when on, it records each run with the root error's type and a digest, never the message.** A second dlt run writes the row after the data run, and the original failure is then re-raised. If writing the row fails after a failed data run, the original failure is the one raised. When off, no table is created. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `source_config` | required | the source kind (`type`), its connection parts or string, and the `sources` list of tables |
| `dest_config` | required | the destination kind and its credential |
| `staging_config` | none | an optional staging destination |
| `pipelines` | empty | named pipeline groups, each with its own settings below |
| `table_config` | none | per table: key, cursor, `history` |
| `row_filters` | none | structured filters on a SQL source, bound as parameters |
| `table_prefix` | none | table name prefix; refused on the legacy kind |
| `schedule.cron` | required when `schedule` is set | cron expression for the pipeline's job |
| `schedule.enabled` | `false` | starts the schedule running; otherwise it starts stopped |
| `schedule.timezone` | none | time zone; none is invented |
| `ingest_runs` | `false` | writes one row per run to the pipeline's own dataset |
| `DAGTOOLS_DLT_ENV_FILE` (environment) | unset | under `dagster dev`, the path that receives real destination values instead of the redacted dump |

## Gaps

- **One unit's instantiation failure is printed and skipped** (finding 7). Failures other than a kind's own validation do not stop the load, so a pipeline can load with fewer assets, or none, and the only sign is a printed line. Remedy: not on the roadmap.
- **An unknown or failing destination falls back to a bare `filesystem` name with a printed message** (finding 5). Where that destination then writes depends on dlt's own configuration, which was not read. No test covers the fallback. Remedy: not on the roadmap.

## Not established from code

- Which destinations a deployment uses, and which credential each holds.
- Whether the source accounts are read-only. The component sets no such limit.
- Where dlt writes when the destination falls back to the bare `filesystem` name (gap 2).
- What the lineage component does with the catalog metadata; that is a separate record.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | copies source tables into a destination as Dagster assets, with an optional schedule, row-count checks and a run table | — |
| How the mesh wires it | a YAML component in a code location, sharing one dlt resource with the other dlt-based components | — |
| Sign-in | to the source and the destination, with a credential from its config or environment; nothing is presented to the mesh | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct from the source to the destination; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | operational, where producers' tables are read out | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none for mesh reads; the redacted dump and the message-free run row limit what leaks, which is hygiene, not access control | — |
| Services served | moves producer data into a store a consumer reads; the run row records each run | [DoD S7](../alignment/dod-dmra.md), [S13](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/dlt_pipeline/`; other paths under `dag_tools/asset_wrappers/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Assets, optional schedule and checks; no sensor of its own; catalog metadata | `component.py:41-142`, `:45-47`, `:112-133`; `dlt_assets_factory.py:614` | AGENTS.md §4p | no test of the absence of sensors | `built` |
| Required config | `component.py:29-38` | AGENTS.md §5 | no test | `built` |
| Source kinds | `source_kinds.py:23`, `:25`, `:108-122`, `:134-157` | AGENTS.md §4p | `test_dlt_source_kinds.py`: `test_a_legacy_alias_is_legacy_and_silent`, `test_an_unknown_type_is_legacy_with_exactly_one_warning`, `test_a_near_miss_of_a_registered_kind_fails_naming_it` | `built` |
| Credentials; environment references; redacted dump | `dlt_assets_factory.py:309-383`, `:619-668`; `dlt_assets_parsing.py:423-425`; `dag_tools/utils/env.py:32-63` | AGENTS.md §5 | `test_dlt_env_dump_is_redacted.py`: `test_the_default_dump_does_not_contain_the_secret`, `test_real_values_require_naming_a_path_not_flipping_a_flag`, `test_nothing_is_written_outside_dagster_dev` | `built` |
| Destinations and the fallback | `dlt_assets_parsing.py:85-124` | not stated | no test of the fallback | `built` |
| Direct connection | `dlt_assets_parsing.py:240`, `:308`; `sources/sql_ct_database.py:58-73` | not stated | `test_sql_ct_database_config.py` (mocked engine); no test of absence | `built` |
| Load and run refusals | `component.py:66-77`; `dlt_assets_factory.py:109-117`; `dlt_assets_parsing.py:207-214`, `:576-578`; `row_filters.py:126-132` | AGENTS.md §4p | `test_dlt_pipeline_read_config.py`: `test_a_table_config_naming_a_table_outside_sources_fails_the_load`, `test_row_filters_on_a_filesystem_source_fail_the_load`, `test_an_unknown_column_fails_the_run_naming_the_table_and_the_column`; `test_dlt_pipeline_scd2.py`: `test_scd2_with_a_cursor_fails_the_load_naming_both_fields`; `test_systemlink_component.py`: `test_a_legacy_kind_with_table_prefix_fails_the_load`; `test_dlt_source_kinds.py`: `test_validate_raising_fails_create_dlt_assets_and_is_not_swallowed` | `built` |
| `table_config` and scd2 | `dlt_assets_factory.py:43-87`, `:90-137`, `:123-127` | AGENTS.md §4p | `test_dlt_pipeline_read_config.py`: `test_an_unpopped_table_config_would_be_silently_ignored`, `test_an_explicit_hint_still_wins_over_table_config`; `test_dlt_pipeline_scd2.py`: `test_a_key_absent_from_the_second_load_is_not_retired` | `built` |
| `row_filters` | `row_filters.py:45-136` | AGENTS.md §4p | `test_dlt_pipeline_read_config.py`: `test_values_are_bound_and_no_literal_is_formatted_into_sql`, `test_not_equal_and_not_in_exclude_nulls` | `built` |
| Schedule | `schedule.py:19-76`; `schema.py:5-10` | AGENTS.md §4p | `test_dlt_pipeline_schedule.py`: `test_the_schedule_is_stopped_by_default`, `test_each_job_selects_only_its_own_pipelines_keys`, `test_a_schedule_on_a_zero_asset_pipeline_fails_the_load_naming_it`, `test_a_started_run_of_the_job_skips_the_tick` | `built` |
| Row-count check | `row_count_check.py:36-133` | AGENTS.md §4p | `test_dlt_pipeline_read_config.py`: `test_an_absent_rows_loaded_fails_the_check`, `test_the_check_opens_no_connection`, `test_a_table_with_no_entry_at_all_gets_no_check_either`, `test_an_uppercase_source_table_still_gets_its_row_count_check` | `built` |
| `ingest_runs` | `source_kinds.py:73-97`; `ingest_runs.py:34-150`; `dlt_assets_factory.py:194` | AGENTS.md §4p | `test_dlt_pipeline_ingest_runs.py`: `test_a_failed_run_is_recorded_and_the_failure_is_re_raised`, `test_no_exception_message_reaches_the_row`, `test_a_failure_writing_the_row_re_raises_the_original_failure`, `test_ingest_runs_is_off_by_default_and_no_table_is_created` | `built` |
| Instantiation failure swallowed | `dlt_assets_parsing.py:590-603` | AGENTS.md §4p | `test_validate_raising_fails_create_dlt_assets_and_is_not_swallowed` covers only the kind's own validation | `built` |
