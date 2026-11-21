# SystemLink source (CO-SYSTEMLINK-SOURCE)

The SystemLink source reads test results, test steps and, optionally, products from a SystemLink Test Monitor server and loads them into a warehouse as raw tables. It is a source kind on the dlt pipeline component's seam, not a component class of its own. A deployment selects it by setting `source_config.type` to `systemlink_test_monitor` inside a dlt pipeline component. The kind then builds the assets, jobs and checks below. A pure engine under `dag_tools/systemlink/` does the requests, paging and incremental logic with no Dagster or dlt import.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It is a registered source kind, not a component.** The kind is registered under the name `systemlink_test_monitor`, and the dlt pipeline component dispatches to it when `source_config.type` matches exactly. The kind supplies its own load checks, assets, jobs, asset checks and run body. The shared kind seam and its dispatch rules are in AGENTS.md §4p. `built`
2. **It is read-only: three query routes, all POST.** The client accepts only `query-results`, `query-steps` and `query-products`, and raises on any other route before a request is made. It has no method that sends anything else. Filter values travel in a substitutions list and never inside the filter text. `built`
3. **The API key comes from an environment variable and goes only in a request header.** The setting `api_key_env` names the variable. At run start the kind reads it, and fails the run naming the variable if it is unset. The key is held in the HTTP client's headers only. The client's text form hides it, and the request log carries a hash of the body, not the body or any header. A test scans the pipeline directory, every table and every log for the key. The definition-time source reads no variable, so a code location loads with no key and no network. `built`
4. **It signs in to the SystemLink server with that key and nothing else, and goes direct.** The gateway and broker are not on the path, so no ticket is issued. A 401 or 403 raises an authentication error and is counted as an event. The warehouse side uses the pipeline's `dest_config`, as for any dlt pipeline. `built`
5. **Where a person's name goes has to be a stated decision.** The `operator` block is required. Its `store` is `pseudonym`, `name` or `drop`. The pseudonym form needs `pseudonym_key_env`, and the stored value is a keyed hash made before the row reaches dlt. A pseudonym policy with no key variable name fails the load. A named variable that is unset fails the run. `built`
6. **Every setting the server decides is a YAML setting with a conservative default, and a 400 names the setting.** Page size, step-filter form, step batch default and maximum, sort-field spelling, the steps key and the server-time filter all sit in one `DEFAULTS` table. A 400 raises an error that names the setting most likely at fault, and is never read as an empty page. On a multi-id steps batch, the batch is re-sent once with one id to tell a too-long chain from a refused filter form. The reasons are in AGENTS.md §4q. `built`
7. **The terminal statuses are required, and an empty list is refused on its own.** The `filters` block is required because `filters.terminal_statuses` has no default. Leaving the block out and giving an empty list each fail the load with their own message. `failed_statuses` is required only under `step_detail_for: failed_only`. No status value appears in the spec. `built`
8. **Incremental runs work from the ids already stored, and a backfill is its own job.** Each run reads back the stored id, update time and status from the results table. A first run with no table is not an error. A result is stored when it is new or its terminal state changed, and a still-running result is stored without steps. A scan stops after a set number of pages in a row where nothing changed. The backfill counts each part first, loads one part at a time, records finished parts in resource state and skips them next time. A job tag forces a full re-read. `built`
9. **Two jobs and one schedule; the schedule is stopped by default.** The kind defines `<name>_incremental` and `<name>_backfill`, both over the pipeline's own asset keys. It defines no `<name>_job`. The pipeline's `schedule:` block targets the incremental job and starts stopped unless `enabled: true`. The backfill job is chosen by the tag `dagtools/systemlink_mode=backfill`. `built`
10. **Every run writes one row to `<prefix>ingest_runs`, and three checks read it.** A second dlt run appends the row after the data run, whether the data run failed or not, and then the original failure is re-raised. If writing the row fails after a failed data run, the original failure is the one raised. If it fails after a successful data run, that failure is raised and the run fails. The row holds status, counts, the engine's own error class and a digest of the message, never the message. The checks are `no_auth_failed`, `not_visible_within_threshold` and `step_batch_within_max`. With no ingest run recorded yet, each check fails at warning severity. `built`
11. **`table_prefix` and `dest_schema` are required on every pipeline using this kind.** A forced `pipeline_kwargs.table_name` is refused, because it would give every resource one name. The legacy SQL kind refuses `table_prefix`, which it would ignore. A separate shipped dbt package, `dag_tools/dbt_project_packages/mfg_trace_curated`, derives curated models from conformed documents a deployment produces. It is not part of this kind (AGENTS.md §4r). `built`

## Settings and defaults

Source attributes are under `source_config`. `table_prefix`, `dest_schema` and `schedule` are on the pipeline.

| Setting | Default | What it does |
|---|---|---|
| `type` | required | must be `systemlink_test_monitor` |
| `source_id` | required | names the source; becomes part of the upstream asset key and a column on every row |
| `connection.base_url` | required | the server address |
| `connection.api_key_env` | required | name of the environment variable that holds the key |
| `connection.api_version_path` | a default path | prefix before the three routes |
| `connection.workspace_id` | none | scopes the result and step queries to one workspace when set; the product query is not scoped |
| `connection.verify_tls` | `true` | whether the client checks the server certificate |
| `seed.kind` and its fields | `kind` required | where the part numbers come from: a file, an inline list or a ClickHouse query; the fields that kind needs are required and stray ones are refused |
| `filters.terminal_statuses` | required, no default | statuses this server calls terminal; empty is refused |
| `filters.step_detail_for` | `all_terminal` | which results get steps; `failed_only` needs `failed_statuses` |
| `filters.failed_statuses` | empty | used only under `failed_only` |
| `filters.program_include` / `program_exclude` | empty | keep or drop results by program name |
| `limits.page_size` | `100`, at least 1 | rows per page |
| `limits.step_batch_default` / `step_batch_max` | `15` / `20` | ids per steps query; the default may not exceed the maximum |
| `limits.step_filter` | `or_chain` | the filter form for steps; the other is `in` |
| `limits.max_concurrency` | `1`, at most 4 | requests in flight |
| `limits.request_timeout_s` / `max_retries` | `30` / `5` | timeout and retry count |
| `limits.not_visible_warn_threshold` | `0` | the limit the `not_visible_within_threshold` check uses |
| `incremental.order_by` | `UPDATED_AT` | sort-field spelling; the other is `updatedAt` |
| `incremental.lookback_pages` | `3` | quiet pages in a row before a scan stops |
| `incremental.use_server_time_filter` | `false` | sends an update-time filter when on |
| `incremental.strategy` | `known_ids` | the only strategy accepted |
| `steps_key` | a two-field key | the key for steps; the only other allowed value is one field |
| `keep_json_columns` | four named columns | columns kept as JSON |
| `operator.store` / `pseudonym_key_env` | block required; `store` defaults to `pseudonym`, which needs the key variable name | where a person's name goes |
| `products.enabled` | `false` | adds the products table |
| `table_prefix`, `dest_schema` (pipeline) | required | table names and destination schema |
| `schedule` (pipeline) | none; stopped unless `enabled: true` | the schedule for the incremental job |

## Gaps

- **A read goes straight to the SystemLink server and not through the mesh** (findings 3 and 4). No ticket is issued, and the central gateway and domain broker narrow nothing. Remedy: not on the roadmap.

## Not established from code

- Which server versions, status vocabularies or limits any deployment has. The code ships conservative defaults and no measured values.
- Which destination a deployment loads into, and with which credentials.
- Whether a deployment turns the schedule on, or attaches other automation to the jobs.
- Whether the operator probe `dag_tools/systemlink/spike.py` matches any server. No run against a real server was read.
- Whether anything publishes these tables to the catalog. The kind declares one upstream asset spec and no catalog call was found in the kind or the engine (`systemlink.py:446`, `:466`).

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-DLT-PIPELINE | is called by | in-process | none | the source kind dispatch | `dag_tools/asset_wrappers/source_kinds.py:28, :129`; no test | `built` |
| XE-SYSTEMLINK-API | calls | HTTP | API key in a request header | three read-only POST query routes | `dag_tools/systemlink/client.py:130-140`; `test_the_key_is_sent_in_the_header_only` | `built` |
| XE-SQL-WAREHOUSE | reads | SQL through the dlt client | not established | stored ids and versions | `dag_tools/asset_wrappers/sources/systemlink.py:348-364`; `test_the_first_run_stores_everything_and_the_second_stores_nothing_new` | `built` |
| XE-SQL-WAREHOUSE | writes | dlt destination connection | not established | results, steps, products and a run row | `dag_tools/asset_wrappers/sources/systemlink.py:587-599`; `test_the_ingest_run_row_counts_the_run` | `built` |
| CO-DAGSTER | is called by | in-process | none | jobs and asset checks | `dag_tools/asset_wrappers/sources/systemlink.py:473-474, :488-491`; `test_the_schedule_targets_incremental_and_no_name_job_exists` | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | loads raw test results, steps and products from SystemLink Test Monitor into a warehouse, with a record of each run | — |
| How the mesh wires it | a source kind inside the dlt pipeline component, configured in YAML; the chart ships nothing for it | — |
| Sign-in | an API key read from an environment variable and sent in a request header to the SystemLink server | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct from the SystemLink server to the pipeline and then to the destination; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | `operational-data`: where producers' data is extracted | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none for mesh reads; it only reads, and it keeps a person's name out of the tables unless the deployment chooses to store it | — |
| Services served | ingest run rows and checks as quality metadata on the load | [DoD S13](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Kind paths are `dag_tools/asset_wrappers/sources/systemlink.py`; engine paths are under `dag_tools/systemlink/`; tests are under `dag_tools_tests/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| A registered source kind, not a component | kind `:393`, `:715-716`; `asset_wrappers/source_kinds.py:28`, `:129` | AGENTS.md §4p, §4q | `test_systemlink_component.py`: `test_the_example_loads_with_no_network_and_no_key` | `built` |
| Read-only, three POST routes | `queries.py:10`, `:33-90`; `client.py:134-136` | AGENTS.md §4q | `test_systemlink_engine.py`: `test_the_client_only_ever_posts_the_three_query_routes`, `test_the_client_has_no_method_that_can_write`, `test_filter_values_appear_only_in_substitutions` | `built` |
| Key from a named variable, header only | `spec.py:39`; kind `:367-371`, `:545`, `:559`; `client.py:103-112`, `:137-139` | AGENTS.md §4q | `test_systemlink_component.py`: `test_the_key_is_sent_in_the_header_only`, `test_neither_secret_appears_in_the_pipeline_dir_any_table_or_any_log`, `test_a_missing_key_variable_fails_the_run_naming_the_variable`; `test_systemlink_engine.py`: `test_the_client_repr_never_shows_the_key` | `built` |
| Direct sign-in with the key; 401/403 raise | `client.py:31`, `:163-165`; kind `:550-558` | AGENTS.md §4q | `test_systemlink_engine.py`: `test_a_401_is_auth_failed_not_an_empty_result`; no test of the path to the destination | `built` |
| Operator policy required; pseudonym needs a key | `spec.py:134-142`, `:159`; `incremental.py:66-78`; kind `:200`, `:546-548` | AGENTS.md §4q | `test_systemlink_component.py`: `test_pseudonym_without_a_key_env_fails_the_load`, `test_the_operator_is_pseudonymised_before_it_reaches_any_table`, `test_the_drop_policy_leaves_no_operator_column`; `test_systemlink_engine.py`: `test_the_operator_block_is_required` | `built` |
| Server-decided settings in YAML; a 400 names the setting | `spec.py:15-23`, `:102-123`; `client.py:166-170`; `paging.py:70-110`; kind `:640-647` | AGENTS.md §4q | `test_systemlink_component.py`: `test_a_server_that_rejects_the_configured_spelling_fails_naming_the_setting`; `test_systemlink_engine.py`: `test_a_400_on_a_long_chain_is_re_issued_once_with_one_id_and_names_the_length`, `test_a_400_is_never_split_the_way_a_timeout_is`, `test_the_spike_decided_defaults_are_all_in_one_block` | `built` |
| Terminal statuses required; empty refused separately | `spec.py:70-99`, `:154`, `:162-172` | AGENTS.md §4q | `test_systemlink_engine.py`: `test_the_filters_block_is_required_because_terminal_statuses_has_no_default`, `test_an_empty_terminal_statuses_gets_ITS_OWN_refusal`, `test_failed_statuses_is_required_UNDER_failed_only_and_not_otherwise`; `test_systemlink_component.py`: `test_an_absent_filters_block_fails_the_LOAD_naming_terminal_statuses` | `built` |
| Incremental by known ids; backfill resumes | `incremental.py:36-47`, `:84-131`; kind `:213-215`, `:348-364`, `:624-678` | AGENTS.md §4q | `test_systemlink_component.py`: `test_the_first_run_stores_everything_and_the_second_stores_nothing_new`, `test_a_repeated_backfill_resumes_past_the_parts_already_done`, `test_control_the_restart_tag_reads_every_part_again`, `test_the_incremental_job_is_never_a_backfill` | `built` |
| Two jobs; schedule targets incremental, stopped by default | kind `:473-474`, `:488-491`; `asset_wrappers/source_kinds.py:103-106`; `components/dlt_pipeline/schedule.py:71-72` | AGENTS.md §4p, §4q | `test_systemlink_component.py`: `test_the_schedule_targets_incremental_and_no_name_job_exists`; `test_dlt_pipeline_schedule.py`: `test_the_schedule_is_stopped_by_default` | `built` |
| Ingest run row and three checks | kind `:498-534`, `:587-599`, `:680-712` | AGENTS.md §4q | `test_systemlink_component.py`: `test_the_ingest_run_row_counts_the_run`, `test_a_refused_key_fails_the_run_and_is_recorded_without_the_key`, `test_the_asset_checks_pass_on_a_clean_run`, `test_the_not_visible_check_fails_when_a_result_has_no_steps`, `test_two_runs_of_one_failure_carry_the_SAME_digest` | `built` |
| `table_prefix` and `dest_schema` required; forced table name refused; legacy kind refuses the prefix; the dbt package is separate | kind `:398-406`; `asset_wrappers/dlt_assets_parsing.py:576-578`; `dag_tools/dbt_project_packages/mfg_trace_curated/dbt_project.yml` | AGENTS.md §4p, §4q, §4r | `test_systemlink_component.py`: `test_a_pipeline_without_a_prefix_or_with_a_forced_table_name_fails_the_load`, `test_a_legacy_kind_with_table_prefix_fails_the_load` | `built` |
