# OTel API sync (CO-OTEL-API-SYNC)

The OTel API sync component turns telemetry rows in a ClickHouse store into ordered calls on an HTTP API. Dagster reads the rows, groups them into execution groups, checks each group is complete and renders its calls into a plan. It posts each plan to the [Restate worker](restate-worker.md), which makes the calls and survives restarts. A check then asks Restate whether each plan finished.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It builds, per pipeline, an optional extraction asset, one dispatch asset and one asset check, and ships no schedule, sensor or job.** The extraction asset is a dlt asset that copies the telemetry into a staging database. The dispatch asset is named `<pipeline>_dispatch`. The check is `restate_plans_completed`, on by default and blocking. The returned definitions hold assets, checks and the shared dlt resource only. An operator attaches automation from YAML or Python. `built`
2. **A pipeline needs a source query, a mapping and a Restate address, and the staging database when it is staged.** `source_config`, `restate_endpoint` and at least one source have no default. The mapping is inline or a file and must declare a group expression, the steps, and a base URL either in the mapping or in the component's `api:` block. A staged pipeline needs `dest_config`. Staged is the default. `built`
3. **It reads the telemetry store with a username and password that sit in the component's own attributes.** The same `source_config` is used by the dlt extraction and by the direct read of an unstaged pipeline. The password is whatever the YAML supplies, normally by Dagster's environment substitution. The component adds no sign-in beyond that. `built`
4. **It reads the staging database back with the same credential dlt writes with, and an explicit host wins over an ambient environment variable.** The URL comes from the one shared credential resolver. Order is the parts or DSN in `dest_config`, then the `DESTINATION__<DRIVER>__CREDENTIALS` variable. With neither, the run fails naming both. `built`
5. **Dagster renders every plan and Restate only executes it.** The mapping is validated at load and rendered in a restricted template environment that keeps native number, boolean and list types. A non-finite number in a plan fails the render and names the field. The plan hash is a digest of the rendered calls and does not depend on row order. See [the worker record](restate-worker.md) for the execution side. `built`
6. **Dispatch is a one-way post to the group's Restate object, sent with no credential of its own.** Each plan goes to the plan-executor service under its group key, on the `/send` ingress. The idempotency header is the plan id (group key and plan hash). The post and the completion check's status polls carry no authorization header. A failed hand-off is collected and raises after all groups are tried. `built`
7. **Readiness is gated before dispatch, a ledger stops re-sends, and an oversized plan is refused.** A group is held back while its newest row is younger than the quiet period unless `complete_when` matches or `max_age_seconds` has passed. With no gate configured every group is ready. A ledger of group and plan hash (in the staging database when staged, else in asset metadata) skips plans already sent. A plan over `max_plan_bytes` fails the run before anything is sent. Run config can bypass the gate or the ledger, cap groups, or dry-run. `built`
8. **Fallbacks are scoped to the item that failed.** An item fallback re-renders in the failing item's scope only. An aggregate fallback is pre-rendered as one fragment per call, and the worker splices in only the fragments of calls that failed. A step with an aggregate fallback must fan out, or the load fails. `built`
9. **API settings split between the mapping and the component, and a credential stays on the worker unless the operator opts in.** The component's `api:` block is merged over the mapping's before validation. A `base_url` there removes `base_url_env` so a worker variable cannot win. Header values from `header_env` stay as variable names in the plan. `plan_carried_headers` renders values into the plan and so into the Restate journal, and the component logs a warning naming the headers. `built`
10. **The completion check polls Restate and treats an unconfirmed plan as failed.** It reads the dispatch asset's last materialization, polls each group's status, and checks the last result before the completed list. A plan that finished with errors fails. A timeout fails. No materialization, or nothing dispatched, passes. `built`
11. **It goes direct, not through the central gateway or domain broker, and it publishes nothing to the catalog.** The code under both packages makes no call to either and has no catalog code. The data path is the telemetry store, the staging database, the Restate ingress and, from the worker, the target API. `built`
12. **Load refuses a bad mapping or setup; the run refuses what only data can show.** At load: no mapping, an invalid mapping, no sources, and a staged pipeline with no `dest_config`. At run: an unresolvable staging database, an oversized plan, a non-finite number and a failed hand-off. The pipeline attributes are plain dictionaries, and the schema models for them are exported but not applied in the component, so an unknown key is ignored. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `source_config` | required | connection to the telemetry store (host, port, username, password, database, secure) |
| `restate_endpoint` | required | Restate ingress base address |
| `dest_config` | empty | staging database; required when a pipeline is staged; parts, a DSN, or the dlt variable |
| `staging_config` | empty | optional intermediate staging configuration |
| `pipelines.<key>.sources` | required (at least one) | extraction queries: name, query or table, cursor column, lookback, key, write mode, map columns |
| `pipelines.<key>.mapping` / `mapping_file` | one required | the mapping document, inline or a path relative to the component |
| `pipelines.<key>.staged` | `true` | stage through the staging database; `false` reads the store directly |
| `pipelines.<key>.dispatch_from` | first source | which source the dispatch asset reads |
| `pipelines.<key>.dest_schema` | unset | staging schema |
| `pipelines.<key>.api` | unset | `base_url`, `headers`, `plan_carried_headers`, `timeout_seconds`, `max_concurrency`, `max_attempts`, `max_retry_seconds`; merged over the mapping's `api` |
| `pipelines.<key>.ledger` | enabled; `sql` when staged, else `dagster` | where dispatched plans are recorded; a table name and schema |
| `pipelines.<key>.completion_check` | enabled, blocking, 2 s poll, 300 s timeout | the Restate reconciliation check |
| `pipelines.<key>.max_plan_bytes` | 4 MiB | refuse a larger rendered plan; 0 disables |
| `pipelines.<key>.group_name` / `io_manager_key` / `pipeline_kwargs` | unset / `io_manager` / empty | Dagster grouping, IO manager, extra dlt run arguments |
| mapping `readiness` | no gate | quiet period, timestamp column, `complete_when`, `max_age_seconds` |
| mapping `api.retry_statuses` | a list of transient statuses | statuses the worker retries |
| mapping `api.max_concurrency` / `max_attempts` / `max_retry_seconds` | unset (no cap, unbounded) | cap and bounds, enforced per worker process |
| run config | all off or zero | `dry_run`, `limit`, `max_groups`, `only_group`, `ignore_readiness`, `ignore_ledger` |

## Gaps

- **The SQL ledger has no test.** The ledger that a staged pipeline uses by default creates its schema and table and writes rows (`ledger.py:104-158`). No test file refers to its load or flush functions. Remedy: not on the roadmap.
- **The oversized-plan refusal has no test.** The guard at `component.py:625-644` is not named in any test of the component. Remedy: not on the roadmap.
- **The ingress is called with no credential.** The plan post and the status polls carry no authorization header (`component.py:224`, `:766-770`). Whether the ingress needs one is a matter for the deployment. Remedy: [roadmap row 13](../ROADMAP.md) (not filed).

## Not established from code

- Which telemetry store, staging database and Restate ingress a deployment points at, and which credentials it supplies.
- Whether a deployment attaches a schedule or automation condition, and whether it starts it.
- What the target API does with a repeated or replayed call. The mapping can name an idempotency header; whether the API honours it is not shown.
- How a worker is deployed and how many replicas it has. The concurrency cap multiplies by replicas.
- The worker-side behaviour in full. It is covered by the [worker record](restate-worker.md) and was read here only through the plan it receives.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | delivers telemetry-derived records to an external API as ordered, resumable calls | — |
| How the mesh wires it | a YAML component; it posts plans to the Restate worker's plan-executor service and polls its status | — |
| Sign-in | a password for the telemetry store and one for the staging database, from the component's attributes; no identity presented to the ingress or the gateway | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct to the telemetry store, the staging database and the Restate ingress; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | operational, where a producer moves its own data out | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none in access decisions; the target API's credential stays on the worker by default, and is in the journal only if the operator opts in | [NATO R5](../alignment/nato-dcra.md) |
| Services served | turns source rows into the shape an API needs | [DoD S14](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/otel_api_sync/`; engine paths under `dag_tools/otel_api_sync/`; tests under `dag_tools_tests/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Extraction, dispatch asset, check; no schedule, sensor or job | `component.py:409-464`, `:563-567`, `:731-737` | AGENTS.md §4a | `test_otel_api_sync_component.py`: `test_unstaged_pipeline_generates_only_the_dispatch_asset`, `test_staged_pipeline_wires_extraction_into_dispatch`, `test_build_defs_emits_the_completion_check_by_default`, `test_completion_check_can_be_disabled`; `test_otel_api_sync_automation.py`: `test_yaml_automation_gets_the_default_sensor_which_starts_stopped`, `test_on_cron_never_fires_for_this_component`; no test that the component itself returns no sensor | `built` |
| Required config and defaults | `component.py:309-322`, `:417-432`; `schema.py:134-201` | AGENTS.md §4a, §5 | `test_a_staged_pipeline_without_dest_config_is_rejected`; no test of each default | `built` |
| Telemetry store credential from component attributes | `component.py:482-492`; `dag_tools/asset_wrappers/sources/clickhouse_query.py:38-56` | AGENTS.md §4a | no test of the credential path | `built` |
| Staging database resolved by the shared resolver; explicit host wins | `component.py:75-119` | AGENTS.md §4a | `test_staging_url_accepts_the_same_host_parts_as_every_other_component`, `test_explicit_dest_config_host_beats_the_ambient_dlt_env_var`, `test_staging_url_falls_back_to_the_dlt_env_var`, `test_staging_url_names_the_parts_shape_when_nothing_resolves`, `test_a_password_with_url_metacharacters_survives` | `built` |
| Dagster renders; native types; non-finite refused; stable hash | `component.py:581`; `render.py:296-304`, `:317-393`; `environment.py:79-89` | AGENTS.md §4a | `test_otel_api_sync_render.py`: `test_numeric_boolean_and_array_fields_keep_their_json_types`, `test_plan_hash_is_independent_of_source_row_order`; `test_otel_api_sync_numeric.py`: `test_a_raw_non_finite_value_fails_the_render_and_names_the_field`, `test_render_plans_surfaces_the_same_failure_so_dry_runs_see_it` | `built` |
| One-way post, plan id as idempotency key, no auth header, failed hand-off raises | `component.py:55-58`, `:660-661`, `:701-704`, `:748-783` | AGENTS.md §4a | `test_dispatch_posts_one_plan_per_group_to_the_group_keyed_object`, `test_group_keys_are_url_encoded`, `test_a_failed_handoff_fails_the_asset`, `test_only_group_narrows_the_dispatch`, `test_max_groups_caps_the_dispatch`; no test of absence of a credential | `built` |
| Readiness gate, ledger, plan size guard, run overrides | `render.py:57-101`, `:373-393`; `component.py:583-644`; `ledger.py:44-158`; `schema.py:13-48` | AGENTS.md §4a | `test_a_group_that_is_still_filling_is_deferred_not_dispatched`, `test_ignore_readiness_dispatches_a_filling_group`, `test_the_ledger_stops_a_second_dispatch_of_an_unchanged_group`, `test_ignore_ledger_forces_a_re_dispatch`, `test_dry_run_does_not_write_to_the_ledger`; render: `test_readiness_defers_a_group_that_is_still_filling`, `test_complete_when_dispatches_immediately_on_a_terminal_marker`, `test_max_age_forces_dispatch_of_a_stranded_group`; no test of the SQL ledger or of `max_plan_bytes` | `built` |
| Fallbacks touch only failed items | `spec.py:98-146`, `:196-204`; `render.py:222-293` | AGENTS.md §4a | render: `test_aggregate_fallback_renders_one_fragment_per_item_not_a_group_wide_body`, `test_aggregate_body_assembly_covers_only_failed_items`, `test_item_mode_fallback_is_rendered_in_the_failing_items_scope`, `test_aggregate_fallback_without_for_each_is_rejected`; `test_otel_api_sync_executor.py`: `test_aggregate_fallback_sends_only_the_items_that_failed` | `built` |
| API config split and credential placement | `component.py:326-405`; `schema.py:87-116`; `spec.py:22-60` | AGENTS.md §4a | `test_component_base_url_override_wins_and_clears_base_url_env`, `test_mapping_with_no_base_url_is_valid_when_component_supplies_one`, `test_plan_carried_headers_appear_in_the_rendered_plan`, `test_mapping_headers_survive_with_component_headers_layered_on_top`, `test_component_max_concurrency_overrides_the_mapping`; render: `test_secrets_are_never_rendered_into_the_plan`; no test of the warning | `built` |
| Completion check order and timeout | `component.py:175-276`, `:708-746` | AGENTS.md §4a | `test_completion_check_passes_when_every_group_completed`, `test_completion_check_fails_when_a_group_recorded_failed`, `test_completion_check_treats_completed_with_errors_as_a_failure`, `test_completion_check_times_out_on_a_group_that_never_reports`, `test_completion_check_passes_when_nothing_was_dispatched`, `test_completion_check_passes_when_there_is_no_materialization`, `test_completion_check_records_an_ingress_error_instead_of_crashing` | `built` |
| No gateway, broker or catalog call | whole of `dag_tools/components/otel_api_sync/` and `dag_tools/otel_api_sync/` | AGENTS.md §4a | no test of absence; both packages searched for gateway and broker, none found | `built` |
| Load and run refusals; schema models not applied | `component.py:326-354`, `:417-419`, `:433-436`, `:625-644`; `schema.py:134-201` | AGENTS.md §4a, §5 | `test_a_malformed_mapping_fails_at_definition_load_not_at_dispatch`, `test_a_staged_pipeline_without_dest_config_is_rejected`; no test that an unknown pipeline key is ignored | `built` |
