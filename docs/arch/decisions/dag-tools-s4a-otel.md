# dag-tools decisions: §4a (OTel)

This record covers the decisions in `AGENTS.md` section 4a, the OTel to API sync section, at the dag-tools pin (`503607d`). The section governs the pure mapping engine, the Dagster component that renders plans and the Restate handler that executes them. [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

The component itself is described in the [OTel to API sync record](../components/otel-api-sync.md). Engine paths below are under `dag_tools/otel_api_sync/`; the component is `dag_tools/components/otel_api_sync/component.py`.

## §4a (OTel) OTel to API sync

### §4a (OTel).1 The mapping engine is pure

- **Decision.** The mapping engine imports nothing from Dagster, Restate or dlt. The component, the handler and the tests share it.
- **Why.** A pure engine can be tested for its mapping semantics with no infrastructure.
- **Status.** `built`. The package's six modules import none of the three libraries. No test asserts the absence, so the property holds only until an edit breaks it.

### §4a (OTel).2 Plans are rendered by Dagster and executed by Restate

- **Decision.** Dagster renders each plan in full before dispatch. The handler holds no template engine, so every fallback body is rendered before anyone knows which call will fail.
- **Why.** A dry run then shows the exact bytes, and a mapping edit needs no worker redeploy.
- **Status.** `built`. See the [OTel to API sync record](../components/otel-api-sync.md), finding 5.

### §4a (OTel).3 A fallback never touches items that succeeded

- **Decision.** An item fallback re-renders in the failing item's scope only. An aggregate fallback is pre-rendered as one fragment per call, and only the fragments of failed calls are sent. No fallback mode renders over the whole group.
- **Why.** Bulk creation endpoints often replace what they are given. A group-wide fallback would delete server state that never appeared in the telemetry.
- **Status.** `built`. An aggregate fallback with no per-item loop is refused. See the component record, finding 8.

### §4a (OTel).4 Templates keep their native types

- **Decision.** Templates render through an environment that is both restricted and native-typed, so a single-expression template returns a real integer, boolean or list. Coercion helpers are the normal way to read telemetry attributes, which arrive as strings.
- **Why.** The restriction and native typing come from two different template classes. The restricted class alone turns every number, boolean and array into a string, with no error.
- **Status.** `built`.

### §4a (OTel).5 Numbers are exact or absent

- **Decision.** No plan carries a non-finite number or a rounded integer. Three guards apply:
  - a string whose literal parse would yield a non-finite or complex number stays a string;
  - integer text parses exactly, and the integer helper refuses a float-parsed value outside the exact range;
  - building a plan refuses any non-finite value that still reaches it, naming the field path.
- **Why.** JSON cannot carry non-finite numbers, and the HTTP client refuses to serialise them. Two silent paths used to produce them: the template engine's literal parse of an identifier, and parsing integers through a float.
- **Where else.** The override of the string concatenation works only because a matching template class carries it. Overriding the environment alone does nothing.
- **Status.** `built`.

### §4a (OTel).6 Dispatch waits for a group to be ready

- **Decision.** A group is dispatched when it is ready: after a quiet period, on a completion marker, or once it is too old to wait. A group is not assumed to arrive at once.
- **Why.** One group's telemetry arrives across several extraction runs. A half-filled group would be sent as an authoritative upsert of incomplete collections.
- **Status.** `built`, with one exception. A group with no parseable timestamp is treated as ready, even when a completion marker was declared ([component record](../components/otel-api-sync.md), finding 7). The gate fails open in that case.

### §4a (OTel).7 Scheduling is attached from YAML with a cron-tick condition

- **Decision.** The component ships no schedule or sensor. An operator attaches an automation condition through the component file's post-processing block, built from a cron tick that has passed since it was last handled. The cron condition that waits for all dependencies is not used.
- **Why.** The dependency-waiting condition waits on the extraction asset's upstream, an external source that never materialises. Dispatch then never runs, and no error says why. Readiness is time-based, so the trigger is too.
- **Where else.** Dagster's default automation sensor picks the condition up and is created stopped. The compatibility floor, Dagster 1.10.19, refuses the post-processing key and fails the whole component load. On that version the condition has to be set from Python.
- **Status.** `built`. The three tests skip on a version that refuses the key, matched by the error raised rather than by a version number.

### §4a (OTel).8 Lookups tolerate dlt's renaming

- **Decision.** Attribute lookups normalise keys, so one mapping works whether the data is staged through dlt or read directly. Anything that scans attributes in bulk resolves them through one helper.
- **Why.** dlt snake-cases identifiers and flattens nested maps into joined column names. A lookup by exact name returns nothing after staging, and an empty result passes any all-of check without checking anything.
- **Status.** `built`.

### §4a (OTel).9 Attribute maps are pinned to JSON on the way into staging

- **Decision.** The telemetry source pins its attribute map columns to JSON by default, so dlt does not flatten them.
- **Why.** Flattening lower-cases every part of the path. Mixed-case attribute names cannot be recovered afterwards.
- **Status.** `built`.

### §4a (OTel).10 Path substitutions are percent-encoded

- **Decision.** A value substituted into a URL path is percent-encoded. The path's own separators are not. An opt-out exists for a value that is already a path.
- **Why.** Servers decode a path before routing it. An identifier that contains a slash therefore cannot travel in a path segment at all, and has to go in the query string or the body.
- **Status.** `built`. The census says both facts were found by running the example. No run is recorded by this lane.

### §4a (OTel).11 A failed plan is recorded, never raised

- **Decision.** The executor never lets a plan-level failure escape as a terminal error. A halted plan sets a failed status in object state and returns normally. Calls delivered before the halt stay recorded as delivered. The plan's hash is not recorded as completed, so a corrected plan can be dispatched again.
- **Why.** Dagster dispatches through a one-way send, so nothing would observe a raised error. A terminal failure would also roll back the object state the failure record needs.
- **Status.** `built`. An unsupported plan version and an unresolvable API setting are recorded the same way.

### §4a (OTel).12 Non-secret API settings belong to the Dagster side

- **Decision.** The component's API block, holding the base URL, static headers and timeout, is resolved by Dagster and merged over the mapping's own block before validation. Setting the base URL on the component also removes the mapping's environment-variable name for it. A header is rendered into the plan only through one separately named field.
- **Why.** The mapping file stays portable. Without the removal, the worker's environment variable would still win. A header in the plan travels into the Restate journal, so that is never an accident.
- **Status.** `built`. See the component record, finding 9.

### §4a (OTel).13 The completion check reads the last result first

- **Decision.** The completion check compares the group's last result with the dispatched plan's hash before it consults the list of completed hashes.
- **Why.** A plan that completed with errors still has its hash recorded as completed. Reading that list first would report a partly failed plan as passed.
- **Status.** `built`. See the component record, finding 10.

### §4a (OTel).14 The staging destination takes the shape every component takes

- **Decision.** Both the dlt writer and the read-back resolve the staging destination through the one shared resolver. Explicit configuration, as parts or a connection string, wins. The dlt environment variable is the last resort.
- **Why.** The read-back used to prefer the environment variable over the parts. A variable left from another pipeline could then retarget the read away from the host the operator named.
- **Status.** `built`, with two notes:
  - The census names a test by an older name that does not exist at the pin. The test that pins the parts shape exists under its current name (see Sources).
  - The component record notes one more fallback: the Postgres form of the variable is read whatever the driver is ([component record](../components/otel-api-sync.md), finding 4).

### §4a (OTel).15 Resolving the destination returns a URL; building an engine is separate

- **Decision.** Resolution returns a URL string, and tests parse it. The engine is built by a thin wrapper that no test drives against a live database.
- **Why.** Building an engine imports the database driver. CI installs no Postgres driver, so a test that built one passed locally and failed on every runner.
- **Status.** `built`.

## Sources

All paths are at `503607dd321cd6627509bfc93955357b8566597f`. Test files are under `dag_tools_tests/`. Test files are named here in short: `render` is `test_otel_api_sync_render.py`, `numeric` is `test_otel_api_sync_numeric.py`, `automation` is `test_otel_api_sync_automation.py`, `executor` is `test_otel_api_sync_executor.py` and `component` is `test_otel_api_sync_component.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| .1 pure engine | `dag_tools/otel_api_sync/` (six modules); no import of the three libraries | AGENTS.md §4a (OTel), bullet 1 | no test of the absence | `built` |
| .2 render in Dagster, execute in Restate | `component.py:581`; `render.py:296-304`, `:317-393` | bullet 2 | see the component record, finding 5 | `built` |
| .3 scoped fallbacks | `spec.py:98-146`, `:196-204`; `render.py:222-293` | bullet 3 | render: `test_aggregate_fallback_renders_one_fragment_per_item_not_a_group_wide_body`, `test_aggregate_body_assembly_covers_only_failed_items`, `test_item_mode_fallback_is_rendered_in_the_failing_items_scope`, `test_aggregate_fallback_without_for_each_is_rejected` | `built` |
| .4 native typing in a restricted environment | `environment.py:79` | bullet 4 | render: `test_numeric_boolean_and_array_fields_keep_their_json_types` | `built` |
| .5 exact numbers | `environment.py:69`, `:82`, `:85`; `functions.py:425-463` (refusal at `:453`); `render.py:308`, `:344` | bullet 5 | numeric: `test_a_string_whose_literal_parse_json_cannot_carry_stays_a_string`, `test_ordinary_literal_parsing_of_a_string_result_is_unchanged`, `test_integer_strings_parse_exactly_past_float_precision`, `test_a_raw_non_finite_value_fails_the_render_and_names_the_field` | `built` |
| .6 readiness gate; fails open without a timestamp | `render.py:57-101` (no timestamp at `:75-81`) | bullet 6 | render: `test_readiness_defers_a_group_that_is_still_filling`, `test_complete_when_dispatches_immediately_on_a_terminal_marker`, `test_max_age_forces_dispatch_of_a_stranded_group`, `test_render_plans_separates_ready_groups_from_deferred_ones`; component: `test_a_group_that_is_still_filling_is_deferred_not_dispatched`, `test_ignore_readiness_dispatches_a_filling_group`; no test of the fail-open case | `built` |
| .7 YAML schedule; floor refuses the key | `examples/otel_to_api/README.md:176-184` | bullet 7 | automation: `test_readme_condition_requests_extraction_and_dispatch_on_every_tick`, `test_on_cron_never_fires_for_this_component`, `test_yaml_automation_gets_the_default_sensor_which_starts_stopped` | `built` |
| .8 normalised lookups | `functions.py:52`, `:127` | bullet 8 | render: `test_mapping_survives_dlt_column_normalization`, `test_attributes_resolve_through_dlt_flattened_columns`, `test_metrics_resolve_from_a_renamed_attribute_column` | `built` |
| .9 attribute maps pinned to JSON | `dag_tools/asset_wrappers/sources/clickhouse_query.py:35`, `:201` | bullet 9 | render: `test_clickhouse_source_pins_attribute_maps_to_json_by_default` | `built` |
| .10 percent-encoded paths | `environment.py:110`, `:119`, `:138`, `:151` | bullet 10 | render: `test_path_substitutions_are_percent_encoded`, `test_raw_opts_a_substitution_out_of_encoding`, `test_fallback_paths_are_encoded_too`, `test_query_parameters_are_encoded_separately_from_the_path`; component: `test_group_keys_are_url_encoded` | `built` |
| .11 failure recorded, not raised | `dag_tools/restate_handlers/api_call_plan.py:478`; version at `:484-498`; settings at `:511-515`; halt at `:757-759`; banked calls at `:774-778`; hash withheld at `:782-783`; last result at `:784` | bullet 11 | executor: `test_unhandled_client_error_halts_and_is_recorded_not_raised`, `test_halted_plan_banks_calls_delivered_before_the_halt`, `test_halted_plan_does_not_record_its_plan_hash_as_completed`, `test_halted_plan_does_not_execute_a_later_step` | `built` |
| .12 API settings on the Dagster side | `component.py:326-405` (override at `:368-382`); `dag_tools/components/otel_api_sync/schema.py:87-116`; `spec.py:37-40` | bullet 12 | component: `test_component_base_url_override_wins_and_clears_base_url_env`, `test_mapping_with_no_base_url_is_valid_when_component_supplies_one`, `test_plan_carried_headers_appear_in_the_rendered_plan`, `test_mapping_headers_survive_with_component_headers_layered_on_top` | `built` |
| .13 last result read first | `component.py:175-276` (last result at `:237`, completed hashes at `:251`) | bullet 13 | component: `test_completion_check_treats_completed_with_errors_as_a_failure` | `built` |
| .14 explicit destination wins | `component.py:75-119` | bullet 14 | component: `test_explicit_dest_config_host_beats_the_ambient_dlt_env_var`, `test_staging_url_accepts_the_same_host_parts_as_every_other_component` (the census's name for it, `test_staging_engine_accepts_the_same_host_parts_as_every_other_component`, is absent at the pin), `test_staging_url_still_accepts_a_credentials_dsn`, `test_staging_url_falls_back_to_the_dlt_env_var`, `test_staging_url_names_the_parts_shape_when_nothing_resolves` | `built` |
| .15 URL resolution split from the engine | `component.py:75-119` | bullet 15 | the five component tests above parse URLs; none builds an engine | `built` |
