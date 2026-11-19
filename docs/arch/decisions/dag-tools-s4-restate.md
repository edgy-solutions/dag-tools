# dag-tools decisions: §4 Restate worker deployment

This record covers the decisions in `AGENTS.md` section 4 at the component-records pin (`add5bc4`). Section 4 governs the shared Restate worker and the durable handlers it serves. [The decisions index](README.md) explains how each decision is addressed. The [Restate worker record](../components/restate-worker.md) describes the worker itself.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## Handlers and their deployment

### §4.1 Handlers ship in one shared worker image

- **Decision.** No project writes its own worker entrypoint or image. A new durable handler is added to one registry in the library and ships in the shared worker image.
- **Why.** One image and one registry keep every handler on the same runtime and the same release.
- **Status.** `built`. The registry maps each key to a handler module. The worker serves the services it names.

### §4.2 A deployment selects handlers and wires Restate through the environment only

- **Decision.** A deployment chooses which handlers run, and where Restate is, through three environment variables. The worker registers itself with Restate on startup.
- **Why.** A deployment changes values, not code.
- **Status.** `built`. The worker reads the selection and refuses an unknown key by name. It then registers its advertised address with the Restate admin endpoint. Tests cover the selection, but no test covers the registration.

## Failure handling inside a durable step

### §4.3 A status the plan branches on is returned as data, not raised

- **Decision.** An HTTP step returns its status from the durable step. Classification happens outside the step:
  - a success status succeeds;
  - a status with a declared fallback runs that fallback;
  - a retryable status raises, so Restate retries;
  - anything else is terminal.
- **Why.** A durable step retries whatever raises. A status meant to trigger a fallback would be retried forever if it raised, and the fallback would never run.
- **Status.** `built`. The API call plan executor follows this shape. Tests cover both the retryable case and the fallback case.

### §4.4 A step that opens a database connection is bounded, and terminal on damaging failures

- **Decision.** Every database-connecting step carries both:
  - a classifier, which makes a recognised authentication failure terminal at once;
  - a retry bound, which is the backstop for any failure the classifier does not recognise.

  The classifier reads error codes from the exception text, so it imports no driver. Listener-unavailable and connect-timeout codes are deliberately left retryable. The terminal message carries a code and a fixed sentence, and nothing from the driver's text.
- **Why.** A failed login spends one of the account's failed-login attempts. Retrying a wrong password therefore locks the account, and that takes down every other consumer of the account. Ending the step eventually is not enough.
- **Status.** `built`. All five connecting steps carry the shared bound. Tests cover:
  - a wrong password costing one attempt, with a control showing that without the guard it would loop;
  - an unrecognised failure being bounded;
  - a restarting listener being retried;
  - an async action being guarded;
  - the terminal message carrying no driver text;
  - every connecting step carrying a bound;
  - the real acknowledging handler end to end.

### §4.5 A payload the handler cannot act on is refused inside the step

- **Decision.** A handler validates its payload as the first act inside its durable step. A bad payload raises the library's own malformed-payload refusal, which is terminal. The refusal names every bad field and carries no value from any of them.
- **Why.** A refusal raised before any step fails the whole invocation. Restate retries a failed invocation with no bound, so the invocation never ends. The worst case is a list of ids sent as a string. A string looks like a list, so the update would run one character at a time and change the wrong rows.
- **Status.** `built`. All three payload handlers are converted. A structural test walks the handler package and fails if any payload handler reads its payload outside a step. It names the handlers it expects, and a planted-defect control proves it can see the defect.

### §4.6 A handler with two steps refuses in both

- **Decision.** When one payload feeds two durable steps, each step validates it again.
- **Why.** The two steps are separate journal entries. A replay can run the second step without the first, so a check made once before both would be skipped.
- **Status.** `built`. The API sync handler validates at the start of both steps. Its refusals include:
  - row data sent as a string;
  - an API path that does not begin with a slash, which would join onto the host name.

  The Oracle control handler refuses a string where it expects a list of identifiers.

### §4.7 A step that does not connect to a database uses the refusal guard, not the damage guard

- **Decision.** An HTTP step makes only the library's own refusals terminal. It does not use the database classifier.
- **Why.** The classifier matches error codes anywhere in an exception's text. An HTTP error quotes its URL, and an order number in that URL can look exactly like a database failed-login code. The call would then be abandoned instead of retried.
- **Status.** `built`. In the API sync handler, the HTTP step takes the refusal guard and stays unbounded. The acknowledging step beside it keeps the damage guard and its bound. A test asserts the two together, and a control shows the damage guard would have refused the same failure.

### §4.8 Absent credentials are terminal

- **Decision.** A handler that finds its credentials absent fails terminally. The message names the missing variables and carries no value.
- **Why.** Environment variables are read once, when the process starts. A retry reads the same snapshot, so it cannot pick up a rotated secret. A secret that changes in place has to arrive as a mounted file.
- **Status.** `built`. The missing-credentials refusal subclasses the standard value error, so existing handlers still catch it. Three handlers raise it.

## Data handling

### §4.9 Request rows are deduplicated on the whole row

- **Decision.** When the control handler builds request rows, it removes only rows that are identical in every column. Rows sharing an identifier are kept. The rows are compared after they are aligned to a common column set. A row that cannot be hashed is kept.
- **Why.** Two rows with one identifier are two versions, and so two requests. Collapsing them lost a request with no error or count.
- **Status.** `built`. Tests cover:
  - two versions of one identifier surviving;
  - an exact repeat collapsing;
  - comparison after alignment;
  - an unhashable value keeping its row;
  - the handler writing both versions.

### §4.10 Keying a virtual object serializes calls; it does not deduplicate them

- **Decision.** A handler whose calls append records keeps what it has completed in object state, and skips a repeat. The producer carries a matching guard, so the duplicate is not sent.
- **Why.** Two dispatches with the same key are two invocations. A durable step deduplicates retries within one invocation only.
- **Status.** `built`.
  - The API call plan executor keeps completed call keys and completed plan hashes in object state, and skips a plan it has already completed.
  - On the producer side, the telemetry sync component keeps a ledger of what it has dispatched.

  Tests cover a completed plan not running again, a bounded history of completed hashes, a re-dispatch sending only undelivered calls, and a changed call body being sent again.

## Trust

### §4.11 A private CA is merged into one bundle, and the bundle is named for every HTTP client

- **Decision.** When a deployment adds private CA certificates, an init container builds one merged bundle from three sources:
  - the system roots;
  - the Python certificate roots;
  - the mounted certificates.

  The environment variables that each HTTP client reads point at that merged bundle.
- **Why.** The two HTTP libraries in the handlers read different variables. Setting one leaves half the calls rejecting the endpoint, including the worker's own registration. Each variable also replaces the trust store rather than adding to it, so naming the private certificate alone breaks every public endpoint.
- **Status.** `built`. The census names two variables. The chart helper sets three; the third is for curl. Tests cover:
  - both variables being set;
  - both naming the merged bundle;
  - the merge keeping the public roots;
  - a canary that pins which variable each client honours.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Handler paths are under `dag_tools/restate_handlers/`. Chart paths are under `helm/dag-tools/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4.1 registry and shared image | `serve.py:35-41`, `:60-72` | AGENTS.md §4 | `test_restate_worker_services.py`: `test_the_registry_is_not_empty`, `test_every_registered_service_imports_and_exposes_a_service`, `test_every_registered_service_is_documented_in_the_chart` | `built` |
| §4.2 selection by environment; unknown key refused | `serve.py:46-56` | AGENTS.md §4 | `test_restate_worker_services.py`: `test_the_chart_default_is_a_valid_selection`, `test_selecting_the_pdm_pair_builds`, `test_an_unknown_key_is_refused_by_name` | `built` |
| §4.2 self-registration | `serve.py:43`, `:75-112`, `:115-139` | AGENTS.md §4 | no test | `built` |
| §4.3 status as data; classification outside the step | `api_call_plan.py:355-368`, `:431-432`, `:606`, `:622`, `:652`, `:726`, `:731` | AGENTS.md §4 | `test_otel_api_sync_executor.py`: `test_retryable_status_raises_so_restate_retries`, `test_status_with_a_fallback_does_not_raise` | `built` |
| §4.4 bounded and terminal on damage | `db_retry.py:119-122`, `:138-139`, `:187-190`, `:197`, `:223`, `:256`, `:298`, `:337`; callers `oracle_ack.py:151-154`, `oracle_control.py:354-357`, `:411-414`, `api_sync.py:187-190`, `sap_induction/service.py:101-104` | AGENTS.md §4 | `test_restate_db_retry.py`: `test_a_wrong_password_causes_one_login_attempt_not_a_loop`, `test_without_the_guard_a_wrong_password_would_loop`, `test_an_unrecognised_failure_is_bounded_rather_than_endless`, `test_a_restarting_listener_is_retried_because_that_is_the_point`, `test_an_async_action_is_guarded_too`, `test_the_terminal_message_carries_nothing_from_the_drivers_text`, `test_every_connecting_step_in_the_handlers_carries_a_bound`, `test_the_real_ack_handler_makes_one_login_attempt_on_a_wrong_password` | `built` |
| §4.5 payload refused inside the step | `db_retry.py:78`, `:162`; `oracle_control.py:82`, `:105`, `:252-268` | AGENTS.md §4 | `test_restate_payload_refusal.py`: `test_a_malformed_payload_costs_one_invocation_not_a_loop`, `test_the_old_shape_would_loop_at_the_invocation_level`, `test_the_refusal_happens_inside_the_step_not_before_it`, `test_a_string_of_ids_is_refused_rather_than_flipping_the_wrong_rows`, `test_the_refusal_carries_no_value_from_the_payload`, `test_every_problem_is_reported_at_once`, `test_a_well_formed_payload_still_updates_and_commits`, `test_the_stats_row_is_still_written_when_one_is_asked_for`, `test_every_payload_handler_refuses_inside_its_step`, `test_control_the_check_finds_a_payload_read_before_the_step` | `built` |
| §4.6 refusal in both steps | `api_sync.py:14`, `:39`, `:65`, `:100`, `:113`, `:160` | AGENTS.md §4 | `test_restate_api_sync_refusal.py`: `test_each_step_refuses_for_itself_because_a_replay_can_skip_the_first`, `test_a_string_of_row_data_is_refused_rather_than_posted_as_a_bare_string`, `test_a_path_that_would_extend_the_host_is_refused`; `test_pdm_control_cycle.py`: `test_a_string_of_meis_is_refused_rather_than_writing_one_row_per_character` | `built` |
| §4.7 refusal guard on the HTTP step | `db_retry.py:244`, `:312`; `api_sync.py:146-155`, `:187-190` | AGENTS.md §4 | `test_restate_api_sync_refusal.py`: `test_an_http_failure_quoting_a_five_digit_id_is_retried_not_refused`, `test_control_terminal_on_damage_would_have_refused_that_same_failure`, `test_the_refusal_guard_makes_our_own_refusal_terminal`, `test_the_refusal_guard_leaves_every_other_failure_alone`, `test_an_async_action_is_guarded_too`, `test_the_acking_step_carries_a_bound_and_the_http_step_does_not` | `built` |
| §4.8 absent credentials terminal | `db_retry.py:63`, `:156`; `oracle_ack.py:98-100`; `oracle_control.py:64-66`; `api_sync.py:115-116`, `:164` | AGENTS.md §4 | `test_restate_db_retry.py`: `test_absent_credentials_are_terminal_and_the_message_names_them`, `test_missing_credentials_stays_a_value_error`; `test_pdm_oracle_cycle.py`: `test_mark_as_processed_rejects_missing_oracle_credentials` | `built` |
| §4.9 whole-row dedupe | `oracle_control.py:203`, `:282-290` | AGENTS.md §4 | `test_pdm_control_cycle.py`: `test_two_versions_of_one_identifier_both_survive`, `test_an_exact_repeat_still_collapses`, `test_a_row_that_omits_a_column_is_compared_after_normalisation`, `test_an_unhashable_value_keeps_the_row`, `test_the_handler_writes_both_versions_of_one_identifier`, `test_identical_rows_collapse` | `built` |
| §4.10 completed calls kept in object state; producer ledger | `api_call_plan.py:110`, `:114`, `:780-788`, `:799-801`; `dag_tools/components/otel_api_sync/ledger.py:104-158` | AGENTS.md §4 | `test_otel_api_sync_executor.py`: `test_a_previously_completed_plan_hash_is_not_re_executed`, `test_completed_hash_history_is_bounded`, `test_a_superset_re_dispatch_only_sends_calls_not_already_delivered`, `test_a_call_whose_body_changed_is_delivered_again` | `built` |
| §4.11 merged CA bundle; three variables | `values.yaml:140`; `templates/restate-worker.yaml:36-40`, `:71`, `:76-86`; `templates/_helpers.tpl:195-204` | AGENTS.md §4 | `test_chart_ca_certs.py`: `test_enabling_sets_both_env_vars`, `test_the_env_vars_name_the_MERGED_bundle_not_the_mounted_secret`, `test_the_merge_keeps_the_public_roots`, `TestWhatTheClientsActuallyHonour` | `built` |
