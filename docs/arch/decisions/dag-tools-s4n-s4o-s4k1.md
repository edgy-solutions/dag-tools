# dag-tools decisions: §4n, §4o, §4o-1, §4k-1

This record covers the decisions in `AGENTS.md` sections 4n, 4o, 4o-1 and 4k-1 at the component-records pin (`add5bc4`): classification propagation, ticket identity, refusal reasons, and the refusal fixture. The components that carry them are described in the [classification propagation record](../components/classification-propagation.md), the [domain broker record](../components/domain-broker.md), the [central gateway record](../components/central-gateway.md), the [mesh data client record](../components/mesh-data-client.md) and the [Dagster record](../components/dagster.md). [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §4n Classification propagation

A labelled source has labels a person decided. A derived column's label is a consequence of what the column read. One sensor composes it from the sources' published label sets and the column lineage Dagster already carries, and publishes the result to the catalog. The composition engine is the warehouse source's pure engine; this component is the Dagster surface over it and builds one sensor and no assets.

### §4n.1 The source side reads observations as well as materializations; the derived side reads materializations only

- **Decision.** A labelled source is an observable source asset, which emits observations and never materializations, so the source lookup takes the newest event across both logs by storage id. The derived lookup reads materializations only, because the sensor writes its own observation against the derived asset.
- **Why.** A materializations-only source lookup finds nothing on any real labelled source and refuses every dataset in words that describe a labelling failure. A derived lookup that counted the sensor's own observation would never match the banked cursor and would republish on every tick.
- **Where else.** Found by running the shipped example; a unit test that reports its source event as a materialization cannot see it.
- **Status.** `built`.

### §4n.2 Sources are declared, never inferred from the asset graph

- **Decision.** Each dataset lists its sources. A lineage entry naming an unlisted source is refused, and a listed source with no published label set, a column without a label, or no materialization fails that dataset.
- **Why.** Which upstreams publish labels is a policy fact. Inferred, a silent upstream would drop out and the ceiling would be computed from a subset of the inputs while looking correct.
- **Status.** `built`.

### §4n.3 Composition only tightens, and each dimension composes in its own algebra

- **Decision.** The component calls the engine and re-implements nothing: the maximum on the hierarchical dimension, union on a restrictive or exclusionary set, intersection on a permissive one.
- **Why.** One engine keeps the semantics testable with no Dagster and no catalog, and keeps the permissive direction, which runs opposite to the others, in one place.
- **Where else.** The rule is §4m.10 of the [§4r, §4l, §4m record](dag-tools-s4r-s4l-s4m.md).
- **Status.** `built`.

### §4n.4 A column with no lineage carries the table maximum, and composing no labels is refused

- **Decision.** An output column the lineage says nothing about takes the composition of every labelled column of every source read. The published decision records the basis, column lineage or table maximum, and the exact source columns. An asset that names no columns is refused.
- **Why.** A coarse label then reads as coarse rather than wrong. The least restrictive label in every dimension is the one answer composition must never reach.
- **Status.** `built`.

### §4n.5 A downgrade is accountable, overlaid, and validated when the code location loads

- **Decision.** A downgrade rule needs an id, an owner and a reason, and its id travels on the published decision. Two rules reaching one column are refused, as is a rule that tightens any dimension. A dimension the rule omits keeps the derived value; an empty set clears that dimension. Rule labels are checked against the mounted document at load.
- **Why.** Which rule a record credits decides who is accountable. Overlaying lets a rule relax one dimension without restating the others. A typo in a governance decision is found at deploy, not after it.
- **Status.** `built`.

### §4n.6 Two patch aspects, never a replacement, and no empty token

- **Decision.** The sensor publishes dataset properties (the classification, its origin as propagated, the sources and the document digest), then one glossary term per column per label value plus a removal for every other term in the vocabulary. No token key is sent when there is no token.
- **Why.** Adding without removing would leave a stricter term behind and silently void a downgrade. A full aspect would erase a crawler's metadata. An empty token is sent as a credential and rejected.
- **Where else.** No test of the absent token on this component was found; the rule matches the dbt publisher's (§4i).
- **Status.** `built`.

### §4n.7 Stopped by default, re-read every tick, isolated per dataset, retried without rebuilding

- **Decision.** The sensor starts stopped. It reads the dimensions document on every tick. One dataset's failure is recorded against that dataset and does not stop the others, and the failed dataset is retried on later ticks against the same materialization. An absent document loads and fails the tick. A dry run records everything and emits nothing.
- **Why.** Publishing classifications should not begin because a code location loaded. The mounted document can change between a deploy and a tick. A corrected rule should publish without a rebuild.
- **Status.** `built`.

### §4n.8 Chained propagation is not supported

- **Decision.** A derived dataset cannot be another's source. The limit is documented in the example README.
- **Why.** The sensor records its decision as an observation against the derived asset and reads its sources' published label sets; it is not built to read its own output as an input.
- **Where else.** No test declares a chain. Read from code, a chained declaration is refused at the tick: the sensor expects each source column entry to carry `labels`, and its own observation writes `label`, so the dataset fails and nothing is published for it ([label propagation](../flows/label-propagation.md), finding 8). A dataset that lists itself as its source is refused at load.
- **Status.** `built`, including the documented limit: no chaining.

### §4n.9 A YAML block key with only comments under it parses as null

- **Decision.** The catalog attribute is a mapping and rejects null, so an unset block has its key commented out as well. The attribute is not loosened, to match the warehouse source component. The example says so beside the block.
- **Why.** An operator editing the example otherwise hits a load failure that names a type rather than the edit.
- **Where else.** Documented only; no test.
- **Status.** `built`.

### §4n.10 The example is part of the contract

- **Decision.** The example's derived asset is ordinary Python, not a dag-tools component. Its three columns cover the three outcomes: composed from lineage, composed then downgraded, and the table-maximum fallback. Tests run a copy of the shipped example under a unique package name, so two copies never share one imported module.
- **Why.** Building the derived table is the deployment's business; propagation needs only the column lineage. Running the example is what found §4n.1.
- **Status.** `built`.

## §4o Ticket identity

Every ticket the broker returns carries an identity: a ticket id, the time of issue, and, where it can be read, the version of the data. The consumer gets a receipt built from that identity, and the gateway writes one line that joins the principal to it.

### §4o.1 `as_of` is the data's version and `issued_at` is the time of the decision; an absent version is never the issue time

- **Decision.** The two are separate fields. An absent `as_of` carries a reason token instead and is counted per source type on the broker's health body. A version is carried exactly as the source wrote it.
- **Why.** Defaulting to the issue time would make every ticket look complete and the field worthless, since a reader could no longer tell a version from a clock reading. Reformatting would make two equal versions look different.
- **Status.** `built`.

### §4o.2 The stamp goes on the one seam every resolve path returns through

- **Decision.** The resolve route calls the inner resolution and stamps whatever it returns. The inner resolution has four return paths: a minted ticket, an unminted ticket for a backend with no minter, and two legacy coordinate routes.
- **Why.** Stamping each path is the shape of invincible-agent ADR-0044, where a newer path returned ahead of the minting. The unprotected path is the one most worth identifying.
- **Status.** `built`.

### §4o.3 The identity is minted per request and never reaches the asset cache

- **Decision.** Each resolve mints a new ticket id and issue time. A version taken from the coordinates cached at load is labelled `advertised_at_load`; one read at authorization is labelled `read_at_authorize` and wins.
- **Why.** The cache lives for the process lifetime. An identity minted there would give every caller one ticket id and the pod's start time, a log that cannot tell two reads apart while appearing to.
- **Status.** `built`.

### §4o.4 Reading a version can never fail the read, and no reader ships

- **Decision.** Version readers are registered per source type, and the registry ships empty. A reader that raises, declines or returns an unusable value yields an absent `as_of` with a distinct reason. An integer converts to text; a boolean, an empty string or a structured value is refused.
- **Why.** Reading a table-format version is input and output against a store nothing here can exercise; a reader that cannot be tested is not shipped. The version is metadata about the read, whereas the credential is the read.
- **Where else.** No reader for any table format ships (`built`); the per-source-type count on the health body is how the gap is seen.
- **Status.** `built`; no reader ships.

### §4o.5 The consumer's receipt is an allowlist

- **Decision.** The mesh data client builds the receipt from a fixed list of ticket fields. The physical URI is not on it. The frame-only call is unchanged and delegates to the call that also returns the receipt.
- **Why.** A receipt is kept and quoted, so a denylist would publish the first secret-shaped field a ticket gains. A presigned URI is a credential in the shape of a path. Notebooks and IO managers call the frame-only method (§6).
- **Status.** `built`.

### §4o.6 The gateway's log line and the broker payload read one expression for the principal

- **Decision.** The subject in the gateway's ticket-issued line is the same variable the broker request carries. A broker that stamps no identity is logged with a dash and the read still serves. No credential is named on the line.
- **Why.** A log subject derived separately can describe someone other than the decision did, which is evidence of something that did not happen. The arm where they would differ is a verified token with no on-behalf-of header.
- **Status.** `built`.

### §4o.7 Keys are always present, as null when unknown

- **Decision.** The ticket always carries `ticket_id`, `issued_at`, `as_of` and `as_of_source`, and the receipt always carries the same four; the absence reason appears only when there is one.
- **Why.** A key that appears only sometimes is read with a default lookup and cannot be told from a misspelling.
- **Status.** `built`.

## §4o-1 Refusal reasons: the lines and the counts

The gateway refuses a request for one of a declared set of reasons. Under the `observe` posture it serves requests that `enforce` would refuse and records what it would have done; under `enforce` it refuses them. This section is about how those refusals are logged and counted.

### §4o-1.1 An `observe`-posture refusal gets a line of its own

- **Decision.** The would-line reports what `enforce` would decide, which can never be `no_principal`, because that reason is produced only by the `observe` branch after the would-line's decision. So `observe` also emits a refused line, at warning level, naming the reason the caller actually got. `enforce` emits no second line, since its audit line already carries the one decision.
- **Why.** Before the refused line, an `observe` refusal for `no_principal` was named to the caller and to nobody in the logs, while the request's one line named another reason. Rewriting the would-line instead would lose the migration-readiness signal.
- **Status.** `built`.

### §4o-1.2 Would-refuse and did-refuse are two counts and are never summed

- **Decision.** The health body carries both counts per reason. Under `enforce` one refusal counts in both.
- **Why.** Under `observe` a would-refused request was served while a did-refused one received a 4xx; one number would report refusals for served requests or the reverse.
- **Status.** `built`.

### §4o-1.3 The vocabulary is installed from the status mapping, and every name is present at zero

- **Decision.** The reason names come from the one mapping that gives each a status code, and are installed into the counter at import. An `observed` count beside them separates a quiet gateway from a counter that was never installed. An undeclared reason is counted as undeclared and warned about once.
- **Why.** A second hand-kept list is how a reason ends up with a status code and no count. A key present only when non-zero cannot be told from a misspelling.
- **Status.** `built`.

### §4o-1.4 The line and the count sit in separate guards, and the health body answers null rather than failing

- **Decision.** The evidence line and the count are each wrapped in their own exception guard after the decision is made. If the counter fails, the health body reports the reason counts as null and still answers.
- **Why.** An instrument may never break what it measures. One shared guard would let whichever ran first survive a defect in the other. A readiness probe that fails because a counter misbehaved takes the pod out.
- **Status.** `built`.

## §4k-1 The refusal fixture

The accepted fixture shows what a governed read should take. This fixture plants rows a governed read should refuse, one rule at a time, and sends them through the same file, frame and storage path as the accepted fixture.

### §4k-1.1 The accepted fixture proves only acceptance

- **Decision.** Planted rows travel the real path from CSV through a frame and Parquet to the IO manager, under the same IO manager configuration as the accepted fixture.
- **Why.** Refusal paths that exist only in a reader's own tests, over frames that reader built, can agree with themselves about a defect no producer can emit.
- **Status.** `built`.

### §4k-1.2 One case per rule and one rule per case

- **Decision.** Each plant is isolated, and a parametrized test runs every rule's check against every case.
- **Why.** A frame with two violations proves nothing: a reader refusing on the first never reaches the second, and one refusing for the wrong rule reads as correct. Running every check is what catches an accidental second plant.
- **Status.** `built`.

### §4k-1.3 A case is addressed by scope, with a clean control scope in the same asset

- **Decision.** Each rule's case is one synthetic program whose scope value is derived from the rule id. A clean control program sits in the same asset, and the control's id is asserted never to be a rule id.
- **Why.** Scope is how the contract addresses anything. Without the control, a broken asset would refuse every scope and read as a full set of successful cases.
- **Status.** `built`.

### §4k-1.4 Two rules are planted per asset rather than per scope, and one is not a defect

- **Decision.** A missing column and a wrong type are properties of a frame, so each has its own asset. The rule that an asset may hold other scopes' rows is planted by the row-level asset holding every case's program, and that case carries no scope.
- **Why.** No row filter reaches a frame property. The contract allows other programs in an asset, so a read that kept them forgot its filter.
- **Status.** `built`.

### §4k-1.5 The withheld column is never the scope or the grain

- **Decision.** The missing-column asset lacks exactly one required column, which is not the scope column.
- **Why.** Withholding the address makes the refusal unaddressable rather than planted, and the reader fails for a reason the rule did not name.
- **Status.** `built`.

### §4k-1.6 The contract is the source of record, and the checks belong to the test

- **Decision.** The test loads the contract through the library's own loader and drives every check from it. Every contract rule must have a planted case, and no case may name a rule the contract lacks. The checks are the test's reading of the rules and are not a reader's implementation.
- **Why.** A rule added to the contract fails the fixture until a case is planted. A fixture that imported the reader it exists to catch would stop catching it.
- **Status.** `built`.

### §4k-1.7 A planted null and a planted NaN stay distinct

- **Decision.** The NaN check fills nulls with false, and both plants are asserted to survive the Parquet round trip as themselves.
- **Why.** The NaN test answers null for a null, so without the fill the null plant would also trip the non-finite rule and the two cases would merge.
- **Status.** `built`.

### §4k-1.8 Rules sharing one mechanism are told apart by the axis

- **Decision.** A broken constant-within condition maps to one of five rules. Three name their column and win; the other two are read from what the value is constant within, the contract's scope or something narrower. The axis is asserted to be stated by the contract, with at least one column that has no rule of its own.
- **Why.** Mapping the general case to one rule is invisible: the plant trips, the frame is broken, and the refusal names the wrong rule, which reads as a pass to anything that checks only that something was refused.
- **Status.** `built`.

### §4k-1.9 The calendar boundary is derived, not written down

- **Decision.** The period plant leaves the fiscal year the accepted fixture reports in.
- **Why.** The contract names its period vocabulary without carrying it, and a name cannot be tested against. A period outside the fiscal year is outside the calendar whatever its months are.
- **Status.** `built`.

### §4k-1.10 Demo-gated, through the accepted fixture's IO manager configuration

- **Decision.** The surface is added only in demo mode, like the demo dbt surface. It writes through the accepted fixture's IO manager configuration under its own resource key, and its assets derive their row filters from the case list.
- **Why.** Rows planted to be rejected should not reach production. Merging two definitions under one resource key raises. Deriving filters from the case list means a case cannot be declared and left out of the rows, nor produced and left undeclared.
- **Where else.** The [Dagster record](../components/dagster.md), finding 5, notes that the invincible-agent chart turns demo mode on by default, so a deployment that keeps that default carries these rows.
- **Status.** `built`.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Test files are under `dag_tools_tests/`. `cpc` is `dag_tools/components/classification_propagation/component.py`, `cps` is `dag_tools/components/classification_propagation/schema.py`, `tid` is `dag_tools/domain_broker/ticket_identity.py`, `brk` is `dag_tools/domain_broker/main.py`, `cli` is `dag_tools/cortex_data/client.py`, `gw` is `dag_tools/central_gateway/main.py`, `idn` is `dag_tools/central_gateway/identity.py`, `rg` is `dag_tools/central_gateway/refusal_gauge.py`, `fix` is `dag_tools/user_deployment/demo_ev_refusal.py`. Test abbreviations: `tcp` is `test_classification_propagation.py`, `twp` is `test_warehouse_source_propagation.py`, `tti` is `test_ticket_identity.py`, `ttr` is `test_ticket_receipt.py`, `tgr` is `test_gateway_refusal_reasons.py`, `ter` is `test_demo_ev_refusal.py`. An em dash in the Source column means the decision is the test's own reading of the contract's rules, so the test cited under Evidence is also its source.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4n.1 observation log for sources; materializations for derived | cpc `:482-492`, `:495-516`, `:519-526` | AGENTS.md §4n, bullet 1 | tcp: `test_the_shipped_example_propagates_from_a_source_it_only_OBSERVED` (`:1085`) | `built` |
| §4n.2 sources declared | cps `:146`, `:165-210`; cpc `:133-149`, `:174-181`, `:248-254` | bullet 2 | tcp: `test_a_source_publishing_no_label_set_fails_rather_than_counting_as_nothing` (`:300`), `test_a_source_publishing_a_column_without_a_label_fails` (`:316`), `test_a_declared_source_that_never_materialized_fails` (`:325`), `test_lineage_naming_an_undeclared_source_is_refused` (`:339`) | `built` |
| §4n.3 composition delegated to the engine | cpc `:65`, `:262` | bullet 3 | tcp: `test_a_column_with_lineage_composes_from_the_columns_it_read` (`:372`); twp: `test_a_rule_that_tightens_a_dimension_is_not_a_downgrade` (`:539`), `test_a_rule_that_tightens_only_a_set_dimension_is_still_refused` (`:550`) | `built` |
| §4n.4 table maximum; basis recorded; no-column asset refused | cpc `:189-216`, `:286-305` | bullet 4 | tcp: `test_a_column_the_lineage_says_nothing_about_carries_the_table_max` (`:401`), `test_the_document_records_how_each_column_was_arrived_at` (`:578`), `test_an_asset_naming_no_columns_is_refused_not_propagated_over_nothing` (`:366`) | `built` |
| §4n.5 accountable, overlaid downgrade; validated at load | cps `:56-120`; cpc `:219`, `:708-717` | bullet 5 | tcp: `test_a_declared_downgrade_is_applied_and_recorded_with_its_accountability` (`:431`), `test_a_downgrade_rule_nobody_is_accountable_for_is_refused` (`:1220`), `test_two_rules_for_one_column_are_refused_rather_than_ordered` (`:1235`), `test_a_declared_downgrade_is_validated_against_the_document_at_load` (`:1146`); twp: `test_a_rule_says_nothing_about_the_dimensions_it_omits` (`:603`), `test_a_rule_can_clear_a_set_dimension_and_not_only_lower_the_ladder` (`:685`) | `built` |
| §4n.6 two patch aspects with removals; no empty token | cpc `:308-370`, `:373-392` | bullet 6 | tcp: `test_both_aspects_are_patched_so_what_else_describes_the_dataset_survives` (`:485`), `test_the_terms_patch_removes_the_rest_of_the_vocabulary` (`:509`), `test_nothing_outside_the_documents_vocabulary_is_touched` (`:547`), `test_the_properties_say_the_label_was_propagated_and_what_from` (`:558`); no test of the absent token | `built` |
| §4n.7 stopped; per-tick read; isolated; retried; absent document fails the tick; dry run | cpc `:544-546`, `:567-573`, `:574-634`, `:590`, `:681`; cps `:266` | bullet 7 | tcp: `test_the_sensor_is_stopped_by_default_and_launches_no_runs` (`:650`), `test_the_document_is_read_at_every_tick_not_once_at_load` (`:856`), `test_one_dataset_failing_does_not_stop_the_others` (`:758`), `test_a_failure_is_recorded_against_the_asset_rather_than_only_logged` (`:788`), `test_a_failing_dataset_is_retried_without_a_re_materialization` (`:806`), `test_dry_run_records_the_whole_decision_and_emits_nothing` (`:884`), `test_the_code_location_loads_with_no_document_mounted` (`:1172`) | `built` |
| §4n.8 no chaining: the documented limit | `examples/warehouse_source/README.md:192`; cps `:186-189` | bullet 8 | no test declares a chain; tcp: `test_a_dataset_block_that_cannot_propagate_is_refused` (`:1205`) refuses a dataset listing itself | `built` |
| §4n.8 no chaining | `examples/warehouse_source/README.md:192`; cps `:186-189` | bullet 8 | no test declares a chain; tcp: `test_a_dataset_block_that_cannot_propagate_is_refused` (`:1205`) refuses a dataset listing itself | `built` |
| §4n.9 commented block key parses as null | cpc `:671`; `examples/warehouse_source/dagster_home/components/propagation/component.yaml:83-92` | bullet 9 | documented; no test | `built` |
| §4n.10 example is part of the contract | `examples/warehouse_source/dagster_home/derived.py:58`, `:93-102` | bullet 10 | tcp: `test_the_shipped_example_builds_one_sensor_and_materializes_nothing` (`:1067`), `test_the_shipped_example_propagates_from_a_source_it_only_OBSERVED` (`:1085`) | `built` |
| §4o.1 version and issue time separate; absence reasoned and counted | tid `:56-64`, `:149-194`, `:197-223`; brk `:80`, `:1140`, `:1186-1188` | AGENTS.md §4o, bullet 1 | tti: `test_an_absent_as_of_is_not_the_issue_time` (`:171`), `test_an_absent_as_of_is_counted_per_source_type` (`:191`), `test_the_absence_is_on_health_where_an_operator_can_see_it` (`:204`), `test_a_version_is_carried_exactly_as_the_source_wrote_it` (`:362`) | `built` |
| §4o.2 one seam | brk `:1173-1189`, `:1192` | bullet 2 | tti: `test_the_unprotected_path_is_identified_TOO` (`:115`), `test_the_legacy_coordinate_path_is_identified_too` (`:131`) | `built` |
| §4o.3 per request; provenance of the version | tid `:47`, `:140-146`, `:197-223`, `:226-243` | bullet 3 | tti: `test_two_resolves_of_one_urn_get_different_ticket_ids` (`:140`), `test_the_identity_does_not_reach_the_cached_coordinates` (`:159`), `test_a_version_the_producer_advertises_is_carried_and_labelled` (`:222`), `test_a_version_read_at_authorize_beats_one_advertised_at_load` (`:234`) | `built` |
| §4o.4 readers registered per source type; ships empty; never fails the read | tid `:110`, `:113`, `:121-137`, `:149-194` | bullet 4 | tti: `test_a_failing_reader_does_not_fail_the_ticket` (`:283`), `test_a_structured_value_is_refused_rather_than_stringified` (`:331`), `test_a_boolean_is_not_a_version` (`:347`), `test_an_empty_string_is_absent_not_a_version` (`:355`), `test_no_as_of_reader_ships_by_default` (`:376`) | `built` |
| §4o.4 no version reader ships | tid `:110`, `:113`, `:121-137`, `:149-194` | bullet 4 | tti: `test_a_failing_reader_does_not_fail_the_ticket` (`:283`), `test_a_structured_value_is_refused_rather_than_stringified` (`:331`), `test_a_boolean_is_not_a_version` (`:347`), `test_an_empty_string_is_absent_not_a_version` (`:355`), `test_no_as_of_reader_ships_by_default` (`:376`) | `built` |
| §4o.5 receipt allowlist; frame-only call unchanged | cli `:23-32`, `:45-56`, `:161-171`, `:173` | bullet 5 | ttr: `test_the_receipt_is_an_allowlist_so_a_new_ticket_field_is_not_published` (`:78`), `test_a_presigned_uri_is_not_on_the_receipt` (`:70`), `test_get_dataframe_still_returns_only_a_frame` (`:168`) | `built` |
| §4o.6 one principal expression; dash for unstamped; no credential | gw `:536`, `:562`, `:591-597` | bullet 6 | ttr: `test_the_log_follows_the_VERIFIED_subject_where_it_differs_from_the_header` (`:337`), `test_an_unstamped_ticket_logs_a_dash_AND_STILL_SERVES_THE_READ` (`:309`), `test_the_log_line_names_no_credential` (`:325`) | `built` |
| §4o.7 keys always present | tid `:68-97`; cli `:37-42` | bullet 7 | tti: `test_as_of_is_always_a_key_even_when_unknown` (`:253`); ttr: `test_an_older_broker_yields_a_receipt_of_the_SAME_SHAPE` (`:108`) | `built` |
| §4o-1.1 refused line under `observe` only | idn `:510-587`, `:627-655` | AGENTS.md §4o-1, bullet 1 | tgr: `test_no_principal_is_logged_by_its_own_name` (`:181`), `test_the_would_line_still_reports_what_ENFORCE_would_do` (`:200`), `test_enforce_emits_no_second_refusal_line` (`:230`), `test_the_refusal_line_names_no_credential` (`:219`) | `built` |
| §4o-1.2 two counts | rg `:52-53`, `:147-174`; gw `:149`, `:171-175` | bullet 2 | tgr: `test_a_would_refusal_and_a_did_refusal_are_counted_separately` (`:249`), `test_under_enforce_one_refusal_counts_in_both` (`:276`) | `built` |
| §4o-1.3 vocabulary from the status mapping; all at zero; undeclared warned once | idn `:106-129`; rg `:54-55`, `:65-103`, `:132-146` | bullet 3 | tgr: `test_every_declared_reason_is_present_at_zero` (`:290`), `test_observed_separates_a_quiet_gateway_from_a_dark_gauge` (`:306`), `test_an_undeclared_reason_is_counted_and_warned_once` (`:318`) | `built` |
| §4o-1.4 separate guards; null on health | idn `:559-585`; gw `:171-175` | bullet 4 | tgr: `test_a_defect_in_the_counter_does_not_refuse_the_read` (`:341`), `test_a_defect_in_the_line_does_not_silence_the_count` (`:356`), `test_health_still_answers_when_the_reason_counter_misbehaves` (`:405`) | `built` |
| §4k-1.1 real write and read path | fix `:61`, `:64`, `:92`, `:256-276`, `:349-361` | AGENTS.md §4k-1, bullet 1 | ter: `test_it_writes_through_the_same_manager_as_the_accepted_fixture` (`:432`), `test_the_planted_null_survives_the_write_and_is_not_a_nan` (`:367`) | `built` |
| §4k-1.2 one rule per case | fix `:111-233` | bullet 2 | ter: `test_a_case_trips_exactly_its_own_rule` (`:303`) | `built` |
| §4k-1.3 scope per case; control scope | fix `:68`, `:95-107`, `:243-253` | bullet 3 | ter: `test_the_control_scope_trips_nothing` (`:294`), `test_the_control_is_not_a_rule` (`:257`) | `built` |
| §4k-1.4 frame-level plants; unscoped case | fix `:72-74`, `:81`, `:87`, `:281-340` | bullet 4 | ter: `test_the_unscoped_case_is_the_asset_holding_every_scope` (`:309`) | `built` |
| §4k-1.5 withheld column is not the scope | fix `:81` | bullet 5 | ter: `test_the_missing_column_is_not_the_scope_column` (`:337`), `test_the_missing_column_asset_is_short_exactly_one_required_column` (`:331`) | `built` |
| §4k-1.6 contract drives the checks | `examples/contract_mapping/contracts/ev_fixture.yaml` | bullet 6 | ter: `test_every_contract_rule_has_a_planted_case` (`:229`), `test_no_case_names_a_rule_the_contract_does_not_have` (`:239`) | `built` |
| §4k-1.7 null and NaN distinct | — | bullet 7 | ter: `test_the_planted_null_survives_the_write_and_is_not_a_nan` (`:367`), `test_the_planted_nan_survives_the_write_and_is_not_a_null` (`:380`) | `built` |
| §4k-1.8 told apart by the axis | — | bullet 8 | ter: `test_the_constant_within_rules_name_contract_columns_and_contract_rules` (`:265`), `test_a_case_trips_exactly_its_own_rule` (`:303`) | `built` |
| §4k-1.9 calendar boundary derived | — | bullet 9 | ter: `test_the_planted_period_leaves_the_calendars_fiscal_year` (`:408`) | `built` |
| §4k-1.10 demo-gated; shared IO manager configuration; own key | `dag_tools/user_deployment/definitions.py:232`, `:254-262`; fix `:92`, `:349-361` | bullet 10 | ter: `test_a_deployment_with_demo_mode_off_does_not_have_them` (`:449`), `test_it_writes_through_the_same_manager_as_the_accepted_fixture` (`:432`), `test_it_merges_with_the_demo_surface` (`:440`), `test_the_refusal_definitions_carry_every_case` (`:424`) | `built` |
