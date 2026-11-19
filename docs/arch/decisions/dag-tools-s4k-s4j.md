# dag-tools decisions: §4k, §4j

This record covers the decisions in `AGENTS.md` sections 4k and 4j at the component-records pin (`add5bc4`): contract mapping, the read audit and the tagged warehouse. The last bullet of §4j is about the concurrency cap in the API call plan handler; it sits in that section of the census and is recorded here. The components that carry these decisions are described in the [contract mapping record](../components/contract-mapping.md), the [read audit record](../components/read-audit.md), the [tagged warehouse record](../components/tagged-warehouse.md) and the [OTel to API sync record](../components/otel-api-sync.md). [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §4k Contract mapping

A contract mapping takes a raw table and a governed contract and produces the dbt model, schema file and tests that make the table match the contract. A pure engine turns the documents into one plan. The SQL route and a row route both read that plan, and the component is the Dagster surface over it.

### §4k.1 The engine is pure, and a row route proves the mapping without a warehouse

- **Decision.** The engine imports no Dagster, dbt or dlt and does no IO. Documents go in as dictionaries and one plan comes out. The SQL route and the row route share that plan. A proof test runs the shipped mapping over rows derived from the shipped fixture and asserts it reproduces the fixture exactly, types included, by inverting every transform the plan declares.
- **Why.** The row route lets a mapping be proven with no warehouse. The inversion reads the plan's columns and names no column itself. It fails if the plan's description of a transform stops matching what the row route does, which is the disagreement that would make a dry run wrong.
- **Status.** `built`.

### §4k.2 A refusal means the emitted artifact would be wrong; everything else is a note

- **Decision.** A mapping that reads a column the source lacks, drops a column a governed read selects, or disagrees with the contract about write cadence fails the code location load. A mapping that cannot get a test written loads and says so in the plan's notes. Every problem is collected and reported together.
- **Why.** A wrong model must not reach a run. A correct model with less proof than it could have is still correct, and an operator fixing a mapping wants the whole list at once.
- **Status.** `built`.

### §4k.3 Nothing is guessed to fill a gap

- **Decision.** There is no default warehouse type, cast template or non-finite predicate, and no inferred period calendar. The enforced-contract setting is emitted only when every column has a declared warehouse type; one missing type drops the whole enforced contract.
- **Why.** A wrong guess emits SQL that fails at the warehouse for a reason unrelated to the data, and a guessed non-finite predicate can match nothing, giving a test that checks nothing. A half-enforced contract reads as an enforced one.
- **Status.** `built`.

### §4k.4 A file contract is checked at load; a served contract is fetched at run time

- **Decision.** A contract read from a file is reconciled with the mapping when the code location loads. A contract served by an endpoint is fetched when the asset materializes, and the notes say the load-time check did not run. An expected contract id, when set, is enforced on both routes.
- **Why.** A code location must not fail to load because a service is down. The expected id stops a service that starts serving a different document from re-pointing the mapping, and on the file route it catches a path edited to the wrong document.
- **Status.** `built`.

### §4k.5 A contract file may hold a bundle, and the expected id selects from it

- **Decision.** A contract path may hold several documents separated by YAML document markers. The expected contract id selects one of them. A bundle read with no expected id fails the load and lists the ids it holds. Empty documents are dropped, so one document plus a trailing marker stays on the single-document route, and a stream with no document at all has its own refusal. Both places that read the file select, at load and at run time. The served route is unchanged.
- **Why.** A deployment governs its contracts together in one file. Reading only single documents would force a copy of one document, which is a second governed text that can drift. Taking the first document of a bundle could map against the wrong table while every column name still lines up. A second selector field was rejected: it would state the same fact twice and make the expected-id check impossible to fail on the bundle route.
- **Where else.** No test was found for the refusal of a stream with no document.
- **Status.** `built`.

### §4k.6 Everything is rendered in memory before anything is written, and pruning is off by default

- **Decision.** The component renders every artifact into memory and writes only after the render succeeds. A dry run shows the exact bytes, and a run-config dry run overrides a configured target for one run. Pruning is off by default, and when on it considers only files named after the component's own model.
- **Why.** A failed render then leaves the target directory untouched rather than half rewritten. The component writes into a directory it does not own, and deleting a test an operator wrote by hand is worse than leaving a stale one.
- **Status.** `built`.

### §4k.7 Type fidelity: exact integers, the source's written numeric form, no non-finite values

- **Decision.** Integer text parses exactly beyond 2^53 and never through a float. A numeric column keeps the source's written form. A non-finite value is refused with the field path. A boolean in a numeric column is refused. A scale factor is read as an exact decimal.
- **Why.** Each of these fails silently otherwise. A whole amount widened into exponent notation reads as the same number and is not, and a unit conversion of money through a binary float stops a total reconciling.
- **Status.** `built`.

### §4k.8 Timestamp, date and string-array columns each refuse the lenient reading

- **Decision.** A timestamp is carried as UTC and a value naming no offset is refused, unless the mapping declares the source is UTC; that declaration is refused on any other column type. A date accepts the ISO calendar spelling only, checked before parsing, and refuses a datetime. A string array refuses a bare string and a set. A number is refused for both date types.
- **Why.** A naive value names no instant, so reading it as UTC shifts every row of a non-UTC source by hours and still looks like a timestamp. A date that accepts a datetime passes until the first row carrying a time. A bare string is iterable and would split into characters, and a set has no order. Epoch seconds and epoch milliseconds overlap in range for recent dates, so the wrong one parses cleanly and is off by decades.
- **Status.** `built`.

### §4k.9 Each transform answers one disagreement, and normalization is a declared rule

- **Decision.** `nullif` turns a sentinel into a null. `in_set` tests membership in the source's own enumeration and changes the type, so the mapping declares a cast for it. `normalize` applies an ordered list of string operations defined in a normalizer document that the operator supplies by path or URL. A raw companion column is the author's own copy, never generated. `run_timestamp` takes one instant per load: the caller passes it on the row route, and the mapping declares an expression for the SQL route. `source.where` filters rows on the same grammar as the dlt component's row filters.
- **Why.** A reader cannot tell a sentinel from data. A status vocabulary the library may not name can still drive a flag. The identifier grammar becomes a file an operator edits, not a library release. A per-row clock would make rows of one load differ by when they were written. One filter grammar means a filter means one thing in the repo.
- **Where else.** The row-filter grammar is §4p.12 of the [§4p record](dag-tools-s4p.md).
- **Status.** `built`.

### §4k.10 `coalesce` counts arguments, and a declared default is one

- **Decision.** One source column plus a declared default is accepted as a two-argument `coalesce`. A lone source with no default is still refused as a no-op. `concat` is not relaxed in the same way. A default the column type cannot take fails the load, using the row route's own acceptance check.
- **Why.** Both routes already implemented the one-source-plus-default case and agreed on it, so the arity rule was the error. `concat` has no fallback to count. A default the type rejects used to be refused on the row route and emitted as a bare literal on the SQL route, where the warehouse might error or silently coerce; checking at load with the row route's rule avoids a second copy of that rule.
- **Status.** `built`.

### §4k.11 A SQL cast may round where the row route refuses, and this is documented rather than hidden

- **Decision.** The row route refuses a scaled value that is not whole when the target column is an integer. The SQL route's cast is the operator's own template, and an integer cast rounds in most dialects. The example README states this hazard. The cast wraps the whole expression, not the source column.
- **Why.** The library owns no SQL dialect, so it cannot make the cast refuse. Rounding the row route to match would hide the problem. What needs correcting is the type the transform produced, so the cast goes around the transform.
- **Status.** `built`.

### §4k.12 Every emitted test fails under a rule the contract declares

- **Decision.** Each singular test carries its rule id in a comment and as a selected column, and the materialization reports how many tests fall under undeclared rules. Key uniqueness always uses the one id `duplicate_key`, whatever the grain. A per-column rule keeps an id derived from its column. A contract written in its own key vocabulary is reconciled through the mapping's rule-id overrides, and an override naming a rule the contract lacks is flagged.
- **Why.** A failure naming an id outside the contract's rules cannot be acted on by a reader of the contract. A grain-derived uniqueness id changed from table to table, so two contracts over the same data could not be read against each other.
- **Status.** `built`.

### §4k.13 The asset of record is the physical platform's dataset URN

- **Decision.** The mapped model is recorded under the physical platform's dataset URN taken from the contract, never a `dagster` platform URN. The raw table's URN and the Dagster asset attach as lineage.
- **Why.** The mapped model is what a governed read is served from, so the catalog and a broker must name the same object.
- **Where else.** [ADR-0002](../../adr/ADR-0002-physical-urn-is-the-catalog-urn.md) holds this decision; §4m.1 of the [§4r, §4l, §4m record](dag-tools-s4r-s4l-s4m.md) is the warehouse source's enforcement of it.
- **Status.** `built`.

### §4k.14 Forward-declared columns are reported, never demanded, and an empty report is still published

- **Decision.** A contract may declare a column before anything produces it. A mapping is not refused for leaving such a column unmapped; the column is listed on the plan and on the materialization. Producing one is noted as a contract edit. The list is published even when empty.
- **Why.** Readers can be written against a column ahead of its producer. An absent key could mean either that nothing is forward-declared or that the plan does not track it, and a reader cannot tell which.
- **Where else.** The shipped example has no forward-declared column left, so the non-empty case is proven on a synthetic contract and on a copy of the example with one column re-opened.
- **Status.** `built`.

### §4k.15 `produced_since` is the one field a dag-tools release changes in the vendored contract

- **Decision.** The field names the dag-tools release whose fixture first carries a column. A fixture gaining a column therefore edits the vendored governed document, and the upstream copy needs the same edit. Nothing else in that file is edited on the dag-tools side.
- **Why.** The proof test requires the plan's output columns to equal the fixture's, so a new fixture column forces the mapping to produce it, and producing a column the contract calls unproduced leaves a note that another test refuses. The chain is enforced by tests, not by convention.
- **Status.** `built`.

### §4k.16 The component file stays inside `type:` and `attributes:` with plain fields

- **Decision.** Component fields are plain dictionaries and scalars validated against sibling models that forbid extra keys. No nested resolvable type is used, environment references are spelled as a call, and the component module has no deferred-annotations import.
- **Why.** The Dagster 1.10.19 floor rejects what these rules avoid. A deferred-annotations import turns a config parameter's annotation into a string, which Dagster then reads as an asset input.
- **Where else.** The census reports running the whole contract mapping module set on the floor with the same result as on the current version and the same plan hash. No recorded run of that is cited.
- **Status.** `built`.

### §4k.17 A scope of `none` means no scope, and only the scope is read that way

- **Decision.** The word `none` in the scope field, in any letter case, and a blank value both mean the contract has no scope. The same reading is not applied to the period column or the status period.
- **Why.** YAML reads `none` as a string, so a document written that way was refused for not producing a column called `none`, which names something nobody wrote. Silently dropping a period a reader filters on would look like a correct answer. The trade is that a contract cannot name a real scope column `none`.
- **Status.** `built`.

### §4k.18 The vendored contract is byte-identical to upstream, and its canary skips when upstream is absent

- **Decision.** The example's contract is the governed upstream document with no added header; provenance lives in the example README. The drift test compares bytes when an environment setting or a sibling checkout points at the upstream copy, and skips otherwise.
- **Why.** A header would break the byte comparison. A canary that passes when it cannot find its target is worse than none, so it says it skipped.
- **Status.** `built`.

## §4j Read audit and the tagged warehouse

The read audit compares a warehouse's query history with what Dagster ran and reports which reads no asset explains. The tagged warehouse resource attaches an attribution tag to every query it sends, so later audits can explain those reads. The last decision in the section concerns the API call plan handler's concurrency cap.

### §4j.1 The component is the primary surface; the CLI is an extra door

- **Decision.** A deployment configures the audit in `component.yaml`. The `dagtools audit` command is a thin wrapper over the same engine for an operator who has an export and no deployment.
- **Why.** The repo's standing rule is that everything is a component configured from YAML. Behaviour the component cannot express must not grow in the CLI.
- **Where else.** [rulings.md](../../rulings.md), 2026-11-04, holds the component-first ruling.
- **Status.** `built`.

### §4j.2 The audit package names no relation, column, level or identity

- **Decision.** The history-column settings have five required fields and no defaults.
- **Why.** Each warehouse spells its query history differently. A wrong default would audit the wrong column and report an empty window as a clean audit.
- **Where else.** [rulings.md](../../rulings.md), 2026-11-04, widened 2026-11-05, holds the wider rule.
- **Status.** `built`.

### §4j.3 The statement is built, never executed, and only identifiers are interpolated

- **Decision.** The history statement builder returns SQL and bound parameters and hands both to a resource. An identifier cannot be bound, so it must match a grammar of one to three unquoted dotted parts and is refused otherwise rather than escaped. Column names go through the same grammar.
- **Why.** Building without executing keeps resolution testable with no database driver installed. Refusing is safer than escaping an untrusted identifier.
- **Status.** `built`.

### §4j.4 Only the tag's principal is tested against the pipeline grammar

- **Decision.** The `pipeline:<name>` grammar is applied to the principal inside an attribution tag, never to the identity on a history row. An unknown principal kind inside a tag still denies.
- **Why.** A warehouse user name never matches the grammar, so testing it marked every untagged read as not a pipeline and left the correlation branch unreachable. The row's identity belongs to the warehouse's namespace; narrowing to a service identity is the operator's principal filter. The tag is the library's own format, so an unknown kind there is a contract break.
- **Status.** `built`.

### §4j.5 The unexplained gap is the deliverable, and it is published broken down

- **Decision.** A read nothing explains is listed as unexplained and also broken into ambiguous, untagged and non-pipeline reads. A read is never attributed to a plausible neighbour; a run that materialized two assets explains neither.
- **Why.** The three causes go to different people: untagged reads close by deploying the tagged resource, ambiguous ones by narrowing a run's selection, and a non-pipeline read is a brokering question rather than an attribution gap.
- **Status.** `built`.

### §4j.6 Coverage is absent on an empty window, and a malformed row is reported

- **Decision.** Coverage is `None` when nothing was read, and the metadata then omits it. A malformed history row is recorded as a row failure rather than dropped, and does not stop the pass.
- **Why.** Zero of zero is not full coverage. Dropping rows under-reports the gap, which is the one direction the audit must not err in.
- **Status.** `built`.

### §4j.7 `fetch_rows(statement, parameters)` is the whole resource seam

- **Decision.** The audit component calls one method on its resource. A resource without it fails with an error naming the method.
- **Why.** One method keeps a driver out of CI and a credential out of the repo, and lets the tagged warehouse resource drop in underneath without changing the component.
- **Status.** `built`.

### §4j.8 The Dagster 1.10.19 floor holds, and the component file is shaped for it

- **Decision.** The component file uses only `type:` and `attributes:`, and every field is a plain dictionary or scalar validated against models in the schema module. A test helper skips with the refusal named if a future version rejects the component file.
- **Why.** The floor rejects an unknown top-level key in a component file, and that rejection fails the whole code location load.
- **Where else.** The census reports that the shipped example loads and materializes on 1.10.19 with the same numbers as on the current version, and that the whole audit suite passes there. No recorded run of that is cited.
- **Status.** `built`.

### §4j.9 No deferred-annotations import in the read audit component module

- **Decision.** The component module does not import deferred annotations.
- **Why.** The asset's config parameter must be a real class at definition time, or Dagster raises an invalid-config error naming the string.
- **Status.** `built`.

### §4j.10 The example is part of the contract

- **Decision.** The read audit example runs with no warehouse and no credentials, using a synthetic export, and the component tests load it the way a deployment would.
- **Why.** Building the example found two defects that no unit test as written could see: the unreachable correlation branch of §4j.4 and the import problem of §4j.9.
- **Status.** `built`.

### §4j.11 The tagged warehouse resource sends a query with its attribution or not at all

- **Decision.** Planning a tagged query refuses an empty tag, and execution re-checks that the plan carries its tag at the one point that touches a connection. The refusal comes before the connection is resolved. The pipeline name stays optional, so a per-request principal is still a reachable configuration.
- **Why.** Two layers guard against a later edit that builds a plan another way. Ordering the refusal first means a misconfigured connection cannot hide a missing principal.
- **Where else.** The census names three tests by earlier names; the Sources table cites the names at the pin.
- **Status.** `built`.

### §4j.12 The resource tags the run, and `for_asset` narrows the tag to an asset

- **Decision.** The resource tags each read with its principal and run id. An asset that wants attribution by asset key calls `for_asset(context)`. A read made for a person uses `for_user` with the identity the gateway verified.
- **Why.** The context a resource is initialized with has no asset key. A run-scoped tag resolves only when the run selected exactly one asset. A test pins the shape of Dagster's own classes, so a version that adds the attribute fails the test rather than leaving a stale workaround.
- **Where else.** The census reports measuring the context's attributes; the pinning test is the standing check.
- **Status.** `built`.

### §4j.13 A tag in a SQL comment is escaped so the driver does not read it as a bind parameter

- **Decision.** The colons inside the generated leading comment are escaped, and nothing else. The escape is removed at compile time, so the warehouse receives the tag unchanged. Session placement is not escaped.
- **Why.** The SQL text layer reads a colon followed by a digit as a bind parameter, and the tag's JSON contains one, so every comment-placed read failed at submission. A unit test that stubs the transport cannot see this; running the example found it.
- **Where else.** The census names three tests by earlier names; the Sources table cites the names at the pin.
- **Status.** `built`.

### §4j.14 The connection has no environment fallback and is resolved at load

- **Decision.** The `connection:` block must name a DSN or a driver name with parts, and it is resolved to a URL during validation. No environment variable stands in for a missing connection.
- **Why.** A fallback variable would belong to whichever pipeline set it last, and a resource that silently connected elsewhere would produce a confident report about the wrong account. Resolving to a URL builds no engine and imports no driver, so the check can run at load and a typo fails the load, not a run.
- **Where else.** The census names one test by an earlier name; the Sources table cites the name at the pin.
- **Status.** `built`.

### §4j.15 The library ships no tag dialect

- **Decision.** Comment placement needs no statement. Session placement runs the operator's own statement, which must bind the tag parameter, be a single statement and carry no comment marker. One transport model checks this at load and at query time.
- **Why.** Shipping a vendor's session statement would name a product in a library that names nothing. A statement that does not bind the tag runs happily and attributes every later read to nothing, so it fails the load.
- **Status.** `built`.

### §4j.16 Environment references are spelled as a call so the floor resolves them

- **Decision.** Every environment reference in the tagged warehouse example uses the call form.
- **Why.** On the floor the attribute form raises an undefined-name error and fails the whole code location load. The current version accepts either form.
- **Where else.** The census reports loading the example on both versions; a test checks the spelling.
- **Status.** `built`.

### §4j.17 Resolving the connection does not import dlt

- **Decision.** The connection URL is built with the SQL toolkit's own URL constructor, not the dlt factory's credential helper. The `connection:` block's shape is unchanged.
- **Why.** dlt ships in the orchestrator extra, not in the core dependencies, so on an install without that extra the resource could not resolve a connection. The test runs in a separate process because the development interpreter may already have dlt imported.
- **Status.** `built`.

### §4j.18 The drop-in is proven end to end

- **Decision.** The tagged warehouse example runs the read audit component unchanged over the tagged resource, on SQLite with no credentials, and the test checks exact per-method counts. The fixture's tag is taken from a plan the shipped resource built.
- **Why.** Exact counts make a change that merges one cause into another show up as a wrong number. Reading the tag from the resource keeps the writer and the reader from drifting.
- **Status.** `built`.

### §4j.19 The concurrency cap waits inside the step, and a permit is held until the request ends

- **Decision.** `api.max_concurrency` bounds requests in flight per worker process and per base URL, so the cap at the API is that number times the worker replicas. The wait for a permit is inside the `ctx.run` action, so a replay never waits. The permit is released when the request's thread finishes, not when the coroutine is cancelled. The action will not start a request it cannot finish within the service's inactivity plus abort timeouts, less the request timeout and a margin; otherwise it raises a retryable error before sending, and a deadline at or below zero is never read as no deadline. The service timeouts are passed to Restate only when set. Each thread keeps one HTTP session, which still honours the private-CA bundle setting. An exhausted retry bound is caught wherever a call runs and recorded as failed, never raised. The plan format version is unchanged, so an older worker ignores the cap.
- **Why.** Waiting outside the step would repeat on every replay. Releasing on cancellation would hand the permit on while the abandoned request is still in flight, and the cap would overshoot. A request in flight when the abort fires is the one that can be duplicated, so the action refuses to start one it cannot finish. The census reports that the SDK version in use rejects the timeout settings during discovery, which would leave the worker serving nothing, so unset settings leave discovery unchanged.
- **Where else.** The census reports these behaviours measured on two Restate server versions and one SDK version; a live regression exists behind an environment gate, with no recorded run cited. The handler's failure recording is §4a (OTel).11 of the [OTel record](dag-tools-s4a-otel.md).
- **Status.** `built`.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Test files are under `dag_tools_tests/`. `cmc` is `dag_tools/components/contract_mapping/component.py`, `cms` is `dag_tools/components/contract_mapping/schema.py`, `con` is `dag_tools/contract_mapping/contract.py`, `emt` is `dag_tools/contract_mapping/emit.py`, `map` is `dag_tools/contract_mapping/mapping.py`, `nrm` is `dag_tools/contract_mapping/normalize.py`, `pln` is `dag_tools/contract_mapping/plan.py`, `acl` is `dag_tools/audit/cli.py`, `ahs` is `dag_tools/audit/history.py`, `ard` is `dag_tools/audit/reads.py`, `atg` is `dag_tools/audit/tag.py`, `awh` is `dag_tools/audit/warehouse.py`, `rac` is `dag_tools/components/read_audit/component.py`, `ras` is `dag_tools/components/read_audit/schema.py`, `twc` is `dag_tools/components/tagged_warehouse/component.py`, `tws` is `dag_tools/components/tagged_warehouse/schema.py`, `rwh` is `dag_tools/resources/warehouse.py`, `acp` is `dag_tools/restate_handlers/api_call_plan.py`. Test abbreviations: `tmc` is `test_contract_mapping_component.py`, `tpl` is `test_contract_mapping_plan.py`, `tme` is `test_contract_mapping_emit.py`, `tpr` is `test_contract_mapping_proof.py`, `tty` is `test_contract_mapping_types.py`, `ttf` is `test_contract_mapping_transforms.py`, `tno` is `test_contract_mapping_normalize.py`, `tac` is `test_audit_cli.py`, `tah` is `test_audit_history.py`, `tar` is `test_audit_reads.py`, `trc` is `test_read_audit_component.py`, `ttc` is `test_tagged_warehouse_component.py`, `ttw` is `test_tagged_warehouse_resource.py`, `toc` is `test_otel_api_sync_concurrency.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4k.1 pure engine; row route proof | con `:28-32`; pln `:25-34`; emt `:21-42`, `:354`, `:437`; nrm `:42-47` | AGENTS.md §4k, bullet 1 | tpr: `test_the_mapping_reproduces_the_fixture_exactly` (`:171`); tno: `test_the_engine_imports_none_of_the_orchestration_stack` (`:352`) | `built` |
| §4k.2 refusals versus notes | pln `:489-560`, `:719-761`, `:822-861` | bullet 2 | tpl: `test_every_problem_is_reported_at_once` (`:302`), `test_a_cadence_disagreement_is_refused_because_it_fails_silently` (`:253`), `test_a_named_but_unlisted_calendar_gets_no_test_and_says_why` (`:474`), `test_no_warehouse_types_means_no_enforced_contract_and_says_so` (`:395`) | `built` |
| §4k.3 no guesses; whole contract or none | map `:426-474`; pln `:690-691` | bullet 3 | tpl: `test_one_missing_warehouse_type_drops_the_contract_rather_than_half_of_it` (`:410`); tme: `test_the_contract_is_enforced_only_when_every_column_has_a_warehouse_type` (`:357`) | `built` |
| §4k.4 file at load; served at run | cmc `:401-429`, `:165-187`, `:430-436`, `:454-462`; cms `:49-60` | bullet 4 | tmc: `test_a_served_contract_is_not_fetched_at_load` (`:380`), `test_a_served_contract_is_fetched_at_materialization` (`:406`), `test_the_expected_contract_id_is_enforced` (`:301`), `test_a_served_contract_with_the_wrong_id_fails_the_run` (`:437`) | `built` |
| §4k.5 bundle selected by expected id | cmc `:100-162`, `:135`, `:212-219`; cms `:62-74` | bullet 5 | tmc: `test_a_contract_bundle_is_selected_from_by_the_expected_id` (`:685`), `test_a_bundle_with_no_expected_id_fails_the_load_listing_the_ids` (`:722`), `test_a_single_document_contract_still_checks_the_id_rather_than_selecting` (`:737`); none found for the empty stream | `built` |
| §4k.6 render then write; prune off | cmc `:322-351`, `:353-369`, `:492-502`; cms `:202-211`; emt `:903-916` | bullet 6 | tmc: `test_nothing_is_written_when_the_render_fails` (`:601`), `test_prune_is_off_by_default_so_a_hand_written_test_survives` (`:543`), `test_prune_removes_only_files_this_component_would_have_written` (`:568`) | `built` |
| §4k.7 type fidelity | emt `:185-218`, `:351` | bullet 7 | tme: `test_integer_text_parses_exactly_past_float_precision` (`:70`), `test_a_numeric_column_keeps_the_sources_written_form` (`:110`), `test_a_non_finite_value_is_refused_and_the_message_names_the_field` (`:81`), `test_scale_renders_the_factor_as_written_not_as_a_float` (`:223`) | `built` |
| §4k.8 timestamp, date, string array | emt `:78`, `:84-136`, `:215`, `:218` | bullet 8 | tty: `test_an_aware_timestamp_is_carried_as_utc` (`:93`), `test_a_naive_timestamp_is_refused_and_the_message_names_the_column` (`:105`), `test_a_naive_timestamp_is_accepted_only_where_the_mapping_declares_it` (`:116`), `test_assume_utc_on_a_non_timestamp_column_fails_the_load` (`:135`), `test_a_number_is_not_a_timestamp` (`:167`), `test_a_date_is_not_a_timestamp_and_a_timestamp_is_not_a_date` (`:181`), `test_a_date_parses_only_the_iso_spelling` (`:201`), `test_a_bare_string_is_not_an_array` (`:214`), `test_a_set_is_not_an_array` (`:246`) | `built` |
| §4k.9 transforms; normalizer document; one instant; row filter | map `:33-47`, `:52-87`, `:358`, `:458`; nrm `:203-264`, `:288`; pln `:638-641` | bullet 9 | ttf: `test_nullif_turns_only_the_sentinel_into_null` (`:120`), `test_in_set_is_type_changing_so_an_uncast_mapping_is_noted` (`:342`), `test_every_row_of_one_call_carries_the_same_instant` (`:843`), `test_a_run_timestamp_column_with_no_sql_expression_fails_the_load` (`:878`), `test_a_filter_drops_the_row_on_both_routes` (`:604`), `test_the_literal_rules_are_the_same_as_in_set` (`:740`); tno: `test_a_mapping_using_normalize_with_no_normalizers_fails_the_load_naming_both` (`:389`), `test_a_path_based_normalizer_document_loads_and_builds` (`:396`) | `built` |
| §4k.10 coalesce counts arguments | map `:40-41`, `:231-249`; pln `:372` | bullet 10 | ttf: `test_one_source_and_a_default_is_two_arguments_and_is_accepted` (`:458`), `test_a_lone_source_with_no_default_is_still_refused_as_a_no_op` (`:473`), `test_concat_is_not_relaxed_by_the_coalesce_change` (`:499`), `test_a_default_the_contract_type_cannot_take_fails_the_load` (`:512`), `test_a_default_the_contract_type_takes_agrees_on_both_routes` (`:527`) | `built` |
| §4k.11 cast rounds; documented | `examples/contract_mapping/README.md:58`, `:130-132` | bullet 11 | tme: `test_a_non_whole_value_into_an_integer_column_is_refused_not_rounded` (`:95`); tpr: `test_the_scaled_columns_carry_a_cast_so_the_contract_can_be_enforced` (`:310`) | `built` |
| §4k.12 tests under declared rules; one key id | pln `:55`; cmc `:554` | bullet 12 | tpr: `test_every_emitted_test_fails_under_a_rule_the_contract_declares` (`:222`); tpl: `test_the_key_uniqueness_id_does_not_change_with_the_grain` (`:530`), `test_a_contract_in_its_own_key_vocabulary_says_so_in_the_mapping` (`:553`), `test_an_override_naming_a_rule_the_contract_lacks_is_still_flagged` (`:586`) | `built` |
| §4k.13 physical URN is the asset of record | con `:135-148`; pln `:879-880`; cmc `:559-562` | bullet 13 | tmc: `test_the_materialization_records_the_physical_platform_as_the_asset_of_record` (`:166`) | `built` |
| §4k.14 forward-declared columns reported; empty published | pln `:206`, `:548-555`; cmc `:512`, `:552` | bullet 14 | tmc: `test_the_examples_forward_edge_report_is_published_EMPTY_not_omitted` (`:184`), `test_a_forward_declared_column_is_reported_by_NAME_rather_than_hiding_it` (`:201`); tpr: `test_the_example_produces_every_column_its_contract_declares` (`:253`), `test_every_column_the_fixture_carries_is_recorded_as_produced` (`:267`); tpl: `test_producing_a_forward_declared_column_is_noted_as_a_contract_edit` (`:362`) | `built` |
| §4k.15 `produced_since` moves with the fixture | con `:80`, `:106-108`, `:262` | bullet 15 | tpr: `test_the_example_mapping_leaves_the_plan_with_nothing_to_report` (`:243`), `test_the_example_produces_every_column_its_contract_declares` (`:253`) | `built` |
| §4k.16 plain fields; no deferred annotations | cmc `:36`; cms `:40`, `:96`, `:131`, `:175`, `:231` | bullet 16 | the census reports a floor run; no recorded run cited | `built` |
| §4k.17 `none` scope means no scope | con `:61`, `:235` | bullet 17 | tpl: `test_a_scope_of_none_is_no_scope_rather_than_a_column_called_none` (`:199`), `test_an_unproduced_scope_column_is_refused` (`:184`), `test_only_the_scope_is_read_that_way` (`:227`), `test_a_blank_scope_is_the_same_absence_as_an_absent_one` (`:240`), `test_a_real_scope_column_is_still_read_as_itself` (`:248`) | `built` |
| §4k.18 vendored byte-identical; canary skips | `examples/contract_mapping/contracts/ev_fixture.yaml`; `examples/contract_mapping/README.md` | bullet 18 | tpr: `test_the_vendored_contract_has_not_drifted_from_upstream` (`:355`) | `built` |
| §4j.1 component first; CLI a wrapper | acl `:77`, `:111`, `:153`, `:178-179`; rac `:50-56` | AGENTS.md §4j, bullet 1 | tac: `test_the_audit_app_is_reachable_from_the_console_script` (`:39`), `test_the_audit_package_imports_neither_dagster_nor_the_qual_package` (`:187`) | `built` |
| §4j.2 no names, no defaults | ras `:25-109` | bullet 2 | tac: `test_nothing_in_the_audit_package_names_an_object` (`:208`); trc: `test_reading_through_a_resource_demands_every_column_name` (`:316`) | `built` |
| §4j.3 built not executed; identifiers checked | ahs `:41-42`, `:92-148` | bullet 3 | tah: `test_every_value_is_bound_and_none_is_interpolated` (`:60`), `test_anything_that_is_not_an_identifier_is_refused_not_escaped` (`:119`), `test_a_column_name_goes_through_the_same_grammar` (`:124`); trc: `test_the_component_executes_nothing_itself` (`:228`) | `built` |
| §4j.4 only the tag's principal is tested | atg `:123-172`; ard `:170-213` | bullet 4 | tar: `test_an_untagged_read_correlates_through_one_single_asset_window` (`:100`), `test_an_opaque_row_identity_is_not_evidence_either_way` (`:159`), `test_an_unknown_principal_kind_in_a_tag_is_not_a_pipeline` (`:152`) | `built` |
| §4j.5 gap decomposed; no neighbour attribution | ard `:145-151` | bullet 5 | tar: `test_a_multi_asset_run_cannot_attribute` (`:84`), `test_the_gap_contains_every_read_no_asset_explains` (`:172`); trc: `test_the_metadata_decomposes_the_gap` (`:138`) | `built` |
| §4j.6 coverage absent on empty; failed rows kept | ard `:276-278`; ahs `:189-225` | bullet 6 | tar: `test_coverage_is_none_when_nothing_was_read` (`:215`); tah: `test_one_malformed_row_is_reported_not_swallowed_and_not_fatal` (`:190`) | `built` |
| §4j.7 one-method resource seam | rac `:170-233` | bullet 7 | trc: `test_a_resource_without_fetch_rows_fails_with_a_pointed_error` (`:287`) | `built` |
| §4j.8 floor shape | ras `:40`, `:120`, `:165`; rac `:299-310` | bullet 8 | trc: `test_the_component_file_uses_no_top_level_key_beyond_type_and_attributes` (`:386`), `test_every_component_field_is_a_shape_the_floor_resolves` (`:403`), skip gate `:44`; the census reports a floor run; no recorded run cited | `built` |
| §4j.9 no deferred annotations | rac `:29` | bullet 9 | trc: `test_every_component_field_is_a_shape_the_floor_resolves` (`:403`) | `built` |
| §4j.10 example is part of the contract | `examples/read_audit/`; trc `:79` | bullet 10 | trc: `test_the_metadata_decomposes_the_gap` (`:138`) | `built` |
| §4j.11 tag or no send | awh `:190-237` | bullet 11 | ttw: `test_a_query_with_no_principal_is_refused_and_nothing_is_sent` (`:75`), `test_an_untagged_plan_is_refused_at_the_point_of_execution` (`:94`), `test_the_resource_refuses_before_resolving_a_connection` (`:112`); census names `test_a_query_without_a_principal_is_refused_not_sent`, `test_execute_plan_refuses_an_untagged_plan`, `test_the_refusal_happens_before_any_connection_is_resolved` | `built` |
| §4j.12 run tag; `for_asset`; `for_user` | rwh `:282`, `:352-414`, `:383`, `:393` | bullet 12 | ttw: `test_for_asset_sharpens_a_run_scoped_tag_to_the_asset_key` (`:145`), `test_for_user_attributes_the_read_to_the_verified_caller` (`:177`), `test_the_fixtures_match_what_dagster_offers` (`:518`) | `built` |
| §4j.13 comment tag escaped | rwh `:156-182` | bullet 13 | ttw: `test_a_comment_placed_tag_survives_sqlalchemy_text` (`:231`), `test_the_escape_does_not_reach_the_warehouse` (`:251`), `test_the_escape_leaves_the_operators_own_statement_alone` (`:268`), `test_the_escape_is_not_applied_to_session_placement` (`:281`); census names `test_a_comment_placed_tag_round_trips_through_sqlite`, `test_the_escape_never_reaches_the_warehouse`, `test_the_escape_leaves_the_callers_own_parameters_alone` | `built` |
| §4j.14 connection resolved at load; no fallback | rwh `:77-132`; tws `:78-105` | bullet 14 | ttc: `test_a_connection_naming_nothing_fails_the_load` (`:218`); ttw: `test_a_connection_naming_nothing_is_refused_with_no_env_fallback` (`:449`); census names `test_no_environment_variable_stands_in_for_a_connection` | `built` |
| §4j.15 no tag dialect | awh `:71-120`, `:171-187` | bullet 15 | ttc: `test_a_tag_statement_without_the_bound_tag_fails_the_load_not_the_run` (`:194`) | `built` |
| §4j.16 call-form environment references | `examples/tagged_warehouse/` | bullet 16 | ttc: `test_the_example_names_env_vars_the_way_the_floor_resolves_them` (`:288`), `test_the_component_file_uses_no_top_level_key_beyond_type_and_attributes` (`:275`), `test_every_component_field_is_a_shape_the_floor_resolves` (`:307`) | `built` |
| §4j.17 no dlt import | rwh `:101` | bullet 17 | ttw: `test_resolving_a_connection_does_not_import_dlt` (`:411`) | `built` |
| §4j.18 drop-in end to end | twc `:54-86`; `examples/tagged_warehouse/` | bullet 18 | ttc: `test_the_resource_drops_in_under_an_unchanged_read_audit` (`:125`), `test_the_tag_in_the_fixture_is_the_tag_the_resource_sends` (`:159`) | `built` |
| §4j.19 concurrency cap | acp `:68-69`, `:103-106`, `:192`, `:206`, `:293-320`, `:373-421`, `:513`, `:599`, `:644`, `:716` | bullet 19 | toc: `test_cap_bounds_calls_in_flight_across_groups` (`:63`), `test_replay_does_not_wait_for_a_permit` (`:86`), `test_cap_is_per_base_url` (`:106`), `test_a_cancelled_action_keeps_its_permit_until_the_request_finishes` (`:123`), `test_permit_wait_past_the_deadline_raises_retryable_before_sending` (`:210`), `test_permit_deadline_reads_the_worker_timeout_env` (`:241`), `test_exhausted_retry_bound_halts_and_is_recorded_not_raised_primary` (`:286`), `test_service_timeouts_unset_leave_discovery_unchanged` (`:346`), `test_only_the_set_timeout_is_passed` (`:368`), `test_one_session_per_thread_is_reused` (`:377`), `test_session_requests_still_honour_REQUESTS_CA_BUNDLE` (`:391`); live: `test_restate_concurrency_e2e.py`, gated, no recorded run cited | `built` |
