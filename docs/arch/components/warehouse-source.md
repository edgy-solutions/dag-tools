# Warehouse source (CO-WAREHOUSE-SOURCE)

The warehouse source component describes one classified source dataset and observes it. It builds one Dagster observable source asset. Materializing the asset counts the rows, names the columns and records an as-of. It then publishes the dataset, with a classification label on every column, to the data catalog. It moves no data.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the dag-tools pin listed there (`503607d`). The record shape is provisional.

## Findings

1. **It builds one observable source asset and nothing else.** There is no asset check, sensor, schedule or job. The asset carries the dataset address, access mode, owner and label origin as definition metadata. A job that observes it must name its key, because an "all assets" selection skips source assets and would run no step. Tests observe it through a named selection. `built`
2. **The asset takes no run config; every setting is a YAML attribute.** Dagster calls an observe function with resources only, so a config parameter would fail at run time. A test pins that the observe function has no config parameter. `built`
3. **The asset of record is the dataset's own physical-platform address, and a Dagster-platform address is refused at load.** The component also checks at load that the source names a relation, an access mode from a fixed list (`brokered`, `passthrough`), a label origin from a fixed list (`board`, `catalog_tag`) and an owner. It refuses a column labelled twice and an empty label. It renders the observation statements at load, so a bad statement fails the load and not a run. `built`
4. **The classification document is read from a mounted file, at load when present and always at run.** The path is the explicit `dimensions_file` setting, else an environment variable, else a default mount path. If the file is absent at load, the component loads and validates nothing. At run an absent file fails the run and names the path. A document that declares no dimensions refuses every label. A dimension with no values fails the run. With `require_ratified` on, a value with no ratifier also fails the run. A label naming a dimension or value the document does not declare fails the load. dag-tools ships no such document. `built`
5. **The labels are validated against the document, and an unlabelled column is published as most restrictive.** The label on a column must carry the one hierarchical dimension, with one value. Every observed column is published. One with no declared label gets the most restrictive label and is counted in the metadata and warned about. A labelled column the source no longer has fails the run. Two source columns that differ only by case fail the run. `built`
6. **The observation reads through a resource named by key, and any resource with `fetch_rows` and `fetch_columns` is accepted.** The key defaults to `warehouse`. The component does not define the resource. The tagged warehouse resource is the one dag-tools ships with both methods, and the shipped example binds it. The component does not check the resource's class. A missing resource or a missing `fetch_columns` fails the run with a message that says so. `built`
7. **The observation sends the operator's statements and reads no row of the source.** Defaults are a count and a projection that matches no rows. The column names come from a separate `fetch_columns` call because an empty result has no rows to read names from. A count must be one row with one value. An as-of must be one non-null value, kept as written. A relation containing a statement separator or comment marker fails the load. The data version changes when the schema, the count or the as-of changes. With no as-of statement, the as-of is the observation time, so the data version changes on every observation even when the schema and count do not. `built`
8. **It connects directly, with no sign-in of its own and no use of the gateway or broker.** The warehouse credential belongs to the resource, which takes a connection string or parts from its own configuration. The tagged resource tags each query with a pipeline principal so the read shows in query history. The component presents no identity to the warehouse beyond that tag. The code does not call the [central gateway](central-gateway.md) or the [domain broker](domain-broker.md); the broker appears only in the catalog metadata the component writes. `built`
9. **It publishes the dataset to the catalog with a token read from config, else an environment variable.** It sends dataset properties, a status, a table subtype and one glossary term per label value on each column. It also sends, by default, a lineage edge from the physical dataset to a Dagster-platform node for the asset. It sends no schema aspect because only column names were read. The token is `catalog.token`, else `DATAHUB_TOKEN`. An empty token is never sent. With no `server` set, nothing is emitted. `built`
10. **A catalog failure fails the materialization, and a dry run emits nothing.** The sends are not caught. `dry_run` records the whole label set in the asset's metadata and sends nothing. A test pins the failure and the empty-token case. A test pins the dry run. `built`
11. **The label set published records which document ratified it.** The properties carry the document's source path and a short digest of its bytes, the access mode, the owner and the label origin. The same facts appear in the observation metadata. The per-column `cell_predicate` is published as written and never interpreted. `built`

## Where it exists and what its example proves

Commits are named in full. A release is a version tag whose release page carries the built wheel. The wheel holds the component and not its example, which stays in the repository under `examples/`. Each `measured` row cites the Tests workflow run 44451597250 at the records pin. Every test such a row names ran in process and passed in both test jobs: the current Dagster and the 1.10.19 compatibility floor. A row saying what the example does not prove, or what a deployment must still configure, describes a condition of the code and carries that condition's status. A value a deployment must configure is required or has no default, and neither the chart nor the shipped code location supplies it. Supplying it is the deployment's job, not a remedy, so no roadmap row follows from it.

### Commit and release

| # | Claim | Evidence | Status |
|---|---|---|---|
| 1 | The component and its example were first committed at `50f11c86e93232f47500b21631873da23288b58a`. | the commit that adds `dag_tools/components/warehouse_source/` and `examples/warehouse_source/` | `built` |
| 2 | Merged to master by PR #24, merge commit `dfc6b09ebb9965401a69ef184c930a20817f30a5`, on 2026-11-07 (UTC). | the pull request's merge commit; master contains it | `built` |
| 3 | First released in v0.12.0, tag at `1b149e2135aef9b8349d44c09442a6f734dd3873`, published 2026-11-09. Also in v0.17.0, tag at `a85c5f99233962a8c34e6eb0f399c23030865ee4`, the most recent release, and at the records pin. | each tag contains the merge commit; the component's module is in each release's wheel | `built` |
| 4 | A component file names it as `type: dag_tools.WarehouseSourceComponent`; the package root exports the class. | `dag_tools/__init__.py:13, :33` | `built` |

### What the example proves, and what it does not

| # | Claim | Evidence | Status |
|---|---|---|---|
| 1 | The example loads as a code location with one observable source asset, read through a tagged warehouse resource (CO-TAGGED-WAREHOUSE). | Tests run 44451597250, both test jobs; `test_warehouse_source_component.py`: `test_the_shipped_example_loads_and_builds_one_source_asset` | `measured` |
| 2 | Observing the source counts its rows and names its columns without moving a row. It records an as-of that is not the observation time, and names the document and digest that ratified the labels. | Tests run 44451597250, both test jobs; `test_warehouse_source_component.py`: `test_observing_the_example_counts_and_names_without_moving_a_row` | `measured` |
| 3 | A column the example leaves unlabelled is published at the strictest value in every dimension, and counted. | Tests run 44451597250, both test jobs; `test_warehouse_source_component.py`: `test_the_unlabelled_column_is_published_most_restrictive_and_counted` | `measured` |
| 4 | A value with no ratifier is reported rather than refused. With `require_ratified` on, the publish is refused and names the values. | Tests run 44451597250, both test jobs; `test_warehouse_source_component.py`: `test_an_unratified_value_is_reported_rather_than_refused`, `test_require_ratified_refuses_the_publish_and_names_the_values` | `measured` |
| 5 | A label the document does not declare, and an asset of record on the Dagster platform, each fail the load. The observation takes no run config. | Tests run 44451597250, both test jobs; `test_warehouse_source_component.py`: `test_a_label_the_document_does_not_declare_fails_the_LOAD`, `test_a_dagster_platform_urn_fails_the_load`, `test_the_observe_function_takes_no_run_config` | `measured` |
| 6 | Not proved: a send to a catalog. The example names no catalog server, so it emits nothing. The send's own tests replace the catalog client with a stand-in. | Tests run 44451597250, both test jobs; `test_warehouse_source_component.py`: `test_the_example_emits_nothing_because_it_names_no_catalog`, `test_a_catalog_failure_FAILS_the_materialization`; no test sends to a running catalog | `built` |

### What a deployment must still configure

| # | A deployment must still configure | What the example uses instead | Evidence | Status |
|---|---|---|---|---|
| 1 | The source itself: the dataset's URN on its physical platform, its relation, owner, access and label origin. | a dataset in a local sqlite file | Settings `source`; `examples/warehouse_source/dagster_home/components/source/component.yaml:15` | `built` |
| 2 | A label on each column, from values the policy's dimensions document declares. An unlisted column takes the strictest value. | labels on all but one column | Settings `source.columns`; `examples/warehouse_source/dagster_home/components/source/component.yaml:43` | `built` |
| 3 | A bound resource with a real connection, normally a tagged warehouse resource. | a tagged warehouse resource over a local sqlite file | Settings `resource_key`; `examples/warehouse_source/dagster_home/components/source/component.yaml:121` | `built` |
| 4 | The policy's dimensions document, mounted where the setting or its environment variable names. dag-tools ships none. | a stand-in document beside the example | Settings `classification.dimensions_file`; `examples/warehouse_source/classification/dimensions.yaml` | `built` |
| 5 | The catalog server and its token. | nothing; no server is named, so nothing is emitted | Settings `catalog.*`; `examples/warehouse_source/dagster_home/components/source/component.yaml:102` | `built` |
| 6 | A schedule or job that names the asset's key. A selection of all assets does not include a source asset. | none | Settings; AGENTS.md §4m | `built` |

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `source` | required | the dataset: physical-platform address, relation, `access`, `owner`, `label_origin`, labelled `columns` |
| `source.columns` | empty list | per-column labels `{dimension: value(s)}` and an optional `cell_predicate`; an unlisted column is most restrictive |
| `asset_name` | required | name of the asset |
| `key_prefix` | empty | asset key prefix |
| `resource_key` | `warehouse` | key of the resource the observation reads through |
| `group_name` / `tags` | unset / empty | Dagster grouping and tags |
| `dry_run` | `false` | observe and record the label set; emit nothing |
| `observation.count_rows` | `true` | whether to count rows |
| `observation.row_count_statement` | a count of the relation | your own count statement with a `{table}` placeholder |
| `observation.columns_statement` | a projection matching no rows | your own schema statement with `{table}` |
| `observation.as_of_statement` | unset | your own freshness statement; without it the as-of is the observation time |
| `classification.dimensions_file` | unset (environment variable, then a default mount path) | path to the dimensions document |
| `classification.require_ratified` | `false` | fail the run while a value has no ratifier |
| `catalog.server` | unset | catalog URL; unset means nothing is emitted |
| `catalog.token` | unset (falls back to `DATAHUB_TOKEN`) | catalog token |
| `catalog.description` | unset | description published on the dataset |
| `catalog.emit_lineage_from_asset` | `true` | draw the edge from the dataset to the asset |
| `DAGTOOLS_CLASSIFICATION_LEVELS` (environment) | unset | names the dimensions document when the setting is unset |

## Gaps

- **No test drives a real catalog client.** Emission tests record or monkeypatch the sends (finding 10); the shipped example names no catalog. What a live catalog does with the aspects is not shown. Remedy: not on the roadmap. Status: `built`.
- **The resource is not checked to be the tagged one.** Any object with the two methods is accepted (finding 6), so a resource that sends untagged reads would pass, and the read would not be attributed to the asset. Remedy: not on the roadmap. Status: `built`.
- **Three texts describe a broker the code does not have.** The example's README and its component file say that if the label set does not land, a broker finds no labels on the dataset and denies every read of it; the `emit_payload` docstring says the same. At the pin no broker or gateway code reads a label, and the read decision asks for `can_read` on relations alone, so a dataset whose labels never reached the catalog is decided as one whose labels did ([label propagation](../flows/label-propagation.md), findings 9 and 10). Remedy: [roadmap row 18](../ROADMAP.md), for a decision that reads labels. Status: `built`.

## Not established from code

- Which resource a deployment binds, which warehouse it reaches, and with which credential.
- Whether a deployment sets a catalog server and token, or runs the catalog with authentication.
- Whether a deployment mounts the dimensions document, and what it declares.
- How a deployment schedules the observation. The component ships no schedule.
- What the catalog does with an earlier publish. Each publish sends the editable schema aspect whole, carrying only the current terms, and only for columns that have terms (`dag_tools/components/warehouse_source/component.py:236-255`). Whether that drops an earlier, stricter term, or a person's edit on the same aspect, is the catalog's behaviour and was not read.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-DAGSTER | is called by | in-process | none | a run of the observable source asset | `dag_tools/components/warehouse_source/component.py:347-368`; no test | `built` |
| CO-TAGGED-WAREHOUSE | calls | in-process | none from the component; the resource holds its own | count and schema reads; any resource with the two read methods is accepted | `dag_tools/components/warehouse_source/component.py:390-405`; no test | `built` |
| CO-DATAHUB | calls | HTTP | token from config, else from the environment; none sent when empty | dataset metadata and per-column label terms | `dag_tools/components/warehouse_source/component.py:169-188, :191-215, :427-443`; `test_an_empty_token_is_never_sent_as_a_credential`, `test_a_catalog_failure_FAILS_the_materialization` | `built` |
| XE-CLASSIFICATION-DOC | reads | file read | none | the dimensions and values labels are validated against | `dag_tools/components/warehouse_source/component.py:139-166`; `test_an_absent_document_loads_and_then_fails_the_run_naming_the_path` | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | records what a source dataset is, how big and how fresh, and what each column's classification label is, and publishes that to the catalog | — |
| How the mesh wires it | a YAML component; the warehouse resource is bound by key; the label set is validated against a mounted document | — |
| Sign-in | none to the catalog beyond a token; none to the warehouse beyond the resource's own credential and query tag | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct to the warehouse through the resource and direct to the catalog; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | `operational-data`: where a producer declares and labels its own source | [NATO L2](../alignment/nato-dcra.md) |
| Security role | publishes the labels a later access decision could read; it makes no access decision itself | [NATO R3](../alignment/nato-dcra.md) |
| Services served | the catalog entry and per-column labels for a source dataset, and the catalog's record of the observing asset | [DoD S3](../alignment/dod-dmra.md), [S4](../alignment/dod-dmra.md), [S12](../alignment/dod-dmra.md) |

## Sources

All paths are at `503607dd321cd6627509bfc93955357b8566597f`. Component paths are under `dag_tools/components/warehouse_source/`; engine paths under `dag_tools/warehouse_source/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| One observable source asset; no check, sensor, job | `component.py:347-368`, `:534-536` | AGENTS.md §4m | `test_warehouse_source_component.py`: `test_the_shipped_example_loads_and_builds_one_source_asset`, `test_the_definition_carries_the_urn_and_the_access_mode`; no test that a sensor or check is absent | `built` |
| No run config | `component.py:369` | AGENTS.md §4m | `test_the_observe_function_takes_no_run_config` | `built` |
| Load-time refusals | `schema.py:158-175`; `spec.py:170-202`; `observe.py:120-130` | AGENTS.md §4l, §4m | `test_a_dagster_platform_urn_fails_the_load`, `test_an_unrenderable_statement_fails_the_load_not_the_run`, `test_one_column_cannot_carry_two_labels`, `test_an_empty_label_is_refused_so_it_is_never_read_as_deny_by_default`, `test_a_relation_carrying_a_statement_separator_is_refused_at_load` | `built` |
| Document path, load and run reads, run refusals | `component.py:119-166`, `:370-387`, `:530-532`; `dimensions.py:43-48`, `:639-655` | AGENTS.md §4l, §4m | `test_an_absent_document_loads_and_then_fails_the_run_naming_the_path`, `test_a_label_the_document_does_not_declare_fails_the_LOAD`, `test_require_ratified_refuses_the_publish_and_names_the_values`, `test_explicit_configuration_beats_the_ambient_variable`, `test_an_empty_document_is_unusable_rather_than_permissive` | `built` |
| Label validation, defaulting, stale and case-fold refusals | `spec.py:241-332`; `dimensions.py:569-583` | AGENTS.md §4l, §4m | `test_a_label_missing_the_ladder_is_refused_not_defaulted`, `test_an_unlabelled_column_is_published_most_restrictive`, `test_a_labelled_column_the_source_no_longer_has_fails_loudly`, `test_columns_differing_only_by_case_are_refused` | `built` |
| Resource by key; `fetch_columns` required; class not checked | `component.py:390-405`; `schema.py:151-152`; `dag_tools/resources/warehouse.py:430-445` | AGENTS.md §4m | `test_a_resource_without_the_columns_seam_is_refused_with_its_reason`, `test_the_component_requires_the_resource_key_it_was_given`; no test that a non-tagged resource is refused | `built` |
| Statements and observation reading | `observe.py:27-32`, `:88-94`, `:107-148`, `:162-251`; `component.py:405-416` | AGENTS.md §4m | `test_the_default_statements_move_nothing`, `test_a_grouped_count_is_refused_rather_than_summed`, `test_an_absent_as_of_is_refused_rather_than_recorded_as_empty`, `test_the_data_version_moves_when_only_the_as_of_moves`; `test_tagged_warehouse_resource.py`: `test_an_empty_result_set_still_names_its_columns`, `test_a_schema_read_still_has_to_carry_its_tag` | `built` |
| Direct connection; no gateway or broker call; tag only | `component.py:390-412`; `dag_tools/resources/warehouse.py:300-326`, `:447-462` | AGENTS.md §4j, §4m | no test of absence; code searched for gateway and broker calls, none found | `built` |
| Catalog emission, token source, no schema aspect | `component.py:169-188`, `:191-274`; `catalog.py:93-159` | AGENTS.md §4i, §4m | `test_no_schema_aspect_is_emitted_because_only_names_were_read`, `test_the_lineage_edge_runs_FROM_the_physical_dataset_TO_the_asset`, `test_every_term_on_a_column_reaches_the_field_it_labels`, `test_an_empty_token_is_never_sent_as_a_credential`, `test_the_lineage_edge_can_be_turned_off_and_nothing_else_changes` | `built` |
| Catalog failure fails the run; dry run | `component.py:277-293`, `:427-443` | AGENTS.md §4m | `test_a_catalog_failure_FAILS_the_materialization`, `test_dry_run_records_everything_and_emits_nothing` | `built` |
| Published record of the ratifying document | `catalog.py:122-147`; `component.py:296-327` | AGENTS.md §4m | `test_the_published_label_set_says_which_document_ratified_it`, `test_a_cell_predicate_is_published_and_never_interpreted` | `built` |
| The broker-denies texts; no label is read by the decision | `examples/warehouse_source/README.md:113-116`; `examples/warehouse_source/dagster_home/components/source/component.yaml:111-112`; `component.py:285-286`; dag-tools `dag_tools/central_gateway/main.py:238-243`, `:320`; invincible-agent `helm/invincible-agent/templates/topaz-configmap.yaml:164`, `:190` | not stated | — | `built` |
