# Classification propagation (CO-CLASSIFICATION-PROPAGATION)

Classification propagation labels the columns of derived datasets. A source dataset is labelled by a person; a derived column's label is a consequence of the source columns it read, so nobody decides it. The component builds one Dagster sensor. For each derived dataset it names, the sensor composes a label for every output column from the published labels of the declared sources, and writes the result to the catalog and to the derived asset's own metadata. It builds no asset, launches no run, and reads no warehouse.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It builds one sensor and nothing else.** `build_defs` returns a `Definitions` holding a single sensor. It builds no asset, check, job or schedule. The sensor launches no runs; each tick ends in a `SkipReason` that reports what it did. The derived asset belongs to whatever builds it (dbt, dlt or a Python asset). Test: `test_the_shipped_example_builds_one_sensor_and_materializes_nothing`. `built`
2. **The sensor is stopped by default.** `default_status` defaults to `STOPPED`, and the schema accepts only `RUNNING` or `STOPPED`, so a typo fails the load instead of choosing one. `minimum_interval_seconds` is unset by default and must be at least 1 when set. Tests: `test_the_sensor_is_stopped_by_default_and_launches_no_runs`, `test_an_unknown_sensor_status_is_refused_rather_than_defaulted`. `built`
3. **The sources are declared in YAML, never read off the asset graph.** Each dataset must list at least one source, may not list itself, and may not list one twice. A column whose lineage reads an asset that is not in the list fails that dataset, as does a listed source with no published label set or no event at all. Two dataset blocks for one asset are refused. Tests: `test_lineage_naming_an_undeclared_source_is_refused`, `test_a_declared_source_that_never_materialized_fails`, `test_a_source_publishing_no_label_set_fails_rather_than_counting_as_nothing`, `test_a_dataset_block_that_cannot_propagate_is_refused`. `built`
4. **A source's labels are read from its newest observation or materialization; the derived side reads materializations only.** A labelled source is an observable source asset, so it emits observations and never materializations. The sensor takes the newest event across both logs for each source. For the derived asset it reads materializations only, because the sensor writes its own observation there and counting it would republish on every tick. Test: `test_the_shipped_example_propagates_from_a_source_it_only_OBSERVED`. `built`
5. **Each dimension composes by its own rule, from one order.** The result for a column is the narrowest label that admits only readers every input label admits. On the hierarchical dimension that is the highest value. On a restrictive-set or exclusionary dimension it is the union of the values. On a permissive-set dimension it is the intersection. A dimension no input declares takes its least restrictive value. Composing no labels at all is refused, and so is composing against a document in which any dimension has no values. Tests: `test_a_permissive_set_composes_by_INTERSECTION_not_union`, `test_composition_is_the_join_of_one_order_in_every_type`, `test_composing_no_labels_at_all_is_refused`, `test_composition_against_an_unusable_document_is_refused`. `built`
6. **A column with no lineage entry carries the table maximum, and an unlabelled source column fails.** If a column has no lineage, it gets the composition of every labelled column of every declared source. The record names those columns as `table_max_columns`, and marks each decision with its basis (`column_lineage` or `table_max`). A lineage reference to a source column with no label is an error, not a skip. A case-only difference between two source names or two column names resolves when it matches one, and is refused when it matches several. An asset with neither a declared schema nor lineage is refused. Tests: `test_a_column_with_no_declared_lineage_falls_back_to_the_table_max`, `test_an_unlabelled_source_column_fails_rather_than_counting_as_unrestricted`, `test_source_columns_differing_only_by_case_are_refused`, `test_an_asset_naming_no_columns_is_refused_not_propagated_over_nothing`. `built`
7. **A downgrade is the only way to widen a column, and each one names an owner, a reason and an id.** A rule must carry all three, name at least one column, and carry a label with the hierarchical dimension. A rule is refused if it names a column the asset does not produce, if two rules reach one column, if it makes any dimension more restrictive, or if it leaves the derived label unchanged. A dimension the rule omits keeps the derived value, and an empty set clears that dimension. Each application records the rule, the owner, the reason and both labels. Tests: `test_an_applied_downgrade_records_the_rule_owner_reason_and_both_labels`, `test_a_rule_that_tightens_a_dimension_is_not_a_downgrade`, `test_a_rule_says_nothing_about_the_dimensions_it_omits`, `test_a_rule_can_clear_a_set_dimension_and_not_only_lower_the_ladder`, `test_a_downgrade_rule_nobody_is_accountable_for_is_refused`. `built`
8. **The catalog receives exactly two patch aspects, and the patch removes the other terms of the vocabulary.** One patch adds custom properties to the dataset's properties (origin `propagated`, the document, the sources, counts, the asset key, and the dimensions document's digest and source when it has them). The other patches each column's glossary terms: it adds the terms the label publishes and removes every other term in the dimensions document. Terms outside that document are left alone, so a crawler's terms and a person's tags stay. Tests: `test_both_aspects_are_patched_so_what_else_describes_the_dataset_survives`, `test_the_terms_patch_removes_the_rest_of_the_vocabulary`, `test_nothing_outside_the_documents_vocabulary_is_touched`. `built`
9. **The dimensions document is read on every tick, and an absent document fails the tick, not the load.** The sensor re-reads the document each time it runs. If the file is absent, the tick fails with an error that names the path. At load the component reads the document only if it is there, and then checks every downgrade label against it. An absent file at load is not an error. With `require_ratified` on, the tick fails while any declared value has no `ratified_by`. Tests: `test_the_document_is_read_at_every_tick_not_once_at_load`, `test_the_code_location_loads_with_no_document_mounted`, `test_a_declared_downgrade_is_validated_against_the_document_at_load`, `test_require_ratified_fails_the_tick_rather_than_publishing`. `built`
10. **One dataset's failure does not stop the others, and a failing dataset is retried without a new materialization.** Each dataset runs in its own `try`. A failure is logged, and recorded once per materialization as an observation on the derived asset with `classification_propagated` false and the error text. The cursor keeps a storage id and a status per dataset. A dataset is skipped only when its newest materialization has already been published. A cursor that cannot be read is reset. Tests: `test_one_dataset_failing_does_not_stop_the_others`, `test_a_failure_is_recorded_against_the_asset_rather_than_only_logged`, `test_a_failing_dataset_is_retried_without_a_re_materialization`, `test_an_unchanged_materialization_is_not_republished`, `test_an_unreadable_cursor_is_reset_rather_than_crashed_on`. `built`
11. **It connects straight to the catalog with a token from config or the environment, and a catalog failure fails the dataset.** With no catalog server set, nothing is sent and every decision is still recorded. With a server set and `dry_run` off, it opens a catalog client and sends the patches; a send error fails that dataset. The token is the configured one, else the standard catalog token variable; an empty value is not sent. It presents no other identity, and the central gateway and the domain broker are not on this path. With `dry_run` on, nothing reaches the catalog even when a server is set. Tests: `test_dry_run_records_the_whole_decision_and_emits_nothing`, `test_with_a_catalog_configured_the_tick_emits_what_it_recorded`, `test_emit_sends_every_proposal_and_counts_them`. No test covers the token resolution. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `datasets` | required, at least one | the derived datasets to label |
| `datasets[].asset_key` | required | the derived asset, key parts joined by `/` |
| `datasets[].dataset_urn` | required | the physical dataset's URN; a URN on the Dagster platform is refused |
| `datasets[].sources` | required, at least one | the upstream assets whose published labels are composed |
| `datasets[].description` | unset | a description for the dataset; the schema accepts it and the patch code at the pin does not use it |
| `datasets[].downgrades` | empty | standing downgrade rules, each with `id`, `owner`, `reason`, `columns`, `label`, all required |
| `classification.dimensions_file` | unset: a default mount path, or the environment variable that names one | where the dimensions document is read from |
| `classification.require_ratified` | `false` | fail the tick while any declared value has no `ratified_by` |
| `catalog.server` | unset: nothing is sent | the catalog's metadata service address |
| `catalog.token` | unset: the standard catalog token variable | the catalog token |
| `sensor_name` | `classification_propagation` | the sensor's name |
| `minimum_interval_seconds` | unset | the shortest gap between ticks |
| `default_status` | `STOPPED` | `RUNNING` or `STOPPED` |
| `dry_run` | `false` | compose and record, send nothing |

## Gaps

- **Catalog token resolution has no test** (finding 11). The code takes the configured token, else the environment variable, and sends none when both are empty. No test at the pin exercises that choice. Remedy: not on the roadmap.

## Not established from code

- Whether anything uses these labels to decide a read. This component publishes them as catalog terms and as asset metadata; it makes no claim that a gateway, broker or policy reads them.
- Chained propagation. AGENTS.md §4n says a derived dataset cannot be another's source. No check or test for that was found at the pin.
- Which sources, derived assets and downgrade rules any deployment declares.
- Whether the catalog runs with token authentication on. The code sends a token when it has one.
- How the derived asset's builder attaches column lineage and a column schema. The sensor reads both from the asset's own metadata and fails a dataset that has neither.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-DAGSTER | is called by | in-process | none | a sensor tick | `dag_tools/components/classification_propagation/component.py:529-541`; no test | `built` |
| CO-DAGSTER | reads | in-process Dagster instance API | none | materializations and observations from the event log | `dag_tools/components/classification_propagation/component.py:482-526`; no test | `built` |
| CO-WAREHOUSE-SOURCE | reads | in-process Dagster instance API | none | the label set a source published in its newest observation | `dag_tools/components/classification_propagation/component.py:482-526`; `test_the_shipped_example_propagates_from_a_source_it_only_OBSERVED` | `built` |
| CO-DAGSTER | writes | in-process Dagster instance API | none | an observation on the derived asset | `dag_tools/components/classification_propagation/component.py:544-640`; no test | `built` |
| CO-DATAHUB | calls | HTTP | token from config, else from the environment; none sent when empty | two patch aspects per dataset | `dag_tools/components/classification_propagation/component.py:373-414`; no test | `built` |
| none: a mounted classification document | reads | file read | none | the dimensions and values, read every tick | `dag_tools/components/classification_propagation/component.py:544-554, :700-717`; no test | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | computes the classification of each derived column from the source columns it read, and publishes it | [DoD S12](../alignment/dod-dmra.md) |
| How the mesh wires it | a Dagster component; one sensor, stopped by default, reading the Dagster event log and the dimensions document, writing to the catalog | [DoD P4](../alignment/dod-dmra.md) |
| Sign-in | to the catalog with a token from config or the environment, if one is set; none to anything else | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct to the catalog; the gateway and broker are not on it, and no warehouse is read | [DoD S3](../alignment/dod-dmra.md) |
| Plane | data management | [NATO L3](../alignment/nato-dcra.md) |
| Security role | none for access control; it publishes labels and records how each was composed | — |
| Services served | classification markings that rise when data is combined, with downgrades named and owned | [DoD S12](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/classification_propagation/`; engine paths under `dag_tools/warehouse_source/`; tests under `dag_tools_tests/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| One sensor, no asset, no run launched | `component.py:529-541`, `:644`, `:719` | AGENTS.md §4n | `test_classification_propagation.py`: `test_the_shipped_example_builds_one_sensor_and_materializes_nothing` | `built` |
| Stopped by default; status validated | `component.py:534`, `:681`; `schema.py:37`, `:266`, `:288-296` | AGENTS.md §4n | `test_classification_propagation.py`: `test_the_sensor_is_stopped_by_default_and_launches_no_runs`, `test_an_unknown_sensor_status_is_refused_rather_than_defaulted` | `built` |
| Sources declared, not inferred | `schema.py:146-152`, `:176-194`, `:276-285`; `component.py:154-186`, `:246-257` | AGENTS.md §4n | `test_classification_propagation.py`: `test_lineage_naming_an_undeclared_source_is_refused`, `test_a_declared_source_that_never_materialized_fails`, `test_a_source_publishing_no_label_set_fails_rather_than_counting_as_nothing`, `test_a_dataset_block_that_cannot_propagate_is_refused` | `built` |
| Sources read from observations and materializations; derived side materializations only | `component.py:482-516`, `:519-526`, `:563`, `:578` | AGENTS.md §4n | `test_classification_propagation.py`: `test_the_shipped_example_propagates_from_a_source_it_only_OBSERVED` | `built` |
| Composition per dimension kind | `dimensions.py:290-339`; `propagate.py:190-236` | AGENTS.md §4l, §4m | `test_warehouse_source_propagation.py`: `test_a_permissive_set_composes_by_INTERSECTION_not_union`, `test_composition_is_the_join_of_one_order_in_every_type`, `test_composing_no_labels_at_all_is_refused`, `test_composition_against_an_unusable_document_is_refused` | `built` |
| Table-max fallback; unlabelled source column fails | `propagate.py:262-302`, `:463-521`; `component.py:189-216`, `:438-440` | AGENTS.md §4m | `test_warehouse_source_propagation.py`: `test_a_column_with_no_declared_lineage_falls_back_to_the_table_max`, `test_an_unlabelled_source_column_fails_rather_than_counting_as_unrestricted`, `test_source_columns_differing_only_by_case_are_refused`; `test_classification_propagation.py`: `test_an_asset_naming_no_columns_is_refused_not_propagated_over_nothing` | `built` |
| Downgrades accountable and overlaid | `schema.py:56-123`, `:195-211`; `propagate.py:305-379`, `:406-460`; `component.py:219-230`, `:441-443` | AGENTS.md §4m, §4n | `test_warehouse_source_propagation.py`: `test_an_applied_downgrade_records_the_rule_owner_reason_and_both_labels`, `test_a_rule_that_tightens_a_dimension_is_not_a_downgrade`, `test_a_rule_says_nothing_about_the_dimensions_it_omits`, `test_a_rule_can_clear_a_set_dimension_and_not_only_lower_the_ladder`; `test_classification_propagation.py`: `test_a_downgrade_rule_nobody_is_accountable_for_is_refused` | `built` |
| Two patch aspects, with removes | `component.py:272-283`, `:308-332`, `:335-370` | AGENTS.md §4n | `test_classification_propagation.py`: `test_both_aspects_are_patched_so_what_else_describes_the_dataset_survives`, `test_the_terms_patch_removes_the_rest_of_the_vocabulary`, `test_nothing_outside_the_documents_vocabulary_is_touched` | `built` |
| Document re-read each tick; absent fails tick; load-time label check | `component.py:76`, `:544-554`, `:700-717`; `dag_tools/components/warehouse_source/component.py:139-166` | AGENTS.md §4m, §4n | `test_classification_propagation.py`: `test_the_document_is_read_at_every_tick_not_once_at_load`, `test_the_code_location_loads_with_no_document_mounted`, `test_a_declared_downgrade_is_validated_against_the_document_at_load`, `test_require_ratified_fails_the_tick_rather_than_publishing` | `built` |
| Per-dataset isolation, cursor, retry | `component.py:468-479`, `:561-634` | AGENTS.md §4n | `test_classification_propagation.py`: `test_one_dataset_failing_does_not_stop_the_others`, `test_a_failure_is_recorded_against_the_asset_rather_than_only_logged`, `test_a_failing_dataset_is_retried_without_a_re_materialization`, `test_an_unchanged_materialization_is_not_republished`, `test_an_unreadable_cursor_is_reset_rather_than_crashed_on` | `built` |
| Direct catalog connection, token handling, dry run | `component.py:373-414`, `:589-593`; `schema.py:227-249` | AGENTS.md §4i, §4n | `test_classification_propagation.py`: `test_dry_run_records_the_whole_decision_and_emits_nothing`, `test_with_a_catalog_configured_the_tick_emits_what_it_recorded`, `test_emit_sends_every_proposal_and_counts_them`; no test of token resolution | `built` |
