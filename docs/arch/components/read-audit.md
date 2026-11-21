# Read audit (CO-READ-AUDIT)

The read audit component makes one Dagster asset. That asset compares a warehouse's query history with Dagster's runs and reports which reads an asset explains and which it does not. The unexplained reads are the gap, and the gap is what the audit delivers. It reads history either from an exported file or through a resource that runs a query against the history.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It makes one asset and nothing that runs it.** The component builds a single asset. It defines no sensor, schedule, job or check, so the deployment decides when the audit runs. `built`
2. **History comes from exactly one of two places, and the second names everything.** `history` must name either an exported rows file or a resource key, never both. Through a resource, the history relation and all five column names are required, with no defaults. The library names no relation, column or level. The reasons are in AGENTS.md §4j. `built`
3. **The history query binds every value and checks every name, but reads with no row limit.** The relation must be one to three plain dotted identifiers, and each column name passes the same check; anything else is refused, not escaped. The time window and the optional principal filter are bound parameters. The statement has no row limit. Over the default lookback of 168 hours, it returns every matching row and holds them in memory. `built`
4. **The lookback does not apply to an exported file.** A rows file is read whole. The lookback, and its per-run override, only set the window of the resource query. `built`
5. **A read is attributed by its tag first, and the tag is not signed.** A tag carrying a non-pipeline principal is reported as not a pipeline read. A tag carrying an asset key explains the read by that key. A tag carrying a run id explains it only when that run selected exactly one asset. The tag is plain JSON written by the reader itself. It carries no signature, so a client that writes a pipeline tag is believed. `built`
6. **An untagged read is correlated by time, by any identity unless a filter is set.** The row's own identity is never read. An untagged read inside exactly one run window, where that run selected one asset, is attributed to that asset. The query's principal filter is optional. Without it, an untagged read by any identity during such a window is attributed to the asset. The result is ambiguous rather than a guess in three cases: a tagged run selected more than one asset; exactly one window overlaps but its run selected more than one asset; or more than one window overlaps. The second case is remedied by narrowing a run's selection. `built`
7. **An open window contains every later read.** A run with no end time, whether still running or recorded without one, has an open window. Every later untagged read falls inside it. `built`
8. **One attributed read explains its relation.** A relation counts as explained if any one read of it was attributed. Other reads of the same relation that nothing explains do not put it in the gap. `built`
9. **The gap is reported in parts, and an empty window has no coverage figure.** Unexplained relations are split into ambiguous, untagged and not-a-pipeline. With nothing read, coverage is absent rather than full. A history row that cannot be parsed is listed in `row_failures`, not dropped. `built`
10. **Failure text from rows and run files travels into the output.** A row or run window that fails to parse is recorded with the exception's message. The full report is the asset's output, so it goes to the asset's IO manager. A list of unexplained relations, capped by `max_listed`, goes into the materialization metadata. `built`
11. **Run windows come from a file, from the Dagster instance, or both.** The instance read takes up to `limit` run records. If that read fails, a warning is logged and no instance windows are used. The audit then runs with fewer windows and more reads in the gap. `built`
12. **The chart and the shipped code location do not deploy it.** No chart template and nothing in the user code location wires this component. A deployment adds it in its own code location. `not built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `history.rows_file` | unset; one of this or `resource_key` is required | an exported history file, read whole |
| `history.resource_key` | unset | the resource whose `fetch_rows` runs the history query |
| `history.relation` | required with a resource | the history relation, one to three plain identifiers |
| `history.columns` | required with a resource; five names, no defaults | how the history spells the query, principal, start time, relations and tag |
| `history.principal_filter` | unset | limits the query to one identity |
| `history.lookback_hours` | `168` | the query window; must be above zero |
| `runs.runs_file` | unset | an exported run-window file |
| `runs.from_instance` | `false` | also reads run windows from the Dagster instance |
| `runs.limit` | `2000` | how many run records the instance read takes |
| `asset_name` | `read_audit` | the asset's name |
| `group_name`, `io_manager_key`, `owners`, `tags` | unset or empty | passed to the asset |
| run config `lookback_hours` | `0`, meaning the YAML value | overrides the lookback for one run |
| run config `max_listed` | `200` | caps the unexplained list in the metadata |

## Gaps

- **The history read has no row limit** (finding 3). A busy warehouse over a long lookback returns every row into one process. Remedy: [roadmap row 4](../ROADMAP.md) (not filed).
- **The attribution tag is self-asserted** (finding 5). A read that carries a forged pipeline tag counts as explained. Remedy: [roadmap row 17](../ROADMAP.md) (not filed) signs the tag; it depends on [row 1](../ROADMAP.md), verified principals at the gateway.
- **Without a principal filter, time correlation credits any identity's reads to an asset** (finding 6). A person's untagged read during a single-asset run is reported as that asset's read. Remedy: not on the roadmap.
- **A run with no end time absorbs every later untagged read** (finding 7). Remedy: not on the roadmap.
- **One explained read hides the rest of a relation's reads** (finding 8). Remedy: not on the roadmap.
- **Exception text reaches the asset's output** (finding 10). The metadata carries only the count of rows not read. A driver message can quote a relation or an account. Remedy: not on the roadmap.

## Not established from code

- Whether a given warehouse's history view keeps a comment-placed tag intact, so that a tag the reader sent can be read back.
- When a deployment runs the audit, and over which history.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-DAGSTER | is called by | in-process | none | asset run and run config | `dag_tools/components/read_audit/component.py:170-233`; `test_read_audit_component.py`: `test_the_shipped_example_loads` | `built` |
| CO-DAGSTER | reads | in-process instance read | none | run windows | `dag_tools/components/read_audit/component.py:114-141`, `:213-223`; `test_read_audit_component.py`: `test_run_rows_become_aware_utc_windows` | `built` |
| XE-HISTORY-RESOURCE | calls | in-process resource call | none of its own; the resource holds the warehouse credential | a history statement with bound parameters | `dag_tools/components/read_audit/component.py:144-167`, `:200-205`; `dag_tools/audit/history.py:92-148`; `test_audit_history.py`: `test_every_value_is_bound_and_none_is_interpolated` | `built` |
| XE-HISTORY-EXPORT | reads | file read | none | history rows | `dag_tools/components/read_audit/schema.py:25-109`; `test_read_audit_component.py`: `test_history_demands_exactly_one_topology` | `built` |
| XE-RUN-WINDOW-EXPORT | reads | file read | none | run windows | `dag_tools/components/read_audit/component.py:114-141`; `test_read_audit_component.py`: `test_run_rows_become_aware_utc_windows` | `built` |
| XE-IO-MANAGER | writes | in-process | none | the full report | `dag_tools/components/read_audit/component.py:213-231`; no test of the failure text | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | reports which warehouse reads a Dagster asset explains, and which it does not | — |
| How the mesh wires it | a Dagster component a deployment adds; the chart and the shipped code location do not | — |
| Sign-in | none of its own; the history resource holds the warehouse credential | [NATO R7](../alignment/nato-dcra.md) |
| Data path | a history query through a resource, or an exported file; the gateway and broker are not on it | — |
| Plane | `data-management`: attribution of reads to producers | [NATO L3](../alignment/nato-dcra.md) |
| Security role | attribution of reads to identities, from an unsigned tag or a time window | [NATO R5](../alignment/nato-dcra.md) |
| Services served | mesh performance analytics, where the read audit is named | [DoD S10](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/read_audit/`; engine paths under `dag_tools/audit/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| One asset; no sensor, schedule or check | `component.py:170-233`, `:299-310` | AGENTS.md §4j | `test_read_audit_component.py`: `test_the_shipped_example_loads`, `test_the_component_executes_nothing_itself` | `built` |
| Exactly one history source; names required, no defaults | `schema.py:25-109` | AGENTS.md §4j | `test_read_audit_component.py`: `test_history_demands_exactly_one_topology`, `test_reading_through_a_resource_demands_every_column_name`, `test_the_resource_key_is_required_so_the_load_names_a_missing_resource` | `built` |
| Names checked, values bound, no row limit | `history.py:41-42`, `:92-148` | AGENTS.md §4j | `test_audit_history.py`: `test_every_value_is_bound_and_none_is_interpolated`, `test_anything_that_is_not_an_identifier_is_refused_not_escaped`, `test_a_column_name_goes_through_the_same_grammar`; no test of a limit | `built` |
| Lookback applies to the query only | `component.py:144-167`, `:200-205` | not stated | `test_read_audit_component.py`: `test_the_run_config_overrides_the_configured_lookback` | `built` |
| Tag first; tag is unsigned JSON | `reads.py:170-213`; `tag.py:123-172` | AGENTS.md §4j | `test_audit_reads.py`: `test_a_tagged_asset_key_needs_nothing_else`, `test_a_run_id_resolves_through_a_single_asset_run`, `test_a_multi_asset_run_cannot_attribute`, `test_a_person_behind_the_tag_is_not_a_pipeline`; `test_audit_tag.py`: `test_decode_never_raises_on_what_other_tools_wrote` | `built` |
| Time correlation; optional principal filter | `reads.py:154-213`; `history.py:134-141` | AGENTS.md §4j | `test_audit_reads.py`: `test_an_untagged_read_correlates_through_one_single_asset_window`, `test_two_overlapping_windows_are_ambiguous`, `test_an_opaque_row_identity_is_not_evidence_either_way`; `test_audit_history.py`: `test_an_unset_principal_filter_is_not_bound_at_all` | `built` |
| Open window contains later reads | `reads.py:106-118`, `:154-159`; `component.py:83-111` | not stated | `test_audit_reads.py`: `test_an_open_window_still_contains_a_later_read`; `test_read_audit_component.py`: `test_a_still_running_run_yields_an_open_window` | `built` |
| One attributed read explains its relation | `reads.py:249-261` | not stated | `test_audit_reads.py`: `test_an_object_read_both_ways_counts_as_explained` | `built` |
| Gap in parts; no coverage on an empty window; failed rows listed | `reads.py:145-151`, `:276-278`; `history.py:189-225` | AGENTS.md §4j | `test_audit_reads.py`: `test_the_gap_contains_every_read_no_asset_explains`, `test_coverage_is_none_when_nothing_was_read`; `test_audit_history.py`: `test_one_malformed_row_is_reported_not_swallowed_and_not_fatal`; `test_read_audit_component.py`: `test_the_metadata_decomposes_the_gap` | `built` |
| Failure text in the output; report to the IO manager | `component.py:213-231`; `history.py:222-223` | not stated | no test of the text | `built` |
| Run windows from file or instance; soft instance failure | `component.py:114-141`, `:213-223` | not stated | `test_read_audit_component.py`: `test_run_rows_become_aware_utc_windows`, `test_a_run_with_no_start_time_is_skipped_not_guessed` | `built` |
| Not wired by the chart or the shipped code location | no match for the component under `helm/` or `dag_tools/user_deployment/` | not stated | — | `not built` |
