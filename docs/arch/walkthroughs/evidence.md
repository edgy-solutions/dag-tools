# A decision-maker asks for evidence (WT-EVIDENCE)

A decision-maker is shown a figure computed from mesh data and asks what stands behind it: which dataset, where it came from, whether it was checked, how it is labelled, who may read it, who did read it, and which version was read. This walkthrough answers each question from the records. It is framed by elements of the NATO *Data Centric Reference Architecture for the Alliance*, version 2:

- the data management plane (Appendix 14, paragraphs 8 to 9), where the lifecycle services that answer most of these questions sit;
- the analytical data plane (Appendix 14, paragraph 10), where the figure is made;
- the policy enforcement, decision, information and administration points (Appendix 9, paragraph 3), which answer who may read.

Each step names the mesh component that answers it and the records it relies on. A step adds no claim of its own: its status is the status of the records it cites, and its evidence is theirs.

**This record describes; it proposes nothing.** Where a gap has a remedy, it is a row in [the roadmap note](../ROADMAP.md), marked `proposed`; three gaps below have such a row, and the other two are settled by a decision, which each names. Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **Every question but one has a built answer, and none has a measured one.** Identity, lineage, checks, labels, grants, reads and run records are each `built` with a test. The version actually read is the exception: the mechanism is `built` and no reader of a version ships. No step has a recorded run against a running system, so none is `measured`. `built`; version readers `not built`
2. **The dataset's identity is its physical address, not its pipeline.** A labelled source and a mapped model each record the physical platform's dataset as the asset of record, and the Dagster asset attaches as lineage. A Dagster-platform address for a labelled source is refused when the code location loads. `built`
3. **Whether the catalog holds the dataset depends on the path that published it.** A labelled source whose labels fail to reach the catalog fails its materialization. A dbt build whose publishing fails still succeeds, by design, so a built model can be missing from the catalog without anything failing. `built`
4. **Lineage reaches the catalog only where a deployment turns it on.** The lineage sensor starts stopped. The shipped code location starts it only when a catalog server address is set. `built`
5. **A label is evidence of what was read, not of who may read.** Labels are composed and published with the basis each was derived from, and every downgrade names an owner and a reason. The read decision does not consult them. Labels as published metadata: `built`. As a decision input: `proposed`.
6. **Who did read has two sources that are not joined.** The gateway logs each ticket it issues, with a ticket id and the verified principal. The read audit attributes warehouse reads by a query tag, which is not signed. No record joins the two. Each source: `built`. A join: `not built`.

## The walkthrough, step by step

| # | Question | Answer from the records | Component | Status | Evidence | Records relied on |
|---|---|---|---|---|---|---|
| 1 | Which dataset is this? | The figure's dataset is named by its catalog URN on the physical platform. A labelled source refuses a Dagster-platform URN at load. A mapped model records the physical platform as its asset of record, distinct from the raw table it reads. | warehouse source; contract mapping | `built` | `test_a_dagster_platform_urn_fails_the_load`; `test_the_materialization_records_the_physical_platform_as_the_asset_of_record` | [§4m.1](../decisions/dag-tools-s4r-s4l-s4m.md), [§4k.13](../decisions/dag-tools-s4k-s4j.md); [warehouse source](../components/warehouse-source.md); [contract mapping](../components/contract-mapping.md) |
| 2 | Is it in the catalog? | For a labelled source, yes or the materialization failed. For a dbt model, publishing failures are swallowed, so its absence from the catalog is possible after a green build. | warehouse source; dbt project | `built` | `test_a_catalog_failure_FAILS_the_materialization`; the swallow is `dag_tools/components/dbt_project/component.py:161`, `:223-224`, and no test asserts it | [§4m.6](../decisions/dag-tools-s4r-s4l-s4m.md), [§4i.3](../decisions/dag-tools-s4i-s4h-s4g.md); [dbt project](../components/dbt-project.md) |
| 3 | Where did it come from? | Lineage runs from the physical dataset to the Dagster asset. Upstreams are the union of the asset graph and the producer's declared inputs; the graph read is best-effort. The sensor that publishes this starts stopped. | DataHub lineage; warehouse source | `built` | `test_the_lineage_edge_runs_FROM_the_physical_dataset_TO_the_asset`; `test_graph_upstreams_capture_a_deps_edge`; `test_sensor_can_start_running`; no test covers the default physical-only path (DataHub lineage, finding 5) | [DataHub lineage](../components/datahub-lineage.md), findings 1, 2, 5 and 9; [§4m.1](../decisions/dag-tools-s4r-s4l-s4m.md) |
| 4 | Was it checked? | A mapped model's tests each fail under a rule its contract declares. A table with no cursor and a configured entry gets a row-count check, and a missing count fails it rather than reading as zero. | contract mapping; dlt pipeline | `built` | `test_every_emitted_test_fails_under_a_rule_the_contract_declares`; `test_an_absent_rows_loaded_fails_the_check` | [§4k.12](../decisions/dag-tools-s4k-s4j.md), [§4p.14](../decisions/dag-tools-s4p.md); [dlt pipeline](../components/dlt-pipeline.md) |
| 5 | Did the load succeed, and if not, why? | With `ingest_runs` on, every dlt run is recorded, failed or not, with the root error's type and a digest of its message. The SystemLink source records every run and carries three checks on that table. `ingest_runs` is off by default. | dlt pipeline; the SystemLink source | `built` | `test_a_failed_run_is_recorded_and_the_failure_is_re_raised`; `test_the_ingest_run_row_counts_the_run`; `test_the_digest_is_of_the_error_the_row_NAMES` | [§4p.16](../decisions/dag-tools-s4p.md), [§4q.12, §4q.13](../decisions/dag-tools-s4q.md); [the SystemLink source](../components/systemlink-source.md) |
| 6 | How is it labelled, and why? | A derived column's label is composed from the labels of the columns its lineage names. A column with no lineage carries the table maximum, and the published record says which basis was used. A widening is a declared downgrade with an id, an owner and a reason. | classification propagation | `built` | `test_a_column_the_lineage_says_nothing_about_carries_the_table_max`; `test_the_document_records_how_each_column_was_arrived_at`; `test_a_declared_downgrade_is_applied_and_recorded_with_its_accountability` | [§4n.4, §4n.5](../decisions/dag-tools-s4n-s4o-s4k1.md); [classification propagation](../components/classification-propagation.md), findings 6 and 7 |
| 7 | Who may read it? | One explicit grant on one dataset, `can_read`, decided by Topaz. Grants are reviewed files carrying the grantor and a reason. Labels, persona and domain are not consulted. | central gateway; Topaz | `built` | as cited in the need-to-know flow, steps 5 and 6 | [need to know](../flows/need-to-know.md), findings 1 and 3 and the policy administration row; [Topaz](../components/topaz.md) |
| 8 | Who did read it, through the gateway? | Each ticket carries its own id, minted per request. The gateway's issue line names the principal through the same expression the broker payload uses. The receipt a consumer keeps is an allowlist of ticket fields. | central gateway; domain broker; mesh data client | `built` | `test_the_log_follows_the_VERIFIED_subject_where_it_differs_from_the_header`; `test_two_resolves_of_one_urn_get_different_ticket_ids`; `test_the_receipt_is_an_allowlist_so_a_new_ticket_field_is_not_published` | [§4o.3, §4o.5, §4o.6](../decisions/dag-tools-s4n-s4o-s4k1.md); [central gateway](../components/central-gateway.md); [mesh data client](../components/mesh-data-client.md) |
| 9 | Who did read it, in the warehouse? | The read audit attributes each read by its tag, then by a single run window. What it cannot explain is published in three parts, and an empty window carries no coverage figure. The tag is not signed, and a person's tag does not change the credential. | read audit; tagged warehouse | `built` | `test_the_metadata_decomposes_the_gap`; `test_a_multi_asset_run_cannot_attribute`; `test_coverage_is_none_when_nothing_was_read` | [§4j.5, §4j.6](../decisions/dag-tools-s4k-s4j.md); [read audit](../components/read-audit.md), finding 5; [tagged warehouse](../components/tagged-warehouse.md), finding 4 |
| 10 | Which version was read? | The ticket keeps the data's version apart from the time of issue, and an absent version is counted per source type on the health route rather than filled with the issue time. No reader of a version ships, so every ticket today carries an absent version. | domain broker | mechanism: `built`. Readers: `not built`. | `test_an_absent_as_of_is_not_the_issue_time`; `test_no_as_of_reader_ships_by_default` | [§4o.1, §4o.4](../decisions/dag-tools-s4n-s4o-s4k1.md); [domain broker](../components/domain-broker.md); [roadmap row 5](../ROADMAP.md) |

## Roles in the NATO frame

| Element | Mesh component | What it does here | Status | Evidence | Gap |
|---|---|---|---|---|---|
| Data management plane (Appendix 14, ¶8 to ¶9) | contract mapping, classification propagation, DataHub lineage, the read audit, ingest run records | Answers steps 1 to 6 and 9: identity, lineage, checks, labels and attributed reads, each written to the catalog or to a run table | `built` | steps 1 to 6 and 9 above | Retirement and recall are absent ([NATO alignment](../alignment/nato-dcra.md), row L3) |
| Analytical data plane (Appendix 14, ¶10) | the mesh data client and the SDK reading through it | Where the figure is made, from a read the gateway issued a ticket for | `built` | step 8 above | The receipt names no version (step 10) |
| Policy decision point (Appendix 9, ¶3) | Topaz | Decides `can_read` on the dataset | `built` | step 7 above | Labels are not an input |
| Policy enforcement point (Appendix 9, ¶3) | central gateway | Refuses or issues the ticket, and logs the issue | `built` | steps 7 and 8 above | — |
| Policy administration point (Appendix 9, ¶3) | policy files and their seed job | Holds each grant with its grantor and reason | `built` | step 7 above | No grant ledger beyond version history ([need to know](../flows/need-to-know.md)) |

## Gaps

- **The version read is unknown.** No reader ships, so a receipt cannot say which version of the dataset a figure came from. Remedy: [roadmap row 5](../ROADMAP.md).
- **A dbt model can be missing from the catalog after a green build.** Publishing failures are swallowed by design (§4i.3). The end-to-end run behind an environment gate (§4i.9) is `measured` in CI, in process, against an in-process catalog recorder; with a real catalog server it is still `built` (item A2 of [the built-to-measured list](../built-to-measured.md)). No row: swallowing is the decision.
- **Gateway reads and warehouse reads are attributed separately.** The ticket log and the read audit are not joined, and the warehouse tag is not signed. Remedy: [roadmap row 17](../ROADMAP.md).
- **Labels do not decide reads.** A decision-maker reading a label should not infer that it limited who read the data ([need to know](../flows/need-to-know.md), findings 1 and 3). Remedy: [roadmap row 18](../ROADMAP.md).
- **Run records are optional for dlt pipelines.** `ingest_runs` is off by default, so a failed load leaves no row unless a deployment turned it on. No row: off by default is the decision (§4p.16), and turning it on is a deployment setting.

## Not established from code

- Whether a given deployment runs the lineage sensor, the propagation sensor or the read audit. Each is off or absent by default, and a deployment's settings are not described here.
- Whether any figure has been traced end to end through these steps on a running system. No recorded run exists, so nothing here is `measured`.
- How catalog ownership data, which the policy directory is synced from, is kept correct (as in [the need-to-know flow](../flows/need-to-know.md)).

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Physical URN is the asset of record | dag-tools decisions §4m.1, §4k.13 | [§4m.1](../decisions/dag-tools-s4r-s4l-s4m.md), [§4k.13](../decisions/dag-tools-s4k-s4j.md) | `test_a_dagster_platform_urn_fails_the_load`; `test_the_materialization_records_the_physical_platform_as_the_asset_of_record` | `built` |
| Catalog failure fails a source; dbt publishing failure does not fail the build | dag-tools decisions §4m.6, §4i.3 | [§4m.6](../decisions/dag-tools-s4r-s4l-s4m.md), [§4i.3](../decisions/dag-tools-s4i-s4h-s4g.md) | `test_a_catalog_failure_FAILS_the_materialization` | `built` |
| Lineage sensor stopped by default; union of graph and declared inputs | [DataHub lineage](../components/datahub-lineage.md), findings 1, 2 and 9 | not stated | `test_sensor_can_start_running`; `test_graph_upstreams_capture_a_deps_edge` | `built` |
| Contract tests under declared rules; row-count check | dag-tools decisions §4k.12, §4p.14 | [§4k.12](../decisions/dag-tools-s4k-s4j.md), [§4p.14](../decisions/dag-tools-s4p.md) | as step 4 | `built` |
| Run records | dag-tools decisions §4p.16, §4q.12, §4q.13 | [§4p.16](../decisions/dag-tools-s4p.md), [§4q.12](../decisions/dag-tools-s4q.md) | as step 5 | `built` |
| Label basis and accountable downgrades | dag-tools decisions §4n.4, §4n.5 | [§4n.4](../decisions/dag-tools-s4n-s4o-s4k1.md) | as step 6 | `built` |
| Who may read | [need to know](../flows/need-to-know.md) | as cited there | as cited there | `built` |
| Ticket identity, principal and receipt | dag-tools decisions §4o.3, §4o.5, §4o.6 | [§4o.3](../decisions/dag-tools-s4n-s4o-s4k1.md) | as step 8 | `built` |
| Warehouse read attribution | dag-tools decisions §4j.5, §4j.6; [read audit](../components/read-audit.md) | [§4j.5](../decisions/dag-tools-s4k-s4j.md) | as step 9 | `built` |
| Version mechanism and absent readers | dag-tools decisions §4o.1, §4o.4 | [§4o.4](../decisions/dag-tools-s4n-s4o-s4k1.md) | `test_no_as_of_reader_ships_by_default` | mechanism: `built`. Readers: `not built`. |
| No step is `measured` | the status columns of the records cited | — | none found | — |
