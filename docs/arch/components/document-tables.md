# Document tables (CO-DOCUMENT-TABLES)

The document tables component turns bundles of XML or JSON documents into tables. A bundle is a zip file or a directory tree, held on a local path or in an S3-compatible store. A mapping file, written by the deployment, says which documents to read, which rows and columns to take from each, and which tables to fill. The component writes one Lance dataset per table and builds the Dagster assets, checks, sensor and jobs around them. A separate command, `dagtools documents profile`, reads bundles and reports their shape so that a mapping can be written.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It reads XML and JSON documents out of zip or directory bundles, chosen by globs in the mapping.** The mapping's `format` is `xml`, read with XPath, or `json`, read with JMESPath. `match` lists path globs, and a member that matches none is not a document. Documents are read in path order, and one larger than `max_bytes` is refused. The XML parser resolves no entities, loads no DTD and does not use the network. `built`
2. **It reads from a local path or an S3-compatible prefix, and it passes no credential to the store.** `source.uri` is required and must be `s3://`, `file://` or a plain path; any other scheme is refused. The S3 client is built from the store client's default credential chain, with an optional `endpoint_url` and a fixed retry setting. A zip bundle on S3 is read with ranged requests, not downloaded whole. The S3 tests run against a stand-in store only. `built`
3. **The mapping declares everything format-specific, and it is checked in full at load.** Every mapping block refuses unknown keys. Validation collects every problem before it raises one error. A missing file, an unreadable file or a root that is not a mapping is refused. A file column stores the bytes by default, or only the reference. A reference is looked up by the mapping's strategies in order; the first strategy that finds one member wins, and one that finds more than one stops the search as ambiguous, so nothing is stored. `built`
4. **It writes one Lance dataset per table under `destination.uri`, replacing a bundle's rows before appending them.** For each bundle it creates any missing table, compares schemas, deletes the rows carrying that bundle's key, then appends the new rows in chunks. A changed schema raises an error naming the added, removed and retyped columns, before anything is deleted. The only credential channel to the destination is `destination.storage_options`, which is passed to Lance as given and not logged. A test pins that a re-run changes no count. `built`
5. **A bad document is rejected into a `_rejects` table; it does not stop the bundle.** Any error while reading one document writes a row to `_rejects`, and none of that document's rows reach any table. With `redact` on, the text of any error that is not the component's own is replaced by its type and a short digest. A structural path the mapping does not expect is ignored, counted with a warning, or made a rejection, by `on_unknown_path`; the default is `warn`. `built`
6. **Dagster gets one multi-asset, two jobs, a sensor and an optional schedule.** The multi-asset has one output per table plus `_rejects`. It is of kind `lance`, cannot be subset, has one dynamic partition per bundle and runs in the write pool. The jobs are an ingest job and a maintenance job. The maintenance schedule exists only when `maintenance.cron` is set. The discovery sensor and the schedule both start stopped unless their `default_status` is `RUNNING`. `built`
7. **The checks are warnings only and never block a run.** `reject_rate` is always defined on `_rejects`. `unknown_paths` is defined there only when the mapping declares `expected_paths` and does not ignore unknown paths. `file_resolution_rate` is defined on each table with a file column, and passes when a bundle carries no file reference, and `non_empty` on each table not listed in `allow_empty`. Every result is at warning severity, and a bundle with no documents fails `reject_rate`. `built`
8. **Load refuses a bad configuration; the run refuses a bad bundle or a mismatched partition.** At load it refuses an unknown key in a block, an invalid mapping, a missing mapping file, and an `allow_empty` entry naming a table the mapping lacks. At run it refuses a missing source directory, an unsupported scheme and a changed schema. It also refuses a run whose bundle tag disagrees with its partition or does not match exactly one bundle. `built`
9. **Writes are serialised by a Dagster pool named from the destination, limited to `write_concurrency`.** The pool name is a fixed prefix plus a short digest of the destination address, so the address itself is not shown. The component sets the pool's limit, and a failure to set it is logged and not fatal. The documentation records that delete-then-append is not atomic, which is why the pool exists, and that a run coordinator which ignores pools gives no serialisation. `built`
10. **It publishes nothing to the catalog itself; it emits the metadata the lineage sensor reads.** Each table's materialization carries the row count, the dataset location and the column schema in Dagster's standard metadata keys, plus counters. The documentation names the lineage sensor as the reader. A test pins that the column names reach the catalog's schema extractor. `built`
11. **It presents no identity to anything, and the gateway and broker are not on its path.** The source is reached with the store client's default credential chain and the destination with `storage_options`. No token or identity is sent by the component. The documentation says credentials never go in the component file, and that the destination's are given as environment references. No test pins the absence. `built`
12. **The profiler is a separate command that reads bundles and writes local reports only.** `dagtools documents profile` takes a source, an output directory and options for sampling, redaction, rebuilding the report only and starting afresh. The source is required unless only the report is rebuilt. It keeps a checkpoint under the output directory, so a later run skips the bundles already done. Reports carry example values unless redaction is asked for. It builds no Dagster objects. It exits with code 2 when the documents extra is missing, on a source or profile error, or with no source, and with code 130 when interrupted. `built`
13. **`source.bundle_depth` decides what a bundle is, and its default makes the whole source one bundle.** At `0`, the default, the root is the one bundle and the whole source is listed every time. At a depth of one or more, each directory at exactly that depth is a bundle, a zip file at or above it is its own bundle, and discovery lists only those levels. The setting's own description calls `0` unsuitable for large trees; the documentation's quick start and the shipped example set `1`. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `name` | required | names the assets, jobs, sensor and schedule; must be an identifier |
| `source.uri` | required | the bundle location: `s3://`, `file://` or a path |
| `source.endpoint_url` | none | points the S3 client at an S3-compatible store |
| `source.bundle_depth` | `0` | directory levels that name a bundle; `0` makes the whole root one bundle |
| `mapping` | required | a path to a mapping file, or the mapping inline |
| `destination.uri` | required | where the Lance datasets are written |
| `destination.storage_options` | empty | passed to Lance as given; the destination's credential channel |
| `destination.flush_rows` | 50,000 | rows buffered before a chunk is appended |
| `destination.flush_attachment_bytes` | 256 MiB | attachment bytes buffered before a chunk is appended |
| `asset_key_prefix` | `[name]` | prefix of the asset keys |
| `write_concurrency` | `1` | limit of the write pool |
| `redact` | `false` | replaces error text that is not the component's own in `_rejects` |
| `group_name` / `owners` / `tags` | none / empty / empty | Dagster labels on the assets |
| `checks.max_reject_rate` | `0.0` | highest share of rejected documents before the check fails |
| `checks.min_file_resolution_rate` | `1.0` | lowest share of file references resolved |
| `checks.unknown_paths` | `0` | most unexpected paths allowed |
| `checks.allow_empty` | empty | tables exempt from `non_empty`; each must exist in the mapping |
| `discovery.interval_seconds` | `300` | sensor interval |
| `discovery.max_new_partitions_per_tick` | `25` | most partitions requested in one tick |
| `discovery.default_status` | `STOPPED` | whether the sensor starts running |
| `discovery.start_after` | none | skips bundle keys at or before this one |
| `maintenance.older_than_days` | `7.0` | age before old versions are cleaned up |
| `maintenance.retain_versions` | none | versions to keep |
| `maintenance.cron` | none | adds a maintenance schedule when set |
| `maintenance.default_status` | `STOPPED` | whether that schedule starts running |
| mapping `max_bytes` | 64 MiB | largest document read |
| mapping `on_unknown_path` | `warn` | `ignore`, `warn` or `fail` |

## Gaps

- **A bundle is read, and a destination written, with a store credential and no identity, outside the mesh's read path** (findings 2 and 11). The component does not call the gateway or broker, so a read of a bundle is not authorized, ticketed or audited by them. Remedy: not on the roadmap.
- **Every check is a warning, so a run that rejects every document still succeeds** (finding 7). Only a reader of the check results sees it. Remedy: not on the roadmap.
- **`source.bundle_depth` defaults to `0`, which makes the whole source one bundle and lists all of it every time** (finding 13). The setting's own description calls this unsuitable for large trees, and the documentation's quick start sets a depth of one. Remedy: not on the roadmap.

## Not established from code

- Which credentials a deployment gives the S3 client or Lance, and whether it uses an object store at all.
- Whether an object store is safe as a Lance destination. The documentation says this was not measured, and that S3 as a source was tested only against a stand-in store.
- Whether a deployment's run coordinator honours pools, so that `write_concurrency` really limits concurrent writes.
- Whether the lineage sensor that reads this metadata is deployed.
- Whether the ingest rate the documentation targets is met. The documentation says the rate it measured falls short.
- Whether Lance reads environment variables of its own when `storage_options` is empty.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-DAGSTER | is called by | in-process | none | a partitioned asset per bundle | `dag_tools/components/document_tables/component.py:372-376`; no test | `built` |
| none: an S3-compatible object store | reads | S3 API | none passed; the client's default credential chain | bundles of documents | `dag_tools/documents/sources.py:612-618, :699-702`; `test_s3_credentials_are_never_passed_by_the_source` | `built` |
| none: a Lance destination store | writes | Lance library | storage options set by the deployment | table rows | `dag_tools/documents/lance_writer.py:192-200`; `test_storage_options_reach_every_lance_entry_point_and_are_never_logged` | `built` |
| none: a Lance destination store | reads | Lance library | storage options set by the deployment | existing table schemas | `dag_tools/documents/lance_writer.py:192-200`; no test | `built` |
| CO-DATAHUB-LINEAGE | writes | in-process output metadata | none | row count, location and column schema | `dag_tools/components/document_tables/component.py:259-266`; `test_the_column_names_survive_to_the_datahub_schema_extractor` | `built` |
| person: an operator | is called by | command line | none | a profile command | `dag_tools/documents/cli.py:28-69`; `test_report_only_reads_no_bundle` | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | turns bundles of XML or JSON documents into Lance tables under a deployment's mapping, and reports how well each bundle mapped | — |
| How the mesh wires it | a YAML component in a code location; one partition per bundle, a discovery sensor, a maintenance job | [DoD P3](../alignment/dod-dmra.md) |
| Sign-in | none; the source uses the store client's default credential chain and the destination uses `storage_options` | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct between the run, the bundle store and the destination; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | operational, where a producer's documents become tables as they arrive | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none for mesh reads; it refuses bad configuration and rejects bad documents, which is input validation, not access control | — |
| Services served | mediation of documents into a table shape; checks kept with the asset; column schema and location emitted as metadata for the catalog path | [DoD S14](../alignment/dod-dmra.md), [S13](../alignment/dod-dmra.md), [S4](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/document_tables/`; engine paths under `dag_tools/documents/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| XML and JSON from bundles; parser flags | `mapping.py:336-346`; `engine.py:1220-1237`; `reader.py:287-295` | not stated | `test_document_tables_component.py`: `test_a_partition_materializes_with_the_expected_rows_and_passing_checks`; `test_documents_engine.py`: `test_an_external_entity_is_never_read`, `test_the_external_entity_control_would_be_visible_if_it_were_read` | `built` |
| Local or S3 source; no credential passed | `schema.py:24-53`; `sources.py:13-18`, `:614-617`, `:699-702` | docs/DOCUMENT_TABLES.md, "The component" | `test_documents_sources.py`: `test_s3_credentials_are_never_passed_by_the_source`, `test_s3_lists_and_opens_a_zip_bundle_and_a_directory_bundle` | `built` |
| Mapping checked in full at load; file resolution | `mapping.py:219`, `:846`, `:955`, `:978-990`; `files.py:57-100` | docs/DOCUMENT_TABLES.md, "Validation" | `test_document_tables_component.py`: `test_a_mapping_with_a_validation_problem_fails_the_load`, `test_a_missing_mapping_file_fails_the_load_naming_the_file` | `built` |
| One Lance dataset per table; replace then append | `lance_writer.py:29-30`, `:192-245` | docs/DOCUMENT_TABLES.md, "Concurrency and Lance: what was measured" | `test_documents_lance_writer.py`: `test_rerunning_a_bundle_changes_no_count_and_leaves_the_other_untouched`, `test_a_changed_column_type_raises_naming_the_column_and_deletes_nothing`, `test_storage_options_reach_every_lance_entry_point_and_are_never_logged` | `built` |
| Bad documents go to `_rejects`; unknown paths | `engine.py:965-968`, `:1000-1003`, `:1240-1277`; `mapping.py:459` | not stated | `test_documents_engine.py`: `test_fail_rejects_the_drifted_document_and_keeps_the_clean_one`, `test_warn_counts_unknown_paths_and_still_ingests_the_document`, `test_ignore_does_not_even_look` | `built` |
| Dagster objects; stopped by default | `component.py:372-433`, `:436-499`, `:502-519`, `:572-598`; `schema.py:107-148` | docs/DOCUMENT_TABLES.md, "What it defines" | `test_document_tables_component.py`: `test_the_assets_are_partitioned_by_bundle`, `test_the_shipped_example_loads`, `test_a_cron_adds_a_stopped_maintenance_schedule`, `test_the_first_tick_requests_both_partitions_and_then_skips` | `built` |
| Checks are warnings; `unknown_paths` conditional | `component.py:198-222`, `:322-368`, `:351-355` | docs/DOCUMENT_TABLES.md, "What it defines" | `test_document_tables_component.py`: `test_default_thresholds_fail_the_checks_as_warnings_and_the_run_succeeds`, `test_allow_empty_drops_the_non_empty_check_for_that_table` | `built` |
| Load and run refusals | `component.py:166-183`, `:226-255`; `sources.py:532`, `:542`, `:699-702` | not stated | `test_document_tables_component.py`: `test_an_unknown_key_in_source_fails_the_load`, `test_allow_empty_naming_an_unknown_table_fails_the_load`, `test_a_tag_that_disagrees_with_the_partition_fails_the_run`; `test_documents_sources.py`: `test_missing_directory_and_unknown_scheme_are_refused` | `built` |
| Write pool | `component.py:104-112`, `:137-146` | docs/DOCUMENT_TABLES.md, "Concurrency and Lance: what was measured" | `test_document_tables_component.py`: `test_the_pool_is_named_from_the_destination_and_limited_to_write_concurrency`; enforcement by a run coordinator not tested | `built` |
| Metadata for the lineage sensor | `component.py:259-319` | docs/DOCUMENT_TABLES.md, "What a run produces" | `test_document_tables_component.py`: `test_the_column_names_survive_to_the_datahub_schema_extractor` | `built` |
| No identity; gateway and broker not on the path | `schema.py:24-81`; `sources.py:614-617`; `lance_writer.py:29-30` | docs/DOCUMENT_TABLES.md, "The component" | no test of absence; code searched for a token or identity being sent, none found | `built` |
| Profiler is a separate command | `cli.py:16-69`; `profiler.py:158-162`, `:259-306`, `:1064`, `:1144` | docs/DOCUMENT_TABLES.md, "Profiling bundles before writing a mapping" | `test_documents_profiler.py`: `test_report_only_reads_no_bundle`, `test_redact_hides_value_and_control_sees_it`, `test_resume_skips_done_bundles` | `built` |
| Bundle depth and its default | `schema.py:33-44`; `examples/document_tables/dagster_home/components/document_tables/component.yaml:19` | docs/DOCUMENT_TABLES.md, "The component" and "Concurrency and Lance: what was measured" | no test name looked up | `built` |
