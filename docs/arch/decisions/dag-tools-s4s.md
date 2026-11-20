# dag-tools decisions: §4s

This record covers the decisions in `AGENTS.md` section 4s at the component-records pin (`add5bc4`): documents to tables. The component that carries them is described in the [document tables record](../components/document-tables.md). [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §4s Documents to tables

One component instance turns bundles of documents into one Lance table per mapped table plus a `_rejects` table, with one dynamic partition per bundle. A bundle is a zip or a directory tree, on a local disk or in S3. `dag_tools/documents/` is the engine, and `dag_tools/components/document_tables/` is the Dagster surface over it.

### §4s.1 No document format is named outside the mapping a deployment supplies

- **Decision.** Element names, attributes, paths, codes and value vocabularies live only in the deployment's mapping file. The one format that appears in code, tests, fixtures and docs is an invented format used by the shipped example and the synthetic test module. A test holds the example's mapping equal to the synthetic module's, and every example bundle file byte-equal to what the module generates. A vocabulary test scans the package for anything else.
- **Why.** The example and the suite then describe one format, and no real format can enter the library unnoticed.
- **Status.** `built`.

### §4s.2 The engine is pure and its dependencies are an optional extra

- **Decision.** Sources, mapping, reader, coercion, file resolution, the Lance writer and the profiler import no Dagster, and each runs from a test with only the `documents` extra installed. The extra is optional, and the component is exported lazily, so a base install still imports the package.
- **Why.** The engine can be tested without an orchestrator, and a deployment that does not read documents does not install their libraries.
- **Where else.** No test runs a base install without the extra.
- **Status.** `built`.

### §4s.3 No value from a document reaches a log at INFO or above

- **Decision.** A log line at INFO or above may name a bundle, a member, a count or a column, and never a value. With `redact` set, a reject whose error is not one of the engine's own document errors is stored as the error's type and a digest.
- **Why.** A parser's message can quote the bytes it failed on, so storing it would copy document content into a table and a log.
- **Status.** `built`.

### §4s.4 Coercion never raises

- **Decision.** A value that does not coerce becomes null and is counted per column. Integers are parsed exactly, with no path through a float, and 32-bit integers are range-checked. Floats must be finite. A timestamp with no offset and no declared time zone is a failure, not UTC. Timestamps are stored as UTC microseconds.
- **Why.** Reading a timestamp with no offset as UTC shifts every row of a source in another zone, and the result still looks like a timestamp.
- **Status.** `built`.

### §4s.5 The writer is idempotent per bundle

- **Decision.** A write deletes the bundle's rows, then appends. The schema comes from the mapping, never from the first batch, so a bundle that yields no rows for a table still creates or matches that table. A dataset whose schema disagrees raises a schema mismatch naming the column instead of widening. Blob columns are large binary carrying the blob encoding flag. Flushes are chunked by row count and by attachment bytes.
- **Why.** Re-materializing a partition then leaves every count where it was.
- **Status.** `built`.

### §4s.6 One writer per destination by default, through a pool named by digest

- **Decision.** `write_concurrency` defaults to 1. Writes go through a Dagster pool whose name is a fixed prefix plus the first 12 hex digits of the SHA-256 of the destination address. The component sets the pool's limit where the instance allows it and warns otherwise; the operator sizes it.
- **Why.** The census reports that concurrent appends, and concurrent delete-then-append on different bundles, were correct on a local filesystem, but two writers interleaved on the same bundle duplicated its rows, because each deleted before the other appended. The pool is named by a digest because an address can carry a bucket name.
- **Where else.** The concurrency figures are the census's; no test or recorded run reproduces them. S3 as a Lance destination is not verified.
- **Status.** `built`.

### §4s.7 A file reference that matches two members stores no bytes

- **Decision.** The row is kept with its resolution column set to `ambiguous`; a reference that matches nothing is recorded as `missing`. Otherwise the column names the strategy that resolved it. The `file_resolution_rate` check reads that column.
- **Why.** Picking one of two members would be a guess that reads as a correct answer.
- **Status.** `built`.

### §4s.8 A member name is clamped to its bundle; a reference inside a document is not

- **Decision.** A member label collapses `.` and `..` and drops any part that climbs above the bundle root, so no label leaves the bundle. A reference inside a document keeps its `..`, resolves against the document's directory, and is a miss if it still climbs out.
- **Why.** A zip can name a member that climbs out of it, while a reference's meaning depends on where its document sits.
- **Status.** `built`.

### §4s.9 A nested zip is spooled into something seekable on every supported Python

- **Decision.** The engine spools a nested zip into an in-memory buffer up to a size limit and into a temporary file above it, not into the standard spooled temporary file.
- **Why.** That temporary file is not seekable before Python 3.11, so a member of a nested zip could not be read on the oldest supported Python. The census records that running the suite on 3.10 found it.
- **Status.** `built`.

### §4s.10 An XPath fast path answers exactly what XPath answers

- **Decision.** Six simple expression shapes are evaluated in Python instead of by the XPath engine; anything else, and anything referencing a variable, goes through XPath unchanged. Tests assert agreement on the scalar, string-list and node views over every node of the synthetic bundle and over edge cases, and assert that the mapping exercises each view.
- **Why.** The census reports about 18 times fewer XPath calls on the bulk benchmark. The exercise test stops the agreement test passing over shapes nothing uses.
- **Status.** `built`.

### §4s.11 Under redaction a profiler parse error keeps no document text

- **Decision.** With `--redact`, the profiler stores a parse error as its type and a digest. A control test shows the planted text does appear without the flag.
- **Why.** The parser's message quotes the bytes it failed on.
- **Status.** `built`.

### §4s.12 Variables are evaluated once per document, against the root

- **Decision.** Each table's variables are reset per document and evaluated against the document root in declaration order before any row is read. A relative variable therefore reads the root and is the same on every row; a null variable becomes an empty string. Indexes have no variables, so an index scope naming one is refused as undefined.
- **Why.** Evaluating per row is the natural wrong reading and produces plausible-looking rows, so the docs call it out and the tests pin it.
- **Status.** `built`.

### §4s.13 A scoped index files under scope and key, and a lookup into one must give a scope

- **Decision.** A scoped index files each entry under a scope and a key, and a lookup into it must name a scope. A scoped index with no lookup scope, and an unscoped index with one, both fail the load. A null or empty scope on either side matches nothing and is counted.
- **Why.** A key unique only inside a larger record otherwise links rows across records with no error, and the wrong record's data reads as a correct answer.
- **Status.** `built`.

### §4s.14 `prefer` picks from one list; `first_of` picks among expressions; the two are refused together

- **Decision.** With `prefer`, the first preferred entry present wins, compared as strings after the value map is applied to each element. An empty list gives the declared empty value; a non-empty list with no preferred value gives null and is counted. The source must be a list; a single-value lookup or a scalar expression is refused at the load.
- **Why.** An unfamiliar code must stay visible rather than be replaced, and choosing from a list that can only hold one value chooses nothing.
- **Status.** `built`.

### §4s.15 `order: keys` keeps the keys' order

- **Decision.** Entries come back in the order the keys name them; an entry reached by two keys keeps its first place. The default document order is unchanged. The setting is refused on lookup modes that have nothing to order.
- **Why.** A setting that cannot act must fail the load rather than be ignored.
- **Status.** `built`.

### §4s.16 A parser-limit refusal is told apart from malformed XML by a probe parse

- **Decision.** A refused document is parsed once more with the parser's size limits lifted and entities, DTDs and network still off, and that tree is discarded. If it parses, the reject names the `huge_tree` option and quotes nothing from the document. `max_bytes` is enforced before parsing either way. The profiler always parses with the limits lifted.
- **Why.** The parser reports each limit under its own error code, and the codes do not hold across versions. The census reports that size alone trips no limit.
- **Status.** `built`.

### §4s.17 `document.match` is a path glob

- **Decision.** `*` never crosses `/`, `**` is a whole segment, and a bracket set never matches `/`. Matching is case-sensitive and anchored. An empty glob, a leading `/`, a backslash, `**` inside a segment or an unclosed `[` fails the load.
- **Why.** The earlier filename matcher let a glob meant for the root's documents read every nested one too. Each refused form can only mean something other than what it says.
- **Status.** `built`.

### §4s.18 Opening a bundle lists only its own prefix

- **Decision.** Opening one directory-tree bundle in S3 lists only that bundle's prefix, in two listing calls: one existence probe and one listing of the bundle. Discovery at depth 1 is one delimited listing call, and finding a zip bundle is one metadata request and no listing. Locally, discovery walks only to the bundle depth. The whole source is listed only when the root itself is the one bundle.
- **Why.** The cost of opening one bundle must not grow with the number of bundles beside it.
- **Where else.** The call counts are asserted against an in-process S3 double, not a real store.
- **Status.** `built`.

### §4s.19 Maintenance keeps an index on the bundle column, and the index may never fail maintenance

- **Decision.** Maintenance creates a BTREE index on `source_bundle` once and folds later appends into it. It reports `created`, `updated`, `unsupported` or `failed` with the error type. A failure is a warning, and compaction and cleanup still run.
- **Why.** Every write starts by deleting a bundle's rows, which is a full scan without the index. The index is an optimization, so it must not cost the maintenance it serves.
- **Status.** `built`.

### §4s.20 A JSON document with nothing in it is no rows, not a reject

- **Decision.** Zero bytes, an empty object, an empty array and a bare scalar give no rows. Text that is not JSON is a reject, and so are `NaN` and `Infinity`. A number too large to be finite is kept as text and refused by a float column as a counted failure. The reject message names a line and column and quotes nothing. A UTF-8 byte-order mark is accepted.
- **Why.** An empty document is a fact about the data, and rejecting it would put a reject row against every placeholder file a producer writes.
- **Status.** `built`.

### §4s.21 The JSON route is held to the XML route row for row

- **Decision.** A synthetic JSON module re-spells the synthetic XML bundle, and a test asserts that both produce the same tables. A dotted chain of keys skips JMESPath and must answer what JMESPath answers on every node, including a missing key, a null and a quoted key. JSON numbers arrive typed: `3` and `3.0` fill a 32-bit integer column, and `3.5` is a counted failure, never truncated.
- **Why.** The equivalence test is what shows the XML guarantees hold for JSON, rather than asserting it.
- **Status.** `built`.

### §4s.22 A rows chain is how a column reaches another level

- **Decision.** A column reads a parent level with `from:`, and the root is always available. `index_of:` gives a zero-based position within that level that restarts under every parent. A missing level, a non-root `from:` on a plain-string `rows`, a duplicate level name, and `split` on an `index_of` column fail the load. A JSON mapping has no variables.
- **Why.** JMESPath has no parent axis and no variables, so the chain is the only way a JSON column reads its parent. A counter running across parents would look right in any test with one parent.
- **Status.** `built`.

### §4s.23 An `extract` miss is null and counted, and nothing stands in for it

- **Decision.** A value the pattern does not match is null, is counted, and bypasses the empty value, the map's null entry and the map's default. With `split`, the pattern runs on each part after the cut.
- **Why.** The source had a value the pattern did not describe, and a default would publish an answer the document never gave.
- **Status.** `built`.

### §4s.24 `split` is applied before a member attribute is read

- **Decision.** `split` is dispatched before the member branch, so a split member attribute gives its parts. `prefer` on a split member is accepted and on an unsplit one refused.
- **Why.** With the member branch first, a split member column typed as a list stored the string's characters, with no error and no count.
- **Status.** `built`.

### §4s.25 `basename_in` looks in exactly one folder

- **Decision.** It looks in the document's folder or the bundle root and never recurses, so it cannot be ambiguous. A reference that is not text is a counted failure, never turned into a path.
- **Why.** It serves references carrying a folder prefix the bundle does not have; the recursive strategy is unchanged.
- **Status.** `built`.

### §4s.26 A member table is written like any other table

- **Decision.** It holds one row per matching member per bundle and is counted and idempotent like the others; a bundle that now matches nothing clears its old rows. A zip member's modification time has no zone, so a timestamp column counts it as a failure until the column declares a zone. A member that cannot be read is a reject, the others still land, and the log line omits the error message.
- **Why.** Reading a zone-less time as UTC would shift every row from a producer in another zone.
- **Status.** `built`.

### §4s.27 The profiler quotes a JSON key that is not an identifier

- **Decision.** The profiler's path files quote such keys and write identifier keys as before.
- **Why.** The files can then be pasted into a mapping's expected paths unchanged.
- **Status.** `built`.

### §4s.28 Throughput is benchmarked, and the target is not met

- **Decision.** A bulk benchmark for each route prints rows per second and asserts the 20,000 rows per second target only behind an environment gate. The target is not to be reached by loosening a coercion rule.
- **Why.** The census reports both routes below the target on a development machine, with most of the time spent in per-value evaluation, and names a compiled reader as the next step.
- **Where else.** The census figures have no recorded run, so the benchmark is `built`; the target is `not built`.
- **Status.** `built` for the benchmark; the target `not built`.

### §4s.29 The mapping is checked at the load; the source and destination only in a run

- **Decision.** A malformed mapping, an unknown column type or a lookup naming no index fails the code-location load and lists every problem. Neither the source nor the destination is contacted while the code location loads.
- **Why.** A source or object store that is down must not take the code location with it.
- **Status.** `built`.

### §4s.30 Partitions are bundles, and two bundles may never share a key

- **Decision.** A bundle's partition key replaces `/` with `__` and refuses Dagster's invalid substrings. The sensor raises naming both bundles when two map to one key. A run reads its bundle from a run tag, which must agree with the partition.
- **Why.** Otherwise one bundle would silently shadow the other.
- **Status.** `built`.

### §4s.31 The discovery sensor and the maintenance schedule start stopped

- **Decision.** Both default to stopped. The sensor adds at most a set number of partitions per tick and does not request a bundle that already has a partition; a changed bundle is re-materialized by hand.
- **Why.** The re-request limit is stated in the docs rather than half-built.
- **Status.** `built`.

### §4s.32 Every check warns and none blocks

- **Decision.** `reject_rate` and `unknown_paths` run on `_rejects`, `file_resolution_rate` on each table with a file column, and `non_empty` on every table, all at warning severity. The default thresholds are strict; the example loosens them in its own file and says why.
- **Why.** The checks report on the data without failing the run that wrote it.
- **Status.** `built`.

### §4s.33 The component file stays inside the 1.10.19 floor

- **Decision.** The component file uses only `type:` and `attributes:`, with plain fields validated by models that forbid extra keys. Environment values are spelled as calls. The component module does not postpone annotation evaluation. Jobs select assets by explicit key.
- **Why.** This is the same floor discipline as §4j. The census reports that the example loads and materializes on 1.10.19 and on 1.13.1.
- **Where else.** The default test suite does not run on 1.10.19.
- **Status.** `built`.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Test files are under `dag_tools_tests/`. `eng` is `dag_tools/documents/engine.py`, `map` is `dag_tools/documents/mapping.py`, `rdr` is `dag_tools/documents/reader.py`, `src` is `dag_tools/documents/sources.py`, `lw` is `dag_tools/documents/lance_writer.py`, `coe` is `dag_tools/documents/coerce.py`, `fil` is `dag_tools/documents/files.py` and `dt` is `dag_tools/components/document_tables/`. `tc` is `test_document_tables_component.py`, `te` is `test_documents_engine.py`, `tm` is `test_documents_mapping.py`, `tl` is `test_documents_lance_writer.py`, `ts` is `test_documents_sources.py`, `tp` is `test_documents_profiler.py`, `tj` is `test_documents_json.py` and `tf` is `test_documents_features.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4s.1 format only in the mapping | `examples/document_tables/`; `dag_tools_tests/documents_synthetic.py` | AGENTS.md §4s, bullet 1 | tm: `test_no_deployment_vocabulary_appears_in_the_library` (`:240`); tc: `test_the_examples_mapping_is_the_synthetic_mapping` (`:450`), `test_each_example_bundle_is_exactly_the_synthetic_bundle` (`:456`) | `built` |
| §4s.2 pure engine; optional extra; lazy export | `pyproject.toml:231-250`; `dag_tools/__init__.py:9`, `:24`, `:38` | bullet 2 | tm: `test_importing_the_engine_modules_imports_no_dagster` (`:253`), control `:268`; no test of a base install | `built` |
| §4s.3 no value at INFO; redact | eng `:1000-1003`, `:1256`, `:1302`; dt `schema.py:165` | bullet 3 | te: `test_no_log_record_at_info_or_above_carries_a_document_value` (`:912`), controls `:925`, `:932`; `test_redacted_reject_text_carries_no_document_value` (`:766`), control `:776`, `:784`, `test_redaction_does_not_hide_the_engines_own_document_errors` (`:795`) | `built` |
| §4s.4 coercion never raises | coe `:84`, `:99-107`, `:129-136`, `:140-154`, `:190`, `:197` | bullet 4 | te: `test_int32_coercion` (`:300`), `test_int64_coercion_is_exact` (`:316`), `:351`, `test_a_timestamp_is_stored_as_a_utc_instant` (`:398`), `test_format_with_no_offset_needs_a_timezone` (`:462`), `test_no_coercer_raises_on_garbage` (`:508`), `test_a_failed_coercion_never_carries_a_value` (`:514`), `:538` | `built` |
| §4s.5 delete then append; schema from the mapping | lw `:29-30`, `:41`, `:44`, `:192-245` (delete at `:223`) | bullet 5 | tl: `test_rerunning_a_bundle_changes_no_count_and_leaves_the_other_untouched` (`:101`), `test_begin_bundle_with_no_rows_creates_every_dataset_with_the_mapping_schema` (`:157`), `test_a_changed_column_type_raises_naming_the_column_and_deletes_nothing` (`:280`), `:295`, `:207`, `:213`, `:258`; te `:1002`; tc: `test_rematerializing_a_partition_leaves_every_count_unchanged` (`:227`) | `built` |
| §4s.6 concurrency 1; pool named by digest | dt `component.py:104-112`, `:137-146`; dt `schema.py:164` | bullet 6 | tc: `test_the_pool_is_named_from_the_destination_and_limited_to_write_concurrency` (`:383`; skips where the instance cannot report pool limits); no test of the concurrency figures | `built` |
| §4s.7 ambiguous reference stores no bytes | fil `:31-32`, `:57-100` | bullet 7 | tl: `test_ambiguous_and_missing_files_store_no_bytes` (`:239`); te: `test_photo_file_resolution_counts_match_the_variant` (`:156`) | `built` |
| §4s.8 member clamped; reference keeps `..` | src `:51`, `:59` | bullet 8 | ts `:256`, `:271`, `:279` | `built` |
| §4s.9 nested zip spooled seekably | src `:188`, `:265-268` | bullet 9 | ts: `test_a_zip_nested_in_a_zip_bundle_is_expanded_under_its_own_path` (`:134`) | `built` |
| §4s.10 XPath fast path | rdr `:25-30`, `:113`, `:134` | bullet 10 | tm `:142`, `:162`, `:203`, `:211` | `built` |
| §4s.11 profiler redaction | `dag_tools/documents/profiler.py` | bullet 11 | tp `:181`, `:187`, `:122` | `built` |
| §4s.12 variables once per document | eng `:233`, `:239` | bullet 12 | tm `:211`, `:294`, `:56` | `built` |
| §4s.13 scoped index | eng `:122`, `:157`, `:170`, `:179-180`; map `:126` | bullet 13 | te `:1038` with its control `:1057`, `:1090`; tm `:278`, `:285`, `:301` | `built` |
| §4s.14 `prefer` and `first_of` | map `:196`, `:719` | bullet 14 | te `:1117`, `:1138`, `:1144`, `:1150`; tm `:344` | `built` |
| §4s.15 `order: keys` | map `:128-130` | bullet 15 | te `:1097`, `:1104`; tm `:313` | `built` |
| §4s.16 parser-limit probe | rdr `:277-295` (probe at `:283`), `:321` | bullet 16 | te `:1168`, `:1176`, `:1182`, `:1188`, `:1194`, `:1202` | `built` |
| §4s.17 path glob | map `:250`, `:281-286`, `:336-346` | bullet 17 | te `:1247`, `:1252`, `:1256`; tm `:367`, `:389` | `built` |
| §4s.18 bundle listing | src `:532`, `:542`, `:614-617`, `:631-678` | bullet 18 | ts `:490`, `:507`, `:521`, `:564` (against an in-process S3 double) | `built` |
| §4s.19 bundle index | lw `:119`, `:142`, `:144`, `:300` | bullet 19 | tl `:425`, `:440`, `:452`, `:468` | `built` |
| §4s.20 empty JSON is no rows | rdr `:526` | bullet 20 | tj `:119`, `:141`, `:155`, `:226`, `:234` | `built` |
| §4s.21 JSON held to XML | rdr `:405-413`, `:469-472` | bullet 21 | tj `:181`, `:403`, `:420`, `:89` | `built` |
| §4s.22 rows chain | map `:184`, `:696-697` | bullet 22 | tf `:322`, `:386`, `:413`, `:418` | `built` |
| §4s.23 `extract` miss | eng `:119`, `:397-408` | bullet 23 | tf `:145`, `:168`, `:193` | `built` |
| §4s.24 `split` before member | eng `:320-327` | bullet 24 | tf `:529`, `:76` | `built` |
| §4s.25 `basename_in` | map `:142`; fil `:85-90` | bullet 25 | tf `:222`, `:236` | `built` |
| §4s.26 member table | `dag_tools/documents/` | bullet 26 | tf `:567`, `:594`, `:692` | `built` |
| §4s.27 profiler quotes JSON keys | `dag_tools/documents/profiler.py` | bullet 27 | tj `:522` | `built` |
| §4s.28 throughput benchmark | `dag_tools_tests/test_documents_engine.py:942`; `dag_tools_tests/test_documents_json.py:695` | bullet 28 | te: `test_throughput_on_a_bulk_manifest` (`:942`); tj: `test_json_throughput_on_a_bulk_manifest` (`:695`); the target is asserted only behind an environment gate | `built` |
| §4s.28 throughput target met | `dag_tools_tests/test_documents_engine.py:942`; `dag_tools_tests/test_documents_json.py:695` | bullet 28 | te: `test_throughput_on_a_bulk_manifest` (`:942`); tj: `test_json_throughput_on_a_bulk_manifest` (`:695`); the target is asserted only behind an environment gate | `not built` |
| §4s.29 mapping at the load | dt `component.py` | bullet 29 | tc: `test_an_unknown_key_in_source_fails_the_load` (`:426`), `test_a_mapping_with_a_validation_problem_fails_the_load` (`:436`), `test_a_missing_mapping_file_fails_the_load_naming_the_file` (`:442`); tm: `test_an_unknown_type_is_refused` (`:62`), `test_an_unknown_lookup_index_is_refused` (`:68`), `test_several_problems_are_all_reported_in_one_error` (`:74`); no test that the source and destination are not contacted at the load | `built` |
| §4s.30 partitions are bundles | dt `component.py:82`, `:91-101` | bullet 30 | tc `:168`, `:207`, `test_a_tag_that_disagrees_with_the_partition_fails_the_run` (`:215`), `test_two_bundle_keys_encoding_to_one_partition_fail_the_tick` (`:336`), `:353` | `built` |
| §4s.31 sensor and schedule stopped | dt `schema.py:113-114`, `:140`; dt `component.py:444`, `:589` | bullet 31 | tc: `test_the_shipped_example_loads` (`:150`; the sensor is asserted stopped at `:156`), `test_a_cron_adds_a_stopped_maintenance_schedule` (`:160`), `test_the_tick_is_bounded_by_max_new_partitions_per_tick` (`:305`), `test_a_changed_bundle_of_a_known_partition_is_not_requested_again_by_the_sensor` (`:321`) | `built` |
| §4s.32 checks warn | dt `component.py:198-222`, `:322-368` (severity at `:324`); dt `schema.py:89`, `:92`, `:98`, `:101` | bullet 32 | tc: `test_default_thresholds_fail_the_checks_as_warnings_and_the_run_succeeds` (`:243`), `:260`, `:267` | `built` |
| §4s.33 floor discipline | dt `component.py:23`, `:578`; dt `schema.py` | bullet 33 | tc: `test_env_references_are_spelled_as_calls` (`:474`), `test_the_shipped_example_loads` (`:150`); no run on 1.10.19 in the default suite | `built` |
