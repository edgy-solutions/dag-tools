# dag-tools decisions: §4r, §4l, §4m

This record covers the decisions in `AGENTS.md` sections 4r, 4l and 4m at the component-records pin (`add5bc4`): the curated measurement slice, the classification dimensions file, and the warehouse source. The curated slice sits on the source described in the [SystemLink source record](../components/systemlink-source.md) and is built by the [dbt record](../components/dbt.md)'s tooling from documents the [contract mapping record](../components/contract-mapping.md) describes. The dimensions file and the source that reads it are described in the [warehouse source record](../components/warehouse-source.md) and the [classification propagation record](../components/classification-propagation.md). [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §4r The curated slice

Two conformed contract documents are the seam. A deployment's own mapping produces them, and that mapping is the one place a source's names appear. A shipped dbt package derives three curated models from them and names nothing from the source. The rule module, `dag_tools/systemlink/measurements.py`, imports no Dagster, dbt, dlt, database or template library, and both routes read it.

### §4r.1 One rule table, two routes, and two tests that each catch what the other cannot

- **Decision.** One rule table is read by the Python evaluator and by the SQL generator. One test checks that every shipped `.sql` file is byte for byte what the generator emits. A second runs every measurement case through both routes over an embedded database and checks that they agree. A third checks that the cases do not all give the same answer. The byte comparison reads bytes and decodes them; it does not read the file as text.
- **Why.** The byte test alone passes on a generator whose SQL is wrong in the same way as its template. The agreement test alone passes on a shipped file nobody regenerated. Reading as text translates line endings, so a file re-saved with another platform's line endings would compare equal, and the one difference a deployment installs would be invisible.
- **Status.** `built`.

### §4r.2 The shipped file set is what the generator emits plus a named hand-written set

- **Decision.** The test compares the shipped files with the generator's output joined to a named list of hand-written files. Each hand-written entry must exist and must not be something the generator emits.
- **Why.** Skipping whatever the generator does not emit would pass on a stray file left in the package, which is what the byte test exists to catch.
- **Status.** `built`.

### §4r.3 A cast to a double is not a finiteness check; the guard is that zero times the value is zero

- **Decision.** Number text is accepted on the SQL route only when the value times zero equals zero. That is true of every finite value and false of both infinities and of not-a-number. The tolerance path that uses an exact decimal type raises on such text instead, and no portable expression catches that.
- **Why.** The census reports that, on the embedded database, a cast of an overflowing number gives infinity where the Python route gives no value. The two routes would disagree, and the SQL side would publish an infinite limit. The generator's own note adds that postgres raises on the double cast, as every decimal cast in the tolerance arms does, so the limit belongs to the approach and not to one engine.
- **Where else.** The raising tolerance path is the stated limit, and no test covers it. The census's database readings are carried as the census's; no run is recorded by this lane.
- **Status.** `built`.

### §4r.4 The package directory avoids a name the repo's ignore rules match

- **Decision.** The package ships under `dag_tools/dbt_project_packages/`, not under the name dbt uses for installed packages.
- **Why.** The repo's ignore file matches that name, and the wheel builder honours ignore files, so a package under it would have been left out of the wheel. The census reports building the wheel and finding all eleven files. The test runs the version-control ignore check over every shipped file.
- **Status.** `built`.

### §4r.5 An operator the comparator table does not cover is counted, at warning severity

- **Decision.** A bound whose operator the deployment's comparator table does not cover is kept as text and counted. The test that counts them is set to warn, not fail. It is not a parse failure, which means a number was expected and not found.
- **Why.** At failing severity, the first unfamiliar operator in a real source stops the build, and the pressure becomes deleting the row rather than adding to the table. The two counts go to different people.
- **Status.** `built`.

### §4r.6 Operators match whole, and the comparator table is the deployment's

- **Decision.** A table entry for one operator does not cover a longer operator that starts with it. The library ships no comparison codes, only the two words for the low and high side. The SQL lookup renders a CASE over the deployment's table with every value's quotes doubled. An empty table renders a typed null, which is what the Python route gives with no table.
- **Why.** Reading a stricter operator as a looser one publishes the right number on the right side with the wrong strictness, and that reads as correct. Matching the empty case keeps the two routes agreeing rather than one failing to compile.
- **Status.** `built`.

### §4r.7 The latest result is the latest terminal attempt

- **Decision.** The latest flag marks the highest terminal attempt for a unit. A unit with no terminal attempt has no latest row.
- **Why.** Taking the highest attempt of any state would serve a reader a row whose result is not in yet.
- **Status.** `built`.

### §4r.8 The package declares no default for a required variable

- **Decision.** Four variables are required: two relations, the warehouse's exact numeric type, and the comparator table. The package declares no default for any of them, so an unset one fails the dbt parse. The tests render with undefined variables raising, and check in both directions that the variables the SQL reads are the ones declared required.
- **Why.** A default would be either a name from the source or a wrong type.
- **Status.** `built`.

### §4r.9 The example's sample comparator table is run, not read

- **Decision.** Every row of the example's sample table is executed on both routes. An operator the sample deliberately leaves out comes back uncovered. The sample carries a multi-character operator and a non-ASCII one.
- **Why.** A sample nothing runs passes vacuously. The two kinds of operator make the per-row test say something about the whole-operator rule.
- **Status.** `built`.

### §4r.10 The first stage is shipped, read through the transforms it needed

- **Decision.** The two conforming mappings ship in the example and read one shared normalizer document, so a part number is conformed the same way in both tables. They use the transforms the stage waited for: normalizing against a document, a membership test, a fallback with a constant default, null-if, one load timestamp, and a source row filter.
- **Why.** The stage was not shipped until those transforms existed. A constant standing in for the membership test would have said every attempt is terminal.
- **Where else.** The transforms are §4k decisions.
- **Status.** `built`.

### §4r.11 The grain and reference tests are generated and tested both ways

- **Decision.** The grain and orphan-reference tests are generated from two tables in the generator. Each clean-direction test has a partner that plants a duplicate or an orphan and checks the test finds it. Both rule ids are declared by every curated contract document.
- **Why.** A clean test alone passes on an empty model; the planted partner also proves the model has rows. A test naming a rule outside the contract cannot be acted on by a reader of the contract.
- **Status.** `built`.

### §4r.12 The test inputs are not named after the real inputs

- **Decision.** The tests' input relations carry neutral names, not the conformed assets' names.
- **Why.** A model that hard-coded a relation name instead of reading its variable then fails the test instead of happening to work.
- **Status.** `built`.

## §4l The classification dimensions file

These are the rules every reader of the dimensions file is built to. The warehouse source (§4m) is the first reader. The census places the file's ownership with the policy repo, under a decision of that repo's numbered series.

### §4l.1 dag-tools ships no classification file and reads the deployment's copy

- **Decision.** The file is owned by the policy repo and served to dag-tools as a mounted configuration file. dag-tools reads it from the mount, located by an environment variable with a default mount path. The top-level key holds a list of dimensions. Each dimension has an id, a flag saying whether it is hierarchical, and an ordered list of values. Each value carries an id, a reason and who ratified it.
- **Why.** Code reads what the server serves rather than a copy that can disagree with it. The mount is unchanged from when the file held one ordered list of levels, because the hierarchical dimension is that list.
- **Where else.** A mount path is a location, not a name from the source, which is why it may have a default.
- **Status.** `built` for the reader. The file's content and its mount in a chart are owned by the policy repo and not established here.

### §4l.2 Exactly one dimension is hierarchical, or the load fails

- **Decision.** A document that declares no hierarchical dimension, or more than one, fails validation.
- **Why.** The ceiling rule, "most restrictive" and the composition downstream are each defined only on a ladder. Failing also means dag-tools learns which dimension is the ladder from the flag, never from a name it carries.
- **Status.** `built`.

### §4l.3 Order is list position; a clearance is a ceiling on the ladder and a declared membership test elsewhere

- **Decision.** Within a dimension, the first value is the least restrictive. On the hierarchical dimension a clearance is a ceiling: a reader cleared at one value reads it and every earlier one. On any other dimension, the kind of check is declared: a permissive set needs the reader to hold at least one of the values, a restrictive set needs all of them, and an exclusionary one needs none. Access is the conjunction across dimensions. A dimension with no declared kind is a restrictive set.
- **Why.** An exact match on the ladder would deny a cleared reader everything below their level. The undeclared kind keeps a document already mounted meaning what it meant before the kinds existed.
- **Where else.** The check against a reader's clearances, and how a clearance is spelled, are policy-side. dag-tools builds the ordering and the per-kind comparison; the enforcement at read time is not established here.
- **Status.** `built` for the ordering and the kinds in dag-tools.

### §4l.4 An absent or empty file means nothing is served

- **Decision.** With no document, or a document with no usable dimensions, no label validates and no most-restrictive label exists, so a source cannot publish. With a usable document, most restrictive means the last rung of the ladder, every value of each restrictive and exclusionary dimension, and no value of a permissive one.
- **Why.** A missing mount looks the same as a failed one, and reading it as "nothing is restricted" would serve a governed column to someone with no clearance. The file may legitimately ship with nothing ratified yet. A permissive set is the one kind where the empty set is the most restrictive, because a reader needs one of its values.
- **Status.** `built`.

### §4l.5 No dimension id or value appears in dag-tools

- **Decision.** No dimension id and no value appears in the code as a constant, a default or a comment. The hierarchical dimension is found by its flag. A component taking a label validates every dimension and value against the mounted file and fails the load on anything the file does not declare.
- **Why.** The name of the ladder is as much a governance decision as its values; compiled into the library, it would turn a board decision into a release.
- **Where else.** No test asserts the absence. This lane searched the warehouse source package and both components for the example document's ids at the pin and found none.
- **Status.** `built`.

### §4l.6 A column label maps dimensions to values, and the ladder is required

- **Decision.** A column's label maps each dimension id to a value or a list of values, with an optional cell predicate. The hierarchical dimension is required and takes exactly one value. A column with no entry is most restrictive in every dimension. A column whose entry leaves out the ladder fails the load.
- **Why.** A list on the ladder is ambiguous, so it is refused rather than interpreted. Defaulting a half-written label is how a column gets labelled by accident, so the two cases stay distinct.
- **Status.** `built`.

### §4l.7 Propagation composes each dimension in its own algebra, and no downgrade is automatic

- **Decision.** No output column is less restricted than the source columns it is computed from. Read through each kind, that is the maximum on the ladder, the union on a restrictive or exclusionary set, and the intersection on a permissive one. Column lineage is the input; with none, the table's maximum applies. A downgrade names the dimension, the value, a reason and a steward. Each application is recorded with the rule, the owner, both labels and the reason.
- **Why.** A wider permissive set admits more readers, so a union there would publish a derived column as readable by people who could read neither source.
- **Where else.** §4m.10 to §4m.14 and the classification propagation record.
- **Status.** `built`.

### §4l.8 The asset of record is the physical dataset

- **Decision.** The catalog registers the physical platform's dataset and attaches the Dagster asset as lineage.
- **Where else.** [ADR-0002](../../adr/ADR-0002-physical-urn-is-the-catalog-urn.md) holds this decision; §4m.1 is how the warehouse source enforces it.
- **Status.** `built`.

## §4m The warehouse source

One component instance is one labelled source dataset, observed rather than moved. The propagation engine beside it is what §4l.7 and the classification propagation component compose with.

### §4m.1 The asset of record is the physical dataset, and a Dagster dataset address is refused at the load

- **Decision.** The configured dataset address must name the physical platform; one on the Dagster platform fails the load. The Dagster asset attaches as lineage, with the physical dataset upstream. The fabric of the lineage node is read off the operator's own address.
- **Why.** Otherwise the catalog describes one object, a crawler finds a second, and a broker asked about the physical one finds no labels. A lineage node in the wrong fabric is a second object again.
- **Status.** `built`.

### §4m.2 The dimensions document is read at materialization, and an absent one fails the run

- **Decision.** The document is re-read when the source is observed. An absent document fails the run, not the load. A label the document contradicts fails the load. The published label set names the document and its digest.
- **Why.** The mounted document is the authority at the moment of the read. One missing mount failing the load would take down every other asset in the code location. Publishing only happens in the run, so deny-by-default is not weakened.
- **Status.** `built`.

### §4m.3 Reading column names is a separate call from reading rows

- **Decision.** The resource has a column-reading call separate from its row-reading call. It reads the result's keys and falls back to the cursor description. A schema read is refused at the same point as any other read if it cannot carry its tag.
- **Why.** The census reports that, on an embedded database, a query matching no rows yields no rows, so names taken from the first row report zero columns. A correct, labelled, empty source would then look renamed.
- **Status.** `built`.

### §4m.4 The observation takes no run config

- **Decision.** Every setting arrives as a YAML attribute. The owner travels as metadata and to the catalog.
- **Why.** Dagster calls an observable source asset's function with the context and resources only, so a config parameter fails at run time. The census reports this on both Dagster versions it ran. The observable source asset also has no owners parameter.
- **Status.** `built`.

### §4m.5 A job selecting all assets does not include source assets

- **Decision.** Anything that observes the source names its keys. The example README says so.
- **Why.** The census reports that a job built from an all-assets selection runs zero steps and reports success.
- **Where else.** No test is named for this, and it is documented rather than checked. The tests that observe the source do follow it: the component tests' shared observe helper and the propagation example runner both name the keys rather than selecting all assets.
- **Status.** `built` (documented).

### §4m.6 A catalog failure fails the materialization

- **Decision.** If the catalog publish fails, the materialization fails. A dry run records the payload and emits nothing. No token key is sent when there is no token.
- **Why.** Here the publish is the deliverable. A source whose labels never reached the catalog is one a broker answers unlabelled, which serves nothing and looks like an outage somewhere else. The dbt publisher makes the opposite choice because its deliverable is the tables (§4i).
- **Status.** `built`.

### §4m.7 No schema aspect is emitted

- **Decision.** Labels ride on the editable schema aspect as glossary terms per field. The crawler's schema aspect is not emitted.
- **Why.** The observation reads only column names, no types, and an aspect asserting an unread shape is one a crawler would then disagree with.
- **Status.** `built`.

### §4m.8 The ratification gate checks values, not dimensions

- **Decision.** The opt-in ratification gate refuses the publish when a declared value has nobody's name against it, and names those values. Unratified values are reported in the observation metadata whether or not the gate is on.
- **Why.** A similarly named check reports a dimension that declares no values at all; gating on it could never fire. A value awaiting ratification is a legitimate state while the board signs rows.
- **Status.** `built`.

### §4m.9 An unlabelled column is counted; a labelled column the source lacks fails

- **Decision.** A source column with no label is published most restrictive, counted in the metadata, and warned about. A labelled column the source no longer has fails the observation. Columns are matched without regard to case, the source's spelling is published, and two source columns differing only by case are refused.
- **Why.** A missing label under-serves. A stale label means the YAML describes another table; publishing it would govern a name that no longer exists while the renamed column is served to nobody.
- **Status.** `built`.

### §4m.10 Composition is one order read four ways

- **Decision.** One comparison asks of every kind whether a label admits at least every reader another admits. Composition is the least upper bound under it, which works out as §4l.7 describes. The most and least restrictive labels come from the same order.
- **Why.** Four separate rules is how one ends up pointing the wrong way, and the permissive set is where intuition from the other three is backwards.
- **Status.** `built`.

### §4m.11 A derived label carries every dimension; a declared one may not

- **Decision.** A derived label always carries every dimension. A declared label, from an operator or a downgrade rule, carries the ladder and whatever else it means to say. A downgrade rule is overlaid on the derived label, so a dimension it leaves out keeps what the data derived.
- **Why.** On a permissive set, "absent" and "empty" mean opposite things, and a value lookup cannot tell them apart. Compared directly, every dimension a rule leaves out would look like a tightening.
- **Status.** `built`.

### §4m.12 An unlabelled source column fails propagation

- **Decision.** A source column with no label fails propagation. So do a lineage edge from a source nothing labelled, an asset with no labelled input, and two source columns differing only by case.
- **Why.** Deny-by-default is applied where the source is declared. At propagation, a missing label means lineage and labels disagree, and composing the labels that happen to exist gives a ceiling from a subset of inputs that looks correct.
- **Status.** `built`.

### §4m.13 A column with no lineage gets the table maximum

- **Decision.** An output column with no declared lineage carries the composition of every labelled column of every source read, and the result names those columns. Composing no labels at all is refused.
- **Why.** Without lineage, the whole table was read as far as anyone can prove. The least restrictive label in every dimension is the one answer that must never be reached.
- **Status.** `built`.

### §4m.14 A downgrade must be accountable and must relax

- **Decision.** A rule is refused if it has no owner, no reason, no columns, a label outside the document, or no ladder value. Also refused: two rules for one column, a rule for a column the asset does not produce, a rule that tightens any dimension, and a rule that changes nothing. Each application records the rule, owner, reason and both labels.
- **Why.** Which rule is credited decides who is accountable. A rule for a missing column is a permission nobody watches. A tightening would be recorded as a release that never happened, and a no-op tells an auditor a downgrade happened when none did.
- **Status.** `built`.

### §4m.15 The engine imports no Dagster, catalog or warehouse library, and the test restores what it evicts

- **Decision.** A test evicts the package's modules, re-imports them and checks that no Dagster, catalog, database or dlt module was loaded. It restores the evicted modules afterwards and checks the restore.
- **Why.** A re-imported module is a new module object with new classes, so without the restore another test's check for this package's errors would stop matching, and only in some test orders.
- **Status.** `built`.

### §4m.16 The observation moves no data, and its statements are the operator's

- **Decision.** The default statements count rows and name columns. An optional as-of statement is sent verbatim. The row count is read by position and exactly, with no float path. A grouped count is refused, an absent as-of value is refused, and a relation containing a statement separator fails the load. The schema hash depends on column order.
- **Why.** A float loses precision on large counts. Summing a grouped count or recording an absent as-of as empty would publish a wrong value as a correct one. A reordered table is a changed table.
- **Status.** `built`.

### §4m.17 Nothing names a database object, dimension or value

- **Decision.** The package and component name no database, schema, table, column, dimension or value. The example's vocabulary belongs to its own database file. The example runs with no warehouse and no credentials, loads on the oldest supported Dagster, and spells environment variables in the form that version resolves.
- **Why.** As §4l.5.
- **Where else.** No test asserts the absence; see §4l.5 for the search this lane ran.
- **Status.** `built`.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Test files are under `dag_tools_tests/`. `meas` is `dag_tools/systemlink/measurements.py`, `sql` is `dag_tools/systemlink/sql.py`, `dim` is `dag_tools/warehouse_source/dimensions.py`, `prop` is `dag_tools/warehouse_source/propagate.py` and `ws` is `dag_tools/components/warehouse_source/`. `cur` is `test_systemlink_curated_sql.py`, `conf` is `test_mfg_trace_conform.py`, `eng` is `test_warehouse_source_engine.py`, `wprop` is `test_warehouse_source_propagation.py`, `wcomp` is `test_warehouse_source_component.py` and `tw` is `test_tagged_warehouse_resource.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4r.1 one rule table; byte identity and agreement | meas `:13-17`, `:120`; sql `:44`, `:723` | AGENTS.md §4r, bullet 1 | cur: `test_every_generated_file_is_shipped_byte_for_byte` (bytes read at `:379`), `test_every_measurement_case_agrees_between_the_two_routes`, `test_the_cases_are_not_all_the_same_answer` | `built` |
| §4r.2 file set with a hand-written list | cur `:84` | bullet 2 | cur: `test_the_shipped_sql_file_set_is_exactly_what_the_generator_emits` | `built` |
| §4r.3 the times-zero guard; decimal path raises | sql `:201-225`, `:364`; meas `:199` | bullet 3 | cur `:466`; `test_systemlink_measurements.py` `:158`, `:164`; no test of the raising decimal path | `built` |
| §4r.4 package path outside the ignore rules | `.gitignore:80`; sql `:873`, `:876` | bullet 4 | cur: `test_the_shipped_package_is_on_a_path_no_vcs_ignore_rule_matches` | `built` |
| §4r.5 uncovered operator counted, warns | sql `:673-687` | bullet 5 | cur: `test_the_uncovered_comparator_test_warns_rather_than_failing`, `test_an_uncovered_bound_is_not_a_parse_failure`; `test_systemlink_measurements.py:309` | `built` |
| §4r.6 whole-operator match; quoting; empty table | meas `:32-33`, `:73`, `:76`; sql `:128-130`, `:272`, `:297-299`, `:302-329` | bullet 6 | cur: `test_a_comparator_code_carrying_a_quote_survives_the_render`, `test_an_uncovered_operator_yields_no_bound_on_either_route` | `built` |
| §4r.7 latest is terminal-only | sql `:456`, `:487`, `:494-498` | bullet 7 | cur: `test_the_result_model_marks_the_latest_TERMINAL_attempt`, `test_a_unit_with_no_terminal_attempt_has_no_latest` | `built` |
| §4r.8 no default for a required variable | sql `:55-60`; the package has no `vars:` block | bullet 8 | cur: `test_the_package_declares_a_default_for_no_required_variable`, `test_every_variable_the_shipped_sql_reads_is_declared_required`, `test_an_undeclared_variable_is_not_quietly_rendered` | `built` |
| §4r.9 sample comparator table run | `examples/mfg_trace/curated/dbt_project.yml:81` | bullet 9 | cur: `test_every_row_of_the_examples_sample_table_bounds_what_it_says`, `test_the_example_covers_both_spellings_of_each_bound`, `test_an_operator_the_examples_table_omits_is_uncovered_not_guessed` | `built` |
| §4r.10 first stage shipped | `examples/mfg_trace/conform/components/` (two mappings and the normalizer document) | bullet 10 | conf: `test_both_conformed_mappings_load_with_no_problems`, `test_every_contract_column_is_produced`, `test_the_membership_transform_decides_terminal_and_nothing_else_does`, `test_the_library_names_nothing_the_mappings_carry` | `built` |
| §4r.11 grain and reference tests both ways | sql `:753`, `:767`, `:775-776`, `:792`, `:827` | bullet 11 | cur `:601`, `:607`, `:623`, `:629` | `built` |
| §4r.12 neutral input names | cur `:90-91` | bullet 12 | cur `:815` | `built` |
| §4l.1 reads the mount; document shape | dim `:43`, `:48`, `:51`, `:87-102`, `:639-655` | AGENTS.md §4l, bullet 1 | eng: `test_explicit_configuration_beats_the_ambient_variable` | `built` for the reader; the file's content and mount not established here |
| §4l.2 exactly one ladder | dim `:433-455` (count checked at `:444-446`) | bullet 2 | eng: `test_exactly_one_dimension_is_the_ladder` | `built` |
| §4l.3 list order; ceiling; three set kinds | dim `:65-80`, `:211`, `:225-235`, `:290-306` | bullet 3 | wprop: `test_an_untyped_dimension_still_means_what_it_always_meant` and `:209`, `:216`, `:235`; read-time enforcement not established here | `built` for the ordering and the kinds in dag-tools |
| §4l.4 absent or empty serves nothing | dim `:237-264` (permissive empty at `:260`), `:396-431`, `:457-473`, `:521-583` | bullet 4 | eng: `test_an_empty_document_is_unusable_rather_than_permissive`, `test_a_dimension_declaring_no_values_makes_the_document_unusable`, `test_the_most_restrictive_label_is_the_top_rung_and_EVERY_facet`; wprop: `test_the_most_restrictive_permissive_set_is_empty_and_releases_to_nobody` | `built` |
| §4l.5 no id or value named | dim `:501` (ladder found by flag), `:521-567` | bullet 5 | eng: `test_a_value_the_document_does_not_declare_is_refused`; absence by search, no test | `built` |
| §4l.6 column label shape | `dag_tools/warehouse_source/spec.py:93-130`, `:280-286`; dim `:550-553` | bullet 6 | eng: `test_a_list_on_the_ladder_is_refused_rather_than_interpreted`, `test_a_label_missing_the_ladder_is_refused_not_defaulted`, `test_an_unlabelled_column_is_published_most_restrictive` | `built` |
| §4l.7 per-dimension composition; accountable downgrade | dim `:312-339`; prop `:190-236`, `:305-379` | bullet 7 | as §4m.10 to §4m.14 | `built` |
| §4l.8 asset of record | ADR-0002 | bullet 8 | as §4m.1 | `built` |
| §4m.1 physical address; Dagster address refused; lineage | `dag_tools/warehouse_source/spec.py:74`, `:144`, `:170-180`; ws `component.py:257-273` | AGENTS.md §4m, bullet 1 | wcomp: `test_a_dagster_platform_urn_fails_the_load`, `test_the_physical_dataset_is_the_entity_every_label_is_attached_to`, `test_the_lineage_edge_runs_FROM_the_physical_dataset_TO_the_asset`, `test_the_asset_key_in_the_lineage_node_is_the_prefixed_key`; eng: `test_the_asset_of_record_may_not_be_a_dagster_urn` | `built` |
| §4m.2 read at materialization; absent fails the run | ws `component.py:347-368`, `:370-387` | bullet 2 | wcomp: `test_an_absent_document_loads_and_then_fails_the_run_naming_the_path`, `test_a_label_the_document_does_not_declare_fails_the_LOAD`; eng: `test_the_published_label_set_says_which_document_ratified_it` | `built` |
| §4m.3 separate column read | `dag_tools/resources/warehouse.py:430-445` | bullet 3 | tw: `test_an_empty_result_set_still_names_its_columns`, `test_a_result_that_refuses_keys_falls_back_to_the_cursor_description`, `test_fetch_columns_is_a_separate_seam_from_fetch_rows`, `test_a_schema_read_still_has_to_carry_its_tag` | `built` |
| §4m.4 no run config | ws `component.py:38-47`, `:369` | bullet 4 | wcomp: `test_the_observe_function_takes_no_run_config` | `built` |
| §4m.5 all-assets selection excludes source assets | `examples/warehouse_source/README.md:35` | bullet 5 | documented; no test by name, but wcomp's shared observe helper (`:95-123`) and `test_classification_propagation.py:1042` name the keys rather than selecting all assets | `built` (documented) |
| §4m.6 catalog failure fails; dry run; no empty token | ws `component.py:169-178`, `:185-187`, `:277-293`, `:427-443` | bullet 6 | wcomp: `test_a_catalog_failure_FAILS_the_materialization`, `test_an_empty_token_is_never_sent_as_a_credential`, `test_dry_run_records_everything_and_emits_nothing` | `built` |
| §4m.7 no schema aspect | ws `component.py:198-202`, `:232-255` | bullet 7 | wcomp: `test_no_schema_aspect_is_emitted_because_only_names_were_read`, `test_every_term_on_a_column_reaches_the_field_it_labels` | `built` |
| §4m.8 ratification gate on values | ws `schema.py:93`; ws `component.py:321`, `:380-384`; dim `:473-495` | bullet 8 | wcomp: `test_require_ratified_refuses_the_publish_and_names_the_values`; eng: `test_values_with_no_signature_are_reported_not_refused` | `built` |
| §4m.9 unlabelled counted; stale fails; case | `dag_tools/warehouse_source/spec.py:241-332` | bullet 9 | eng: `test_an_unlabelled_column_is_published_most_restrictive`, `test_a_labelled_column_the_source_no_longer_has_fails_loudly`, `test_the_declared_spelling_is_only_how_the_column_was_found`, `test_columns_differing_only_by_case_are_refused` | `built` |
| §4m.10 one order | dim `:237-339` | bullet 10 | wprop: `test_composition_is_the_join_of_one_order_in_every_type`, `test_a_permissive_set_composes_by_INTERSECTION_not_union`, `test_the_most_restrictive_permissive_set_is_empty_and_releases_to_nobody`, `test_an_untyped_dimension_still_means_what_it_always_meant` | `built` |
| §4m.11 derived total; declared overlaid | dim `:343-362`; prop `:358-379` | bullet 11 | wprop: `test_a_derived_label_carries_every_dimension_even_an_undeclared_one`, `test_an_absent_dimension_and_an_empty_one_are_told_apart_by_declares`, `test_a_rule_says_nothing_about_the_dimensions_it_omits` | `built` |
| §4m.12 unlabelled source column fails | prop `:242-302`, `:382-403`, `:463-521` | bullet 12 | wprop: `test_an_unlabelled_source_column_fails_rather_than_counting_as_unrestricted`, `test_lineage_from_a_source_nothing_labelled_fails`, `test_propagating_with_no_labelled_input_at_all_is_refused`, `test_source_columns_differing_only_by_case_are_refused` | `built` |
| §4m.13 table maximum | prop `:179`, `:190-236`, `:242-302` | bullet 13 | wprop: `test_a_column_with_no_declared_lineage_falls_back_to_the_table_max`, `test_composing_no_labels_at_all_is_refused` | `built` |
| §4m.14 downgrade accountable and relaxing | prop `:82-127`, `:305-355`, `:406-460` | bullet 14 | wprop: `test_an_applied_downgrade_records_the_rule_owner_reason_and_both_labels`, `test_a_standing_rule_with_no_named_owner_fails`, `test_a_declared_downgrade_with_no_reason_fails`, `test_a_rule_that_tightens_a_dimension_is_not_a_downgrade`, `test_a_rule_that_tightens_only_a_set_dimension_is_still_refused`, `test_a_downgrade_that_changes_nothing_is_refused`, `test_a_rule_aimed_at_a_column_the_asset_does_not_produce_fails`, `test_two_rules_for_one_column_are_refused_rather_than_ordered` (also `test_classification_propagation.py:1235`) | `built` |
| §4m.15 engine purity; restore | eng `:556-576` | bullet 15 | eng: `test_the_engine_imports_no_dagster_no_datahub_no_warehouse` | `built` |
| §4m.16 no data moved; exact count | `dag_tools/warehouse_source/observe.py:79-94`, `:107-148`, `:162-251` | bullet 16 | eng: `test_a_row_count_past_float_precision_is_exact` and `:347`, `:357`, `:371`, `:392`, `:401`, `:406`, `:414`, `:424` | `built` |
| §4m.17 nothing named | the package and `ws` | bullet 17 | absence by search, no test | `built` |
