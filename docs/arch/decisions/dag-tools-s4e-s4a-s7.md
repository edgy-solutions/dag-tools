# dag-tools decisions: §4e, §4d, §4c, §4b, §4a (inventory), §7

This record covers the decisions in `AGENTS.md` sections 4e, 4d, 4c, 4b, the inventory section numbered 4a, and 7 at the component-records pin (`add5bc4`): the equivalence-class matrix, the qualification manifest, the survey, the qualification registry, the shared inventory contract, and the rule that keeps the three guide files in step. The inventory contract is read by the domain broker, described in the [domain broker record](../components/domain-broker.md), and every qualification step runs against Dagster, described in the [Dagster record](../components/dagster.md); there is no component record for the qualification system, and its procedure is in the [recipe](../../RECIPE.md). [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

Two census bullets describe more than the code does. The registry key rule in §4b.1 has one exception, and the field-level warning in §4a.4 is not logged by most field readers. Each entry says which part is `built`.

## §4e Equivalence-class matrix

The class matrix groups every pinned asset into equivalence classes, so that a qualification launches one representative per class rather than every asset. A class is keyed by the components that decide how an asset behaves under an upgrade.

### §4e.1 The class matrix reads the manifest, not the registry pointer

- **Decision.** The matrix is built from the qualification manifest's inventory pins. For each pinned repository and commit it reads that build's asset inventory and dbt project inventory. It never reads a repository's latest pointer.
- **Why.** A survey published in the middle of a qualification would otherwise change what is being qualified. The pins freeze the target.
- **Where else.** The pointer is read once, when a qualification is created and its pins are taken (§4d.1).
- **Status.** `built`.

### §4e.2 The class hash is deterministic

- **Decision.** A class hash is the first 12 hexadecimal characters of a SHA-256 over the key components, serialized with sorted keys. Unordered components are sorted before hashing.
- **Why.** Equal components must give an equal hash however they were assembled, or one class would appear as two.
- **Status.** `built`.

### §4e.3 A custom dbt translator always forms its own class

- **Decision.** An asset counts as a dbt asset when its inventory lists the Dagster dbt integration or its compute kind is dbt. The matrix joins that with the dbt project inventory's custom translator flag and attaches every custom translator in the repository to every dbt asset's key.
- **Why.** A custom translator can change how an asset is keyed and loaded. Attaching all of them may split classes more finely than needed, but it never merges two classes that differ.
- **Status.** `built`.

### §4e.4 Runnability comes from tags

- **Decision.** A class is runnable unless an asset carries the tag `synthetic_required` or `observe_only` set to true. The synthetic tag wins when both are set. Runnability is not inferred from an IO manager's class name.
- **Why.** Which backends can be staged differs between organisations, so an inference rule would be wrong for some of them. A rule of that kind would need its own manifest setting.
- **Status.** `built`.

### §4e.5 Staging need is reported, never gated, and unknown is not none

- **Decision.** Each representative is marked as needing staged upstream data, not needing it, or unknown. An inventory that does not record upstream keys gives unknown, never "none needed". An upstream counts as needing staging when any pinned repository computes it, and "required" outranks "unknown". The matrix reports how many representatives can launch today: runnable and needing no staging. The mark changes no runnability and no launch.
- **Why.** An inventory surveyed before the field existed would otherwise report that nothing needs staging, which reads as a clean result and is not one. Counting any repository that computes the upstream errs toward reporting too much.
- **Where else.** The supported ways to stage data are in the [recipe](../../RECIPE.md), in its section on staging data. The staging pass is §4f.11 in [dag-tools-s4g1-s4g-s4f.md](dag-tools-s4g1-s4g-s4f.md).
- **Status.** `built`. Generated seeder assets: `not built`.

## §4d Qualification manifest

A qualification starts by writing a manifest: the baseline and candidate deployments, the inventory pins, and the co-upgrade risks between the two dependency sets. Every later step reads it.

### §4d.1 The manifest is the contract for every later step

- **Decision.** Creating a qualification reads each repository's latest pointer once and freezes the result as the manifest's inventory pins. Every later step reads those pins and does not read the pointer.
- **Why.** A CI publish during a qualification would otherwise move the target without anyone choosing to.
- **Where else.** The class matrix is the first reader (§4e.1). The registry status command also reads the pointer; it reports the registry and is not a qualification step.
- **Status.** `built`.

### §4d.2 One identifier per qualification, written once

- **Decision.** The manifest is written through the registry's write-once put before anything is written locally. Writing the same identifier again raises an error. An explicit overwrite option exists for deliberately redoing a qualification, and nothing falls back to it.
- **Why.** A qualification's manifest is what its results are compared against. A manifest that could change silently would make those results describe a different qualification.
- **Status.** `built`.

### §4d.3 Co-upgrade risks exclude the Dagster family

- **Decision.** The manifest lists libraries whose version differs between the baseline and the candidate. Libraries whose names start with `dagster` are left out, and a library present on one side only is not a risk.
- **Why.** The Dagster family is the upgrade being qualified. The list exists to surface the other bumps the upgrade brings along, such as dbt or a warehouse adapter.
- **Status.** `built`.

### §4d.4 Risks use the recipe's field names on disk

- **Decision.** A co-upgrade risk is written with the fields `from` and `to`, through aliases on the model. The manifest is always serialized using the aliases.
- **Why.** The on-disk shape matches the sample in the [recipe](../../RECIPE.md).
- **Status.** `built`.

## §4c Survey

The survey loads every code location of a repository at a commit, records what loaded and what it warned about, introspects assets, sensors, schedules, checks, IO managers and dbt projects, and publishes the results to the registry.

### §4c.1 Nothing is published for code that does not load

- **Decision.** If any code location fails to load, the survey writes nothing to the registry and reports the failure.
- **Why.** Every later qualification step assumes the inventory describes code that loads. An inventory of broken code would send operators after inventory drift that is really a load failure.
- **Status.** `built`.

### §4c.2 Every warning during load is captured

- **Decision.** Every module import during load runs inside a warning capture that records every warning, not only the first of each kind. The captured warnings are written into the load validation artifact.
- **Why.** Deprecation and experimental warnings show how far each repository has drifted from the current Dagster, without anyone collecting them by hand.
- **Status.** `built`.

### §4c.3 One malformed item never aborts introspection

- **Decision.** Each sensor, schedule and asset check is read in its own error handler; a failure is logged and the item is skipped. A failing code location is recorded, not raised.
- **Why.** One bad definition would otherwise hide every other definition in the repository.
- **Status.** `built`. A test pins the per-location case; no test pins the per-item case.

### §4c.4 One schema per artifact, each with its own version

- **Decision.** Each survey artifact has its own pydantic model and its own schema version constant. Every model ignores unknown fields. Fields are only added.
- **Why.** Readers on an older release must still read artifacts written by a newer one.
- **Where else.** The same rule for the asset inventory is §4a.2.
- **Status.** `built`. No test pins the per-artifact tolerance of unknown fields.

## §4b Qualification registry

The registry is object storage holding the survey's inventories, per commit, and every qualification's manifest, state and run records.

### §4b.1 The layout module owns every key

- **Decision.** Registry keys are built by helper functions in one layout module. Writers and readers compose keys through those helpers.
- **Why.** One place to read and change the layout means a writer and a reader cannot disagree about where something lives.
- **Status.** `built` for every write found. One exception: the synthetic probe generator spells a probe source key itself and records it on the probe's manifest entry. The write of that source goes through the layout helper, which builds the same key. No test checks that keys are built only in the layout module.

### §4b.2 Per-build artifacts are write-once

- **Decision.** Every per-build artifact is written with a put that first checks whether the key exists and raises an error if it does. Only the pointers use a put that overwrites.
- **Why.** A build's artifacts describe that build. An artifact that could be replaced would let two readers see different content for one commit.
- **Status.** `built`.

### §4b.3 The latest pointer is written last

- **Decision.** Publishing a build writes every artifact first and updates the latest pointer last.
- **Why.** A reader that finds the pointer can rely on every artifact it names being present, so a publish that fails partway is invisible.
- **Where else.** [ADR-0005](../../adr/ADR-0005-registry-writes-the-pointer-last.md) records this decision.
- **Status.** `built`.

### §4b.4 Command output is JSON by default

- **Decision.** Commands accept an output format option that defaults to JSON, with a table form for people.
- **Why.** The recipe's rule is that every command is idempotent and emits machine-readable output.
- **Status.** `built` for 12 of the 15 commands in the command module. The command that writes an image record and the two dlt development commands take no format option: `not built`. One test pins the JSON default, for the registry status command.

## §4a (inventory) Shared inventory contract

The asset inventory record is one schema used by two callers on different release cadences: the domain broker, a long-running service, and the survey, which runs in CI on every commit.

### §4a.1 The asset record is a contract across releases

- **Decision.** The asset record schema is the contract between the domain broker and the qualification survey. Both build their records through the same extractor.
- **Why.** The two run different releases at the same time, so the schema has to hold across versions.
- **Status.** Shared schema and extractor: `built`. The census says the two read each other's records across versions. The broker builds its own records in-process, and no path where it reads records the survey wrote was found: not established.

### §4a.2 The schema evolves by addition only

- **Decision.** Fields are only added, and only as optional fields with defaults. The schema version goes up by one in the same commit. Readers ignore unknown fields. A breaking change is a major library release with a coordinated rollout.
- **Why.** Readers on last quarter's release must still read records written by today's.
- **Where else.** [ADR-0004](../../adr/ADR-0004-inventory-schema-evolves-additively-only.md) records this decision.
- **Status.** `built`.

### §4a.3 IO managers are classified by a registry of class names

- **Decision.** A table maps IO manager class names to families. A custom subclass is classified by walking its base classes. A substring match is the last resort, logged as a warning.
- **Why.** The warning is a signal to add the class to the table, so the fallback does not quietly become the rule.
- **Status.** `built`.

### §4a.4 The extractor fails soft

- **Decision.** The extractor never aborts because one asset is malformed. Field reads that fail record no value.
- **Why.** The schema is mostly optional so that a missing field is safe; a crash on one asset would lose every other asset.
- **Status.** Isolation per asset: `built`. A failing asset is logged as an error and skipped rather than recorded with empty fields. Field reads that fail return no value: `built`. The census adds a warning for each failed field read. Most field readers return no value without logging: `not built`.

## §7 Keeping the guide files in step

### §7.1 A new category of tool updates three files together

- **Decision.** Adding a new category of common tool updates, in the same change, `llms.txt` when the domain or intent changes, `.cursorrules` when the enforced stack rules change, and `README.md` to document the tools.
- **Why.** These three files are what an agent or a developer reads first. A tool missing from them is a tool nobody is guided to use correctly.
- **Status.** The three files exist at the pin: `built`. Nothing checks that they change together: `not built`.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Test files are under `dag_tools_tests/`. `bld` is `dag_tools/qual/classes/builder.py`, `key` is `dag_tools/qual/classes/key.py`, `sel` is `dag_tools/qual/classes/selection.py`, `stg` is `dag_tools/qual/classes/staging.py`, `isc` is `dag_tools/inventory/schema.py`, `ini` is `dag_tools/qual/qualify/init.py`, `rsk` is `dag_tools/qual/qualify/risks.py`, `man` is `dag_tools/qual/qualify/manifest.py`, `rcl` is `dag_tools/qual/registry/client.py`, `lay` is `dag_tools/qual/registry/layout.py`, `rss` is `dag_tools/qual/registry/status.py`, `qcl` is `dag_tools/qual/cli.py`, `pub` is `dag_tools/qual/survey/publisher.py`, `ldr` is `dag_tools/qual/survey/loader.py`, `isp` is `dag_tools/qual/survey/introspector.py`, `ssc` is `dag_tools/qual/survey/schemas.py`, `gen` is `dag_tools/qual/synthetic/generator.py`, `cls` is `dag_tools/inventory/classifier.py`, `ext` is `dag_tools/inventory/extractors.py`, `dbm` is `dag_tools/domain_broker/main.py`, `rcp` is `docs/RECIPE.md`. Test abbreviations: `tck` is `test_classes_key.py`, `tcb` is `test_classes_builder.py`, `tcs` is `test_classes_selection.py`, `tsn` is `test_qual_staging_need.py`, `tqi` is `test_qualify_init.py`, `tqc` is `test_dagtools_qual_init_cli.py`, `tqr` is `test_qualify_risks.py`, `tqm` is `test_qualify_manifest.py`, `tsp` is `test_survey_publisher.py`, `tsl` is `test_survey_loader.py`, `trl` is `test_registry_layout.py`, `trc` is `test_registry_client.py`, `tdc` is `test_dagtools_cli.py`, `tin` is `test_inventory.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4e.1 manifest pins, not the pointer | bld `:40`, `:62`, `:270`, `:281-284`, `:304-312` | AGENTS.md §4e, bullet 1 | tcb: `test_q1_reads_pinned_sha_not_latest` (`:244`) | `built` |
| §4e.2 deterministic hash | key `:80`, `:198`, `:241-250` | bullet 2 | tck: `test_class_hash_is_deterministic` (`:124`), `test_class_hash_insensitive_to_set_field_ordering` (`:147`), `test_class_hash_changes_when_any_component_changes` (`:141`), `test_compute_class_key_sorts_unordered_components` (`:56`) | `built` |
| §4e.3 custom translator forms its own class | key `:209-225`, `:253-259`; bld `:319` | bullet 3 | tck: `test_custom_dbt_translator_segregates_assets_into_own_class` (`:182`), `test_custom_dbt_translator_detected_via_integration_libs` (`:102`), `test_custom_dbt_translator_only_attached_for_dbt_assets` (`:83`) | `built` |
| §4e.4 runnability from tags | sel `:128-131`; key `:42-56` | bullet 4 | tcs: `test_classify_default_is_runnable` (`:23`), `test_classify_synthetic_required_via_tag` (`:30`), `test_classify_observe_only_via_tag` (`:37`), `test_classify_synthetic_wins_over_observe` (`:53`) | `built` |
| §4e.5 staging need reported; unknown is not none | key `:59-72`, `:187`; stg `:36`; bld `:128-143`; isc `:98` | bullet 5 | tsn: `test_pre_v3_inventory_classifies_unknown_not_none_needed` (`:107`), `test_upstream_executable_in_any_repo_means_staging_required` (`:137`), `test_required_wins_over_unknown_when_both_apply` (`:164`), `test_launchable_now_counts_only_runnable_and_unstaged` (`:249`), `test_staging_need_does_not_change_runnability` (`:263`), `test_pick_representatives_without_index_yields_unknown` (`:291`) | `built`; seeder assets `not built` |
| §4d.1 pins frozen at creation | ini `:150-156`; bld `:62`; rss `:67` | AGENTS.md §4d, bullet 1 | tcb: `test_q1_reads_pinned_sha_not_latest` (`:244`) | `built` |
| §4d.2 write-once manifest; explicit overwrite | ini `:77`, `:127-132`; rcl `:58`, `:89-96`, `:363-375`; qcl `:593-594`, `:645` | bullet 2 | tqi: `test_re_init_same_qual_id_raises` (`:172`), `test_allow_overwrite_bypasses_immutability` (`:187`); tqc (`:177-183`) | `built` |
| §4d.3 Dagster family excluded | rsk `:39`, `:46`, `:67` | bullet 3 | tqr: `test_dagster_family_is_filtered_out` (`:28`), `test_lib_in_one_side_only_is_not_a_risk` (`:111`), `test_dbt_core_minor_bump_is_warning` (`:38`), `test_dbt_core_major_bump_is_major` (`:51`) | `built` |
| §4d.4 `from` / `to` on disk | man `:95`, `:106-107`; ini `:299`; qcl `:652` | bullet 4 | tqm: `test_co_upgrade_risk_uses_from_to_aliases` (`:35`), `test_manifest_dumps_with_aliases` (`:48`); tqi: `test_registry_yaml_uses_recipe_aliases` (`:238`) | `built` |
| §4c.1 load gate | pub `:66`, `:89-106` | AGENTS.md §4c, bullet 1 | tsp: `test_run_survey_refuses_to_publish_when_load_fails` (`:151`) | `built` |
| §4c.2 every warning captured | ldr `:202-213`, `:240-241`, `:302`, `:319-320`; pub `:85-87`, `:152`, `:169` | bullet 2 | tsl: `test_load_locations_captures_warnings` (`:93`) | `built` |
| §4c.3 per-item soft failure | isp `:69-81`, `:88-99`, `:114-132`; ldr `:91-92` | bullet 3 | tsl: `test_load_locations_load_failure_is_recorded_not_raised` (`:81`); per item: none found | `built` |
| §4c.4 one versioned schema per artifact | ssc `:23-27`, `:39`, `:53`, `:63`, `:76`, `:94`, `:109`, `:121`, `:132`, `:147`, `:161`, `:173`, `:193`, `:206` | bullet 4 | none found | `built` |
| §4b.1 layout owns the keys | lay `:85-107`, `:136-145`; rcl `:572-581`; exception gen `:129` | AGENTS.md §4b, bullet 1 | trl: `test_inventory_artifact_key_uses_constants` (`:18`), `test_latest_pointer_key_lives_above_build_prefix` (`:29`), `test_qualification_prefixes` (`:47`); key-ownership scan: none found | `built`, one exception |
| §4b.2 write-once artifacts | rcl `:58`, `:62`, `:80`, `:89-96`, `:102` | bullet 2 | trc: `test_storage_put_immutable_refuses_overwrite` (`:62`), `test_publish_build_is_immutable_for_same_sha` (`:124`), `test_publish_build_allow_overwrite_bypasses_immutability` (`:131`) | `built` |
| §4b.3 pointer last | rcl `:231`, `:264-273` | bullet 3 | trc: `test_pointer_not_updated_when_artifact_write_fails` (`:143`) | `built` |
| §4b.4 JSON by default | qcl `:41`, `:180`, `:290`, `:387`, `:597`, `:787`, `:870`, `:997`, `:1098`, `:1217`, `:1295`, `:1398`, `:1455`; no option at `:235`, `:1618`, `:1657`; rcp `:61` | bullet 4 | tdc: `test_registry_status_json_default` (`:52`) | `built` for 12 of 15; `not built` for 3 |
| §4a.1 one schema, one extractor, two callers | isc `:47`; dbm `:445`, `:656-661`, `:847`; isp `:42-44` | AGENTS.md §4a (inventory), bullet 1 | none found | `built`; cross-version reading not established |
| §4a.2 additive only | isc `:33`, `:57-59` | bullet 2 | tin: `test_schema_version_is_stamped_by_default` (`:28`), `test_schema_tolerates_unknown_fields` (`:33`) | `built` |
| §4a.3 class-name registry, base-class walk, warned fallback | cls `:48`, `:127-130`, `:142`, `:152-156` | bullet 3 | tin: `test_classify_known_fqn_string` (`:65`), `test_classify_walks_mro_for_custom_subclass` (`:75`), `test_classify_substring_fallback_logs_warning` (`:90`) | `built` |
| §4a.4 soft failure | ext `:58-70`, `:93`, `:241-247` | bullet 4 | tin: `test_extract_records_soft_fails_on_broken_spec` (`:174`) | `built`; per-field warning `not built` |
| §7.1 three files together | `llms.txt`, `.cursorrules`, `README.md` | AGENTS.md §7 | none found | `built`; the check `not built` |
