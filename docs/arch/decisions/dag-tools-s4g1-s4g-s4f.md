# dag-tools decisions: §4g1, §4g, §4f

This record covers the decisions in `AGENTS.md` sections 4g1, 4g and 4f at the component-records pin (`add5bc4`): the synthetic probes and the code location that loads them, the qualification preflight, and run execution, including the staging pass. All three run against Dagster, described in the [Dagster record](../components/dagster.md); there is no component record for the qualification system, and its procedure is in the [recipe](../../RECIPE.md). [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §4g1 Synthetic probes

A class that cannot be run against real data gets a generated probe: a small Python module holding an upstream and a downstream asset bound to the class's IO manager. Every probe module is loaded by one separate code location, the probes location. The generator writes the modules and a manifest; the location's loader reads them from a directory.

### §4g1.1 Generated source always parses, and the location always loads

- **Decision.** The generated module imports the class's IO manager inside a `try`. If the import fails, it falls back to an in-memory IO manager and keeps the import error; the probe's upstream asset raises that error when it is materialized.
- **Why.** One module that fails to import would stop the whole probes location from loading, and with it every other probe. Raising at materialization moves the failure to one probe, where it can be handled on its own.
- **Status.** `built`.

### §4g1.2 Each probe binds its IO manager under a key unique to its class

- **Decision.** The IO manager's resource key carries a short form of the class hash rather than the bare default key.
- **Why.** Merging definitions fails on two conflicting resources under one key, so probes that all used the default key could not be merged into one location.
- **Status.** `built`.

### §4g1.3 IO manager configuration comes from the qualification manifest, and environment references stay references

- **Decision.** The constructor arguments for a probe's IO manager come from the manifest's probe settings, keyed by the IO manager's class name, with a narrower per-class override that wins. A value that refers to an environment variable is rendered as a read of that variable, never as its value. Arguments are sorted. A value that cannot be rendered is refused when the manifest is validated. The probe manifest records argument names only. With no configuration, the generated line is the earlier no-argument construction.
- **Why.** A no-argument construction works only for IO managers that need no configuration, so every other probe could never pass. The class name is the key because class hashes change on every survey. The generated source is published to the registry and written to the operator's disk, so a resolved value would place a credential in both. Sorting keeps regenerated source byte-identical.
- **Status.** `built`.

### §4g1.4 Probe resources are bound only when a configuration entry names them

- **Decision.** The resource keys a class uses travel from the class members to the representative, mapped to each resource's class name. A resource is bound in the probe only when the manifest's resource configuration has an entry for its class name; an empty entry means a no-argument construction. A resource with no entry is left out and named in the probe's notes. Each bound key carries the short class hash, each resource is constructed in its own `try`, and a resource that fails is left out of the probe's definitions. With no entries, the generated source is unchanged from before this feature.
- **Why.** The class hash leaves resource keys out on purpose, so that two repositories naming a resource differently still share a class; the keys are carried beside the hash instead. Binding every resource automatically could make a passing probe fail, because a resource may connect when it is set up, and a coverage regression caused this way could not be told apart from a real finding. The probe proves only that the resource imports, constructs and is available; it says nothing about the backend.
- **Status.** `built`.

### §4g1.5 Probes import nothing from the qualification package

- **Decision.** A generated module imports Dagster and the IO manager's own module, and nothing from the qualification package.
- **Why.** The probes location is deployed on its own cadence. A probe importing the qualification package would turn every dag-tools release into a probe deployment.
- **Status.** `built`.

### §4g1.6 The probe manifest is written last

- **Decision.** Publishing a bundle writes every probe source first and the probe manifest after them.
- **Why.** A reader that sees the manifest can then rely on every source it names being present. This is the same ordering as the inventory registry's pointer.
- **Where else.** [ADR-0005](../../adr/ADR-0005-registry-writes-the-pointer-last.md) records the pointer-last rule for the inventory registry.
- **Status.** `built`.

### §4g1.7 The probes location fails one probe at a time

- **Decision.** The loader records each module that fails to load and continues with the rest. The location logs a warning for the failures and merges what loaded.
- **Why.** A syntax error or a stale file in one module must not stop the whole location; operators work from the load log.
- **Status.** `built`.

### §4g1.8 The probes location loads with no probes deployed

- **Decision.** When the probes directory variable is unset, or names a directory that does not exist, the loader returns an empty report and the location an empty set of definitions.
- **Why.** Operators can deploy the location once and add probe bundles later without deploying it again.
- **Status.** `built`.

### §4g1.9 An uncovered class is not hidden by a result that was not produced

- **Decision.** Probes are launched by the probe runner, and the verdict counts a class as covered once its probes pass on both sides and their run records agree (§4g0, §4g0a). The round trip for runnable classes is §4g2. The acceptance flag for missing synthetic coverage is needed only while some synthetic class is still uncovered. A class is not reported as covered by any shortcut that fakes a probe result.
- **Why.** A faked result would remove the one signal that a class has no evidence.
- **Status.** `built`.

### §4g1.10 What the generator cannot synthesize is written as a note

- **Decision.** Partition definitions, partition mappings and custom dbt translators that the generator cannot reproduce are recorded in the probe's notes, which travel in the bundle. So is an IO manager with no configuration entry.
- **Why.** The operator sees the gap rather than receiving a probe that silently tests less than its class uses. Extending the generator means synthesizing more, not dropping notes.
- **Status.** `built`.

## §4g Preflight

Preflight runs against one side's Dagster deployment before qualification runs are launched, and publishes a report of named checks. The baseline side has three checks: the Dagster version, that every code location loads, and that the deployed images match the pinned commits. The candidate side adds a fourth: that sampled baseline runs still render.

### §4g.1 Every check becomes a report entry; none raises

- **Decision.** A GraphQL failure during the version lookup, the workspace query or the image check becomes a failed check with the error in its detail.
- **Why.** The purpose of preflight is a report for the operator, and a preflight that crashes while reporting gives none.
- **Status.** `built`.

### §4g.2 The run rendering check runs on the candidate side only

- **Decision.** The fourth check, which asks the candidate deployment to render a sample of runs that passed on the baseline, is added only on the candidate side.
- **Why.** On the baseline side there are no earlier runs to check backward compatibility against.
- **Status.** `built`.

### §4g.3 The image check is driven by the pins, and identifies a location by digest, then commit tag, then stamp

- **Decision.** Each code location is identified from the image it reports. A digest is compared with the digests recorded for a pin, repository first. Failing that, a commit tag is compared as a prefix of at least seven hexadecimal characters. Failing that, the earlier stamp is used. The first source that attributes a location decides, and a mismatch is final, so a digest that disagrees with a pin outranks a tag that claims it. Each location lands in exactly one outcome: matched, mismatch, unpinned (informational), unverified (fails the check) or waived. A pin is mismatched if any location mismatched it, matched if at least one matched and none mismatched, and otherwise unmatched, which fails. A waiver is a row in the immutable qualification manifest with a required reason, one per location; the verdict has no flag for it. With no pins, the check passes and reports how many locations were unverified without gating on them. Every outcome is published in the report. One malformed item in a location's display metadata is skipped with a warning naming the location, and the rest of the entry is kept.
- **Why.** The census reports that Dagster's chart, measured on 1.10.19 and 1.13.1, gives the code server its image through an environment variable and that the server reports it in its display metadata, while the workspace file's extra metadata cannot be set through the chart, so a check relying on the stamp alone could never pass on a chart deployment. A tag is a label and a digest is the bytes, so the digest decides. A waiver is recorded in the manifest because a gap excused only at verdict time is one nobody recorded.
- **Where else.** The CI side that writes the image records is `AGENTS.md` §8.
- **Status.** `built`.

### §4g.4 A pin may declare where its image record lives, and that record is checked again with no fallback

- **Decision.** A pin may carry an image record source and the SHA-256 of the record's bytes, taken when the qualification is created. The source is set by a template filled with the repository and commit by literal replacement, or by an explicit path or address that wins over the template; the commit placeholder is required and any other brace is refused. A pin that declares a record must carry a full forty-character lowercase commit. At creation, the record fetched must also equal the bucket's record for the same commit, if there is one. At preflight the record is fetched again, and a pin whose record is absent, unreadable, changed, unparseable or for another commit fails, whatever any location reports. An absent record is reported separately as one that can lag its build, and still fails. Only an `https` address or a local path is accepted, checked before any request; a redirect to anything but `https` is refused, no credential header is sent, and the body is capped at 64 KiB. The record grammar lives in one module that imports only the standard library and pydantic; the fetching lives in a separate module.
- **Why.** An address can be changed by a force-push, so the hash of the bytes is what makes the record a pin. A record that was declared and cannot be read is a failure, not an absence: letting a commit tag rescue it would make the record decorative. CI records under the full commit, so a short or upper-case commit would never be found and would read as a record lagging its build, sending the operator to the wrong place. Whether a pin with a record must also carry a digest is left unruled.
- **Status.** `built`.

### §4g.5 Every GraphQL document the client sends is validated against the installed schema

- **Decision.** A test collects every query and mutation document the client defines and validates each one against the schema of the installed Dagster GraphQL package.
- **Why.** A field that does not exist on its type fails the whole request (§4f.5), and a test that stubs the transport cannot see it.
- **Status.** `built`.

### §4g.6 The baseline runs are sampled deterministically

- **Decision.** The identifiers of passed baseline runs are sorted before the sample is taken.
- **Why.** A repeated preflight checks the same runs, so a failure is never an accident of which runs were picked.
- **Status.** `built`. No test asserting the order was found.

### §4g.7 The version grammar is an exact version or a `.x` suffix

- **Decision.** A deployed Dagster version is compatible with the expected one if they are equal, or if the expected one ends in `.x` and the deployed one starts with what precedes it. Ranges, pre-releases and hashes are not supported.
- **Why.** The census states this is the whole grammar and that the rest is out of scope on purpose.
- **Status.** `built`.

## §4f Run execution

The qualification runner launches one representative asset per equivalence class on one side, through Dagster's GraphQL interface, and records each run. A state file, mirrored to the registry, tracks each representative's status. A separate staging pass materializes the upstream assets the representatives read.

### §4f.1 State is saved after every transition

- **Decision.** The runner saves the side's state locally and mirrors it to the registry after each change of a representative's status.
- **Why.** If the operator's machine stops between a launch and the save, the mirrored state is what lets the next invocation reconcile the launched run instead of launching it again.
- **Status.** `built`.

### §4f.2 A passed representative is never launched again

- **Decision.** A repeated invocation skips representatives that passed. A launched one is reconciled by polling its run, not relaunched.
- **Why.** A relaunch would spend compute twice and give the verdict two run records for one class and asset on one side.
- **Status.** `built`.

### §4f.3 A failed poll is not a failed run

- **Decision.** A timeout, a transport error or a failed event-log read while polling leaves the representative launched with the error noted, and the next invocation reconciles it. Only a run-not-found answer, matched by its GraphQL type name, marks it failed so that it is launched again. The qualification runner, the probe runner and the staging pass follow this rule. A probe left launched counts as uncovered in the verdict, not as failed.
- **Why.** A failed poll says nothing about the run, which may still be executing; marking it failed let the next pass start a second run of the same representative alongside it. A run that does not exist will never finish.
- **Status.** `built`.

### §4f.4 GraphQL drift is expected, and the client reads public fields only

- **Decision.** The client selects public fields only. Each event is parsed into a typed entry that also keeps the raw event, and an event that cannot be parsed is skipped with a warning. A new event subtype that matters is handled by extending the record builder, not by reading internal attributes.
- **Why.** The client talks to more than one Dagster version, and a version adding an event subtype must not break run records.
- **Status.** `built`.

### §4f.5 A GraphQL document never selects a field its type lacks

- **Decision.** The launch mutation selects a message only on the error types that have one. Documents are edited against the live schema, and §4g.5 checks them against the installed one.
- **Why.** A missing field in one inline fragment rejects the whole request, which here meant every launch failed. A test that stubs the transport cannot see this.
- **Status.** `built`.

### §4f.6 The launch location and job come from the qualification manifest

- **Decision.** The runner passes the manifest's deployment location name and job name to the launcher. The launcher's fallback name exists only for a call that supplies none.
- **Why.** Real deployments name their location after their own code server, and the operator sets it when creating the qualification.
- **Status.** `built`.

### §4f.7 A representative is launched alone, and widened only when the deployment refuses

- **Decision.** The selection is the representative alone. If the deployment refuses a partial selection, matched by the GraphQL error's type name, the launch is retried once with the assets that share the representative's op. Any other launch failure is not retried, and nor is a refusal when there are no op-mates to add.
- **Why.** Op-mates are compute the qualification never reads. Dagster refuses a partial selection only for an asset definition that cannot be subset, and a dbt asset definition can be, so selecting op-mates up front built a whole dbt project for each representative; the census reports that most of the extra models then failed for lack of their own upstreams. Whether a definition can be subset cannot be asked through GraphQL first, so the refusal is what tells the cases apart.
- **Status.** `built`.

### §4f.8 The qualification run does not materialize upstreams; the staging pass does

- **Decision.** A representative whose upstream assets are computed needs their output staged before it can run. The selection is never widened to include the upstreams. The supported mechanisms are dbt's empty build and operator-staged data named by the staging overrides; generated seeder assets are not built.
- **Why.** Dagster loads an upstream that is not in the run from its IO manager, so its output must already exist. Widening the selection would turn a one-asset comparison into a build of the whole subgraph and bring back the cost in §4f.7.
- **Status.** `built`; generated seeder assets `not built`.

### §4f.9 The staging overrides field is a pointer, resolved once per side before any launch

- **Decision.** The manifest's staging overrides field names a run configuration document in object storage or on a local path. Each runner resolves it once per side, and a resolution failure stops the side rather than launching without it. The document's top-level keys must be ones Dagster accepts. A launcher handed a declared pointer with no resolved document raises instead of launching. The document is merged under each representative's own configuration recursively, the representative's value winning at each leaf.
- **Why.** Both launchers had placed the pointer string itself into the run configuration under a key Dagster rejects, so every launch with the field set failed at submission; the tests had asserted on the dictionary built, not on whether Dagster would accept it. Launching without the overrides would record runs that look like a qualification but describe the wrong deployment. Run configuration nests resources several levels deep, so a shallow merge of one representative's resources would drop every staged resource it did not restate.
- **Status.** `built`.

### §4f.10 The round-trip probe is built

This bullet points to §4g2, where the round-trip probe's decisions are recorded. It states no decision of its own.

### §4f.11 The staging pass materializes what the qualification run reads, in its own slot

- **Decision.** The staging pass keeps its state and records in a slot of its own, with types distinct from the representative and probe slots, and its record is not a run record, so the verdict's comparison has nothing to match it on. The staging overrides are mandatory: an unresolved pointer raises, and an empty answer from the deployment about which assets can be launched refuses the plan. Keys are grouped one run per partition definition and launched in dependency order; a failed group skips only the groups that depend on it, and keys on a cycle between groups are reported and not launched. A passed group is not launched again for the same selection hash, and a changed hash launches again. Stage runs are tagged with their purpose and carry no class hash. The qualification run's selection is unchanged, and it only warns when representatives need staging and the staging pass has not passed for the current selection. The verdict records each side's staging summary and never compares it or lets it block. Staging is per side.
- **Why.** Staged outputs written to the wrong place would record a qualification of the wrong deployment, so the pass refuses where the qualification run degrades. A run cannot mix partition definitions. Keeping staging out of the comparison and the blocking list leaves the verdict a statement about the representatives alone.
- **Status.** `built`.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Test files are under `dag_tools_tests/`. `gen` is `dag_tools/qual/synthetic/generator.py`, `bnd` is `dag_tools/qual/synthetic/bundle.py`, `sch` is `dag_tools/qual/synthetic/schema.py`, `man` is `dag_tools/qual/qualify/manifest.py`, `key` is `dag_tools/qual/classes/key.py`, `bld` is `dag_tools/qual/classes/builder.py`, `pll` is `dag_tools/probes_location/loader.py`, `pld` is `dag_tools/probes_location/definitions.py`, `prn` is `dag_tools/qual/probes/runner.py`, `vvd` is `dag_tools/qual/verdict/verdict.py`, `pfl` is `dag_tools/qual/preflight/preflight.py`, `idn` is `dag_tools/qual/preflight/identity.py`, `irs` is `dag_tools/qual/image_record_source.py`, `ird` is `dag_tools/qual/image_record.py`, `ini` is `dag_tools/qual/qualify/init.py`, `qcl` is `dag_tools/qual/cli.py`, `gql` is `dag_tools/qual/graphql/client.py`, `rrn` is `dag_tools/qual/runs/runner.py`, `rln` is `dag_tools/qual/runs/launcher.py`, `rst` is `dag_tools/qual/runs/staging.py`, `rrt` is `dag_tools/qual/runs/roundtrip.py`, `srn` is `dag_tools/qual/stage/runner.py`, `spl` is `dag_tools/qual/stage/plan.py`, `sst` is `dag_tools/qual/stage/state.py`, `lay` is `dag_tools/qual/registry/layout.py`, `rcp` is `docs/RECIPE.md`. Test abbreviations: `tsg` is `test_synthetic_generator.py`, `tsc` is `test_synthetic_probe_config.py`, `tsr` is `test_synthetic_probe_resources.py`, `tsb` is `test_synthetic_bundle.py`, `trg` is `test_qual_roundtrip_generate.py`, `tpl` is `test_probes_location_loader.py`, `tpf` is `test_preflight.py`, `tpi` is `test_preflight_image_identity.py`, `tpg` is `test_preflight_image_identity_grpc.py`, `tri` is `test_image_record_reader.py`, `tid` is `test_image_record_document.py`, `trc` is `test_registry_record_image.py`, `tgv` is `test_graphql_query_schema_validation.py`, `tgc` is `test_graphql_client.py`, `trn` is `test_runs_runner.py`, `tpr` is `test_probes_runner.py`, `tlp` is `test_launch_plan.py`, `tso` is `test_qual_staging_overrides.py`, `tqs` is `test_qual_stage.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4g1.1 import fallback; error raised at materialization | gen `:303`, `:338-352`, `:272` | AGENTS.md §4g1, bullet 1 | tsg: `test_generated_source_parses_as_python` (`:89`), `test_generated_source_has_inmemory_fallback_for_missing_io_manager` (`:103`) | `built` |
| §4g1.2 class-unique IO manager key | gen `:258` | bullet 2 | tsg: `test_each_probe_uses_class_unique_io_manager_resource_key` (`:127`); tpl: `test_loaded_probes_merge_into_one_definitions_without_resource_collision` (`:128`) | `built` |
| §4g1.3 IO manager configuration from the manifest; references stay references | man `:204`, `:235-251`, `:256`, `:280`, `:303-336`; gen `:110-118`, `:457`, `:471`, `:477`; sch `:65` | bullet 3 | tsc: `test_env_reference_renders_os_environ_not_the_literal_value` (`:52`), `test_the_construct_stays_inside_the_try_block` (`:118`), `test_no_config_emits_the_no_arg_construct_unchanged` (`:33`), `test_class_hash_override_beats_the_fqn_entry` (`:72`); tsb: `test_probe_manifest_records_config_key_names_never_values` (`:274`); trg: `test_env_reference_renders_os_environ_not_the_literal_value` (`:118`) | `built` |
| §4g1.4 resources bound by opt-in entry | key `:94`, `:132`, `:152`; bld `:75`; man `:286`, `:321-325`; gen `:388`, `:401`, `:411`, `:418`, `:434-451`; sch `:76` | bullet 4 | tsr: `test_no_resource_config_emits_source_identical_to_c1` (`:50`), `test_two_probes_with_the_same_resource_key_do_not_collide_on_merge` (`:65`), `test_a_resource_without_a_config_entry_is_not_bound_and_is_noted` (`:82`), `test_a_failing_resource_does_not_reach_the_definitions_resources` (`:121`), `test_env_reference_in_resource_config_never_renders_the_literal` (`:89`), `test_each_resource_construct_is_in_its_own_try_block` (`:102`) | `built` |
| §4g1.5 no qualification import | gen `:248` | bullet 5 | tsg: `test_generate_probe_source_is_self_contained` (`:148`) | `built` |
| §4g1.6 probe manifest last | bnd `:143-184` | bullet 6 | tsb: `test_publish_bundle_writes_sources_then_manifest` (`:181`) | `built` |
| §4g1.7 location fails per probe | pll `:133-143`; pld `:42-53`, `:62` | bullet 7 | tpl: `test_loader_soft_fails_a_malformed_probe` (`:152`) | `built` |
| §4g1.8 empty location loads | pll `:88-98`, `:118-130`; pld `:56-57` | bullet 8 | tpl: `test_loader_returns_empty_report_when_env_unset` (`:77`) | `built` |
| §4g1.9 coverage wired; no faked result | prn `:247-261`, `:372-373`; vvd `:225-263` | bullet 9 | the §4g0 and §4g0a tests in [dag-tools-s4i-s4h-s4g.md](dag-tools-s4i-s4h-s4g.md) | `built` |
| §4g1.10 deferrals as notes | gen `:92-118` | bullet 10 | tsg: `test_partitions_def_observed_is_noted_not_synthesized_in_v1` (`:76`), `test_custom_dbt_translator_is_called_out_in_notes` (`:157`); tsr: `test_a_resource_without_a_config_entry_is_not_bound_and_is_noted` (`:82`) | `built` |
| §4g.1 checks never raise | pfl `:240-293` | AGENTS.md §4g, bullet 1 | tpf: `test_run_preflight_graphql_failure_becomes_failed_check` (`:182`), `test_sha_check_becomes_a_failed_check_not_an_exception_on_graphql_error` (`:485`); tri: `test_a_fetch_error_never_raises_out_of_run_preflight` (`:495`) | `built` |
| §4g.2 rendering check on the candidate only | pfl `:295-302`, `:700-719`, `:747` | bullet 2 | tpf: `test_candidate_preflight_samples_baseline_runs` (`:224`), `test_baseline_preflight_does_not_sample_baseline_runs` (`:272`), `test_run_preflight_candidate_includes_run_rendering_check` (`:107`) | `built` |
| §4g.3 identity by digest, then tag, then stamp; outcomes; waivers | pfl `:127`, `:140`, `:377-378`, `:404-496`, `:541-570`, `:655-659`; idn `:25`, `:33`; man `:54`, `:86`, `:112`, `:174-186`; qcl `:160-161`, `:547`; gql `:646`, `:668-680` | bullet 3 | tpi: `test_a_location_deployed_by_digest_matches_its_pin` (`:150`), `test_control_one_changed_digest_digit_is_a_mismatch` (`:159`), `test_the_digest_decides_over_a_tag_that_claims_the_pin` (`:217`), `test_an_unverified_location_fails_the_check` (`:257`), `test_control_a_waiver_row_passes_it_and_records_the_reason` (`:267`), `test_the_four_outcomes_land_in_four_buckets` (`:312`), `test_with_no_pins_an_unverified_location_is_reported_not_gated` (`:340`), `test_control_with_one_pin_the_same_location_fails` (`:349`); tpf: `test_an_unverified_location_fails_while_an_unpinned_one_stays_informational` (`:442`); trc: `test_one_malformed_display_metadata_entry_drops_only_itself_and_warns` (`:221`), `test_two_waiver_rows_for_one_location_are_refused` (`:204`); tpg: `test_dagster_reports_the_container_image_through_the_workspace_query` (`:66`), against a live code server process; the census reports the chart measurement on 1.10.19 and 1.13.1; no recorded run cited | `built` |
| §4g.4 declared image record re-checked with no fallback | man `:35`, `:63`, `:87`; ini `:170-181`, `:186-256`; qcl `:555`, `:563`; pfl `:499-538`, `:640`, `:689`; irs `:15`, `:48-96`; ird `:11-19`, `:76`, `:112`, `:171` | bullet 4 | tri: `test_changed_bytes_fail_even_when_the_location_matches_by_digest` (`:440`), `test_an_absent_source_fails_even_when_the_location_matches_by_sha_tag` (`:451`), `test_control_the_same_location_and_pin_without_a_record_match_by_sha_tag` (`:463`), `test_a_scheme_other_than_https_is_refused_before_any_request` (`:125`), `test_a_redirect_to_http_is_refused` (`:134`), `test_control_a_redirect_to_https_is_followed` (`:144`), `test_the_cap_is_exact` (`:153`), `test_a_record_that_disagrees_with_the_bucket_record_fails_init` (`:317`), `test_a_pin_sha_that_is_not_full_lowercase_is_refused_naming_the_survey` (`:277`), `test_control_a_short_pin_sha_with_no_record_declared_inits_as_before` (`:294`), and the purity test for the fetching module (`:521`); tid: `test_every_problem_is_reported_at_once` (`:142`), `test_a_v2_file_is_refused_by_its_version_not_a_missing_field` (`:76`), `test_the_document_module_imports_no_storage_or_network` (`:270`), `test_control_the_purity_scan_sees_a_planted_import` (`:274`) | `built` |
| §4g.5 documents validated against the schema | gql `:296`, `:410`, `:570` | bullet 5 | tgv: `test_the_client_exposes_documents_to_validate` (`:33`), `test_client_document_validates_against_the_installed_schema` (`:38`) | `built` |
| §4g.6 deterministic sampling | pfl `:736-744` | bullet 6 | none found | `built` |
| §4g.7 version grammar | pfl `:362-374` | bullet 7 | tpf: `test_run_preflight_accepts_wildcard_version_match` (`:123`), `test_run_preflight_fails_on_version_mismatch` (`:142`) | `built` |
| §4f.1 state saved after each transition | rrn `:254-269`, `:322-388`, `:485-503` | AGENTS.md §4f, bullet 1 | trn: `test_run_side_state_is_mirrored_to_registry_after_each_transition` (`:303`) | `built` |
| §4f.2 passed never relaunched; launched reconciled | rrn `:314`, `:352-363` | bullet 2 | trn: `test_a_poll_timeout_leaves_the_rep_launched_and_is_never_relaunched` (`:396`) | `built` |
| §4f.3 failed poll leaves the run launched; not-found fails it | rrn `:464-483`; prn `:422-431`; srn `:460-466`; gql `:159-168`, `:421-428` | bullet 3 | trn: `test_a_poll_timeout_leaves_the_rep_launched_and_is_never_relaunched` (`:396`), `test_a_run_that_no_longer_exists_fails_so_the_next_pass_relaunches` (`:422`); tpr: `test_a_probe_poll_timeout_stays_launched_and_reconciles` (`:418`); tgc: `test_an_unknown_run_id_raises_the_permanent_not_found_error` (`:382`) | `built` |
| §4f.4 public fields; raw event kept; malformed skipped | gql `:862-866`, `:875-901` | bullet 4 | tgc: `test_get_event_log_parses_materialization_with_metadata` (`:340`); none found for the skip | `built` |
| §4f.5 no field selected that its type lacks | gql `:296-306` | bullet 5 | tgc: `test_launch_mutation_does_not_select_message_on_fieldless_error_types` (`:212`); tgv (see §4g.5) | `built` |
| §4f.6 location and job from the manifest | rrn `:369-370`; rln `:136-138` | bullet 6 | trn: `test_run_side_threads_manifest_location_and_job_into_launch` (`:171`) | `built` |
| §4f.7 launched alone; widened on refusal | rln `:128-158`, `:215`, `:302`; gql `:143`, `:395-400` | bullet 7 | tlp: `test_a_subsettable_dbt_op_does_not_drag_in_the_whole_project` (`:142`), `test_the_whole_project_is_not_launched_through_the_real_entrypoint` (`:151`), `test_a_refused_partial_selection_is_retried_with_the_op_mates` (`:196`), `test_other_launch_failures_are_not_retried` (`:219`) | `built` |
| §4f.8 upstreams not materialized by the qualification run | rln `:128`; rcp `:408` | bullet 8 | tqs: `test_q2_selection_is_unchanged_by_stage` (`:681`) | `built`; seeder assets `not built` |
| §4f.9 staging overrides resolved once per side; recursive merge | rst `:40`, `:53`, `:75-130`, `:133-152`, `:190-195`; rrn `:296-303`; rln `:115-125`; prn `:287`, `:384`; rrt `:371`, `:501` | bullet 9 | tso: `test_the_pointer_itself_never_reaches_run_config` (`:71`), `test_launched_run_config_has_only_keys_dagster_accepts` (`:85`), `test_an_unresolved_pointer_raises_instead_of_launching` (`:110`), `test_merge_is_recursive_so_a_per_rep_block_does_not_drop_siblings` (`:212`), `test_probe_launch_does_not_put_the_pointer_in_run_config` (`:267`), `test_probe_launch_refuses_an_unresolved_pointer` (`:282`) | `built` |
| §4f.10 pointer to §4g2 | rrt | bullet 10 | the §4g2 tests in [dag-tools-s4i-s4h-s4g.md](dag-tools-s4i-s4h-s4g.md) | — |
| §4f.11 staging pass in its own slot | lay `:214-230`; sst `:28`, `:62-98`; spl `:77-90`, `:136-166`, `:244-278`; srn `:97-152`, `:173-201`, `:323-344`, `:387-422`; rrn `:400-437`; vvd `:153`, `:392`, `:528` | bullet 11 | tqs: `test_stage_run_tags_carry_purpose_and_no_class_hash` (`:342`), `test_unresolved_staging_pointer_raises_instead_of_launching` (`:324`), `test_empty_launch_info_refuses_to_launch` (`:387`), `test_q2_selection_is_unchanged_by_stage` (`:681`), `test_q6_status_is_identical_with_and_without_stage_artifacts` (`:644`), `test_stage_status_never_reaches_blocking_issues` (`:667`), `test_two_partition_definitions_launch_as_two_runs_in_dependency_order` (`:435`), `test_a_failed_group_skips_only_the_groups_that_depend_on_it` (`:456`), `test_passed_is_sacred_for_the_same_selection_hash` (`:508`), `test_changed_selection_hash_launches_again` (`:518`) | `built` |
