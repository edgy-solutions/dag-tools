# invincible-agent decisions: ADR-0025 to ADR-0048

invincible-agent records its decisions as numbered ADRs in its own repository, under `docs/adr/`. This record indexes ADR-0025 to ADR-0048 at invincible-agent commit `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. For each ADR it gives what the ADR decides and why, in short, what the ADR's own header states, and what the code at that commit does. It points rather than restates: the ADR is the decision, and where an ADR and any line here disagree, the ADR holds. [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). "Stated status" is the ADR's own header, paraphrased; "Status" is this record's reading of the code at the pin. Several headers say less is built than the code shows, or more. Line numbers are at the pin. The record shape is provisional.

An ADR number here is always invincible-agent's own series, cited elsewhere as "invincible-agent ADR-NNNN". A bare section number in a dag-tools record means the dag-tools census.

## ADR-0025 Deny-by-default access decisions and two captures at answer creation

- **Decision.** Access to governed data is decided by one Topaz-backed decider that refuses unless an explicit grant exists. When an answer is written, two facts are captured: where the reader's entitlements came from, and a reserved slot on each source citation for the access decision taken.
- **Why.** An answer's audit trail cannot be rebuilt after the fact, so what is needed later must be recorded when the answer is created.
- **Stated status.** Proposed, with four dated amendments.
- **Where else.** ADR-0026 (entitlement model).
- **Status.** Deny-by-default data gate: `built`. Entitlement-source capture on the user node: `built`. Access-decision slot on the citation edge: slot `built`, population not established (the writer notes it is null in production writes). Enforcement on every retrieval path: not established.

## ADR-0026 Persona and domain entitlements decided in Topaz

- **Decision.** A reader's persona and domain cells come from a persona-by-domain matrix held in Topaz, asserted in git and applied by sync tools. The token's persona claim is ignored. A picker choice outside the reader's cells is refused with a not-entitled error, and an unreachable decider yields a distinct unavailable error rather than an empty grant.
- **Why.** Identity claims in a token can drift from policy. Telling "not entitled" apart from "decider down" keeps a refusal honest.
- **Stated status.** Proposed.
- **Where else.** ADR-0025; ADR-0031 cites its abstention gate.
- **Status.** Server-side matrix, per-token entitlement cache, 403 and 503 split, and claim ignoring: `built`. The user-interface picker and retirement of the identity-provider persona mapper: not established.

## ADR-0027 Composable approval policy for grant issuance

- **Decision.** Who may approve a grant is itself policy, decided by Topaz rules over several approvals. Automatic checks run before a human is asked, and anything unclear fails closed.
- **Why.** Granting access is the highest-risk change, so it needs governance that does not depend on one approver.
- **Stated status.** Accepted for the architecture; the single-dimension form is built and the composable form is deferred.
- **Where else.** ADR-0029 (human-await steps); ADR-0034 (promotion as an approval).
- **Status.** Single-dimension task-audience check: `built`. Composable multi-approval engine for grant issuance: `accepted`, code not established.

## ADR-0028 Canvas as an answer-composition workspace

- **Decision.** A canvas holds cards that carry subject-predicate-object provenance, and it inherits the same enforcement as any other read. Version one fixes a small set of forward-compatibility constraints; a deeper canvas is left as direction.
- **Why.** Composing answers from cards must not open a path around access control or lose where each claim came from.
- **Stated status.** Accepted for the version-one constraints; deeper canvas directional and deferred.
- **Where else.** ADR-0029 (answer seeding of steps).
- **Status.** `accepted`; no code confirming card-level provenance was found, so not established.

## ADR-0029 Process workflows as subject-verb-object steps on Restate

- **Decision.** A workflow is a declared definition of steps of three kinds: an operation on a subject through a registered verb, a wait for a human, and a direct call. A verifier checks each operation step against the capability graph at load time, and execution runs on Restate with the caller's identity threaded through. An addendum lets an answered ask seed a step that is already resolved.
- **Why.** Domain experts can then author processes without a data engineer, and an invented verb cannot survive validation.
- **Stated status.** Accepted for the model shape and the Restate seam; other standard seams directional. A note says the second interviewer slice is implemented with no callers and the cutover is not taken.
- **Where else.** ADR-0030, ADR-0032, ADR-0034, ADR-0035.
- **Status.** Step executor, definition loading and answer seeding: `built`. Pre-resolved step on the skip path: `built`. Cutover of the gateway to the new interviewer: `not built`. Directional standard seams: `proposed`.

## ADR-0030 One fixed output type per verb

- **Decision.** A verb declares one output type and never changes it. A projection parameter narrows content, not type. A filtered lineage topology may legitimately have no edges, and an outcome field must tell filtered, failed and non-empty cases apart.
- **Why.** Load-time verification and answer-to-step matching both need a static answer to what a verb produces. Drawing a graph from edgeless data had led a renderer to invent edges.
- **Stated status.** Accepted; re-confirms ADR-0017 under the new constraint and records a rejected alternative.
- **Where else.** ADR-0017; ADR-0029.
- **Status.** Edgeless-topology degradation to a document keyed on the outcome field, in the presentation layer: `built`. Producer-side outcome field and the owed consumer items: not established.

## ADR-0031 Instance-resolution ladder

- **Decision.** A named subject resolves through four rungs: exact match, containment after stripping descriptor words, a language-model candidate rung, and a flagged classifier-only fallback. The language model may only nominate names for the catalog to confirm.
- **Why.** A classifier standing alone silently answered a different question than the one asked. Only the catalog should say what a subject is.
- **Stated status.** Accepted; rungs 1 and 2 built, rung 3 deferred until telemetry shows a need.
- **Where else.** ADR-0032, ADR-0033.
- **Status.** Rungs 1 and 2: `built`. Rung 3: `accepted`. Rung 4 flagging: not established.

## ADR-0032 Goal-oriented queries and the catalog-analyst loop

- **Decision.** A goal-shaped question is answered by an analyst loop that explores the catalog and authors a proposed workflow. Every step must pass the capability graph and the plan waits at a human-await step. Build order: an honest goal-shape card first, the loop next, promotion of recurring approved plans last.
- **Why.** Single-subject routing fails on goals with no single subject. Authorship by a model is safe when enforcement still gates execution.
- **Stated status.** Proposed; model shape and build order; evaluation set banked red first.
- **Where else.** ADR-0011, ADR-0029, ADR-0031.
- **Status.** `proposed`. No goal-shape card was confirmed built.

## ADR-0033 Ask as a third behavior between route and abstain

- **Decision.** Weak resolutions ask one clarifying question drawn only from resolvable catalog entities, never an open question. One round, then commit or abstain. Strong matches never ask. A confirmed pick is recorded as provenance-bearing data.
- **Why.** Asking captures the signal that guessing and abstaining both discard.
- **Stated status.** Accepted, woken by the reachability condition, not the frequency one. Amendments cover an unfilled-slot trigger and an ambiguous-class trigger.
- **Where else.** ADR-0031, ADR-0032.
- **Status.** Ask for a missing or wrong-kind mandatory slot: `built`. Provenance tier for user-confirmed picks and alias growth: not established. Frequency gate: not fired.

## ADR-0034 Trust lifecycle for autonomous review

- **Decision.** A trust table keyed on input format and pipeline version sets a rung: supervised, monitored or trusted. Unknown keys are supervised, jumping from supervised to trusted is forbidden, and the starter combines posture with content checks. Decision records are emitted artifacts, and no workflow definition reads the table.
- **Why.** Autonomy needs evidence gathered under the same regime, and trust must never replace a check.
- **Stated status.** Proposed; phase one is buildable now, the autonomous definition deferred.
- **Where else.** ADR-0029, ADR-0027.
- **Status.** Trust table, promotion rule, starter routing and autonomous host: `built` (the autonomous host is expected to be denied until a dispatch grant exists). Decision-record emission: not established.

## ADR-0035 Process plane and data plane joined by embedded provenance

- **Decision.** Processes and ingestion are separate planes, split by who authors them. Data is input and output of a process, never a step in it. Provenance is read-only in the process plane and is a field on each assertion, naming the authoritative source, how it was obtained and as of when.
- **Why.** Domain experts should not need to read ingestion detail, and manual copies must not be laundered into sources.
- **Stated status.** Accepted for the plane boundary and provenance doctrine; data-plane exemplar deferred.
- **Where else.** ADR-0034, ADR-0029.
- **Status.** `accepted`. A provenance field named for how a value was obtained appears in ontology-service reads, but the code path was not traced, so nothing is marked `built`.

## ADR-0036 Config layering: seed, overlay, composition

- **Decision.** Shared mechanism and seed content live in the open repository. Deployment deltas live in a separate overlay repository and are composed there, before ingest. Composition must revalidate, record which layer asserted each entry, and let an overlay delete a seeded entry.
- **Why.** Content hashes must cover a readable file, and forking a seed stops inheriting mechanism updates.
- **Stated status.** Accepted for pattern and composition location; written before any overlay existed.
- **Where else.** ADR-0034, ADR-0035.
- **Status.** Overlay composition with deletion of seeded entries in the graph host: `built`. Composition in the overlay repository's CI, and per-entry layer provenance: not established.

## ADR-0037 Docs corpus and help surface

- **Decision.** The knowledge graph is the index and documentation pages are the leaves, joined by an explains edge. Pages are ratifiable config. Answering is a ladder: resolve an anchor through the graph, then nominate by vector search with entitlement filtering, then abstain. An amendment limits the first slice to vocabulary declarations and seals.
- **Why.** A help answer should be traceable to a page the graph points at, and the system should decline when no page applies.
- **Stated status.** Proposed; design settled, with amendments. The first slice is vocabulary and seals only; the overlay by declared identifier is design only.
- **Status.** Vocabulary, prefix registration, the explain handler with abstain, and the entitlement partition: `built`. Vector nomination (rung 2): `proposed`. Overlay slice: `proposed`.

## ADR-0038 Telemetry as a provenance projection

- **Decision.** Provenance fields are mapped onto the primitives of a language-model tracing service. Trace identifiers derive from the artifact. The mapping is ratifiable config, emission fails soft and is counted, and content fields are declared for redaction. An amendment makes the boundary span emit once, inside its own Restate step, so a replay does not double it.
- **Why.** Traces and provenance should describe one history, and a retry must not write it twice.
- **Stated status.** Partly implemented. The shape validator and truth check are live; scores and the later stages are specification only.
- **Status.** Mapping file, shim, truth check and the journaled boundary span: `built`. Scores, artifact-keyed tracing, prompt-hash linkage and the migration: `accepted`. The leaf package behind the shim is outside the repo: not established.

## ADR-0039 Workflow definition authoring

- **Decision.** YAML is the authoritative form of a workflow. A schema is committed and generated with a drift test. Process-notation export is a generated projection and import is rejected. Step kinds are renamed. An amendment moves choice into decision tables whose rows match by equality or membership and are total and unique, composed by the shared composer with a key field.
- **Why.** One source of truth avoids a second authoring surface, and total, unique tables make routing checkable.
- **Stated status.** Proposed, with a 2026-09 amendment on decision tables.
- **Status.** Decision tables and their totality and uniqueness seals: `built`. Committed generated schema, export and the rename: `proposed` (the old step kind is still used in a policy file). Import rejection is the ruling, but a legacy path at the pin does import: an authenticated compile endpoint upserts a permissively parsed payload into a catalog table that the dynamic job factory turns into jobs, and a second caller coerces steps and edges the same permissive way. The gateway's own note marks that path superseded by the committed-YAML route and scheduled for deletion.

## ADR-0040 Board as a generated index

- **Decision.** The work board is generated from packet headers. Status uses a closed five-value vocabulary. Items name a repo when cross-repo and carry a marker when tied to code. Seals cover drift, parked items needing a blocker or trigger, and closed items needing a resolvable, attributable closer.
- **Why.** A hand-kept board drifts from the packets it summarizes.
- **Stated status.** Proposed.
- **Status.** Generator, vocabulary, parked rule, closer checks and drift seal: `built`. Header migration is partial (the generated file discloses unheadered packets). Per-marker and orphan seals: not established.

## ADR-0041 User-contributed documents

- **Decision.** User documents are ingested on arrival with a structural unvouched stamp set by the entry door. Truth is granted later by a separate promotion event, recorded as a new fact plus a decision record, never by rewriting the provenance block. Rejection is a keyed sweep. The weakest provenance rides on the answer. Duplicates key on issuer, number and revision, and the official feed wins.
- **Why.** Users need to add material without it being mistaken for ratified source.
- **Stated status.** Proposed; build deferred; a delegate amendment ruled 2026-10.
- **Status.** Promotion and rejection on the approval plane, and the user-drop stamp at the gateway: `built`. Label on the answer envelope, classifier-then-human flow, delegate part and drop-box UI: not established.

## ADR-0042 Live view artifacts

- **Decision.** A live view is an archetype, not an artifact kind. The card declares what is wanted and the selector decides. The measure is a server-side verb, content is the state master, and an anonymous caller is refused a live view. A typed intent catalog restricts an earlier ADR.
- **Why.** Live figures should come from a verb the server runs, so nothing is stale or client-computed.
- **Stated status.** Proposed, written before the build; its rollout section lists what was built and what is ruled but not built.
- **Status.** Planning verbs, the period-series and threshold-grid contracts, and the anonymous refusal: `built`. Per-evaluation validity stamping and the intent catalog: `accepted`. Behavior in a deployed topology: not established.

## ADR-0043 Presentation trace

- **Decision.** The selector's evaluation is returned to the client unrenamed as a trace on the answer body. It is evidence only, so nothing may branch on it. The display must tell a satisfied candidate from a not-evaluated one.
- **Why.** A reader should see why a presentation was chosen and what else was considered.
- **Stated status.** Proposed; build deferred behind a trigger.
- **Status.** `proposed`. No reference to the field was found in agent, source or test directories.

## ADR-0044 Routing ticket credentials minted per request

- **Decision.** A ticket carries only a credential the broker minted for that request: scoped to the asset, expiring with the access window, read-only. The producer's writing credential is never advertised. Each backend declares a mode: mint a temporary object-store credential, mint a database role, or proxy only. A backend with no declared mode does not advertise. Hostnames must be fully qualified.
- **Why.** A credential copied from the producer outlives the request and can write.
- **Stated status.** Accepted. A same-day amendment moved minting into the broker, because the coordinate lookup runs once at startup.
- **Where else.** [The domain broker](../components/domain-broker.md); [dag-tools-s4n-s4o-s4k1.md](dag-tools-s4n-s4o-s4k1.md).
- **Status.** Object-store, ClickHouse and Postgres minting, dropping the echoed credential, fail-closed on mint failure, and reporting non-qualified hosts: `built`. A source type with no minter still gets the producer credential, counted as unprotected: `built`, as a deviation from the no-advertise rule. Proxy-only mode and reaping of minted database accounts: `not built`. The legacy no-coordinates SQL path does not mint.

## ADR-0045 Finance engine verbs

- **Decision.** A finance engine offers six deterministic verbs (variance, estimate at completion, performance indices, burn rate, variance drivers, funding status). The estimate method slot is mandatory with no default. Data is notional and reads go through the mesh on per-request tickets. The engine is a template for later ones.
- **Why.** Money figures must come from fixed arithmetic with explicit method choice.
- **Stated status.** Accepted, sequenced after the planning engine; an amendment replaced the archetype column and refused a ranked-set archetype.
- **Status.** Six verbs registered, mandatory method refusal, and ticketed reads: `built`. Tree archetype, notional-data boundary and the template claims: not established.

## ADR-0046 Agent-graph library graphs as mesh verbs

- **Decision.** One graph is one registered verb with a full contract. A generic run-any-graph runner is refused by name. The output class is a subclass of the response class, so it is never grounded, and internal steps are telemetry, not provenance.
- **Why.** A verb contract is what the mesh can authorize and audit.
- **Stated status.** Proposed, "not started"; an amendment retired the exemplar slice. The header is out of date against the code.
- **Status.** Row-driven registration, 404 for unloaded ids, refusal enforcement and refusal to start with no rows: `built`. Durability, memory and entitlement seal sections: not established.

## ADR-0047 Computation export

- **Decision.** A package is produced by a registered export verb with a mandatory recipient scope. Zip scripts are refused. The package carries real computation modules at a pinned commit plus a verification manifest. Filtering happens once per recipient. A service identity is never a recipient. Fixes propagate by re-issuance.
- **Why.** A recipient needs to re-verify the algorithm, and a service identity as recipient is a confused-deputy risk.
- **Stated status.** Proposed, "not started"; a section 5.1 amendment ratified 2026-09.
- **Status.** A package-export route in the finance engine with recipient scope, a verification manifest and per-recipient file names: `built` for that engine, not checked against every rule. Pinned-commit modules, service-identity refusal and supersedes chain: not established.

## ADR-0048 External validation package

- **Decision.** First consumer of ADR-0047: two formats (an HTML package and a notebook) from one emit, with differing guarantees, proven first on notional data.
- **Why.** A format decision should rest on a measured notional run.
- **Stated status.** Proposed, "not started"; only the header and outline were read.
- **Status.** Not established. The finance engine's export module refers to this ADR and writes an HTML file and a database file; a notebook output was not found. Notebook format: `proposed`.

## Not established

- Checked only in part: the ADR-0027 status. A threshold join exists in the workflow join module, but it serves human-await steps under ADR-0029, not grant issuance, so it was not counted for ADR-0027.
- Checked only in part: ADR-0035. Provenance fields appear in ontology-service reads, but the path was not traced, so the ADR stays `accepted` with no source cited.
- Checked only in part: ADR-0031 rung 4 flagging (a recall-guard module exists, not read in depth). A second copy of the matching functions exists in the ontology service with its own descriptor set, which the ADR does not mention.
- ADR-0025 production population of the access-decision slot and enforcement on every retrieval path; ADR-0026 picker and mapper retirement and the end-to-end probe; ADR-0028 card provenance; ADR-0033 frequency gate (the ADR says it has not fired) and the user-confirmed provenance tier; ADR-0034 decision-record emission; ADR-0036 composition in CI at the overlay repository and per-entry layer provenance. The graph host composes overlay directories at service start, which is a different place than the ADR names.
- Checked only in part: ADR-0037 rung 2 not found by search, which may have missed it. ADR-0039 schema generator absence rests on a non-exhaustive search. ADR-0045 line ranges for verb entries are the first entry to the last, not every line between.
- leaf telemetry package internals (0038); a separate newer board system and its link to 0040; answer-envelope label and delegate flow (0041); where the user-drop constant is defined (0041); deployed behavior of live views (0042); archetype per finance verb and notional boundary (0045); graph durability and entitlement seal (0046); pinned-commit modules, service-identity refusal, notebook output (0047, 0048).
- ADR-0047 and 0048 were judged mostly from the finance export module header, which carries both numbers; the export-profile files are a separate feature and were not read in depth. The ADR-0044 status of ClickHouse settings-profile pinning is not established.
- Headers saying Proposed or not started understate code for 0040, 0041, 0042, 0046 and 0047. ADR-0045's header says Accepted but defers the build, and understates the code the same way.

## Sources

All paths are in invincible-agent at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb` (IA) or in dag-tools at `add5bc48021fe6d2c79c24c31072142fc93c8179` (DT). Line ranges are at those commits. Tests are named by function, with their line in the test file. `aaw` is IA `src/iagent/answer_artifact_writer.py`, `ans` is IA `agent_fleet/restate_analyst/answer_step_seeding.py`, `arw` is IA `agent_fleet/restate_analyst/autonomous_review_workflow.py`, `ath` is IA `src/iagent/auth.py`, `cap` is IA `agent_fleet/presentation_agent/capability_registry.py`, `dca` is IA `agent_fleet/docs_agent/main.py`, `dsq` is DT `dag_tools/domain_broker/sql_minting.py`, `dtb` is DT `dag_tools/domain_broker/main.py`, `dtm` is IA `agent_fleet/restate_analyst/decision_table.py`, `dyf` is IA `src/iagent/defs/dynamic_factory.py`, `ent` is IA `agent_fleet/finance_agent/entities.py`, `exp` is IA `agent_fleet/finance_agent/export.py`, `fin` is IA `agent_fleet/finance_agent/main.py`, `gbd` is IA `scripts/generate_board.py`, `ghm` is IA `agent_fleet/graph_host/main.py`, `gwy` is IA `src/iagent/gateway.py`, `imm` is IA `agent_fleet/datahub_wrapper/instance_match.py`, `ogw` is IA `src/iagent/origin_writer.py`, `pma` is IA `agent_fleet/presentation_agent/main.py`, `pro` is IA `src/iagent/promotion.py`, `rab` is IA `agent_fleet/restate_analyst/main.py`, `rvs` is IA `agent_fleet/restate_analyst/review_starter.py`, `sld` is IA `src/iagent_pure/slot_disposition.py`, `sse` is IA `agent_fleet/restate_analyst/spo_step_executor.py`, `tcm` is IA `helm/invincible-agent/templates/topaz-configmap.yaml`, `tpc` is IA `src/iagent/authz/topaz_client.py`, `ttb` is IA `agent_fleet/utils/trust_table.py`, `ttl` is IA `setup/ontologies/mesh_system.ttl`, `wdf` is IA `agent_fleet/restate_analyst/workflow_definition.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| ADR-0025 deny-by-default data gate | tcm `:351-391` | ADR-0025 | no test | `built` |
| ADR-0025 entitlement source captured | aaw `:603-645`, ath `:132-136` | ADR-0025 | test_entitlement_source_required_on_user_model (:72), test_topaz_state_constructs (:83), test_none_state_is_honest_empty (:99), test_retired_jwt_origin_values_are_rejected (:117) | `built` |
| ADR-0025 access-decision slot on citation | aaw `:651-692` | ADR-0025 | test_layer_1_2_3_populated_access_decision_roundtrips_to_neo4j (:160), test_layer_1_2_null_access_decision_roundtrips_as_null (:219), test_layer_6_cortex_ui_source_type_declares_access_decision (:303) | Slot: `built`. Population: not established |
| ADR-0026 matrix, 403/503, claim ignored | tcm `:276-335`, tpc `:60-69`, tpc `:237`, tpc `:366`, gwy `:12076-12090` | ADR-0026 | test_honest_empty_when_no_entitlements (:80), test_persona_from_topaz_default_cell (:108), test_persona_from_first_cell_when_no_default (:130), test_jwt_persona_claim_is_ignored (:148) | Matrix, 403 and 503 split, claim ignored: `built`. Picker and mapper retirement: not established |
| ADR-0027 single-dimension task audience: single dimension | tcm `:216-230` | ADR-0027 | no test | `built` |
| ADR-0027 single-dimension task audience: composable engine | tcm `:216-230` | ADR-0027 | no test | `accepted` |
| ADR-0028 canvas card provenance | — | ADR-0028 | no test | `accepted` |
| ADR-0029 step dispatch with caller identity | sse `:181-224`, wdf `:92` | ADR-0029 | no test | `built` |
| ADR-0029 answer seeds an operation step | ans `:81-104` | ADR-0029 | test_a_single_asset_verb_on_the_SKIP_path_is_flagged_needs_instance (:154), test_the_flag_is_NOT_set_when_the_ask_carried_an_instance (:172), test_a_multi_arity_verb_is_never_flagged (:182), test_NO_MODEL_CALL_IS_MADE_ON_THE_SKIP (:192), test_the_route_carried_is_the_route_returned (:209), test_a_verb_that_is_no_longer_compatible_ROUTES_THE_FULL_PATH (:221) | `built` |
| ADR-0030 edgeless topology degrades honestly | pma `:411-450`, pma `:1255-1262` | ADR-0030 | tests/test_topology_degrade.py: test_list_outcome_degrades_to_document_with_summary (:27), test_every_say_so_outcome_degrades_not_just_the_list (:38), test_structured_data_may_arrive_as_a_dict_already (:47), test_empty_summary_still_yields_a_coherent_document (:54), test_no_discriminant_means_real_topology_render (:62), test_discriminant_but_nonempty_edges_is_a_real_graph (:69), test_non_dict_payload_declines (:76); tests/test_lineage_answer.py: test_genuinely_none_is_distinct_from_walk_failure (:93), test_access_denied_walk_is_a_failure_not_none (:106) | Degradation: `built`. Producer side: not established |
| ADR-0031 exact and containment rungs: rungs 1 and 2 | imm `:37-92` | ADR-0031 | test_strip_removes_entity_type_and_platform_and_articles (:18), test_strip_is_a_noop_when_no_descriptors (:24), test_core_name_inside_descriptor_phrase_is_a_strong_hit (:30), test_exact_and_suffix_unchanged (:35), test_lone_descriptor_word_does_NOT_win (:41) | `built` |
| ADR-0031 exact and containment rungs: rung 3 | imm `:37-92` | ADR-0031 | test_strip_removes_entity_type_and_platform_and_articles (:18), test_strip_is_a_noop_when_no_descriptors (:24), test_core_name_inside_descriptor_phrase_is_a_strong_hit (:30), test_exact_and_suffix_unchanged (:35), test_lone_descriptor_word_does_NOT_win (:41) | `accepted` |
| ADR-0032 analyst loop and goal-shape card | — | ADR-0032 | no test | `proposed` |
| ADR-0033 ask for unfilled mandatory slot | sld `:312`, sld `:585`, sld `:787`, sld `:848` | ADR-0033 | test_A1_H06_a_required_slot_named_nowhere_asks (:62), test_A2_E05_a_name_that_resolved_to_the_wrong_kind_asks (:77), test_A3_a_missing_mandatory_slot_is_an_ask_and_not_a_400 (:125), test_E04_a_genuine_miss_on_an_OPTIONAL_slot_still_routes (:172) | Ask: `built`. Confirmed-pick tier: not established |
| ADR-0034 trust table and promotion rule | ttb `:106-170` | ADR-0034 | test_an_unlisted_format_is_supervised (:57), test_an_empty_table_permits_no_autonomy_at_all (:65), test_a_pipeline_upgrade_re_supervises (:73), test_supervised_to_trusted_is_forbidden (:92) | `built` |
| ADR-0034 starter routes by rung | rvs `:364`, arw `:1-25` | ADR-0034 | test_unpromoted_format_routes_to_workflow_1 (:105), test_promoted_format_routes_to_workflow_2 (:114), test_the_pair_discriminates_in_one_run (:128), test_pipeline_version_mismatch_re_supervises (:139), test_unreadable_table_supervises_and_does_not_block (:151) | `built` |
| ADR-0035 plane boundary and provenance | — | ADR-0035 | no test | `accepted` |
| ADR-0036 overlay compose with deletion | ghm `:105`, ghm `:378` | ADR-0036 | test_the_sample_overlay_EXISTS_and_is_labelled (:63), test_A_TOMBSTONE_REMOVES_A_SEEDED_KIND (:124), test_A_TOMBSTONE_FOR_AN_UNSEEDED_KEY_IS_AN_ERROR (:136), test_CONTROL_an_EMPTY_overlay_composes_to_exactly_the_seed (:152) | Graph-host composition: `built`. Composition in the overlay repository's CI: not established |
| ADR-0037 explains edge, abstain, vocabulary: vocabulary, explain handler and partition | ttl `:775-797`, dca `:380-425` | ADR-0037 | test_the_doc_vocabulary_is_declared_in_a_ttl_that_actually_primes (:94), test_the_docs_prefix_is_registered_on_the_WRITE_side (:124), test_the_committed_corpus_matches_the_frontmatter (:43) | `built` |
| ADR-0037 explains edge, abstain, vocabulary: rung 2 and overlay slice | ttl `:775-797`, dca `:380-425` | ADR-0037 | test_the_doc_vocabulary_is_declared_in_a_ttl_that_actually_primes (:94), test_the_docs_prefix_is_registered_on_the_WRITE_side (:124), test_the_committed_corpus_matches_the_frontmatter (:43) | `proposed` |
| ADR-0038 journaled boundary span, mapping: mapping, shim, truth check and boundary span | rab `:1468-1474` | ADR-0038 | test_boundary_is_emitted_once_across_a_replay (:180), test_mesh_mapping_truth_check (:75) | `built` |
| ADR-0038 journaled boundary span, mapping: scores and migration | rab `:1468-1474` | ADR-0038 | test_boundary_is_emitted_once_across_a_replay (:180), test_mesh_mapping_truth_check (:75) | `accepted` |
| ADR-0039 decision tables total, unique: decision tables and seals | dtm `:91-233` | ADR-0039 | test_TOTALITY_every_input_the_domain_admits_has_a_row (:183), test_UNIQUENESS_no_input_matches_two_rows (:195) | `built` |
| ADR-0039 decision tables total, unique: generated schema, export and rename | dtm `:91-233` | ADR-0039 | test_TOTALITY_every_input_the_domain_admits_has_a_row (:183), test_UNIQUENESS_no_input_matches_two_rows (:195) | `proposed` |
| ADR-0039 legacy import path, marked superseded | gwy `:12504-12566`, gwy `:10091-10123`, gwy `:12960-12972`, dyf `:73`, dyf `:465` | ADR-0039 | no test located | Legacy import path present: `built`, marked superseded and scheduled for deletion |
| ADR-0040 generated board with seals | gbd `:16-175` | ADR-0040 | test_board_matches_the_packet_headers (:44), test_the_check_can_actually_fail (:59) | Generator, closer checks and drift seal: `built`. Per-marker and orphan seals: not established |
| ADR-0041 promotion on approval plane | pro `:191-690`, gwy `:13902`, ogw `:54` | ADR-0041 | test_the_BLOCK_is_BYTE_IDENTICAL_across_a_promotion (:176), test_a_REJECTION_writes_the_record_THEN_sweeps_graph_indexes_and_MOVES_the_objects (:192) | Promotion and drop stamp: `built`. Envelope label and drop-box: not established |
| ADR-0042 live views, anonymous refusal: verbs, contracts and anonymous refusal | cap `:425-455` | ADR-0042 | test_an_anonymous_caller_asking_for_a_live_view_is_REFUSED (:65), test_an_IDENTIFIED_caller_gets_its_live_view (:111) | `built` |
| ADR-0042 live views, anonymous refusal: validity stamping and intent catalog | cap `:425-455` | ADR-0042 | test_an_anonymous_caller_asking_for_a_live_view_is_REFUSED (:65), test_an_IDENTIFIED_caller_gets_its_live_view (:111) | `accepted` |
| ADR-0043 presentation trace on answer | — | ADR-0043 | no test | `proposed` |
| ADR-0044 per-request minted credentials: per-request minting and fail-closed | dtb `:226-365`, dsq `:152-246` | ADR-0044 | test_consumer_receives_a_minted_credential_not_the_producers (:134), test_minted_policy_is_scoped_to_the_asset_and_cannot_write (:148), test_minting_failure_fails_closed (:197) | `built` |
| ADR-0044 per-request minted credentials: proxy-only mode and reaping | dtb `:226-365`, dsq `:152-246` | ADR-0044 | test_consumer_receives_a_minted_credential_not_the_producers (:134), test_minted_policy_is_scoped_to_the_asset_and_cannot_write (:148), test_minting_failure_fails_closed (:197) | `not built` |
| ADR-0045 six finance verbs, method required | fin `:245-392`, ent `:86` | ADR-0045 | test_the_eac_method_values_come_out_of_the_literal (:134) | Verbs, method refusal and ticketed reads: `built`. Archetype and template claims: not established |
| ADR-0046 one graph one verb | ghm `:371-607` | ADR-0046 | test_every_ratified_row_is_a_verb_in_neo4j (:129), test_a_host_with_no_ratified_rows_REFUSES_TO_START (:208) | Registration and refusals: `built`. Seal sections: not established |
| ADR-0047 export verb with recipient scope | exp `:69-215` | ADR-0047 | no test | Export route in the finance engine: `built`. Pinned modules and supersedes chain: not established |
| ADR-0048 external validation package | exp `:82-87` | ADR-0048 | no test | Not established. Notebook format: `proposed` |
