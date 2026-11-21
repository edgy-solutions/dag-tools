# invincible-agent decisions: ADR-0001 to ADR-0024

invincible-agent records its decisions as numbered ADRs in its own repository, under `docs/adr/`. This record indexes ADR-0001 to ADR-0024 at invincible-agent commit `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. For each ADR it gives what the ADR decides and why, in short, what the ADR's own header states, and what the code at that commit does. It points rather than restates: the ADR is the decision, and where an ADR and any line here disagree, the ADR holds. [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). "Stated status" is the ADR's own header, paraphrased; "Status" is this record's reading of the code at the pin. Several headers say less is built than the code shows, or more. Line numbers are at the pin. The record shape is provisional.

An ADR number here is always invincible-agent's own series, cited elsewhere as "invincible-agent ADR-NNNN". A bare section number in a dag-tools record means the dag-tools census.

## ADR-0001 Decouple the memory library's model from the reasoning model

- **Decision.** The model used inside the memory library is chosen by its own environment variable, then by the shared reasoning-model variable, then by a provider default. The embedder is left alone.
- **Why.** Memory writes were slow, and a reasoning model crashed the runner when asked for structured output.
- **Stated status.** Accepted.
- **Where else.** ADR-0003 generalises this to all workload classes.
- **Status.** `built`. Resolution chain at mem:532-538. Pinned by one end-to-end test that skips without containers, an inference server and a network.

## ADR-0002 Two patches to the memory library

- **Decision.** Two runtime patches are installed before the library is imported. One guards the re-ranking step against a missing score. The other makes the vector-store output parser carry the score through.
- **Why.** Both defects broke retrieval, and the library offers no hook for either.
- **Stated status.** Accepted.
- **Status.** `built`. Guard at mem:64-99, score propagation at mem:119-147, library import at mem:149. The round-trip test checks that both patches took effect.

## ADR-0003 Model choice per workload class

- **Decision.** Each workload class gets its own model variable: reasoning, structured output, memory, embedding. A rollout runs in three phases, memory first.
- **Why.** One model for every class is too slow for some work and unsuitable for other work.
- **Stated status.** Accepted. Phase 1 done, later phases planned.
- **Where else.** ADR-0001.
- **Status.** Phase 1 (memory): `built`, mem:535. Phase 2 (structured-output variable): `accepted`, no code hit. Phase 3 (embedding variable): `accepted`. The code has a differently named memory-embedder variable at mem:399 and mem:445, which is not the variable the ADR names.

## ADR-0004 Routing through a graph of typed predicates

- **Decision.** A tool, agent or engine is registered as a typed predicate: a named edge between two concept classes, with routing data on the edge. Persona, domain and intent become views over that graph. Work is split into steps A to F.
- **Why.** Fixed classification axes need a code change for every new engine.
- **Stated status.** Accepted.
- **Where else.** ADR-0005, ADR-0006, ADR-0009.
- **Status.** Step C (path and tool lookup as a plain graph query): `built`. Steps A, B, D, E, F: not established at the pin. Steps A and B live in other repositories. ADR-0009 replaces the wording of steps E and F.

## ADR-0005 Two kinds of namespace for verbs and concepts

- **Decision.** Domain prefixes belong to the domain's own ontology. One platform prefix, `mesh:`, belongs to the platform. Every verb and concept carries an authority marker, and the registration checks differ per kind.
- **Why.** A platform-minted term and a domain-owned term need different rules for who may add them.
- **Stated status.** Accepted.
- **Where else.** ADR-0007.
- **Status.** Authority marker on registered predicates: `built`. Rejection of unknown domain URIs at the proposal stage: not established. Prefix registry file named by the ADR: `not built`.

## ADR-0006 DataHub for proposals, Neo4j for runtime routing

- **Decision.** DataHub holds the proposal queue. Neo4j serves routing. Sync runs one way, DataHub to Neo4j, and drift is reported, not repaired. Two later addenda make the registrar gateway the only writer of tool edges in Neo4j and the vector store, using a Restate saga, and make engine source declarations the authority.
- **Why.** A half-registered verb is unroutable, because reads need both stores to agree.
- **Stated status.** Accepted, amended by two addenda in June.
- **Where else.** ADR-0004, ADR-0007.
- **Status.** Routing reads Neo4j only: `built`. Registration saga with bounded retry, read-back probe and compensating deletes: `built`. Drift-reconciliation asset: `not built`; the ADR defers it. Sole-writer claim: `accepted`, no standing guard found. Source-declaration rule: `built`, with no guard that runs without a database: the coverage guard test_substrate_covers_routing_via_v02_saga_edges (tests/routing/test_substrate_invariants.py:914) holds it, and needs a live graph store.

## ADR-0007 Survey existing standards before minting a term

- **Decision.** Before minting a `mesh:` concept or verb, check a fixed list of existing vocabularies in order and take the first fit. Mint only when none fits, and log one row per mint.
- **Why.** Homegrown terms drift from standards and are costly to retire.
- **Stated status.** Accepted. Retroactive mappings are proposals only.
- **Where else.** ADR-0005.
- **Status.** Mint log: `—`, a document with twelve entries, no test. Survey rule: `accepted`; no code enforces it. Retroactive mappings: `proposed`.

## ADR-0008 Generalist fallback when routing finds nothing

- **Decision.** If predicate search returns no match, or a top score under a threshold, the supervisor sends the subtask to the generalist engine with a reason flag. An infrastructure outage or an engine error does not trigger the fallback. The threshold is a supervisor setting. Each decision writes a log line that metrics can be built from.
- **Why.** An honest answer from a generalist beats an abort, but hiding an outage behind it would mask the real signal.
- **Stated status.** Accepted. Migration plan marked implemented, except an operator-doc step.
- **Where else.** ADR-0009, ADR-0010.
- **Status.** Threshold, three-way status, fallback call, payload fields, prompt preamble and log lines: `built`. Operator doc step: not established. The code has since added a case the ADR does not list: a classifier abstain is answered directly, not sent to the fallback.

## ADR-0009 Retire persona, domain and intent as routing axes

- **Decision.** Intent becomes a two-value mode. Domain becomes a scope filter. Persona splits into user persona and answerer persona. The three-axis classifier is replaced by mode and entity extraction plus hybrid predicate search. A later correction says the vector store is required, with no graph-query fallback.
- **Why.** The classification grid was brittle, and verbs lacked the semantic matching nouns already had.
- **Stated status.** Proposed, with a recorded mid-course correction.
- **Where else.** ADR-0004, ADR-0008.
- **Status.** Hybrid predicate search endpoint, mode-and-entities extraction call, removal of the master persona, domain and intent dicts: `built`. Identity-claims work: not established. Persona and domain views as graph queries: not established.

## ADR-0010 OpenTelemetry tracing at the HTTP boundary

- **Decision.** Use OTel for traces across service boundaries only. Leave Dagster ops, language-model interpretation, pure compute and lineage to the tools that own them. Propagate the standard trace-context header, sample 1% in production, and pick an OTel collector backend later.
- **Why.** One tracing source per concern avoids duplicate telemetry that drifts.
- **Stated status.** Proposed.
- **Where else.** ADR-0008.
- **Status.** `proposed` as written. There are no OpenTelemetry auto-instrumentation packages and no `traceparent` propagation at the HTTP boundary. The OpenTelemetry API and SDK are direct dependencies (pyproject.toml:26-27); the exporter distributions are not. Entry-point spans exist on a different telemetry primitive under a later ruling, carrying their own headers.

## ADR-0011 Multi-hop routing from natural language

- **Decision.** Deferred. The ADR lays out where chain awareness could live and pre-commits only a framing: chains inside one engine stay there; cross-engine chains, if built, use the path endpoint.
- **Why.** Cross-engine chains are real but unmeasured, so building for them now is speculative.
- **Stated status.** Proposed; decision deferred.
- **Where else.** ADR-0004, ADR-0008, ADR-0009.
- **Status.** `proposed`. No code for the ADR's own claim.

## ADR-0012 UI archetype rigidity

- **Decision.** Deferred. Direction favoured: make the metric archetype accept dynamic columns first, then widen the other archetypes if needed. Free-form generative UI is rejected. ADR-0017 replaces this ADR.
- **Why.** The fixed archetype set loses data shape, but typed archetypes keep answers grounded.
- **Stated status.** Proposed; decision deferred with a recommended direction. The header still reads Proposed although ADR-0017 replaces it.
- **Where else.** ADR-0009.
- **Status.** `—`, superseded by ADR-0017. The metric archetype at bam:776-781 still has a fixed entity list.

## ADR-0013 The catalog engine's capability surface

- **Decision.** The catalog engine keeps its single search tool and grows by adding one narrow capability tool per change, starting with owner, lineage, assets by owner, stale assets and personal-data assets with no owner. A generic query-passthrough option is rejected. The choice between hand-written tools and generating them is deferred until about fifteen tools exist.
- **Why.** In a setting that is audited, validated tools are worth more than the flexibility of free-form queries.
- **Stated status.** Proposed; the decision is recommended but marked deferred.
- **Where else.** ADR-0012 (the parallel UI concern), ADR-0017.
- **Status.** `proposed`. No capability tool of the named kind exists at the pin.

## ADR-0014 No hard-coded catalog hints; broker and catalog stay separate

- **Decision.** Agent prompts must not list catalog URNs or table names. The broker's local asset table holds only assets it can issue credentials for, and the catalog owns descriptive metadata. The gateway's routing-registry keys are not catalog data and must not enter model context.
- **Why.** A hand-kept list in a prompt drifts from the catalog and the model then trusts the stale copy.
- **Stated status.** Accepted. Open items remain: a lint for hard-coded URNs, an audit of verb descriptions, and a register-time platform check.
- **Where else.** [domain-broker component](../components/domain-broker.md); [ticket identity and the broker's cached coordinates](dag-tools-s4n-s4o-s4k1.md).
- **Status.** Rule 1 `built` (the URN hint block was removed; no enumerated catalog URNs remain in the agent entry points, and the scheme prefix survives in a parser at data_analyst/main.py:428-430, which ADR-0014 permits at :105), no test. Rule 2 `built` as a configuration shape (two entries), no test; the deployed copy is not established. Rule 3 `accepted` (a review rule, no code). The lint and the register-time check: `not built`.

## ADR-0015 Router regression testing at predicate search

- **Decision.** Agents are to declare example questions at registration, feeding four loops (registration validation, a canary pulse, an audit of live traffic, confidence gating) over one audit table. Phase 1 ships only a structured log line per routing decision, shaped like the later table.
- **Why.** Routing correctness depends on a graph that can change silently, so each decision needs a durable record before anything can be checked against it.
- **Stated status.** Phase 1 accepted. Phase 2 (table, canary schedule, drift detection) deferred.
- **Where else.** ADR-0019 (promotes the canary to required).
- **Status.** Phase 1 log `built`, no test of the emit itself. Phase 2 and the four loops: `accepted` (deferred, nothing exists).

## ADR-0016 Fact storage authority, read from the system of record

- **Decision.** Each kind of fact has one authority and is read from it on demand: catalog facts from the catalog and graph, ontology from the ontology store, routing predicates from the graph plus a vector store, caller facts from a memory library. Tier 0 turns off the memory library's model-driven fact extraction in the two engines that write to it. Limiting memory to caller facts, and a derived-fact subgraph, are later steps.
- **Why.** Manufactured copies of catalog facts go stale and contradict the source.
- **Stated status.** Accepted in a second revision; Tier 0 shipped. The first revision (a two-collection vector-store split) was withdrawn.
- **Where else.** ADR-0001, ADR-0015, ADR-0017.
- **Status.** Tier 0 `built`, no test. Memory limited to caller facts, the derived-fact subgraph and the revisit prerequisites: `accepted`.

## ADR-0017 Presentation as a predicate (replaces ADR-0012)

- **Decision.** How an answer is shown is chosen through the same predicate graph as everything else. Each rendering capability is registered per verb, and each front end registers which capabilities it can draw. An amendment sets the transport to graph registration, validates (but does not entitle) registrations, binds each decision to the caller's own menu, stamps the registration version on the answer, and labels the default menu used for an unregistered caller.
- **Why.** A fixed list of chart types kept in the presentation agent could not follow what each front end can actually render.
- **Stated status.** Proposed, with a later amendment ruling the mechanism. It supersedes ADR-0012.
- **Where else.** ADR-0012, ADR-0013, ADR-0015, ADR-0004.
- **Status.** Mixed. Per-verb registrations: `built`. Admission, menu-scoped selection, version stamp and labelled default menu: `built`. Retiring the generic fallback verb: `proposed`. Making component exports the single home of the contract and dissolving the hand-kept capability table: `not built` (the table still exists). Persisting decision provenance and re-resolving it for other consumers: not established.

## ADR-0018 Symmetric subject-predicate routing

- **Decision.** The ontology service gains a predicate-classification route that mirrors the subject-resolution route: vector recall, then one language-model choice among candidates with an explicit none-of-these option. The old verification step and its middle band are retired. An addendum inserts a graph query between the two routes that returns only verbs compatible with the resolved subject, and a subject with no compatible verb goes to the generalist without a model call.
- **Why.** Choosing a verb without first grounding the subject let the model pick confidently and wrongly.
- **Stated status.** Accepted, with an accepted addendum on the graph as the compatibility reasoner.
- **Where else.** ADR-0004, ADR-0019.
- **Status.** `built`, including the addendum.

## ADR-0019 The ontology is the routing substrate

- **Decision.** Routing is noun resolution over the ontology, in three legs: resolve the subject, query compatible verbs in the graph with no model, classify the verb. Four contracts follow. A: a single compatible verb still goes through the model with a two-value choice, and the model is skipped only for a verb explicitly marked default. B: every case that cannot confidently resolve falls to the one generalist, including an unresolved subject. C: registration is verified in both stores, the ontology is a protected snapshot-diffed artifact, and the canary is required. D: a verb's input and output types must already exist as ontology classes and are never created by registration; identity is the pair (verb, tool).
- **Why.** The graph proves only that a verb is available for a subject, not that it answers the question, and an unprotected graph can be wiped or polluted without anyone noticing.
- **Stated status.** Proposed.
- **Where else.** ADR-0004, ADR-0008, ADR-0015, ADR-0018, ADR-0011.
- **Status.** Mixed. Contract A: `built`. Contract B: `built` for the unresolved-subject and no-compatible-verb routes. Contract C: `proposed` (its self-verify and drop-detection tests are skip-marked placeholders). Contract D: `proposed` (its rejection tests are skip-marked placeholders). The dedup rule, the pair identity and the reference binding pattern: not established.

## ADR-0020 Structural disambiguation at weak anchors

- **Decision.** The compatible-verb graph walk would gain a second traversal axis over declared composition edges (for example a document and its content), alongside the existing inheritance walk. The traversal stays in the graph query, is limited to named weak boundaries, and the model still sees one flat candidate list. Direction, depth and dedup precedence are left to the implementing change.
- **Why.** Named identifiers already bypass the vocabulary contest; unnamed queries at a document-versus-content boundary have no such protection.
- **Stated status.** Proposed; design and evidence only, explicitly not implemented, with a trigger list for when to build it.
- **Where else.** ADR-0011, ADR-0019.
- **Status.** `proposed`. The existing inheritance-only walk is in the code; no composition axis is.

## ADR-0021 Deterministic content-kind selection at ingest

- **Decision.** The content kind is never chosen by a language model. A declared kind in the manifest wins, then a kind derived from the storage path, otherwise ingest halts with a named error. A code-owned table maps each kind to its extractor settings and target ontology class, and ingest stamps an instance-of edge. Amendments make the path segment under the user-drop prefix a file format rather than a kind (so path derivation is off there), keep the declared kind as the only rule at that door, and add a third format leaf.
- **Why.** A single hard-coded kind left extracted instances unreachable by routing, and a silent default is how that happened.
- **Stated status.** Proposed, with two amendments (user-drop door; third format leaf). Only the single-kind edge stamping was marked for immediate work.
- **Where else.** ADR-0019, ADR-0041.
- **Status.** Mixed. Gateway side of the amendments (declared kind written to manifest metadata, format leaves, events refused as a file kind, a format leaf in the ontology): `built`. The precedence rule, the mapping table and the halt: not established here, since the driver that applies them is outside this repository. The driver recording a failed stage and the kind-to-format binding: `accepted` as open items.

## ADR-0022 DataHub integration through an owned wrapper

- **Decision.** The integration layer stays the owned wrapper. The vendor's own tool server is treated as a reference for correct usage and mined for edge-case handling, and may be called as a backend for specific capabilities that are costly to rebuild. Wholesale adoption is rejected.
- **Why.** A failure was traced to the wrapper mishandling an error that DataHub reported correctly, so the fix is to learn from the reference rather than discover cases by failure.
- **Stated status.** Proposed; to be ratified before the next wrapper change.
- **Where else.** ADR-0013, ADR-0024.
- **Status.** Mixed. Owned wrapper with hand-kept entity-type tables in both directions: `built`. The follow-up porting tasks and the per-capability backend audit: `proposed`.

## ADR-0023 Answer artifact as a graph-native object with a separate read model

- **Decision.** An answer artifact is a graph node in Neo4j, the source of truth, with timestamps, the question and resolved intent, the rendered output, a required valid-as-of and an optional valid-until, plus typed edges such as the producing actor. A read model in Postgres, fed by a Neo4j-to-Postgres projector and synced to the UI by a read-sync service, serves the canvas. Edges for standards (process, conformance, identity, scope) are reserved vocabulary only.
- **Why.** Answers must persist with their provenance and freshness, and the canvas needs a fast, offline-capable view.
- **Stated status.** Proposed.
- **Where else.** ADR-0024, ADR-0017.
- **Status.** Mixed. Node write with idempotent merge, valid-as-of and producer edge: `built`. Projector and Postgres read table: `built`. The read-sync service deployment template exists; its wiring to the canvas is not established. The four standards edges: `not built` (reserved by the ADR). Collaborative edits and the canvas metaphor: `accepted` as deferred.

## ADR-0024 Standards composition and publish/promotion

- **Decision.** Part A reserves the place for composing external process-notation, architecture-description, and data-product and data-contract standards, to be written when a first integration needs it. Part B rules that publishing is a one-way emit: the target tool becomes the source of truth, iagent keeps the frozen answer and a thin reference node, publishing runs backend-side on an artifact id, a staleness gate applies at the boundary, and a missing target is marked orphaned without restoring content. An amendment limits the sequencing stop to tool targets and adds a recipient target type.
- **Why.** One authority per concept; a copy kept after publishing could show a deleted asset as live.
- **Stated status.** Part A reserved; Part B proposed and later amended.
- **Where else.** ADR-0022, ADR-0023, ADR-0047.
- **Status.** Part A: `proposed` (reserved). Part B: `proposed`. The read table carries nullable placeholder columns for it, which do not implement the rules.

## Not established

- Checked only in part: ADR-0009 `built` parts rest on greps. The master persona, domain and intent dicts have no definitions in the ontology service at the pin, only comments (ont:478). The extraction function body was not read.
- Checked only in part: the ADR-0009 test (test_a1_infra_error_records_no_verb) is in tests/routing/test_an_early_exit_records_no_verb_and_its_reason.py and covers the infra-error path only. Weak pin.
- Checked only in part: the ADR-0004 test names are from tests/test_ontology_routing.py; their assertions were not opened to confirm they check the graph-query shape.
- Checked only in part: ADR-0005: test_emits_full_predicate_payload (:176) also asserts domain authority for a domain prefix; two nearer tests are cited instead.
- Checked only in part: the ADR-0008 abstain case comes from comments at sup:2906-2923, not from a test.
- ADR-0004 steps A, B, D, E, F; ADR-0005 propose-time rejection and the prefix registry file; ADR-0006 concurrency tests, conjunctive-read guard test, doc-sync sole-writer claim; ADR-0009 identity-claims work; ADR-0008 operator doc step.
- ADR-0010: the OpenTelemetry API and SDK are direct dependencies at pyproject.toml:26-27, and the exporter distributions are not. Entry-point spans exist on a separate primitive (src/iagent/trace_spans.py, entered at gwy:8311, :14088 and :18072), with a parent-span header of their own. A trace header is parsed at wfr:683-690 through a separate telemetry package. An older trace header is still read in two services. Neither matches the ADR's plan, so it stays `proposed`.
- Checked only in part: ADR-0014 rule 1 is judged from comments recording the removal plus absence of enumerated catalog URNs in agent entry points; a fuller prompt audit was not done.
- Checked only in part: ADR-0017 "dissolve into a validator" is marked not built because the hand-kept capability file still exists with fixed expected fields; a chart normalizer file was not seen. Whether the validator now duplicates it was not examined.
- Checked only in part: ADR-0018 asserts a log reason for the no-compatible-verbs short-circuit; the grep did not surface that exact line, so the claim rests on the route's return of UNKNOWN with no model call.
- Checked only in part: ADR-0019 Contract B is cited to the supervisor's classify function; the test that confirms the unresolved-subject path never calls classification is the parametrized contract test, whose case list was not read.
- Checked only in part: ADR-0023 read-sync deployment template exists, but no sync-shape or UI consumer code was checked, so only the template's existence is claimed.
- ADR-0015 whether any registration path accepts example questions (only a test text mentions them). ADR-0019 Contract C verification at registration (no code seen; the runnable outcome test only inspects state), the dedup rule (5a) and pair identity. ADR-0021 precedence, mapping table, halt and instance-of stamping live in a different repository or were not located. ADR-0022 whether the deployed wrapper calls the vendor tool server at all (no reference found). ADR-0016 whether any test pins the memory-library setting.

## Sources

All paths are in invincible-agent at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb` (IA) or in dag-tools at `add5bc48021fe6d2c79c24c31072142fc93c8179` (DT). Line ranges are at those commits. Tests are named by function, with their line in the test file. `adm` is IA `agent_fleet/presentation_agent/capability_admission.py`, `ana` is IA `agent_fleet/restate_analyst/main.py`, `bam` is the shared response-contract definitions file (IA), `brk` is IA `helm/invincible-agent/files/domain-broker.py`, `cap` is IA `agent_fleet/presentation_agent/capability_registry.py`, `dhw` is IA `agent_fleet/datahub_wrapper/main.py`, `elc` is the read-sync service's chart template under `helm/invincible-agent/templates/` (IA), `etm` is IA `agent_fleet/restate_analyst/entity_type_mapping.py`, `gwy` is IA `src/iagent/gateway.py`, `ing` is IA `src/iagent/ingest_status.py`, `mem` is the memory helper module under `agent_fleet/utils/` (IA), `mrg` is IA `agent_fleet/utils/mesh_registration.py`, `nex` is IA `agent_fleet/neo4j_expert/service.py`, `ont` is IA `agent_fleet/ontology_service/main.py`, `prj` is IA `src/iagent/projector/apply_loop.py`, `reg` is IA `docs/adr/minted-concepts.md`, `rst` is IA `agent_fleet/mesh_registrar/v2_restate.py`, `sag` is IA `agent_fleet/mesh_registrar/v2_saga.py`, `sql` is IA `sql/create_answer_artifact_projection.sql`, `sup` is IA `src/iagent/defs/dynamic_supervisor.py`, `ttl` is IA `setup/ontologies/mesh_system.ttl`, `wfr` is IA `agent_fleet/restate_analyst/workflow_runner.py`, `wtr` is IA `src/iagent/answer_artifact_writer.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| ADR-0001 Memory model env chain | mem `:532-538` | ADR-0001 | the end-to-end memory round-trip test (:220) | `built` |
| ADR-0002 Two memory-library patches | mem `:64-99`, mem `:119-147` | ADR-0002 | the end-to-end memory round-trip test (:220) | `built` |
| ADR-0003 Per-class model variables | mem `:532-538` | ADR-0003 | no test | Phase 1: `built`. Phases 2 and 3: `accepted` |
| ADR-0004 Predicate-graph path lookup | ont `:4296-4315`, ont `:4320`, ont `:4353-4354`, ont `:4538-4575` | ADR-0004 | test_find_tool_happy_path (:156), test_find_path_happy_two_hop (:240), test_find_path_validates_max_hops_range (:295) | Step C: `built`. Steps A, B, D, E and F: not established |
| ADR-0005 Platform and domain authority | mrg `:633`, mrg `:650`, mrg `:1153`, mrg `:1182` | ADR-0005 | test_mesh_prefix_resolves_to_platform_authority (:252), test_unknown_prefix_resolves_to_domain_authority (:262) | Authority marker: `built`. Propose-time check: not established. Prefix registry file: `not built` |
| ADR-0006 Neo4j routing, saga writes | ont `:4296-4315`, sag `:151-360`, sag `:467-536`, rst `:68-127` | ADR-0006 | test_neo4j_failure_returns_503_with_no_compensation_needed (:139), test_probe_failure_compensates_both_stores_and_returns_503 (:160), test_mesh_resolve_instance_has_one_edge_per_provider (:658), test_substrate_covers_routing_via_v02_saga_edges (:914, live substrate) | Routing and saga: `built`. Source-declaration rule: `built`, live-substrate guard only. Sole-writer claim: `accepted`. Reconcile asset: `not built` |
| ADR-0007 Survey before minting | reg `:18-29` | ADR-0007 | no test | Mint log: `—`. Survey rule: `accepted`. Retroactive mappings: `proposed` |
| ADR-0008 Generalist fallback policy | sup `:70-74`, sup `:1861-1885`, sup `:2765-2893`, sup `:2998`, ana `:1107-1130` | ADR-0008 | test_matched_returns_predicate_dict (:180), test_no_match_returns_none (:228), test_http_503_returns_infra_error (:291), test_fallback_payload_for_no_match (:372), test_fallback_payload_for_low_confidence (:405), test_fallback_emits_telemetry_log_line (:435), test_threshold_default_loaded_from_env (:464) | Fallback core: `built`. Operator doc: not established |
| ADR-0009 Retire classification axes | ont `:4405`, ont `:5452` | ADR-0009 | test_a1_infra_error_records_no_verb (:118) | Search and extraction: `built`. Identity and graph views: not established |
| ADR-0010 OTel at the HTTP boundary | IA `pyproject.toml` `:26-27`; wfr `:683-690` | ADR-0010 | no test | `proposed` |
| ADR-0011 Multi-hop routing deferred | — | ADR-0011 | no test | `proposed` |
| ADR-0012 UI archetype rigidity | bam `:776-781` | ADR-0012 | no test | `—`, superseded by ADR-0017 |
| ADR-0013 capability tools per change | — | ADR-0013 | no test | `proposed` |
| ADR-0014 no URN hints; broker local assets | brk `:72-86`; ana `:906` | ADR-0014 | no test | Rules 1 and 2: `built`; rule 2's deployed copy: not established. Rule 3: `accepted`. Lint and register-time check: `not built` |
| ADR-0015 routing decision audit log | ont `:151-205`, `:4405-4515` | ADR-0015 | no test (the only test touching the emit replaces it) | Phase 1: `built`. Phase 2: `accepted` |
| ADR-0016 infer off on memory writes | ana `:1543-1552`; nex `:1076-1084` | ADR-0016 | no test | Tier 0: `built`. The rest: `accepted` |
| ADR-0017 per-verb presentation registration | ana `:3642-3980`; mrg `:931`; adm `:188`, `:260`; cap `:54`, `:205`, `:265`, `:413` | ADR-0017 | test_an_unknown_archetype_is_refused (:59), test_a_registered_caller_selects_from_ITS_OWN_menu (:47), test_the_registration_version_is_stamped_into_the_decision (:54), test_an_UNREGISTERED_caller_gets_the_default_menu_and_it_is_LABELLED (:63), test_registered_but_unrenderable_is_DISTINCT_from_unregistered (:78) | Registry: `built`. Fallback retirement: `proposed`. Single component home: `not built`. Decision provenance: not established |
| ADR-0018 classify route and compat verbs | ont `:3730`, `:6660`, `:6825-6975`; sup `:387`, `:703` | ADR-0018 | test_routing_decision (:830, live service) | `built` |
| ADR-0019 contracts A and B in routing | ont `:7025-7075`; sup `:703`, `:725-732` | ADR-0019 | test_n1_on_topic_still_dispatches (:350), test_adr0019_contract (:862); contracts C and D: skip-marked placeholders such as test_D_invalid_input_uri_rejected_no_phantom_created (:407) | Contract A: `built`. Contract B: `built` for two routes only. Contracts C and D: `proposed`. Dedup rule, pair identity and reference binding: not established |
| ADR-0020 composition axis in verb walk | — | ADR-0020 | no test | `proposed` |
| ADR-0021 door writes declared content kind | gwy `:13893`, `:13940-13945`; ing `:128`; ttl `:852` | ADR-0021 | test_ingest_declared_content_kind_lands_in_manifest_and_metadata (:308), test_ingest_refuses_event_as_a_file_kind_before_any_write (:144), test_every_file_kind_the_door_accepts_is_a_declared_leaf_of_artifact (:49) | Gateway side: `built`. Driver rules: not established. Failed-stage record and kind-to-format binding: `accepted` as open items |
| ADR-0022 owned wrapper, hand-kept type maps | dhw `:541`; etm `:64` | ADR-0022 | test_engine_a_table_inverts_datahub_to_idp (:115) | Wrapper: `built`. Porting tasks: `proposed` |
| ADR-0023 artifact node, projector, read table | wtr `:488-495`, `:572-590`; prj `:535`; sql `:16`; elc `:32` | ADR-0023 | test_every_property_the_projector_reads_is_one_the_writer_writes (:90) | Write and projection: `built`. Read-sync wiring to the canvas: not established. Standards edges: `not built`. Collaborative edits: `accepted` as deferred |
| ADR-0024 standards reserved; publish rules | — | ADR-0024 | no test | `proposed` |
