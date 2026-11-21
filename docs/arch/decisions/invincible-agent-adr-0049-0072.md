# invincible-agent decisions: ADR-0049 to ADR-0072

invincible-agent records its decisions as numbered ADRs in its own repository, under `docs/adr/`. This record indexes ADR-0049 to ADR-0072 at invincible-agent commit `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. For each ADR it gives what the ADR decides and why, in short, what the ADR's own header states, and what the code at that commit does. It points rather than restates: the ADR is the decision, and where an ADR and any line here disagree, the ADR holds. ADR-0058, ADR-0059, ADR-0066 and ADR-0069 have no file at the pin and no entry. [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). "Stated status" is the ADR's own header, paraphrased; "Status" is this record's reading of the code at the pin. Several headers say less is built than the code shows, or more. Line numbers are at the pin. The record shape is provisional.

An ADR number here is always invincible-agent's own series, cited elsewhere as "invincible-agent ADR-NNNN". A bare section number in a dag-tools record means the dag-tools census.

## ADR-0049 Cross-engine composition goes through the mesh

- **Decision.** When one engine's verb needs another engine's data, it calls the other engine through the mesh, as the person who started the request. Direct reads of another engine's store are refused. Entitlement is the intersection of the grants involved, derivation edges carry a validity date, and an unavailable sibling yields a named refusal, never a partial figure.
- **Why.** A direct read has no gate, no identity and no provenance. A partial number looks like a whole one, so the three states (empty, unavailable, unentitled) must stay distinct.
- **Stated status.** Proposed. Mesh-mediated option chosen; seeded duplication left open as transitional; supervisor composition limited to routing.
- **Where else.** ADR-0060 extends the direct-read refusal from engines to every consumer. ADR-0051 repeats it for the sustainment domain.
- **Status.** The stalest-source stamp, a ruling of the ADR: `proposed`; no code for it at the pin, though several engines call each other. The request-scoped cache and the registration-time dependency declaration are open questions of the ADR, the second leaning yes, so their absence is not a shortfall against a ruling; both are in the not-established list.

## ADR-0050 Canvas templates are ratified YAML; a panel is a pre-resolved step

- **Decision.** A canvas template is a ratified YAML file. Each panel is a ready-made step (verb, slots, layout) sent through the second-stage gate under the initiator's grants, never through the classifier. One seed verb takes a template id. A panel that cannot be invoked draws as a named hole. A template reference is its id plus a short hash of canonical content, and the schema is generated from the models.
- **Why.** Free-text seeding cannot express a fixed panel set. Hashing content rather than bytes keeps the reference stable across formatting edits.
- **Stated status.** Proposed. Later falsification blocks were renamed as an acceptance section; the decision did not change.
- **Status.** `built` for the loader, reference, seed verb, schema and drift seals. The shared-slot carry: `built`, at cvt `:278-298`, gwy `:5182-5229` and gwy `:5370-5396`. The one-ask-for-many-panels seal was not located and is in the not-established list.

## ADR-0051 Sustainment safety: the mesh drafts a risk and never accepts it

- **Decision.** A governed-reading engine works over sustainment data. The risk matrix and authority ladder are data in an ontology file. No verb writes an acceptance; acceptance is a human-task outcome. Higher-severity acceptances need a prior peer concurrence, and accepted or rejected is terminal. An acceptance-setting verb, a runner verb, direct store reads, an open payload slot and a defaulted verdict are refused. The trend verb is deferred.
- **Why.** A system that drafts and also accepts a risk removes the human accountability the process exists to hold.
- **Stated status.** Proposed; later sections closed, and a caveat on the ladder lifted after the source instruction was read.
- **Status.** `built` for the engine, matrix loader, draft verb and acceptance workflow. Verb IRIs use the `mesh:` prefix, and the engine also registers instance-resolution verbs, which differs from what the ADR text says. Two further verbs named in the ADR were not located.

## ADR-0052 Pulled source data enters through DataHub, never an engine

- **Decision.** Each external source gets one pull asset feeding a DataHub dataset with a published schema. Engines read only by dataset URN, as the initiator. Pulls are for extraction, not transactional writes, and a dataset is not state. A fixture path wears the same interface as the real path and contains the failures the consumer must catch.
- **Why.** A single connector shape keeps reads gated and identifiable. A fixture that cannot fail proves nothing.
- **Stated status.** Accepted for a first slice. The ADR says acceptance is not conformance and tabulates its six rules: the ticketed-read rule is built but not enforced, the no-pull rule is met trivially, the rest are not met or only in part.
- **Where else.** ADR-0060, on reading by URN.
- **Status.** Mixed. Read by URN under the initiator: `built`. Published schema, pull asset as a dependency, and the remaining rules: `accepted`.

## ADR-0053 Measures are pure versioned modules selected by data rows

- **Decision.** A measure is a pure, versioned module and a verb is a thin wrapper. A row in policy data maps a method to its module and carries a transcription seal against its cited source. Selecting among ratified methods is allowed; a formula-as-slot verb is refused. A row pointing at an unmanifested module is refused. Money is `Decimal`, quantised only after derivation. The artifact records method name and version beside each figure.
- **Why.** Method choice becomes a reviewable row instead of code, and a figure can be traced to the exact formula version.
- **Stated status.** Accepted and ratified as a numbered rule.
- **Status.** Mixed. Rows, loader, modules and row seals: `built`. Runtime selection from the rows: `accepted`, because the type literal and constant tuple are still what the runtime reads. The single export envelope: `proposed`; two envelopes remain, at IA `agent_fleet/cost_agent/export.py:57` and IA `agent_fleet/finance_agent/export.py:28`. Name and version on row-declared measures: `built` (fmn `:1024-1025`). Name and version in every artifact: `proposed`; built-in measures carry "builtin" (fmn `:1517-1525`). Sha-pinned external modules: `proposed`; IA `agent_fleet/finance_agent/measure_verbs.py:654-734` computes a sha, but no row carries an expected one.

## ADR-0054 Data classes: every write declares what it is

- **Decision.** A store's class is declared once, with four fields: reproducibility (rebuildable or stateful), authority, sync obligation and releasability (reserved). Named shapes are shorthand only. Three enforcement points: the prime's clearing step reads the class, a census refuses an unclassified store, and write interfaces carry the class. Rollout labels first, enforces second.
- **Why.** The prime currently decides what to clear from manifest domains, so a stateful store can be cleared by accident.
- **Stated status.** Accepted, with two amendments in the following days (four answers, then five from the write census).
- **Status.** Mixed. Class declaration and enforcement: `accepted`, no code at the pin. The edge-type partition seal between registrar and trace writer: `built`.

## ADR-0055 Archetypes are packages and an absence is a declared fact

- **Decision.** A front-end archetype is a package: a contract, a card, fixtures that each flip a declared absence, and a backend row. The one mandatory mechanism is the declared absence (or an explicit no-absence value), readable by a test. A row replaces the hand-kept tuple table, and a seal asserts the table is gone. The library covers payload-bound cards only. Cross-repository seals need a pinned producer, a no-skip floor and an asserted sha.
- **Why.** Several hand-maintained places had to change per archetype, and a card could draw over a gap without saying so.
- **Stated status.** Accepted after review, covering extraction and registration. Later amendments add per-subject flip rules and the no-absence value.
- **Status.** `accepted`. The backend row directory does not exist at the pin and the tuple table is still present. The package side is in another repository and was not read.

## ADR-0056 Failure reporting and corrective action, phase one

- **Decision.** One read-only verb answers what failed on a part across programs, reusing an existing safety-critical-item class and adding record and record-set classes. An unlisted part is refused by name; a listed part with no failures gets an explicit empty list. Every record cites its source and carries no narrative. An amendment moved reads to a many-record connector query, filters per record by program membership, refuses non-person callers, and adds a monthly trend verb.
- **Why.** Answering "no failures" for a part the engine cannot identify looks the same as a clean part.
- **Stated status.** Proposed when written; phase two (live connector) out of scope. An October amendment records answers to both open questions.
- **Status.** `built` for both verbs, source gathering and the person and program filter.

## ADR-0057 The domain model is edited by conversation and changed by review

- **Decision.** Model editing happens in a conversation backed by three domain-agnostic verbs (explain, propose, diff). A proposal answers eight questions (identity, system of record, owner, consumers, cardinality, history, lifecycle, reuse). It renders as a model canvas. A conversation never changes the model: approval yields a reviewed change plus migration note, applied by the roll, and the proposer cannot ratify.
- **Why.** Existing modelling tools do not ask the ownership and entitlement questions the platform depends on.
- **Stated status.** Proposed, approved with three additions and merged. It becomes Accepted when the first verb lands; nothing built.
- **Status.** `proposed`. No verb or route for model editing was found.

## ADR-0060 Consumers read through the mesh, never the stores

- **Decision.** Any consumer outside the owning engine reads through three mesh surfaces: a document manifest with blob references, entity verbs generated from a persistence descriptor, and origin-filtered search. Each applies grants, then the origin rule, to every row.
- **Why.** A consumer holding store credentials sits outside the gate; the interim credential is a standing exception.
- **Stated status.** Proposed and merged as proposed, not ruled. Only inert SDK types were allowed to be built.
- **Where else.** ADR-0049, ADR-0052, ADR-0054.
- **Status.** `proposed`. No matching routes or descriptor type found in this repository at the pin; the SDK is elsewhere.

## ADR-0061 A workflow described in dialogue and compiled to overlay rows

- **Decision.** A person describes a workflow in conversation and the system compiles it to a bundle of overlay rows, the only output format. Each turn restates the draft and asks one question about the most important gap. A deferral is a recorded absence with a reason, some gaps can never be deferred, and the model only proposes patches while code validates and chooses questions.
- **Why.** Fixed forms miss what a domain expert knows, and a free-form model that decides for itself cannot be checked. Keeping the decisions in deterministic code keeps every question and every gap explainable.
- **Stated status.** Proposed. The bar for "the loop works" is ruled met on one run, but the header says no later instrument version has a run that meets it. The prototype and synthetic fixtures are built and measured; no route, verb or canvas exists.
- **Where else.** ADR-0062 section 4 routes the resulting proposals through the repository.
- **Status.** The prototype loop (sections 3 to 5 and 7: gap asking, restating, non-deferrable gaps, model as patch extractor): `built`. The proposal verb, canvas and ratification route (section 6): `proposed`.

## ADR-0062 The mesh holds its repositories; forges are mirrors

- **Decision.** A mesh service holds the repositories of record and is the only writer of refs, one writer per repository. It offers propose, review, merge-by-sha with a two-parent merge, tag and audit, with pre-merge checks for secrets, identifying terms, trailers and governed paths. Merges are signed and audited in git notes, and outside forges receive push mirrors only.
- **Why.** A forge that owns the repository puts the history and its approvals outside the mesh's control. Holding the record in the mesh lets approval, audit and identity rules be enforced in one place.
- **Stated status.** Proposed; the header says nothing is built. All seven sections are ruled and merged as Proposed.
- **Where else.** ADR-0061 for the workflow proposals in section 4.
- **Status.** Core library (propose, approvals, merge checks, tag, audit): `built`. The ref update is a check-then-set inside the library. The single-writer Restate wrapper, push mirrors, inbound forge proposals and the section 5 review audience wiring: `proposed`.

## ADR-0063 Three event-branch field kinds taken from OpenDDIL

- **Decision.** Register three event-branch content kinds: a failure report, a safety finding and a cyber indicator. Each gets a seed row and its own seeded workflow, with hyphenated ids and no arrival or review shape set.
- **Why.** The three kinds are named by OpenDDIL and the mesh needs rows to receive them. Seeding them keeps the rows generic and the workflows testable.
- **Stated status.** Proposed; design only, re-measured on a stated date, with four questions open for the architect.
- **Status.** `proposed`. No registry seed row and no kind id is present at the pin: the content-kinds directory holds only a README. One of the three kinds is further along than the ADR's header suggests: the safety finding review has an authored workflow, an overlay task kind, a task grant and five tests, each naming ADR-0063 section 2. The workflow's own header marks it authored and not yet triggered.

## ADR-0064 Classification propagates through derivation

- **Decision.** Labels carry through derived data: a source is labelled by people, and each derived column is labelled by composing the labels of what it read. Composition only tightens, and any loosening is a declared, owned and recorded downgrade. Labels are a set of dimensions, one of them ordered, read from a document the deployment mounts.
- **Why.** A derived table that is less restricted than its inputs leaks data without anyone deciding to. The label of a derived column should follow from its inputs, not from a later guess.
- **Stated status.** Proposed; design only, extending ADR-0052 and amended to align with an OpenDDIL ADR. The six build steps are carried by dag-tools releases.
- **Where else.** [dag-tools-s4r-s4l-s4m.md](dag-tools-s4r-s4l-s4m.md), [dag-tools-s4n-s4o-s4k1.md](dag-tools-s4n-s4o-s4k1.md), [classification-propagation](../components/classification-propagation.md), [warehouse-source](../components/warehouse-source.md).
- **Status.** The ADR's six steps, read from code. Steps 1 to 4, auditing the reads, attributing every read, classifying the sources and declaring source assets with propagation: `built` in dag-tools, at `dag_tools/audit/cli.py:152-223`, `dag_tools/resources/warehouse.py:300-463`, dim `:83-655`, `dag_tools/warehouse_source/spec.py:120-200`, prp `:190-305`, prp `:406-521` and cpc `:395-415`. Steps 5 and 6, enforcement at the broker and the data plane inheriting it: `proposed`. The gateway narrows only by the columns the policy engine returns (`dag_tools/central_gateway/main.py:360-363`) and reads no label, and the level-readers list at IA `policy/level_readers.yaml:20` is empty.

## ADR-0065 Obsolescence is a process, and change notices are its intake

- **Decision.** Obsolescence is treated as a six-step process in which a supplier notice is one intake among several. Intake stays as an earlier ADR built it, plus one write of a part's lifecycle status. A where-used verb answers one row per end item and program, from a declared configuration, with absent values stated as absent rather than zero.
- **Why.** Notices are one signal of a longer process, and the same impact question follows from any intake. Stating the configuration and absence in each row stops a design view being read as installed truth.
- **Stated status.** Accepted with one amendment; the header says design only, with no code and no roll.
- **Where else.** ADR-0064 for how labels ride on served tables.
- **Status.** Step 2, the where-used verb over a repository dataset: `built`. The lifecycle write, resolution workflow, later steps and data contracts: `accepted`.

## ADR-0067 One identity per thing, decided in OpenDDIL

- **Decision.** This record only points at the OpenDDIL decision that each thing has one identity, and lists what this repository does to follow it. The work is alias and binding rows written through the graph mapping and a realigned case episode id.
- **Why.** The decision belongs to the standard that owns the identifiers, so it is recorded once there.
- **Stated status.** Reference; accepted on its date, decided elsewhere, with nothing built yet.
- **Status.** `—` (reference). The follow-on work: `proposed`. No alias or binding rows go through graph mapping at the pin (the alias-shaped rows are read by wuv `:180-193`), and the case episode id is still a fixed prefix plus a sha256 (IA `agent_fleet/restate_analyst/case_routing.py:244-249`).

## ADR-0068 An edge tier with profiles and a case journal

- **Decision.** Tier profiles are configuration rows, and a case journal records entries. Each step has one owning tier, a step that crosses tiers crosses as a journal entry and never as a call, and a wait that loses its link keeps waiting.
- **Why.** An edge tier must keep working when disconnected, and one owner per step avoids two tiers acting on one case.
- **Stated status.** Proposed, not ratified, amended twice; nothing is built.
- **Status.** `proposed`. No tier profile file or journal exists at the pin.

## ADR-0070 Built-unit traceability as a read-only engine

- **Decision.** A read-only engine over built-unit data answers whether a unit may ship, how far a defect reaches, and how healthy a process is, following the pattern used for finance data. Its inputs are a test-result source and a curated slice built in dag-tools.
- **Why.** The answers are derived reads of data that already exists, so they need no write path and can reuse the governed-read pattern.
- **Stated status.** Proposed; five questions were ruled, but the record is not ratified. Design only for the engine, with the dag-tools spec and contract template stated to exist at a tag.
- **Where else.** [dag-tools-s4q.md](dag-tools-s4q.md), [dag-tools-s4r-s4l-s4m.md](dag-tools-s4r-s4l-s4m.md), [systemlink-source](../components/systemlink-source.md).
- **Status.** Data side (test-result source kind, curated measurement rules): `built` in dag-tools. The read-only engine and its verbs in this repository: `proposed`.

## ADR-0071 A directory client is built once per process

- **Decision.** The directory client is built once per process, lazily and under a lock, and shared by readers. Its transport follows the URL scheme, a batch job builds one client per run, and a seal keeps the rule.
- **Why.** Building a client for every call spent most of a request's CPU on setup. One pooled client removes that cost.
- **Stated status.** Proposed; design plus a census. Steps 1 and 2 are built by another lane in their own changes.
- **Status.** The shared, lazily built, lock-guarded client keyed per process, with a plain context for plain-text URLs: `built`. Batching: `built`, at IA `src/iagent/human_tasks.py:428-470`. The batch job: `built`, at IA `policy/sync/topaz_sync.py:391-404` and IA `policy/sync/grant_sync.py:239`. The seal's static check: `proposed`.

## ADR-0072 A live answer is one durable workflow

- **Decision.** Only live answers move off Dagster. Each is a Restate workflow keyed by an answer id, whose main handler runs the five supervisor steps unchanged. A flag chooses the path and defaults to the current one.
- **Why.** A Dagster run adds launch latency to a short interactive answer. A workflow gives durability without that cost.
- **Stated status.** Proposed; the prototype is a change behind a flag defaulting to the existing path. Two sections were ruled on a stated date.
- **Status.** `built` behind the flag: the workflow, the poll read, the client and the path switch.

## Not established

- ADR-0049: the request-scoped cache and the registration-time dependency declaration are open questions of the ADR and were not seen. Calls between engines exist in several engine files, but no test was read.
- ADR-0050: the one-ask-for-many-panels seal was not located. The named-hole test file was seen by filename only.
- ADR-0051: the ADR names verbs under a different prefix and says this engine does not register instance resolution; the code differs. Two further verbs named in the ADR were not found. The task kind used for concurrence is likely but unverified. Seals for the registry call points and for ladder routing were seen by filename only.
- ADR-0052: three of six rules are stated by the ADR as not met; their status here follows that table, not independent checks. Open items (asset location, drift disposition, fixture URN registration) are unresolved.
- ADR-0053: the runtime still reads the type literal.
- ADR-0054: whether the census exists in this repository is not established; the ADR says it was on another branch. The prime still derives its drop set from manifest domains.
- ADR-0055: front-end packages and cross-repo seals live elsewhere and were not read. Presentation code cites the ADR, but the tuple table remains.
- ADR-0056: the ADR header still reads Proposed; the code from the amendment is built.
- ADR-0060: SDK types (v0.9.12) are in another repository; not checked.
- ADR-0061: sections 1, 2 and 6 (bundle format, facets, proposal handoff) were not checked against code beyond the loop functions. A grep found no proposal verb at the pin.
- ADR-0062: the header says nothing is built, but the core library and its tests exist at the pin. The ADR text lags the code. The serializing wrapper and the review-audience wiring were not found in the library. The ref update is check-then-set.
- ADR-0063: the absence of seed rows rests on the content-kinds directory listing and a search for the three kind ids. Whether the other two kinds have workflows authored under names other than their kind ids was not checked.
- ADR-0064: the header says design only, but the dag-tools code exists. Lines cited for dimensions, propagation and the component are coarse ranges, not per-claim lines.
- ADR-0065: the header says no code, but the where-used verb exists at the pin. Whether the lifecycle-status write exists was not checked.
- ADR-0070: the dag-tools line ranges are whole-file. No read-only engine or verb was found in the invincible-agent tree by a keyword grep, which is weak evidence of absence. The ADR body was not re-read beyond its header and decision summary.
- ADR-0071: the listed tests pin the shared client and batching, not the full rule.
- ADR-0072: the ruled sections 3 and 8.1 were not compared to code. The five-step run is delegated to a function outside the cited lines. Behaviour of the live view with more than one replica is not established.

## Sources

All paths are in invincible-agent at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb` (IA) or in dag-tools at `add5bc48021fe6d2c79c24c31072142fc93c8179` (DT). Line ranges are at those commits. Tests are named by function, with their line in the test file. `cpc` is DT `dag_tools/components/classification_propagation/component.py`, `cvt` is IA `src/iagent/canvas_template.py`, `dim` is DT `dag_tools/warehouse_source/dimensions.py`, `dra` is IA `src/iagent/durable_answers.py`, `fai` is IA `agent_fleet/finance_agent/asset_inputs.py`, `fef` is IA `agent_fleet/finance_agent/measure_modules/eac_formulas.py`, `fmn` is IA `agent_fleet/finance_agent/main.py`, `fmr` is IA `agent_fleet/finance_agent/method_registry.py`, `fsr` is IA `agent_fleet/safety_agent/failure_source.py`, `gwy` is IA `src/iagent/gateway.py`, `obs` is DT `dag_tools/warehouse_source/observe.py`, `prp` is DT `dag_tools/warehouse_source/propagate.py`, `rcr` is IA `agent_fleet/repository_agent/repo_core.py`, `saf` is IA `agent_fleet/safety_agent/main.py`, `saw` is IA `agent_fleet/restate_analyst/safety_acceptance_workflow.py`, `sfo` is IA `policy/overlays/sample/task_kinds/safety_finding_review.yaml`, `sfw` is IA `policy/workflows/safety_finding_review.yaml`, `slk` is DT `dag_tools/asset_wrappers/sources/systemlink.py`, `slm` is DT `dag_tools/systemlink/measurements.py`, `sls` is DT `dag_tools/systemlink/spec.py`, `sms` is IA `agent_fleet/safety_agent/measures.py`, `smx` is IA `agent_fleet/safety_agent/matrix.py`, `soc` is IA `agent_fleet/restate_analyst/socratic_elicitation.py`, `sup` is IA `src/iagent/supervisor_service.py`, `tgr` is IA `policy/task_grants.yaml`, `tpz` is IA `src/iagent/authz/topaz_client.py`, `wuv` is IA `agent_fleet/ontology_service/where_used.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| ADR-0050 ratified template panels | cvt `:75`, cvt `:157-189`, cvt `:229-299`, cvt `:366`, gwy `:791-804`, gwy `:5182-5229`, gwy `:5370-5396` | ADR-0050 | test_schema_matches_the_models (:90), test_the_check_can_actually_fail (:98), test_every_ratified_template_validates (:122), test_template_ref_is_content_not_bytes (:161), test_seedCanvas_is_registered_with_template_id_as_a_SLOT (:108), test_an_unknown_id_RAISES_rather_than_defaulting (:67) | `built` |
| ADR-0051 draft risk, never accept | saf `:145-199`, sms `:61-101`, sms `:217`, sms `:315`, smx `:100-163`, saw `:149` | ADR-0051 | test_no_measure_function_writes_an_acceptance (:50), test_changing_one_row_in_the_ttl_changes_the_drafted_level_with_no_code_edit (:43), test_an_unrecognised_pair_refuses_loudly_and_names_the_file (:73), test_a_concurrence_level_is_not_routed_straight_to_an_acceptance (:118) | `built` |
| ADR-0052 read by URN, initiator: read rule | fmn `:2317-2356`, fai `:55`, fai `:227` | ADR-0052 | test_an_identity_that_is_not_a_person_is_a_401_and_no_client_is_built (:400), test_not_entitled_is_a_403_that_names_the_grant_and_carries_no_figure (:418), test_each_contract_rule_is_its_own_502_and_names_no_money (:504) | `built` |
| ADR-0052 read by URN, initiator: the rest | fmn `:2317-2356`, fai `:55`, fai `:227` | ADR-0052 | test_an_identity_that_is_not_a_person_is_a_401_and_no_client_is_built (:400), test_not_entitled_is_a_403_that_names_the_grant_and_carries_no_figure (:418), test_each_contract_rule_is_its_own_502_and_names_no_money (:504) | `accepted` |
| ADR-0053 measure rows and modules: rows | fmr `:75-175`, fef `:33` | ADR-0053 | test_the_module_COMPUTES_what_its_row_TRANSCRIBES (:105), test_a_row_pointing_at_an_UNMANIFESTED_module_is_refused (:260), test_a_row_whose_VERSION_disagrees_with_its_module_is_refused (:272) | `built` |
| ADR-0053 measure rows and modules: runtime selection | fmr `:75-175`, fef `:33` | ADR-0053 | test_the_module_COMPUTES_what_its_row_TRANSCRIBES (:105), test_a_row_pointing_at_an_UNMANIFESTED_module_is_refused (:260), test_a_row_whose_VERSION_disagrees_with_its_module_is_refused (:272) | `accepted` |
| ADR-0053 measure rows and modules: export envelope | IA `agent_fleet/cost_agent/export.py:57`, IA `agent_fleet/finance_agent/export.py:28` | ADR-0053 | no test | `proposed` |
| ADR-0053 measure rows and modules: name and version on row-declared measures | fmn `:1024-1025` | ADR-0053 | no test | `built` |
| ADR-0053 measure rows and modules: name and version in every artifact | fmn `:1517-1525` | ADR-0053 | no test | `proposed` |
| ADR-0053 measure rows and modules: sha-pinned external modules | IA `agent_fleet/finance_agent/measure_verbs.py:654-734` | ADR-0053 | no test | `proposed` |
| ADR-0054 store class declaration | — | ADR-0054 | no test | `accepted` |
| ADR-0054 edge-type partition seal | the seal is the test | ADR-0054 | test_EVERY_TYPE_A_DOOR_WRITES_IS_DECLARED (:194), test_EVERY_TYPE_THE_SDK_DECLARES_IS_ACTUALLY_WRITTEN (:205) | `built` |
| ADR-0055 archetype package and row | — | ADR-0055 | tests/test_cortex_archetype_packages_are_seen.py: test_every_archetype_package_is_seen_by_BOTH_registries (:60), test_the_archetype_package_population_is_inhabited (:49). Package half only, and the module skips when the sibling UI checkout is absent. The row half has no test | `accepted` |
| ADR-0056 failure lookup and trend | saf `:225`, saf `:278`, sms `:550`, sms `:623`, sms `:792`, fsr `:135` | ADR-0056 | test_an_unknown_part_number_refuses_rather_than_reporting_clean (:96), test_the_module_has_no_write_path_for_this_verb (:126), test_a_caller_with_no_person_is_refused_not_answered_empty (:57), test_a_record_whose_platform_has_no_mapping_row_is_never_returned (:102) | `built` |
| ADR-0057 model edit by review | — | ADR-0057 | no test | `proposed` |
| ADR-0060 consumers read via mesh | — | ADR-0060 | no test | `proposed` |
| ADR-0061 elicitation loop asks only gaps: loop | soc `:1413-1430`, soc `:490-544`, soc `:934`, soc `:1147`, soc `:1498`, soc `:1576`, soc `:1814`, soc `:2017` | ADR-0061 | test_t1_an_unanswerable_refusal_is_left_open_never_deferred (:399), test_t3_who_is_barred_is_left_open_never_deferred (:417), test_t5_status_is_runnable_only_when_nothing_is_left_open_or_refused (:429), test_t7_every_fixture_is_runnable_under_the_oracle_and_the_aligner (:459), test_overwrite_control_a_different_value_later_is_a_pending_contradiction_and_an_equal_one_is_not (:229) | `built` |
| ADR-0061 elicitation loop asks only gaps: verb and route | soc `:1413-1430`, soc `:490-544`, soc `:934`, soc `:1147`, soc `:1498`, soc `:1576`, soc `:1814`, soc `:2017` | ADR-0061 | test_t1_an_unanswerable_refusal_is_left_open_never_deferred (:399), test_t3_who_is_barred_is_left_open_never_deferred (:417), test_t5_status_is_runnable_only_when_nothing_is_left_open_or_refused (:429), test_t7_every_fixture_is_runnable_under_the_oracle_and_the_aligner (:459), test_overwrite_control_a_different_value_later_is_a_pending_contradiction_and_an_equal_one_is_not (:229) | `proposed` |
| ADR-0062 core repository library with audited merge: core | rcr `:284`, rcr `:346-366`, rcr `:368-506`, rcr `:584-617`, rcr `:617-645` | ADR-0062 | test_a_happy_merge_is_a_two_parent_signed_audited_commit (:82), test_the_proposer_cannot_approve (:129), test_a_secret_in_the_diff_is_refused_and_not_echoed (:180), test_a_commit_without_the_trailers_is_refused (:238), test_a_release_tag_never_moves_except_by_a_logged_repair (:305), test_a_merge_with_its_note_removed_is_found (:367), test_the_package_never_shells_out (:400) | `built` |
| ADR-0062 core repository library with audited merge: wrapper and mirrors | rcr `:284`, rcr `:346-366`, rcr `:368-506`, rcr `:584-617`, rcr `:617-645` | ADR-0062 | test_a_happy_merge_is_a_two_parent_signed_audited_commit (:82), test_the_proposer_cannot_approve (:129), test_a_secret_in_the_diff_is_refused_and_not_echoed (:180), test_a_commit_without_the_trailers_is_refused (:238), test_a_release_tag_never_moves_except_by_a_logged_repair (:305), test_a_merge_with_its_note_removed_is_found (:367), test_the_package_never_shells_out (:400) | `proposed` |
| ADR-0063 three event kinds seeded | sfw `:3-5`, sfo `:3`, sfo `:18`, tgr `:326` | ADR-0063 | tests/safety/test_the_safety_finding_review_is_declared.py: test_the_workflow_loads_with_one_step_and_the_ruled_stages (:91), test_the_step_names_the_audience_and_the_overlay_kind (:98), test_the_overlay_row_carries_the_three_outcomes_and_no_accepted (:104), test_dismissed_requires_a_reason (:112), test_the_grant_row_is_keyed_by_the_audience_and_names_a_populated_group (:118); one kind of three, and no seed row | `proposed` |
| ADR-0064 labels compose through derivation: steps 1 to 4 | dim `:83-378`, prp `:190-305`, obs `:1-256`, cpc `:233-308` | ADR-0064 | test_composition_is_the_join_of_one_order_in_every_type (:172), test_a_permissive_set_composes_by_INTERSECTION_not_union (:112), test_a_dagster_platform_urn_fails_the_load (:359), test_the_shipped_example_propagates_from_a_source_it_only_OBSERVED (:1085), test_the_code_location_loads_with_no_document_mounted (:1172) | `built` |
| ADR-0064 labels compose through derivation: steps 5 and 6 | `dag_tools/central_gateway/main.py:360-363`, IA `policy/level_readers.yaml:20` | ADR-0064 | no test | `proposed` |
| ADR-0065 where-used verb with stated configuration: verb | wuv `:57`, wuv `:240`, wuv `:393` | ADR-0065 | test_AS_BUILT_AND_AS_MAINTAINED_ARE_REFUSED_WITH_NO_ROWS (:76), test_CONTROL_AS_DESIGNED_BY_THE_SAME_PATH_GIVES_ROWS (:85) | `built` |
| ADR-0065 where-used verb with stated configuration: the rest | wuv `:57`, wuv `:240`, wuv `:393` | ADR-0065 | test_AS_BUILT_AND_AS_MAINTAINED_ARE_REFUSED_WITH_NO_ROWS (:76), test_CONTROL_AS_DESIGNED_BY_THE_SAME_PATH_GIVES_ROWS (:85) | `accepted` |
| ADR-0068 edge tier profiles and journal | — | ADR-0068 | no test | `proposed` |
| ADR-0070 test-result source and curated slice: data side | slk `:1-716`, sls `:1-183`, slm `:1-370` | ADR-0070 | test_the_schedule_targets_incremental_and_no_name_job_exists (:114), test_the_filters_block_is_required_because_terminal_statuses_has_no_default (:494) | `built` |
| ADR-0070 test-result source and curated slice: engine | slk `:1-716`, sls `:1-183`, slm `:1-370` | ADR-0070 | test_the_schedule_targets_incremental_and_no_name_job_exists (:114), test_the_filters_block_is_required_because_terminal_statuses_has_no_default (:494) | `proposed` |
| ADR-0071 one shared directory client per process | tpz `:166-202`, tpz `:220-226` | ADR-0071 | test_a_reader_builds_no_client_after_its_first_call (:105), test_a_whole_entitlements_request_builds_no_client_after_warm_up (:118), test_CONTROL_a_client_built_per_call_is_counted (:167), test_an_http_directory_loads_no_trust_store_and_fails_tls_closed (:181), test_a_forked_child_builds_its_own_client (:205) | `built` |
| ADR-0071 one shared directory client per process: batching and batch job | IA `src/iagent/human_tasks.py:428-470`, IA `policy/sync/topaz_sync.py:391-404`, IA `policy/sync/grant_sync.py:239` | ADR-0071 | test_b1_a_request_is_one_list_and_one_batched_check (:105) | `built` |
| ADR-0071 one shared directory client per process: the seal's static check | — | ADR-0071 | no test | `proposed` |
| ADR-0072 live answer as one Restate workflow | sup `:29-58`, sup `:96-100`, dra `:19-30`, gwy `:9682-9702` | ADR-0072 | test_run_impl_journals_each_step_once_and_persists (:42), test_snapshot_serves_live_mid_run_then_durable_and_queued (:53), test_answer_path_flag (:101), test_launch_posts_and_returns_the_id_or_none (:38) | `built` |

### Rows that carry no status

These rows count or point at records, or name a decision that is superseded or decides nothing here. None is a claim about the mesh, so none carries a status.

| Claim | Source | Decision | Evidence |
|---|---|---|---|
| ADR-0067 one identity decided elsewhere | — | ADR-0067 | no test |
