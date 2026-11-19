# Agent capabilities through the SDK (FL-AGENT)

This flow follows an agent's read of the graph and vector stores, and asks where telemetry is recorded. It asks who reaches the store, on whose credential, what decides whether the person may see the answer, and what the answer carries back. Two reference elements frame it:

- the analytical data plane, where data is turned into insight (NATO Appendix 14, ¶10; [NATO record, row L4](../alignment/nato-dcra.md));
- mesh instrumentation: metrics and traces about how the mesh runs ([DoD record, row S15](../alignment/dod-dmra.md)).

Dataset reads through the gateway are in [need to know](need-to-know.md) and [big data](big-data.md) and are not repeated here.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional. The SDK lines were supplied by the SDK's session and not read here.

## Findings

1. **The SDK's remote graph client has no server end.** The SDK's remote graph client posts to a mesh graph route, and its own docstring says no gateway handler exists; it is tested against a stub only (supplied). The SDK's remote vector and ontology clients are deliberately not built (supplied). A search of both repositories at their pins finds no route under either path. The server end is `not built`.
2. **Engines implement the SDK's read interfaces in-process.** Engine-o holds `Neo4jGraph`, which implements the SDK's graph interface over Neo4j. `WeaviateVectors` implements the SDK's vector interface over Weaviate. It lives in the fleet's shared utilities because two engines read through it: engine-o and the Weaviate engine. Neither module imports a driver. The driver is injected by the engine, so the store connection lives in the engine, not the SDK. `built`
3. **The store sees the engine, not the person.** Every read in this flow uses the engine's own credential:
   - engine-o opens Neo4j with a credential from its posture settings;
   - the Neo4j engine opens its own driver with a credential from its environment;
   - the shared Weaviate client is constructed with no credential argument at all.

   No credential is minted for a read. `built`
4. **Not every engine reads through the SDK's interfaces.** The Neo4j engine runs its graph queries through its own driver and does not use `Neo4jGraph`. The registrar holds a separate Neo4j driver, because it writes. The documents engine holds no driver; it asks engine-o over HTTP. `built`
5. **On the SDK paths a person is required, and the guard is an allowlist.** Every operation of `Neo4jGraph` and `WeaviateVectors` first calls the SDK's person guard. The guard admits a person and refuses a service or a delegate. Engine-o's verb route refuses a request that names no person, with a 400, when its SDK path is on, which it is by default. The `on_behalf_of` field cannot change the guard's decision. In the SDK the person is provenance: the SDK itself makes no access decision (supplied). `built`
6. **The access decision is in the engines, per object, and off by default.**
   - The Weaviate and Neo4j engines ask the authorizer whether the person may read each source document (`can_read = reader | owner`). They deny by default and fail closed.
   - Engine-o asks whether the person may view each ontology class before it classifies.
   - All three checks run only when `ENABLE_AGENTIC_AUTH` is on. The code default is off, and so is the chart default. With it off, every result is kept.
   - The chart says the flag turns on three gates at once, and that turning it on is the last step of invincible-agent ADR-0025, whose preconditions include work not yet built. ADR-0025 and its flip plan were not read.

   The checks are `built`; by default no access decision is made on these reads. Compare [need to know](need-to-know.md), where the gateway decides per dataset.
7. **These reads carry no ticket, receipt or version.** A search of the graph reader, the vector reader and both expert engines finds no ticket, receipt or `as_of`. The gateway and broker are not on this path. So an answer built from a graph or vector read cannot cite what it read the way an answer from a dataset read can cite the receipt's ticket and as-of ([big data, finding 8](big-data.md)). `not built`
8. **The flags that route reads through the SDK's interfaces default on.** There are three:
   - engine-o's verb route;
   - engine-o's class-candidate pool on its resolve route;
   - the Weaviate engine's knowledge search, where unset means on.

   Tests pin each default. The comment above the class-candidate flag still says it is off by default; the code and its test say on. `built`
9. **Telemetry is not an SDK capability.** The SDK has no telemetry interface, and its design puts tracing at the platform layer (supplied). Seven fleet engines and the fleet root pin `provenance-telemetry==0.2.2`. The documents engine's packaging says that library brings an exporter in at the binding only, and a test holds its core free of it. So telemetry here is ambient: an engine is instrumented; it does not call a capability. What the library emits, where it lands, and whether it carries the person or the read is pending the perf lane's answers. The pin is `built`; the rest is not established.
10. **Conformance.** The fleet runs the SDK's offline check over both readers. The checks show that each satisfies the interface, refuses a service identity on every operation, and reports a missing driver as unreachable, not empty. The vector reader also refuses a collection whose embedding marker does not match. The offline check is contract conformance, not read conformance: it proves properties that need no store, and what the reader's operations touch is the reader's choice (supplied). In both fleet tests the reader runs over a stub, a stub driver for the graph and a stub client for the vectors, so no check reads a live graph or vector store. `built` (contract conformance); read conformance against a live store is `not built`.

## The flow, step by step

| # | Step | Component | Status | Evidence |
|---|---|---|---|---|
| 1 | An agent or route needs a graph or vector answer for a person. | an engine route | `built` | invincible-agent `agent_fleet/ontology_service/main.py:6584-6593`, `:6605-6607` |
| 2 | The engine builds an SDK `Initiator` for the person and refuses a request that names none. | engine | `built` | `main.py:6613-6622` |
| 3 | The engine calls its in-process reader: `Neo4jGraph` or `WeaviateVectors`. | engine; reader | `built` | `main.py:6633`; `agent_fleet/utils/mesh_vectors.py:215-238` |
| 4 | The reader calls the SDK's person guard. | reader; SDK | `built` | `agent_fleet/ontology_service/mesh_graph.py:188-200`; `mesh_vectors.py:119-131` |
| 5 | The reader queries the store through the engine's driver, on the engine's credential. | reader; store | `built` | `main.py:466-468`, `:886`; `agent_fleet/utils/weaviate_utils.py:33-40` |
| 6 | The reader returns a result marked answered, empty, failed or unreachable. A vector search that lost its embedding says so. | reader | `built` | `mesh_graph.py:1-25`; `mesh_vectors.py:1-27` |
| 7 | When `ENABLE_AGENTIC_AUTH` is on, the engine asks the authorizer per document or class and drops what the person may not see. Off by default. | engine; authorizer | `built` | `agent_fleet/weaviate_expert/service.py:96`, `:110-148`, `:338`, `:602`; `main.py:226-240` |
| 8 | The engine answers. Nothing on the answer says which version was read or under which decision. | engine | `not built` | finding 7 |

## Gaps

- **No server end for the SDK's remote graph client, and no remote vector client** (finding 1). Remedy: retire the remote graph client, [roadmap row 8](../ROADMAP.md) (`accepted`).
- **No conformance check reads a live graph or vector store** (finding 10). The offline check proves the contract over stubs. No roadmap row yet; it will go on the list of claims a seal could make `measured`, which is not yet written.
- **No access decision on these reads by default** (finding 6). Remedy: roadmap row 6.
- **The store sees the engine's credential, and no receipt comes back** (findings 3, 7). Remedy: roadmap row 7.
- **What telemetry records, and where:** pending the perf lane's answers (finding 9). No row until they arrive.
- **A stale comment on the class-candidate flag** (finding 8). Owner: invincible-agent. It is a comment and changes nothing that runs, so it has no roadmap row.

## Not established from code

- What `provenance-telemetry` emits, where it lands, and whether a trace reaches from an agent's answer to the store read. These wait on the perf lane.
- Whether any deployment turns `ENABLE_AGENTIC_AUTH` on. That is a fact of deployments, not of code.
- What the ADR-0025 flip plan lists as unbuilt. It was not read.
- The SDK lines were supplied by the SDK's session and not read here.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| The remote graph client has no handler; remote vector and ontology clients not built | SDK `remote_clients.py:36-41`, `:58` (supplied) | not stated | tested against a stub (supplied) | `built` (client); `not built` (server end) |
| No route for the SDK's remote clients in either repository | dag-tools: the only two HTTP apps, `dag_tools/central_gateway/main.py:108` and `dag_tools/domain_broker/main.py:1094`; invincible-agent: whole-tree search at the pin | not stated | the complete dag-tools route list, none a graph or vector route: gateway `main.py:142`, `:172`, `:184`, `:374`; broker `main.py:1097`, `:1145`, `:1168` | `not built` |
| Engine-o's graph reader implements the SDK interface, imports no driver, is read-only | invincible-agent `agent_fleet/ontology_service/mesh_graph.py:1-25`, `:158` | not stated | `test_mesh_graph_conforms.py::test_it_satisfies_the_protocol_structurally` | `built` |
| The vector reader implements the SDK interface, imports no driver, serves two engines | invincible-agent `agent_fleet/utils/mesh_vectors.py:1-27`, `:83`; `ontology_service/main.py:1074-1076` | not stated | `test_mesh_vectors_conforms.py::test_the_SDK_conformance_suite_passes` | `built` |
| Engine-o reads Neo4j on its posture credential | invincible-agent `ontology_service/main.py:466-468`, `:886` | not stated | — | `built` |
| The Neo4j engine reads through its own driver | invincible-agent `agent_fleet/neo4j_expert/tools.py:9-14`; `neo4j_expert/service.py:160-163` | not stated | — | `built` |
| The Weaviate client passes no credential | invincible-agent `agent_fleet/utils/weaviate_utils.py:33-40` | not stated | — | `built` |
| Every reader operation calls the SDK person guard | invincible-agent `mesh_graph.py:188-200`; `mesh_vectors.py:119-131` | not stated | `test_mesh_graph_conforms.py::test_EVERY_operation_refuses_a_service_identity`; `test_mesh_vectors_conforms.py::test_a_service_identity_is_refused_by_BOTH_operations`; `test_the_person_guard_is_an_allowlist_everywhere.py::test_each_fleet_wrapper_is_ONE_call_to_the_SDK_guard` | `built` |
| `on_behalf_of` cannot change the guard's decision | invincible-agent `tests/test_the_person_guard_is_an_allowlist_everywhere.py:157` | not stated | `test_ON_BEHALF_OF_CANNOT_CHANGE_A_DECISION` | `built` |
| The verb route refuses a request with no person | invincible-agent `ontology_service/main.py:6613-6622` | not stated | `test_FLAG_ON_WITHOUT_A_PERSON_IS_A_400_AND_READS_NOTHING` | `built` |
| The SDK makes no access decision; the person is provenance | SDK `interfaces.py:150`, `:255`; `refusal.py:1`, `:51`; `core.py:593-619` (supplied) | not stated | — | `built` |
| Per-document `can_read` in the Weaviate engine, deny by default, fail closed | invincible-agent `weaviate_expert/service.py:92-96`, `:110-148`, `:338`, `:602` | cited in code: invincible-agent ADR-0025 (not read) | `test_an_EMPTY_caller_sees_NOTHING_on_either_path` | `built` |
| The same check in the Neo4j engine | invincible-agent `neo4j_expert/service.py:92-135`, `:626`, `:671`, `:750` | cited in code: ADR-0025 (not read) | — | `built` |
| Per-class `can_view` in engine-o | invincible-agent `ontology_service/main.py:226-240` | cited in code: ADR-0025 (not read) | — | `built` |
| `ENABLE_AGENTIC_AUTH` defaults off in code and chart; three gates flip together | invincible-agent `helm/invincible-agent/values.yaml:2927-2944`; `templates/configmap.yaml:388-397` | cited in chart: ADR-0025 (not read) | `test_the_control_with_auth_OFF_both_paths_keep_EVERY_chunk` | `built` |
| No ticket, receipt or `as_of` on graph or vector reads | invincible-agent `mesh_graph.py`, `mesh_vectors.py`, `neo4j_expert/service.py`, `neo4j_expert/tools.py`, `weaviate_expert/service.py`: no match at the pin | not stated | — | `not built` |
| Verb route via the SDK reader defaults on | invincible-agent `ontology_service/main.py:6584-6593` | not stated | `test_THE_FLAG_DEFAULTS_ON`; `test_FLAG_ON_READS_LEG_1_THROUGH_verbs_for_AS_THE_PERSON` | `built` |
| Class-candidate pool via the SDK reader defaults on; the comment above says off | invincible-agent `ontology_service/main.py:1630-1633`, `:1644-1646` | not stated | `test_the_flag_default_is_on_and_every_claim_about_it_agrees.py::test_the_DEFAULT_RESOLVES_to_True_when_the_environment_is_silent` | `built` |
| Knowledge search via the SDK reader defaults on; a degraded search is marked | invincible-agent `weaviate_expert/service.py:181-186` | not stated | `test_the_mesh_read_is_ATTRIBUTED_to_the_caller_as_a_person`; `test_a_DEGRADED_mesh_search_is_MARKED` | `built` |
| Both readers pass the SDK offline check; a missing driver is unreachable, not empty; a mismatched marker refuses | invincible-agent `tests/test_mesh_graph_conforms.py:182`, `:201`; `tests/test_mesh_vectors_conforms.py:182`, `:267` | not stated | `test_check_offline_passes_for_every_operation`; `test_the_SDK_conformance_suite_passes`; `test_NO_DRIVER_IS_UNREACHABLE_not_empty`; `test_a_MISMATCHING_marker_REFUSES_and_names_BOTH` | `built` |
| The offline check is contract conformance; both fleet runs use stubs; no live-store read check | SDK `iagent_mesh/conformance.py:106-155` (supplied); invincible-agent `tests/test_mesh_graph_conforms.py:55-84`, `tests/test_mesh_vectors_conforms.py:135` | not stated | stub driver and stub client in the fleet tests | `built` (contract); `not built` (live read) |
| The SDK has no telemetry capability; tracing is at the platform layer | SDK `docs/architecture_manifesto.md:82` (supplied) | not stated | — | `built` (absence) |
| Fleet engines pin `provenance-telemetry==0.2.2` | invincible-agent `pyproject.toml:72`; `agent_fleet/{data_analyst,docs_agent,neo4j_expert,ontology_service,presentation_agent,restate_analyst,weaviate_expert}/pyproject.toml` | not stated | the documents engine's core is held free of it: `tests/docs/test_explain_refuses_before_it_renders.py::test_the_core_holds_no_driver` | `built` (pin); rest pending |
| What the telemetry library emits and where | provenance-telemetry, at a pin the perf lane gives | — | — | pending |
