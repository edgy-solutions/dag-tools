# SDK decisions: the standing decisions its session states

This record lists the standing decisions of the mesh SDK. The SDK has no committed decision census. Its session supplied each entry, with the code path, line and test at the SDK commit given in the entry. Nothing here was read in the SDK's repository. [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). The record shape is provisional.

Entries are addressed `SDK.N`. The numbers belong to this record only, not to any series of the SDK's.

Two SDK commits are cited. Entries 1 to 4 were supplied at the SDK pin in [the README](../README.md), `d6a73783`. Entries 5 to 8, and the deprecation in entry 3, were supplied at a later SDK commit, `f1a94103`, and postdate that pin. A line number holds at the commit it is given at and is not guaranteed at the other. The gate read of this record found 24 of the 25 line ranges in entries 1 to 4 unchanged at `f1a94103`. The one that moved is `remote_clients.py:58`, now `:69`, displaced by the deprecation that entry 3 cites.

### SDK.1

- **Decision.** The SDK carries who a call is for and makes no access decision: it consults no grants. It does check the kind of identity at the boundary, where its person guard admits a person and refuses a service or a delegate. Inbound, it verifies a bearer token signed with one asymmetric algorithm, against the issuers and audiences a deployment lists in environment variables. The status header it sets is a diagnostic only, and it exchanges no token. Outbound, its client attaches the authorization header. The initiator's on-behalf-of field records provenance and does not decide anything. The decision is left to the caller, which must name the person.
- **Why.** Not supplied.
- **Where else.** [Agent capabilities, finding 5](../flows/agent-capabilities.md).
- **Status.** `built`.

### SDK.2

- **Decision.** Large data travels by reference. Resolving a dataset returns a data reference, and the caller reads it with dag-tools' mesh data client. The SDK wraps no read. A contract test parses the dag-tools client's source to pin the call the SDK relies on. The read that also returns the ticket is not referenced.
- **Why.** Not supplied.
- **Where else.** [The mesh data client](../components/mesh-data-client.md); [the big-data flow](../flows/big-data.md).
- **Status.** `built`.

### SDK.3

- **Decision.** The SDK's remote graph client is retired rather than completed. The client posts to a mesh graph route that no gateway handles, and it is tested against a stub only. The server end is not to be built. Instead the client warns on construction that it is deprecated.
- **Why.** The architect's ruling of 2026-11-18 on roadmap row 8, as supplied.
- **Where else.** [Agent capabilities, finding 1](../flows/agent-capabilities.md); [the roadmap note](../ROADMAP.md), row 8.
- **Status.** The client and its deprecation warning: `built`. No server end: `built`. Removal of the client: `accepted` (roadmap row 8).

### SDK.4

- **Decision.** The SDK defines a vector interface, with a nomination call and two retrieval modes, and deliberately builds no remote client for vectors or the ontology. Engines implement the read interfaces in-process. The SDK's offline conformance checks a writer's contract; it has no read conformance.
- **Why.** Not supplied.
- **Where else.** [Agent capabilities, findings 1, 2 and 10](../flows/agent-capabilities.md).
- **Status.** The vector interface: `built`. No remote vector or ontology client, by decision: `built`. No read conformance check: `built`.

### SDK.5

- **Decision.** Most SDK fields take an open string that the SDK does not check against a vocabulary. One field is the deliberate exception: the attestation on a content-kind registration takes one of two fixed values.
- **Why.** Stated as deliberate; the reason is not supplied.
- **Status.** `built`.

### SDK.6

- **Decision.** Declared-configuration models, the workflow definition and its kin, inherit one base that forbids unknown keys and freezes the object.
- **Why.** An unrecognised key refuses the whole object at load, rather than being dropped without notice.
- **Where else.** dag-tools meets the same question where a settings model ignores unknown keys, and answers it by removing the key first; the block's own model forbids them: [§4p](dag-tools-s4p.md).
- **Status.** `built`.

### SDK.7

- **Decision.** One family of mesh verbs pages through a single envelope, not a cursor per call. The cursor is opaque: the caller passes it back as it came, and the SDK never inspects or materializes it.
- **Why.** It follows invincible-agent ADR-0070, section 3, as supplied.
- **Status.** `built`.

### SDK.8

- **Decision.** The SDK's action contract carries an explicit, named version constant, now at its second version.
- **Why.** Optional fields can be added without renegotiating the shape.
- **Status.** `built`.

## Not established

- The reason for entries 1, 2, 4 and 5. The session supplied the code, not the reasoning.
- Tests for entries 1 and 4. None were supplied.

## Sources

All paths are in the SDK repository, as supplied by its session, and were not read here. `d6` is commit `d6a737837c3d0609ace95017c6b4b45b500ea755`. `f1` is commit `f1a941030bd68f1218ee6122750619ea08ee7276`. Paths are under `iagent_mesh/` unless they start with `tests/`. The person-guard row was supplied by the gate read of this record, not by the SDK's session.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Bearer verified; issuers and audiences from the environment; header diagnostic only; no token exchange | d6 `transport_auth.py:380`, `:442-460`, `:570`, `:585-590` | SDK.1 | not supplied | `built` |
| Outbound authorization header | d6 `client.py:88-96`, `:146` | SDK.1 | not supplied | `built` |
| On-behalf-of is provenance; no access decision in the SDK | d6 `interfaces.py:150`, `:255`; `delegate_identity.py:16-20`; `refusal.py:1`, `:51`; `core.py:593-619` | SDK.1 | not supplied | `built` |
| Person guard checks the kind of identity at the boundary; no grants consulted | d6 `interfaces.py:216-243` | SDK.1 | not supplied | `built` |
| Resolve returns a data reference; caller reads | d6 `interfaces.py:1013`, `:1076`, `:1085`; `core.py:503` | SDK.2 | `tests/test_cortex_data_client_contract.py` | `built` |
| Remote graph client | d6 `interfaces.py:283`; `remote_clients.py:40-41`, `:58` | SDK.3 | `tests/test_remote_clients.py` (against a stub) | `built` |
| No server end for the remote graph client | d6 `interfaces.py:283`; `remote_clients.py:40-41`, `:58` | SDK.3 | `tests/test_remote_clients.py` (against a stub) | `built` |
| Graph client warns it is deprecated | f1 `remote_clients.py:43`, `:80`, `:90` | SDK.3 | `test_construction_warns_deprecated` (`tests/test_remote_clients.py:30`) | `built` |
| Retire the graph client | roadmap row 8; architect's ruling 2026-11-18 | SDK.3 | — | `accepted` |
| Vector interface | d6 `interfaces.py:724`, `:792`, `:800`; `remote_clients.py:36-38` | SDK.4 | not supplied | `built` |
| No remote vector or ontology client | d6 `interfaces.py:724`, `:792`, `:800`; `remote_clients.py:36-38` | SDK.4 | not supplied | `built` |
| No read conformance | d6 `interfaces.py:373-374`; `conformance.py:725` | SDK.4 | not supplied | `built` |
| Attestation is a closed set of two values | f1 `ingest.py:507` | SDK.5 | `test_attestation_is_a_closed_literal_not_an_open_string` (`tests/test_ingest.py:592`) | `built` |
| Declared models forbid unknown keys and freeze | f1 `workflow_definition.py:86` | SDK.6 | `test_the_definition_itself_rejects_an_unknown_top_level_key` (`tests/test_workflow_definition.py:100`) | `built` |
| One paging envelope; opaque cursor | f1 `paged_answer.py:201` | SDK.7 | `test_paged_answer_next_is_opaque_and_passed_through` (`tests/test_paged_answer.py:106`) | `built` |
| Named action contract version | f1 `maintenance_bridge.py:423` | SDK.8 | `test_action_contract_version_is_named_and_bumped_for_this_change` (`tests/test_maintenance_bridge.py:277`) | `built` |
