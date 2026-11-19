# Roadmap note

Status: A note, not a decision.

The remedies proposed for gaps the flow and component records found, in the order they would run, with no dates. Every row is `proposed` except row 8: it is written here and nowhere else, no decision record accepts it, and no code does it. Each row would be decided in its own record by the repository that owns the code. Row 8 is `accepted`: the architect ruled on it on 2026-11-18, and no code does it yet. Rows 6 and 7 have owners named by the architect the same day and stay `proposed`. Line numbers are at the pins in [the README](README.md): rows 1 to 8 at the flows' pins, rows 9 to 12 at the component-records pin. Where an issue tracks a row, it is named beside the row; "not filed" means no issue exists yet.

## Order

| # | increment | what isolates | home | depends on |
|---|---|---|---|---|
| 1 | Verified principal by default (dag-tools #70) | the gateway reads who a request is for from the verified token, not from a header | — (proposed; no decision yet) | the policy delegate rail and its reader |
| 2 | Refuse what the object store cannot narrow (dag-tools #71) | an object-store ticket is never wider than the decision that allowed it | — (proposed; no decision yet) | — |
| 3 | No ticket on the producer's credential (dag-tools #72) | every backend a ticket names is read on a credential minted for that ticket | — (proposed; no decision yet) | — |
| 4 | Bounded reads from SQL backends (dag-tools #73) | a read from a SQL backend is no larger than the caller asked for and can stop before the whole table moves | — (proposed; no decision yet) | — |
| 5 | A version on every read (dag-tools #74) | a receipt says which version of the dataset was read | — (proposed; no decision yet) | — |
| 6 | Engine read decisions on by default (not filed) | a graph or vector read returns only what the person may read | — (proposed; owner named: invincible-agent) | the preconditions invincible-agent's chart names for the switch |
| 7 | A receipt on graph and vector reads (not filed) | a graph or vector answer says what was read, for whom, and which version | — (proposed; owners named: the SDK and invincible-agent; no work now) | row 5, for the version |
| 8 | Retire the SDK's remote graph client (not filed) | no client posts to a route nothing serves; in-process readers inside engines are the supported shape | the architect's ruling, 2026-11-18 (accepted: retire) | — |
| 9 | No placeholder on a served path (not filed) | a registered dataset and an issued ticket are real or refused | — (proposed; no decision yet) | — |
| 10 | Remove expired minted SQL accounts (not filed) | a database holds no minted account past its lifetime | — (proposed; no decision yet) | — |
| 11 | The gateway says what it does (not filed) | the gateway's code and warnings describe the gateway that runs | — (proposed; no decision yet) | — |
| 12 | No mesh token sent to a backend (dag-tools #76) | the token meant for the gateway reaches only the gateway | — (proposed; no decision yet) | row 9 |

## Where later slices fold in

| row | slice | folds into | why it costs the increment nothing |
|---|---|---|---|

## The rows

### 1. Verified principal by default

- **Gap:** [need to know, finding 4](flows/need-to-know.md). Under `observe`, the code default, the gateway uses the named principal as given (`dag_tools/central_gateway/identity.py:178-197`, `:588-608`). `built`
- **What already exists:** the gateway counts what `enforce` would have refused, separately from what it did refuse, and reports both on `/health` (`dag_tools/central_gateway/refusal_gauge.py:20-53`; `main.py:142-170`). `built`
- **Remedy:** a deployment moves `GATEWAY_IDENTITY_POSTURE` to `enforce` once the would-refuse counts read zero over a period it chooses. Before that, the delegation seam (`identity.py:270-285`) gets a reader of the policy rail that lists which services may act for a person, so those services are not refused. `proposed`
- **Depends on:** the delegate rail, which policy owns in invincible-agent. Its reader in dag-tools is not built.
- **Issue:** dag-tools #70.

### 2. Refuse what the object store cannot narrow

- **Gap:** [need to know, finding 5](flows/need-to-know.md). The object-store minter scopes a credential to the dataset's prefix and ignores the decision's column and row narrowing (`dag_tools/domain_broker/main.py:226-256`). `built`
- **Remedy:** when a decision narrows and the backend cannot enforce the narrowing, the broker refuses the ticket rather than issuing a prefix credential. This holds until a narrowed read path exists on the server side. Minting already fails closed with a 503 when it errors (`main.py:1242-1268`), so the refusal has a path to use. `proposed`
- **Note:** no deployed policy narrows today (finding 2), so this row changes nothing until one does. It keeps the gap from opening when one does.
- **Issue:** dag-tools #71.

### 3. No ticket on the producer's credential

- **Gap:** [need to know, finding 5](flows/need-to-know.md). A backend with no entry in `_MINTERS` (`dag_tools/domain_broker/main.py:298-308`) is returned on the producer's own credential, and the code says so rather than calling it protected (`:1219-1223`). `built`
- **Remedy:** add a minter for each backend in use. Until each has one, a deployment switch makes the broker refuse a ticket for an unminted backend instead of returning the producer's credential. `proposed`
- **Issue:** dag-tools #72.

### 4. Bounded reads from SQL backends

- **Gap:** [big data, findings 2, 3 and 5](flows/big-data.md). The Postgres and ClickHouse paths read the whole table eagerly into client memory (`dag_tools/cortex_data/client.py:318`, `:323`, `:352-353`), so a consumer's own size gate runs after the transfer. `built`
- **Remedy:** the client passes the caller's column list, predicate and a row limit into the SQL it sends, and reads the result in batches rather than all at once. The data read gets a timeout of its own, as the ticket request already has (`client.py:205`). `proposed`
- **Note:** the object-store paths are already lazy scans (`client.py:260-264`, `:295-297`) and need nothing here.
- **Issue:** dag-tools #73.

### 5. A version on every read

- **Gap:** [big data, finding 8](flows/big-data.md). The receipt has an `as_of` field, and the table of version readers ships empty (`dag_tools/domain_broker/ticket_identity.py:110`). `built`
- **Who fills it:** the domain broker (`ticket_identity.py:149-194`). The client only copies the field onto the receipt (`dag_tools/cortex_data/client.py:45-56`). **Owner: dag-tools.**
- **Waiting on it:** invincible-agent #375 cites the receipt's as-of in a finance agent's answer, and claims none when the receipt has none.
- **Remedy:** add a version reader for each backend in use, starting with the table formats that keep one (Delta, Iceberg). `proposed`
- **Issue:** dag-tools #74.

### 6. Engine read decisions on by default

- **Gap:** [agent capabilities, finding 6](flows/agent-capabilities.md). The engines' per-document and per-class checks run only when `ENABLE_AGENTIC_AUTH` is on, and the code and chart both default it off (invincible-agent `agent_fleet/weaviate_expert/service.py:92-96`; `helm/invincible-agent/values.yaml:2927-2944`). With it off, every result is kept. `built`
- **Owner:** invincible-agent, with review by a second lane (named by the architect, 2026-11-18). The chart ties the switch to the last step of its ADR-0025 and to a plan for the switch, which lists preconditions that are not built. Neither was read here.
- **First deliverable (read-only):** what exactly is unprotected with the switch off, meaning which reads return results the caller is not entitled to and which other checks still apply; what ADR-0025's remaining step is; and what breaks when the switch is on. Then a plan to turn it on in a test deployment and then to make it the chart default. The row leads the planning of invincible-agent's next release; it is not deferred. `proposed`
- **Remedy:** the switch defaults on once those preconditions are met, so a graph or vector read is decided per object unless a deployment opts out. `proposed`
- **Note:** the chart says the switch does nothing for the data-analyst read path, which the gateway decides (`templates/configmap.yaml:388-397`). That path is [need to know](flows/need-to-know.md), not this row.
- **Issue:** not filed.

### 7. A receipt on graph and vector reads

- **Gap:** [agent capabilities, findings 3 and 7](flows/agent-capabilities.md). Graph and vector reads carry no ticket, receipt or version, and the store sees the engine's credential, not the person's. `not built`
- **Owner:** invincible-agent, whose engines hold the readers, and the SDK, whose result shape would have to carry the receipt.
- **Remedy:** each reader returns, with its result, the person it read for, the decision it read under and a version of what it read. The version would use the dataset receipt's own fields (`as_of`, `as_of_source`, `as_of_unavailable`; dag-tools `dag_tools/cortex_data/client.py:23-32`). The person and the decision would need fields the dataset receipt does not have: it carries no principal, and its `allowed_columns` and `row_filters` are the narrowing a decision produced, not the decision. `proposed`
- **Note:** the person is already on every read as provenance, but not on any receipt. The version would not appear for free: the table of version readers ships empty (row 5), so a receipt reusing those fields would say the version is unavailable until a reader exists for that store.
- **Placement (2026-11-18):** the architect placed this row jointly with the SDK and invincible-agent, to be recorded as an invincible-agent decision that these records cite, and confirmed the placement the same day. No work starts on it now. `proposed`
- **Issue:** not filed.

### 8. Retire the SDK's remote graph client

- **Gap:** [agent capabilities, finding 1](flows/agent-capabilities.md). The SDK ships a remote graph client that posts to a route nothing serves (supplied by the SDK's session). `not built`
- **What already exists:** in-process readers inside engines are what ships and is used today; the remote client was built after them, and has no production caller (supplied by the SDK's session). `built`
- **Decision:** retire the client (the architect's ruling, 2026-11-18). It has no route and no production caller, and in-process readers are what ships. Before the ruling, the SDK had declined to build a server end without a scheduled consumer or to retire the client on its own (supplied by the SDK's session). `accepted`
- **Owner:** the SDK and invincible-agent, each for its own part.
- **Remedy:** the SDK marks the client deprecated in its next patch release and removes it in its next minor release, with a note that in-process readers are the supported shape. invincible-agent closes its open item for generic mesh gateway routes as not planned; the item reopens when a named consumer needs a remote route, and that route is then designed behind a decision point. `accepted`; no code does it yet.
- **Issue:** not filed.

### 9. No placeholder on a served path

- **Gap:** [domain broker, findings 3 and 8](components/domain-broker.md). With no definitions module named, the broker registers two placeholder datasets (`dag_tools/domain_broker/main.py:42`, `:802-818`). On the older path, a SQL ticket carries a fixed placeholder token, and an object-store credential has a fixed one-hour lifetime rather than the configured one (`main.py:1273-1360`). No test covers the SQL branch. `built`
- **Owner:** dag-tools.
- **Remedy:** with no module named, the broker registers nothing and says why on `/ready`. On the older path, a SQL ticket is refused, and an object-store credential takes the configured lifetime. `proposed`
- **Issue:** not filed.

### 10. Remove expired minted SQL accounts

- **Gap:** [domain broker, finding 10](components/domain-broker.md). The ClickHouse users and Postgres roles the broker mints are refused once they expire, and are never dropped (`dag_tools/domain_broker/sql_minting.py:22-33`). `not built`
- **Owner:** dag-tools.
- **Remedy:** a sweep drops minted accounts whose lifetime has passed, using the identity that created them. `proposed`
- **Issue:** not filed.

### 11. The gateway says what it does

- **Gap:** [central gateway, findings 9 and 13](components/central-gateway.md). A relationship-check payload is built and never sent (`dag_tools/central_gateway/main.py:325`). A startup warning says no verification is enforced when a retired flag is set, though the identity posture can enforce it (`subject_gauge.py:310-316`). `built`
- **Owner:** dag-tools.
- **Remedy:** remove the unsent payload, or send it under a recorded decision; reword the warning to name the identity posture. `proposed`
- **Issue:** not filed.

### 12. No mesh token sent to a backend

- **Gap:** [mesh data client, finding 9](components/mesh-data-client.md). The client uses its own mesh token as the database password when the ticket carries no password (Postgres, `dag_tools/cortex_data/client.py:315`) or neither a password nor a token (ClickHouse, `:339`). It is the same bearer token the client presents to the gateway (`:189-191`). On the broker's older path the ticket carries a placeholder token and no password, so it is Postgres that receives the mesh token; no path in this repo issues a ClickHouse ticket with neither. No test reaches either branch. `built`
- **Owner:** dag-tools.
- **Remedy:** the client refuses a SQL ticket that carries no password, rather than sending a token meant for the gateway; and a test reaches each SQL branch. `proposed`
- **Note:** the older path is where a Postgres ticket carries no password (row 9), so the two rows close the same opening from both ends. The owner reads this row as a present exposure, not tidiness: once that path is taken, the send is unconditional in code. Whether a database receives it there depends on name resolution of the placeholder host, which the code cannot settle.
- **Issue:** dag-tools #76.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Posture default `observe`, set per deployment | dag-tools `dag_tools/central_gateway/identity.py:178-197` | not stated | `test_posture_parse` | `built` |
| Would-refuse and did-refuse counts on `/health` | dag-tools `dag_tools/central_gateway/refusal_gauge.py:20-53`, `main.py:142-170` | not stated | `test_gateway_refusal_reasons.py` | `built` |
| Delegation seam answers no | dag-tools `identity.py:270-285` | not stated | `test_the_default_seam_is_false_and_says_once_that_the_rail_is_not_wired` | `built` |
| Object-store minter ignores narrowing | dag-tools `dag_tools/domain_broker/main.py:226-256` | cited in code: invincible-agent ADR-0044 (not read) | `test_minting_failure_fails_closed` | `built` |
| No-minter backends get the producer credential | dag-tools `dag_tools/domain_broker/main.py:298-308`, `:1219-1223` | not stated | — | `built` |
| SQL reads are the whole table, eagerly | dag-tools `dag_tools/cortex_data/client.py:318`, `:323`, `:352-353` | not stated | no test reaches either call | `built` |
| No version reader ships | dag-tools `dag_tools/domain_broker/ticket_identity.py:110` | dag-tools AGENTS.md §4o | `test_no_as_of_reader_ships_by_default` | `built` |
| Engine read checks default off in code and chart | invincible-agent `agent_fleet/weaviate_expert/service.py:92-96`; `helm/invincible-agent/values.yaml:2927-2944` | cited in chart: invincible-agent ADR-0025 (not read) | `test_the_control_with_auth_OFF_both_paths_keep_EVERY_chunk` | `built` |
| Graph and vector reads carry no ticket, receipt or version | invincible-agent `agent_fleet/ontology_service/mesh_graph.py`, `agent_fleet/utils/mesh_vectors.py`: no match at the pin | not stated | — | `not built` |
| The remote graph client has no server end | SDK `remote_clients.py:40-41`, `:58` (supplied) | not stated | tested against a stub (supplied) | `not built` (server end) |
| Placeholder datasets; the older path's fixed token and lifetime | dag-tools `dag_tools/domain_broker/main.py:42`, `:802-818`, `:1273-1360` | not stated | no test of the SQL branch | `built` |
| Minted SQL accounts are never removed | dag-tools `dag_tools/domain_broker/sql_minting.py:22-33` | not stated | — | `not built` |
| An unsent check payload; a stale warning | dag-tools `dag_tools/central_gateway/main.py:325`, `subject_gauge.py:310-316` | not stated | — | `built` |
| The client's mesh token as a SQL password | dag-tools `dag_tools/cortex_data/client.py:315`, `:339` | not stated | no test | `built` |
| The twelve remedies, except row 8 | this note | none | — | `proposed` |
| Row 8: retire the remote graph client | this note | the architect's ruling, 2026-11-18 | — | `accepted` |
