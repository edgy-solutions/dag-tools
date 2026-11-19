# Roadmap note

Status: A note, not a decision.

The remedies proposed for gaps the flow records found, in the order they would run, with no dates. Every row is `proposed`: it is written here and nowhere else, no decision record accepts it, and no code does it. Each row would be decided in its own record by the repository that owns the code. Line numbers are at the dag-tools pin in [the README](README.md).

## Order

| # | increment | what isolates | home | depends on |
|---|---|---|---|---|
| 1 | Verified principal by default | the gateway reads who a request is for from the verified token, not from a header | — (proposed; no decision yet) | the policy delegate rail and its reader |
| 2 | Refuse what the object store cannot narrow | an object-store ticket is never wider than the decision that allowed it | — (proposed; no decision yet) | — |
| 3 | No ticket on the producer's credential | every backend a ticket names is read on a credential minted for that ticket | — (proposed; no decision yet) | — |
| 4 | Bounded reads from SQL backends | a read from a SQL backend is no larger than the caller asked for and can stop before the whole table moves | — (proposed; no decision yet) | — |
| 5 | A version on every read | a receipt says which version of the dataset was read | — (proposed; no decision yet) | — |

## Where later slices fold in

| row | slice | folds into | why it costs the increment nothing |
|---|---|---|---|

## The rows

### 1. Verified principal by default

- **Gap:** [need to know, finding 4](flows/need-to-know.md). Under `observe`, the code default, the gateway uses the named principal as given (`dag_tools/central_gateway/identity.py:178-197`, `:588-608`). `built`
- **What already exists:** the gateway counts what `enforce` would have refused, separately from what it did refuse, and reports both on `/health` (`dag_tools/central_gateway/refusal_gauge.py:20-53`; `main.py:142-170`). `built`
- **Remedy:** a deployment moves `GATEWAY_IDENTITY_POSTURE` to `enforce` once the would-refuse counts read zero over a period it chooses. Before that, the delegation seam (`identity.py:270-285`) gets a reader of the policy rail that lists which services may act for a person, so those services are not refused. `proposed`
- **Depends on:** the delegate rail, which policy owns in invincible-agent. Its reader in dag-tools is not built.

### 2. Refuse what the object store cannot narrow

- **Gap:** [need to know, finding 5](flows/need-to-know.md). The object-store minter scopes a credential to the dataset's prefix and ignores the decision's column and row narrowing (`dag_tools/domain_broker/main.py:226-256`). `built`
- **Remedy:** when a decision narrows and the backend cannot enforce the narrowing, the broker refuses the ticket rather than issuing a prefix credential. This holds until a narrowed read path exists on the server side. Minting already fails closed with a 503 when it errors (`main.py:1242-1268`), so the refusal has a path to use. `proposed`
- **Note:** no deployed policy narrows today (finding 2), so this row changes nothing until one does. It keeps the gap from opening when one does.

### 3. No ticket on the producer's credential

- **Gap:** [need to know, finding 5](flows/need-to-know.md). A backend with no entry in `_MINTERS` (`dag_tools/domain_broker/main.py:298-308`) is returned on the producer's own credential, and the code says so rather than calling it protected (`:1219-1223`). `built`
- **Remedy:** add a minter for each backend in use. Until each has one, a deployment switch makes the broker refuse a ticket for an unminted backend instead of returning the producer's credential. `proposed`

### 4. Bounded reads from SQL backends

- **Gap:** [big data, findings 2, 3 and 5](flows/big-data.md). The Postgres and ClickHouse paths read the whole table eagerly into client memory (`dag_tools/cortex_data/client.py:318`, `:323`, `:352-353`), so a consumer's own size gate runs after the transfer. `built`
- **Remedy:** the client passes the caller's column list, predicate and a row limit into the SQL it sends, and reads the result in batches rather than all at once. The data read gets a timeout of its own, as the ticket request already has (`client.py:205`). `proposed`
- **Note:** the object-store paths are already lazy scans (`client.py:260-264`, `:295-297`) and need nothing here.

### 5. A version on every read

- **Gap:** [big data, finding 8](flows/big-data.md). The receipt has an `as_of` field, and the table of version readers ships empty (`dag_tools/domain_broker/ticket_identity.py:110`). `built`
- **Who fills it:** the domain broker (`ticket_identity.py:149-194`). The client only copies the field onto the receipt (`dag_tools/cortex_data/client.py:45-56`). **Owner: dag-tools.**
- **Waiting on it:** invincible-agent #375 cites the receipt's as-of in a finance agent's answer, and claims none when the receipt has none.
- **Remedy:** add a version reader for each backend in use, starting with the table formats that keep one (Delta, Iceberg). `proposed`

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
| The five remedies | this note | none | — | `proposed` |
