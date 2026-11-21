# Mesh data client (CO-CLIENT)

The mesh data client is the consumer's side of a dataset read. It is a Python library, `cortex_data`, imported into the process that wants the data. It asks the central gateway to authorize a read of a URN, takes the ticket the gateway returns, and reads the data itself, straight from the store the ticket names. It returns a lazy frame and, when asked, a receipt for the read. The decision is followed in [need to know](../flows/need-to-know.md) and the read's size in [big data](../flows/big-data.md); neither is repeated here.

This record covers what the code provides by default, and what a deployment or a caller may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`), not at the flows' pin. The record shape is provisional.

## Findings

1. **It is a library, not a service.** Nothing deploys it. Its read dependencies (the SQL, ClickHouse, Delta and Iceberg readers) are base dependencies of the package, so any install can read every backend type the client knows. Its own docstring names three kinds of user: notebooks, Dagster IO managers and agents. In dag-tools the one caller is a read-only IO manager, which builds a client from service credentials on each input it loads and raises on every write. `built`
2. **Construction fails early, naming what is missing.** The gateway address comes from an argument or an environment variable, and its absence raises naming the variable. Authentication is a token handed in, a development token from the environment (which wins over service credentials), or service credentials exchanged for a token by a client-credentials grant against a token URL. With none of these, construction raises; it does not wait for the first read. `built`
3. **Who the read is for is resolved in a fixed order of rungs.** An explicit subject first, then a caller object, then the caller of the current request through the SDK, then an environment variable naming the user, then the service itself. Inside a request, a caller that cannot be resolved raises rather than falling through to the service: that rung is terminal even when the environment names a user. The SDK is optional; without it, that rung is skipped and the refusal says so. `built`
4. **Reading as the service needs an opt-in, and every constructed client has one.** The service rung is allowed when the caller passes a flag or the client holds a token or service credentials. Construction already requires one of those (finding 2), so outside a request a client that names no subject reads as the service. The code states this as deliberate: the guard is the terminal request rung, not the flag. `built`
5. **The authorize call is one POST with a ten-second timeout.** It carries the client's token as a bearer token. When a subject is set it is sent twice, under the current header name and a legacy one, so an older gateway still reads it. An optional user identifier travels in a third header. `built`
6. **Outcomes are three separate errors.** A 401 or 403 is "not entitled". A 404 is "unavailable", worded as the owning deployment's liveness rather than the dataset's absence, because routes expire and are re-sent by the brokers ([gateway, finding 10](central-gateway.md)). Any other failure, and a gateway that cannot be reached, is "unavailable" too. Each can be caught on its own. `built`
7. **The client reads the data itself, by source type.**

   | Source type | How it reads | Where the decision's narrowing is applied |
   |---|---|---|
   | object store: Parquet, Delta | a lazy scan with the ticket's credential; a plain-HTTP endpoint is allowed when the ticket names one | in the client, on the frame |
   | object store: Iceberg | loads the table through a catalog the ticket describes, then a lazy scan | in the client, on the frame |
   | Postgres | the whole table, eagerly, by a `SELECT *` over a connection URI | not in the client; the code leaves it to the database |
   | ClickHouse | the whole table, eagerly, as Arrow | in the client, on the frame, after the whole table has arrived |

   Any other source type raises. For the object store, a ticket that carries no key or secret is not refused: both default to empty, so the client attempts an unsigned scan. `built`
8. **Client-side narrowing is a column selection and a filter expression.** Where the client applies it, it selects the decision's allowed columns and filters by the decision's row filter, which it parses as a SQL expression. It runs in the consumer's own process, so it narrows what this client returns and nothing else. `built`
9. **For SQL backends, the client falls back to its own mesh token as the database password.** For Postgres it uses the ticket's password, else the client's token. For ClickHouse it uses the ticket's password, else the ticket's token, else the client's token. The code says the token fallback is there for a database sign-in pattern that is not wired. On the broker's older path, which mints nothing for SQL ([broker, finding 8](domain-broker.md)), the ticket carries a fixed placeholder token and no password: ClickHouse then signs in with the placeholder, and Postgres with the client's mesh token. No test reaches either SQL branch. `built`
10. **The receipt is an allowlist.** It carries the URN and eight named ticket fields: the ticket id, issue time, version, version source, the reason a version is absent, the source type, and the allowed columns and row filters. The first four are always present, as empty when the broker gave none. No credential and no physical location is on it, because a presigned location is a credential. `get_dataframe` still returns the frame alone; `get_dataframe_with_ticket` returns both. `built`

## Settings and defaults

No chart deploys the client. These are read by the client in the consumer's process.

| Setting | Code default | What it does |
|---|---|---|
| `broker_url`, else `CORTEX_BROKER_URL` | none; construction raises | the gateway's address (the name is historical) |
| `jwt_token`, else `MESH_DEV_TOKEN` | none | a token used as is; wins over service credentials |
| `client_id`/`client_secret`, else `CORTEX_CLIENT_ID`/`CORTEX_CLIENT_SECRET` | none | service credentials exchanged for a token |
| `keycloak_url`, else `KEYCLOAK_TOKEN_URL` | an in-cluster token URL | where service credentials are exchanged |
| `on_behalf_of` (alias `originator_email`) | none | the subject read for; naming two different ones raises |
| `caller` | none | an object the subject is read from |
| `CORTEX_USER_TOKEN` | none | a subject named by the environment, below the request rung |
| `service_identity` | off | an explicit opt-in to read as the service |
| `originator_sub` | none | an optional user identifier, sent in its own header |

The read-only IO manager takes the gateway address, the service credentials and the token URL as its own configuration and passes them through.

## Gaps

- **SQL reads move the whole table before anything narrows or limits them** (finding 7). Remedy: [roadmap row 4](../ROADMAP.md) (dag-tools #73).
- **The receipt's version is empty until a version reader exists** (finding 10). That is the broker's gap ([broker, finding 11](domain-broker.md)). Remedy: [roadmap row 5](../ROADMAP.md) (dag-tools #74).
- **The client can send its mesh token to a backend as a password** (finding 9). A token meant for the gateway reaches a database, and on the older path the database sees either that token or a fixed placeholder. Neither branch is tested. Remedy: [roadmap row 12](../ROADMAP.md) (not filed), with [row 9](../ROADMAP.md) (not filed) for the placeholder.
- **Narrowing applied in the client binds only callers that use the client** (findings 7, 8). For the object store, it is the only place narrowing happens. Remedy: [roadmap row 2](../ROADMAP.md) (dag-tools #71).

## Not established from code

- Whether a whole-table read succeeds on a minted SQL account that the decision narrowed to some columns. The broker grants only those columns ([broker, finding 6](domain-broker.md)) and the client asks for all of them. What each database answers is its own behaviour; no test or run reads a narrowed table through the client.
- Whether any Postgres deployment applies the decision in the database for a read on the older path. The client skips its own narrowing for Postgres on every path.
- Which processes outside dag-tools use the client, and with which identity. The SDK's use of it is recorded in [agent capabilities](../flows/agent-capabilities.md) as supplied by the SDK's session.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-GATEWAY | calls | HTTP | bearer token | a dataset URN; back comes a ticket | `dag_tools/cortex_data/client.py:188-205`; `test_authorize_endpoint_shape` | `built` |
| CO-KEYCLOAK | calls | HTTP | client-credentials grant, used when no token is handed in | service credentials; back comes a token | `dag_tools/cortex_data/client.py:89-112`, `:146-159`; `test_cortex_client_m2m_env` | `built` |
| XE-OBJECT-STORE | reads | object-store scan | the ticket's credential; an unsigned scan when the ticket carries none | the table's data, narrowed in the client | `dag_tools/cortex_data/client.py:236-356`; no test | `built` |
| XE-POSTGRES | reads | SQL through a database driver | the credential the ticket carries | the whole table, eagerly | `dag_tools/cortex_data/client.py:236-356`, `:315`; no test | `built` |
| XE-CLICKHOUSE | reads | SQL through a database driver | the credential the ticket carries | the whole table, eagerly | `dag_tools/cortex_data/client.py:236-356`, `:339`; no test | `built` |
| CO-DAGSTER | is called by | in-process | none | a URN in; a frame out, through a read-only IO manager | `dag_tools/cortex_data/client.py:59-63`; `dag_tools/io_managers/cortex_io_manager.py:145-153`, `:155-173`; no test | `built` |

## Place in the mesh

| Aspect | What the code shows | Alignment row |
|---|---|---|
| Purpose | the consumer's side of a dataset read: authorize, then read directly | — |
| How the mesh wires it | imported into the consumer's process; finds the gateway by an address it is given; in dag-tools, through the read-only IO manager | — |
| Sign-in | a bearer token, either handed in or fetched by client credentials from the identity provider; the person read for travels as a header beside it | [NATO R7](../alignment/nato-dcra.md) |
| Data path | it is the data path: bytes move from the store to this process, never through the gateway or broker | [DoD S7](../alignment/dod-dmra.md) |
| Plane | `analytical-data`: where agents and notebooks read. It reaches the data-management plane only through the gateway's API | [NATO L4](../alignment/nato-dcra.md) |
| Security role | enforcement point, for the narrowing it applies itself | [NATO R1](../alignment/nato-dcra.md) |
| Services served | data exchange; the receipt's ticket id is a unique identifier for the read | [DoD S7, S1](../alignment/dod-dmra.md) |

## Sources

All dag-tools paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Client source paths are under `dag_tools/cortex_data/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| A library; read dependencies in the base install; one dag-tools caller, read-only | `client.py:59-63`; `pyproject.toml:44-54`; `dag_tools/io_managers/cortex_io_manager.py:145-153`, `:155-173` | not stated | — | `built` |
| Construction fails early; development token wins | `client.py:89-112`, `:146-159` | not stated | `test_cortex_client_documented_contract.py`: `test_missing_broker_url_names_the_env_var`, `test_missing_auth_fails_at_construction_not_at_read`, `test_dev_token_wins_over_m2m`; `test_cortex_client_zero_config.py`: `test_cortex_client_zero_config_missing_env`, `test_cortex_client_m2m_env` | `built` |
| Subject resolved in a fixed order; request rung terminal; SDK optional | `identity.py:17`, `:146-237`; `client.py:81-87`, `:139-144` | not stated | `test_cortex_caller_identity.py`: `test_the_request_rung_is_terminal_even_with_the_env_var_set`, `test_the_request_caller_outranks_the_environment`, `test_inside_a_request_an_unresolved_caller_raises`, `test_without_the_sdk_the_refusal_says_the_rung_was_skipped`, `test_the_sdk_is_optional_not_required` | `built` |
| Service rung needs an opt-in; any constructed client has one | `client.py:109-112`, `:136-138` | not stated | `test_cortex_caller_identity.py`: `test_the_service_identity_requires_opting_in`, `test_outside_a_request_a_provisioned_process_reads_as_itself`, `test_naming_the_service_credentials_is_the_opt_in` | `built` |
| One POST, ten-second timeout, subject under two header names | `client.py:188-205` | not stated | `test_cortex_client_documented_contract.py`: `test_authorize_endpoint_shape`, `test_originator_headers_are_sent_when_reading_for_a_user`, `test_originator_headers_are_absent_when_reading_as_the_service` | `built` |
| Three separate outcomes; 404 is liveness | `identity.py:73-143`; `client.py:206-228` | not stated | `test_cortex_caller_identity.py`: `test_a_refusal_is_unentitled`, `test_a_404_is_unavailable_not_absent`, `test_server_errors_are_unavailable`, `test_the_three_outcomes_are_separately_catchable` | `built` |
| Reads by source type; SQL reads whole and eager; Postgres not narrowed in the client | `client.py:236-356` | not stated | object-store branches through `test_ticket_receipt.py`'s frames; no test reaches either SQL branch | `built` |
| Client-side narrowing: select and filter | `client.py:358-366` | not stated | `test_ticket_receipt.py`: `test_the_frame_is_still_narrowed_when_the_receipt_is_asked_for` | `built` |
| Mesh token as the SQL password fallback | `client.py:315`, `:339` | not stated | no test | `built` |
| Receipt is an allowlist; frame-only call unchanged | `client.py:23-56`, `:161-171`, `:368` | dag-tools AGENTS.md §4o | `test_ticket_receipt.py`: `test_the_receipt_is_an_allowlist_so_a_new_ticket_field_is_not_published`, `test_a_presigned_uri_is_not_on_the_receipt`, `test_the_receipt_carries_no_credential`, `test_get_dataframe_still_returns_only_a_frame` | `built` |
| Remedies | [the roadmap note](../ROADMAP.md), rows 2, 4, 5, 9 and 12 | none | — | `proposed` |
