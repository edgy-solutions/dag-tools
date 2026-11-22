# Topaz (CO-TOPAZ)

Topaz is the policy engine the [central gateway](central-gateway.md) asks whether a principal may read a dataset. The gateway is its only caller in this repo. Its answer can also carry a narrowing, a list of allowed columns and row filters, which the gateway passes to the [domain broker](domain-broker.md). The dag-tools chart does not deploy Topaz; a deployment supplies one and points the gateway at it.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **The chart deploys no Topaz, and the gateway is the only caller.** The gateway reads the engine's address from two variables in turn, with a code default naming an in-cluster service. No other module in this repo calls the engine. No template in the dag-tools chart creates a Topaz service. `built`
2. **One decision per read, at a fixed policy path.** For each read the gateway posts one request to the engine's decision route, naming a fixed policy path, the dataset URN as the resource, the principal as the identity and a read permission. The identity is passed as a plain value the gateway states; the engine is not asked to establish it from a token. A principal that names nobody is denied before any call. `built`
3. **The caller's own bearer token is sent to the engine.** The request's authorization header carries the token the reader presented to the gateway, not a credential of the gateway's own. Under the default `observe` posture that token has not been verified by the gateway (see [the gateway record](central-gateway.md), finding 3). `built`
4. **An optional key is sent as a tenant header.** When `TOPAZ_AUTHORIZER_API_KEY` is set, its value goes in a tenant header on every call. The chart passes it through the gateway's `envFrom`. Nothing else authenticates the gateway to the engine. `built`
5. **Any failure denies, and nothing is cached.** A non-200 answer or any exception denies, as [the gateway record](central-gateway.md), finding 8, describes, with a five-second limit. Each call also opens a new HTTP client, and no decision is kept, so a second read of the same dataset asks again. `built`
6. **A denial log quotes the engine's answer.** On a non-200 answer the gateway logs the address, the status, the user, the URN and up to 500 characters of the response body at ERROR. `built`
7. **The narrowing is read from the answer, under several spellings.** The gateway takes allowed columns from `allowed_columns` or `fields`, and row filters from `row_filters`, `row_filter` or `filters`. It passes both to the broker and adds them to the ticket when present. The broker's SQL minting turns them into column grants and a row policy. The path is `built`; [need to know, finding 2](../flows/need-to-know.md) found that no deployed policy produces a narrowing. `built`
8. **A second request body is built and never sent.** [The gateway record](central-gateway.md), finding 9, notes it. It is keyed by the token's user value rather than the resolved subject, so it would not ask the same question if it were sent. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `TOPAZ_AUTHORIZER_URL`, then `TOPAZ_URL` | an in-cluster service name | the engine's address, read by the gateway |
| `TOPAZ_AUTHORIZER_API_KEY` | empty | sent as a tenant header when set; the chart passes it through `envFrom` |
| request timeout | five seconds, fixed in code | how long the gateway waits for a decision |
| policy path and permission | fixed in code | the decision the gateway asks for |

## Gaps

- **The reader's bearer token reaches the engine** (finding 3). A token meant for the gateway is presented to a second service. [Roadmap row 12](../ROADMAP.md) (dag-tools #76) covers a mesh token sent to a database backend, not to the policy engine. Remedy: not on the roadmap. Status: `built`.
- **No test covers the non-200 denial or the tenant header** (findings 4 and 5). Remedy: not on the roadmap. Status: `built`.
- **The denial log carries part of the engine's response body** (finding 6). Remedy: not on the roadmap. Status: `built`.
- **The directory-check body is dead code** (finding 8). Remedy: not on the roadmap. Status: `built`.

## Not established from code

- Which policy a deployment loads into the engine, and so whether any decision narrows.
- Whether the engine a deployment runs returns the narrowing fields at the top level of its answer, where the gateway reads them.
- Whether the engine checks the bearer token or the tenant header it is sent.
- Where a deployment runs the engine and how the gateway reaches it.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-GATEWAY | is called by | HTTP | optional tenant key header, and the reader's own bearer token forwarded as presented | the dataset URN, the principal and a read permission in; allow or deny, and optionally allowed columns and row filters, back | `dag_tools/central_gateway/main.py:287-292`, `:308-322`, `:299-300`, `:405`; `test_gateway_verified_identity.py`: `test_enforce_refuses_and_never_calls_topaz` | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | decides whether a principal may read a dataset, and may narrow the read | — |
| How the mesh wires it | an external service the gateway calls once per read; not deployed by the chart | — |
| Sign-in | an optional tenant key from the environment; the caller's bearer token is forwarded | [NATO R7](../alignment/nato-dcra.md) |
| Data path | none; it sees the URN and the principal, never the data | — |
| Plane | `data-management`: a shared service that decides each dataset read; it is the policy decision point | [NATO L3](../alignment/nato-dcra.md), [NATO R2](../alignment/nato-dcra.md) |
| Security role | authorization of each read; the decision the gateway enforces | [NATO R6](../alignment/nato-dcra.md) |
| Services served | the access decision behind each exchange, and the narrowing a label would feed | [DoD S7](../alignment/dod-dmra.md), [DoD S12](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Gateway paths are under `dag_tools/central_gateway/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Address from two variables | `helm/dag-tools/templates/` (no Topaz template); `main.py:72-73` | not stated | — | `built` |
| No Topaz in the chart | `helm/dag-tools/templates/` (no Topaz template); `main.py:72-73` | not stated | — | `built` |
| One call per read; fixed path; identity stated by the gateway; empty principal denied | `main.py:238`, `:287-292`, `:308-322`, `:498` | not stated | `test_gateway_verified_identity.py`: `test_enforce_refuses_and_never_calls_topaz`; `test_gateway_one_resolution.py`: `test_a_refusal_from_the_one_resolution_reaches_neither_topaz_nor_the_broker`; `test_gateway_entitlement_claim_follows_env.py`: `test_missing_claim_still_fails_closed` | `built` |
| Caller's token forwarded | `main.py:405`, `:419`, `:295-298` | not stated | no test | `built` |
| Tenant header; chart `envFrom` | `main.py:299-300`; `helm/dag-tools/values.yaml:181-185` | not stated | no test | `built` |
| Failure denies; five seconds; new client per call | `main.py:339-346`, `:364-402` | not stated | no test of the non-200 denial | `built` |
| Denial log quotes the body | `main.py:364-384` | not stated | no test | `built` |
| Narrowing read and passed on | `main.py:349-363`, `:560-561`, `:598-601`; `dag_tools/domain_broker/sql_minting.py:165-170` | not stated | `test_sql_minting.py`: `test_ch_columns_narrow_when_topaz_narrows`, `test_pg_columns_narrow_when_topaz_narrows`; `test_ticket_receipt.py`: `test_the_receipt_names_the_narrowing_that_was_applied` | `built` |
| Unused directory-check body | `main.py:324-335` | not stated | — | `built` |
