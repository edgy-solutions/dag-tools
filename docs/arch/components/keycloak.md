# Keycloak (CO-KEYCLOAK)

Keycloak is the identity provider the mesh code expects. It meets this repo in two places. The [central gateway](central-gateway.md) verifies the tokens it issues. The [mesh data client](mesh-data-client.md) can obtain a service token from it by a client-credentials grant. The dag-tools chart does not deploy Keycloak.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **The chart deploys no identity provider.** No template in the dag-tools chart creates one. The chart's values document the gateway's verification settings in comments and set none of them. `not built`
2. **The gateway names no issuer, audience or key of its own.** Accepted issuers, accepted audiences and the key source come only from the environment. An empty list refuses every token. The refusal sent to the caller names the three kinds of configuration that may be missing (a key source, an accepted issuer, an accepted audience) and no variable; the variables are named only in the gateway's error log. A key set address is preferred over a static key. The accepted algorithms default to RS256. The reasons are in AGENTS.md §4o-1 and in [the gateway record](central-gateway.md). `built`
3. **Verification checks signature, audience and issuer, in that order, and requires three claims.** A token must carry an expiry, an issuer and an audience. A token signed correctly by an issuer the gateway does not accept is refused for that reason, not as an invalid token. `built`
4. **The key set is fetched with a three-second limit, cached, and a failed fetch is not remembered.** The key set client keeps the keys it has fetched and is rebuilt only when the address changes. If the fetch fails, that request gets a 503 and the next request tries again. `built`
5. **Under the default posture, a header decides the principal before a verified token does.** The token is verified under both postures; the posture decides only whether the verified answer is the principal. Under `enforce` it is. Under `observe`, the default, the gateway takes the principal from a header when one names it; with no header, from the entitlement claim of a token it verified; with neither, it refuses for naming no principal. Under `observe` the gateway also logs what `enforce` would have decided, which it can do because verification has already run. [Roadmap row 1](../ROADMAP.md) (dag-tools #70) would make `enforce` the default. `built`
6. **The internal service doors use the same verification, under a posture of their own that defaults to `observe`.** The broker's resolve route and the gateway's register route verify a service's token with the same code as the read path, require a service claim, and require that service to be listed for that door. The doors' posture, `INTERNAL_DOOR_POSTURE`, is separate from the read path's and moves independently of it. Under `enforce` a request that fails any check is refused. Under `observe`, the default, it is logged as one the door would refuse, and served. The chart's values mention the doors' posture only in a comment and do not mention the caller lists. `built`
7. **The mesh data client fetches one service token and never refreshes it.** When no token is handed in and no development token is set, the client exchanges a client id and secret for a token at construction, with a ten-second limit. It keeps that token for its lifetime and does not fetch another when it expires. The caller side of the internal doors, in the gateway package, does refresh: it caches its service token, fetches a new one 30 seconds before expiry, and on a failed fetch sends no token rather than raising. A development token from the environment wins over the service credentials. The token address has a code default naming an in-cluster service. `built`
8. **The read-only IO manager builds a new client for every input it loads.** The IO manager's client id and secret are required. Each load constructs a client. Unless a development token is set, that client fetches a token, so each load is one grant. Writing through it always raises. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `GATEWAY_JWT_ISSUERS` (or the singular form) | none | accepted issuers; empty refuses everything |
| `GATEWAY_JWT_AUDIENCES` (or the singular form) | none | accepted audiences; empty refuses everything |
| `GATEWAY_JWKS_URL`, else `GATEWAY_JWT_PUBLIC_KEY`, else a legacy public-key variable | none | where the gateway gets its verification key |
| `GATEWAY_JWT_ALGORITHMS` | `RS256` | accepted signing algorithms |
| `GATEWAY_IDENTITY_POSTURE` | `observe` | whether the verified token decides the principal on the read path |
| `INTERNAL_DOOR_POSTURE` | `observe` | whether the internal doors refuse or only log; separate from the read path's posture |
| `BROKER_RESOLVE_CALLERS`, `GATEWAY_REGISTER_CALLERS` | none | the services each internal door admits |
| `CORTEX_CLIENT_ID`, `CORTEX_CLIENT_SECRET` (or arguments) | none | the client's service credentials |
| `KEYCLOAK_TOKEN_URL` (or an argument) | an in-cluster token address | where the client exchanges them |
| `MESH_DEV_TOKEN` | none | a development token that wins over the service credentials |

## Gaps

- **Under the default posture, a header outranks the verified token** (finding 5). A header naming a principal decides it, whatever the token proves; the verified token decides only when no header is sent. Remedy: [roadmap row 1](../ROADMAP.md) (dag-tools #70).
- **A long-lived client keeps an expired token** (finding 7). After the token expires, every read whose principal depends on that token fails until the client is rebuilt; under `observe`, a read whose header names the principal does not depend on it. Remedy: not on the roadmap. Refresh before expiry is `built` on the internal doors' caller side (finding 7).
- **A token request on every IO manager load** (finding 8). Unless a development token is set, the identity provider receives one grant per input loaded. Remedy: not on the roadmap.

## Not established from code

- Which identity provider a deployment runs, and which issuers, audiences and key source it sets.
- How long a deployment's service tokens live, and so how soon a long-lived client stops working.
- Which claim a deployment's tokens carry for the entitlement key.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-GATEWAY | is called by | HTTP | none | the signing key set, fetched with a three-second limit and cached, only when a key set address is configured | `dag_tools/central_gateway/identity.py:296-325`, `:417-427`; no test | `built` |
| CO-CLIENT | is called by | HTTP | client id and secret exchanged by a client-credentials grant | a service token, fetched once at construction and never refreshed | `dag_tools/cortex_data/client.py:95-112`, `:146-159`; `test_cortex_client_documented_contract.py`: `test_dev_token_wins_over_m2m` | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | issues the tokens the gateway verifies and the client presents | — |
| How the mesh wires it | an external service; the gateway reads its key set and the client posts to its token address; not deployed by the chart | — |
| Sign-in | it is the sign-in: user tokens and client-credentials service tokens | [NATO R7](../alignment/nato-dcra.md) |
| Data path | none; it issues and publishes keys, and sees no data | — |
| Plane | identity: the authority behind each principal | [NATO R9](../alignment/nato-dcra.md) |
| Security role | authentication of people and services; tokens are verified under both postures and decide the principal only under `enforce`; the read path and the internal doors each have their own posture, both `observe` by default | [NATO R5](../alignment/nato-dcra.md), [NATO R7](../alignment/nato-dcra.md) |
| Services served | the identity every governed exchange is decided for | [DoD S7](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No identity provider in the chart; settings documented only | `helm/dag-tools/templates/`; `helm/dag-tools/values.yaml:186-216` | not stated | — | `not built` |
| No compiled issuer, audience or key; empty refuses; key set preferred; RS256 | `dag_tools/central_gateway/identity.py:208-236`, `:257`, `:264-265`, `:417-421`; the refusal message `:134-137`, sent at `dag_tools/central_gateway/main.py:144`; the variables logged at `identity.py:335` | AGENTS.md §4o-1 | `test_gateway_accepted_issuers_and_audiences.py`: `test_no_issuer_and_no_audience_is_compiled_in`, `test_the_library_writes_down_no_issuer_and_no_audience_value`, `test_an_empty_list_refuses_everything`, `test_a_jwks_url_is_a_key_source_so_it_clears_that_blocker` | `built` |
| Order of checks; three required claims | `dag_tools/central_gateway/identity.py:399-445` | not stated | `test_gateway_accepted_issuers_and_audiences.py`: `test_an_unaccepted_issuer_is_refused_for_THAT_reason_not_as_an_invalid_token`, `test_a_token_naming_another_audience_is_refused` | `built` |
| Key set: three seconds, cached | `dag_tools/central_gateway/identity.py:91`, `:296-325` | not stated | no test | `built` |
| Key set: a failed fetch is not remembered | `dag_tools/central_gateway/identity.py:422-427` | not stated | `test_gateway_verified_identity.py`: `test_jwks_unreachable_refuses_then_recovers_without_latching`; `test_internal_doors.py`: `test_enforce_jwks_outage_is_503_for_that_request_only_no_latch` | `built` |
| Posture default `observe`; verified under both; the `observe` order | `dag_tools/central_gateway/identity.py:186-198`, `:540-554`, `:589-609` | not stated | `test_gateway_verified_identity.py`: `test_posture_parse` | `built` |
| Internal doors share verification; own posture; caller lists | `dag_tools/central_gateway/internal_door.py:50`, `:55-56`, `:75-81`, `:101-131`; `helm/dag-tools/values.yaml:212` | AGENTS.md §4 | `test_internal_doors.py` (22 tests), including `test_the_posture_is_independent_of_the_identity_posture` | `built` |
| Door caller side refreshes before expiry; never raises | `dag_tools/central_gateway/internal_door.py:71`, `:143-181` | not stated | `test_internal_doors.py`: `test_service_headers_refetches_30s_before_expiry`, `test_service_headers_fetch_failure_is_empty_logs_the_type_and_retries` | `built` |
| One service token, no refresh; development token wins and skips the grant | `dag_tools/cortex_data/client.py:95-112`, `:146-159`, `:190` | not stated | `test_cortex_client_documented_contract.py`: `test_missing_auth_fails_at_construction_not_at_read`, `test_dev_token_wins_over_m2m`; no test of expiry | `built` |
| A client per IO manager load; writes raise | `dag_tools/io_managers/cortex_io_manager.py:111-117`, `:147-153`, `:155-175` | not stated | no test of the per-load grant | `built` |
