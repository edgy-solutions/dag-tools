# Central gateway (CO-GATEWAY)

The central gateway is the mesh's one door for a dataset read. A consumer asks it to authorize a read of a URN. The gateway decides who the request is for, asks the policy engine whether that principal may read the URN, finds the domain broker that serves the URN, and returns the ticket the broker issues. It also takes the brokers' registrations. The read itself does not pass through it ([big data, finding 1](../flows/big-data.md)); the decision is followed in [need to know](../flows/need-to-know.md) and is not repeated here.

This record covers what the code and the dag-tools chart provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`), not at the flows' pin. The gateway changed between the two. The record shape is provisional.

## Findings

1. **One image, from this package alone.** CI builds it as one of the three services in the release fleet. The dag-tools chart deploys it by default: two replicas, one port, a readiness probe on `/ready` and a liveness probe on `/health`. The chart sets no environment for it, so every setting below is at its code default until a deployment sets it. `built`
2. **The identity settings are read per request, not at startup.** Posture, accepted issuers and audiences, the key source and the algorithms are re-read on each call. Four other settings are read once at import and fixed for the process: the cache address, the policy engine's address, the tenant key and the deny list. A changed setting takes effect when the process sees it in its environment, which for an environment variable is the next restart. `built`
3. **The identity posture defaults to `observe`.** Under `observe` the gateway takes the principal a request names in a header and counts what `enforce` would have refused. Under `enforce` it reads the principal from a verified token. An unrecognised posture value is read as `enforce` and logged at ERROR, so a typo tightens rather than loosens. `built`
4. **Under `enforce`, an empty issuer or audience list refuses everything.** There is no default issuer, audience or key. The empty list is checked before the signature, so the refusal says the gateway is unconfigured rather than calling the token invalid. Its message names the three kinds of configuration that may be missing (a key source, an accepted issuer, an accepted audience) and no variable; the variables are named only in the gateway's error log. The accepted algorithms default to RS256. `built`
5. **Delegation is not wired.** The seam that would let a listed service read on a user's behalf answers no for every caller and says so once at ERROR. Under `enforce`, a service token naming a user is therefore refused. This is the dependency of [roadmap row 1](../ROADMAP.md) (dag-tools #70). `built`
6. **The gateway checks at startup that it can verify a signed token.** It runs the check once. A negative verdict makes `/ready` answer 503 and is reported before the cache check. A check that has not run never gates readiness. `/health` answers 200 whatever the state, and reports the principal record, the refusal counts and the signature verdict. `built`
7. **The deny list is applied before the policy engine, and under `observe` it reads the header first.** A listed subject gets a 403. Under `enforce` the verified subject is always checked, and the header subject is added only for a delegated request, so under `enforce` a header can add a refusal but never remove one. Under `observe`, the subject checked is the header subject when one is sent, and the token's unverified subject only otherwise. The deny list is empty by default. Tests cover the `enforce` cases only. `built`
8. **The policy check is one call, and any failure denies.** The gateway posts the URN, the principal and a read permission to the policy engine's decision route, with a five-second timeout. It reads the allowed columns and row filters from the answer. A non-200 answer or an exception denies. The engine's address has a code default naming an in-cluster service. The caller's own bearer token is forwarded to the engine. An optional tenant key is sent as a header when set. `built`
9. **The code builds a second policy check and never sends it.** A relationship-check payload is assembled in the same function and not used. `built` (unused)
10. **Routes come from the brokers, through a cache, for a fixed time.** A broker registers its URNs. The gateway stores each URN against the broker's address in the cache with a five-minute expiry, which is not a setting. A URN with no live entry gets a 404. The broker's default re-register interval is two minutes, so a route outlives two missed registrations; a broker set to register less often than every five minutes loses its routes in between. The cache address has a code default naming a local instance. `built`
11. **The broker call carries the decision and a service credential.** The gateway posts the URN, the allowed columns, the row filters and the subject to the broker's resolve route, with a ten-second timeout. A broker 404 becomes a 404; any other failure becomes a 502. Each issued ticket is logged with its id, subject, URN and version. `built`
12. **Both internal routes sit behind a service door, with its own posture.** The broker's resolve route and the gateway's register route each check the calling service against a list for that route. The door's posture defaults to `observe` and is set separately from the identity posture. Under `observe`, a caller the door would refuse, including one with no token, is logged as `would_refuse` and served. The chart does not set the door's posture, so the default applies. Under `enforce`, an empty list refuses every caller and a door with no verifier configured answers 503. On the calling side, a service token is fetched by client credentials and refreshed thirty seconds before it expires. With no credentials configured, the caller sends no token rather than failing. `built`
13. **A startup warning about two retired flags is out of date.** When either retired flag is set, the gateway warns that it is ignored and that no verification is enforced. Verification can now be enforced, by the identity posture (finding 3). `built`

## Settings and defaults

The chart's `centralGateway.env` is empty, so the chart column reads "not set" unless it says otherwise.

| Setting | Code default | Chart | What it does |
|---|---|---|---|
| `GATEWAY_IDENTITY_POSTURE` | `observe` | not set | `observe` or `enforce`; anything else is `enforce` |
| `GATEWAY_JWT_ISSUERS` (or the singular form) | none | not set; documented in comments | accepted issuers, comma-separated |
| `GATEWAY_JWT_AUDIENCES` (or the singular form) | none | not set; documented in comments | accepted audiences, comma-separated |
| `GATEWAY_JWKS_URL`, `GATEWAY_JWT_PUBLIC_KEY`, or a legacy public-key variable | none | not set; documented in comments | where the verification key comes from |
| `GATEWAY_JWT_ALGORITHMS` | RS256 | not set | accepted signing algorithms |
| `USER_ENTITLEMENT_CLAIM` | `email` | not set | the token claim read as the user |
| `DENIED_USER_SUBS` | empty | not set | subjects refused before the policy check |
| `TOPAZ_AUTHORIZER_URL`, then `TOPAZ_URL` | an in-cluster service name | not set | the policy engine |
| `TOPAZ_AUTHORIZER_API_KEY` | empty | not set | sent as a tenant header when set |
| `REDIS_URL` | a local instance | not set | the route cache |
| `INTERNAL_DOOR_POSTURE` | `observe` | not set | the service door's posture |
| `BROKER_RESOLVE_CALLERS`, `GATEWAY_REGISTER_CALLERS` | empty | not set | the services each internal route admits |
| `MESH_SERVICE_TOKEN_URL`, `_CLIENT_ID`, `_CLIENT_SECRET` | none | not set | the calling side's service credential |
| `GATEWAY_SUBJECT_GAUGE`, `GATEWAY_GAUGE_LOG_AUTOCONFIG` | posture source only; `1` | not set | the subject gauge and its logging |
| `centralGateway.enabled`, `replicas`, `port` | — | on, 2, one port | the deployment |
| `centralGateway.caCerts` | — | off | a private certificate authority, merged into both trust stores |
| `centralGateway.resources` | — | small requests and limits | the pod's share of a node |
| `centralGateway.ingress` | — | off | exposure outside the cluster |

## Gaps

- **The principal is taken from a header by default** (findings 3, 7). This includes the deny list, which under `observe` reads the header subject first. Remedy: [roadmap row 1](../ROADMAP.md) (dag-tools #70). Status: `built`.
- **Delegation is not wired** (finding 5). It is the dependency of roadmap row 1. Status: `built`.
- **Two things in the code describe a gateway that no longer exists** (findings 9, 13): an unsent check payload and a warning that says no verification is enforced. Remedy: [roadmap row 11](../ROADMAP.md) (not filed). Status: `built`.
- **By default the internal routes admit any caller** (finding 12). Under `observe`, which the chart leaves in place, a caller with no token can register URNs to an address of its choosing at the gateway, or call the broker's resolve route, and is only logged. Remedy: [roadmap row 16](../ROADMAP.md) (not filed). Status: `built`.
- **No test covers the deny list under `observe`** (finding 7). Roadmap row 1 would retire the posture it applies under. Status: `built`.

## Not established from code

- Which policy a deployment loads into the policy engine, and so whether any decision narrows. [Need to know, finding 2](../flows/need-to-know.md) found that no deployed policy produces narrowing; that is not re-read here.
- How many gateway replicas a deployment runs, and whether they share one cache. The chart's default is two replicas and one cache address.
- Whether any deployment sets a posture other than the default. A deployment's settings are not described here.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-TOPAZ | calls | HTTP | the caller's bearer token forwarded; an optional tenant header | the dataset URN, the principal and a read permission; back come allowed columns and row filters | `dag_tools/central_gateway/main.py:72-73`, `:238-402`, `:295-300`, `:346`; no test | `built` |
| CO-BROKER | calls | HTTP | client-credentials bearer token for the service door | the URN, allowed columns, row filters and subject; back comes a ticket | `dag_tools/central_gateway/main.py:404-606`; `dag_tools/central_gateway/internal_door.py:143`; `test_service_headers_refetches_30s_before_expiry` | `built` |
| CO-BROKER | is called by | HTTP | service-door caller list, posture observe by default | the broker's address and the URNs it serves | `dag_tools/central_gateway/internal_door.py:50-72`, `:98`; `dag_tools/central_gateway/main.py:214-236`; `test_observe_serves_and_logs_would_refuse` | `built` |
| CO-CLIENT | is called by | HTTP | bearer token verified by the gateway under enforce; the principal a header names under observe, the default | a dataset URN in; a ticket out | `dag_tools/central_gateway/identity.py:179-196`; `dag_tools/central_gateway/main.py:468-494`; `test_posture_parse` | `built` |
| XE-ROUTE-CACHE | writes | in-process | not established | each URN against the broker's address | `dag_tools/central_gateway/main.py:63`, `:214-236`, `:506`; no test | `built` |
| XE-ROUTE-CACHE | reads | in-process | not established | the broker address for a URN | `dag_tools/central_gateway/main.py:63`, `:214-236`, `:506`; no test | `built` |
| CO-KEYCLOAK | calls | HTTP | none | the keys that verify a signed token | `dag_tools/central_gateway/identity.py:212`, `:256-257`, `:301-315`; no test | `built` |

## Place in the mesh

| Aspect | What the code and chart show | Alignment row |
|---|---|---|
| Purpose | the one door for a dataset read: decide who is asking, ask the policy engine, route to the broker | — |
| How the mesh wires it | the dag-tools chart deploys it; brokers register their URNs to it; the mesh data client calls its authorize route | — |
| Sign-in | under `enforce`, a token from the identity provider, verified by a key set or a key, with issuer and audience checked; under `observe`, the default, the principal a header names; the internal routes have their own service door | [NATO R7](../alignment/nato-dcra.md) |
| Data path | none: it carries the decision and the ticket, never the data | [DoD S7](../alignment/dod-dmra.md) |
| Plane | `data-management`: access to the shared services goes through its API | [NATO L3](../alignment/nato-dcra.md) |
| Security role | enforcement point: it refuses before any data path is resolved | [NATO R1](../alignment/nato-dcra.md) |
| Services served | data exchange; policy access control, by enforcing the engine's answer; performance analytics, by its refusal counts; instrumentation, by `/health` and `/ready` | [DoD S7, S5, S10, S15](../alignment/dod-dmra.md) |

## Sources

All dag-tools paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Gateway source paths are under `dag_tools/central_gateway/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Built as one fleet service; chart deploys it with two replicas and both probes | `.github/workflows/build-containers.yml:20`; `helm/dag-tools/values.yaml:173-215`; `helm/dag-tools/templates/central-gateway.yaml:65-73` | dag-tools AGENTS.md §8 | `test_image_release_retag.py`: `test_every_built_service_is_retagged` | `built` |
| Identity settings read per call; four others read at import | `identity.py:1-60`; `main.py:63`, `:72-73`, `:78-81` | not stated | — | `built` |
| Posture default `observe`; unknown is `enforce` at ERROR | `identity.py:179-196` | not stated | `test_gateway_verified_identity.py`: `test_posture_parse`, `test_an_unknown_posture_is_enforce_and_logged_at_error` | `built` |
| No default issuer, audience or key; empty refuses; RS256 default | `identity.py:208-236`, `:239`, `:264`; the message `:134-137`, sent at `main.py:144`; the variables logged at `identity.py:335` | not stated | `test_gateway_accepted_issuers_and_audiences.py`: `test_the_empty_list_is_checked_before_the_signature_so_the_reason_is_actionable` | `built` |
| Delegation seam answers no | `identity.py:271-286` | not stated | `test_the_default_seam_is_false_and_says_once_that_the_rail_is_not_wired` | `built` |
| Signature self-check once at startup; gates `/ready` only when negative; `/health` always 200 | `identity.py:885-975`; `main.py:92-112`, `:148-211` | not stated | `test_gateway_signing.py`: `test_the_check_reports_every_problem_at_once`, `test_the_extra_is_what_carries_signature_support`; `test_gateway_startup_readiness.py`: `test_a_gateway_that_cannot_verify_a_signed_token_is_not_ready`, `test_partial_support_is_not_ready_and_names_the_algorithm`, `test_the_signature_fault_is_reported_before_redis`, `test_health_still_answers_200_when_the_gateway_is_not_ready` | `built` |
| Deny list before the policy check; header first under `observe` | `main.py:78-81`, `:468-494` | not stated | `test_gateway_verified_identity.py`: `test_enforce_deny_list_uses_the_verified_sub_not_the_originator_sub` and the four numbered deny tests, all under `enforce`; none under `observe` | `built` |
| Policy check: one call, five-second timeout, failure denies; the caller's token forwarded | `main.py:72-73`, `:238-402`, `:295-300`, `:346` | not stated | — | `built` |
| Unsent relationship-check payload | `main.py:325` | not stated | — | `built` (unused) |
| Route cache with a fixed five-minute expiry | `main.py:63`, `:214-236`, `:506` | not stated | — | `built` |
| Broker call, ten-second timeout, error mapping, issued-ticket log line | `main.py:404-606`, `:568`, `:579-591` | dag-tools AGENTS.md §4o | `test_ticket_receipt.py`: `test_the_log_follows_the_VERIFIED_subject_where_it_differs_from_the_header`, `test_the_log_line_names_no_credential` | `built` |
| Service door on both internal routes; own posture; client-credentials caller | `internal_door.py:17-21`, `:50-72`, `:98`, `:101-131`, `:143`; `helm/dag-tools/values.yaml:173-215` and `helm/dag-tools/templates/central-gateway.yaml` set no door posture | not stated | `test_internal_doors.py`: `test_observe_serves_and_logs_would_refuse`, `test_enforce_with_no_list_refuses_everyone`, `test_a_list_for_one_door_does_not_open_the_other`, `test_enforce_unconfigured_verifier_is_503`, `test_the_posture_is_independent_of_the_identity_posture`, `test_service_headers_refetches_30s_before_expiry` | `built` |
| Retired-flag warning says no verification is enforced | `subject_gauge.py:310-316` | not stated | — | `built` |
| Remedies | [the roadmap note](../ROADMAP.md), rows 1, 11 and 16 | none | — | `proposed` |
