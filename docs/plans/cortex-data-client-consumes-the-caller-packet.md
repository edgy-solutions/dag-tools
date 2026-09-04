---
id:         cortex-client-consumes-caller
status:     closed
owner:      agent
blocked-on:
closed-by:  a34c77d — four rungs landed; rung 2 terminal inside a request; acceptance 3 replaced by a bite-check verified failing on reversal; raise_for_status no longer the interpreter. CI dispatched explicitly (workflow_dispatch), headSha matched local HEAD, four jobs green. LANDED ON master, NOT RELEASED — see below.
repo:       dag-tools
summary:    CortexDataClient consumes the verified caller the SDK hands a handler. caller= -> request context -> CORTEX_USER_TOKEN -> service; inside a request, failing to resolve raises. Rung 4's opt-in refined in place: provisioning a transport credential IS the opt-in, because rung 2's terminality is the guard.
---

> ## RELEASED IN 0.6.0
>
> Landed on `master` at `a34c77d`; shipped in **`edgy-dag-tools==0.6.0`**, cut alongside the
> otel_api_sync helpers.
>
> **Consumers do not get this by upgrading alone.** The fleet pins `dag-tools`, so a deployment
> reads as the service until its pin moves to 0.6.0 or later. The defect is closed in the repo
> and in the index; it is closed **in a given deployment only when that deployment's pin moves.**
>
> Two things a consumer bump brings with it, neither of which is a break in normal use:
>
> * a bare `CortexDataClient()` **inside a mesh request** now reads as the request's caller
>   rather than as the service — which is the fix, and changes what rows come back;
> * inside a request whose caller did not resolve, construction now **raises**
>   `CallerUnresolved` rather than returning a client that reads as the service. Under the
>   OBSERVE posture an unauthenticated caller yields exactly that, so this is the ordinary
>   case, not an exotic one.
>
> Outside a request nothing changes: provisioning a transport credential is the opt-in, and
> every notebook, CLI and Dagster asset behaves as before.

# `CortexDataClient` consumes the caller — the SDK identity fix's other half

**The gap in one sentence.** The SDK hands a handler a verified caller, but nothing stops the
author from dropping it one line later, and if they do the read silently runs as the service.

```python
@app.execute()
def detect(data: In, caller: CallerIdentity) -> Out:
    client = CortexDataClient()                                            # compiles, runs,
                                                                           # returns rows,
                                                                           # reads as the SERVICE
    client = CortexDataClient(originator_email=caller.require_authz_id())  # correct today
```

Both lines compile. Both return rows. Only one is right, and **the wrong one has no symptom.**

This is the MeshTool defect's shape moved one package over: the identity arrives and drops, and
the drop is silent. `require_authz_id()` is loud — but only if you call it. Bare construction
stays quiet.

**State as of this writing.** `dag_tools/cortex_data/client.py`, last touched for identity in
`61cbfa9` (*"thread the ORIGINATING USER's email to the email-keyed gate"*, 2026-07-10). The
constructor takes `broker_url, jwt_token, client_id, client_secret, keycloak_url, originator_sub,
originator_email`. There is no `caller=`, no contextvar read, no `CORTEX_USER_TOKEN`, and no
opt-in service identity. Verified, not assumed.

---

## 1. The resolution order — the ruling

```
caller=  →  contextvar  →  CORTEX_USER_TOKEN  →  service identity (opt-in only)
```

Inside a request, failure to resolve **raises**.

Four changes to `CortexDataClient.__init__`:

1. **Accept `caller=`.** An explicit override, wins over everything. A `CallerIdentity`, or
   whatever narrower shape the implementer prefers — the packet does not mint that type here.
2. **Read the SDK's request-scoped contextvar when no `caller=` is passed.** This is the change
   that makes a bare `CortexDataClient()` *correct* inside an agent handler rather than wrong.
3. **Add the `CORTEX_USER_TOKEN` rung below the contextvar.**
4. **Make the service identity opt-in and loud.** Bare construction inside a request must raise,
   never fall through to rungs 3 or 4.

### Why the ordering is the ruling, not a detail

**Reversed, a config change on a pod becomes a silent cross-tenant read with no code to review.**
`CORTEX_USER_TOKEN` set on an agent pod is a values edit; it passes no code review, appears in no
diff a reviewer reads for authorization, and — if it outranked the request's caller — would make
every user of that agent read as whoever that token names. The caller outranking it is what keeps
that variable harmless.

**Rung 4 being loud is what closes the gap the SDK left.** Being inside a handler is *precisely*
when reading as the service is wrong. A bare construction there should refuse, not fall through.
Outside a request — a notebook, a Dagster asset, a CLI — the service identity is the ordinary and
correct answer, so the refusal must be scoped to request context rather than global.

### This is implementable today with no new SDK API

The SDK already draws the distinction rung 4 depends on. `iagent_mesh.current_caller()` returns:

| Return | Means | Rung 4 behaviour |
| --- | --- | --- |
| `None` | **no request in scope** | fall through to rungs 3 / 4 |
| `CallerIdentity(authz_id=None, …)` | a request whose caller **did not resolve** | **raise** |
| `CallerIdentity(authz_id="…")` | resolved | use it |

From `transport_auth.py`, verbatim: *"`None` means NO REQUEST — never 'a request by nobody'. […]
Collapsing them is what lets an agent-pod read fall through to a notebook-shaped env fallback."*

So "inside a request" is detectable as `current_caller() is not None`. The implementer does not
need to invent a request-scope signal, and must not collapse these two states.

---

## 2. Acceptance 3 is vacuously true — the sharper finding

The SDK packet's acceptance 3 reads *"`CORTEX_USER_TOKEN` on an agent pod changes nothing."*

**It holds only because nothing reads that variable.** The designed property — the caller
outranks it — does not exist. The test is green because of an **absence**, not a **precedence**.

Two consequences:

- It certifies nothing about the property it names.
- **It breaks the moment someone adds rung 3** — which this packet does. A green that flips red
  when the feature it describes is implemented is worse than no test, because it will read as a
  regression introduced by this work rather than as a check that was never measuring anything.

**Required: a bite-check.** Set `CORTEX_USER_TOKEN` *and* provide a caller, then assert the
**caller won**. That test must be verified to fail with the rungs reversed — a passing assertion
that has never been seen failing is decoration, and this one has already fooled us once.

The absence-shaped assertion may stay as a second test, but it must be renamed to say what it
actually checks, so it is not read as covering precedence.

---

## 3. The coupling ruling — soft import, loud fallback

`dag-tools` has **no dependency on `iagent_mesh`** (verified in `pyproject.toml`). Step 2 creates
that edge. The ruling:

> **Soft `try: import iagent_mesh` with a documented fallback.** Not a hard edge, not a third
> package.

**Why not a hard dependency.** The SDK's templates already import `dag_tools`. A hard dependency
the other way is a cycle-in-waiting — not a cycle at package level today, but one a resolver will
find the day someone adds one more import. Discovering that in a resolver error, mid-upgrade, is
strictly worse than deciding it here.

**Why not a shared contextvar micro-package.** It is the principled answer and it is a third repo
with its own release cadence and its own silent-CI risk, for one variable. Too much machinery for
what it protects.

**The fallback must be loud, and that is the condition on this ruling.** When the SDK is absent,
rung 2 is skipped — and the code must **say so at construction**, so a `CortexDataClient` running
outside the mesh knows it has no request context rather than silently reading a variable that
isn't set. Seal that loudness the same way rung 4's is sealed: with a test that removes the SDK
and asserts the statement is made.

Document `iagent_mesh` as an **optional peer**, and state the version floor the contextvar
contract requires (`current_caller` is exported from `iagent_mesh` as of 0.4.0).

---

## 4. Rider from ADR-0049 — the failure modes must stay distinct

These reads will be **inner calls in compositions**. A composing verb one level up cannot report
honestly over a client that collapses its failures. The client must distinguish, as separate and
catchable outcomes:

| Outcome | Cause | What the composer must be able to say |
| --- | --- | --- |
| **unresolved** | no caller in scope, or unresolved inside a request (rung 4's raise) | *"this composition has no identity to read as"* — a defect in the call, not the data |
| **unentitled** | caller resolved, broker refused (403) | *"this caller may not see that"* — an authorization outcome, reportable as such |
| **unavailable** | broker down, gateway 502, timeout | *"could not reach it"* — retryable, says nothing about entitlement |

**A fourth shape needs an explicit ruling, because it is currently ambiguous.** The gateway
answers `404 No active domain broker found` when no broker holds a live route for the URN. That is
a **liveness** signal — routes carry a TTL and are re-pushed on a heartbeat — **not** a statement
that the asset does not exist. It belongs under **unavailable**. Today it is indistinguishable
from a genuine absence, and a composer reporting *"no such data"* when the truth is *"the owning
deployment is down"* is exactly the dishonest report this rider exists to prevent.

Today all four collapse into `httpx.HTTPStatusError` via `raise_for_status()`.

---

## 5. Acceptance criteria

Each must be a test that has been **verified to fail** without the change.

1. Bare `CortexDataClient()` inside a request with a resolved caller reads **as that caller** —
   the same rows as an explicit `caller=`.
2. Bare `CortexDataClient()` inside a request whose caller did **not** resolve **raises**, and
   does not read as the service.
3. `caller=` beats a contextvar caller.
4. **The bite-check.** With `CORTEX_USER_TOKEN` set *and* a caller in scope, the **caller** is
   used. Verified to fail with the rungs reversed.
5. Outside a request (`current_caller() is None`), `CORTEX_USER_TOKEN` is honoured.
6. Outside a request with nothing else set, the service identity is used **only** when explicitly
   opted in; without the opt-in it raises.
7. With `iagent_mesh` absent, construction states that rung 2 is unavailable.
8. The three failure modes are separately catchable, and a gateway 404 is classified
   **unavailable**, not absence.

### What "opt-in" resolved to, as implemented

One reading had to be made and is recorded here rather than left in the code.

**Provisioning the process with a transport credential — by argument or by environment — IS the
opt-in.** Requiring a further code flag would have broken every notebook, CLI and Dagster asset
that reads with credentials an operator deliberately gave it, and would have added no safety:
**the dangerous case never reaches rung 4.** Inside a request, rung 2 is terminal and raises. The
guard is that terminality, not a flag on the rung below it.

`service_identity=True` remains as an explicit statement of intent for a process with no
credential of its own.

Corollary on ordering: a process with *nothing* provisioned has a **transport** problem, not an
identity one. The credential error speaks first, because "no caller resolved" would send an
operator hunting in the wrong place.

**Non-goals.** Minting a caller type in `dag-tools`; changing the gateway or broker; changing the
SDK; the row-level entitlement semantics behind the broker's decision.

---

## 6. Verification discipline — dag-tools CI is silent

A red build here notifies nobody. Before any claim that this landed:

- run the workflow **explicitly** (`gh workflow run`, then watch the run to completion — do not
  infer from a push);
- verify the published artifact by **digest**, not by the workflow going green;
- if a release is cut, install from the index and confirm the code is present in the installed
  package, not merely in the tag.

The standing precedent for why: a green certifying an absence rather than a precedence is exactly
what acceptance 3 already was. The same failure at the CI layer is a workflow that ran on a
different commit than the one being claimed.
