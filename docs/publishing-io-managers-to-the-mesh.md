# Publishing an IO manager to the mesh

How an asset becomes readable through the domain broker, what stops it, and
how to find out which.

## The contract, in one sentence

The broker calls **`physical_coordinates(asset_key_path)`** on whatever object
sits in `Definitions(resources=...)` under the asset's `io_manager_key`, and
checks `hasattr`. That is the entire requirement.

**The protocol is duck-typed. Nothing requires a dag-tools class.** Any IO
manager from any package participates by growing that one method.

## Adopting it

```python
from dag_tools.io_managers import MeshPublishable

class MyIOManager(MeshPublishable, ConfigurableIOManagerFactory):
    def mesh_uri(self, asset_key_path):
        # The one thing only this manager knows: where the bytes actually went.
        return f"{self.uri_base.rstrip('/')}/{'/'.join(asset_key_path)}/"

    def mesh_endpoint(self):
        return self.fs.common.end_point      # FQDN — see below
```

`physical_coordinates` comes from the mixin. Optional hooks:
`mesh_source_type`, `mesh_endpoint`, `mesh_region`, `mesh_scope`, `mesh_extra`.

**There is no hook for credentials, and that is structural.** Per
[ADR-0044](https://github.com/edgy-solutions/invincible-agent) the *broker*
mints, per request, scoped to the asset and expiring with the access window. An
IO manager runs in a pipeline pod and knows neither the caller nor the window.
Anything a producer puts under `credentials` is dropped by the broker and
counted in `/health.adr0044.echoed_credentials_dropped`.

## Three things that look fine and are not

**1. `mesh_uri` must be where the WRITER wrote.** When something other than
this IO manager performs the write — a dlt pipeline with its own filesystem
destination, say — deriving the path from the asset key is a guess. It will be
wrong in a way nothing detects until a read returns nothing.

**2. A directory of part files needs the trailing slash.** `scan_parquet`
treats a slash-less path as an object key; the HEAD 404s against real object
storage and works fine on a local filesystem. Invisible until deployment.

**3. The endpoint must resolve from where the CONSUMER runs.** A bare
Kubernetes service name (`minio-svc:9000`) resolves only inside the producing
deployment's namespace, so a notebook or agent elsewhere cannot read the asset.
Use `minio-svc.<ns>.svc.cluster.local:9000`. The broker reports violations in
`/health.adr0044.non_fqdn_hosts`.

## Returning `None` is a first-class answer

An asset on the pod's local disk, or in a format no consumer can read, has no
location another process can use. Return `None` and it is not advertised.

**An advertised-but-unreadable location is worse than an unadvertised asset**,
because the gateway routes consumers to it with full confidence. The broker
enforces the same rule from its end: an asset with no physical identity is not
registered, and the reason is counted in `/health.adr0044.unadvertised`.

## Diagnosing a deployment that publishes nothing

```bash
kubectl cp scripts/diagnose_unadvertised_assets.py <ns>/<broker-pod>:/tmp/d.py
kubectl exec -n <ns> <broker-pod> -- python /tmp/d.py
kubectl exec -n <ns> <broker-pod> -- python /tmp/d.py <asset-name-substring>
```

It walks the broker's own preconditions per asset and names which one declined.

### The failure that motivated this document

A deployment reported `Registered 104 assets` and could serve none of them. The
symptom presented as a URN-naming puzzle — the broker advertised
`dataPlatform:dagster` names while consumers asked for `dataPlatform:s3` ones —
and the naming was a *consequence*, not the cause.

The cause: its IO managers were **vendored copies** of dag-tools'
(`orch.resources.arrow.ConfigurableArrowIOManager`), forked before
`physical_coordinates()` existed. Same class name, no shared ancestry, no
protocol. `physical_urn_for` therefore could not derive a physical identity and
fell back to `record.urn`, which forces `platform="dagster"` and a dotted name
layout.

**A `dagster`-platform URN is not a misspelling of the `s3` one.** It means the
asset has no physical location — and the ticket that resolved for it pointed at
`s3://default-bucket/warehouse/...`, a bucket that does not exist.

Two lessons worth keeping: **check the MODULE, not the class name** — a fork and
the original are indistinguishable in a stack trace — and a contract that exists
only as an implementation is one a copy can silently omit. This mixin exists so
the contract is stated once rather than demonstrated four times.
