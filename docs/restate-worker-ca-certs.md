# Trusting a private or self-signed TLS endpoint

Every outbound call the Restate worker makes goes over TLS against whatever
the deployment points it at: the API endpoints `api_sync`, `api_call_plan`
and `otel_api_sync` POST to, the SAP OData host, and `RESTATE_ADMIN_URL`
itself if Restate is fronted by TLS. If any of those presents a certificate
signed by an internal CA, the call fails certificate verification and there
was, until now, **no way to fix it from the chart** — the worker Deployment
had no `volumes`, no `volumeMounts`, and no trust-store environment
variables anywhere in it.

```yaml
restateWorker:
  caCerts:
    enabled: true
    secretName: corp-ca          # a Secret holding one or more PEM files
```

`centralGateway` takes the identical block. Both are off by default and add
nothing to the podspec when off.

---

## The three source forms

Set **exactly one**. The chart refuses to render with none or with two,
naming the component — mounting an empty directory, or silently dropping
the second source while the operator debugs against the wrong cert, are
both worse than a failed `helm upgrade`.

**An existing Secret** — for a cert managed outside the chart:

```yaml
restateWorker:
  caCerts:
    enabled: true
    secretName: corp-ca
    items: []          # optional: restrict to specific keys
```

**An existing ConfigMap** — same shape, `configMapName:`. Often the better
fit, since a root CA is not secret material.

**Inline** — the common case, one corporate root:

```yaml
restateWorker:
  caCerts:
    enabled: true
    inline:
      corp-root.crt: |
        -----BEGIN CERTIFICATE-----
        MIIC1zCCAb+gAwIBAgIU...
        -----END CERTIFICATE-----
```

The chart renders that into its own ConfigMap. A root CA certificate is
public material — it is what the endpoint already presents to every client
that connects — so this is a ConfigMap and putting it in a values file is
not a leak. A private **key** is a different thing and has no business
here; the key stays on the server.

Key names inside the source do not matter. Every regular file in the mount
is concatenated, so a Secret with `ca.crt` and another with `root.pem` both
work, as does one holding several.

---

## What it actually does, and why it is not one line

Turning it on mounts your certs read-only, runs an init container that
**concatenates them with the image's public roots** onto an `emptyDir`, and
points `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE` and `CURL_CA_BUNDLE` at the
merged file.

Both halves of that are load-bearing, and both were measured rather than
assumed. The measurements are pinned in
[`dag_tools_tests/test_chart_ca_certs.py`](../dag_tools_tests/test_chart_ca_certs.py).

### Why two environment variables

The handlers use **both** `requests` and `httpx`, and the two libraries
disagree about which variable names a CA bundle:

| env var | `requests` | `httpx` |
| --- | --- | --- |
| *(neither)* | rejected | rejected |
| `REQUESTS_CA_BUNDLE` | **trusted** | rejected |
| `SSL_CERT_FILE` | rejected | **trusted** |
| both | trusted | trusted |

Pick one and half the call sites still fail. `SSL_CERT_FILE` alone leaves
`api_call_plan` and `api_sync` rejecting the endpoint; `REQUESTS_CA_BUNDLE`
alone leaves the SAP client *and the worker's own Restate registration*
failing — and a worker that cannot register serves nothing at all, which
looks like a deployment problem rather than a certificate one.

`CURL_CA_BUNDLE` is set to the same path as a third: `requests` falls back
to it, and it makes `kubectl exec … curl https://endpoint` behave the way
the handler does, which is how you confirm a cert without a redeploy.

### Why the merge

Those variables **replace** the default trust store; they do not extend it.
Pointing them at a private CA alone makes every public endpoint fail:

| bundle | the private endpoint | a public endpoint |
| --- | --- | --- |
| private CA alone | trusted | **rejected** |
| merged | trusted | trusted |

That failure is nasty in practice because it lands somewhere else — a
DataHub push or a public API call starts failing after a change that was
only ever about an internal host. So the init container concatenates
`/etc/ssl/certs/ca-certificates.crt`, certifi's bundle (what `requests` and
`httpx` use by default, so that enabling this can never *narrow* trust) and
your certs, in that order.

Verified in the real base image: 272 certificates in the merged bundle,
the private CA appended last.

### Why it crash-loops on an empty mount

If the Secret is missing, misnamed, or holds no files, the init container
exits 1 and the pod goes to `CrashLoopBackOff` with:

```
caCerts is enabled but no files were mounted at /etc/dag-tools/ca-source.
Check that the Secret/ConfigMap exists in this namespace and holds PEM data.
```

The alternative is a pod that starts with a bundle identical to the default,
looks configured, and still rejects the endpoint. Verified: exit 1 on an
empty mount, exit 0 with one cert.

---

## Checking it worked

```bash
kubectl logs <pod> -c ca-certs-merge
# merged 1 extra CA file(s) into /etc/dag-tools/ca/ca-bundle.crt (465712 bytes)

kubectl exec <pod> -- sh -c 'grep -c "BEGIN CERTIFICATE" $SSL_CERT_FILE'
# a number in the hundreds -- if it is 1, the public roots are missing and
# every public endpoint is about to start failing

kubectl exec <pod> -- curl -sS -o /dev/null -w '%{http_code}\n' https://your-endpoint/
```

The third works because `CURL_CA_BUNDLE` is set; `curl` and the handlers
read the same file, so agreement there means agreement in the handler.

---

## What this does not cover

- **ODBC and Oracle.** `api_sync` acks back to MSSQL through
  `msodbcsql18`, and the Oracle handlers use `oracledb`. Both go through
  OpenSSL, which honours `SSL_CERT_FILE` for its default verify paths, so
  the merged bundle is **expected** to apply to them as well — but that is
  reasoning from OpenSSL's documented behaviour, not something measured
  here. If a database TLS handshake still fails, treat it as unverified
  and check the driver's own options.
- **Client certificates.** This is server verification only. Mutual TLS
  needs a key and a certificate chain presented *by* the worker, which is
  a different mechanism and not wired up.
- **Verification being off.** There is no `insecure: true`. Adding one
  would be a single values key away from disabling certificate checking in
  production, and the merge path costs an init container.
