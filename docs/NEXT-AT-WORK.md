# Next at work — 2026-08-27 handoff

Written at the end of the ADR-0044 session so the state is on disk rather than
in anyone's memory. Ordered by what unblocks the most people.

---

## 1. mfg-tools: make the forked IO managers publish

**This is the one blocking your own retest.** `orch.resources.arrow` and
`orch.resources.sql` are vendored copies of dag-tools' IO managers, forked
before `physical_coordinates()` existed. The classifier confirms no shared
ancestry (it walks the MRO and reports `no match`), so they are independent
copies rather than subclasses.

Diagnosis output, for reference:

```
advertised (physical identity): 0
not advertised               : 142
      73  no IO manager resolved for io_manager_key='io_manager'
      29  DataHubSnowflakePandasIOManager has no physical_coordinates()
      23  ConfigurableSQLIOManager has no physical_coordinates()
      17  ConfigurableArrowIOManager has no physical_coordinates()
```

**Do:**

- [ ] Diff `orch.resources.arrow` against `dag_tools.io_managers.arrow`. If
      they are close, **subclass** — you get `physical_coordinates` free *and*
      the classifier starts matching, which removes the `no match` warnings too.
- [ ] If they have drifted, mix in `dag_tools.io_managers.MeshPublishable`
      (new in 0.3.2) and define `mesh_uri()`. See
      [publishing-io-managers-to-the-mesh.md](publishing-io-managers-to-the-mesh.md).
- [ ] **Before choosing the URI derivation**, `mc ls` the bucket for one table.
      dlt wrote those bytes, not the IO manager, so a naive asset-key join is a
      guess. If the real prefix is `staging/vdspc_axi/dbo/board_mapping/` a
      simple join works; if dlt inserted a `dataset_name` or load-id segment it
      does not, and `mesh_uri` must read the dlt destination config.
- [ ] Re-run `python /tmp/d.py board_mapping` in the broker pod. It should flip
      to `ADVERTISED as: urn:li:dataset:(urn:li:dataPlatform:s3,...)`.

The 73 assets on `io_manager_key='io_manager'` are a separate matter — that key
is not in `Definitions(resources=...)` at all, so Dagster uses its local
filesystem default. There is genuinely nothing to publish; leave them.

The 29 Snowflake ones need ADR-0044 step 6 regardless.

## 2. Set the broker's endpoint to the FQDN

- [ ] `S3_ENDPOINT_URL` on the **mfg** broker → `.svc.cluster.local` form.
      Already done for pub-tools. The minted credential comes back pointing at
      whatever the producer advertised, so a bare service name breaks any
      consumer outside that namespace.
- [ ] After changing it, **restart the broker pod**. `physical_coordinates()`
      runs once at startup and the ticket is cached in `LOCAL_ASSETS` for the
      process lifetime — an env change alone does nothing.

## 3. Gateway: two checks, then decide

Neither is run. Both are quick, and together they say whether deploying the
0.3.x gateway is safe.

- [ ] **Tally the subject sources** against the *current* gateway:
      ```bash
      kubectl logs -n <ns> deploy/iagent-central-gateway --since=24h \
        | grep -o 'source=[a-z-]*' | sort | uniq -c
      ```
      Only `source=token-claim` requests are affected by the change. If that
      count is zero, deploying is risk-free.
      **If you see no `subject-source:` lines at all, that is a dark
      instrument, not a clean result.**
- [ ] **Decode one M2M token** and look at both claims:
      ```bash
      curl -s -d grant_type=client_credentials -d client_id=iagent-data-analyst \
           -d client_secret=... "$REALM/protocol/openid-connect/token" \
        | jq -r .access_token | cut -d. -f2 | base64 -d | jq '{email, preferred_username}'
      ```
      Two mappers write `preferred_username` — yours and Keycloak's built-in —
      and only the token says which won. Verified in the edge sandbox that the
      built-in *does* fire for service accounts (`service-account-<clientId>`),
      so the collision is real.

      | `email` | `preferred_username` | verdict |
      |---|---|---|
      | `svc:data-analyst` | `svc:data-analyst` | safe |
      | absent | `svc:data-analyst` | safe — it's a fix |
      | absent | `service-account-…` | safe, no change |
      | `svc:data-analyst` | `service-account-…` | **regression — do not deploy** |

- [ ] Deploy order if you go: **gateway image first, then**
      `USER_ENTITLEMENT_CLAIM`. The reverse leaves the old hardcoded `email`
      lookup running and token-only reads fail closed.
- [ ] Pin the gateway image. It runs `:latest`, so "when did this change" is
      currently unanswerable.

## 4. Measure the STS ceiling from inside a pod

- [ ] `scripts/measure_adr0044_sts_ceiling.py`, run **in-cluster**, no
      port-forward. The existing number (57 mints/s) is a **floor** — the
      forward saturated before MinIO did, so nothing should be designed against
      it. Until this runs, the credential-cache question is unanswered rather
      than answered no.

## 5. `AWS_ASSUME_ROLE_ARN`

- [ ] Still the placeholder `arn:aws:iam::123456789012:role/DataAccessRole`.
      MinIO ignores a RoleArn it has no configuration for (verified — a
      pre-registered prediction that it would fail was falsified). **Real AWS
      does not.** Dormant on MinIO, live defect for any AWS-backed deployment.

---

## Mine, not yours — for context

Tracked in `invincible-agent/docs/plans/`, visible on `docs/BOARD.md`:

- `broker-advertises-unminted-credential` — ADR-0044. Object-store half done and
  verified on pub-tools at d4. ClickHouse/Postgres minting is step 4/6.
- `da-sends-no-user-token` — DA passes `jwt_token=None`, so killing the
  gateway's header override takes DA's data access down. Prerequisite for the
  gateway verification work.
- `sdk-discards-caller-identity` — `MeshTool` computes a `CallerIdentity` and
  throws it away, so an agent cannot read as its invoker. **The blocker on the
  notebook→agent path**, which is where your users are actually heading.
- `jupyter-user-token-data-access` part 3 — `CortexDataClient` reading
  `CORTEX_USER_TOKEN` + refresh-at-use. Until then notebooks pass `jwt_token=`
  explicitly, which is also the safe form.

Unfiled: the SDK runs sync handlers on the event loop while its quickstart
promises a background thread; the dag-tools ADR series; the
file-backed-vs-table-backed client API distinction.
