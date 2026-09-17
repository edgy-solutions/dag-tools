# Surveying a deployed code location

`dagtools survey` publishes the per-build inventory every later qualification
phase reads. It works by **importing your `Definitions`** — which means it
needs every credential that import touches.

That one fact decides everything below.

---

## Where the survey has to run, and why not CI

[`RECIPE.md`](RECIPE.md) §Part 1 places the survey in a **Jenkins** stage,
and the reasoning is sound: the inventory is keyed by `(repo, git_sha)`, a
property of a *build*. Jenkins knows both, so every build gets an inventory
and a qualification can pin any historical SHA.

**That placement breaks for a fleet whose definitions need secrets to
construct.** Jenkins has the code identity but not the runtime
environment, and a code location that cannot import produces no inventory
at all — the publisher refuses rather than publishing a partial one.

A running pod has the opposite pair: the full environment, and — provided
the image stamps its SHA — the code identity too. So the pod can supply
both.

**Keep Jenkins as the trigger, move the execution to the pod.** Jenkins
builds and deploys, then starts a Job in the cluster. The RECIPE's intent
survives (one inventory per build); only the execution site changes.

---

## Survey the BASELINE containers, and only those

For an upgrade qualification, the inventory describes **the code** —
assets, IO managers, resources, partitioning, automation conditions. None
of that changes between the two sides: the same pinned SHAs run on both.
What changes is the Dagster version underneath.

```
survey (baseline containers)  ->  inventory per (repo, git_sha)
        |
  qual init      pins those SHAs
  qual classes   equivalence classes from them
  qual run --side baseline      against the OLD deployment
  <upgrade the test deployment>
  qual preflight --side candidate
  qual run --side candidate     SAME representatives, new Dagster
```

**Do not re-survey for the candidate.** Re-surveying lets the inventory
drift between the two sides, and the entire comparison rests on both sides
running the same pinned code.

This is also why dag-tools keeps a CI job pinned to the oldest supported
Dagster: the survey must run on the version you are upgrading **from**.
Verified against a legacy-shaped code location on 1.10.19 — loads clean,
3 assets, 1.3s.

---

## Installing into an old container

The containers almost certainly already have `dag_tools` — if any
`defs.yaml` says `type: dag_tools.SomethingComponent`, it is installed.
What is usually missing is `typer`, so the `dagtools` console script exists
and cannot start.

**You do not need the CLI.** The survey path never imports typer.

### Do NOT do this

```bash
pip install "edgy-dag-tools[qual]"      # ← measured: 53 -> 90 packages
```

`[qual]` does not spare you dag-tools' **base** dependencies, which include
`oracledb`, `pyodbc`, `polars`, `deltalake`, `pyiceberg`, `connectorx`,
`clickhouse-connect` and `pyarrow`. That is 37 packages into a container
whose dependency graph you are about to measure.

Dagster itself held at 1.10.19 in a clean test, but that is the resolver
being lucky, not a guarantee — a real container with `snowflake-connector`,
`dbt` and `pyspark` already pinned has far more to collide with. **If the
install moves anything, the inventory describes a container that is no
longer what you deployed.**

### Do this

```bash
pip install --no-deps "edgy-dag-tools"
pip install boto3 pyyaml        # only if not already present
```

**+6 packages instead of +37**, Dagster untouched. The survey path needs
only `dagster` (present), `pydantic` (Dagster's own), `boto3` and `pyyaml`.

### Bake it into the image — do not install into a live pod

An install into a running container is lost on restart, and while it lasts
the pod differs from its own image. The SHA you key the inventory by then
describes something you did not survey. Add the two lines to the baseline
image's build, redeploy, then survey.

---

## The SHA must come from the image

The inventory is keyed by it, `qual init` pins it, and every later phase
trusts it. Guessing makes the whole qualification describe code that was
never deployed.

Stamp it at build time and read it at survey time:

```dockerfile
ARG GIT_SHA
ENV APP_GIT_SHA=${GIT_SHA}
```

A pod that does this already:

```
$ kubectl exec deploy/<user-code> -- printenv | grep GIT_SHA
IAGENT_GIT_SHA=cfa3f0d26aa7
```

If your baseline images stamp nothing, add it **before** surveying.

---

## Running it

### By hand, to try it

```bash
kubectl exec deploy/<user-code> -- python -c "
import os
from dag_tools.qual.survey.publisher import run_survey
from dag_tools.qual.registry import InventoryRegistry
from dag_tools.qual.registry.client import StorageSettings

run_survey(
    '<your.definitions>',                 # module path, or workspace.yaml, or a .py file
    repo='<repo>',
    git_sha=os.environ['APP_GIT_SHA'],
    registry=InventoryRegistry.from_settings(
        StorageSettings(bucket='dag-tools', endpoint_url=os.environ.get('DAGTOOLS_S3_ENDPOINT'))
    ),
    skip_publish=True,                    # drop this once it looks right
)"
```

Start with `skip_publish=True`. It exercises the whole load and
introspection and writes nothing, so a first attempt cannot leave a
half-formed inventory in the registry.

The locations spec is the same one the deployment uses: `pkg.module`,
`pkg.module:attr`, a `workspace.yaml`, or a `.py` file. A bare token with
no dot or colon is refused rather than guessed at.

### As a Job, to do it repeatedly

Template this from the **user-code deployment's own podspec** so the
environment wiring is copied rather than reinvented — same image, same
`envFrom`, same service account. Only the command changes.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: survey-<repo>-<short-sha>
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: <same as the deployment>
      containers:
        - name: survey
          image: <the SAME image tag the deployment runs>
          envFrom:                      # copied verbatim from the deployment
            - configMapRef: {name: <app-config>}
            - secretRef:    {name: <app-secrets>}
          env:
            - name: DAGTOOLS_REGISTRY
              value: "s3://dag-tools"
            - name: DAGTOOLS_S3_ENDPOINT
              value: "<minio endpoint>"
            # plus whatever credentials the REGISTRY needs -- see below
          command: ["python", "-c"]
          args:
            - |
              import os
              from dag_tools.qual.survey.publisher import run_survey
              from dag_tools.qual.registry import InventoryRegistry
              from dag_tools.qual.registry.client import StorageSettings
              outcome = run_survey(
                  os.environ["SURVEY_LOCATIONS"],
                  repo=os.environ["SURVEY_REPO"],
                  git_sha=os.environ["APP_GIT_SHA"],
                  registry=InventoryRegistry.from_settings(StorageSettings(
                      bucket="dag-tools",
                      endpoint_url=os.environ.get("DAGTOOLS_S3_ENDPOINT"),
                  )),
              )
              assert outcome.published, "survey refused to publish -- see the load failures above"
```

The `assert` matters: the publisher **refuses to publish when any location
fails to load**, and returns rather than raising. Without that line the Job
exits 0 having published nothing, which is the silent-green shape this
whole document exists to avoid.

### The registry credential is a separate question

The survey writes to MinIO/S3, and the user-code deployment has no reason
to hold registry credentials. Check before assuming:

```bash
kubectl exec deploy/<user-code> -- printenv | grep -E "AWS_|DAGTOOLS_"
```

If they are absent, add them to the **Job** rather than to the deployment.
The deployment has no business writing to the registry, and widening its
credentials to make a survey convenient is the wrong trade.

---

## What to check afterwards

```bash
kubectl logs job/survey-<repo>-<short-sha>
```

- `loads: True` and a non-zero asset count per location
- the Job exited 0 *and* the assert passed — see above for why both
- `inventory/<repo>/latest.json` in the registry now names your SHA

A survey that loads but inventories **zero** assets is worth investigating
rather than accepting: it usually means the locations spec pointed at a
module that imports cleanly but holds no `Definitions`.
