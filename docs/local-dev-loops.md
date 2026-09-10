# Iterating on dbt and dlt without the Dagster UI

Both tools have a fast local loop. dbt's has always existed and was never
written down here; dlt's did not exist until recently. This covers both,
plus the awkward part they share — credentials.

---

## Why the two feel different

**dbt's project directory is the artifact.** dbt ships its own runner, so
Dagster only ever wraps something already runnable. `dbt run` works because
the project does not need Dagster to mean anything.

**A dlt pipeline here is YAML that only dag-tools can interpret.** The
factory resolved credentials, built the destination, constructed the
source and wrapped it in a `@multi_asset` in one breath — so nothing
outside Dagster could construct a pipeline, and changing one cursor meant
a full definitions load.

That asymmetry is now closed from the dlt side.

---

## dbt

The project directory named in your component's `project:` is an ordinary
dbt project. Run it directly:

```bash
cd dbt_projects/<project>
dbt parse          # the same command the component runs at load
dbt run --select <model>
dbt build --select <model>+
```

`dbt parse` is worth knowing specifically: it is what the Dagster component
invokes to produce `manifest.json`, so if it fails locally the code
location will fail to load for the same reason — and finding out in two
seconds beats finding out in ninety.

Credentials come from dbt's own `profiles.yml`, which is *not* the
component's config. That is the one thing to watch: a dbt run that works
locally proves the models are right, not that the deployed component
points at the same warehouse.

---

## dlt

### The fast loop

```bash
dagtools dlt env --defs path/to/defs.yaml
dagtools dlt run --defs path/to/defs.yaml --destination ./dev.duckdb --limit 100
```

`--destination` lands the data in a local DuckDB file instead of the
configured warehouse. **Reflection, hints, cursors and write dispositions
stay exactly as configured** — only where the data lands changes, which is
what makes the local run meaningful rather than a different pipeline that
resembles it.

Only DuckDB paths are accepted (`./dev.duckdb`, `duckdb:///tmp/d.duckdb`,
`:memory:`). The override exists to get *away* from a destination that
needs credentials, so it will not let you point at another one.

### Validating without writing anywhere

```bash
dagtools dlt run --defs path/to/defs.yaml --extract-only
```

Extract without normalize or load. This exercises reflection, hints,
cursors and the query adapter against the **real source** and writes
nothing — which covers most of what a mapping change needs to prove.

### Other flags

| Flag | Effect |
| --- | --- |
| `--pipeline KEY` | required only when the file declares several |
| `--tables a,b` | a subset of the declared sources, without editing YAML |
| `--limit N` | **rows**, not pages — see the note below |

> **`limit` means rows as of 0.6.3, and did not before.** dlt's `add_limit`
> counts *"yields/batches/pages"* by default and counts empty pages too.
> With the sqlalchemy backend the first yield is not data, so `limit: 1`
> used to extract **nothing at all** while `limit: 2` extracted an entire
> table. If you are on an older pin, do not trust small values.

### From Python

The CLI is a thin shell over an importable API:

```python
from dag_tools.asset_wrappers.dlt_assets_parsing import build_dlt_runnables

runnable = build_dlt_runnables(
    ["widgets"], source_config, dest_config, config,
    destination_override=dlt.destinations.duckdb("/tmp/dev.duckdb"),
)[0]

runnable.extract()   # no writes
runnable.run()       # extract -> normalize -> load
```

Both the CLI and the Dagster asset path go through the same config
resolution, so a local run cannot resolve credentials, pipeline names or
destinations differently from the asset that ships.

### What this does *not* avoid

It still imports Dagster — the dlt factory does. What it avoids is loading
**your definitions module**, which pulls every component in the location,
takes 90–180 seconds, and fails entirely if any *one* of them fails to
import. That last part matters most: a broken sibling component used to
block iterating on a working one.

---

## Credentials, which is the hard part

A dbt or dlt run needs credentials to **source systems** — the operational
databases upstream of the mesh. Those are not mesh assets and have no
broker to mint scoped tickets, which is why `CortexDataClient`'s model does
not transfer: it authorizes reads of things already landed and catalogued.
dlt exists to bring them in.

So, in order of preference:

**1. Don't use real sources for the inner loop.** `--destination
./dev.duckdb` plus a small recorded sample covers hints, cursors, column
selection, naming and write disposition — which is most of what changes.
No warehouse credential needed at all; `dagtools dlt env` will show you
that only the *source* variables remain.

**2. Run where the credentials already are.** For work that needs the real
source, `kubectl exec` into the user-code pod and run there. Every
credential is present and correctly scoped, and nothing lands on a laptop:

```bash
kubectl exec -it deploy/<user-code> -- \
    dagtools dlt run --defs /app/<...>/defs.yaml --extract-only
```

**3. Only then, a `.env`.** If you must, `dagtools dlt env` tells you
exactly which variables that defs file needs — derived from the file, both
`{{ env.NAME }}` and `{env: NAME}` forms — so the list cannot drift the way
a hand-written `.env.example` does.

> **`.env.dlt` is redacted as of 0.6.2 and was not before.** The factory
> writes it on every local `dagster dev`, in whichever directory you run
> from, and it used to contain resolved destination **credentials** while
> `.gitignore` matched only `.env`. If you ran `dagster dev` on an older
> pin, check for that file and rotate anything it exposed:
>
> ```bash
> git log --all --diff-filter=A -- '**/.env.dlt'
> ```

---

## Known limits

- `dagtools dlt` needs the orchestrator extra (dlt plus a source driver).
  `[qual]` deliberately installs no Dagster, so it has neither; the command
  says which extra is missing rather than failing with an ImportError from
  three layers down.
- The defs reader resolves `{{ env.NAME }}` only. Anything richer is
  refused with the line number, because rendering it here could give a
  different answer than Dagster does — and a config that means one thing
  locally and another when deployed is worse than a refusal.
- Component machinery that is not dlt configuration — control tables,
  cycle sensors, acknowledgment settings — is ignored by `dlt run`. It
  builds and runs the **extraction**, not the surrounding cycle.
