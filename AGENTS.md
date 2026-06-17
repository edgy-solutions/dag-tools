# Guide for AI Agents Operating on `dag-tools`

Welcome, AI Agent! When interacting with this project, you must adhere strictly to this operational guide to maintain safety, reusability, and consistency.

## Pre-Flight Checklist
Before modifying *any* code or executing external network commands, you **MUST**:
1. Read `README.md` to understand the library's purpose and how to test changes locally.
2. Read `llms.txt` to align your semantic context (this is a shared utility library, not a pipeline execution repo).
3. Read `.cursorrules` to ensure your proposed edits comply with the strict tech stack (`uv`) and type-hinting standards.
4. Before touching anything under `dag_tools/inventory/` or `dag_tools/qual/`, read [`docs/RECIPE.md`](docs/RECIPE.md) — it contains the load-bearing invariants and the ADRs explaining why the qualification system is shaped the way it is.

## Operational Safety Boundaries

### 1. Reusability Focus
- **DO NOT** hardcode project-specific logic, bucket names, or table names into this repository. 
- All resources, IO Managers, and sensors must accept configuration at runtime via Dagster's `ConfigurableResource` or `ConfigurableIOManager` patterns so downstream projects can inject their own values.

### 2. Declarative Component First (Dagster 1.12 GA)
- When building new data integration wrappers (e.g., DLT, DBT, Airbyte), use the **Dagster 1.12 GA Component API** (`dagster.components`). The old experimental `dagster-components` library is deprecated.
- New components **MUST** use the `Component, Resolvable, Model` triple-inheritance pattern:
  ```python
  class MyComponent(Component, Resolvable, Model):
      my_field: str
      def build_defs(self, context: ComponentLoadContext) -> Definitions: ...
  ```
- When integrating with event-driven or stateful engines like **Restate**, avoid loose programmatic Python factory scripts. Wrap the existing extractors inside Declarative Components (like `restate_dlt_sync` and `restate_api_sync`).
- Pipeline metadata, hints, and configurations should be offloaded to YAML structure rather than hardcoded Python factory kwargs.
- When defining `@asset` functions inside `build_defs()` with closure variables, use a **factory function** pattern — Dagster 1.12 introspects all function parameters as asset inputs.

### 3. Durable State Machines & Double-Writes
- When building handlers that modify both a database and an external API (e.g., `SapInductionService`), follow the **Double-Write / Outbox Pattern**.
- Every state transition (e.g., SUCCESS -> ERROR) must be recorded in the local database AND pushed to any remote callback/webhook APIs.
- Use Restate's `ctx.run()` to wrap **both** the notification and the update in separate, durable steps to ensure they are eventually consistent, even if one system fails.
- **NEVER** instantiate database clients or heavy resources inside a handler loop; use a global singleton/cache pattern to prevent connection leaks.

### 4. Restate Worker Deployment
- **DO NOT** write per-project Restate worker entrypoints or Dockerfiles. New durable handlers are registered in `dag_tools.restate_handlers.serve.SERVICE_REGISTRY` and ship in the shared `restate-worker` image (`Dockerfile.restate-worker`).
- Deployments select handlers and wire Restate **purely through environment variables** (`RESTATE_SERVICES`, `RESTATE_ADMIN_URL`, `RESTATE_ADVERTISED_URI`); the worker self-registers on startup. When adding a handler module, add its key to `SERVICE_REGISTRY` so it becomes selectable.

### 4f. Run execution (`dag_tools.qual.runs` + `dag_tools.qual.graphql`)
- **Save state after every transition.** The runner's resumability story depends on it. If a desktop dies between launch and persistence, the registry-mirrored state file is what makes the next invocation reconcile rather than re-launch. `test_run_side_state_is_mirrored_to_registry_after_each_transition` is the regression — if it fails, you've broken resumability.
- **PASSED is sacred.** A re-invocation MUST NOT re-launch a passed rep — that would burn duplicate compute and confuse Q6's diff (two run records for the same `(class_hash, asset_key)` on the same side). LAUNCHED reps reconcile via `client.get_run_status(run_id)`, not relaunch.
- **GraphQL drift is expected.** The client uses public fields only and falls back gracefully on unknown event subtypes (records them as raw dicts with what we could decode). If a Dagster version surfaces a new event subtype that matters for run records, extend the parser in `build_run_record` — don't add internal-attribute access.
- **The Q2 IO round-trip probe is deferred.** Recipe item 4 of Q2 — "for each class, also launch the class's probe asset" — depends on Q5's probe code location. Tracked in `docs/RECIPE.md` known limitations; do not implement a half-measure that fakes it without the real probe infrastructure.

### 4e. Equivalence-class matrix (`dag_tools.qual.classes`)
- **Q1 reads the manifest, not the registry.** `build_class_matrix` walks `manifest.inventory_pins[]` and fetches each pinned SHA's `assets.json` + `dbt_projects.json`. It never calls `read_latest_pointer`. If you write a new Q-phase, follow the same pattern — that's how the qualification stays reproducible.
- **The class hash is deterministic.** Identical `ClassKeyComponents` → identical 12-char SHA-256 prefix. Tests in `test_classes_key.py` enforce this. Don't introduce nondeterministic ordering or transient state into the components.
- **Custom dbt translators always form their own class.** The detection joins `assets.json` (looking for `dagster_dbt` in `integration_libs` or `compute_kind == "dbt"`) with `dbt_projects.json` (looking for `is_custom_translator: true`). Conservative: any custom translator in the repo gets attached to every dbt asset's key. That can over-segregate but never under-segregates.
- **Runnability is tag-driven**, not FQN-inferred. The contract: `synthetic_required: "true"` / `observe_only: "true"`. Adding inference rules (e.g. "Snowflake is always synthetic") is a backwards-compatibility hazard — different orgs have different staging realities. If you want to add such a rule, gate it behind a manifest knob.

### 4d. Qualification manifest (`dag_tools.qual.qualify`)
- **The manifest is the contract for everything in Phase 2.** Q1–Q6 read it; none of them re-read the registry's `latest.json` directly. If you write a Q-phase that calls `read_latest_pointer` instead of the manifest's `inventory_pins[]`, you've broken qualification reproducibility — a mid-qualification CI publish would silently change the qualification target.
- **One qual_id per qualification, immutable.** `create_qualification` writes via the immutable registry put. Re-running with the same id raises `ImmutableKeyExists`. `--allow-overwrite` exists for the deliberate "redo this qualification" path; don't silently fall back to it.
- **`co_upgrade_risks` exists to surface hidden dbt-core / warehouse / dbt-adapter bumps inside a Dagster upgrade.** The diff explicitly filters the Dagster family (anything matching `dagster*`) because those are the explicit upgrade target. Tests in `test_qualify_risks.py` cover the matrix; extending the diff means extending the tests.
- **Recipe-aliased YAML.** `CoUpgradeRisk` uses `from`/`to` (not `from_version`/`to_version`) on disk, matching the recipe sample in `docs/RECIPE.md`. Always serialize with `by_alias=True`.

### 4c. Survey (`dag_tools.qual.survey`)
- **The load-gate invariant is non-negotiable.** `run_survey` refuses to write anything to the registry when any code location fails to load. The recipe spelled this out: "the registry never contains an inventory for code that doesn't load." This is the contract that makes every downstream qualification phase safe; without it, qualification operators would burn cycles diagnosing inventory drift caused by broken code. The test `test_run_survey_refuses_to_publish_when_load_fails` is the regression — if you change the publisher's control flow and that test fails, the failure is the system warning you off.
- **Capture every warning emitted during load.** Use the `_capture_warnings` context manager (it sets `simplefilter("always")` — equivalent to `python -W all`) around every `importlib` call. Deprecations and experimental-API warnings are part of `load_validation.json`; that's how operators see Dagster drifting across the fleet without manual scraping.
- **Per-item soft-failure**, same discipline as the inventory extractor. One malformed sensor must never abort the whole introspection pass.
- **One pydantic schema per artifact**, each with its own `SCHEMA_VERSION_*` constant. Same additive-only evolution rules as `AssetRecord`. When adding fields, bump the relevant `SCHEMA_VERSION_*` in the same commit; readers tolerate unknown fields.

### 4b. Qualification Registry (`dag_tools.qual.registry`)
- **The layout module owns every S3 key.** `dag_tools/qual/registry/layout.py` is the only place where key strings are constructed. Writers and readers compose paths via helpers like `inventory_artifact_key(repo, sha, filename)` and `latest_pointer_key(repo)`. **Never** literal-string an S3 key anywhere else in the codebase.
- **Immutability is enforced at the storage layer.** Every per-build artifact uses `S3Storage.put_immutable`, which HEAD-then-PUTs and raises `ImmutableKeyExists` on collision. Only `latest.json` (and the equivalent qualification pointers) use `put_mutable`.
- **`latest.json` is written LAST.** `InventoryRegistry.publish_build` writes all immutable artifacts first, then updates the mutable pointer. **Do not reorder** — this invariant is what makes partial publishes invisible to readers. The test `test_pointer_not_updated_when_artifact_write_fails` enforces it; if you change the publish ordering, that test will fail and that failure is the system telling you to stop.
- **Machine-readable JSON is the default CLI output**, per the recipe rule "Every command is idempotent and emits machine-readable JSON". Every `dagtools` sub-command must accept `--format` defaulting to `json`, with `table` available for human consumption.

### 4a. Shared Inventory Contract (`dag_tools.inventory`)
- The `AssetRecord` pydantic schema in `dag_tools/inventory/schema.py` is the **cross-process contract** between the runtime Domain Broker (long-lived pods, last quarter's release) and the CI Dagster qualification survey (every commit, today's release). They will read each other's records across versions.
- **Evolution rules**: additive-only. Never rename or remove fields. Only add new `Optional[...]` fields with defaults, and bump `SCHEMA_VERSION` by 1 in the same commit. Readers tolerate unknown fields via `extra="ignore"`. If you truly need a breaking change, it's a major library version and requires coordinated rollout — talk to the runtime mesh team first.
- **IO manager classification**: extend `dag_tools/inventory/classifier.py::FAMILY_REGISTRY` with the FQN of any new IO manager. MRO walking catches custom subclasses of registered ancestors. The substring fallback is a logged-at-WARNING last resort whose presence in the logs is a signal to add a registry entry.
- **Soft-failure discipline**: the extractor must never abort because one asset is malformed. Wrap every field access in per-field exception handling; record `None` and log WARNING. The whole point of the schema being mostly-optional is to make this safe.

### 5. Normalized Configuration Design
- **Dagster Configuration First**: All new components and resources MUST use the Dagster configuration framework (`Config`, `ConfigurableResource`, or `dagster.components` attributes).
- **Wrap & Standardize**: Do not expose raw, pass-through configurations for underlying tools (like `dlt` or `dbt`) directly if they deviate from Dagster's standard practices. Instead, define a Dagster-centric configuration schema that wraps and translates to the native tool's requirements.
- **Component Translation**: The component's `build_defs()` or the resource's initialization logic is responsible for mapping these standardized Dagster inputs into the specific format required by the underlying engine.

### 6. Backwards Compatibility
- Since other repositories (like `pub-tools`) depend on `dag-tools`, **DO NOT** make breaking changes to function signatures or export names without explicit approval from the user. 
- If adding a new feature to an existing shared component, make the new parameters optional with sensible defaults.

### 7. Maintaining the Documentation Trifecta
As an agent operating on this project, part of your job is self-maintenance of the AI guardrails.
If you add a new category of common tool (e.g., a new suite of dbt wrappers), you MUST concurrently update:
1.  `llms.txt` (if the domain/intent changes).
2.  `.cursorrules` (if the enforced stack rules change).
3.  `README.md` (to document the new tools for human developers).
