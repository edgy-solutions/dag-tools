# Built claims that could become measured

This list names `built` claims in these records, and for each one the seal or test that could make it `measured`. It is a list only. Nothing was run to write it: the runs it cites are the repository's own continuous-integration runs at the pins. Statuses are those of [the README](README.md#status-vocabulary), and line numbers are at the pins listed there.

The README sets the bar, as the architect ruled on 2026-11-19: `measured` is relative to the claim. A recorded run measures what it exercises. So a claim about the code's own behaviour is `measured` by an in-process run in continuous integration, cited by its run id and marked "in CI, in process". A claim about a deployed service or a real backend is not measured by such a run, and stays `built`. A test that could measure a claim is not enough. So this list sorts the candidates by what a run of each shows, and splits a claim where a run measures one part and not the other.

**This record describes; it proposes nothing.** Where a candidate test does not exist, it is named as a candidate, not as work in progress. The record shape is provisional.

**The records themselves are not changed here.** Each keeps `built` until it is revisited, and this list names what the revision would carry.

## Findings

1. **Eight claims are `measured` in CI, in process, at least in part:** A2, A3, B1, B2, B3, D2, D11 and D16 ([below](#measured-in-ci-in-process)). The run cited is 44398336497 at `9d89568a`, and run 44451597250 at `add5bc48` agrees. No other record carries `measured` yet. `measured`, in CI, in process
2. **No claim about a deployed service or a real backend is measured.** The parts still `built` are a real catalog server (A2), the chart's handover of the image (A3) and a real token service (D11). `built`
3. **Group A: two of the four ran in CI.** A2 ran in a job of its own, and A3 ran in each Python job. A1 skipped in both runs. A4 is an invincible-agent test, which dag-tools CI does not run. A1 and A4: `built`
4. **Group B: all three passed in CI.** Whether B1's throughput target is met is not established; the run measures that the benchmark runs and records a rate. Of the four rates each route recorded (two runs, two Python versions), the engine route's reach the target of 20,000 rows per second in two and the JSON route's in none. CI does not assert the target, so these are readings of the runs and change no status. `measured`, in CI, in process
5. **Five claims need a live run that no test performs** (group C). A recorded run against a deployment is the only route to `measured` for these. `built`
6. **Sixteen claims have a unit test over doubles** (group D). A CI run of such a test measures the code's behaviour in process. D2, D11 and the composition part of D16 passed in the cited run. D1 skipped. The invincible-agent tests are not run by dag-tools CI, and no run of them is cited, so they stay `built`. Three of them replace a live service with a fake. D2, D11 and the composition part of D16: `measured`, in CI, in process; the rest `built`
7. **Eighteen claims have no test at all** (group E). A candidate test would make each one `built` with a test. A CI run of it would then measure the code's behaviour in process, and nothing more. `built`

## Measured in CI, in process

The "Tests" workflow at `9d89568a` runs four jobs. Two Python jobs run the whole test directory (`test.yml:27-28`, `:86-90`), fail if a test skipped because a package or binary is missing (`:97-106`), and upload their results (`:108-115`). A job on Dagster 1.10.19 runs everything but one module (`:117-118`, `:224-227`). An end-to-end job runs the dbt-to-catalog test with its environment gate set, and fails if the test skips (`:245-246`, `:277-289`). Run 44398336497 ran at `9d89568a`, and every job succeeded. Run 44451597250 ran at `add5bc48`, and every job succeeded there too. A run id is accepted as evidence today; its logs expire, and a durable form is the owner's to design.

| # | Part measured | Run and job | Outcome | Still `built` | Status |
|---|---|---|---|---|---|
| A2 | The artifact handoff into one directory, sources in the catalog, and the publish with a real dbt and the real ingest command line. The database is a real one in a container; the catalog server is an in-process recorder. | 44398336497, the end-to-end job | the job succeeded, and it fails on any skip | a real catalog server | `measured`, in CI, in process |
| A3 | A live code-server process reports its image through the workspace query. | 44398336497, both Python jobs | passed | the chart's handover of the image on a deployment | `measured`, in CI, in process |
| B1 | The two throughput benchmarks run and record a rate. Without the gate each asserts only a rate above zero. | 44398336497, both Python jobs | passed, with the rate recorded as a test property | whether the throughput target is met, which is not established | `measured`, in CI, in process |
| B2 | The document tables example loads and materializes on Dagster 1.10.19. | 44398336497, the 1.10.19 job | the job succeeded; none of its skips is in these modules | — | `measured`, in CI, in process |
| B3 | The contract mapping, read audit and tagged warehouse examples load and materialize on Dagster 1.10.19. | 44398336497, the 1.10.19 job | the job succeeded; none of its skips is in these modules | — | `measured`, in CI, in process |
| D2 | Every GraphQL document the client sends validates against the installed schema. | 44398336497, both Python jobs and the 1.10.19 job | passed, eleven cases in each Python job | — | `measured`, in CI, in process |
| D11 | A minted credential is scoped to the asset and read-only, and a minting failure refuses the ticket. | 44398336497, both Python jobs | both tests passed, against a fake token service | a real token service | `measured`, in CI, in process |
| D16 | Labels compose as the join of one order, in every dimension type. | 44398336497, both Python jobs | passed, four cases | the three invincible-agent parts | `measured`, in CI, in process |

## A. An existing test runs a real process

| # | Built claim | Test | What a recorded run would show | Record |
|---|---|---|---|---|
| A1 | §4j.19: the concurrency cap holds against a live Restate server, and the timeout settings reach discovery. | `test_restate_concurrency_e2e.py`, behind an environment gate. It skipped in both CI runs. | The cap and the discovery settings, on a real Restate server | [§4j.19](decisions/dag-tools-s4k-s4j.md) |
| A2 | §4i.3, §4i.8, §4i.9: every artifact the catalog recipe names lands in one directory, the catalog includes sources, and the publish works with a real dbt and a real ingest command line. | `test_dbt_datahub_e2e.py`, behind an environment gate. CI runs it in a job of its own with the gate set, and that job fails if it skips. It runs a real database, dbt and the ingest command line against an in-process catalog recorder. | The artifact handoff and what the catalog received, `measured` in CI, in process ([above](#measured-in-ci-in-process)). The catalog server itself is a recorder, so the run does not measure a real catalog. | [§4i.3](decisions/dag-tools-s4i-s4h-s4g.md) |
| A3 | §4g.3: the Dagster chart hands the code server its image, and the server reports it in display metadata. | `test_dagster_reports_the_container_image_through_the_workspace_query` (`test_preflight_image_identity_grpc.py:66`), a live code-server process, skipped when its dependencies are absent | The image reported through the workspace query, on each supported Dagster version. The server's part is `measured` in CI, in process ([above](#measured-in-ci-in-process)); the chart's handover is a deployment fact, and is not. | [§4g.3](decisions/dag-tools-s4g1-s4g-s4f.md) |
| A4 | invincible-agent ADR-0025: the access-decision slot on a citation round-trips to the graph store. | `test_layer_1_2_3_populated_access_decision_roundtrips_to_neo4j`, which reads its graph store address from the environment. dag-tools CI does not run it. | The slot written and read back from a running graph store | [ADR-0025](decisions/invincible-agent-adr-0025-0048.md) |

## B. In-process runs recorded by continuous integration

All three passed in the cited run and are `measured` in CI, in process ([above](#measured-in-ci-in-process)).

| # | Built claim | Run | Record |
|---|---|---|---|
| B1 | §4s.28: the documents throughput benchmark exists. Its throughput target is asserted only behind an environment gate. | `test_throughput_on_a_bulk_manifest` (`test_documents_engine.py:942`) and `test_json_throughput_on_a_bulk_manifest` (`test_documents_json.py:695`). Both run in each Python job and record the rate as a test property. The target is checked only behind an environment gate, which CI does not set. | [§4s.28](decisions/dag-tools-s4s.md) |
| B2 | §4s.33: the document tables example loads and materializes on Dagster 1.10.19. | The 1.10.19 continuous-integration job, which ran those tests and succeeded. The job does not block a merge ([Dagster](components/dagster.md), finding 7). | [§4s.33](decisions/dag-tools-s4s.md) |
| B3 | §4k.16, §4j.8, §4j.16: the contract mapping, read audit and tagged warehouse examples load and materialize on Dagster 1.10.19. | The same job, which ran those test modules and succeeded | [§4k.16](decisions/dag-tools-s4k-s4j.md) |

## C. The claim needs a live run that no test performs

| # | Built claim | What would measure it | Record |
|---|---|---|---|
| C1 | Restate API sync records state after acknowledging. The call is a state write on an unkeyed service (`api_sync.py:11`, `:193-194`), and only a context double accepts it (`test_restate_api_sync_refusal.py:536`). | A run against a real Restate server, in the shape of `test_restate_concurrency_e2e.py` | [restate API sync](components/restate-api-sync.md), finding 14 |
| C2 | §4s.6: an S3 store as a destination for the documents writer. | A write, a re-write and a maintenance run against an S3 store, recorded. The documentation states this is not verified. | [§4s.6](decisions/dag-tools-s4s.md) |
| C3 | §4o.4: an absent version is counted per source type on the broker's health body. | A recorded health response from a running broker. No version reader ships. | [§4o.4](decisions/dag-tools-s4n-s4o-s4k1.md) |
| C4 | §4i.4: a catalog server with metadata authentication on rejects an ingest without a token and accepts one with it. | A recorded run against such a server, with and without the token. The decision reports a comparison, but no record of the run is cited. | [§4i.4](decisions/dag-tools-s4i-s4h-s4g.md) |
| C5 | §4g0.2: the probe runner launches the upstream and the downstream asset in one run, and the downstream reads what the upstream wrote. | A recorded launch against a running Dagster with a deployed probe location. The decision reports a live check, but no record of the run is cited. | [§4g0.2](decisions/dag-tools-s4i-s4h-s4g.md) |

## D. A unit test over doubles exists

A run of these tests shows the code does what the claim says, against doubles. It does not show a running system doing it. Where the cited CI run passed a dag-tools test, that part is `measured` in CI, in process: D2, D11 and the composition part of D16 ([above](#measured-in-ci-in-process)). D3 to D10, D12 to D15 and three parts of D16 are invincible-agent tests, which dag-tools CI does not run, and no run of them is cited.

| # | Built claim | Test | Record |
|---|---|---|---|
| D1 | §4k.18: the vendored contract is byte-identical to the upstream copy. | `test_the_vendored_contract_has_not_drifted_from_upstream`. It compares files when the upstream copy is available and skips otherwise. A recorded comparison would show the two files agree on that day. It skipped in both CI runs, so no comparison is recorded. | [§4k.18](decisions/dag-tools-s4k-s4j.md) |
| D2 | §4g.5, §4f.5: every GraphQL document the client sends validates against the installed schema. | `test_client_document_validates_against_the_installed_schema` (`test_graphql_query_schema_validation.py:38`), against the installed library, run on each supported Dagster version | [§4g.5](decisions/dag-tools-s4g1-s4g-s4f.md) |
| D3 | invincible-agent ADR-0026: the token's persona claim is ignored, and entitlements come from the decider. | `test_jwt_persona_claim_is_ignored` | [ADR-0026](decisions/invincible-agent-adr-0025-0048.md) |
| D4 | invincible-agent ADR-0031: a core name inside a descriptor phrase resolves on the containment rung. | `test_core_name_inside_descriptor_phrase_is_a_strong_hit` | [ADR-0031](decisions/invincible-agent-adr-0025-0048.md) |
| D5 | invincible-agent ADR-0033: a missing mandatory slot becomes a question, not an error response. | `test_A3_a_missing_mandatory_slot_is_an_ask_and_not_a_400` | [ADR-0033](decisions/invincible-agent-adr-0025-0048.md) |
| D6 | invincible-agent ADR-0034: a jump from supervised to trusted is forbidden. | `test_supervised_to_trusted_is_forbidden` | [ADR-0034](decisions/invincible-agent-adr-0025-0048.md) |
| D7 | invincible-agent ADR-0036: an overlay can delete a seeded entry. | `test_A_TOMBSTONE_REMOVES_A_SEEDED_KIND` | [ADR-0036](decisions/invincible-agent-adr-0025-0048.md) |
| D8 | invincible-agent ADR-0038: the boundary span is emitted once across a replay. | `test_boundary_is_emitted_once_across_a_replay`. It has no live gate. | [ADR-0038](decisions/invincible-agent-adr-0025-0048.md) |
| D9 | invincible-agent ADR-0039: a decision table is total over the inputs its domain admits. | `test_TOTALITY_every_input_the_domain_admits_has_a_row` | [ADR-0039](decisions/invincible-agent-adr-0025-0048.md) |
| D10 | invincible-agent ADR-0042: an anonymous caller is refused a live view. | `test_an_anonymous_caller_asking_for_a_live_view_is_REFUSED` | [ADR-0042](decisions/invincible-agent-adr-0025-0048.md) |
| D11 | invincible-agent ADR-0044: a minted credential is scoped to the asset and read-only, and a minting failure refuses the ticket. | dag-tools `test_minted_policy_is_scoped_to_the_asset_and_cannot_write` and `test_minting_failure_fails_closed`. Both replace the token service with a fake. | [ADR-0044](decisions/invincible-agent-adr-0025-0048.md) |
| D12 | invincible-agent ADR-0050: every ratified canvas template validates against its schema. | `test_every_ratified_template_validates` | [ADR-0050](decisions/invincible-agent-adr-0049-0072.md) |
| D13 | invincible-agent ADR-0051: no measure function writes a risk acceptance. | `test_no_measure_function_writes_an_acceptance` | [ADR-0051](decisions/invincible-agent-adr-0049-0072.md) |
| D14 | invincible-agent ADR-0052, ADR-0053: a non-person identity gets a 401 with no client built, and a measure module computes what its policy row transcribes. | `test_an_identity_that_is_not_a_person_is_a_401_and_no_client_is_built`; `test_the_module_COMPUTES_what_its_row_TRANSCRIBES` | [ADR-0052](decisions/invincible-agent-adr-0049-0072.md) |
| D15 | invincible-agent ADR-0056: a record whose platform has no mapping row is never returned. | `test_a_record_whose_platform_has_no_mapping_row_is_never_returned` | [ADR-0056](decisions/invincible-agent-adr-0049-0072.md) |
| D16 | invincible-agent ADR-0062, ADR-0064, ADR-0071, ADR-0072: a merge is a two-parent signed audited commit; labels compose as the join of one order; one directory client serves every request after warm-up; a live answer journals each step once. | `test_a_happy_merge_is_a_two_parent_signed_audited_commit`; dag-tools `test_composition_is_the_join_of_one_order_in_every_type`; `test_a_whole_entitlements_request_builds_no_client_after_warm_up`; `test_run_impl_journals_each_step_once_and_persists`. The last uses a fake durable context. | [ADR-0062](decisions/invincible-agent-adr-0049-0072.md) |

## E. No test exists

A candidate test is named for each. Writing it would give the claim a test, and a CI run of it would measure the code's behaviour in process. A claim about a running system would still need a run against one.

| # | Built claim | Candidate test | Record |
|---|---|---|---|
| E1 | Every source type the producer side advertises has a minter in the broker. At the pin the two sets are equal, five types in each. | Assert the broker's minter keys equal the advertised source types (`dag_tools/domain_broker/main.py:298-308`; `dag_tools/io_managers/mesh_publishing.py:47-49`). This would not settle roadmap row 3. It would stop a new advertised type from silently using the producer's own credential. | [domain broker](components/domain-broker.md); [roadmap](ROADMAP.md) row 3 |
| E2 | The S3 sensor's listing keeps objects after a timestamp cursor and lists again when the cursor is gone; the run key stops a duplicate. | A listing double with two objects sharing one modification time, a cursor no longer listed, and an object with no modification time (`dag_tools/components/s3_sensor/utils.py:6-52`) | [S3 sensor](components/s3-sensor.md), findings 3, 12 |
| E3 | The S3 sensor starts stopped, and any value other than `RUNNING` means stopped. | Build the sensor with no status, with `RUNNING` and with a misspelling (`sensor_component.py:62-65`, `:73`) | [S3 sensor](components/s3-sensor.md), finding 2 |
| E4 | OTel readiness: the gate itself is tested (`test_otel_api_sync_render.py:347`, `:361`, `:380`); the order of the completion rule, missing timestamps, maximum age and quiet period are not. | `group_readiness` with no parseable timestamps and an unmatched completion rule, and with an unmatched rule, a quiet period of zero and no maximum age (`render.py:75-81`, `:98-99`) | [OTel API sync](components/otel-api-sync.md), finding 7 |
| E5 | The OTel asset-metadata ledger stops re-sends. | Fill the ledger past its limit and assert which entries survive, and make the read-back raise (`ledger.py:39`, `:79`, `:83-97`) | [OTel API sync](components/otel-api-sync.md), finding 7 |
| E6 | The OTel SQL ledger records plans for a staged pipeline. | Load and flush it against a database (`ledger.py:104-158`) | [OTel API sync](components/otel-api-sync.md), gaps |
| E7 | The OTel oversized-plan refusal fails the run before anything is sent. | A component test with a plan over the size limit (`component.py:625-644`) | [OTel API sync](components/otel-api-sync.md), finding 7 |
| E8 | The Restate dlt sync's scoped read-back falls back to acknowledging the whole table when the scoped read raises. | A destination lacking the load-id column; assert the fallback and that the mark does not move (`restate_dlt_sync/component.py:573-589`) | [restate dlt sync](components/restate-dlt-sync.md), finding 7 |
| E9 | Restate API sync sends each row's key and routes a local address in-cluster. | A key column whose case differs, and a local address on another port (`restate_api_sync/component.py:133-142`, `:146`) | [restate API sync](components/restate-api-sync.md), findings 5, 8 |
| E10 | §4r.3: the exact-decimal tolerance path raises on number text that is not finite. | Feed overflowing text to the tolerance path on the embedded database and assert the error (`sql.py:364-379`, the decimal tolerance arms; the statement is at `:201-225`) | [§4r.3](decisions/dag-tools-s4r-s4l-s4m.md) |
| E11 | §4m.5: a job built from an all-assets selection does not include the observable source asset. | Build that job over the example and assert it has zero steps. Today this is only documented. | [§4m.5](decisions/dag-tools-s4r-s4l-s4m.md) |
| E12 | §4l.5, §4m.17: no dimension id, value or database object is named in the warehouse source package or its components. | A vocabulary scan like `test_no_deployment_vocabulary_appears_in_the_library`, over those packages | [§4l.5](decisions/dag-tools-s4r-s4l-s4m.md) |
| E13 | §4s.6: the pool per destination is tested (`test_document_tables_component.py:383`); the duplicate that two writers on one bundle would produce is not. | Two writers interleaved on one bundle, asserting the duplicate, plus a concurrent-append count | [§4s.6](decisions/dag-tools-s4s.md) |
| E14 | §4s.2: a base install without the documents extra still imports the package. | In an environment without the extra, import the package and assert the lazy export fails only when used | [§4s.2](decisions/dag-tools-s4s.md) |
| E15 | §4n.8: a derived dataset cannot be another's source. | Declare a chain over the example and assert what happens. Which outcome happens, a refusal or a missing label set, is not established. | [§4n.8](decisions/dag-tools-s4n-s4o-s4k1.md) |
| E16 | §4n.9: a catalog block with only comments under it parses as null and fails the load. | Load the component with such a block and assert the load fails naming the attribute | [§4n.9](decisions/dag-tools-s4n-s4o-s4k1.md) |
| E17 | §4n.6: no token key is sent when there is no token. | An empty-token test for this component, like `test_an_empty_token_is_never_sent_as_a_credential` | [§4n.6](decisions/dag-tools-s4n-s4o-s4k1.md) |
| E18 | §4k.5: a contract stream holding no document is refused with its own message. | Load an all-empty stream and assert that refusal (`component.py:135`) | [§4k.5](decisions/dag-tools-s4k-s4j.md) |

## Not a built claim

- `S3ToArrowComponent` fails when its definitions are built ([S3 sensor](components/s3-sensor.md), finding 10). A test that only builds its definitions would have caught it. Once the component is fixed, that test would give it a test. It is not a built claim, so it is not in the tables.

## Where "no test" is written

Every evidence cell that says "no test" is a further candidate for group E. The count below is of lines containing the words in any case, per record, so it is an upper bound on the cells. Lines in this list are not counted. Fifty records carry 281 such lines.

| Record | Lines | Record | Lines |
|---|---|---|---|
| [Grist ingest](components/grist-ingest.md) | 17 | [tagged warehouse](components/tagged-warehouse.md) | 3 |
| [S3 sensor](components/s3-sensor.md) | 17 | [SystemLink source](components/systemlink-source.md) | 2 |
| [restate API sync](components/restate-api-sync.md) | 16 | [Redpanda](components/redpanda.md) | 1 |
| [OTel API sync](components/otel-api-sync.md) | 14 | [big data](flows/big-data.md) | 3 |
| [dlt pipeline](components/dlt-pipeline.md) | 9 | [need to know](flows/need-to-know.md) | 2 |
| [Restate worker](components/restate-worker.md) | 9 | [evidence walkthrough](walkthroughs/evidence.md) | 1 |
| [classification propagation](components/classification-propagation.md) | 8 | [not-established list](not-established.md) | 1 |
| [contract mapping](components/contract-mapping.md) | 8 | [overview](overview.md) | 1 |
| [mesh data client](components/mesh-data-client.md) | 8 | [roadmap](ROADMAP.md) | 7 |
| [restate dlt sync](components/restate-dlt-sync.md) | 8 | [invincible-agent ADR-0001 to 0024](decisions/invincible-agent-adr-0001-0024.md) | 15 |
| [Restate server](components/restate-server.md) | 8 | [invincible-agent ADR-0025 to 0048](decisions/invincible-agent-adr-0025-0048.md) | 10 |
| [DataHub lineage](components/datahub-lineage.md) | 7 | [invincible-agent ADR-0049 to 0072](decisions/invincible-agent-adr-0049-0072.md) | 8 |
| [DataHub](components/datahub.md) | 6 | [dag-tools §4r, §4l, §4m](decisions/dag-tools-s4r-s4l-s4m.md) | 8 |
| [dbt project](components/dbt-project.md) | 6 | [dag-tools §4n, §4o, §4k-1](decisions/dag-tools-s4n-s4o-s4k1.md) | 6 |
| [warehouse source](components/warehouse-source.md) | 6 | [dag-tools §1 to §3](decisions/dag-tools-s1-s3.md) | 5 |
| [central gateway](components/central-gateway.md) | 5 | [dag-tools §4s](decisions/dag-tools-s4s.md) | 5 |
| [document tables](components/document-tables.md) | 5 | [dag-tools §4a otel](decisions/dag-tools-s4a-otel.md) | 4 |
| [Topaz](components/topaz.md) | 5 | [dag-tools §4k, §4j](decisions/dag-tools-s4k-s4j.md) | 4 |
| [domain broker](components/domain-broker.md) | 4 | [dag-tools §4 Restate](decisions/dag-tools-s4-restate.md) | 2 |
| [JupyterHub](components/jupyterhub.md) | 4 | [dag-tools §4e, §4a, §7](decisions/dag-tools-s4e-s4a-s7.md) | 1 |
| [Keycloak](components/keycloak.md) | 4 | [dag-tools §4g1, §4g, §4f](decisions/dag-tools-s4g1-s4g-s4f.md) | 1 |
| [MinIO](components/minio.md) | 4 | [dag-tools §4i, §4h, §4g](decisions/dag-tools-s4i-s4h-s4g.md) | 1 |
| [Dagster](components/dagster.md) | 3 | [dag-tools §4q](decisions/dag-tools-s4q.md) | 1 |
| [read audit](components/read-audit.md) | 3 | [dag-tools §5, §6](decisions/dag-tools-s5-s6.md) | 1 |
| [Superset](components/superset.md) | 3 | [pub-tools](decisions/pub-tools.md) | 1 |

## Not established

- Whether B1's throughput target is met. Continuous integration records a rate in each Python job and does not assert the target.
- Whether invincible-agent's own continuous integration runs the tests of A4, D3 to D10, D12 to D15 and three parts of D16, and with what result. That is the owning repository's to say.
- For each "no test" line in the table above, which candidate test would cover it. They are counted, not yet sorted.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| What `measured` requires | [the README](README.md#status-vocabulary); the architect's ruling of 2026-11-19 | — | the vocabulary table | `accepted` |
| The CI jobs, and the end-to-end job's failure on a skip | `.github/workflows/test.yml` at `9d89568a` | — | `:27-28`, `:86-90`; `:117-118`, `:224-227`; `:245-246`, `:277-289` | `built` |
| The runs and their outcomes | the "Tests" workflow, run 44398336497 at `9d89568a` and run 44451597250 at `add5bc48`: every job succeeded in both. Per-test outcomes come from each Python job's uploaded results and the 1.10.19 job's log. | — | the table under "Measured in CI, in process" | `measured`, in CI, in process |
| Groups A to E | the records named in each row | as each row | as each row | `built`, as each record |
| Three group-D tests replace a live service with a fake | the tests at their pins: each replaces the live service with a fake or has no live gate | — | D8, D11, D16 | `built` |

### Rows that carry no status

These rows count or point at records, or name a decision that is superseded or decides nothing here. None is a claim about the mesh, so none carries a status.

| Claim | Source | Decision | Evidence |
|---|---|---|---|
| No record but this list carries `measured` | the status columns of the records in this directory | — | none found outside this list |
| 281 "no test" lines in fifty records | the records in this directory, except this list | — | a count of the lines containing the words in any case |
