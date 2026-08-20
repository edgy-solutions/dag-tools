"""End-to-end: real dbt -> real artifacts -> real `datahub ingest`.

The unit tests in test_dbt_datahub_artifacts.py pin the directory contract
with a fake resource. This one runs the whole thing for real -- postgres in
docker, the actual dbt CLI, the actual DataHub CLI -- because the failure
this guards against is invisible to any test that mocks dbt: every
``DbtCliResource.cli()`` call lands its artifacts in a *different*
``target/<op>-<run>-<uuid>`` directory, so ``datahub ingest`` starts from a
directory that is missing files nobody noticed were missing.

The only stubbed piece is the DataHub GMS server, replaced by an in-process
HTTP recorder. That is deliberate: a real GMS needs Kafka, Elasticsearch and
MySQL, and the bug lives entirely on the *source* side of ingestion -- dbt
artifacts are read and parsed before a single byte reaches the sink. The
recorder still proves ingestion ran to completion and emitted metadata for
both the dbt model and its source table.

Run it with:

    DAGTOOLS_E2E_DBT=1 pytest dag_tools_tests/test_dbt_datahub_e2e.py

It is skipped by default so the normal unit run stays hermetic and fast.
"""

import json
import os
import shutil
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

pytest.importorskip("dagster_dbt")

import dagster as dg  # noqa: E402
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("DAGTOOLS_E2E_DBT"),
    reason="end-to-end dbt/DataHub test; set DAGTOOLS_E2E_DBT=1 (needs docker)",
)

HERE = Path(__file__).parent / "dbt_datahub_e2e"
COMPOSE_FILE = HERE / "docker-compose.yaml"
DBT_PROJECT_DIR = HERE / "dbt_project"
PG_PORT = 55433

SEED_SQL = """
create schema if not exists raw;
drop table if exists raw.orders;
create table raw.orders (
    order_id int,
    customer_id int,
    amount numeric,
    loaded_at timestamp default now()
);
insert into raw.orders (order_id, customer_id, amount) values
    (1, 100, 25.00), (2, 100, 40.50), (3, 200, 12.25);
"""


# ---------------------------------------------------------------------------
# DataHub GMS recorder
# ---------------------------------------------------------------------------


class _GmsRecorder:
    """Minimal stand-in for a DataHub GMS REST endpoint.

    Answers the emitter's `/config` handshake and records every metadata
    change proposal posted to `/aspects`.
    """

    recording = True

    def __init__(self):
        self.proposals = []
        self._server = None
        self._thread = None

    def start(self):
        proposals = self.proposals

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # keep pytest output readable
                pass

            def _reply(self, payload, status=200):
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if self.path.startswith("/config"):
                    self._reply(
                        {
                            "noCode": "true",
                            "statefulIngestionCapable": True,
                            "supportsImpactAnalysis": True,
                            "versions": {"acryldata/datahub": {"version": "v1.3.1"}},
                        }
                    )
                elif self.path.startswith("/aspects/"):
                    # `infer_dbt_schemas` looks up each target-platform
                    # dataset's schemaMetadata. An empty GMS answers 404, and
                    # the client turns that into "no aspect"; answering 200
                    # with an empty body makes it raise GraphError instead.
                    self._reply({}, status=404)
                else:
                    self._reply({})

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                try:
                    proposals.append(json.loads(raw))
                except (ValueError, UnicodeDecodeError):
                    proposals.append({"_raw": raw[:200].decode("utf-8", "replace")})
                self._reply({})

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    @property
    def url(self):
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()

    def urns(self):
        """Every entity URN mentioned in a recorded proposal."""
        found = set()

        def walk(node):
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in ("entityUrn", "urn") and isinstance(value, str):
                        found.add(value)
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(self.proposals)
        return found


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _compose(*args, check=True):
    return subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _port_open(port, host="127.0.0.1"):
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex((host, port)) == 0


@pytest.fixture(scope="module")
def postgres():
    if not shutil.which("docker"):
        pytest.skip("docker is not on PATH")
    _compose("up", "-d", "--wait")
    try:
        deadline = time.time() + 120
        while time.time() < deadline and not _port_open(PG_PORT):
            time.sleep(1)
        assert _port_open(PG_PORT), f"postgres never came up on {PG_PORT}"

        seed = _compose(
            "exec", "-T", "postgres",
            "psql", "-U", "dagtools", "-d", "dagtools", "-v", "ON_ERROR_STOP=1",
            "-c", SEED_SQL,
        )
        assert seed.returncode == 0, seed.stderr
        yield
    finally:
        _compose("down", "-v", check=False)


@pytest.fixture
def gms():
    """The GMS to ingest into: the in-process recorder by default.

    Set ``DAGTOOLS_E2E_DATAHUB_SERVER`` to a real DataHub GMS URL to run
    against a live instance instead. Worth doing at least once per change to
    this path -- a stub can only be as accurate as the author's model of the
    thing it replaces. The trade-off is that a real run leaves dataset
    entities behind, so it stays opt-in and out of CI.
    """
    real = os.getenv("DAGTOOLS_E2E_DATAHUB_SERVER")
    if real:
        yield _RealGms(real)
        return
    recorder = _GmsRecorder().start()
    try:
        yield recorder
    finally:
        recorder.stop()


class _RealGms:
    """Adapter so the test body reads the same against a live GMS."""

    def __init__(self, url):
        self.url = url
        self.recording = False

    def urns(self):
        return set()


@pytest.fixture
def ingest_result(monkeypatch):
    """Record how the real `datahub ingest` process actually exited.

    `_publish_to_datahub` swallows ingestion failures on purpose -- a
    DataHub outage must not fail the dbt build -- and reports them only via
    ``context.log``, which does NOT reach pytest's caplog. So asserting on
    that log line passes even when ingestion produced zero events. Wrap the
    real Popen instead: the CLI still runs for real, we just get to see the
    exit code it swallowed.
    """
    from dag_tools.components.dbt_project import component as component_module

    real_popen = component_module.Popen
    seen = {}

    class _SpyPopen(real_popen):
        def communicate(self, *args, **kwargs):
            out = real_popen.communicate(self, *args, **kwargs)
            seen["returncode"] = self.returncode
            seen["output"] = (out[0] or b"").decode("utf-8", "replace")
            return out

    monkeypatch.setattr(component_module, "Popen", _SpyPopen)
    return seen


@pytest.fixture
def dbt_project(tmp_path):
    """A throwaway copy of the fixture project, so target/ never dirties git."""
    project_dir = tmp_path / "dbt_project"
    shutil.copytree(DBT_PROJECT_DIR, project_dir)
    project = DbtProject(project_dir=project_dir, profiles_dir=project_dir)
    project.preparer.prepare(project)
    return project


def _component(datahub_server):
    from dag_tools.components.dbt_project.component import CustomDbtProjectComponent

    comp = CustomDbtProjectComponent.__new__(CustomDbtProjectComponent)
    comp.datahub_config = {"server": datahub_server}
    comp.k8s_resource_env_prefix = None
    comp.k8s_default_cpu = "500m"
    comp.k8s_default_mem = "1Gi"
    comp.op = None
    comp.select = "fqn:*"
    comp.exclude = ""
    comp.selector = ""
    # cli_args resolution is a base-class concern with its own contextvar;
    # this test is about the artifact handoff, so pin the default directly.
    comp.get_cli_args = lambda context: ["build"]
    return comp


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------


def test_dbt_build_publishes_to_datahub_end_to_end(
    postgres, gms, dbt_project, ingest_result
):
    """The whole path: freshness -> build -> docs generate -> datahub ingest.

    Before the target-path fix, `datahub ingest` aborted here with a
    missing-file error on sources.json, because `source snapshot-freshness`
    wrote it into a different target directory.
    """
    assert shutil.which("datahub"), "the datahub CLI must be installed"

    comp = _component(gms.url)

    @dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
    def e2e_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        # Mirror what DbtProjectComponent's generated asset fn does.
        yield from comp.execute(context=context, dbt=dbt)

    result = dg.materialize(
        [e2e_dbt_assets],
        resources={"dbt": DbtCliResource(project_dir=dbt_project)},
    )
    assert result.success, "the dbt build itself failed"

    # --- every artifact the recipe names landed in one directory ----------
    target_root = Path(dbt_project.project_dir) / "target"
    run_dirs = [
        d for d in target_root.iterdir() if d.is_dir() and (d / "recipe.yaml").exists()
    ]
    assert len(run_dirs) == 1, f"expected exactly one ingest directory, got {run_dirs}"
    run_dir = run_dirs[0]

    for artifact in (
        "manifest.json",
        "catalog.json",
        "sources.json",
        "run_results_build.json",
    ):
        assert (run_dir / artifact).exists(), (
            f"{artifact} is missing from the directory datahub ingest runs in "
            f"({run_dir}); it holds {sorted(p.name for p in run_dir.iterdir())}"
        )

    # --- ingestion actually succeeded -------------------------------------
    assert ingest_result.get("returncode") == 0, (
        "`datahub ingest` exited non-zero (the component swallows this by "
        f"design, so only the spy sees it):\n{ingest_result.get('output')}"
    )
    output = ingest_result["output"]
    assert "produced 0 events" not in output, (
        f"ingestion reported success but emitted nothing:\n{output}"
    )

    if not gms.recording:
        return  # live GMS: nothing captured locally to assert against

    # --- ...and emitted metadata for both the model and its source -------
    urns = gms.urns()
    assert urns, f"datahub ingest emitted nothing; output:\n{output}"

    joined = " ".join(sorted(urns))
    assert "customer_orders" in joined, (
        f"the dbt model never reached DataHub; emitted urns: {sorted(urns)}"
    )
    assert "orders,PROD" in joined or "raw.orders" in joined, (
        "the dbt *source* never reached DataHub, which is exactly what "
        f"sources.json carries; emitted urns: {sorted(urns)}"
    )
