"""A real code server reports its image through the client's own workspace query.

The chart sets ``DAGSTER_CURRENT_IMAGE``; the identity check is only worth
anything if that reaches ``displayMetadata`` under the key ``image`` on every
Dagster version this library supports. That is a fact about Dagster, so it is
measured here against a real ``dagster api grpc`` process and the real GraphQL
schema, not asserted against a stub.
"""
import os
import socket
import subprocess
import sys
import time
from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_graphql")
pytest.importorskip("grpc")
pytest.importorskip("httpx")

from dagster import DagsterInstance
from dagster._core.workspace.context import (
    WorkspaceProcessContext,
    WorkspaceRequestContext,
)
from dagster._core.workspace.load_target import GrpcServerTarget
from dagster_graphql.test.utils import execute_dagster_graphql

from dag_tools.qual.graphql import DagsterGraphQLClient

DIGEST = "sha256:" + "c" * 64
IMAGE = f"registry.example.com:5000/team/app:sha-abcdef0@{DIGEST}"

_DEFS = '''
from dagster import Definitions, asset

@asset
def one():
    return 1

defs = Definitions(assets=[one])
'''


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for(port, proc, timeout=90.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"dagster api grpc exited early ({proc.returncode})")
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.3)
    raise RuntimeError("dagster api grpc did not open its port")


def test_dagster_reports_the_container_image_through_the_workspace_query(tmp_path):
    defs = tmp_path / "defs.py"
    defs.write_text(_DEFS, encoding="utf-8")
    port = _free_port()
    env = dict(os.environ)
    env["DAGSTER_CURRENT_IMAGE"] = IMAGE
    env["DAGSTER_HOME"] = str(tmp_path / "home")
    (tmp_path / "home").mkdir()
    log = (tmp_path / "grpc.log").open("wb")
    proc = subprocess.Popen(
        [sys.executable, "-m", "dagster", "api", "grpc",
         "--python-file", str(defs), "--host", "127.0.0.1", "--port", str(port)],
        env=env, stdout=log, stderr=subprocess.STDOUT, cwd=str(tmp_path),
    )
    try:
        _wait_for(port, proc)
        (tmp_path / "inst").mkdir()
        target = GrpcServerTarget(
            host="127.0.0.1", port=port, socket=None, location_name="e2e",
        )
        with DagsterInstance.local_temp(str(tmp_path / "inst")) as instance:
            with WorkspaceProcessContext(instance, target, version="") as wpc:
                ctx = WorkspaceRequestContext(
                    instance=instance,
                    current_workspace=wpc.get_current_workspace(),
                    process_context=wpc,
                    version=wpc.version,
                    source=None,
                    read_only=False,
                    read_only_locations=None,
                )
                result = execute_dagster_graphql(
                    ctx, DagsterGraphQLClient.LOCATIONS_QUERY
                )
        assert not result.errors, result.errors

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"data": result.data}
        http = MagicMock()
        http.post.return_value = resp
        client = DagsterGraphQLClient("http://x/graphql", http=http)
        (loc,) = client.get_code_locations()
        assert loc.name == "e2e" and loc.load_status == "LOADED"
        assert loc.display_metadata.get("image") == IMAGE
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=20)
        log.close()
