"""Chart values for the api_call_plan service's Restate timeouts."""
import pathlib
import shutil
import subprocess

import pytest

yaml = pytest.importorskip("yaml")

CHART = pathlib.Path(__file__).parent.parent / "helm" / "dag-tools"
BASE = ["--set", "restateServer.enabled=true"]

helm = shutil.which("helm")
requires_helm = pytest.mark.skipif(helm is None, reason="helm is not installed")


def _worker_env(*overrides):
    result = subprocess.run(
        [helm, "template", "t", str(CHART), *BASE, *overrides],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    for doc in yaml.safe_load_all(result.stdout):
        if doc and doc.get("kind") == "Deployment" and doc["metadata"]["name"].endswith("restate-worker"):
            container = doc["spec"]["template"]["spec"]["containers"][0]
            return {e["name"]: e.get("value") for e in container.get("env", [])}
    raise AssertionError("no restate-worker Deployment rendered")


@requires_helm
def test_abort_timeout_renders_by_default_and_unset_inactivity_renders_nothing():
    env = _worker_env()
    assert env["API_CALL_PLAN_ABORT_TIMEOUT_SECONDS"] == "3600"
    assert "API_CALL_PLAN_INACTIVITY_TIMEOUT_SECONDS" not in env


@requires_helm
def test_inactivity_timeout_renders_when_set():
    env = _worker_env("--set", "restateWorker.apiCallPlan.inactivityTimeoutSeconds=90")
    assert env["API_CALL_PLAN_INACTIVITY_TIMEOUT_SECONDS"] == "90"


@requires_helm
def test_clearing_the_abort_timeout_restores_restates_default():
    env = _worker_env("--set", "restateWorker.apiCallPlan.abortTimeoutSeconds=null")
    assert "API_CALL_PLAN_ABORT_TIMEOUT_SECONDS" not in env
