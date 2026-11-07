"""Docker harness for running the real ApiCallPlanService against a real Restate.

Not a test module (no ``test_`` prefix). Used by
``test_restate_concurrency_e2e.py`` (opt-in, ``DAGTOOLS_E2E_RESTATE=1``) and
by the pre-merge gate script, so both exercise the same stack:

* a Restate server container (default 1.6.2, the version deployments pin);
* a worker container running ``python -m dag_tools.restate_handlers.serve``
  with ``RESTATE_SERVICES=api_call_plan``, the repository mounted read-only
  and put on ``PYTHONPATH`` so it runs the working tree, not a published image;
* a fake target API on the host that logs a key for every request it
  receives and counts requests in flight.

Everything the worker needs is wired through env vars, as in the chart.
"""
from __future__ import annotations

import json
import pathlib
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional

REPO = pathlib.Path(__file__).resolve().parent.parent
WORKER_IMAGE = "dagtools-api-call-plan-e2e:v3"
RESTATE_IMAGE = "restatedev/restate:1.6.2"
# restate-sdk is pinned to the version the measurements were taken with.
_DOCKERFILE = """\
FROM python:3.12-slim
RUN pip install --no-cache-dir restate-sdk==0.14.2 hypercorn requests httpx pyyaml oracledb pydantic pydantic-settings sqlalchemy psycopg2-binary jinja2
WORKDIR /app
"""


def docker(*args: str, check: bool = True, input: Optional[str] = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, encoding="utf-8", errors="replace", check=check, input=input
    )


def docker_available() -> bool:
    try:
        return docker("info", check=False).returncode == 0
    except FileNotFoundError:
        return False


def ensure_worker_image() -> None:
    if docker("image", "inspect", WORKER_IMAGE, check=False).returncode != 0:
        docker("build", "-t", WORKER_IMAGE, "-", input=_DOCKERFILE)


class FakeApi:
    """A target API on the host that logs a key per request.

    ``latency(in_flight)`` returns the seconds to hold a request, given how
    many requests (this one included) are in flight when it arrives. The
    request body must carry ``{"key": ...}``.
    """

    def __init__(self, latency: Callable[[int], float], port: int = 0):
        self.latency = latency
        self._lock = threading.Lock()
        self.in_flight = 0
        self.max_in_flight = 0
        self.keys: List[str] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                with outer._lock:
                    outer.in_flight += 1
                    outer.max_in_flight = max(outer.max_in_flight, outer.in_flight)
                    outer.keys.append(str(body.get("key")))
                    n = outer.in_flight
                try:
                    time.sleep(outer.latency(n))
                finally:
                    with outer._lock:
                        outer.in_flight -= 1
                payload = b"{}"
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self._httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        self._httpd.daemon_threads = True
        self.port = self._httpd.server_address[1]
        threading.Thread(target=self._httpd.serve_forever, daemon=True).start()

    def duplicates(self) -> Dict[str, int]:
        with self._lock:
            counts: Dict[str, int] = {}
            for key in self.keys:
                counts[key] = counts.get(key, 0) + 1
        return {k: v for k, v in counts.items() if v > 1}

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()


class Stack:
    """Restate + one api_call_plan worker, on a throwaway docker network."""

    def __init__(
        self,
        worker_env: Optional[Dict[str, str]] = None,
        restate_image: str = RESTATE_IMAGE,
    ):
        self.id = uuid.uuid4().hex[:8]
        self.network = f"dtcap-{self.id}"
        self.restate = f"dtcap-restate-{self.id}"
        self.worker = f"dtcap-worker-{self.id}"
        self.worker_env = dict(worker_env or {})
        self.restate_image = restate_image
        self.ingress = ""
        self.admin = ""

    # -- lifecycle -----------------------------------------------------------

    def up(self) -> "Stack":
        ensure_worker_image()
        docker("network", "create", self.network)
        docker(
            "run", "-d", "--name", self.restate, "--network", self.network,
            "-p", "127.0.0.1::8080", "-p", "127.0.0.1::9070",
            self.restate_image,
        )
        self.ingress = f"http://127.0.0.1:{self._host_port(self.restate, 8080)}"
        self.admin = f"http://127.0.0.1:{self._host_port(self.restate, 9070)}"
        self._wait_http(f"{self.admin}/health", 60)

        env = {
            "RESTATE_SERVICES": "api_call_plan",
            "RESTATE_ADMIN_URL": f"http://{self.restate}:9070",
            "RESTATE_ADVERTISED_URI": f"http://{self.worker}:9080",
            "PYTHONPATH": "/src",
            "PYTHONUNBUFFERED": "1",
            **self.worker_env,
        }
        args = ["run", "-d", "--name", self.worker, "--network", self.network,
                "--add-host", "host.docker.internal:host-gateway",
                "-v", f"{REPO}:/src:ro"]
        for key, value in env.items():
            args += ["-e", f"{key}={value}"]
        args += [WORKER_IMAGE, "python", "-m", "dag_tools.restate_handlers.serve"]
        docker(*args)
        self._wait_registered(120)
        return self

    def down(self) -> None:
        for name in (self.worker, self.restate):
            docker("rm", "-f", name, check=False)
        docker("network", "rm", self.network, check=False)

    def __enter__(self) -> "Stack":
        try:
            return self.up()
        except BaseException:
            self.down()  # a half-started stack must not leak containers
            raise

    def __exit__(self, *exc) -> None:
        self.down()

    def worker_logs(self) -> str:
        out = docker("logs", self.worker, check=False)
        return out.stdout + out.stderr

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _host_port(container: str, port: int) -> int:
        out = docker("port", container, str(port)).stdout.strip().splitlines()[0]
        return int(out.rsplit(":", 1)[1])

    @staticmethod
    def _wait_http(url: str, timeout: float) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                urllib.request.urlopen(url, timeout=3).read()
                return
            except Exception:
                time.sleep(0.5)
        raise TimeoutError(url)

    def _wait_registered(self, timeout: float) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                body = json.loads(urllib.request.urlopen(f"{self.admin}/services", timeout=3).read())
                if any(s.get("name") == "ApiCallPlanService" for s in body.get("services", [])):
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise TimeoutError("ApiCallPlanService never registered:\n" + self.worker_logs()[-2000:])

    # -- driving -------------------------------------------------------------

    def send(self, group: str, plan: Dict[str, Any]) -> int:
        request = urllib.request.Request(
            f"{self.ingress}/ApiCallPlanService/{group}/execute_plan/send",
            data=json.dumps(plan).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            return urllib.request.urlopen(request, timeout=30).status
        except urllib.error.HTTPError as exc:
            return exc.code

    def query(self, sql: str) -> List[Dict[str, Any]]:
        request = urllib.request.Request(
            f"{self.admin}/query",
            data=json.dumps({"query": sql}).encode(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        return json.loads(urllib.request.urlopen(request, timeout=30).read())["rows"]

    def invocations(self) -> List[Dict[str, Any]]:
        return self.query(
            "SELECT id, target_service_key, status, retry_count, completion_result "
            "FROM sys_invocation WHERE target_service_name = 'ApiCallPlanService'"
        )

    def status_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for row in self.invocations():
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        return counts


def plan_for(group: str, api_port: int, **api: Any) -> Dict[str, Any]:
    """One group, one POST to the fake API, carrying the group as its key."""
    from dag_tools.restate_handlers.api_call_plan import PLAN_FORMAT_VERSION

    return {
        "format_version": PLAN_FORMAT_VERSION,
        "plan_id": f"{group}:h",
        "group_key": group,
        "plan_hash": f"h-{group}",
        "api": {
            "base_url": f"http://host.docker.internal:{api_port}",
            "headers": {},
            "header_env": {},
            "timeout_seconds": 30,
            "retry_statuses": [429, 500, 502, 503, 504],
            **api,
        },
        "steps": [
            {
                "id": "s",
                "calls": [
                    {
                        "item_key": "single",
                        "method": "POST",
                        "path": "/ingest",
                        "headers": {},
                        "body": {"key": group},
                        "call_key": f"ck-{group}",
                        "on_status": {},
                        "fragments": {},
                    }
                ],
                "aggregate_fallbacks": [],
            }
        ],
    }


def dispatch_all(stack: Stack, plans: List[Dict[str, Any]], workers: int = 16) -> None:
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(workers) as pool:
        codes = list(pool.map(lambda p: stack.send(p["group_key"], p), plans))
    bad = [c for c in codes if c not in (200, 202)]
    if bad:
        raise RuntimeError(f"dispatch rejected for {len(bad)} groups: {sorted(set(bad))}")
