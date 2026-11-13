"""A stdlib fake of the three SystemLink read routes, for tests that must not touch a network.

It accepts exactly ONE spelling of ``orderBy`` and ONE form of the step filter and answers 400 to the
other, so a test can prove that the matching setting works and the mismatching one is a hard error.
Values are synthetic. It records every request (route, body, headers) and the peak number of
requests in flight.
"""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional

API_PATH = "/nitestmonitor/v2"


def make_result(i: int, part: str = "PN-1000-A", status: str = "PASSED", updated: Optional[str] = None,
                **extra: Any) -> Dict[str, Any]:
    row = {
        "id": f"R-{part[-1]}{i:04d}",
        "partNumber": part,
        "programName": "prog_functional",
        "serialNumber": f"SN-{i:04d}",
        "operator": "op-synthetic",
        "status": {"statusType": status},
        "updatedAt": updated or f"2026-01-01T00:{i // 60:02d}:{i % 60:02d}Z",
        "workspace": "ws-1",
        "properties": {"station": "S1"},
        "keywords": ["k1"],
    }
    row.update(extra)
    return row


def make_step(result_id: str, n: int = 0) -> Dict[str, Any]:
    return {
        "resultId": result_id, "stepId": f"{result_id}-S{n}", "name": f"step{n}",
        "inputs": [{"name": "limit", "value": "1"}], "outputs": [{"name": "reading", "value": "0.5"}],
        "data": {"parameters": [{"name": "p", "value": "v"}]},
    }


class FakeSystemLink:
    def __init__(self, results=(), steps=(), products=(), *, accept_order_by="UPDATED_AT",
                 accept_step_form="or_chain", api_key="test-key-0000", page_cap=None) -> None:
        self.results: List[Dict[str, Any]] = list(results)
        self.steps: List[Dict[str, Any]] = list(steps)
        self.products: List[Dict[str, Any]] = list(products)
        self.accept_order_by, self.accept_step_form = accept_order_by, accept_step_form
        self.api_key, self.page_cap = api_key, page_cap
        self.requests: List[Dict[str, Any]] = []
        self.failures: List[tuple] = []  # (route, status) consumed in order
        self.step_batch_limit: Optional[int] = None  # 504 above this many ids
        self.empty_200_routes: set = set()
        self.retry_after: Optional[str] = None
        self.delay_s = 0.0
        self._lock = threading.Lock()
        self._inflight = 0
        self.peak_inflight = 0
        self._server: Optional[ThreadingHTTPServer] = None

    # ------------------------------------------------------------------ lifecycle
    def __enter__(self) -> "FakeSystemLink":
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # silence
                pass

            def do_POST(self):
                fake._handle(self)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc) -> None:
        self._server.shutdown()
        self._server.server_close()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def routes_seen(self) -> set:
        return {r["route"] for r in self.requests}

    # ------------------------------------------------------------------ handling
    def _send(self, h, status: int, payload: Any = None, headers: Optional[Dict[str, str]] = None) -> None:
        raw = json.dumps(payload if payload is not None else {}).encode()
        h.send_response(status)
        h.send_header("Content-Type", "application/json")
        h.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            h.send_header(k, v)
        h.end_headers()
        h.wfile.write(raw)

    def _handle(self, h) -> None:
        length = int(h.headers.get("Content-Length") or 0)
        body = json.loads(h.rfile.read(length) or b"{}")
        route = h.path.rsplit("/", 1)[-1]
        with self._lock:
            self._inflight += 1
            self.peak_inflight = max(self.peak_inflight, self._inflight)
            self.requests.append({
                "route": route, "path": h.path, "body": body, "method": "POST",
                "request_id": h.headers.get("x-request-id"),
                "key_header": h.headers.get("x-ni-api-key"),
            })
        try:
            if self.delay_s:
                time.sleep(self.delay_s)
            self._respond(h, route, body)
        finally:
            with self._lock:
                self._inflight -= 1

    def _respond(self, h, route: str, body: Dict[str, Any]) -> None:
        if h.headers.get("x-ni-api-key") != self.api_key:
            return self._send(h, 401, {"error": "unauthorized"})
        with self._lock:
            for i, (r, status) in enumerate(self.failures):
                if r in (route, "*"):
                    del self.failures[i]
                    hdr = {"Retry-After": self.retry_after} if self.retry_after else None
                    return self._send(h, status, {"error": "injected"}, hdr)
        if route in self.empty_200_routes:
            return self._send(h, 200, {})
        if route == "query-results":
            return self._results(h, body)
        if route == "query-steps":
            return self._steps(h, body)
        if route == "query-products":
            return self._products(h, body)
        self._send(h, 404, {})

    def _page(self, rows, body, key):
        take = int(body.get("take") or 100)
        if self.page_cap:
            take = min(take, self.page_cap)
        start = int(body.get("continuationToken") or 0)
        chunk = rows[start : start + take]
        out: Dict[str, Any] = {key: chunk}
        if start + take < len(rows):
            out["continuationToken"] = str(start + take)
        return out

    def _results(self, h, body):
        subs = body.get("substitutions") or []
        if body.get("orderBy") not in (None, self.accept_order_by):
            return self._send(h, 400, {"error": "orderBy"})
        rows = [r for r in self.results if r["partNumber"] == subs[0]]
        if "updatedAt >" in (body.get("filter") or ""):
            rows = [r for r in rows if r["updatedAt"] > subs[1]]
        rows.sort(key=lambda r: r["updatedAt"], reverse=bool(body.get("descending")))
        if body.get("returnCount"):
            return self._send(h, 200, {"results": rows[:1], "totalCount": len(rows)})
        self._send(h, 200, self._page(rows, body, "results"))

    def _steps(self, h, body):
        filt, subs = body.get("filter") or "", body.get("substitutions") or []
        form = "in" if " in " in filt else "or_chain"
        if form != self.accept_step_form:
            return self._send(h, 400, {"error": "filter form"})
        ids = {x for s in subs for x in (s if isinstance(s, list) else [s])}
        if self.step_batch_limit is not None and len(ids) > self.step_batch_limit:
            return self._send(h, 504, {"error": "gateway timeout"})
        rows = [s for s in self.steps if s["resultId"] in ids]
        self._send(h, 200, self._page(rows, body, "steps"))

    def _products(self, h, body):
        subs = set(body.get("substitutions") or [])
        rows = [p for p in self.products if p["partNumber"] in subs]
        self._send(h, 200, self._page(rows, body, "products"))
