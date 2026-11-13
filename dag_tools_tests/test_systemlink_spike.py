"""The SystemLink spike probe: read-only, secret-free, data-free.

A synthetic server (stdlib ThreadingHTTPServer) records every request. Each
case has a control that differs in exactly one thing.
"""
import copy
import json
import logging
import re
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import yaml

from dag_tools.systemlink import spike

KEY = "KEY-LEAK-90210"
PN = "PN-1000-A"
WS = "ws-leak-5521"
VERSION = "2.3.4"
DEFAULT_SWAGGER = "/nitestmonitor/swagger/v2/nitestmonitor-v2.yml"
JSON_SWAGGER = "/nitestmonitor/swagger/v2/nitestmonitor-v2.json"
ALLOWED_POSTS = {"/nitestmonitor/v2/query-results", "/nitestmonitor/v2/query-steps",
                 "/nitestmonitor/v2/query-products"}


# ----------------------------------------------------------------- the document

def _openapi3() -> Dict[str, Any]:
    s = "#/components/schemas/"

    def op(ref: str) -> Dict[str, Any]:
        return {"post": {"requestBody": {"content": {"application/json": {"schema": {"$ref": s + ref}}}}}}

    def derived(order: str, proj: str) -> Dict[str, Any]:
        return {"allOf": [{"$ref": s + "QueryBase"},
                          {"type": "object", "properties": {
                              "orderBy": {"$ref": s + order},
                              "projection": {"type": "array", "items": {"$ref": s + proj}}}}]}

    schemas = {
        "QueryBase": {"type": "object", "properties": {
            "filter": {"type": "string"}, "substitutions": {"type": "array"},
            "take": {"type": "integer", "maximum": 10000},
            "continuationToken": {"type": "string"},
            "returnCount": {"type": "boolean"}, "descending": {"type": "boolean"}}},
        "ResultsOrderBy": {"type": "string", "enum": ["ID", "UPDATED_AT", "STARTED_AT"]},
        "ResultsProjection": {"type": "string", "enum": ["ID", "PART_NUMBER"]},
        "StepsOrderBy": {"type": "string", "enum": ["ID", "STEP_NAME"]},
        "StepsProjection": {"type": "string", "enum": ["ID", "NAME"]},
        "QueryResultsRequest": derived("ResultsOrderBy", "ResultsProjection"),
        "QueryStepsRequest": derived("StepsOrderBy", "StepsProjection"),
        "QueryProductsRequest": {"type": "object", "properties": {
            "filter": {"type": "string"}, "substitutions": {"type": "array"},
            "take": {"type": "integer"}, "returnCount": {"type": "boolean"},
            "orderBy": {"type": "string", "enum": ["ID", "PART_NUMBER"]}}},
    }
    return {
        "openapi": "3.0.1",
        "info": {"title": "synthetic", "version": VERSION},
        "paths": {
            "/nitestmonitor/v2/query-results": op("QueryResultsRequest"),
            "/nitestmonitor/v2/query-steps": op("QueryStepsRequest"),
            "/nitestmonitor/v2/query-products": op("QueryProductsRequest"),
            "/nitestmonitor/v2/delete-results": {"post": {}},
        },
        "components": {"schemas": schemas},
    }


def _swagger2(doc: Dict[str, Any]) -> Dict[str, Any]:
    text = json.dumps(doc).replace("#/components/schemas/", "#/definitions/")
    out = json.loads(text)
    out.pop("openapi")
    out["swagger"] = "2.0"
    out["definitions"] = out.pop("components")["schemas"]
    for item in out["paths"].values():
        post = item["post"]
        if post:
            schema = post.pop("requestBody")["content"]["application/json"]["schema"]
            post["parameters"] = [{"in": "body", "name": "body", "schema": schema}]
    return out


# ------------------------------------------------------------------ the server

class Fake:
    def __init__(self, part_number=PN, reject_in=False, reject_contains=False,
                 share_step_ids=False, shuffle_pages=False, max_take=None,
                 unauthorized=False, descending_broken=False, swagger2=False,
                 swagger_at=DEFAULT_SWAGGER):
        self.__dict__.update(locals())
        self.seen: List[Dict[str, Any]] = []
        self.walks = 0
        self.results = [{"id": f"rid-7f3a-{n:04d}", "partNumber": part_number,
                         "programName": "prog_functional", "serialNumber": "SN-0001",
                         "status": "Passed", "updatedAt": f"2026-01-0{n}T00:00:00Z"}
                        for n in range(1, 5)]
        self.results.append({"id": "rid-other-9999", "partNumber": "PN-OTHER", "programName": "x",
                             "serialNumber": "SN-0002", "status": "Failed",
                             "updatedAt": "2026-02-01T00:00:00Z"})
        self.steps = []
        for r in self.results:
            for i in (1, 2):
                sid = f"step-{i}" if share_step_ids else f"{r['id']}-step-{i}"
                self.steps.append({"stepId": sid, "resultId": r["id"], "name": f"n{i}"})
        self.doc = _openapi3()
        if swagger2:
            self.doc = _swagger2(self.doc)

    # -- behaviour
    def answer(self, route: str, body: Dict[str, Any]):
        filt, subs = body.get("filter", ""), body.get("substitutions", [])
        if self.reject_in and " in @" in filt:
            return 400, {"error": "in"}
        if self.reject_contains and ".Contains(" in filt:
            return 400, {"error": "contains"}
        take = body.get("take", 100)
        if self.max_take is not None and take > self.max_take:
            return 400, {"error": "take"}
        ws = re.search(r"workspace == @(\d+)", filt)
        ws_idx = int(ws.group(1)) if ws else None
        if route == "query-steps":
            refs = {int(x) for x in re.findall(r"@(\d+)", filt)} - {ws_idx}
            ids: List[Any] = []
            for i in sorted(refs):
                ids.extend(subs[i] if isinstance(subs[i], list) else [subs[i]])
            rows = [s for s in self.steps if s["resultId"] in ids]
            key = "steps"
        elif route == "query-products":
            rows = [{"id": "prod-1", "partNumber": subs[0]}] if "partNumber" in filt else []
            key = "products"
        else:
            rows = [r for r in self.results if r["partNumber"] == subs[0]]
            key = "results"
            if body.get("orderBy"):
                rows = sorted(rows, key=lambda r: r["updatedAt"])
                if body.get("descending") and not self.descending_broken:
                    rows.reverse()
        total = len(rows)
        token = None
        if key == "results" and take:
            cursor = body.get("continuationToken")
            if cursor:
                off, rev = cursor.split(":")
                off, rev = int(off), rev == "1"
            else:
                off = 0
                self.walks += 1
                rev = self.shuffle_pages and self.walks % 2 == 0
            ordered = rows if body.get("orderBy") else sorted(rows, key=lambda r: r["id"], reverse=rev)
            rows = ordered[off:off + take]
            if off + take < total:
                token = f"{off + take}:{int(rev)}"
        else:
            rows = rows[:take]
        if body.get("projection"):
            keep = {p.lower().replace("_", "") for p in body["projection"]}
            rows = [{k: v for k, v in r.items() if k.lower() in keep} for r in rows]
        out: Dict[str, Any] = {key: rows, "continuationToken": token}
        if body.get("returnCount"):
            out["totalCount"] = total
        return 200, out


@pytest.fixture
def make_server():
    started = []

    def factory(**kw) -> Fake:
        fake = Fake(**kw)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _reply(self, code: int, payload: Any, raw: Optional[str] = None):
                data = (raw if raw is not None else json.dumps(payload)).encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _gate(self, body):
                fake.seen.append({"method": self.command, "path": self.path, "body": body,
                                  "key": self.headers.get("x-ni-api-key")})
                if fake.unauthorized or self.headers.get("x-ni-api-key") != KEY:
                    self._reply(401, {})
                    return False
                return True

            def do_GET(self):
                if not self._gate(None):
                    return
                if self.path == fake.swagger_at:
                    raw = (json.dumps(fake.doc) if self.path.endswith(".json")
                           else yaml.safe_dump(fake.doc))
                    self._reply(200, None, raw)
                else:
                    self._reply(404, {})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])) or b"{}")
                if not self._gate(body):
                    return
                if self.path not in ALLOWED_POSTS:
                    self._reply(404, {})
                    return
                code, payload = fake.answer(self.path.rsplit("/", 1)[1], body)
                self._reply(code, payload)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        started.append(server)
        fake.url = f"http://127.0.0.1:{server.server_address[1]}"
        fake.host = f"127.0.0.1:{server.server_address[1]}"
        return fake

    yield factory
    for s in started:
        s.shutdown()
        s.server_close()


# --------------------------------------------------------------------- harness

def _component(tmp_path: Path, connection: Dict[str, Any]) -> Path:
    path = tmp_path / "component.yaml"
    path.write_text(yaml.safe_dump({"type": "dag_tools.DltPipelineComponent", "attributes": {
        "source_config": {"type": "systemlink_test_monitor", "source_id": "x",
                          "connection": connection}}}), encoding="utf-8")
    return path


def run(tmp_path, monkeypatch, fake: Fake, *args: str, connection: Optional[Dict[str, Any]] = None,
        part: Optional[str] = PN, key: Optional[str] = KEY):
    monkeypatch.setenv("SYSTEMLINK_BASE_URL", fake.url)
    if key is None:
        monkeypatch.delenv("SYSTEMLINK_API_KEY", raising=False)
    else:
        monkeypatch.setenv("SYSTEMLINK_API_KEY", key)
    conn = {"base_url": "{{ env('SYSTEMLINK_BASE_URL') }}", "api_key_env": "SYSTEMLINK_API_KEY"}
    conn.update(connection or {})
    argv = ["--component", str(_component(tmp_path, conn)), "--out-dir", str(tmp_path / "out"), *args]
    if part:
        argv += ["--part-number", part]
    return spike.main(argv)


def observations(tmp_path) -> Dict[str, Any]:
    return json.loads((tmp_path / "out" / f"{VERSION}.json").read_text())["observations"]


def value(tmp_path, name):
    o = observations(tmp_path)[name]
    return o["value"]


# ------------------------------------------------------------------------ tests

EXPECTED_SWAGGER = {
    "order_by_enum": {"query-results": ["ID", "UPDATED_AT", "STARTED_AT"],
                      "query-steps": ["ID", "STEP_NAME"], "query-products": ["ID", "PART_NUMBER"]},
    "descending_field": {"query-results": True, "query-steps": True, "query-products": False},
    "projection_field": {"query-results": {"declared": True, "enum": ["ID", "PART_NUMBER"]},
                         "query-steps": {"declared": True, "enum": ["ID", "NAME"]},
                         "query-products": {"declared": False, "enum": None}},
    "max_take_declared": {"query-results": 10000, "query-steps": 10000, "query-products": None},
    "return_count_field": {"query-results": True, "query-steps": True, "query-products": True},
}


@pytest.mark.parametrize("swagger2", [False, True], ids=["openapi3", "swagger2"])
def test_swagger_observations_read_correctly(tmp_path, monkeypatch, make_server, swagger2):
    fake = make_server(swagger2=swagger2)
    assert run(tmp_path, monkeypatch, fake) == 0
    obs = observations(tmp_path)
    for name, expected in EXPECTED_SWAGGER.items():
        assert obs[name]["value"] == expected, name
        assert obs[name]["evidence"] == "swagger"
    assert "/nitestmonitor/v2/delete-results" in obs["routes"]["value"]
    assert obs["routes"]["value"] == sorted(obs["routes"]["value"])


def test_the_answering_swagger_path_is_recorded(tmp_path, monkeypatch, make_server, capsys):
    fake = make_server(swagger_at=JSON_SWAGGER)
    assert run(tmp_path, monkeypatch, fake) == 0
    out = json.loads((tmp_path / "out" / f"{VERSION}.json").read_text())
    assert out["server"] == {"version": VERSION, "swagger_path": JSON_SWAGGER}
    assert [r["status"] for r in out["requests"] if r["route"] == "swagger"] == [404, 200]


def test_every_request_is_on_the_allowlist_and_nothing_else_is_sent(tmp_path, monkeypatch, make_server):
    fake = make_server()
    assert run(tmp_path, monkeypatch, fake) == 0
    assert fake.seen
    for r in fake.seen:
        assert (r["method"] == "GET" and r["path"] == DEFAULT_SWAGGER) or \
               (r["method"] == "POST" and r["path"] in ALLOWED_POSTS), r
    assert not any("delete" in r["path"] for r in fake.seen)


def test_the_client_refuses_other_routes_before_any_request(make_server):
    fake = make_server()
    client = spike.ReadOnlyClient(fake.url, KEY, "/nitestmonitor/v2", [DEFAULT_SWAGGER])
    for method, path in [("POST", "/nitestmonitor/v2/delete-results"),
                         ("DELETE", "/nitestmonitor/v2/query-results"),
                         ("GET", "/nitestmonitor/v2/query-results"),
                         ("POST", DEFAULT_SWAGGER)]:
        with pytest.raises(ValueError):
            client._send(method, path, {})
    assert fake.seen == []
    # control: an allowlisted route goes through
    status, _ = client.query("query-products", {"filter": "partNumber == @0", "substitutions": [PN]})
    assert status == 200 and len(fake.seen) == 1


def test_no_secret_and_no_data_reaches_any_output(tmp_path, monkeypatch, make_server, capsys, caplog):
    pn = "PN-LEAK-7731"
    fake = make_server(part_number=pn)
    caplog.set_level(logging.DEBUG)
    rc = run(tmp_path, monkeypatch, fake, connection={"workspace_id": WS}, part=pn)
    assert rc == 0
    cap = capsys.readouterr()
    file_text = (tmp_path / "out" / f"{VERSION}.json").read_text()
    haystack = "\n".join([file_text, cap.out, cap.err, caplog.text])
    secrets = [KEY, pn, WS, fake.host, "SN-0001", "SN-0002", "prog_functional", "Passed",
               "2026-01-01", "2026-01-02", "rid-7f3a", "rid-other", "step-1-", "PN-OTHER"]
    secrets += [r["id"] for r in fake.results] + [s["stepId"] for s in fake.steps]
    for s in secrets:
        assert s not in haystack, s
    # control: the same strings really did travel to / come from the server
    assert any(r["key"] == KEY for r in fake.seen)
    assert any(pn in (r["body"] or {}).get("substitutions", []) for r in fake.seen)
    assert any(WS in (r["body"] or {}).get("substitutions", []) for r in fake.seen)
    assert "descending_honoured" in cap.out
    # no filter text ever carries a value
    filters = [r["body"]["filter"] for r in fake.seen if r["body"]]
    assert filters and not any(pn in f or WS in f or "rid-7f3a" in f for f in filters)


def test_workspace_is_a_substitution_on_every_live_query(tmp_path, monkeypatch, make_server):
    fake = make_server()
    assert run(tmp_path, monkeypatch, fake, connection={"workspace_id": WS}) == 0
    posts = [r["body"] for r in fake.seen if r["method"] == "POST"]
    assert posts and all(re.search(r"workspace == @\d+", b["filter"]) and WS in b["substitutions"]
                         for b in posts)
    control = make_server()
    (tmp_path / "c").mkdir()
    assert run(tmp_path / "c", monkeypatch, control) == 0
    assert not any("workspace" in r["body"]["filter"] for r in control.seen if r["method"] == "POST")


def test_missing_key_exits_2_naming_the_variable(tmp_path, monkeypatch, make_server, capsys):
    fake = make_server()
    assert run(tmp_path, monkeypatch, fake, key=None) == 2
    assert "SYSTEMLINK_API_KEY" in capsys.readouterr().err
    assert fake.seen == []
    assert run(tmp_path, monkeypatch, fake) == 0  # control: set


def test_env_call_resolves_and_other_templates_and_unknown_keys_are_refused(
        tmp_path, monkeypatch, make_server, capsys):
    fake = make_server()
    assert run(tmp_path, monkeypatch, fake) == 0  # control: the env() form resolves
    assert fake.seen
    capsys.readouterr()

    assert run(tmp_path, monkeypatch, fake, connection={"base_url": "{{ other }}"}) == 2
    assert "base_url" in capsys.readouterr().err
    assert run(tmp_path, monkeypatch, fake, connection={"verify_tls_typo": True}) == 2
    assert "verify_tls_typo" in capsys.readouterr().err
    monkeypatch.delenv("SYSTEMLINK_BASE_URL", raising=False)
    path = _component(tmp_path, {"base_url": "{{ env('NOT_SET_ANYWHERE') }}",
                                 "api_key_env": "SYSTEMLINK_API_KEY"})
    assert spike.main(["--component", str(path), "--out-dir", str(tmp_path / "o2")]) == 2
    assert "NOT_SET_ANYWHERE" in capsys.readouterr().err


def test_a_401_exits_3_and_says_auth_failed(tmp_path, monkeypatch, make_server, capsys):
    bad = make_server(unauthorized=True)
    assert run(tmp_path, monkeypatch, bad) == 3
    cap = capsys.readouterr()
    assert "auth_failed" in cap.out + cap.err
    assert KEY not in cap.out + cap.err
    good = make_server()
    assert run(tmp_path, monkeypatch, good) == 0


@pytest.mark.parametrize("shared", [True, False])
def test_step_id_sharing_is_counted(tmp_path, monkeypatch, make_server, shared):
    fake = make_server(share_step_ids=shared)
    assert run(tmp_path, monkeypatch, fake) == 0
    v = value(tmp_path, "step_id_unique")
    assert v["n_steps"] == 8
    if shared:
        assert v["n_step_ids_shared_across_results"] > 0 and v["n_distinct_step_ids"] == 2
    else:
        assert v["n_step_ids_shared_across_results"] == 0 and v["n_distinct_step_ids"] == 8


def test_each_result_id_list_form_is_recorded_separately(tmp_path, monkeypatch, make_server):
    fake = make_server(reject_in=True)
    assert run(tmp_path, monkeypatch, fake) == 0
    v = value(tmp_path, "result_id_list_filter")
    assert v["in"] == {"status": 400, "result_id_set_matches": False, "count_matches_or_form": False}
    assert v["Contains"] == {"status": 200, "result_id_set_matches": True, "count_matches_or_form": True}
    assert v["or_chain"]["status"] == 200
    control = make_server()
    (tmp_path / "c").mkdir()
    assert run(tmp_path / "c", monkeypatch, control) == 0
    c = json.loads((tmp_path / "c" / "out" / f"{VERSION}.json").read_text())
    forms = c["observations"]["result_id_list_filter"]["value"]
    assert all(f["status"] == 200 and f["result_id_set_matches"] for f in forms.values())


@pytest.mark.parametrize("shuffle", [False, True])
def test_paging_stability_is_two_passes_and_says_so(tmp_path, monkeypatch, make_server, shuffle):
    fake = make_server(shuffle_pages=shuffle)
    assert run(tmp_path, monkeypatch, fake) == 0
    o = observations(tmp_path)["continuation_token_stable"]
    assert o["value"]["identical_sequences"] is (not shuffle)
    assert o["value"]["pages_walked"] == 2
    assert "concurrent" in o["detail"]


def test_the_largest_accepted_take_is_found(tmp_path, monkeypatch, make_server):
    fake = make_server(max_take=500)
    assert run(tmp_path, monkeypatch, fake) == 0
    assert value(tmp_path, "max_take_accepted") == {
        "largest_accepted": 500, "rejected": {"10000": 400, "5000": 400, "1000": 400}}
    control = make_server()
    (tmp_path / "c").mkdir()
    assert run(tmp_path / "c", monkeypatch, control) == 0
    c = json.loads((tmp_path / "c" / "out" / f"{VERSION}.json").read_text())
    assert c["observations"]["max_take_accepted"]["value"] == {"largest_accepted": 10000, "rejected": {}}


@pytest.mark.parametrize("broken", [False, True])
def test_descending_is_checked_against_the_returned_order(tmp_path, monkeypatch, make_server, broken):
    fake = make_server(descending_broken=broken)
    assert run(tmp_path, monkeypatch, fake) == 0
    assert value(tmp_path, "descending_honoured") is (not broken)


def test_projection_and_return_count_are_recorded_per_route(tmp_path, monkeypatch, make_server):
    fake = make_server()
    assert run(tmp_path, monkeypatch, fake) == 0
    proj = value(tmp_path, "projection_accepted")
    assert proj["query-results"] == {"status": 200, "fields_returned_subset": True}
    assert proj["query-steps"] == {"status": 200, "fields_returned_subset": True}
    assert proj["query-products"] is None
    rc = value(tmp_path, "return_count_take_zero")
    assert set(rc) == {"query-results", "query-steps", "query-products"}
    assert all(v == {"status": 200, "total_count_is_int": True} for v in rc.values())


def test_without_a_part_number_no_live_query_is_made(tmp_path, monkeypatch, make_server):
    fake = make_server()
    assert run(tmp_path, monkeypatch, fake, part=None) == 0
    assert [r["method"] for r in fake.seen] == ["GET"]
    obs = observations(tmp_path)
    live = [k for k, v in obs.items() if v["evidence"] == "not_run"]
    assert sorted(live) == sorted(["descending_honoured", "projection_accepted", "max_take_accepted",
                                   "return_count_take_zero", "step_id_unique",
                                   "result_id_list_filter", "continuation_token_stable"])
    assert all(obs[k]["detail"] == "no --part-number" for k in live)
    assert obs["order_by_enum"]["evidence"] == "swagger"
    control = make_server()
    (tmp_path / "c").mkdir()
    assert run(tmp_path / "c", monkeypatch, control) == 0
    assert any(r["method"] == "POST" for r in control.seen)


def test_an_existing_output_is_not_overwritten_without_the_flag(tmp_path, monkeypatch, make_server):
    fake = make_server()
    out = tmp_path / "out" / f"{VERSION}.json"
    out.parent.mkdir()
    out.write_text("SENTINEL")
    assert run(tmp_path, monkeypatch, fake) == 2
    assert out.read_text() == "SENTINEL"
    assert run(tmp_path, monkeypatch, fake, "--overwrite") == 0
    assert json.loads(out.read_text())["probe"] == "dag_tools.systemlink.spike"


def test_stdout_is_the_file_and_keys_are_sorted(tmp_path, monkeypatch, make_server, capsys):
    fake = make_server()
    assert run(tmp_path, monkeypatch, fake) == 0
    text = (tmp_path / "out" / f"{VERSION}.json").read_text()
    assert capsys.readouterr().out.strip() == text.strip()
    assert text == json.dumps(json.loads(text), sort_keys=True, indent=2) + "\n"


def test_the_probe_imports_no_dagster_and_no_dlt():
    code = ("import sys, dag_tools.systemlink.spike; "
            "bad = [m for m in ('dagster', 'dlt') if m in sys.modules]; "
            "sys.exit(1 if bad else 0)")
    assert subprocess.run([sys.executable, "-c", code]).returncode == 0
    # control: the same check does notice a module that is loaded
    control = subprocess.run([sys.executable, "-c", "import sys, dagster; sys.exit(0 if 'dagster' in sys.modules else 1)"])
    assert control.returncode == 0
