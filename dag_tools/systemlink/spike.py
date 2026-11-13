"""Read-only probe of a SystemLink Test Monitor server.

Records API structure (routes, field names, enum spellings) and a handful of
behaviours (booleans, status codes, counts). It never records a base URL, an
API key, a part number or any value out of a response. Every request goes
through ``ReadOnlyClient``, which refuses any route off its allowlist before a
network call is made.
"""
import argparse
import datetime
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx
import yaml

SCHEMA_VERSION = 1
PROBE_NAME = "dag_tools.systemlink.spike"
ROUTES = ("query-results", "query-steps", "query-products")
DEFAULT_SWAGGER_PATHS = (
    "/nitestmonitor/swagger/v2/nitestmonitor-v2.yml",
    "/nitestmonitor/swagger/v2/nitestmonitor-v2.json",
    "/nitestmonitor/swagger/v1/nitestmonitor-v1.yml",
)
DEFAULT_OUT_DIR = "docs/plans/mfg-trace-spike"
CONNECTION_KEYS = {"base_url", "api_key_env", "api_version_path", "workspace_id", "verify_tls"}
_ROW_KEYS = {"query-results": "results", "query-steps": "steps", "query-products": "products"}
_ENV_CALL = re.compile(r"\{\{\s*env\(\s*(['\"])([A-Za-z_][A-Za-z0-9_]*)\1\s*\)\s*\}\}")
_TAKE_LADDER = [10000, 5000, 1000, 500, 100]


class ConfigError(Exception):
    """A configuration problem. The message names variables and keys, never values."""


class AuthFailed(Exception):
    """The server answered 401."""


# --------------------------------------------------------------------- client

class ReadOnlyClient:
    """The only door to the network. Two operations exist: GET a swagger path
    and POST one of the three query routes. Everything else raises ValueError
    before any request is made."""

    def __init__(self, base_url: str, api_key: str, api_version_path: str,
                 swagger_paths: Sequence[str], verify: bool = True, timeout: float = 30.0):
        vp = api_version_path.rstrip("/")
        self._allowed = {("GET", p) for p in swagger_paths}
        self._allowed |= {("POST", f"{vp}/{r}") for r in ROUTES}
        self._route_names = {f"{vp}/{r}": r for r in ROUTES}
        self._vp = vp
        self._http = httpx.Client(base_url=base_url, headers={"x-ni-api-key": api_key},
                                  verify=verify, timeout=timeout)
        self.requests: List[Dict[str, Any]] = []

    def __repr__(self) -> str:
        return "ReadOnlyClient()"

    def get_swagger(self, path: str) -> httpx.Response:
        return self._send("GET", path)

    def query(self, route: str, body: Dict[str, Any]) -> Tuple[int, Optional[Dict[str, Any]]]:
        resp = self._send("POST", f"{self._vp}/{route}", body)
        try:
            data = resp.json()
        except ValueError:
            data = None
        return resp.status_code, data if isinstance(data, dict) else None

    def _send(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> httpx.Response:
        if (method, path) not in self._allowed:
            raise ValueError("route not on the read-only allowlist")
        name = "swagger" if method == "GET" else self._route_names[path]
        entry: Dict[str, Any] = {"method": method, "route": name, "status": None}
        self.requests.append(entry)
        if body is not None:
            resp = self._http.request(method, path, json=body)
        else:
            resp = self._http.request(method, path)
        entry["status"] = resp.status_code
        if resp.status_code == 401:
            raise AuthFailed()
        return resp


# ---------------------------------------------------------------------- config

def _resolve_string(key: str, value: str) -> str:
    def sub(m: "re.Match[str]") -> str:
        name = m.group(2)
        got = os.environ.get(name)
        if not got:
            raise ConfigError(f"environment variable {name} is not set")
        return got
    out = _ENV_CALL.sub(sub, value)
    if "{{" in out or "{%" in out:
        raise ConfigError(f"connection.{key} holds a template other than env('NAME')")
    return out


def load_connection(path: Path) -> Dict[str, Any]:
    try:
        doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as e:
        raise ConfigError(f"cannot read component file ({type(e).__name__})")
    node: Any = doc
    for part in ("attributes", "source_config", "connection"):
        node = node.get(part) if isinstance(node, dict) else None
    if not isinstance(node, dict):
        raise ConfigError("attributes.source_config.connection not found")
    unknown = sorted(str(k) for k in node if k not in CONNECTION_KEYS)
    if unknown:
        raise ConfigError(f"unknown connection keys: {', '.join(unknown)}")
    conn = {k: (_resolve_string(k, v) if isinstance(v, str) else v) for k, v in node.items()}
    for req in ("base_url", "api_key_env"):
        if not conn.get(req):
            raise ConfigError(f"connection.{req} is required")
    conn.setdefault("api_version_path", "/nitestmonitor/v2")
    if conn.get("workspace_id") is not None:
        conn["workspace_id"] = str(conn["workspace_id"])
    verify = conn.get("verify_tls", True)
    if not isinstance(verify, bool):
        raise ConfigError("connection.verify_tls must be true or false")
    conn["verify_tls"] = verify
    conn["api_version_path"] = "/" + str(conn["api_version_path"]).strip("/")
    return conn


# --------------------------------------------------------------------- swagger

def _flatten(node: Any, root: Dict[str, Any], seen: frozenset = frozenset()) -> Dict[str, Any]:
    """Resolve a local $ref and merge allOf properties, one level."""
    if not isinstance(node, dict):
        return {}
    if "$ref" in node:
        ref = node["$ref"]
        if not isinstance(ref, str) or not ref.startswith("#/") or ref in seen:
            return {}
        target: Any = root
        for part in ref[2:].split("/"):
            part = part.replace("~1", "/").replace("~0", "~")
            if not isinstance(target, dict) or part not in target:
                return {}
            target = target[part]
        return _flatten(target, root, seen | {ref})
    out = {k: v for k, v in node.items() if k != "allOf"}
    props: Dict[str, Any] = {}
    for member in node.get("allOf") or []:
        props.update(_flatten(member, root, seen).get("properties") or {})
    props.update(node.get("properties") or {})
    if props:
        out["properties"] = props
    return out


def _body_schema(op: Any, root: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(op, dict):
        return None
    content = _flatten(op.get("requestBody"), root).get("content") or {}
    js = content.get("application/json")
    if isinstance(js, dict) and "schema" in js:
        return _flatten(js["schema"], root)
    for p in op.get("parameters") or []:
        p = _flatten(p, root)
        if p.get("in") == "body":
            return _flatten(p.get("schema"), root)
    return None


def _prop(schema: Dict[str, Any], name: str, root: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    props = schema.get("properties") or {}
    return _flatten(props[name], root) if name in props else None


def _enum(schema: Optional[Dict[str, Any]]) -> Optional[List[Any]]:
    e = schema.get("enum") if schema else None
    return list(e) if isinstance(e, list) else None


def obs(value: Any, evidence: str, detail: str) -> Dict[str, Any]:
    return {"value": value, "evidence": evidence, "detail": detail}


def not_run(detail: str) -> Dict[str, Any]:
    return obs(None, "not_run", detail)


def parse_swagger(doc: Any) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], str]:
    """Return (observations, declared-per-route, server version)."""
    if not isinstance(doc, dict):
        doc = {}
    paths = doc.get("paths") if isinstance(doc.get("paths"), dict) else {}
    info = doc.get("info") if isinstance(doc.get("info"), dict) else {}
    version = str(info.get("version") or "unknown")
    declared: Dict[str, Dict[str, Any]] = {}
    for route in ROUTES:
        match = next((p for p in sorted(paths) if str(p).rstrip("/").endswith("/" + route)), None)
        schema = _body_schema((paths.get(match) or {}).get("post"), doc) if match else None
        if schema is None:
            declared[route] = {"found": False}
            continue
        order = _prop(schema, "orderBy", doc)
        proj = _prop(schema, "projection", doc)
        proj_enum = _enum(_flatten(proj.get("items"), doc)) if proj else None
        take = _prop(schema, "take", doc)
        declared[route] = {
            "found": True,
            "order_enum": _enum(order),
            "descending": _prop(schema, "descending", doc) is not None,
            "projection": proj is not None,
            "projection_enum": proj_enum,
            "max_take": take.get("maximum") if take else None,
            "return_count": _prop(schema, "returnCount", doc) is not None,
        }

    def per_route(fn) -> Dict[str, Any]:
        return {r: (fn(d) if d["found"] else None) for r, d in declared.items()}

    o = {
        "routes": obs(sorted(str(p) for p in paths), "swagger", "path strings in the document"),
        "order_by_enum": obs(per_route(lambda d: d["order_enum"]), "swagger",
                             "orderBy enum per route; null when absent or not an enum, "
                             "or the route is not in the document"),
        "descending_field": obs(per_route(lambda d: d["descending"]), "swagger",
                                "descending property declared"),
        "projection_field": obs(per_route(lambda d: {"declared": d["projection"],
                                                     "enum": d["projection_enum"]}),
                                "swagger", "projection property declared, with its item enum"),
        "max_take_declared": obs(per_route(lambda d: d["max_take"]), "swagger",
                                 "take.maximum per route"),
        "return_count_field": obs(per_route(lambda d: d["return_count"]), "swagger",
                                  "returnCount property declared"),
    }
    return o, declared, version


# ------------------------------------------------------------------ live probes

def _rows(route: str, data: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not data:
        return []
    got = data.get(_ROW_KEYS[route])
    if not isinstance(got, list):
        got = next((v for v in data.values() if isinstance(v, list)), [])
    return [r for r in got if isinstance(r, dict)]


def _norm(s: str) -> str:
    return s.lower().replace("_", "")


class _Probe:
    def __init__(self, client: ReadOnlyClient, part_number: str, workspace_id: Optional[str],
                 declared: Dict[str, Dict[str, Any]], pages: int, page_take: int):
        self.c = client
        self.pn = part_number
        self.ws = workspace_id
        self.declared = declared
        self.pages = pages
        self.page_take = page_take
        self.result_ids: List[str] = []

    def body(self, filt: str, subs: List[Any], **extra: Any) -> Dict[str, Any]:
        subs = list(subs)
        if self.ws is not None:
            filt = f"({filt}) && workspace == @{len(subs)}"
            subs.append(self.ws)
        return {"filter": filt, "substitutions": subs, **extra}

    def part_body(self, **extra: Any) -> Dict[str, Any]:
        return self.body("partNumber == @0", [self.pn], **extra)

    def steps_body(self, ids: List[str], **extra: Any) -> Dict[str, Any]:
        filt = " || ".join(f"resultId == @{i}" for i in range(len(ids)))
        return self.body(filt, ids, **extra)

    def scoped_for(self, route: str, **extra: Any) -> Optional[Dict[str, Any]]:
        if route == "query-steps":
            return self.steps_body(self.result_ids[:20], **extra) if self.result_ids else None
        return self.part_body(**extra)

    def safe(self, fn) -> Dict[str, Any]:
        try:
            return fn()
        except AuthFailed:
            raise
        except Exception as e:  # a failed probe never stops the run
            return obs(None, "live", type(e).__name__)

    def run_all(self) -> Dict[str, Dict[str, Any]]:
        self.safe(self.seed)
        return {
            "descending_honoured": self.safe(self.descending_honoured),
            "projection_accepted": self.safe(self.projection_accepted),
            "max_take_accepted": self.safe(self.max_take_accepted),
            "return_count_take_zero": self.safe(self.return_count_take_zero),
            "step_id_unique": self.safe(self.step_id_unique),
            "result_id_list_filter": self.safe(self.result_id_list_filter),
            "continuation_token_stable": self.safe(self.continuation_token_stable),
        }

    def seed(self) -> Dict[str, Any]:
        status, data = self.c.query("query-results", self.part_body(take=20))
        if status == 200:
            self.result_ids = [str(r["id"]) for r in _rows("query-results", data) if r.get("id")]
        return obs(status, "live", "seed")

    def descending_honoured(self) -> Dict[str, Any]:
        enum = self.declared["query-results"].get("order_enum") or []
        field = next((e for e in enum if isinstance(e, str) and "updated" in e.lower()), None)
        if field is None:
            return not_run("no orderBy enum value containing UPDATED")
        status, data = self.c.query("query-results",
                                    self.part_body(orderBy=field, descending=True, take=5))
        if status != 200:
            return obs(None, "live", f"status {status}")
        stamps = [r.get("updatedAt") for r in _rows("query-results", data)]
        if len(stamps) < 2 or not all(isinstance(s, str) for s in stamps):
            return obs(None, "live", "fewer than 2 rows carrying updatedAt")
        return obs(all(a >= b for a, b in zip(stamps, stamps[1:])), "live",
                   "updatedAt non-increasing with descending true")

    def projection_accepted(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for route in ROUTES:
            d = self.declared[route]
            item = next((e for e in (d.get("projection_enum") or [])
                         if isinstance(e, str) and e.lower() == "id"), None)
            body = self.scoped_for(route, projection=[item], take=5) if item else None
            if body is None:
                out[route] = None
                continue
            status, data = self.c.query(route, body)
            rows = _rows(route, data) if status == 200 else []
            subset = (all({_norm(k) for k in r} <= {_norm(item)} for r in rows)
                      if rows else None)
            out[route] = {"status": status, "fields_returned_subset": subset}
        if all(v is None for v in out.values()):
            return not_run("no route declares a projection enum with an ID item")
        return obs(out, "live", "null: not declared, or no results to scope steps by; "
                                "fields_returned_subset null when no rows came back")

    def max_take_accepted(self) -> Dict[str, Any]:
        declared = self.declared["query-results"].get("max_take")
        tries: List[int] = []
        for t in ([declared] if isinstance(declared, int) else []) + _TAKE_LADDER:
            if t not in tries:
                tries.append(t)
        rejected: Dict[str, int] = {}
        accepted = None
        for t in tries:
            status, _ = self.c.query("query-results", self.part_body(take=t))
            if status == 200:
                accepted = t
                break
            rejected[str(t)] = status
        return obs({"largest_accepted": accepted, "rejected": rejected}, "live",
                   "first 200 walking down from the declared maximum then the ladder")

    def return_count_take_zero(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for route in ROUTES:
            body = self.scoped_for(route, take=0, returnCount=True)
            if body is None:
                out[route] = None
                continue
            status, data = self.c.query(route, body)
            total = (data or {}).get("totalCount")
            out[route] = {"status": status,
                          "total_count_is_int": isinstance(total, int) and not isinstance(total, bool)}
        return obs(out, "live", "null: steps need results from the part; the count itself is never recorded")

    def step_id_unique(self) -> Dict[str, Any]:
        if not self.result_ids:
            return not_run("no results for the part")
        ids = self.result_ids[:20]
        status, data = self.c.query("query-steps", self.steps_body(ids, take=100))
        if status != 200:
            return obs(None, "live", f"status {status}")
        by_step: Dict[Any, set] = {}
        rows = _rows("query-steps", data)
        for r in rows:
            by_step.setdefault(r.get("stepId"), set()).add(r.get("resultId"))
        return obs({"n_steps": len(rows), "n_distinct_step_ids": len(by_step),
                    "n_step_ids_shared_across_results": sum(1 for v in by_step.values() if len(v) > 1)},
                   "live", "Q3: first page (take 100) of steps for up to 20 results of the part")

    def result_id_list_filter(self) -> Dict[str, Any]:
        ids = self.result_ids[:5]
        if len(ids) < 2:
            return not_run("fewer than 2 results for the part")
        forms = {
            "or_chain": self.steps_body(ids, take=100),
            "in": self.body("resultId in @0", [ids], take=100),
            "Contains": self.body("@0.Contains(resultId)", [ids], take=100),
        }
        base_status, base_data = self.c.query("query-steps", forms["or_chain"])
        base_rows = _rows("query-steps", base_data) if base_status == 200 else None
        base_set = {r.get("resultId") for r in base_rows} if base_rows is not None else None
        out: Dict[str, Any] = {}
        for name, body in forms.items():
            if name == "or_chain":
                status, data = base_status, base_data
            else:
                status, data = self.c.query("query-steps", body)
            rows = _rows("query-steps", data) if status == 200 else None
            ok = rows is not None and base_rows is not None
            out[name] = {
                "status": status,
                "result_id_set_matches": bool(ok and {r.get("resultId") for r in rows} == base_set),
                "count_matches_or_form": bool(ok and len(rows) == len(base_rows)),
            }
        return obs(out, "live", "compared against the or_chain form on the same results")

    def continuation_token_stable(self) -> Dict[str, Any]:
        def walk() -> Optional[Tuple[List[Any], int]]:
            seq: List[Any] = []
            token = None
            pages = 0
            while pages < self.pages:
                extra: Dict[str, Any] = {"take": self.page_take}
                if token:
                    extra["continuationToken"] = token
                status, data = self.c.query("query-results", self.part_body(**extra))
                if status != 200:
                    return None
                pages += 1
                seq.extend(r.get("id") for r in _rows("query-results", data))
                token = (data or {}).get("continuationToken")
                if not token:
                    break
            return seq, pages
        first, second = walk(), walk()
        if first is None or second is None:
            return obs(None, "live", "a page request did not return 200")
        return obs({"identical_sequences": first[0] == second[0], "pages_walked": first[1]},
                   "live", "two identical read passes only; stability under a concurrent "
                           "insert cannot be proven read-only")


# ------------------------------------------------------------------------ main

_LIVE_NAMES = ("descending_honoured", "projection_accepted", "max_take_accepted",
               "return_count_take_zero", "step_id_unique", "result_id_list_filter",
               "continuation_token_stable")


def _sanitize_version(v: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", v) or "unknown"


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog=PROBE_NAME, description="Read-only SystemLink Test Monitor probe")
    p.add_argument("--component", required=True)
    p.add_argument("--part-number")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    p.add_argument("--swagger-path", action="append")
    p.add_argument("--pages", type=int, default=3)
    p.add_argument("--page-take", type=int, default=2)
    p.add_argument("--overwrite", action="store_true")
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    # httpx logs full request URLs at INFO; the host must not reach any log.
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)
    try:
        conn = load_connection(Path(args.component))
        key_name = conn["api_key_env"]
        api_key = os.environ.get(key_name)
        if not api_key:
            raise ConfigError(f"environment variable {key_name} is not set")
    except ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    swagger_paths = args.swagger_path or list(DEFAULT_SWAGGER_PATHS)
    client = ReadOnlyClient(conn["base_url"], api_key, conn["api_version_path"],
                            swagger_paths, verify=conn["verify_tls"])
    try:
        doc, swagger_path = None, None
        for sp in swagger_paths:
            resp = client.get_swagger(sp)
            if resp.status_code == 200:
                try:
                    doc = yaml.safe_load(resp.text)
                except yaml.YAMLError:
                    print("error: the swagger document did not parse", file=sys.stderr)
                    return 2
                swagger_path = sp
                break
        if doc is None:
            print("error: no swagger path answered 200", file=sys.stderr)
            return 2
        observations, declared, version = parse_swagger(doc)
        out_file = Path(args.out_dir) / f"{_sanitize_version(version)}.json"
        if out_file.exists() and not args.overwrite:
            print(f"error: {out_file} exists; pass --overwrite", file=sys.stderr)
            return 2
        if args.part_number:
            observations.update(_Probe(client, args.part_number, conn.get("workspace_id"),
                                       declared, args.pages, args.page_take).run_all())
        else:
            for name in _LIVE_NAMES:
                observations[name] = {"value": None, "evidence": "not_run",
                                      "detail": "no --part-number"}
    except AuthFailed:
        print("auth_failed", file=sys.stderr)
        return 3
    except httpx.HTTPError as e:
        print(f"error: {type(e).__name__}", file=sys.stderr)
        return 2
    result = {
        "schema_version": SCHEMA_VERSION,
        "probe": PROBE_NAME,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "server": {"version": version, "swagger_path": swagger_path},
        "observations": observations,
        "requests": client.requests,
    }
    text = json.dumps(result, sort_keys=True, indent=2)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
