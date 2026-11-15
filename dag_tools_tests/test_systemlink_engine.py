"""The SystemLink engine: client discipline, query shapes, paging, step batching, classification."""
import json
import logging

import httpx
import pytest

from dag_tools.systemlink import events as ev
from dag_tools.systemlink import queries
from dag_tools.systemlink.client import (
    AuthFailed, GatewayTimeout, RequestFailed, SettingRejected, SystemLinkClient, Throttled,
)
from dag_tools.systemlink.incremental import (
    EXCLUDED, apply_operator, classify, known_from_rows, scan_pages, wants_steps,
)
from dag_tools.systemlink.paging import StepBatcher, fetch_steps
from dag_tools.systemlink.spec import DEFAULTS, parse_source_config
from dag_tools_tests.systemlink_fake import FakeSystemLink, make_result, make_step

KEY = "test-key-0000"
TERMINAL = ["PASSED", "FAILED", "ERRORED", "DONE"]


class Clock:
    """An injected clock and sleep: nothing in this module really waits."""

    def __init__(self):
        self.t, self.slept = 1000.0, []

    def now(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


def client_for(fake, clock=None, **kw):
    clock = clock or Clock()
    kw.setdefault("max_retries", 3)
    return SystemLinkClient(fake.base_url, KEY, sleep=clock.sleep, now=clock.now, rng=lambda: 1.0, **kw)


def mock_client(handler, clock=None, **kw):
    clock = clock or Clock()
    return SystemLinkClient(
        "http://sl.invalid", KEY, sleep=clock.sleep, now=clock.now, rng=lambda: 1.0,
        transport=httpx.MockTransport(handler), **kw,
    ), clock


# --------------------------------------------------------------------------- read-only


def test_the_client_only_ever_posts_the_three_query_routes():
    with FakeSystemLink([make_result(1)], [make_step("R-A0001")], [{"id": "P1", "partNumber": "PN-1000-A"}]) as fake:
        c = client_for(fake)
        c.post("query-results", queries.results_query("PN-1000-A", order_by="UPDATED_AT", take=10), setting="s")
        c.post("query-steps", queries.steps_query(["R-A0001"], form="or_chain", take=10), setting="s")
        c.post("query-products", queries.products_query(["PN-1000-A"], take=10), setting="s")
        assert {r["method"] for r in fake.requests} == {"POST"}
        assert fake.routes_seen() == {"query-results", "query-steps", "query-products"}


@pytest.mark.parametrize("route", ["delete-results", "results", "../admin", "query-results/x", ""])
def test_control_any_other_route_is_refused_before_a_request_is_made(route):
    sent = []
    c, _ = mock_client(lambda r: sent.append(r) or httpx.Response(200, json={}))
    with pytest.raises(ValueError):
        c.post(route, {}, setting="s")
    assert sent == []


def test_the_client_has_no_method_that_can_write():
    names = {n for n in dir(SystemLinkClient) if not n.startswith("_")}
    assert names == {"close", "events", "post"}


# --------------------------------------------------------------------------- the two spellings


@pytest.mark.parametrize("spelling", ["UPDATED_AT", "updatedAt"])
def test_order_by_matching_the_server_pages_the_results(spelling):
    rows = [make_result(i) for i in range(7)]
    with FakeSystemLink(rows, accept_order_by=spelling) as fake:
        pages = list(scan_pages(
            client_for(fake), "PN-1000-A", order_by=spelling, page_size=3, workspace_id=None,
            known={}, terminal_statuses=TERMINAL, lookback_pages=3,
        ))
    assert sum(len(p) for p in pages) == 7 and len(pages) == 3


@pytest.mark.parametrize("server,setting", [("UPDATED_AT", "updatedAt"), ("updatedAt", "UPDATED_AT")])
def test_control_order_by_mismatch_is_a_hard_error_naming_the_setting(server, setting):
    with FakeSystemLink([make_result(1)], accept_order_by=server) as fake:
        with pytest.raises(SettingRejected, match="incremental.order_by"):
            list(scan_pages(
                client_for(fake), "PN-1000-A", order_by=setting, page_size=3, workspace_id=None,
                known={}, terminal_statuses=TERMINAL, lookback_pages=3,
            ))


@pytest.mark.parametrize("form", ["or_chain", "in"])
def test_step_filter_matching_the_server_returns_the_steps(form):
    rows = [make_result(i) for i in range(3)]
    steps = [make_step(r["id"], n) for r in rows for n in range(2)]
    with FakeSystemLink(rows, steps, accept_step_form=form) as fake:
        got = fetch_steps(client_for(fake), [r["id"] for r in rows], form=form, take=50,
                          batcher=StepBatcher(10, 20))
    assert len(got) == 6


@pytest.mark.parametrize("server,setting", [("or_chain", "in"), ("in", "or_chain")])
def test_control_step_filter_mismatch_is_a_hard_error_naming_the_setting(server, setting):
    with FakeSystemLink([make_result(1)], [make_step("R-A0001")], accept_step_form=server) as fake:
        with pytest.raises(SettingRejected, match="limits.step_filter"):
            fetch_steps(client_for(fake), ["R-A0001"], form=setting, take=50, batcher=StepBatcher(10, 20))


# --------------------------------------------------------------------------- failures and backoff


def test_a_401_is_auth_failed_not_an_empty_result():
    with FakeSystemLink([make_result(1)], api_key="another-key") as fake:
        c = client_for(fake)
        with pytest.raises(AuthFailed):
            c.post("query-results", queries.results_query("PN-1000-A", order_by="UPDATED_AT", take=5), setting="s")
        assert c.events.summary()[ev.AUTH_FAILED] == 1


def test_a_200_that_is_not_a_page_is_an_error_not_an_empty_page():
    c, _ = mock_client(lambda r: httpx.Response(200, content=b"<html>not json</html>"))
    with pytest.raises(RequestFailed):
        c.post("query-results", {}, setting="s")


def test_an_empty_200_object_is_an_empty_page():
    with FakeSystemLink() as fake:
        fake.empty_200_routes.add("query-results")
        pages = list(scan_pages(
            client_for(fake), "PN-1000-A", order_by="UPDATED_AT", page_size=3, workspace_id=None,
            known={}, terminal_statuses=TERMINAL, lookback_pages=3,
        ))
    assert pages == [[]]


@pytest.mark.parametrize("status", [429, 503])
def test_throttling_backs_off_with_full_jitter_then_succeeds(status):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(status, headers={}) if len(calls) < 3 else httpx.Response(200, json={"results": []})

    c, clock = mock_client(handler, backoff_base_s=2.0, backoff_cap_s=100.0)
    data, _ = c.post("query-results", {}, setting="s")
    assert data == {"results": []} and len(calls) == 3
    assert clock.slept == [2.0, 4.0]  # rng pinned to 1.0: the jitter ceiling doubles each attempt
    assert c.events.summary()[ev.THROTTLED] == 2


def test_retry_after_seconds_is_honoured_when_longer_than_the_jitter():
    calls = []

    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "17"})
        return httpx.Response(200, json={})

    c, clock = mock_client(handler, backoff_base_s=1.0)
    c.post("query-results", {}, setting="s")
    assert clock.slept == [17.0]


def test_retry_after_http_date_is_honoured_against_the_injected_clock():
    import email.utils

    calls = []
    clock = Clock()

    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": email.utils.formatdate(clock.t + 40, usegmt=True)})
        return httpx.Response(200, json={})

    c, _ = mock_client(handler, clock=clock, backoff_base_s=1.0)
    c.post("query-results", {}, setting="s")
    assert 38 <= clock.slept[0] <= 41


def test_control_a_throttle_that_never_clears_is_bounded():
    c, clock = mock_client(lambda r: httpx.Response(503), max_retries=2)
    with pytest.raises(Throttled):
        c.post("query-results", {}, setting="s")
    assert len(clock.slept) == 2


def test_an_unsplittable_504_is_retried_like_a_throttle():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(504) if len(calls) == 1 else httpx.Response(200, json={})

    c, _ = mock_client(handler)
    c.post("query-results", {}, setting="s", splittable=False)
    assert len(calls) == 2


def test_a_504_on_a_splittable_route_raises_for_the_caller_to_split():
    c, _ = mock_client(lambda r: httpx.Response(504))
    with pytest.raises(GatewayTimeout):
        c.post("query-steps", {}, setting="s", splittable=True)


# --------------------------------------------------------------------------- step batching


def test_a_504_splits_the_batch_in_halves_and_never_resends_the_oversized_batch():
    rows = [make_result(i) for i in range(8)]
    steps = [make_step(r["id"]) for r in rows]
    with FakeSystemLink(rows, steps) as fake:
        fake.step_batch_limit = 2
        c = client_for(fake)
        got = fetch_steps(c, [r["id"] for r in rows], form="or_chain", take=50, batcher=StepBatcher(8, 8))
        sizes = [len(r["body"]["substitutions"]) for r in fake.requests if r["route"] == "query-steps"]
    assert len(got) == 8
    assert sizes[0] == 8 and sizes.count(8) == 1  # sent once, never again
    assert max(sizes[1:]) <= 4 and c.events.summary()[ev.GATEWAY_TIMEOUT_SPLIT] >= 1


def test_a_single_id_that_still_times_out_is_an_error_not_a_loop():
    with FakeSystemLink([make_result(1)], [make_step("R-A0001")]) as fake:
        fake.step_batch_limit = 0
        with pytest.raises(GatewayTimeout):
            fetch_steps(client_for(fake), ["R-A0001"], form="or_chain", take=50, batcher=StepBatcher(1, 1))


def test_the_batcher_cannot_be_built_to_exceed_its_maximum():
    with pytest.raises(ValueError):
        StepBatcher(30, 20)
    assert max(len(b) for b in StepBatcher(7, 7).batches([str(i) for i in range(50)])) == 7


def test_a_requested_id_with_no_steps_is_logged_not_visible_and_the_rest_are_kept():
    rows = [make_result(i) for i in range(3)]
    with FakeSystemLink(rows, [make_step(rows[0]["id"]), make_step(rows[1]["id"])]) as fake:
        c = client_for(fake)
        got = fetch_steps(c, [r["id"] for r in rows], form="or_chain", take=50, batcher=StepBatcher(10, 10))
    assert len(got) == 2 and c.events.summary()[ev.NOT_VISIBLE] == 1


def test_control_when_every_id_has_steps_nothing_is_not_visible():
    rows = [make_result(i) for i in range(2)]
    with FakeSystemLink(rows, [make_step(r["id"]) for r in rows]) as fake:
        c = client_for(fake)
        fetch_steps(c, [r["id"] for r in rows], form="or_chain", take=50, batcher=StepBatcher(10, 10))
    assert c.events.summary()[ev.NOT_VISIBLE] == 0


def test_the_peak_in_flight_requests_never_exceed_the_configured_concurrency():
    rows = [make_result(i) for i in range(12)]
    with FakeSystemLink(rows, [make_step(r["id"]) for r in rows]) as fake:
        fake.delay_s = 0.05
        c = client_for(fake, max_concurrency=2)
        fetch_steps(c, [r["id"] for r in rows], form="or_chain", take=50, batcher=StepBatcher(2, 2), max_concurrency=2)
        assert fake.peak_inflight <= 2


def test_control_the_fake_does_see_parallel_requests_when_the_client_allows_them():
    rows = [make_result(i) for i in range(12)]
    with FakeSystemLink(rows, [make_step(r["id"]) for r in rows]) as fake:
        fake.delay_s = 0.1
        c = client_for(fake, max_concurrency=3)
        fetch_steps(c, [r["id"] for r in rows], form="or_chain", take=50, batcher=StepBatcher(2, 2), max_concurrency=3)
        assert fake.peak_inflight >= 2


# --------------------------------------------------------------------------- logging


def test_every_request_logs_a_request_id_and_body_sha256_and_never_the_body_or_key(caplog):
    caplog.set_level(logging.INFO)
    with FakeSystemLink([make_result(1)]) as fake:
        c = client_for(fake)
        body = queries.results_query("PN-1000-A", order_by="UPDATED_AT", take=5)
        _, rid = c.post("query-results", body, setting="s")
        sent_id = fake.requests[0]["request_id"]
    lines = [json.loads(r.getMessage()) for r in caplog.records if r.getMessage().startswith("{")]
    req = [l for l in lines if l["event"] == "request"]
    assert req and req[0]["request_id"] == sent_id == rid and len(req[0]["body_sha256"]) == 64
    text = caplog.text
    assert "PN-1000-A" not in text and KEY not in text


def test_control_the_leak_check_would_see_a_body_if_one_were_logged(caplog):
    caplog.set_level(logging.INFO)
    logging.getLogger("planted").info("body=%s", json.dumps(queries.results_query("PN-1000-A", order_by="UPDATED_AT", take=5)))
    assert "PN-1000-A" in caplog.text


def test_the_client_repr_never_shows_the_key():
    c, _ = mock_client(lambda r: httpx.Response(200, json={}))
    assert KEY not in repr(c) and KEY not in str(c)


# --------------------------------------------------------------------------- queries


def test_filter_values_appear_only_in_substitutions():
    hostile = 'PN-1000-A" || id != "x'
    bodies = [
        queries.results_query(hostile, order_by="UPDATED_AT", take=5, workspace_id="ws-1", updated_after="2026-01-01T00:00:00Z"),
        queries.count_query(hostile, workspace_id="ws-1"),
        queries.steps_query([hostile, "R-2"], form="or_chain", take=5, workspace_id="ws-1"),
        queries.steps_query([hostile, "R-2"], form="in", take=5),
        queries.products_query([hostile, "PN-1000-B"], take=5),
    ]
    for body in bodies:
        queries.assert_values_only_in_substitutions(body)
        assert hostile not in body["filter"] and "ws-1" not in body["filter"]
        flat = [x for s in body["substitutions"] for x in (s if isinstance(s, list) else [s])]
        assert hostile in flat


def test_control_the_check_catches_a_value_interpolated_into_the_filter():
    with pytest.raises(AssertionError):
        queries.assert_values_only_in_substitutions({"filter": 'partNumber == "PN-1000-A"', "substitutions": []})
    with pytest.raises(AssertionError):
        queries.assert_values_only_in_substitutions({"filter": "partNumber == @0", "substitutions": []})


def test_the_server_time_filter_is_only_sent_when_asked_for():
    plain = queries.results_query("PN-1000-A", order_by="UPDATED_AT", take=5)
    timed = queries.results_query("PN-1000-A", order_by="UPDATED_AT", take=5, updated_after="2026-01-01T00:00:00Z")
    assert "updatedAt" not in plain["filter"] and "updatedAt" in timed["filter"]


# --------------------------------------------------------------------------- classification and the stop rule


def known_of(*results):
    return {r["id"]: (r["updatedAt"], r["status"]["statusType"]) for r in results}


def test_each_result_is_exactly_one_of_the_four_classes():
    old = make_result(1)
    changed = {**old, "updatedAt": "2026-02-01T00:00:00Z"}
    running = make_result(2, status="RUNNING")
    fresh = make_result(3)
    known = known_of(old)
    got = {
        ev.KNOWN_UNCHANGED: classify(old, known, TERMINAL),
        ev.CHANGED_TERMINAL: classify(changed, known, TERMINAL),
        ev.IN_PROGRESS: classify(running, known, TERMINAL),
        ev.NEW_TERMINAL: classify(fresh, known, TERMINAL),
    }
    assert all(k == v for k, v in got.items())


def test_a_running_result_is_stored_but_fetches_no_steps_and_a_later_terminal_one_does():
    running = make_result(1, status="RUNNING")
    assert wants_steps(running, classify(running, {}, TERMINAL), step_detail_for="all", failed_statuses=["FAILED"]) is False
    done = {**running, "status": {"statusType": "PASSED"}, "updatedAt": "2026-03-01T00:00:00Z"}
    stored = {running["id"]: (running["updatedAt"], "RUNNING")}
    cls = classify(done, stored, TERMINAL)
    assert cls == ev.CHANGED_TERMINAL and wants_steps(done, cls, step_detail_for="all", failed_statuses=["FAILED"])


def test_failed_only_wants_steps_for_failures_alone():
    ok, bad = make_result(1), make_result(2, status="FAILED")
    for r, want in ((ok, False), (bad, True)):
        assert wants_steps(r, ev.NEW_TERMINAL, step_detail_for="failed_only", failed_statuses=["FAILED"]) is want


def scan(fake, known, lookback=2, page=2, **kw):
    return list(scan_pages(
        client_for(fake), "PN-1000-A", order_by="UPDATED_AT", page_size=page, workspace_id=None,
        known=known, terminal_statuses=TERMINAL, lookback_pages=lookback, **kw,
    ))


def test_the_scan_stops_after_lookback_consecutive_known_pages():
    rows = [make_result(i) for i in range(12)]
    with FakeSystemLink(rows) as fake:
        pages = scan(fake, known_of(*rows), lookback=2)
    assert len(pages) == 2  # two quiet pages, then stop; four pages never read


def test_control_a_new_result_resets_the_quiet_count():
    rows = [make_result(i) for i in range(12)]
    # newest-first: the page holding the newest two rows (10, 11) is known; page 2 holds row 8, 9 (new).
    known = known_of(*[r for r in rows if r["id"] not in ("R-A0008", "R-A0009")])
    with FakeSystemLink(rows) as fake:
        pages = scan(fake, known, lookback=2)
    assert len(pages) == 4  # known, new, known, known -> stops on the 4th, not the 2nd


def test_the_backfill_walk_never_stops_early():
    rows = [make_result(i) for i in range(12)]
    with FakeSystemLink(rows) as fake:
        pages = scan(fake, known_of(*rows), lookback=1, stop_on_known=False)
    assert len(pages) == 6


def test_excluded_programs_are_dropped_and_do_not_block_the_stop_rule():
    rows = [make_result(i, programName="prog_other") for i in range(6)]
    with FakeSystemLink(rows) as fake:
        pages = scan(fake, {}, lookback=2, program_include=["prog_functional"])
    assert all(c == EXCLUDED for p in pages for _, c, _ in p) and len(pages) == 2


def test_control_included_programs_are_classified_normally():
    rows = [make_result(i) for i in range(4)]
    with FakeSystemLink(rows) as fake:
        pages = scan(fake, {}, program_include=["prog_functional"])
    assert {c for p in pages for _, c, _ in p} == {ev.NEW_TERMINAL}


def test_the_known_map_is_built_from_stored_rows_as_text():
    assert known_from_rows([("R1", "2026-01-01T00:00:00Z", "PASSED"), ("R2", None, None)]) == {
        "R1": ("2026-01-01T00:00:00Z", "PASSED"), "R2": (None, None),
    }


def test_a_row_without_an_id_is_schema_drift_and_is_skipped():
    with FakeSystemLink([make_result(1), {**make_result(2), "id": ""}]) as fake:
        c = client_for(fake)
        pages = list(scan_pages(c, "PN-1000-A", order_by="UPDATED_AT", page_size=5, workspace_id=None,
                                known={}, terminal_statuses=TERMINAL, lookback_pages=2))
    assert sum(len(p) for p in pages) == 1 and c.events.summary()[ev.SCHEMA_DRIFT] == 1


# --------------------------------------------------------------------------- operator, spec


def test_operator_policy_name_drop_and_pseudonym():
    assert apply_operator({"operator": "op-synthetic"}, "name", None)["operator"] == "op-synthetic"
    assert "operator" not in apply_operator({"operator": "op-synthetic"}, "drop", None)
    one = apply_operator({"operator": "op-synthetic"}, "pseudonym", b"k1")["operator"]
    two = apply_operator({"operator": "op-synthetic"}, "pseudonym", b"k2")["operator"]
    again = apply_operator({"operator": "op-synthetic"}, "pseudonym", b"k1")["operator"]
    assert one == again and one != two and "op-synthetic" not in one and len(one) == 64


def test_pseudonym_without_a_key_is_refused():
    with pytest.raises(ValueError):
        apply_operator({"operator": "op-synthetic"}, "pseudonym", None)


def base_spec(**over):
    spec = {
        "type": "systemlink_test_monitor", "source_id": "sl1",
        "connection": {"base_url": "https://sl.invalid", "api_key_env": "SOME_KEY_VAR"},
        "seed": {"kind": "inline", "values": ["PN-1000-A"]},
        "operator": {"store": "name"},
    }
    spec.update(over)
    return spec


def test_a_valid_spec_parses_with_the_documented_defaults():
    cfg = parse_source_config(base_spec())
    assert cfg.limits.page_size == DEFAULTS["limits.page_size"]
    assert cfg.limits.step_filter == DEFAULTS["limits.step_filter"]
    assert cfg.incremental.order_by == DEFAULTS["incremental.order_by"]


def test_pseudonym_without_a_key_env_fails_the_spec():
    with pytest.raises(ValueError, match="pseudonym_key_env"):
        parse_source_config(base_spec(operator={"store": "pseudonym"}))


def test_control_pseudonym_with_a_key_env_name_passes():
    parse_source_config(base_spec(operator={"store": "pseudonym", "pseudonym_key_env": "SOME_PSEUDO_VAR"}))


def test_the_operator_block_is_required():
    spec = base_spec()
    del spec["operator"]
    with pytest.raises(ValueError):
        parse_source_config(spec)


def test_an_unknown_key_is_refused():
    with pytest.raises(ValueError):
        parse_source_config(base_spec(cursor={"field": "x"}))


def test_a_step_batch_default_above_the_max_is_refused_and_the_cap_on_concurrency_holds():
    with pytest.raises(ValueError, match="step_batch_default"):
        parse_source_config(base_spec(limits={"step_batch_default": 30, "step_batch_max": 20}))
    with pytest.raises(ValueError, match="cap"):
        parse_source_config(base_spec(limits={"max_concurrency": 99}))


def test_the_spike_decided_defaults_are_all_in_one_block():
    assert set(DEFAULTS) >= {
        "limits.page_size", "incremental.use_server_time_filter", "steps_key",
        "incremental.order_by", "limits.step_filter", "limits.step_batch_default",
        "limits.step_batch_max",
    }
