"""Renderer invariants for the OTel -> API mapping engine.

These tests are guardrails around behaviours where a plausible-looking
implementation is silently wrong: string-typed JSON, group-wide fallback
blast radius, and fan-out scoping.
"""
import datetime as dt

import pytest

from dag_tools.otel_api_sync import (
    build_environment,
    build_plan,
    compute_plan_hash,
    group_readiness,
    group_rows,
    load_spec,
    render_plans,
    render_structure,
    render_value,
    set_path,
)


def _row(**attributes):
    """One OTel-shaped span: real columns plus a Map(String, String) bag."""
    base = {
        "Timestamp": attributes.pop("Timestamp", dt.datetime(2026, 8, 10, 20, 2, 36, 338000)),
        "TraceId": attributes.pop("TraceId", "trace-1"),
        "SpanId": attributes.pop("SpanId", "span-1"),
    }
    base["SpanAttributes"] = {k: str(v) for k, v in attributes.items()}
    return base


BASE_SPEC = {
    "api": {"base_url": "https://api.test"},
    "group_by": "{{ attr(row, 'group') }}",
    "derive": {
        "entities": "{{ distinct(rows, attr('entity')) }}",
        "items": "{{ distinct(rows, attr('item')) }}",
        "entities_by_item": "{{ group_map(rows, attr('item'), attr('entity')) }}",
        "artifacts_by_entity": "{{ group_map(rows, attr('entity'), attr('artifact')) }}",
    },
    "steps": [
        {
            "id": "record",
            "for_each": "{{ rows }}",
            "item_key": "{{ attr(item, 'SpanId') }}",
            "method": "POST",
            "path": "/api/RecordExecution",
            "payload": {"itemName": "{{ attr(item, 'item') }}"},
        }
    ],
}


# --- native typing ---------------------------------------------------------


def test_render_is_native_and_sandboxed():
    """Single-expression templates must return Python values, not strings.

    This is the whole reason for the combined Native+Sandboxed
    environment. If a future Jinja2 release moves `code_generator_class`
    or `concat` onto SandboxedEnvironment, the MRO stops picking up
    native mode and this test is the alarm.
    """
    env = build_environment()
    assert render_value("{{ 1 + 2 }}", {}, env) == 3
    assert render_value("{{ [1, 2] }}", {}, env) == [1, 2]
    assert render_value("{{ False }}", {}, env) is False
    assert isinstance(render_value("{{ as_float('81.2') }}", {}, env), float)
    # Mixed literal + expression stays a string, as it must.
    assert render_value("id={{ 1 }}", {}, env) == "id=1"
    # Sandbox: attribute escapes do not resolve.
    assert render_value("{{ x.__class__ }}", {"x": 1}, env) is None


def test_numeric_boolean_and_array_fields_keep_their_json_types():
    """Attribute maps are all strings; payload fields are not."""
    env = build_environment()
    row = _row(group="g1", **{"metric.NUM_ERROR": "0", "metric.TOTAL_TOGGLES": "81.2"})
    payload = {
        "deleteMissingEntries": False,
        "count": "{{ as_int(attr(item, 'metric.NUM_ERROR')) }}",
        "metrics": "{{ metrics_from_prefix(item, 'metric.') }}",
        "ids": "{{ split('a, b ,c') }}",
    }
    rendered = render_structure(payload, {"item": row}, env)

    assert rendered["deleteMissingEntries"] is False
    assert rendered["count"] == 0 and isinstance(rendered["count"], int)
    assert rendered["ids"] == ["a", "b", "c"]
    assert rendered["metrics"] == [
        {"metricName": "NUM_ERROR", "resultNumeric": 0},
        {"metricName": "TOTAL_TOGGLES", "resultNumeric": 81.2},
    ]
    for metric in rendered["metrics"]:
        assert isinstance(metric["resultNumeric"], (int, float))
        assert not isinstance(metric["resultNumeric"], str)


def test_to_iso_formats_clickhouse_datetime_for_the_api():
    env = build_environment()
    value = render_value(
        "{{ to_iso(attr(item, 'Timestamp')) }}",
        {"item": _row(group="g1")},
        env,
    )
    assert value == "2026-08-10T20:02:36.338Z"


def test_for_each_node_builds_an_array_of_objects():
    env = build_environment()
    node = {
        "items": {
            "_for_each": "{{ names }}",
            "_as": "name",
            "_template": {"itemName": "{{ name }}"},
        }
    }
    assert render_structure(node, {"names": ["a", "b"]}, env) == {
        "items": [{"itemName": "a"}, {"itemName": "b"}]
    }


# --- fan-out scoping -------------------------------------------------------


def test_per_item_lookup_scopes_each_item_to_its_own_entities():
    """A fanned-out step must see its item's collection, not the group's.

    Mapping every item to every entity in the group is the kind of error
    that validates fine, sends fine, and corrupts the target's data model.
    """
    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [
                {
                    "id": "item_entity_map",
                    "for_each": "{{ items }}",
                    "method": "POST",
                    "path": "/api/Item/{{ item }}/EntityMapping",
                    "payload": {"entityIdentifiers": "{{ unique(entities_by_item[item]) }}"},
                }
            ],
        }
    )
    rows = [
        _row(group="g1", item="item_a", entity="entity_1"),
        _row(group="g1", item="item_a", entity="entity_1"),
        _row(group="g1", item="item_b", entity="entity_2"),
    ]
    plan = build_plan("g1", rows, spec)
    calls = {c["path"]: c["body"] for c in plan["steps"][0]["calls"]}

    assert calls["/api/Item/item_a/EntityMapping"]["entityIdentifiers"] == ["entity_1"]
    assert calls["/api/Item/item_b/EntityMapping"]["entityIdentifiers"] == ["entity_2"]


def test_fanned_out_calls_get_unique_deterministic_item_keys():
    """Item keys name durable Restate steps: unique and stable across renders.

    Order comes from canonical row ordering (event time, then content
    digest), NOT from the order the source returned rows — see
    test_plan_is_identical_under_any_input_row_order.
    """
    spec = load_spec(BASE_SPEC)
    rows = [
        _row(group="g1", item="a", SpanId="span-1"),
        _row(group="g1", item="b", SpanId="span-2"),
    ]
    first = build_plan("g1", rows, spec)
    second = build_plan("g1", list(reversed(rows)), spec)

    keys = [c["item_key"] for c in first["steps"][0]["calls"]]
    assert sorted(keys) == ["span-1", "span-2"]
    assert len(set(keys)) == len(keys)
    assert keys == [c["item_key"] for c in second["steps"][0]["calls"]]


def test_duplicate_item_labels_are_disambiguated():
    spec = load_spec(
        {**BASE_SPEC, "steps": [{**BASE_SPEC["steps"][0], "item_key": "{{ attr(item, 'item') }}"}]}
    )
    rows = [_row(group="g1", item="same", SpanId="s1"), _row(group="g1", item="same", SpanId="s2")]
    plan = build_plan("g1", rows, spec)
    keys = [c["item_key"] for c in plan["steps"][0]["calls"]]
    assert len(set(keys)) == 2


# --- fallbacks -------------------------------------------------------------


AGGREGATE_SPEC = {
    **BASE_SPEC,
    "steps": [
        {
            "id": "entity_artifacts",
            "for_each": "{{ entities }}",
            "method": "PATCH",
            "path": "/api/EntityMaintenance/{{ item }}",
            "payload": {"artifacts": "{{ join(unique(artifacts_by_entity[item]), ',') }}"},
            "on_status": {
                404: {
                    "mode": "aggregate",
                    "method": "POST",
                    "path": "/api/EntityMaintenance",
                    "collect_into": "entities",
                    "payload": {"deleteMissingEntities": False, "entities": []},
                    "fragment": {
                        "entityIdentifier": "{{ item }}",
                        "artifacts": "{{ join(unique(artifacts_by_entity[item]), ',') }}",
                    },
                }
            },
        }
    ],
}


def test_aggregate_fallback_renders_one_fragment_per_item_not_a_group_wide_body():
    """The bulk body must be assemblable from *only* the failures.

    Pre-rendering a container plus per-item fragments is what makes it
    structurally impossible to send a replace-semantics bulk request
    covering items whose call succeeded.
    """
    spec = load_spec(AGGREGATE_SPEC)
    rows = [
        _row(group="g1", entity="e1", artifact="f1.txt"),
        _row(group="g1", entity="e2", artifact="f2.txt"),
    ]
    step = build_plan("g1", rows, spec)["steps"][0]

    assert [c["fragments"]["404"]["entityIdentifier"] for c in step["calls"]] == ["e1", "e2"]
    aggregate = step["aggregate_fallbacks"][0]
    # The container ships with an EMPTY collection; the handler fills it.
    assert aggregate["body"] == {"deleteMissingEntities": False, "entities": []}
    assert aggregate["collect_into"] == "entities"


def test_aggregate_body_assembly_covers_only_failed_items():
    """set_path is what the handler uses; prove the blast radius directly."""
    spec = load_spec(AGGREGATE_SPEC)
    rows = [
        _row(group="g1", entity="e1", artifact="f1.txt"),
        _row(group="g1", entity="e2", artifact="f2.txt"),
        _row(group="g1", entity="e3", artifact="f3.txt"),
    ]
    step = build_plan("g1", rows, spec)["steps"][0]
    aggregate = step["aggregate_fallbacks"][0]

    # Only e2 404s.
    failed = [c["fragments"]["404"] for c in step["calls"] if c["item_key"] == "e2"]
    body = set_path(aggregate["body"], aggregate["collect_into"], failed)

    assert [e["entityIdentifier"] for e in body["entities"]] == ["e2"]
    assert body["deleteMissingEntities"] is False


def test_item_mode_fallback_is_rendered_in_the_failing_items_scope():
    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [
                {
                    "id": "entity_artifacts",
                    "for_each": "{{ entities }}",
                    "method": "PATCH",
                    "path": "/api/EntityMaintenance/{{ item }}",
                    "payload": {"artifacts": "x"},
                    "on_status": {
                        404: {
                            "method": "POST",
                            "path": "/api/EntityMaintenance",
                            "payload": {
                                "deleteMissingEntities": False,
                                "entities": [{"entityIdentifier": "{{ item }}"}],
                            },
                        }
                    },
                }
            ],
        }
    )
    rows = [_row(group="g1", entity="e1"), _row(group="g1", entity="e2")]
    step = build_plan("g1", rows, spec)["steps"][0]

    for call in step["calls"]:
        entities = call["on_status"]["404"]["body"]["entities"]
        assert [e["entityIdentifier"] for e in entities] == [call["item_key"]]


def test_aggregate_fallback_without_for_each_is_rejected():
    with pytest.raises(ValueError, match="aggregate fallback needs 'for_each'"):
        load_spec(
            {
                **BASE_SPEC,
                "steps": [
                    {
                        "id": "once",
                        "method": "POST",
                        "path": "/x",
                        "on_status": {
                            404: {
                                "mode": "aggregate",
                                "path": "/y",
                                "collect_into": "a",
                                "fragment": {},
                            }
                        },
                    }
                ],
            }
        )


# --- ordering, grouping, readiness ----------------------------------------


def test_step_order_is_execution_order():
    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [
                {"id": "first", "path": "/1"},
                {"id": "second", "path": "/2"},
                {"id": "third", "path": "/3"},
            ],
        }
    )
    plan = build_plan("g1", [_row(group="g1")], spec)
    assert [s["id"] for s in plan["steps"]] == ["first", "second", "third"]


def test_group_by_splits_rows_into_execution_groups():
    spec = load_spec(BASE_SPEC)
    rows = [_row(group="g1"), _row(group="g2"), _row(group="g1")]
    groups = group_rows(rows, spec)
    assert sorted(groups) == ["g1", "g2"]
    assert len(groups["g1"]) == 2


def test_readiness_defers_a_group_that_is_still_filling():
    """A half-arrived group must not be dispatched as authoritative."""
    spec = load_spec({**BASE_SPEC, "readiness": {"quiet_period_seconds": 300}})
    now = dt.datetime(2026, 8, 10, 20, 0, 0, tzinfo=dt.timezone.utc)
    fresh = [{"Timestamp": now - dt.timedelta(seconds=30), "SpanAttributes": {"group": "g1"}}]

    ready, reason = group_readiness(fresh, spec, now=now)
    assert ready is False
    assert "still filling" in reason

    settled = [{"Timestamp": now - dt.timedelta(seconds=600), "SpanAttributes": {"group": "g1"}}]
    assert group_readiness(settled, spec, now=now)[0] is True


def test_complete_when_dispatches_immediately_on_a_terminal_marker():
    spec = load_spec(
        {
            **BASE_SPEC,
            "readiness": {
                "quiet_period_seconds": 3600,
                "complete_when": "{{ filter_rows(rows, attr('terminal'), 'true') | length > 0 }}",
            },
        }
    )
    now = dt.datetime(2026, 8, 10, 20, 0, 0, tzinfo=dt.timezone.utc)
    rows = [
        {"Timestamp": now, "SpanAttributes": {"group": "g1", "terminal": "true"}},
    ]
    ready, reason = group_readiness(rows, spec, now=now)
    assert ready is True
    assert "complete_when" in reason


def test_max_age_forces_dispatch_of_a_stranded_group():
    spec = load_spec(
        {
            **BASE_SPEC,
            "readiness": {
                "quiet_period_seconds": 300,
                "complete_when": "{{ False }}",
                "max_age_seconds": 3600,
            },
        }
    )
    now = dt.datetime(2026, 8, 10, 20, 0, 0, tzinfo=dt.timezone.utc)
    old = [{"Timestamp": now - dt.timedelta(hours=5), "SpanAttributes": {"group": "g1"}}]
    ready, reason = group_readiness(old, spec, now=now)
    assert ready is True
    assert "max_age_seconds" in reason


def test_render_plans_separates_ready_groups_from_deferred_ones():
    spec = load_spec({**BASE_SPEC, "readiness": {"quiet_period_seconds": 300}})
    now = dt.datetime(2026, 8, 10, 20, 0, 0, tzinfo=dt.timezone.utc)
    rows = [
        {"Timestamp": now - dt.timedelta(seconds=900), "SpanId": "s1",
         "SpanAttributes": {"group": "settled", "item": "a"}},
        {"Timestamp": now - dt.timedelta(seconds=10), "SpanId": "s2",
         "SpanAttributes": {"group": "filling", "item": "b"}},
    ]
    plans, deferred = render_plans(rows, spec, now=now)

    assert [p["group_key"] for p in plans] == ["settled"]
    assert [g for g, _ in deferred] == ["filling"]


# --- identity --------------------------------------------------------------


def test_plan_hash_is_stable_for_identical_rows_and_changes_with_payload():
    """The hash is the cross-run idempotency key; it must track content."""
    spec = load_spec(BASE_SPEC)
    rows = [_row(group="g1", item="a", SpanId="s1")]

    assert build_plan("g1", rows, spec)["plan_hash"] == build_plan("g1", rows, spec)["plan_hash"]

    changed = [_row(group="g1", item="b", SpanId="s1")]
    assert build_plan("g1", changed, spec)["plan_hash"] != build_plan("g1", rows, spec)["plan_hash"]


def test_plan_hash_is_independent_of_source_row_order():
    """Row order must not perturb the hash.

    This previously asserted the opposite, which encoded the bug: a
    re-read returning the same rows in a different order produced a new
    plan hash and re-dispatched work that was already delivered.
    """
    spec = load_spec(BASE_SPEC)
    a = _row(group="g1", item="a", SpanId="s1")
    b = _row(group="g1", item="b", SpanId="s2")
    assert compute_plan_hash(build_plan("g1", [a, b], spec)["steps"]) == compute_plan_hash(
        build_plan("g1", [b, a], spec)["steps"]
    )


def test_secrets_are_never_rendered_into_the_plan():
    """Auth rides as an env reference; the worker expands it, not Dagster."""
    spec = load_spec(
        {
            **BASE_SPEC,
            "api": {
                "base_url_env": "TARGET_API_BASE_URL",
                "header_env": {"Authorization": "Bearer ${TARGET_API_TOKEN}"},
            },
        }
    )
    plan = build_plan("g1", [_row(group="g1")], spec)
    assert plan["api"]["header_env"]["Authorization"] == "Bearer ${TARGET_API_TOKEN}"
    assert plan["api"]["base_url"] is None


# --- staged vs direct column naming ---------------------------------------


def test_mapping_survives_dlt_column_normalization():
    """One mapping file must work staged and unstaged.

    dlt snake-cases identifiers on the way into the warehouse, so the
    same span is `SpanAttributes` in ClickHouse and `span_attributes`
    after staging.
    """
    spec = load_spec(BASE_SPEC)
    direct = [_row(group="g1", item="a", SpanId="s1")]
    staged = [
        {
            "timestamp": direct[0]["Timestamp"],
            "trace_id": "trace-1",
            "span_id": "s1",
            "span_attributes": {"group": "g1", "item": "a"},
        }
    ]

    direct_plan = build_plan("g1", direct, spec)
    staged_plan = build_plan("g1", staged, spec)

    assert direct_plan["steps"][0]["calls"][0]["body"] == {"itemName": "a"}
    assert staged_plan["steps"][0]["calls"][0]["body"] == {"itemName": "a"}
    assert staged_plan["steps"][0]["calls"][0]["item_key"] == "s1"


# --- URL encoding ----------------------------------------------------------


def test_path_substitutions_are_percent_encoded():
    """Identifiers are frequently sentences, not slugs.

    A raw substitution of a name containing a space or a slash produces a
    URL that hits the wrong route (or none), and it fails on the first
    real scenario name rather than in review.
    """
    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [
                {
                    "id": "map",
                    "for_each": "{{ items }}",
                    "method": "POST",
                    "path": "/api/Item/{{ item }}/Mapping",
                }
            ],
        }
    )
    rows = [
        _row(group="g1", item="Login flow / smoke test"),
        _row(group="g1", item='has "quotes" & ampersand'),
        _row(group="g1", item="tab\tand#hash?q"),
    ]
    paths = [c["path"] for c in build_plan("g1", rows, spec)["steps"][0]["calls"]]

    assert paths[0] == "/api/Item/Login%20flow%20%2F%20smoke%20test/Mapping"
    assert "%22" in paths[1] and "%26" in paths[1]
    assert "%23" in paths[2] and "%3F" in paths[2]
    # The literal separators the author wrote survive; only values encode.
    for path in paths:
        assert path.startswith("/api/Item/") and path.endswith("/Mapping")
        assert path.count("/") == 4


def test_raw_opts_a_substitution_out_of_encoding():
    """Escape hatch for values that are meant to span path segments."""
    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [{"id": "s", "method": "GET", "path": "/api/{{ raw(prefix) }}/x"}],
            "derive": {"prefix": "{{ 'a/b/c' }}"},
        }
    )
    assert build_plan("g1", [_row(group="g1")], spec)["steps"][0]["calls"][0]["path"] == (
        "/api/a/b/c/x"
    )


def test_fallback_paths_are_encoded_too():
    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [
                {
                    "id": "s",
                    "for_each": "{{ items }}",
                    "method": "PATCH",
                    "path": "/api/E/{{ item }}",
                    "on_status": {404: {"method": "POST", "path": "/api/E/{{ item }}/create"}},
                }
            ],
        }
    )
    call = build_plan("g1", [_row(group="g1", item="a b/c")], spec)["steps"][0]["calls"][0]
    assert call["path"] == "/api/E/a%20b%2Fc"
    assert call["on_status"]["404"]["path"] == "/api/E/a%20b%2Fc/create"


def test_query_parameters_are_encoded_separately_from_the_path():
    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [
                {
                    "id": "s",
                    "method": "GET",
                    "path": "/api/search",
                    "query": {"q": "{{ 'a b&c' }}", "tag": "{{ ['x', 'y'] }}", "full": "{{ True }}"},
                }
            ],
        }
    )
    path = build_plan("g1", [_row(group="g1")], spec)["steps"][0]["calls"][0]["path"]
    assert path.startswith("/api/search?")
    assert "q=a+b%26c" in path
    assert "tag=x&tag=y" in path
    assert "full=true" in path


# --- call identity ---------------------------------------------------------


def test_call_key_is_stable_for_identical_calls_and_tracks_the_body():
    """Content identity is what lets the executor skip a redelivery."""
    spec = load_spec(BASE_SPEC)
    rows = [_row(group="g1", item="a", SpanId="s1")]
    first = build_plan("g1", rows, spec)["steps"][0]["calls"][0]
    again = build_plan("g1", rows, spec)["steps"][0]["calls"][0]
    assert first["call_key"] == again["call_key"]

    changed = build_plan("g1", [_row(group="g1", item="b", SpanId="s1")], spec)
    assert changed["steps"][0]["calls"][0]["call_key"] != first["call_key"]


def test_a_superset_re_render_keeps_the_original_rows_call_keys_identical():
    """The overlapping-dispatch case, at the renderer level.

    Late rows produce a new plan hash, so the plan legitimately
    re-dispatches. The keys of the calls covering the ORIGINAL rows must
    be unchanged, or per-call suppression cannot recognise them.
    """
    spec = load_spec(BASE_SPEC)
    first_rows = [_row(group="g1", item="a", SpanId="s1")]
    later_rows = first_rows + [_row(group="g1", item="b", SpanId="s2")]

    first = build_plan("g1", first_rows, spec)
    second = build_plan("g1", later_rows, spec)

    assert first["plan_hash"] != second["plan_hash"]
    original_key = first["steps"][0]["calls"][0]["call_key"]
    assert original_key in [c["call_key"] for c in second["steps"][0]["calls"]]


def test_dedupe_defaults_on_and_is_configurable_per_step():
    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [
                {"id": "a", "path": "/a"},
                {"id": "b", "path": "/b", "dedupe": False},
            ],
        }
    )
    steps = build_plan("g1", [_row(group="g1")], spec)["steps"]
    assert steps[0]["calls"][0]["dedupe"] is True
    assert steps[1]["calls"][0]["dedupe"] is False


# --- degenerate groups -----------------------------------------------------


def test_a_fanout_over_an_empty_collection_is_a_no_op_not_an_error():
    """Empty collections are routine in production, not exceptional."""
    spec = load_spec(
        {
            **BASE_SPEC,
            "derive": {"nothing": "{{ [] }}"},
            "steps": [
                {"id": "empty", "for_each": "{{ nothing }}", "path": "/x"},
                {"id": "missing", "for_each": "{{ never_defined }}", "path": "/y"},
                {"id": "always", "path": "/z"},
            ],
        }
    )
    plan = build_plan("g1", [_row(group="g1")], spec)
    # Steps with nothing to do are omitted; the rest of the plan is intact.
    assert [s["id"] for s in plan["steps"]] == ["always"]


def test_a_plan_with_no_calls_at_all_still_renders_cleanly():
    spec = load_spec(
        {
            **BASE_SPEC,
            "derive": {"nothing": "{{ [] }}"},
            "steps": [{"id": "empty", "for_each": "{{ nothing }}", "path": "/x"}],
        }
    )
    plan = build_plan("g1", [_row(group="g1")], spec)
    assert plan["steps"] == []
    assert plan["plan_hash"]


def test_plan_hash_is_computed_over_canonical_json():
    """Key order must not change the hash, or dedupe breaks spuriously."""
    a = compute_plan_hash([{"id": "s", "calls": [{"method": "POST", "path": "/x"}]}])
    b = compute_plan_hash([{"calls": [{"path": "/x", "method": "POST"}], "id": "s"}])
    assert a == b


# --- staging shapes (found by a live run, not by review) -------------------


def _staged_flattened_row(**attrs):
    """A row after dlt flattened the attribute map into columns.

    dlt explodes a nested dict into `parent__child` columns and
    lower-cases every part unless the column is hinted as JSON. A mapping
    written against ClickHouse names resolves nothing here unless lookup
    is tolerant of the flattening — and "resolves nothing" means the
    pipeline renders zero groups and silently dispatches nothing.
    """
    row = {"timestamp": dt.datetime(2026, 8, 10, 20, 2, 36, 338000), "span_id": "s1"}
    for key, value in attrs.items():
        row[f"span_attributes__{key}"] = str(value)
    return row


def test_attributes_resolve_through_dlt_flattened_columns():
    spec = load_spec(BASE_SPEC)
    rows = [_staged_flattened_row(group="g1", item="a")]

    groups = group_rows(rows, spec)
    assert list(groups) == ["g1"], "group_by must survive dlt flattening"

    plan = build_plan("g1", rows, spec)
    assert plan["steps"][0]["calls"][0]["body"] == {"itemName": "a"}


def test_metrics_resolve_from_a_renamed_attribute_column():
    """`metrics_from_prefix` must use the same tolerant column resolution.

    A staged load renames SpanAttributes -> span_attributes. Resolving
    the bag by exact name returns an empty metric list — which then
    passes any assertion written as `all(... for m in metrics)`.
    """
    env = build_environment()
    staged = {
        "span_id": "s1",
        "span_attributes": {"metric.NUM_ERROR": "0", "metric.TOTAL_TOGGLES": "81.2"},
    }
    metrics = render_value("{{ metrics_from_prefix(item, 'metric.') }}", {"item": staged}, env)

    assert len(metrics) == 2
    assert metrics == [
        {"metricName": "NUM_ERROR", "resultNumeric": 0},
        {"metricName": "TOTAL_TOGGLES", "resultNumeric": 81.2},
    ]
    # Original attribute casing must survive; it cannot be recovered from
    # flattened column names, which is why maps are pinned to JSON.
    assert {m["metricName"] for m in metrics} == {"NUM_ERROR", "TOTAL_TOGGLES"}


def test_clickhouse_source_pins_attribute_maps_to_json_by_default():
    """Without this hint dlt flattens the maps and loses attribute casing."""
    from dag_tools.asset_wrappers.sources.clickhouse_query import (
        DEFAULT_MAP_COLUMNS,
        clickhouse_query,
    )

    source = clickhouse_query(
        connection={"host": "localhost", "database": "otel"},
        resources=[{"name": "spans", "table": "otel.otel_traces"}],
    )
    columns = source.resources["spans"].columns
    for column in DEFAULT_MAP_COLUMNS:
        assert columns[column]["data_type"] == "json", column


# --- determinism under source row order ------------------------------------


def test_plan_is_identical_under_any_input_row_order():
    """Rendering must depend on the row SET, not the row SEQUENCE.

    SQL guarantees no ordering without ORDER BY, so the same group
    re-read from ClickHouse can arrive shuffled. If a joined list or a
    derived collection reorders, the body changes, the call_key changes,
    and per-call delivery suppression re-sends calls that were already
    delivered — silently defeating the duplicate guard.
    """
    import random

    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [
                {
                    "id": "bulk",
                    "method": "POST",
                    "path": "/bulk",
                    "payload": {
                        "entities": "{{ entities }}",
                        "artifacts": "{{ join(artifacts_by_entity['e1'], ',') }}",
                    },
                },
                {
                    "id": "per_entity",
                    "for_each": "{{ entities }}",
                    "method": "PATCH",
                    "path": "/e/{{ item }}",
                    "payload": {"artifacts": "{{ join(artifacts_by_entity[item], ',') }}"},
                },
                {
                    "id": "per_row",
                    "for_each": "{{ rows }}",
                    "item_key": "{{ attr(item, 'SpanId') }}",
                    "method": "POST",
                    "path": "/r",
                    "payload": {"span": "{{ attr(item, 'SpanId') }}"},
                },
            ],
        }
    )

    rows = [
        _row(group="g1", entity="e1", artifact="f1.txt", item="i1", SpanId="s1"),
        _row(group="g1", entity="e2", artifact="f2.txt", item="i2", SpanId="s2"),
        _row(group="g1", entity="e1", artifact="f3.txt", item="i1", SpanId="s3"),
        _row(group="g1", entity="e3", artifact="f4.txt", item="i3", SpanId="s4"),
    ]

    baseline = build_plan("g1", rows, spec)
    baseline_keys = [c["call_key"] for s in baseline["steps"] for c in s["calls"]]

    random.seed(1234)
    for _ in range(8):
        shuffled = list(rows)
        random.shuffle(shuffled)
        plan = build_plan("g1", shuffled, spec)
        assert plan["plan_hash"] == baseline["plan_hash"]
        assert [c["call_key"] for s in plan["steps"] for c in s["calls"]] == baseline_keys
        assert plan["steps"] == baseline["steps"]


def test_ordering_is_canonical_even_without_usable_timestamps():
    """Rows with no event time still order deterministically."""
    import random

    spec = load_spec(
        {
            **BASE_SPEC,
            "steps": [{"id": "b", "method": "POST", "path": "/b",
                       "payload": {"items": "{{ pluck(rows, attr('item')) }}"}}],
        }
    )
    rows = [
        {"SpanAttributes": {"group": "g1", "item": name}}
        for name in ("c", "a", "b", "d")
    ]
    baseline = build_plan("g1", rows, spec)
    random.seed(99)
    for _ in range(5):
        shuffled = list(rows)
        random.shuffle(shuffled)
        assert build_plan("g1", shuffled, spec)["plan_hash"] == baseline["plan_hash"]


def test_per_event_steps_fan_out_in_chronological_order():
    """A useful side effect of canonical ordering, worth pinning."""
    spec = load_spec(BASE_SPEC)
    rows = [
        _row(group="g1", item="c", SpanId="s3",
             Timestamp=dt.datetime(2026, 8, 10, 20, 3, 0)),
        _row(group="g1", item="a", SpanId="s1",
             Timestamp=dt.datetime(2026, 8, 10, 20, 1, 0)),
        _row(group="g1", item="b", SpanId="s2",
             Timestamp=dt.datetime(2026, 8, 10, 20, 2, 0)),
    ]
    keys = [c["item_key"] for c in build_plan("g1", rows, spec)["steps"][0]["calls"]]
    assert keys == ["s1", "s2", "s3"]
