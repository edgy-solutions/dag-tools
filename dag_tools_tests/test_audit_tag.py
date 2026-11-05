"""The attribution contract carried on the query itself.

One module is written by the warehouse resource and read back out of query
history, in two different processes and at two different times, so the shape
is a contract rather than an implementation detail. These tests pin the parts
that a later change could break silently:

  * an over-long tag raises instead of being truncated -- a half tag decodes
    to nothing, and the read then looks unattributed rather than broken;
  * anything that is not ours decodes to None rather than raising, because a
    warehouse's history is full of tags other tools wrote;
  * a principal kind this library does not know is NOT a pipeline, so a
    future kind cannot quietly inherit a pipeline's attribution.

No object name appears here. The principals and asset keys are synthetic.
"""

import json

import pytest

from dag_tools.audit import tag as tagmod


def test_a_pipeline_principal_round_trips():
    attribution = tagmod.QueryAttribution(
        principal=tagmod.pipeline_principal("daily_load"),
        asset_key="orders_clean",
        run_id="run-1",
    )
    decoded = tagmod.decode(tagmod.encode(attribution))

    assert decoded is not None
    assert decoded.principal == "pipeline:daily_load"
    assert decoded.asset_key == "orders_clean"
    assert decoded.run_id == "run-1"
    assert decoded.schema_version == tagmod.SCHEMA_VERSION


def test_encode_is_byte_stable():
    """Two encodes of the same attribution are the same bytes.

    The tag lands in warehouse history and in the Restate journal; a key
    order that wanders makes two identical reads look different.
    """
    attribution = tagmod.QueryAttribution(
        principal=tagmod.pipeline_principal("daily_load"), asset_key="a", run_id="r"
    )
    assert tagmod.encode(attribution) == tagmod.encode(attribution)


def test_an_over_long_tag_raises_rather_than_truncating():
    attribution = tagmod.QueryAttribution(
        principal=tagmod.pipeline_principal("x" * (tagmod.MAX_TAG_CHARS + 50)),
        asset_key="orders_clean",
    )
    with pytest.raises(tagmod.TagTooLong):
        tagmod.encode(attribution)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        "   ",
        "not json at all",
        "[]",
        '"a string"',
        "123",
        json.dumps({"something": "else"}),
        '{"principal": ',
    ],
)
def test_decode_never_raises_on_what_other_tools_wrote(raw):
    assert tagmod.decode(raw) is None


def test_decode_tolerates_extra_keys():
    """Forward compatibility: a newer writer may add fields."""
    raw = json.dumps(
        {
            "principal": "pipeline:daily_load",
            "asset_key": "orders_clean",
            "schema_version": tagmod.SCHEMA_VERSION,
            "something_new": {"nested": True},
        }
    )
    decoded = tagmod.decode(raw)
    assert decoded is not None
    assert decoded.asset_key == "orders_clean"


@pytest.mark.parametrize(
    "value,kind",
    [
        ("pipeline:daily_load", tagmod.PIPELINE),
        ("user:analyst-one", tagmod.ON_BEHALF_OF),
    ],
)
def test_known_kinds_parse(value, kind):
    parsed_kind, parsed_value = tagmod.parse_principal(value)
    assert parsed_kind == kind
    assert parsed_value == value.split(":", 1)[1]


@pytest.mark.parametrize(
    "value",
    [
        "service:daily_load",
        "robot:daily_load",
        "daily_load",
        "svc_pipeline",
        "",
        ":daily_load",
        "PIPELINE:daily_load",
    ],
)
def test_an_unknown_kind_is_not_a_pipeline(value):
    """Deny by default.

    A kind this version does not know must not inherit a pipeline's
    attribution: a read it explains would be reported as covered when nobody
    has decided whether it should be.
    """
    assert tagmod.is_pipeline(value) is False
    assert tagmod.parse_principal(value)[0] is None


def test_a_value_may_contain_colons():
    kind, value = tagmod.parse_principal("user:realm:analyst-one")
    assert kind == tagmod.ON_BEHALF_OF
    assert value == "realm:analyst-one"
