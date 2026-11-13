"""The vendor platform mapping is its own table, consulted by resolve_platform."""
from dag_tools.components.datahub_lineage.platforms import (
    PRODUCER_ALIASES,
    VENDOR_PLATFORMS,
    resolve_platform,
)


def test_systemlink_resolves_through_the_vendor_table():
    assert VENDOR_PLATFORMS == {"systemlink": "systemlink"}
    assert resolve_platform("systemlink") == "systemlink"


def test_the_vendor_table_is_not_the_producer_alias_table():
    assert "systemlink" not in PRODUCER_ALIASES


def test_a_deployment_override_still_beats_the_vendor_table():
    assert resolve_platform("systemlink", {"systemlink": "custom"}) == "custom"


def test_control_an_unmapped_name_passes_through_unchanged():
    assert resolve_platform("not_a_vendor") == "not_a_vendor"
