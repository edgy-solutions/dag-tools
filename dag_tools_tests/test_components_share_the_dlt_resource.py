"""Two dlt-backed components must be able to live in one defs folder.

Dagster merges the ``Definitions`` a defs folder produces, and its
resource-collision check is IDENTITY, not equality::

    if resource_key in resources and resources[resource_key] is not resource_value:
        raise DagsterInvariantViolationError(...)

``DagsterDltResource() == DagsterDltResource()`` is True -- the class has
no config fields at all -- but ``is`` is False. So four dag-tools
components that each built their own could not coexist:

    DagsterInvariantViolationError: Definitions objects 2 and 3 have
    different resources with same key 'dlt'

The failure is at DEFINITIONS LOAD, which is what makes it severe: the
whole code location fails rather than one component degrading. Adding a
second dlt-backed component to a project took the first one offline with
it, and the message names Definitions by INDEX, so it does not even say
which two components collided.
"""
import pytest

pytest.importorskip("dagster_dlt")

from dagster import Definitions
from dagster_dlt import DagsterDltResource

from dag_tools.components.dlt_resource import (
    DLT_RESOURCE,
    DLT_RESOURCE_KEY,
    dlt_resources,
)


COMPONENT_MODULES = [
    "dag_tools.components.dlt_pipeline.component",
    "dag_tools.components.otel_api_sync.component",
    "dag_tools.components.restate_api_sync.component",
    "dag_tools.components.restate_dlt_sync.component",
]


def test_the_collision_check_is_identity_not_equality():
    """Pins the Dagster behaviour this exists to accommodate. If a future
    release relaxes it to equality, this test says so and the singleton
    becomes optional rather than load-bearing."""
    assert DagsterDltResource() == DagsterDltResource()
    assert DagsterDltResource() is not DagsterDltResource()

    with pytest.raises(Exception, match="different resources with same key"):
        Definitions.merge(
            Definitions(resources={"dlt": DagsterDltResource()}),
            Definitions(resources={"dlt": DagsterDltResource()}),
        )


@pytest.mark.parametrize("module_path", COMPONENT_MODULES)
def test_every_dlt_component_registers_the_same_instance(module_path):
    """The fix, stated per component so a new one that invents its own is
    named rather than showing up as an index in a merge error."""
    import importlib

    module = importlib.import_module(module_path)
    assert module.dlt_resources()[DLT_RESOURCE_KEY] is DLT_RESOURCE


def test_definitions_from_two_components_merge():
    """The reported failure: an extraction component and an otel component
    in one defs folder."""
    merged = Definitions.merge(
        Definitions(resources=dlt_resources()),
        Definitions(resources=dlt_resources()),
    )
    assert merged.resources[DLT_RESOURCE_KEY] is DLT_RESOURCE


def test_all_four_merge_together():
    """A defs folder may hold every one of them."""
    merged = Definitions.merge(
        *[Definitions(resources=dlt_resources()) for _ in COMPONENT_MODULES]
    )
    assert merged.resources[DLT_RESOURCE_KEY] is DLT_RESOURCE


def test_a_fresh_dict_is_handed_out_each_time():
    """The dict is per-call so a caller mutating it cannot reach through
    and corrupt every other component's resource map. Only the RESOURCE is
    shared, which is what identity requires."""
    first, second = dlt_resources(), dlt_resources()
    assert first is not second
    assert first[DLT_RESOURCE_KEY] is second[DLT_RESOURCE_KEY]


def test_the_shared_resource_carries_no_configuration():
    """Sharing is safe precisely because there is nothing for two
    components to disagree about. A future dagster-dlt that adds a
    configurable field would make one shared default a real decision
    rather than a free one -- this is the alarm for that."""
    assert list(DagsterDltResource.model_fields) == []
