"""One `DagsterDltResource` instance, shared by every component that needs it.

WHY A SINGLETON AND NOT A FRESH INSTANCE PER COMPONENT. Dagster merges the
`Definitions` a defs folder produces, and its resource-collision check is
IDENTITY, not equality::

    # dagster/_core/definitions/definitions_class.py
    if resource_key in resources and resources[resource_key] is not resource_value:
        raise DagsterInvariantViolationError(
            f"Definitions objects {i} and {j} have different resources with "
            f"same key '{resource_key}'"
        )

`DagsterDltResource() == DagsterDltResource()` is True -- it has no config
fields at all -- but `is` is False, so two components that each built their
own could not coexist in one defs folder:

    DagsterInvariantViolationError: Definitions objects 2 and 3 have
    different resources with same key 'dlt'

The failure is at DEFINITIONS LOAD, so the whole code location fails to
load rather than one component degrading: adding a second dlt-backed
component to a project took the first one offline with it.

Sharing one instance is safe precisely because the resource carries no
configuration -- there is nothing for two components to disagree about,
and Dagster instantiates per run from the definition either way. A user
who declares their own `dlt` resource still wins; this only removes the
collision between components that would otherwise each invent one.
"""
from dagster_dlt import DagsterDltResource

#: The instance every dag-tools component registers under the key ``dlt``.
#: Module-level so identity holds across components in one defs folder.
DLT_RESOURCE = DagsterDltResource()

DLT_RESOURCE_KEY = "dlt"


def dlt_resources() -> dict:
    """``{"dlt": <the shared instance>}``, ready to hand to ``Definitions``."""
    return {DLT_RESOURCE_KEY: DLT_RESOURCE}
