"""The broker advertises only assets this deployment actually owns.

A Definitions routinely mixes two populations that look identical in the
inventory:

  * locally-produced assets — this deployment writes them, so it can
    truthfully tell the gateway where they live;
  * external/source stubs — read handles for assets ANOTHER deployment
    owns, declared so a local asset can consume them through the mesh.

Advertising the second kind hands the gateway a physical location this
deployment never wrote, and (because routes are stored last-writer-wins
on a short TTL) makes the route flap between the real owner and the
phantom. The broker filters on Dagster's own materializable/external
split so the guard holds regardless of which IO manager is bound.
"""
import pytest

pytest.importorskip("dagster")
pytest.importorskip("fastapi")  # domain_broker.main imports it at module level

from dagster import AssetSpec, Definitions, IOManager, asset

from dag_tools.domain_broker.main import _materializable_asset_keys


class _Noop(IOManager):
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return None


def _defs_with_local_and_foreign() -> Definitions:
    @asset(io_manager_key="iom")
    def local_produced() -> dict:
        return {"v": 1}

    # A read handle for data another deployment owns.
    foreign = AssetSpec(
        key="foreign_from_other_deployment",
        metadata={"dagster/io_manager_key": "iom"},
    )
    return Definitions(assets=[local_produced, foreign], resources={"iom": _Noop()})


def test_materializable_split_excludes_foreign_stub():
    keys = _materializable_asset_keys(_defs_with_local_and_foreign())
    assert keys == {("local_produced",)}
    assert ("foreign_from_other_deployment",) not in keys


def test_inventory_lists_both_and_flags_which_is_foreign():
    """The extractor deliberately does NOT drop the foreign stub -- the
    inventory is a description of the Definitions, and qualification needs
    to see external assets in order to classify them OBSERVE_ONLY.

    It marks them instead: ``is_executable`` carries Dagster's own
    materializable/external split into the record. The broker still
    filters via the asset graph rather than reading this field, because it
    must hold for records written by a survey older than schema_version 2
    (where the field is None), and advertising a phantom route is worse
    than the extra call."""
    from dag_tools.inventory import extract_records

    records = {tuple(r.asset_key): r for r in extract_records(
        _defs_with_local_and_foreign()
    )}
    assert records[("local_produced",)].is_executable is True
    assert records[("foreign_from_other_deployment",)].is_executable is False


def test_filter_is_skipped_safely_when_split_unavailable():
    """If the Dagster API shifts, return None -> callers don't filter.
    An over-advertising broker is a routing bug; a broker advertising
    NOTHING takes the domain offline. Fail toward the status quo."""

    class _Broken:
        def resolve_asset_graph(self):
            raise RuntimeError("API moved")

    assert _materializable_asset_keys(_Broken()) is None


def test_local_only_definitions_are_fully_advertised():
    """No external stubs -> nothing is filtered out."""
    @asset(io_manager_key="iom")
    def a() -> dict:
        return {}

    @asset(io_manager_key="iom")
    def b() -> dict:
        return {}

    defs = Definitions(assets=[a, b], resources={"iom": _Noop()})
    assert _materializable_asset_keys(defs) == {("a",), ("b",)}
