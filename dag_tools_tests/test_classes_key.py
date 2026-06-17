"""Unit tests for class-key construction + hashing.

The key tuple is what makes two assets land in the same equivalence class.
Wrong key construction = wrong fleet grouping = wrong qualification
verdict. So this matrix covers every shape that matters.
"""
from dag_tools.inventory import AssetRecord
from dag_tools.qual.classes import (
    ClassKeyComponents,
    class_hash,
    compute_class_key,
)


def _record(**over):
    """Minimal-viable AssetRecord factory."""
    base = dict(
        asset_key=["foo", "bar"],
        compute_kind="python",
        io_manager_key="io_manager",
        io_manager_class="dagster.InMemoryIOManager",
        partitions_def_class=None,
        partition_mapping_classes=[],
        resource_keys=["io_manager"],
        resource_classes={"io_manager": "dagster.InMemoryIOManager"},
        integration_libs=[],
        has_asset_checks=False,
        automation_condition_type=None,
        tags={},
    )
    base.update(over)
    return AssetRecord(**base)


# ---------------------------------------------------------------------------
# Component construction
# ---------------------------------------------------------------------------


def test_compute_class_key_excludes_io_manager_from_resource_classes():
    """The IO manager has its own component; including its class in the
    resource_classes set too would double-count it without adding any
    discrimination."""
    rec = _record(
        resource_keys=["io_manager", "snowflake_db"],
        resource_classes={
            "io_manager": "dagster.InMemoryIOManager",
            "snowflake_db": "myco.resources.Snowflake",
        },
    )
    key = compute_class_key(rec)
    assert "dagster.InMemoryIOManager" not in key.resource_classes
    assert "myco.resources.Snowflake" in key.resource_classes


def test_compute_class_key_sorts_unordered_components():
    """Partition mapping classes / integration libs / resource classes get
    sorted so two assets with the same set in different orders match."""
    rec = _record(
        partition_mapping_classes=["zeta.M", "alpha.M"],
        integration_libs=["dagster_dbt", "dagster_aws"],
    )
    key = compute_class_key(rec)
    assert key.partition_mapping_classes == ["alpha.M", "zeta.M"]
    assert key.integration_libs == ["dagster_aws", "dagster_dbt"]


def test_compute_class_key_carries_automation_and_checks():
    rec = _record(
        has_asset_checks=True,
        automation_condition_type="dagster.AutoMaterializeCondition",
    )
    key = compute_class_key(rec)
    assert key.has_asset_checks is True
    assert key.automation_condition_type == "dagster.AutoMaterializeCondition"


# ---------------------------------------------------------------------------
# Custom dbt translator handling
# ---------------------------------------------------------------------------


def test_custom_dbt_translator_only_attached_for_dbt_assets():
    """Recipe rule: custom translators always form their own classes, but
    only for assets that actually use dagster_dbt. A python asset in the
    same repo doesn't get pulled into a translator-marked class."""
    dbt_rec = _record(
        compute_kind="dbt",
        integration_libs=["dagster_dbt"],
    )
    python_rec = _record(
        compute_kind="python",
        integration_libs=[],
    )
    translators = ["myco.CustomTranslator"]
    dbt_key = compute_class_key(dbt_rec, custom_dbt_translators_in_repo=translators)
    py_key = compute_class_key(python_rec, custom_dbt_translators_in_repo=translators)
    assert dbt_key.custom_dbt_translator_classes == ["myco.CustomTranslator"]
    assert py_key.custom_dbt_translator_classes == []


def test_custom_dbt_translator_detected_via_integration_libs():
    """If integration_libs includes dagster_dbt, that's enough to be a
    "dbt asset" — even when compute_kind is something other than 'dbt'."""
    rec = _record(
        compute_kind="python",
        integration_libs=["dagster_dbt"],
    )
    key = compute_class_key(rec, custom_dbt_translators_in_repo=["myco.T"])
    assert key.custom_dbt_translator_classes == ["myco.T"]


def test_no_custom_translators_leaves_field_empty():
    rec = _record(compute_kind="dbt", integration_libs=["dagster_dbt"])
    key = compute_class_key(rec, custom_dbt_translators_in_repo=[])
    assert key.custom_dbt_translator_classes == []


# ---------------------------------------------------------------------------
# Deterministic hashing
# ---------------------------------------------------------------------------


def test_class_hash_is_deterministic():
    """Same key components -> same hash, every time."""
    key = ClassKeyComponents(
        compute_kind="python",
        io_manager_class="X",
        partition_mapping_classes=["A", "B"],
        resource_classes=["R1", "R2"],
        integration_libs=["dagster_aws", "dagster_dbt"],
        has_asset_checks=True,
        automation_condition_type="C",
    )
    h1 = class_hash(key)
    h2 = class_hash(key)
    assert h1 == h2
    assert len(h1) == 12


def test_class_hash_changes_when_any_component_changes():
    base = ClassKeyComponents(compute_kind="python")
    other = ClassKeyComponents(compute_kind="dbt")
    assert class_hash(base) != class_hash(other)


def test_class_hash_insensitive_to_set_field_ordering():
    """Two ClassKeyComponents with identically-valued but
    differently-ordered list fields should NOT hash the same unless they
    were both constructed via compute_class_key (which sorts). This test
    confirms class_hash itself doesn't sort — it relies on compute_class_key
    to do so. Documents the contract."""
    unsorted = ClassKeyComponents(
        integration_libs=["dagster_dbt", "dagster_aws"],
    )
    sorted_ = ClassKeyComponents(
        integration_libs=["dagster_aws", "dagster_dbt"],
    )
    # They should hash DIFFERENTLY because class_hash is JSON-deterministic
    # over the ALREADY-stored ordering. compute_class_key is responsible
    # for normalization.
    assert class_hash(unsorted) != class_hash(sorted_)


def test_two_assets_with_same_components_share_a_hash():
    """End-to-end intent check: two AssetRecords whose recipe components
    match produce the same hash even though they come from different
    repos / files / etc."""
    a = _record(asset_key=["repoA", "tab1"])
    b = _record(asset_key=["repoB", "tab2"])
    h_a = class_hash(compute_class_key(a))
    h_b = class_hash(compute_class_key(b))
    assert h_a == h_b


def test_two_assets_differing_only_in_io_manager_have_distinct_hashes():
    a = _record(io_manager_class="dagster.InMemoryIOManager")
    b = _record(io_manager_class="myco.io.CustomIOManager")
    assert class_hash(compute_class_key(a)) != class_hash(compute_class_key(b))


def test_custom_dbt_translator_segregates_assets_into_own_class():
    """The whole point of the translator rule: two dbt assets that would
    otherwise share a class get pushed into separate classes when one's
    repo has a custom translator."""
    rec = _record(compute_kind="dbt", integration_libs=["dagster_dbt"])
    stock_key = compute_class_key(rec, custom_dbt_translators_in_repo=[])
    custom_key = compute_class_key(
        rec, custom_dbt_translators_in_repo=["myco.CustomTranslator"]
    )
    assert class_hash(stock_key) != class_hash(custom_key)
