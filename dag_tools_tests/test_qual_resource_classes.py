"""Phase C3: resource key -> FQN travels via ClassMember / Representative."""
from dag_tools.inventory import AssetRecord
from dag_tools.qual.classes import ClassMember, Representative, Runnability
from dag_tools.qual.classes.selection import _to_representative


def test_class_member_carries_resource_classes_from_the_record():
    rec = AssetRecord(asset_key=["a"], resource_classes={"warehouse": "pkg.Wh"})
    m = ClassMember(
        repo="r", git_sha="s", asset_key=["a"],
        resource_classes=dict(rec.resource_classes or {}),
    )
    assert m.resource_classes == {"warehouse": "pkg.Wh"}
    assert ClassMember(repo="r", git_sha="s", asset_key=["a"]).resource_classes == {}


def test_representative_carries_resource_classes_through_selection():
    m = ClassMember(repo="r", git_sha="s", asset_key=["a"], resource_classes={"w": "pkg.Wh"})
    assert _to_representative(m, prefer_tag="regression").resource_classes == {"w": "pkg.Wh"}


def test_an_old_class_matrix_without_resource_classes_still_loads():
    rep = Representative.model_validate({
        "repo": "r", "git_sha": "s", "asset_key": ["a"],
        "runnability": Runnability.RUNNABLE.value, "runnability_reason": "x",
    })
    assert rep.resource_classes == {}
