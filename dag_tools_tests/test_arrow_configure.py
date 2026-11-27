"""``ConfigurableArrowIOManager`` and ``ArrowResource`` must honour the
``ConfigureFromDict`` seam.

Neither mixed it in and neither defined ``configure``, so a deployment
that configures its IO managers from a config file -- the way every other
one in this package is configured -- got ``AttributeError: type object
'ConfigurableArrowIOManager' has no attribute 'configure'`` at the first
line of its code location. Nothing in the suite called ``configure`` on
either class, which is why the gap survived every release.
"""
from typing import Any, Dict

import pytest

pytest.importorskip("pyarrow")

from dag_tools.io_managers.arrow import ConfigurableArrowIOManager, S3FSConfig
from dag_tools.resources.arrow import ArrowResource
from dag_tools.utils.helper import ConfigureFromDict

# The INNER block -- what a config file nests under ``config:``. The fields of
# this model are flat, unlike ``ConfigurableSQLIOManager``'s single ``config:``
# wrapper, so the two ``configure`` methods take different levels of the same
# document on purpose.
S3_COMMON: Dict[str, Any] = {
    "access_key_id": "k",
    "secret_access_key": "s",
    "end_point": "https://example.invalid",
}


def test_the_io_manager_takes_the_one_key_discriminated_union_form():
    """Dagster's pythonic config spells a discriminated union its own way.

    Exactly ONE key -- the discriminator's VALUE -- holding the rest of the
    variant. This is the spelling an operator's config file carries, and it
    is the one measured to work.
    """
    manager = ConfigurableArrowIOManager.configure({
        "fs": {"s3": {"common": dict(S3_COMMON)}},
        "uri_base": "s3://bucket/prefix",
    })
    assert isinstance(manager, ConfigurableArrowIOManager)
    assert isinstance(manager.fs, S3FSConfig)
    assert manager.fs.common.access_key_id == "k"
    assert manager.uri_base == "s3://bucket/prefix"


def test_pydantics_own_tag_field_spelling_is_refused_by_dagster():
    """The control, and the reason the docstring says what it says.

    ``{"type_": "s3", ...}`` is how pydantic itself tags a discriminated
    union, so it is the shape anyone reading the model's annotation would
    try first. Dagster refuses it -- in ``dagster._config.pythonic_config``,
    not in our ``configure`` -- and nothing here translates one spelling
    into the other, because that would make this the only place in the repo
    where a Dagster config dict means something different.
    """
    with pytest.raises(Exception) as excinfo:
        ConfigurableArrowIOManager.configure({
            "fs": {"type_": "s3", "common": dict(S3_COMMON)},
            "uri_base": "s3://bucket/prefix",
        })
    assert "exactly one key" in str(excinfo.value).lower()


def test_the_io_manager_takes_a_prebuilt_variant_too():
    manager = ConfigurableArrowIOManager.configure({
        "fs": S3FSConfig(common=S3_COMMON),
        "uri_base": "s3://bucket/prefix",
    })
    assert isinstance(manager.fs, S3FSConfig)


def test_the_resource_takes_its_flat_block():
    resource = ArrowResource.configure({
        "uri_base": "s3://bucket/prefix", **S3_COMMON,
    })
    assert isinstance(resource, ArrowResource)
    assert resource.uri_base == "s3://bucket/prefix"
    assert resource.region == "us-east-1"


@pytest.mark.parametrize("cls", [ConfigurableArrowIOManager, ArrowResource])
def test_the_seam_is_implemented_rather_than_merely_inherited(cls):
    """Mixing ``ConfigureFromDict`` in is not the same as honouring it.

    Its ``configure`` raises ``NotImplementedError``, so a class that
    inherits it looks configurable to every ``hasattr`` check and fails at
    the one call that matters. The identity comparison is what tells the
    two states apart.

    Deliberately NOT a sweep over every ``Configurable*`` class in the
    package: ``ConfigurableDuckDBIOManager`` has the same gap and is
    reported rather than fixed here, and ``TaggedWarehouseResource``
    legitimately has no such seam (AGENTS.md 4j).
    """
    assert issubclass(cls, ConfigureFromDict)
    assert cls.configure.__func__ is not ConfigureFromDict.configure.__func__


def test_control_a_class_that_only_inherits_the_seam_fails_that_check():
    """Without this, the check above would pass under any ``configure`` at
    all -- including the inherited one it exists to reject."""

    class OnlyInherits(ConfigureFromDict):
        pass

    assert OnlyInherits.configure.__func__ is ConfigureFromDict.configure.__func__
    with pytest.raises(NotImplementedError):
        OnlyInherits.configure({})
