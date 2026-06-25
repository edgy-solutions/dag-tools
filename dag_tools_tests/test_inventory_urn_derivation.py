"""Pin ``_derive_urn`` to ``dataPlatform:dagster`` — guards the
silent-failure URN-platform mismatch fixed in
``invincible-agent@4f6f4d2``.

This guards a **silent-failure** bug, which raises its priority
above the other "trigger drifts rarely" tests in the set: a
mismatched URN platform produces no error, just a 404 from the
domain broker when downstream consumers (Engine DA, the polars
IO manager, the DataHub emit path) build URNs with
``dataPlatform:dagster`` and the broker registry has them with
``dataPlatform:unknown``. The 404 looks like "data doesn't
exist" from the consumer's perspective, sending the operator on
a wild goose chase into the data pipeline when the actual bug is
the URN string-mismatch one level up.

The 2026-06-25 chart-empty arc spent a session chasing this:
broker returned 404, MinIO clearly had the parquet, the consumer
saw a successful write at materialization. The diagnostic clue
was a single URN string with ``unknown`` instead of ``dagster``
— a difference of 7 characters in a 70-character string, with no
type-checker or test to catch it.

Why this is a *silent* failure: ``_derive_urn`` returns a perfectly
valid URN. Nothing about ``unknown`` is malformed. The broker
accepts it. Consumers accept their own. The two just never match.
That's the failure mode worth pinning.
"""
from __future__ import annotations

import pytest


def _derive_or_skip():
    """Lazy import so this file works even when the dagster /
    datahub plugin chain isn't installed in the test environment.
    ``_derive_urn`` itself handles missing plugins by returning
    None; that's an interesting case but not what this test guards
    (it's a CI/lab plumbing concern, not a silent-failure bug)."""
    try:
        from dag_tools.inventory.extractors import _derive_urn
    except Exception as exc:  # pragma: no cover — env-specific
        pytest.skip(f"_derive_urn not importable: {exc}")
    return _derive_urn


def test_derive_urn_uses_dagster_platform_for_arbitrary_asset_key():
    """An asset key the converter doesn't auto-recognize MUST still
    land on ``dataPlatform:dagster`` — that's the platform the
    inventory walker is by definition operating in. Without the
    explicit ``platform='dagster'`` hint (the fix in
    invincible-agent@4f6f4d2), the converter falls back to
    ``unknown`` for any key whose first segment isn't in its
    recognized-platforms list (currently
    ``clickhouse / snowflake / postgres``)."""
    derive = _derive_or_skip()
    urn = derive(["mesh_demo_customers"])
    if urn is None:
        pytest.skip(
            "datahub_dagster_plugin not installed in this env — "
            "_derive_urn returned None"
        )
    assert "dataPlatform:dagster" in urn, (
        f"URN platform is wrong: {urn!r}. Expected dataPlatform:dagster. "
        f"If you see dataPlatform:unknown, the explicit platform hint "
        f"in _derive_urn was dropped or the converter defaults regressed. "
        f"This is the silent-failure bug from invincible-agent@4f6f4d2 — "
        f"consumers build URNs with :dagster, broker registers with "
        f":unknown, every fetch returns 404 with no actionable error."
    )


def test_derive_urn_uses_dagster_platform_for_multipart_key():
    """Multi-part asset keys (the more typical Dagster shape) must
    also land on ``dataPlatform:dagster``."""
    derive = _derive_or_skip()
    urn = derive(["mesh", "demo", "customers"])
    if urn is None:
        pytest.skip(
            "datahub_dagster_plugin not installed in this env — "
            "_derive_urn returned None"
        )
    assert "dataPlatform:dagster" in urn, (
        f"URN platform is wrong: {urn!r}. Expected dataPlatform:dagster."
    )


def test_derive_urn_never_produces_unknown_platform():
    """Stronger invariant: NO call to _derive_urn should ever
    produce ``dataPlatform:unknown``. If the converter doesn't know
    the platform, ``_derive_urn``'s explicit hint must override
    that. This is the negative form of the assertion above — it's
    what fails first when someone removes the platform hint.

    Per the standing rule [[feedback-verification-must-fail]]:
    REQUIRE that at least one URN was successfully derived before
    declaring the negative assertion passed. Otherwise, in an env
    with no datahub plugin (every derive returns None), the loop
    completes with zero assertions and the test passes for the
    WRONG reason — the very pattern the standing rule was banked
    against.
    """
    derive = _derive_or_skip()
    derived_any = False
    for asset_key in (
        ["mesh_demo_customers"],
        ["mesh", "demo", "customers"],
        ["some", "random", "asset"],
    ):
        urn = derive(asset_key)
        if urn is None:
            continue
        derived_any = True
        assert "dataPlatform:unknown" not in urn, (
            f"_derive_urn produced an UNKNOWN-platform URN for {asset_key!r}: "
            f"{urn!r}. This is the silent failure mode — consumers build "
            f"URNs with dataPlatform:dagster, broker registers with :unknown, "
            f"every downstream fetch returns 404 with no actionable error."
        )
    if not derived_any:
        pytest.skip(
            "datahub_dagster_plugin not installed in this env — no URNs "
            "could be derived to assert against. The test logic is correct; "
            "the env can't exercise it. Skipping (per "
            "[[feedback-verification-must-fail]]) rather than passing "
            "vacuously."
        )
