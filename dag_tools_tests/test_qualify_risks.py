"""Tests for compute_co_upgrade_risks.

The diff is the load-bearing logic for spotting hidden dbt-core / warehouse
bumps inside a Dagster upgrade. False positives are an operator annoyance;
false negatives are an upgrade-day surprise. So the matrix below covers
every shape we care about.
"""
import pytest

from dag_tools.qual.qualify import compute_co_upgrade_risks


def test_no_shared_libs_no_risks():
    risks = compute_co_upgrade_risks(
        {"dbt-core": "1.8.5"},
        {"snowflake-connector": "3.0.0"},
    )
    assert risks == []


def test_same_version_no_risk():
    risks = compute_co_upgrade_risks(
        {"dbt-core": "1.8.5"}, {"dbt-core": "1.8.5"},
    )
    assert risks == []


def test_dagster_family_is_filtered_out():
    """The whole point of the diff is to spot HIDDEN co-upgrades. The
    Dagster family is the explicit upgrade target and is excluded."""
    risks = compute_co_upgrade_risks(
        {"dagster": "1.10.6", "dagster-dbt": "0.27.0", "dagster-k8s": "0.27.0"},
        {"dagster": "1.12.1", "dagster-dbt": "0.29.0", "dagster-k8s": "0.29.0"},
    )
    assert risks == []


def test_dbt_core_minor_bump_is_warning():
    risks = compute_co_upgrade_risks(
        {"dbt-core": "1.8.5"},
        {"dbt-core": "1.9.0"},
    )
    assert len(risks) == 1
    r = risks[0]
    assert r.lib == "dbt-core"
    assert r.from_version == "1.8.5"
    assert r.to_version == "1.9.0"
    assert r.severity == "warning"


def test_dbt_core_major_bump_is_major():
    risks = compute_co_upgrade_risks(
        {"dbt-core": "1.8.5"},
        {"dbt-core": "2.0.0"},
    )
    assert risks[0].severity == "major"


def test_wildcard_x_minor_treated_as_equivalent():
    """1.10.x vs 1.10.5 is "the same minor"; not a risk."""
    risks = compute_co_upgrade_risks(
        {"dbt-core": "1.10.x"},
        {"dbt-core": "1.10.5"},
    )
    assert risks == []


def test_wildcard_x_minor_vs_next_minor_is_warning():
    """1.10.x vs 1.11.0 IS a risk (different minor)."""
    risks = compute_co_upgrade_risks(
        {"dbt-core": "1.10.x"},
        {"dbt-core": "1.11.0"},
    )
    assert len(risks) == 1
    assert risks[0].severity == "warning"


def test_unparseable_versions_are_warning_not_crash():
    """A git-SHA pin or release tag falls back to warning severity rather
    than crashing — operators sometimes use non-semver strings."""
    risks = compute_co_upgrade_risks(
        {"weird-pkg": "release-2023-q1"},
        {"weird-pkg": "release-2024-q2"},
    )
    assert len(risks) == 1
    assert risks[0].severity == "warning"


def test_majors_sort_before_warnings():
    """The result is ordered (severity desc, lib asc) so majors float to
    the top when the operator scans the manifest."""
    risks = compute_co_upgrade_risks(
        {
            "alpha-pkg": "1.0.0",
            "beta-pkg": "1.0.0",
            "gamma-pkg": "1.0.0",
        },
        {
            "alpha-pkg": "1.1.0",  # warning
            "beta-pkg": "2.0.0",   # major
            "gamma-pkg": "1.0.1",  # warning
        },
    )
    severities = [r.severity for r in risks]
    libs = [r.lib for r in risks]
    assert severities == ["major", "warning", "warning"]
    # within a severity, sorted by lib
    assert libs == ["beta-pkg", "alpha-pkg", "gamma-pkg"]


def test_lib_in_one_side_only_is_not_a_risk():
    """A library that exists only in baseline (or only in candidate) isn't
    a "diff" — it's an addition/removal, separate concern. Not flagged."""
    risks = compute_co_upgrade_risks(
        {"only-in-baseline": "1.0.0"},
        {"only-in-candidate": "1.0.0"},
    )
    assert risks == []


def test_extra_zero_versus_unspecified_is_equivalent():
    """1.10 vs 1.10.0 should not be flagged — they're the same version
    expressed differently."""
    risks = compute_co_upgrade_risks(
        {"pkg": "1.10"}, {"pkg": "1.10.0"},
    )
    assert risks == []
