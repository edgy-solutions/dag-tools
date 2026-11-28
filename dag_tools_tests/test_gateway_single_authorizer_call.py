"""The gateway asks the authorizer through ONE endpoint, and nothing in the
package queries the directory itself.

Another lane relies on this: invincible-agent #524 widens a dataset's
``reader`` relation to groups, and was told that dag-tools never reads that
relation directly, so the widening is the policy's alone. A directory-shaped
payload (``check_payload``) once sat in ``topaz_decision`` beside the real
one, built on every call and never sent. It did no harm, but it made the
answer to "does anything here query the directory?" depend on reading closely
enough to notice it was dead. It is gone; this keeps it gone.

The scan is over source text, which is deliberate: the property is about
what the package is able to call, and a directory endpoint named anywhere
in it is the first step towards calling one.
"""
import pathlib
import re

PACKAGE = pathlib.Path(__file__).resolve().parents[1] / "dag_tools"

# The authorizer's REST surface, versioned. Matches the endpoint the gateway
# calls today and any directory route (``/api/v<n>/directory/...``).
ENDPOINT = re.compile(r"/api/v\d+/(?:authz|directory)[A-Za-z0-9_/]*")


def _endpoints(text):
    return set(ENDPOINT.findall(text))


def _named_in_package():
    found = {}
    for path in PACKAGE.rglob("*.py"):
        for endpoint in _endpoints(path.read_text(encoding="utf-8")):
            found.setdefault(endpoint, set()).add(path.relative_to(PACKAGE).as_posix())
    return found


def test_the_package_names_one_authorizer_endpoint_and_no_directory_route():
    found = _named_in_package()
    assert set(found) == {"/api/v2/authz/is"}, found


def test_only_the_gateway_names_it():
    assert _named_in_package()["/api/v2/authz/is"] == {"central_gateway/main.py"}


def test_control_the_scan_sees_a_directory_route():
    """Without this, the first test would pass on a pattern that matched
    nothing but the one string it was written against."""
    planted = 'client.post(f"{TOPAZ_URL}/api/v2/directory/check", json=p)'
    assert _endpoints(planted) == {"/api/v2/directory/check"}
