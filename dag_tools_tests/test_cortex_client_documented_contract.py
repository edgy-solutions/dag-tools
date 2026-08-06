"""Pins the claims docs/cortex-data-client.md makes about the client.

The doc exists because none of this was written down anywhere; these
tests exist so it doesn't quietly stop being true. Each one corresponds
to a statement a reader would act on.

The originator-header cases are the load-bearing ones. That path fails
CLOSED -- the gateway's Topaz check is email-keyed, and an M2M token
carries no user email, so omitting X-Originator-Email denies every user
uniformly rather than erroring in a way anyone would connect back to a
missing header.
"""
import os
from unittest.mock import patch

import httpx
import pytest

from dag_tools.cortex_data.client import CortexDataClient


BROKER = {"CORTEX_BROKER_URL": "http://gw", "MESH_DEV_TOKEN": "t"}
URN = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/table,PROD)"


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_missing_broker_url_names_the_env_var():
    """The doc tells operators to set CORTEX_BROKER_URL; the error has to
    agree, since it's what they'll actually read."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="CORTEX_BROKER_URL"):
            CortexDataClient()


def test_missing_auth_fails_at_construction_not_at_read():
    """Documented as raising up front. Deferring it to get_dataframe would
    surface a credentials problem as a data problem, hours later."""
    with patch.dict(os.environ, {"CORTEX_BROKER_URL": "http://gw"}, clear=True):
        with pytest.raises(ValueError, match="jwt_token|M2M"):
            CortexDataClient()


def test_dev_token_wins_over_m2m():
    env = {**BROKER, "CORTEX_CLIENT_ID": "id", "CORTEX_CLIENT_SECRET": "s"}
    with patch.dict(os.environ, env, clear=True):
        with patch.object(CortexDataClient, "_fetch_m2m_token") as m2m:
            client = CortexDataClient()
    assert client.jwt_token == "t"
    assert not m2m.called, "a dev token must not trigger a Keycloak round trip"


def test_trailing_slash_on_broker_url_is_normalised():
    """Otherwise every request URL doubles the slash."""
    with patch.dict(os.environ, {**BROKER, "CORTEX_BROKER_URL": "http://gw/"},
                    clear=True):
        assert CortexDataClient().gateway_url == "http://gw"


# ---------------------------------------------------------------------------
# The authorize call
# ---------------------------------------------------------------------------


class _Captured:
    """Records the outbound authorize request and returns a ticket whose
    source_type is deliberately unsupported -- every test here cares about
    the REQUEST, and raising keeps polars out of it."""

    def __init__(self):
        self.url = None
        self.headers = None

    def install(self):
        outer = self

        class _Resp:
            status_code = 200

            def raise_for_status(self):
                pass

            def json(self):
                return {"source_type": "__unsupported__", "physical_uri": "x",
                        "credentials": {}}

        class _Client:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def post(self, url, headers=None, **kw):
                outer.url, outer.headers = url, headers
                return _Resp()

        return patch.object(httpx, "Client", _Client)


def _authorize(**client_kwargs) -> _Captured:
    cap = _Captured()
    with patch.dict(os.environ, BROKER, clear=True):
        client = CortexDataClient(**client_kwargs)
        with cap.install():
            with pytest.raises(ValueError, match="Unsupported source_type"):
                client.get_dataframe(URN)
    return cap


def test_authorize_endpoint_shape():
    cap = _authorize()
    assert cap.url == f"http://gw/api/v1/assets/{URN}/authorize"
    assert cap.headers["Authorization"] == "Bearer t"


def test_originator_headers_are_sent_when_reading_for_a_user():
    cap = _authorize(originator_sub="sub-1", originator_email="u@example.com")
    assert cap.headers["X-Originator-Sub"] == "sub-1"
    assert cap.headers["X-Originator-Email"] == "u@example.com"


def test_originator_headers_are_absent_when_reading_as_the_service():
    """A Dagster asset reads as itself. Sending an empty originator would
    hand the gateway a subject it can't evaluate."""
    cap = _authorize()
    assert "X-Originator-Sub" not in cap.headers
    assert "X-Originator-Email" not in cap.headers
