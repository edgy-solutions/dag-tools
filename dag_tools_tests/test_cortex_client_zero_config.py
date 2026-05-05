import os
import pytest
from unittest.mock import patch
from dag_tools.cortex_data.client import CortexDataClient

def test_cortex_client_zero_config_missing_env():
    """Verify it raises ValueError if no env vars or args provided."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Must provide broker_url or set CORTEX_BROKER_URL"):
            CortexDataClient()

def test_cortex_client_zero_config_with_env():
    """Verify it picks up CORTEX_BROKER_URL and MESH_DEV_TOKEN."""
    env = {
        "CORTEX_BROKER_URL": "http://broker",
        "MESH_DEV_TOKEN": "secret-token"
    }
    with patch.dict(os.environ, env, clear=True):
        client = CortexDataClient()
        assert client.gateway_url == "http://broker"
        assert client.jwt_token == "secret-token"

def test_cortex_client_m2m_env():
    """Verify it picks up M2M credentials."""
    env = {
        "CORTEX_BROKER_URL": "http://broker",
        "CORTEX_CLIENT_ID": "id",
        "CORTEX_CLIENT_SECRET": "secret"
    }
    # Mock _fetch_m2m_token to avoid network calls
    with patch.dict(os.environ, env, clear=True):
        with patch.object(CortexDataClient, '_fetch_m2m_token') as mock_fetch:
            client = CortexDataClient()
            assert client.gateway_url == "http://broker"
            assert client.client_id == "id"
            assert client.client_secret == "secret"
            mock_fetch.assert_called_once()
