from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from dagster import AssetKey


class CredentialProvider(ABC):
    """Abstract base class for credential providers that resolve specific integration secrets."""

    def __init__(self, key: str):
        self._key = key

    @abstractmethod
    def get_config(self, key: AssetKey) -> Any:
        """Must be implemented by subclasses to return specific secrets."""
        pass

    def get_key(self) -> str:
        return self._key


_providers: Dict[str, CredentialProvider] = {}


def register_credential_provider(credential_provider: CredentialProvider) -> None:
    """Registers a Credential Provider globally handling its target asset prefix."""
    _providers[credential_provider.get_key()] = credential_provider


def get_credentials(key: AssetKey) -> Optional[Any]:
    """Retrieves credentials for an AssetKey based on its top-level path prefix."""
    if not key.path:
        return None

    prefix = key.path[0]
    provider = _providers.get(prefix)
    return provider.get_config(key) if provider else None
