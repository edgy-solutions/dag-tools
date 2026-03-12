import logging
from typing import Any, Dict, List, Optional
import httpx

logger = logging.getLogger(__name__)

class SapODataClient:
    """A generic, durable-ready HTTP client for SAP OData V2 services.
    Handles CSRF token management and JSON formatting.
    """

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password)
        self.csrf_token: Optional[str] = None
        self.cookies: Optional[httpx.Cookies] = None

    async def _get_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            auth=self.auth,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "x-csrf-token": "Fetch" if not self.csrf_token else self.csrf_token
            },
            cookies=self.cookies,
            timeout=30.0
        )

    async def _update_tokens(self, response: httpx.Response):
        """Extracts CSRF token and cookies from a response."""
        token = response.headers.get("x-csrf-token")
        if token and token.lower() != "fetch":
            self.csrf_token = token
            self.cookies = response.cookies
            logger.debug("Successfully updated SAP CSRF token and cookies.")

    async def get_entities(self, entity_set: str, filters: str = "") -> List[Dict[str, Any]]:
        """Fetches entities from a set with optional OData filters."""
        url = f"{self.base_url}/{entity_set}"
        params = {"$format": "json"}
        if filters:
            params["$filter"] = filters

        async with await self._get_client() as client:
            resp = await client.get(url, params=params)
            await self._update_tokens(resp)
            resp.raise_for_status()
            
            data = resp.json()
            # OData V2 results are typically under d/results or d
            d = data.get("d", {})
            return d.get("results", d) if isinstance(d, dict) else d

    async def call_function_import(
        self, 
        name: str, 
        params: Dict[str, Any], 
        method: str = "GET"
    ) -> List[Dict[str, Any]]:
        """Calls an SAP Function Import."""
        url = f"{self.base_url}/{name}"
        
        # OData V2 function imports take parameters via query string for GET
        # and typically require a CSRF token for any mutation-heavy action.
        request_params = {**params, "$format": "json"}
        
        async with await self._get_client() as client:
            if method.upper() == "GET":
                resp = await client.get(url, params=request_params)
            else:
                resp = await client.post(url, params=request_params)
            
            await self._update_tokens(resp)
            resp.raise_for_status()
            
            data = resp.json()
            d = data.get("d", {})
            return d.get("results", d) if isinstance(d, dict) else d
