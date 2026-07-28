"""Grist resource — read documents and tables from a Grist instance.

`Grist <https://www.getgrist.com/>`_ exposes each document as a
downloadable SQLite database over its REST API. This resource wraps the
small slice of that API the ingestion component needs:

  * list documents across an org's workspaces,
  * list the tables inside a document,
  * download a document and read one table into a pandas DataFrame.

The DataFrame is then handed to a SQL IO manager for publication to
Postgres — this resource only concerns itself with pulling data *out*
of Grist, never with where it lands.
"""
from __future__ import annotations

import sqlite3
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dagster import Config, ConfigurableResource
from pydantic import Field

from dag_tools.utils.helper import ConfigureFromDict


class GristConfig(Config):
    """Connection details for a Grist instance."""

    host: str = Field(description="Grist host, e.g. 'grist.example.com' (no scheme).")
    org: str = Field(description="Grist organization / team subdomain to enumerate.")
    token: str = Field(description="Grist API bearer token.")
    scheme: str = Field(default="https", description="URL scheme for the Grist host.")
    timeout_seconds: int = Field(default=30, description="Per-request HTTP timeout.")


class GristClient:
    """Thin REST client for the Grist API.

    One instance wraps one :class:`GristConfig`. All calls are stateless
    HTTP requests; nothing is cached between calls.
    """

    def __init__(self, config: GristConfig):
        self.config = config

    # --- URL helpers ------------------------------------------------------

    @property
    def _base(self) -> str:
        return f"{self.config.scheme}://{self.config.host}/api"

    @property
    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.config.token}"}

    # --- API calls --------------------------------------------------------

    def list_docs(self, since: Optional[Any] = None) -> List[Dict[str, Any]]:
        """Return every document across the org's workspaces.

        Each returned dict is the Grist doc object augmented with a
        ``workspace`` key (the workspace name) so downstream naming can
        include it. When ``since`` is provided, only docs whose
        ``updatedAt`` is strictly greater are returned — this is what the
        sensor uses to poll for changes.
        """
        url = f"{self._base}/orgs/{self.config.org}/workspaces"
        resp = requests.get(url, headers=self._headers, timeout=self.config.timeout_seconds)
        resp.raise_for_status()
        workspaces = resp.json() or []
        docs: List[Dict[str, Any]] = []
        for ws in workspaces:
            ws_name = ws.get("name", "")
            for doc in ws.get("docs", []) or []:
                if since and not (doc.get("updatedAt", "") > since):
                    continue
                docs.append({**doc, "workspace": ws_name})
        return docs

    def list_tables(self, doc_id: str) -> List[Dict[str, Any]]:
        """Return the tables in a document (each dict has an ``id``)."""
        url = f"{self._base}/docs/{doc_id}/tables"
        resp = requests.get(url, headers=self._headers, timeout=self.config.timeout_seconds)
        resp.raise_for_status()
        data = resp.json() or {}
        return data.get("tables", []) or []

    def load_table(self, doc_id: str, table_id: str, log: Any = None) -> Optional[pd.DataFrame]:
        """Download a document as SQLite and read one table as a DataFrame.

        Grist's ``/download`` endpoint returns a redirect to the SQLite
        snapshot; we stream it to a temp file, open it read-only, and
        pull the requested table. Returns ``None`` if the download fails.
        """
        download_url = f"{self._base}/docs/{doc_id}/download"
        # First hop resolves the redirect to the actual snapshot URL.
        first = requests.get(
            download_url, headers=self._headers, timeout=self.config.timeout_seconds
        )
        resp = requests.get(
            first.url, headers=self._headers, timeout=self.config.timeout_seconds * 2,
            stream=True,
        )
        if resp.status_code != 200:
            if log:
                log.warning(
                    "grist: download for doc=%s returned HTTP %s", doc_id, resp.status_code
                )
            return None

        with NamedTemporaryFile(suffix=".db") as tmp:
            for chunk in resp.iter_content(1024):
                tmp.write(chunk)
            tmp.flush()
            con = sqlite3.connect(f"file:{tmp.name}?mode=ro", uri=True)
            try:
                # Double-quote the identifier and escape embedded quotes so
                # table names with spaces / punctuation read correctly.
                safe_table = table_id.replace('"', '""')
                return pd.read_sql_query(f'SELECT * FROM "{safe_table}"', con)
            finally:
                con.close()


class GristResource(ConfigurableResource, ConfigureFromDict):
    """Dagster resource exposing a configured :class:`GristClient`."""

    config: GristConfig

    @classmethod
    def configure(cls, config: Dict[str, Any]) -> "GristResource":
        return cls.model_validate(config)

    def get_client(self) -> GristClient:
        return GristClient(self.config)
