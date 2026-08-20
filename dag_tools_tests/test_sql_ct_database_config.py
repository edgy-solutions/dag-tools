import unittest
from unittest.mock import MagicMock, patch
import pytest

# `dag_tools.asset_wrappers.__init__` eagerly imports dlt_assets_factory,
# which imports dlt -- so without this the module errors at COLLECTION
# time and aborts the whole run rather than skipping one file.
pytest.importorskip("dlt")

from dag_tools.asset_wrappers.sources.sql_ct_database import sql_ct_database


class TestSqlCtDatabaseConfig(unittest.TestCase):
    def setUp(self):
        self.credentials = "mssql://user:pass@host/db"
        
    @patch("dag_tools.asset_wrappers.sources.sql_ct_database.engine_from_credentials")
    @patch("dag_tools.asset_wrappers.sources.sql_ct_database.official_sql_database")
    @patch("dag_tools.asset_wrappers.sources.sql_ct_database._internal_ct_source")
    def test_use_ct_false_disables_ct(self, mock_ct_source, mock_official_source, mock_engine):
        # Mock CT as enabled on the DB
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 1
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        # Call with use_ct=False
        sql_ct_database(credentials=self.credentials, use_ct=False)
        
        # Should call official source, NOT CT source
        mock_official_source.assert_called()
        mock_ct_source.assert_not_called()

    @patch("dag_tools.asset_wrappers.sources.sql_ct_database.engine_from_credentials")
    @patch("dag_tools.asset_wrappers.sources.sql_ct_database.official_sql_database")
    @patch("dag_tools.asset_wrappers.sources.sql_ct_database._internal_ct_source")
    def test_write_disposition_replace_disables_ct(self, mock_ct_source, mock_official_source, mock_engine):
        # Mock CT as enabled on the DB
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 1
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        # Call with write_disposition="replace"
        sql_ct_database(credentials=self.credentials, write_disposition="replace")
        
        # Should call official source, NOT CT source
        mock_official_source.assert_called()
        mock_ct_source.assert_not_called()

    @patch("dag_tools.asset_wrappers.sources.sql_ct_database.engine_from_credentials")
    @patch("dag_tools.asset_wrappers.sources.sql_ct_database.official_sql_database")
    @patch("dag_tools.asset_wrappers.sources.sql_ct_database._internal_ct_source")
    def test_ct_enabled_by_default(self, mock_ct_source, mock_official_source, mock_engine):
        # Mock CT as enabled on the DB
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 1
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        # Call with default (use_ct=True, write_disposition="merge")
        sql_ct_database(credentials=self.credentials)
        
        # Should call CT source
        mock_ct_source.assert_called()

if __name__ == "__main__":
    unittest.main()
