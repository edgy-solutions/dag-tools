import unittest
from unittest.mock import MagicMock, patch
import sqlalchemy as sa
import pytest

# `dag_tools.asset_wrappers.__init__` eagerly imports dlt_assets_factory,
# which imports dlt -- so without this the module errors at COLLECTION
# time and aborts the whole run rather than skipping one file.
pytest.importorskip("dlt")

from dag_tools.asset_wrappers.sources.sql_ct_database import _make_ct_generator


class TestCtLogicMocked(unittest.TestCase):
    def setUp(self):
        self.engine = MagicMock(spec=sa.engine.Engine)
        self.table = MagicMock(spec=sa.Table)
        self.table.name = "TestCT"
        self.table.schema = "dbo"
        self.table.primary_key.columns = [MagicMock(name="id")]
        self.table.columns = [MagicMock(name="id"), MagicMock(name="name"), MagicMock(name="value")]
        
    @patch("dlt.current.resource_state")
    def test_incremental_extraction_logic(self, mock_state):
        # 1. Setup State (last sync at version 10)
        mock_state.return_value = {"last_sync_version": 10}
        
        # 2. Setup Mock Connection
        conn = self.engine.connect.return_value.__enter__.return_value
        
        # Mock CHANGETABLE results proxy
        mock_result_proxy = MagicMock()
        mock_result_proxy.fetchmany.side_effect = [
            [
                # Row 1: Update
                MagicMock(_mapping={"id": 1, "name": "Updated", "value": 100, "SYS_CHANGE_OPERATION": "U", "SYS_CHANGE_VERSION": 15}),
                # Row 2: Delete
                MagicMock(_mapping={"id": 2, "name": None, "value": None, "SYS_CHANGE_OPERATION": "D", "SYS_CHANGE_VERSION": 16}),
                # Row 3: Insert
                MagicMock(_mapping={"id": 3, "name": "New", "value": 300, "SYS_CHANGE_OPERATION": "I", "SYS_CHANGE_VERSION": 17}),
            ],
            [] # End of results
        ]

        # Mock query sequence
        conn.execute.side_effect = [
            MagicMock(scalar=lambda: 20), # curr_ver_query
            MagicMock(scalar=lambda: 5),  # min_ver_query (10 >= 5 is valid)
            mock_result_proxy             # CHANGETABLE results
        ]
        
        # 3. Initialize Generator
        generator_fn = _make_ct_generator(self.engine, self.table, chunk_size=100)
        generator = generator_fn()
        
        # 4. Consume Generator fully
        all_results = []
        for batch in generator:
            all_results.extend(batch)
        
        # 5. Verify Results
        self.assertEqual(len(all_results), 3)
        
        # Row 1 (Update)
        self.assertEqual(all_results[0]["id"], 1)
        self.assertEqual(all_results[0]["_dlt_deleted"], False)
        
        # Row 2 (Delete)
        self.assertEqual(all_results[1]["id"], 2)
        self.assertEqual(all_results[1]["_dlt_deleted"], True) # CRITICAL: Verify this flag
        
        # Row 3 (Insert)
        self.assertEqual(all_results[2]["id"], 3)
        self.assertEqual(all_results[2]["_dlt_deleted"], False)
        
        # 6. Verify State Update
        self.assertEqual(mock_state.return_value["last_sync_version"], 20)

if __name__ == "__main__":
    unittest.main()
