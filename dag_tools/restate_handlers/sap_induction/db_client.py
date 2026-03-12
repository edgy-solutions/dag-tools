import logging
import sqlalchemy as sa
from typing import Any, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class SapDbClient:
    """A generic database client for updating SAP induction status in the source system."""

    def __init__(self, dsn: str):
        self.engine = sa.create_engine(dsn)

    def update_record_status(
        self,
        table: str,
        pk_column: str,
        pk_value: Any,
        status: str,
        columns_map: Dict[str, str],
        error_message: Optional[str] = None,
        sap_doc: Optional[str] = None,
        retry_count: Optional[int] = None
    ):
        """Updates the status and metadata for a specific record in the source database."""
        now = datetime.now()
        
        # Build set values using the mapped column names from config
        set_values = {
            columns_map["status"]: status,
            columns_map["status_timestamp"]: now,
            columns_map["last_attempt"]: now
        }
        
        if error_message is not None:
            set_values[columns_map["error_message"]] = error_message
            
        if sap_doc is not None:
            set_values[columns_map["sap_document"]] = sap_doc

        if retry_count is not None:
            set_values[columns_map["retry_count"]] = retry_count
        
        # CORRECTED: Proper SQLAlchemy Core syntax
        table_obj = sa.table(table, sa.column(pk_column))
        stmt = (
            sa.update(table_obj)
            .where(table_obj.c[pk_column] == pk_value)
            .values(**set_values)
        )
        
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
            logger.info(f"Updated record {pk_value} to status {status} in {table}")
