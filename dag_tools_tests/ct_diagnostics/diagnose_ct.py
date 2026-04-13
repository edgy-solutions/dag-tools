import dlt
import sqlalchemy as sa
import time
from dag_tools.asset_wrappers.sources.sql_ct_database import sql_ct_database

# Configuration
SQLSERVER_URL = "mssql+pyodbc://sa:Password123!@localhost:1433/SOURCE_DB?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"
POSTGRES_URL = "postgresql://admin:password@localhost:5432/dest_db"

def get_row_count(engine, table_name, schema="extracted_data"):
    with engine.connect() as conn:
        # dlt normalizes table names to lowercase
        query = sa.text(f"SELECT COUNT(*) FROM {schema}.{table_name.lower()}")
        result = conn.execute(query).scalar()
        return result

def run_sync(table_names, pipeline_name="ct_multi_table_pipeline", schema="dbo"):
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.postgres(POSTGRES_URL),
        dataset_name="extracted_data"
    )
    
    source = sql_ct_database(
        credentials=SQLSERVER_URL,
        schema=schema,
        table_names=table_names
    )
    
    info = pipeline.run(source, write_disposition="merge")
    return info

def main():
    source_engine = sa.create_engine(SQLSERVER_URL)
    dest_engine = sa.create_engine(POSTGRES_URL)
    
    print("\n--- STEP 1: Initial Sync (All Tables) ---")
    run_sync(table_names=["TestCT", "TestCT_Extra"])
    
    count1 = get_row_count(dest_engine, "TestCT")
    count2 = get_row_count(dest_engine, "TestCT_Extra")
    print(f"Postgres: TestCT={count1}, TestCT_Extra={count2}")
    assert count1 == 2
    assert count2 == 1
    
    print("\n--- STEP 2: Triggering the Bug (Shared State Data Loss) ---")
    # 1. Add data to BOTH tables
    with source_engine.connect() as conn:
        conn.execute(sa.text("INSERT INTO TestCT (id, name, value) VALUES (3, 'New Row 3', 300)"))
        conn.execute(sa.text("INSERT INTO TestCT_Extra (id, name) VALUES (2, 'Extra Row 2')"))
        conn.commit()
    
    # 2. Sync ONLY TestCT. 
    # EXPECTED BUG: last_sync_version for the source will advance to current DB version.
    print("Syncing ONLY TestCT...")
    run_sync(table_names=["TestCT"])
    
    # 3. Sync TestCT_Extra.
    # EXPECTED BUG: It will see the new last_sync_version and SKIP the new row 2.
    print("Syncing TestCT_Extra...")
    run_sync(table_names=["TestCT_Extra"])
    
    count2 = get_row_count(dest_engine, "TestCT_Extra")
    print(f"Postgres TestCT_Extra Count: {count2} (EXPECTED BUG: 1, SHOULD BE: 2)")
    
    if count2 == 1:
        print("!!! BUG REPRODUCED: TestCT_Extra lost an incremental row due to shared state. !!!")
    else:
        print("??? Bug NOT reproduced. Check implementation. ???")

    print("\n--- STEP 3: Schema Collision Test ---")
    # New resource name for 'other.TestCT' is 'other_testct'
    run_sync(table_names=["TestCT"], schema="other")
    count_other = get_row_count(dest_engine, "other_testct")
    print(f"Postgres other_testct: {count_other}")

    print("\n--- STEP 4: Delete Nullification Trap Test ---")
    # 1. Insert a row to delete
    with source_engine.connect() as conn:
        conn.execute(sa.text("INSERT INTO TestCT (id, name, value) VALUES (99, 'To Be Deleted', 999)"))
        conn.commit()
    
    # 2. Sync it
    print("Syncing inserted row...")
    run_sync(table_names=["TestCT"])
    
    # 3. Verify it's there
    count_before_delete = get_row_count(dest_engine, "TestCT")
    print(f"Postgres TestCT count before delete: {count_before_delete}")
    
    # 4. Delete the row
    with source_engine.connect() as conn:
        conn.execute(sa.text("DELETE FROM TestCT WHERE id = 99"))
        conn.commit()
    
    # 5. Sync again. If the bug exists, this will crash with CannotCoerceNullException.
    print("Syncing deleted row (Testing Delete Nullification Trap)...")
    try:
        run_sync(table_names=["TestCT"])
        print("SUCCESS: Pipeline handled the delete without crashing!")
    except Exception as e:
        print(f"!!! BUG REPRODUCED: Pipeline crashed on delete !!!\n{e}")
        raise
        
    # 6. Verify it was soft/hard deleted in Postgres
    # In DLT, deletes usually result in a soft delete via `_dlt_deleted` flag, or hard delete if configured.
    # Let's just check the total row count or query the specific ID.
    with dest_engine.connect() as conn:
        # Check if the row exists and its _dlt_deleted status
        query = sa.text("SELECT _dlt_deleted FROM extracted_data.testct WHERE id = 99")
        result = conn.execute(query).scalar()
        if result is True:
            print("SUCCESS: Row 99 was correctly marked as deleted in Postgres!")
        elif result is None:
            # Maybe hard deleted?
            count_after = get_row_count(dest_engine, "TestCT")
            print(f"Row 99 not found. Count after delete: {count_after}")
            print("SUCCESS: Row 99 was hard deleted in Postgres!")
        else:
            print(f"!!! BUG: Row 99 still exists and is not marked as deleted! _dlt_deleted={result}")

    print("\n--- DIAGNOSTICS COMPLETE ---")

if __name__ == "__main__":
    main()
