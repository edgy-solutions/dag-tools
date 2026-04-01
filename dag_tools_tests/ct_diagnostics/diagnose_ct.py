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

    print("\n--- DIAGNOSTICS COMPLETE ---")

if __name__ == "__main__":
    main()
