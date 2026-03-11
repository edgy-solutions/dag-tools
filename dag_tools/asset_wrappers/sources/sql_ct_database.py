from typing import Iterable, List, Optional, Union, Any, Dict, Callable
import dlt
import sqlalchemy as sa
from sqlalchemy import inspect
from dlt.extract.source import DltResource

# 1. Import the OFFICIAL dlt source for fallback
from dlt.sources.sql_database import sql_database as official_sql_database

# 2. Import the OFFICIAL dlt helper for engine creation
# This is critical. It knows how to unpack your config dictionary 
# (specifically the 'query' params with the ODBC driver) correctly.
try:
    from dlt.sources.sql_database.helpers import engine_from_credentials
except ImportError:
    # Fallback for different dlt versions: try common lib
    try:
        from dlt.common.libs.sql_alchemy import engine_from_credentials
    except ImportError:
        # Last resort: A naive implementation that respects .to_url() if available
        def engine_from_credentials(credentials: Any) -> sa.engine.Engine:
            if isinstance(credentials, sa.engine.Engine):
                return credentials
            if hasattr(credentials, "to_native_representation"):
                # specific to dlt credentials objects
                return sa.create_engine(credentials.to_native_representation()[0])
            return sa.create_engine(str(credentials))

# INTERNAL CT IMPLEMENTATION
# We define this separately so we can decorate it properly as a source.
# This handles the "Happy Path" where CT is enabled.
@dlt.source(name="sql_database")
def _internal_ct_source(
    credentials=None,
    schema=None,
    table_names=None,
    chunk_size=50000,
    write_disposition="merge",
    engine=None, # Passed explicitly
    defer_table_reflect=False,
    **kwargs
) -> Iterable[DltResource]:
    
    print("--- INFO: MSSQL Change Tracking DETECTED. Using CT Strategy. ---")
    
    # 1. Pre-fetch the list of tables that actually have CT enabled.
    # Just because the DB has it, doesn't mean every table does.
    ct_tables_set = set()
    try:
        with engine.connect() as conn:
            # Get all (schema, table) pairs that are tracked
            ct_check_sql = """
                SELECT s.name, t.name 
                FROM sys.change_tracking_tables ct
                JOIN sys.tables t ON ct.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
            """
            rows = conn.execute(sa.text(ct_check_sql)).fetchall()
            for r in rows:
                # Normalize to lowercase for robust comparison
                ct_tables_set.add((r[0].lower(), r[1].lower()))
    except Exception as e:
        print(f"WARNING: Could not verify table-level Change Tracking status: {e}")
        # Proceeding with empty set will cause fallback to full load, which is safe.

    meta = sa.MetaData()
    
    # Explicitly check for None or empty list to avoid ambiguity
    if table_names is None or len(table_names) == 0:
        meta.reflect(bind=engine, schema=schema)
        tables_to_process = list(meta.tables.values())
    else:
        # Tables have already been filtered of views by the dispatcher
        meta.reflect(bind=engine, schema=schema, only=table_names)
        tables_to_process = []
        for name in table_names:
            found_t = None
            # 1. Try Exact Match
            for t in meta.tables.values():
                if t.name == name:
                    found_t = t
                    break
            # 2. Try Case-Insensitive Match
            if found_t is None:
                for t in meta.tables.values():
                    if t.name.lower() == name.lower():
                        found_t = t
                        break
            
            # FIX: Explicit 'is not None' check.
            # SQLAlchemy objects can raise "Boolean value of this clause is not defined"
            # if evaluated in a boolean context (e.g. 'if found_t:').
            if found_t is not None:
                tables_to_process.append(found_t)
            else:
                print(f"WARNING: Table '{name}' was not found in reflected metadata.")

    for table_obj in tables_to_process:
        # Prepare keys for lookup
        t_schema = (table_obj.schema or "dbo").lower()
        t_name = table_obj.name.lower()
        
        # CHECK 1: Does the table have CT enabled?
        if (t_schema, t_name) not in ct_tables_set:
            print(f"WARNING: Table '{table_obj.name}' does NOT have Change Tracking enabled. Falling back to FULL LOAD (replace).")
            yield dlt.resource(
                _make_full_load_generator(engine, table_obj, chunk_size),
                name=table_obj.name,
                write_disposition="replace" 
            )
            continue

        # CHECK 2: Does the table have a Primary Key? (Required for CT extraction)
        primary_keys = [c.name for c in table_obj.primary_key.columns]
        
        # Fallback for tables without PK
        if not primary_keys:
            print(f"WARNING: Table '{table_obj.name}' has no Primary Key. Falling back to FULL LOAD (replace).")
            yield dlt.resource(
                _make_full_load_generator(engine, table_obj, chunk_size),
                name=table_obj.name,
                write_disposition="replace" 
            )
            continue

        # Happy Path: CT
        print(f"--- [DEBUG] CT Resource '{table_obj.name}' configured with write_disposition='{write_disposition}'")
        yield dlt.resource(
            _make_ct_generator(engine, table_obj, chunk_size),
            name=table_obj.name,
            primary_key=primary_keys,
            write_disposition=write_disposition,
        )


# MAIN DISPATCH FUNCTION
# NOTE: We do NOT decorate this with @dlt.source. 
# It is a plain function that returns a Source object.
# This avoids the "yield from" generator issues and lets us return the official source directly.
def sql_ct_database(
    credentials: Union[str, sa.engine.Engine] = dlt.secrets.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[Any] = None,
    table_names: Optional[List[str]] = dlt.config.value,
    chunk_size: int = 50000,
    write_disposition: str = "merge",
    backend: str = "sqlalchemy",
    detect_precision_hints: Optional[bool] = False,
    reflection_level: Optional[str] = "full",
    defer_table_reflect: Optional[bool] = None,
    table_adapter_callback: Optional[Any] = None,
    backend_kwargs: Dict[str, Any] = None,
    include_views: bool = False,
    type_adapter_callback: Optional[Any] = None,
    query_adapter_callback: Optional[Any] = None,
    resolve_foreign_keys: bool = False,
    engine_adapter_callback: Optional[Callable[[Any], Any]] = None,
    **kwargs: Any
):
    """
    A Smart Proxy for MSSQL. Matches the exact signature of dlt.sources.sql_database.
    Returns either the Official SQL Source OR the Custom CT Source, handling Views cleanly.
    """
    
    # 1. Check for Change Tracking Capability (Silent Fail)
    ct_enabled = False
    engine = None
    try:
        engine = engine_from_credentials(credentials)
        with engine.connect() as conn:
            check_query = "SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID()"
            result = conn.execute(sa.text(check_query)).scalar()
            if result == 1:
                ct_enabled = True
    except Exception as e:
        print(f"WARNING: Change Tracking check failed (assuming disabled). Error: {e}")
        ct_enabled = False

    # 2. AUTO-DISCERN: Separate Tables from Views
    actual_tables = []
    actual_views = []
    
    if table_names and engine:
        try:
            inspector = inspect(engine)
            db_views = inspector.get_view_names(schema=schema)
            for t in table_names:
                if t in db_views:
                    actual_views.append(t)
                else:
                    actual_tables.append(t)
            print(f"--- [AUTO-DISCERN] Found {len(actual_tables)} Tables and {len(actual_views)} Views ---")
        except Exception as e:
            print(f"--- [WARNING] Could not auto-discern views: {e}. Treating all objects as tables. ---")
            actual_tables = table_names
    else:
        actual_tables = table_names

    # Clean up kwargs for official source
    clean_kwargs = kwargs.copy()
    if 'write_disposition' in clean_kwargs:
        del clean_kwargs['write_disposition']

    # 3. Branching & Source Generation
    final_sources = []
    
    # If CT is disabled globally, route EVERYTHING to the official source
    if not ct_enabled:
        print("--- INFO: Reverting to Standard sql_database for all objects ---")
        source = official_sql_database(
            credentials=credentials,
            schema=schema,
            metadata=metadata,
            table_names=table_names, # Pass the original list
            chunk_size=chunk_size,
            backend=backend,
            detect_precision_hints=detect_precision_hints,
            reflection_level=reflection_level,
            defer_table_reflect=defer_table_reflect,
            table_adapter_callback=table_adapter_callback,
            backend_kwargs=backend_kwargs,
            include_views=True if actual_views else include_views, # FORCE views if found
            type_adapter_callback=type_adapter_callback,
            query_adapter_callback=query_adapter_callback,
            resolve_foreign_keys=resolve_foreign_keys,
            engine_adapter_callback=engine_adapter_callback,
            **clean_kwargs
        )
        
        # LOGGING: Inspect the standard source resources to see what they defaulted to
        try:
            for resource_name, resource in source.resources.items():
                wd = resource.write_disposition
                print(f"--- [DEBUG] Fallback Resource '{resource_name}' configured with write_disposition='{wd}'")
        except Exception:
            pass # Don't crash if resources aren't iterable yet
            
        return source

    # CT is enabled. Route standard tables to our custom source
    if actual_tables:
        ct_source = _internal_ct_source(
            credentials=credentials,
            schema=schema,
            table_names=actual_tables, # Only tables
            chunk_size=chunk_size,
            write_disposition=write_disposition,
            engine=engine,
            defer_table_reflect=defer_table_reflect,
            **kwargs
        )
        final_sources.append(ct_source)

    # Route views to the official dlt source (as views cannot have CT)
    if actual_views:
        print(f"--- INFO: Routing {len(actual_views)} Views to Standard sql_database ---")
        view_source = official_sql_database(
            credentials=credentials,
            schema=schema,
            metadata=metadata,
            table_names=actual_views, # Only views
            chunk_size=chunk_size,
            backend=backend,
            detect_precision_hints=detect_precision_hints,
            reflection_level=reflection_level,
            defer_table_reflect=defer_table_reflect,
            table_adapter_callback=table_adapter_callback,
            backend_kwargs=backend_kwargs,
            include_views=True, # CRITICAL: Forces SQLAlchemy to find the views
            type_adapter_callback=type_adapter_callback,
            query_adapter_callback=query_adapter_callback,
            resolve_foreign_keys=resolve_foreign_keys,
            engine_adapter_callback=engine_adapter_callback,
            **clean_kwargs
        )
        final_sources.append(view_source)

    # Combine sources if we processed both
    if len(final_sources) == 2:
        return final_sources[0] + final_sources[1]
    elif len(final_sources) == 1:
        return final_sources[0]
    
    # Fallback return just in case
    return None

# --- GENERATORS (Helper Functions) ---

def _make_full_load_generator(engine: sa.engine.Engine, table_obj: sa.Table, chunk_size: int):
    def generator():
        print(f"[{table_obj.name}] Performing Full Load (No PK Fallback).")
        with engine.connect() as conn:
            query = table_obj.select()
            proxy = conn.execute(query)
            while True:
                batch = proxy.fetchmany(chunk_size)
                if not batch: break
                yield [dict(row._mapping) for row in batch]
    return generator

def _make_ct_generator(engine: sa.engine.Engine, table_obj: sa.Table, chunk_size: int):
    def generator():
        current_state = dlt.current.state()
        last_version = current_state.get("last_sync_version", None)
        table_name = table_obj.name
        schema_name = table_obj.schema or "dbo"
        full_table_name = f"[{schema_name}].[{table_name}]"
        
        print(f"--- [DEBUG] Starting Extraction for {full_table_name}")
        
        with engine.connect() as conn:
            try:
                curr_ver_query = "SELECT CHANGE_TRACKING_CURRENT_VERSION()"
                current_db_version = conn.execute(sa.text(curr_ver_query)).scalar()
            except Exception:
                 current_db_version = None
            
            if current_db_version is None:
                raise Exception(f"Change Tracking disabled during run for {full_table_name}")

            print(f"--- [DEBUG] {full_table_name}: State Version={last_version}, DB Version={current_db_version}")

            if last_version is not None:
                min_ver_query = f"SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('{full_table_name}'))"
                try:
                    min_valid_version = conn.execute(sa.text(min_ver_query)).scalar()
                except Exception:
                    min_valid_version = None
                    
                if min_valid_version is not None and last_version < min_valid_version:
                    print(f"[{table_name}] State version {last_version} is older than min valid {min_valid_version}. Forcing full load.")
                    last_version = None

            if last_version is None:
                print(f"[{table_name}] No state. Performing Full Load.")
                
                # FIX: Inject default Change Tracking columns for schema consistency during initial load.
                # 'SYS_CHANGE_VERSION' = 0 (Base state)
                # 'SYS_CHANGE_OPERATION' = 'I' (Insert)
                query = sa.select(
                    table_obj,
                    sa.literal(0).label("SYS_CHANGE_VERSION"),
                    sa.literal("I").label("SYS_CHANGE_OPERATION"),
                    sa.literal(False).label("_dlt_deleted")
                )
                
                proxy = conn.execute(query)
                while True:
                    batch = proxy.fetchmany(chunk_size)
                    if not batch: break
                    yield [dict(row._mapping) for row in batch]
                current_state["last_sync_version"] = current_db_version
            else:
                if last_version >= current_db_version:
                    print(f"[{table_name}] No changes detected (State >= DB). Skipping.")
                    return

                print(f"[{table_name}] Changes detected. Fetching incremental from {last_version} to {current_db_version}")
                
                # Dynamically construct SELECT to ensure PKs come from the CHANGETABLE (ct)
                # so they are never NULL, even on Deletes (when t.* is NULL).
                primary_keys = [pk.name for pk in table_obj.primary_key.columns]
                t_cols = [c.name for c in table_obj.columns if c.name not in primary_keys]
                
                select_pks = [f"ct.[{pk}]" for pk in primary_keys]
                select_t = [f"t.[{c}]" for c in t_cols]
                select_clause = ", ".join(select_pks + select_t)
                
                pk_conditions = [f"t.[{pk}] = ct.[{pk}]" for pk in primary_keys]
                join_condition = " AND ".join(pk_conditions)

                query_str = f"""
                    SELECT {select_clause}, ct.SYS_CHANGE_OPERATION, ct.SYS_CHANGE_VERSION
                    FROM CHANGETABLE(CHANGES {full_table_name}, {last_version}) AS ct
                    LEFT JOIN {full_table_name} AS t ON {join_condition}
                """
                
                proxy = conn.execute(sa.text(query_str))
                count = 0
                while True:
                    batch = proxy.fetchmany(chunk_size)
                    if not batch: break
                    count += len(batch)
                    
                    processed_batch = []
                    for row in batch:
                        row_dict = dict(row._mapping)
                        if row_dict.get("SYS_CHANGE_OPERATION") == "D":
                            row_dict["_dlt_deleted"] = True
                        else:
                            row_dict["_dlt_deleted"] = False
                        processed_batch.append(row_dict)
                        
                    yield processed_batch
                
                print(f"[{table_name}] Incremental extraction complete. Yielded {count} rows.")
                current_state["last_sync_version"] = current_db_version

    return generator
