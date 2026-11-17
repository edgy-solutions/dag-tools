import os
from collections.abc import Mapping
from typing import Any, Dict, Tuple

import httpx
import oracledb
import restate

from . import db_retry

service = restate.Service(name="GenericApiSyncService")


def _validated(payload: Any) -> Tuple[str, str, str, Any, Dict[str, Any]]:
    """Refuse a payload this handler cannot act on. Runs INSIDE a step.

    Called as the first statement of each step rather than once before
    them, and that is the whole correction. Read before ``ctx.run``, a
    refusal leaves every bound behind: the *invocation* fails, and
    Restate retries an invocation that failed on a non-terminal error
    with no ``max_attempts`` anywhere -- so a payload with a field
    missing was retried with backoff for the life of the worker, and the
    journal never said what was wrong. Nothing was damaged, which is why
    it went unnoticed; the invocation simply never ended.

    Each step calls it for itself, so neither depends on the other
    having run: the two steps are separate journal entries, and a replay
    can execute one without executing the other.

    It names the fields it refuses and **no value** from any of them. A
    payload names a table and a column, and this message travels into
    the ``TerminalError``, from there into the journal, and from there
    back to the caller.

    Every problem is collected rather than raised on the first one,
    because whoever is fixing the producer wants the list.
    """
    if not isinstance(payload, Mapping):
        raise db_retry.MalformedPayload(
            "The payload cannot be acted on: it is not a mapping of fields."
        )

    problems = []

    def _text(name: str) -> str:
        value = payload.get(name)
        if value is None or (isinstance(value, str) and not value.strip()):
            problems.append(f"{name} is required and was not given")
            return ""
        if not isinstance(value, str):
            problems.append(f"{name} must be text")
            return ""
        return value

    api_path = _text("api_path")
    source_table = _text("source_table")
    pk_column = _text("pk_column")

    # The path is joined to a base URL whose trailing slash has been
    # stripped, so a path that does not start with one concatenates into
    # a different host rather than a different route. That failure looks
    # transient -- a name that does not resolve -- and this step is
    # deliberately unbounded, so it would be retried for as long as the
    # worker lives.
    if api_path and not api_path.startswith("/"):
        problems.append(
            "api_path must begin with '/' so it joins the base URL as a path "
            "rather than extending its host"
        )

    pk_value = payload.get("pk_value")
    if pk_value is None or (isinstance(pk_value, str) and not pk_value.strip()):
        problems.append("pk_value is required and was not given")
    elif isinstance(pk_value, bool) or not isinstance(pk_value, (str, int)):
        # A list binds as one parameter and matches nothing; a boolean is
        # not an identifier. Both are named so the producer is told which
        # it sent.
        problems.append(
            "pk_value must be a single identifier; a list, a mapping and a "
            "boolean are not one"
        )

    row_data = payload.get("row_data", {})
    if row_data is None:
        row_data = {}
    if isinstance(row_data, str):
        # The dangerous one, and the reason it is named apart from the
        # other shapes: a string is valid JSON, so the request would be
        # built, sent and quite possibly accepted -- a row posted to the
        # API as a bare string. That is the failure here that reads as a
        # success.
        problems.append(
            "row_data must be a mapping of columns; a string is not one, and "
            "posting it would send a bare JSON string the API may accept"
        )
    elif not isinstance(row_data, Mapping):
        problems.append("row_data must be a mapping of columns")

    if problems:
        raise db_retry.MalformedPayload(
            "The payload cannot be acted on: " + "; ".join(problems) + "."
        )

    return api_path, source_table, pk_column, pk_value, dict(row_data)


@service.handler()
async def process_record(ctx: restate.Context, payload: dict):
    """Generic Restate service for sync data outward to REST APIs securely per row."""

    # Step 1: Execute the API Call natively (durably wrapped against transient failure)
    def _execute_api_post():
        api_path, _, _, _, row_data = _validated(payload)

        base_url = os.environ.get("API_BASE_URL", "").rstrip("/")
        api_key = os.environ.get("API_KEY")

        if not base_url:
            # A ValueError subclass, and terminal: the container's
            # environment is a snapshot taken at process start, so a
            # variable absent now stays absent for the life of the pod.
            # Raised as a plain ValueError, as it was, this step retried
            # it forever -- it is unbounded on purpose.
            raise db_retry.MissingCredentials(
                "Missing API_BASE_URL in environment variables."
            )

        headers = {"Content-Type": "application/json"}
        if api_key:
             headers["Authorization"] = f"Bearer {api_key}"

        # Optional: You could inject a pure dict payload mapper right here against `row_data` if needed
        # row_data = my_mapper(row_data)

        # Using sync requests since this runs exclusively in an isolated sync context node execution block
        import requests
        response = requests.post(f"{base_url}{api_path}", json=row_data, headers=headers)
        response.raise_for_status()

        # Typically systems return an upstream correlation ID; safely parse it generic to standard schemas
        res_json = response.json()
        return str(res_json.get("id") or res_json.get("external_id") or "ACK_RECEIVED")

    # Our own refusals are terminal here; nothing else is touched, and no
    # retry bound is added. This step does not connect to a database, so
    # it takes `terminal_on_refusal` and not `terminal_on_damage`: the
    # driver codes the latter reads are matched as standalone numbers
    # anywhere in the exception text, and an HTTP failure quotes the URL
    # it failed on -- so a `404 ... /orders/18456` would be read as a
    # login failure and the call refused instead of retried. The retries
    # this step does want are the ones `ctx.run` exists for, and they
    # stay unbounded.
    api_id = await ctx.run(
        "sap_api_post",
        db_retry.terminal_on_refusal(_execute_api_post),
    )

    # Step 2: Write back natively to SQL Server establishing the two-phase commit ack durability
    def _execute_sqlserver_ack():
        _, source_table, pk_column, pk_value, _ = _validated(payload)

        import pyodbc

        dsn = os.environ.get("SQLSERVER_DSN")
        if not dsn:
            # A ValueError subclass, and terminal: the container's
            # environment is a snapshot taken at process start, so a
            # variable absent now stays absent for the life of the pod.
            raise db_retry.MissingCredentials(
                "Missing SQLSERVER_DSN connection credentials in environment variables."
            )

        with pyodbc.connect(dsn) as connection:
            with connection.cursor() as cursor:
                sql = f"UPDATE {source_table} SET sync_status = 'COMPLETED', external_id = ? WHERE {pk_column} = ?"
                cursor.execute(sql, api_id, pk_value)
            connection.commit()

        # Returned so the correlation key below is the one the row was
        # actually written against, rather than a second reading of the
        # payload outside any step.
        return pk_value

    # Bounded, and terminal on the first failure for the errors where
    # retrying is what does the damage. See
    # dag_tools.restate_handlers.db_retry.
    acked_pk_value = await ctx.run(
        "update_sqlserver",
        db_retry.terminal_on_damage(_execute_sqlserver_ack),
        **db_retry.run_options(),
    )

    # Step 3: Embed durability correlation back natively to Restate KV
    ctx.set(f"last_sync_{acked_pk_value}", api_id)
