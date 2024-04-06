from typing import Any, Dict, Optional, Union

from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note

from dbt.adapters.protocol import AdapterProtocol

from dbt.tests.adapter.framework import default
from _exception import TestProcessingException
from _relation import relation_from_name


def get_connection(adapter: AdapterProtocol, name: Optional[str] = default.CONNECTION_NAME):
    with adapter.connection_named(name):
        yield adapter.connections.get_thread_connection()


def run_sql_with_adapter(
        adapter: AdapterProtocol, sql: str, fetch: Optional[str] = None
) -> Optional[Any]:
    if sql.strip() == "":
        return None

    # substitute schema and database in sql
    kwargs = {
        "schema": adapter.config.credentials.schema,
        "database": adapter.quote(adapter.config.credentials.database),
    }
    sql = sql.format(**kwargs)

    msg = f'test connection "__test" executing: {sql}'
    fire_event(Note(msg=msg), level=EventLevel.DEBUG)
    with get_connection(adapter) as conn:
        return adapter.run_sql_for_tests(sql, fetch, conn)


def update_rows(
        adapter: AdapterProtocol, update_rows_config: Dict[str, Union[str, Dict[str, str]]]
) -> None:
    """
    update_rows_config:
        {
            "name": "base",
            "dst_col": "some_date",
            "clause": {
                "type": "add_timestamp",
                "src_col": "some_date",
            },
            "where" "id > 10",
        }
    """
    for key in ["name", "dst_col", "clause"]:
        if key not in update_rows_config:
            raise TestProcessingException(f"Invalid update_rows: no {key}")

    clause = update_rows_config["clause"]
    clause = generate_update_clause(adapter, clause)

    where = None
    if "where" in update_rows_config:
        where = update_rows_config["where"]

    name = update_rows_config["name"]
    dst_col = update_rows_config["dst_col"]
    relation = relation_from_name(adapter, name)

    with get_connection(adapter):
        sql = adapter.update_column_sql(
            dst_name=str(relation),
            dst_column=dst_col,
            clause=clause,
            where_clause=where,
        )
        adapter.execute(sql, auto_begin=True)
        adapter.commit_if_has_connection()


def generate_update_clause(adapter: AdapterProtocol, clause: Dict[str, str]) -> str:
    """
    clause:
        {
            "type": "add_timestamp",
            "src_col": "some_date",
        }
    """
    if "type" not in clause or clause["type"] not in ["add_timestamp", "add_string"]:
        raise TestProcessingException("invalid update_rows clause: type missing or incorrect")
    clause_type = clause["type"]

    if clause_type == "add_timestamp":
        if "src_col" not in clause:
            raise TestProcessingException("Invalid update_rows clause: no src_col")
        add_to = clause["src_col"]
        kwargs = {k: v for k, v in clause.items() if k in ("interval", "number")}
        with get_connection(adapter):
            return adapter.timestamp_add_sql(add_to=add_to, **kwargs)

    elif clause_type == "add_string":
        for key in ["src_col", "value"]:
            if key not in clause:
                raise TestProcessingException(f"Invalid update_rows clause: no {key}")
        src_col = clause["src_col"]
        value = clause["value"]
        location = clause.get("location", "append")
        with get_connection(adapter):
            return adapter.string_add_sql(src_col, value, location)

    return ""
