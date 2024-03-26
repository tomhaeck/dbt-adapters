from contextlib import contextmanager
import os
from typing import Optional

from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note

from dbt.adapters.protocol import AdapterProtocol


def write_file(contents: str, *paths) -> None:
    """
    Used in tests to write out the string contents of a file
    to a file in the project directory. We need to explicitly
    use encoding="utf-8" because otherwise on Windows
    we'll get codepage 1252 and things might break
    """
    with open(os.path.join(*paths), "w", encoding="utf-8") as fp:
        fp.write(contents)


@contextmanager
def get_connection(adapter: AdapterProtocol, name: Optional[str] = "_test"):
    with adapter.connection_named(name):
        yield adapter.connections.get_thread_connection()


def run_sql_with_adapter(adapter: AdapterProtocol, sql: str, fetch: Optional[str] = None):
    if sql.strip() == "":
        return

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
