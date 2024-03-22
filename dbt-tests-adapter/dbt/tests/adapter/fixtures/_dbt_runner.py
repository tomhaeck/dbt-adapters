from pathlib import Path
from typing import Any, Optional

import pytest

from dbt.adapters.contracts.connection import Connection
from dbt.adapters.protocol import AdapterProtocol, RelationProtocol


class DbtRunner:

    def __init__(self, adapter: AdapterProtocol) -> None:
        self.adapter = adapter
        self.database = self.adapter.connections.database,

    def connection(self, name: Optional[str] = "_test") -> Connection:
        with self.adapter.connection_named(name):
            conn = self.adapter.connections.get_thread_connection()
            yield conn

    def run_sql_file(self, sql_file: Path, fetch: Optional[str] = None) -> None:
        for statement in sql_file.read_text().split(";"):
            self.run_sql(statement, fetch)

    def run_sql(self, sql: str, fetch: Optional[str] = None) -> Optional[Any]:
        if sql.strip() == "":
            return

        sql = sql.format(
            database=self.adapter.connections.database,
            schema=self.adapter.connections.schema,
        )

        with self.connection() as conn:
            return self._run_sql(sql, fetch, conn)

    def _run_sql(self, sql: str, fetch: str, conn: Connection) -> Optional[Any]:
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if hasattr(conn.handle, "commit"):
                conn.handle.commit()
            if fetch == "one":
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException as e:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            print(sql)
            print(e)
            raise
        finally:
            conn.transaction_open = False

    def create_schema(self, schema: RelationProtocol) -> None:
        self.adapter.create_schema(schema)

    def drop_schema(self, schema: RelationProtocol) -> None:
        self.adapter.drop_schema(schema)

    def execute_macro(self, macro_name: str):
        pass


@pytest.fixture
def dbt_runner(request, adapter: AdapterProtocol) -> DbtRunner:
    yield DbtRunner(adapter)
