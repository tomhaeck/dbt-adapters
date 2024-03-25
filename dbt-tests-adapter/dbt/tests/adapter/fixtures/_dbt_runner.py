from argparse import Namespace
from pathlib import Path
from typing import Any, Dict, List, Optional

from dbt_common.clients.jinja import BaseMacroGenerator, get_template
from dbt_common.dataclass_schema import dbtClassMixin
import pytest

from dbt.adapters.contracts.connection import Connection
from dbt.adapters.protocol import AdapterProtocol, RelationProtocol

import _defaults


class DbtRunner:

    def __init__(self, adapter: AdapterProtocol, relation_factory: Optional[RelationProtocol] = None) -> None:
        self.adapter = adapter
        self.relation_factory = relation_factory or adapter.Relation
        self.schemas = set()

    @property
    def database(self) -> str:
        return self.adapter.connections.credentials.database

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
        with self.connection():
            self.adapter.create_schema(schema)
        self.schemas.add(schema)

    def drop_schema(self, schema: RelationProtocol) -> None:
        with self.connection():
            self.adapter.drop_schema(schema)
        self.schemas.remove(schema)

    def clean_database(self):
        for schema in self.schemas:
            self.drop_schema(schema)

    def execute_macro(
            self,
            name: str,
            macro_code: str,
            context: Dict[str, Any],
            macro_args: List[Any] = None,
            macro_kwargs: Dict[str, Any] = None,
    ) -> Optional[Any]:
        macro_generator = BaseMacroGenerator(context)

        def _get_name(self):
            return name

        def _get_template(self):
            return get_template(macro_code, ctx={})

        macro_generator.get_name = _get_name
        macro_generator.get_template = _get_template
        return macro_generator.call_macro(*macro_args, **macro_kwargs)

    def execute_materialization(
            self,
            name: str,
            materialization_code: str,
            context: Dict[str, Any],
    ) -> Optional[Any]:
        hook_context = self.adapter.pre_model_hook(context)
        try:
            result = self.execute_macro(name, materialization_code, context)
        finally:
            self.adapter.post_model_hook(context, hook_context)

        for relation in result["relations"]:
            self.adapter.cache_added(relation.incorporate(dbt_created=True))

        result = context["load_result"]("main")
        if isinstance(result.response, dbtClassMixin):
            adapter_response = result.response.to_dict(omit_none=True)
        else:
            adapter_response = {}
        return Namespace(
            status="success",
            message=str(result.response),
            adapter_response=adapter_response,
            failures=result.get("failures"),
        )


@pytest.fixture
def dbt_runner(request, adapter: AdapterProtocol, debug_settings: Dict[str, Any]) -> DbtRunner:
    dbt_runner = DbtRunner(adapter)
    yield dbt_runner
    if not debug_settings.get("persist_database_objects", False):
        dbt_runner.clean_database()
