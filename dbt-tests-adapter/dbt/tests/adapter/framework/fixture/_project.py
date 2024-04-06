from argparse import Namespace
import os
from typing import Iterator, Optional
import warnings

import agate
from dbt.adapters.contracts.connection import Connection
from dbt.adapters.factory import get_adapter_by_type
from dbt_common.context import set_invocation_context
from dbt_common.exceptions import CompilationError, DbtDatabaseError
from dbt_common.events.event_manager_client import cleanup_event_logger
from dbt_common.tests import enable_test_caching
import pytest

from dbt.adapters.protocol import AdapterProtocol, RelationProtocol

from dbt.tests.adapter.framework import default
from _logging import setup_event_logger


@pytest.fixture
def initialization() -> None:
    """
    Housekeeping that needs to be done before we start setting up any test fixtures.
    """
    # Create an "invocation context," which dbt application code relies on.
    set_invocation_context(os.environ)

    # Enable caches used between test runs, for better testing performance.
    enable_test_caching()


class TestProjInfo:
    """
    This class is returned from the 'project' fixture, and contains information
    from the pytest fixtures that may be needed in the test functions, including
    a 'run_sql' method.
    """
    def __init__(
        self,
        project_root,
        profiles_dir,
        adapter_type,
        test_dir,
        shared_data_dir,
        test_data_dir,
        test_schema,
        database,
        test_config,
    ):
        self.project_root = project_root
        self.profiles_dir = profiles_dir
        self.adapter_type = adapter_type
        self.test_dir = test_dir
        self.shared_data_dir = shared_data_dir
        self.test_data_dir = test_data_dir
        self.test_schema = test_schema
        self.database = database
        self.test_config = test_config
        self.created_schemas = []

    @property
    def adapter(self) -> AdapterProtocol:
        """
        Each dbt command will create a new one.
        This allows us to avoid patching the providers 'get_adapter' function.

        Returns: the last created "adapter" from the adapter factory
        """
        return get_adapter_by_type(self.adapter_type)

    def get_connection(self, name: Optional[str] = default.CONNECTION_NAME) -> Connection:
        with self.adapter.connection_named(name):
            yield self.adapter.connections.get_thread_connection()

    def run_sql_file(self, sql_path: str, fetch: Optional[str] = None) -> Iterator[agate.Table]:
        with open(sql_path, "r") as f:
            statements = f.read().split(";")
        for statement in statements:
            yield self.run_sql(statement, fetch)

    def run_sql(self, sql: str, fetch: Optional[str] = None) -> Optional[agate.Table]:
        kwargs = {
            "schema": self.adapter.config.credentials.schema,
            "database": self.adapter.quote(self.adapter.config.credentials.database),
        }
        sql = sql.format(**kwargs)
        with self.get_connection() as conn:
            return self.adapter.run_sql_for_tests(sql, fetch, conn)

    def create_test_schema(self, schema_name: Optional[str] = None) -> RelationProtocol:
        """
        Create the unique test schema.
        Used in test setup, so that we're ready for initial sql prior to a run_dbt command.
        """
        if schema_name is None:
            schema_name = self.test_schema
        with self.get_connection():
            schema = self.adapter.Relation.create(database=self.database, schema=schema_name)
            self.adapter.create_schema(schema)
        self.created_schemas.append(schema_name)
        return schema

    def drop_test_schema(self):
        with self.get_connection():
            for schema_name in self.created_schemas:
                relation = self.adapter.Relation.create(database=self.database, schema=schema_name)
                self.adapter.drop_schema(relation)
                self.created_schemas.remove(schema_name)

    # This return a dictionary of table names to 'view' or 'table' values.
    def get_tables_in_schema(self):
        sql = """
        select table_name,
                case when table_type = 'BASE TABLE' then 'table'
                     when table_type = 'VIEW' then 'view'
                     else table_type
                end as materialization
        from information_schema.tables
        where {}
        order by table_name
        """
        sql = sql.format("{} ilike '{}'".format("table_schema", self.test_schema))
        result = self.run_sql(sql, fetch="all")
        return {model_name: materialization for model_name, materialization in result}


@pytest.fixture
def project(
    initialization,
    clean_up_logging,
    project_root,
    profiles_root,
    request,
    unique_schema,
    profiles_yml,
    dbt_project_yml,
    packages_yml,
    dependencies_yml,
    selectors_yml,
    adapter,
    project_files,
    shared_data_dir,
    test_data_dir,
    logs_dir,
    test_config,
) -> TestProjInfo:
    """
    This is the main fixture that is used in all functional tests.
    It pulls in the other fixtures that are necessary to set up a dbt project,
    and saves some of the information in a TestProjInfo class,
    which it returns, so that individual test cases do not have
    to pull in the other fixtures individually to access their information.
    """
    # Logbook warnings are ignored so we don't have to fork logbook to support python 3.10.
    # This _only_ works for tests in `tests/` that use the project fixture.
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")
    log_flags = Namespace(
        LOG_PATH=logs_dir,
        LOG_FORMAT="json",
        LOG_FORMAT_FILE="json",
        USE_COLORS=False,
        USE_COLORS_FILE=False,
        LOG_LEVEL="info",
        LOG_LEVEL_FILE="debug",
        DEBUG=False,
        LOG_CACHE_EVENTS=False,
        QUIET=False,
        LOG_FILE_MAX_BYTES=1000000,
    )
    setup_event_logger(log_flags)

    orig_cwd = os.getcwd()
    os.chdir(project_root)
    # Return whatever is needed later in tests but can only come from fixtures, so we can keep
    # the signatures in the test signature to a minimum.
    project = TestProjInfo(
        project_root=project_root,
        profiles_dir=profiles_root,
        adapter_type=adapter.type(),
        test_dir=request.fspath.dirname,
        shared_data_dir=shared_data_dir,
        test_data_dir=test_data_dir,
        test_schema=unique_schema,
        database=adapter.config.credentials.database,
        test_config=test_config,
    )
    project.drop_test_schema()
    project.create_test_schema()

    yield project

    # deps, debug and clean commands will not have an installed adapter when running and will raise
    # a KeyError here.  Just pass for now.
    # See https://github.com/dbt-labs/dbt-core/issues/5041
    # The debug command also results in an AttributeError since `Profile` doesn't have
    # a `load_dependencies` method.
    # Macros gets executed as part of drop_scheme in core/dbt/adapters/sql/impl.py.  When
    # the macros have errors (which is what we're actually testing for...) they end up
    # throwing CompilationErrors or DatabaseErrors
    try:
        project.drop_test_schema()
    except (KeyError, AttributeError, CompilationError, DbtDatabaseError):
        pass
    os.chdir(orig_cwd)
    cleanup_event_logger()
