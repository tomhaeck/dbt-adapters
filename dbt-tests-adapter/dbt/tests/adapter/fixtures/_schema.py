from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import random
from typing import Any, Dict

import pytest

from dbt.adapters.protocol import RelationProtocol

from _dbt_runner import DbtRunner


@dataclass
class _SchemaConfig:  # implements RelationConfig protocol
    database: str
    schema: str
    identifier = ""
    name = ""
    quoting_dict: Dict[str, bool]
    compiled_code = ""
    config = None


@dataclass
class _HasQuoting:  # implements HasQuoting protocol
    quoting: Dict[str, bool]


@pytest.fixture
def schema(
        request,
        unique_schema_name: str,
        dbt_runner: DbtRunner,
        debug_settings: Dict[str, Any]
) -> RelationProtocol:
    """
    Produce a unique schema and drop it after it's out of scope.

    You generally don't want to override this fixture. Instead, provide updates
    by overriding `unique_schema_name` below, or other corresponding fixtures.

    Args:
        request: the test case using the fixture
        unique_schema_name: a name to use as an override, otherwise the test case and a timestamp will be used
        dbt_runner: the test dbt runner fixture
        debug_settings: these are settings that allow configuration of fixtures without needing to overwrite them

    Returns: a unique schema that only exist while in scope, unless configured otherwise via `debug_settings_overrides`
    """
    quote_policy = dbt_runner.relation_factory.get_default_quote_policy()
    schema_config = _SchemaConfig(
        database=dbt_runner.database,
        schema=unique_schema_name,
        quoting_dict=dict(**quote_policy),
    )
    schema = dbt_runner.relation_factory.create_from(
        relation_config=schema_config,
        quoting=_HasQuoting(dict(**quote_policy)),
    )
    dbt_runner.create_schema(schema)
    yield schema
    if not debug_settings.get("persist_database_objects", False):
        dbt_runner.drop_schema(schema)


@pytest.fixture
def unique_schema_name(request) -> str:
    """
    Provide a default test schema.

    Args:
        request: the test case using the fixture, used to get the test module as a stub

    Returns: a unique test schema name
    """
    module_name = Path(request.module.__name__).stem
    stub = str(random.randint(0, 9999)).zfill(4)
    test_start_timestamp = datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0)
    test_start_microseconds = int(test_start_timestamp.total_seconds() * 1e6) + test_start_timestamp.microseconds
    return "_".join([module_name, test_start_microseconds, stub])
