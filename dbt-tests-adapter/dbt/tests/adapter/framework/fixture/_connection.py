import pytest

from dbt.adapters.contracts.connection import Connection
from dbt.adapters.protocol import AdapterProtocol


@pytest.fixture
def connection(adapter: AdapterProtocol) -> Connection:
    with adapter.connection_named("_test"):
        yield adapter.connections.get_thread_connection()
