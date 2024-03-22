import pytest

from dbt.adapters.protocol import AdapterProtocol, RelationProtocol


@pytest.fixture
def relation_factory(request, adapter: AdapterProtocol) -> RelationProtocol:
    """
    Provides a way for creating and interacting with BaseRelation instances.

    Args:
        request: the test case using the fixture
        adapter: the adapter to use when interacting with the database

    Returns: a relation factory
    """
    relation_factory = adapter.Relation
    yield relation_factory
