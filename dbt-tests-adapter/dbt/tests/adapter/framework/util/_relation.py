import json
from typing import Dict, List, Optional, Tuple

from dbt.adapters.protocol import AdapterProtocol, RelationProtocol

from _connection import get_connection
from _exception import TestProcessingException


def relation_from_name(adapter: AdapterProtocol, name: str) -> RelationProtocol:
    """
    Get a `BaseRelation` from the identifier.

    Uses the default database and schema if `name` does not contain them.

    Args:
        adapter: the adapter for the database
        name: a "."-delimited name for the relation

    Returns: a `BaseRelation` instance reflecting the provided arguments
    """
    cls = adapter.Relation
    credentials = adapter.config.credentials
    quote_policy = cls.get_default_quote_policy().to_dict()
    include_policy = cls.get_default_include_policy().to_dict()

    relation_parts = name.split(".")
    if len(relation_parts) == 1:
        relation_parts.insert(0, credentials.schema)
    if len(relation_parts) == 2:
        relation_parts.insert(0, credentials.database)

    return cls.create(
        database=relation_parts[0],
        schema=relation_parts[1],
        identifier=relation_parts[2],
        include_policy=include_policy,
        quote_policy=quote_policy,
    )


def check_relations_equal(
        adapter: AdapterProtocol,
        relation_names: List[str],
        compare_snapshot_cols: Optional[bool] = False
):
    """
    Wraps check_relations_equal_with_relations by creating relations
    from the list of names passed in.

    Args:
        adapter: the adapter for the database
        relation_names: a list of relation names
        compare_snapshot_cols: allows the comparison of "dbt_*" columns
    """
    if len(relation_names) < 2:
        raise TestProcessingException("Not enough relations to compare")
    relations = [relation_from_name(adapter, name) for name in relation_names]
    return check_relations_equal_with_relations(
        adapter, relations, compare_snapshot_cols=compare_snapshot_cols
    )


def check_relations_equal_with_relations(
        adapter: AdapterProtocol,
        relations: List[RelationProtocol],
        compare_snapshot_cols: bool = False
):
    """
    Asserts that the provided relations are all equal.

    Args:
        adapter: the adapter for the database
        relations: a list of `BaseRelation` instances
        compare_snapshot_cols: allows the comparison of "dbt_*" columns
    """
    with get_connection(adapter):
        basis, compares = relations[0], relations[1:]
        # Skip columns starting with "dbt_", we don't want to compare those since they are time sensitive
        # (unless comparing "dbt_" snapshot columns is explicitly enabled)
        column_names = [
            c.name
            for c in adapter.get_columns_in_relation(basis)
            if not c.name.lower().startswith("dbt_") or compare_snapshot_cols
        ]

        for relation in compares:
            sql = adapter.get_rows_different_sql(basis, relation, column_names=column_names)
            _, tbl = adapter.execute(sql, fetch=True)
            num_rows = len(tbl)
            assert (
                num_rows == 1
            ), f"Invalid sql query from get_rows_different_sql: incorrect number of rows ({num_rows})"
            num_cols = len(tbl[0])
            assert (
                num_cols == 2
            ), f"Invalid sql query from get_rows_different_sql: incorrect number of cols ({num_cols})"
            row_count_difference = tbl[0][0]
            assert (
                row_count_difference == 0
            ), f"Got {row_count_difference} difference in row count betwen {basis} and {relation}"
            rows_mismatched = tbl[0][1]
            assert (
                rows_mismatched == 0
            ), f"Got {rows_mismatched} different rows between {basis} and {relation}"


def check_relation_types(
        adapter: AdapterProtocol, relation_to_type: Dict[str, str]
) -> None:
    """
    Asserts that models have the expected relation type.

    Args:
        adapter: the adapter for the database
        relation_to_type: a mapping of relation names to expected relation types, e.g.:
            {
                "base": "table",
                "other": "view",
            }
    """
    expected_relation_values = {}
    found_relations = []
    schemas = set()

    for key, value in relation_to_type.items():
        relation = relation_from_name(adapter, key)
        expected_relation_values[relation] = value
        schemas.add(relation.without_identifier())

        with get_connection(adapter):
            for schema in schemas:
                found_relations.extend(adapter.list_relations_without_caching(schema))

    for key, value in relation_to_type.items():
        for relation in found_relations:
            if relation.identifier == key:
                assert relation.type == value, (
                    f"Got an unexpected relation type of {relation.type} "
                    f"for relation {key}, expected {value}"
                )


def check_relation_has_expected_schema(
        adapter: AdapterProtocol, relation_name: str, expected_schema: Dict[str, str]
) -> None:
    """
    Asserts that a relation has an expected column schema.

    Args:
        adapter: the adapter for the database
        relation_name: name of the relation
        expected_schema: should look like {"column_name": "expected datatype"}
    """
    relation = relation_from_name(adapter, relation_name)
    with get_connection(adapter):
        actual_columns = {c.name: c.data_type for c in adapter.get_columns_in_relation(relation)}
    assert (
        actual_columns == expected_schema
    ), f"Actual schema did not match expected, actual: {json.dumps(actual_columns)}"


def get_relation_columns(adapter: AdapterProtocol, name: str) -> List[Tuple[str, str, str]]:
    relation = relation_from_name(adapter, name)
    with get_connection(adapter):
        columns = adapter.get_columns_in_relation(relation)
        return sorted(((c.name, c.dtype, c.char_size) for c in columns), key=lambda x: x[0])


def check_table_does_not_exist(adapter: AdapterProtocol, name: str) -> None:
    columns = get_relation_columns(adapter, name)
    assert len(columns) == 0


def check_table_does_exist(adapter: AdapterProtocol, name: str) -> None:
    columns = get_relation_columns(adapter, name)
    assert len(columns) > 0
