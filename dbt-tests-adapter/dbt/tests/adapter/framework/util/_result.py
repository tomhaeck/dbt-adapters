from typing import Any, Iterable, List


def get_unique_ids_in_results(results: Any) -> List[str]:
    """
    Args:
        results: the output of `run_dbt`

    Returns: a list of node ids
    """
    return [result.node.unique_id for result in results]


def check_result_nodes_by_name(results: Any, names: Iterable[str]) -> None:
    """
    Assert the nodes in `results` align with the expected `names`

    Args:
        results: the output of `run_dbt`
        names: a set of expected node names
    """
    result_names = [result.node.name for result in results]
    assert set(names) == set(result_names)


def check_result_nodes_by_unique_id(results: Any, unique_ids: Iterable[str]) -> None:
    """
    Assert the nodes in `results` align with the expected `names`

    Args:
        results: the output of `run_dbt`
        unique_ids: a set of expected node ids
    """
    assert set(unique_ids) == set(get_unique_ids_in_results(results))
