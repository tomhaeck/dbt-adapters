from typing import Any, Dict, List, Optional

import pytest

from dbt.tests.util import run_dbt
from dbt.tests.adapter.fixtures import DbtRunner


_DEFAULT_CHANGE_RELATION_TYPE_MODEL = """
{{ config(materialized=var('materialized')) }}

select '{{ var("materialized") }}' as materialization

{% if var('materialized') == 'incremental' and is_incremental() %}
    where 'abc' != (select max(materialization) from {{ this }})
{% endif %}
"""


class BaseChangeRelationTypeValidator:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_mc_modelface.sql": _DEFAULT_CHANGE_RELATION_TYPE_MODEL}

    def _run_and_check_materialization(
            self,
            dbt_runner: DbtRunner,
            materialization: str,
            args: Optional[List] = None,
            context: Optional[Dict[str, Any]] = None
    ):
        run_args = ["run", "--vars", f"materialized: {materialization}"]
        dbt_runner.execute_materialization(materialization, args, context)

    def test_changing_materialization_changes_relation_type(self, dbt_runner: DbtRunner):
        self._run_and_check_materialization(dbt_runner, "view")
        self._run_and_check_materialization(dbt_runner, "table")
        self._run_and_check_materialization(dbt_runner, "view")
        self._run_and_check_materialization(dbt_runner, "incremental")
        self._run_and_check_materialization("table", extra_args=["--full-refresh"])
