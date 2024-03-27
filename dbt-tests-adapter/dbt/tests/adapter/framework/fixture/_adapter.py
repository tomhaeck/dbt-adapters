from dataclasses import dataclass, field
from multiprocessing import get_context
from typing import Any, Dict

from dbt_common.dataclass_schema import dbtClassMixin
import pytest

from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
from dbt.adapters.protocol import AdapterProtocol

from _macro_resolver import MacroResolver, generate_runtime_macro_context


_MP_CONTEXT = get_context("spawn")


DEFAULT_QUERY_COMMENT = """
{%- set comment_dict = {} -%}
{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
    profile_name=target.get('profile_name'),
    target_name=target.get('target_name'),
) -%}
{%- if node is not none -%}
  {%- do comment_dict.update(
    node_id=node.unique_id,
  ) -%}
{% else %}
  {# in the node context, the connection name is the node_id #}
  {%- do comment_dict.update(connection_name=connection_name) -%}
{%- endif -%}
{{ return(tojson(comment_dict)) }}
"""


@dataclass
class QueryComment(dbtClassMixin):
    comment: str = DEFAULT_QUERY_COMMENT
    append: bool = False
    job_label: bool = field(default=False, metadata={"alias": "job-label"})


@dataclass
class AdapterConfig:
    project_name: str
    query_comment: QueryComment
    cli_vars: Dict[str, Any]
    target_path: str
    log_cache_events: bool
    credentials: Credentials
    profile_name: str
    target_name: str
    threads: int

    def to_target_dict(self):
        target = dict(self.credentials.connection_info(with_aliases=True))
        target.update(
            {
                "type": self.credentials.type,
                "threads": self.threads,
                "name": self.target_name,
                "target_name": self.target_name,
                "profile_name": self.profile_name,
            }
        )
        return target


@pytest.fixture
def adapter(
    logs_dir,
    unique_schema,
    project_root,
    profiles_root,
    profiles_yml,
    dbt_project_yml,
    clean_up_logging,
) -> AdapterProtocol:
    """
    This creates an adapter that is used for running test setup,
    such as creating the test schema, and sql commands that are run in tests
    prior to the first dbt command. After a dbt command is run,
    the `project.adapter` property will return the current adapter
    (for this adapter type) from the adapter factory.

    The adapter produced by this fixture will contain the "base" macros
    (not including macros from dependencies). Anything used here must be working
    (dbt_project, profile, project and internal macros), otherwise this will fail.
    So to test errors in those areas, you need to copy the files into the project
    in the tests instead of putting them in the fixtures.
    """
    kwargs = dict(
        profiles_dir=str(profiles_root),
        project_dir=str(project_root),
        target=None,
        profile=None,
        threads=None,
    )
    runtime_config = AdapterConfig(**kwargs)
    register_adapter(runtime_config, _MP_CONTEXT)
    adapter = get_adapter(runtime_config)

    # We only need the base macros, not macros from dependencies,
    # and we don't want to run 'dbt deps' here.
    manifest = MacroResolver.load_macros(
        runtime_config,
        adapter.connections.set_query_header,
    )
    adapter.set_macro_resolver(manifest)
    adapter.set_macro_context_generator(generate_runtime_macro_context)

    yield adapter

    adapter.cleanup_connections()
    reset_adapters()
