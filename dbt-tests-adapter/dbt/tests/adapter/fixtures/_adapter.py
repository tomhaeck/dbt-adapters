from dataclasses import dataclass, field
from multiprocessing import get_context
from typing import Any, Dict

import pytest

from dbt.adapters.contracts.connection import Credentials, QueryComment
from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
from dbt.adapters.protocol import AdapterProtocol

import _defaults

_MP_CONTEXT = get_context("spawn")


@dataclass
class _AdapterRequiredConfig:
    credentials: Credentials
    threads: int
    project_name: str = _defaults.PROJECT_NAME
    profile_name: str = _defaults.PROFILE_NAME
    query_comment: QueryComment = field(default_factory=QueryComment)
    cli_vars: Dict[str, Any] = field(default_factory=dict)
    target_path: str = _defaults.TARGET_ROOT
    target_name: str = _defaults.TARGET_PROFILE
    log_cache_events: bool = False

    def to_target_dict(self):
        raise NotImplementedError("to_target_dict not implemented")


@pytest.fixture
def adapter(request, profile) -> AdapterProtocol:
    """
    Provide a fully configured adapter for interacting with the database and testing functionality.

    Args:
        request: the test case using the fixture
        profile: the `profile` fixture

    Returns: an adapter instance
    """
    credentials = Credentials(**profile)
    threads = profile.get("threads", _defaults.THREADS)
    config = _AdapterRequiredConfig(credentials=credentials, threads=threads)
    register_adapter(config, _MP_CONTEXT)
    adapter = get_adapter(config)
    adapter.load_macro_manifest(base_macros_only=True)
    yield adapter
    adapter.cleanup_connections()
    reset_adapters()
