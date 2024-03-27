import os
from pathlib import Path

import pytest


@pytest.fixture
def shared_data_dir(request) -> str:
    return os.path.join(request.config.rootdir, "tests", "data")


@pytest.fixture
def test_data_dir(request) -> str:
    return os.path.join(request.fspath.dirname, "data")


@pytest.fixture
def logs_dir(request, prefix: str) -> str:
    """
    Create a separate logs dir for every test
    """
    dbt_log_dir = os.path.join(request.config.rootdir, "logs", prefix)
    os.environ["DBT_LOG_PATH"] = str(dbt_log_dir)

    yield str(Path(str(dbt_log_dir)))

    del os.environ["DBT_LOG_PATH"]
