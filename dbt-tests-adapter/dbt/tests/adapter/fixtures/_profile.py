import os
from typing import Any, Dict

import pytest

import _defaults


@pytest.fixture
def profile(request, profile_overrides) -> Dict[str, Any]:
    """
    Collects all inputs that would normally be in a `dbt_profile.yml`.

    You generally don't want to override this fixture. Instead, provide updates
    by overriding `profile_overrides` below.

    Args:
        request: the test case using the fixture
        profile_overrides: any overrides specific to an adapter

    Returns: a dictionary representative of `dbt_profile.yml`
    """
    profile = {
        "threads": int(os.getenv("DBT_THREADS", _defaults.THREADS)),
    }
    profile.update(profile_overrides)
    yield profile


@pytest.fixture
def profile_overrides(request) -> Dict[str, Any]:
    yield {}
