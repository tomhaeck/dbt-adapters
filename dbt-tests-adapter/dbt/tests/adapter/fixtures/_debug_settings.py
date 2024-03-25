from typing import Any, Dict

import pytest


@pytest.fixture
def debug_settings(request, debug_settings_overrides) -> Dict[str, Any]:
    settings = {"persist_database_objects": False}
    settings.update(debug_settings_overrides)
    yield settings


@pytest.fixture
def debug_settings_overrides(request) -> Dict[str, Any]:
    yield {}
