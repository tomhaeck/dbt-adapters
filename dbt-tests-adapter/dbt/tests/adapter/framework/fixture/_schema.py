from datetime import UTC, datetime
from pathlib import Path
import random

import pytest


@pytest.fixture
def unique_schema(request, prefix: str) -> str:
    file_name = Path(request.module.__name__).stem
    return f"{prefix}_{file_name}"


@pytest.fixture
def prefix() -> str:
    randint = random.randint(0, 9999)
    runtime_timedelta = datetime.now(tz=UTC) - datetime(1970, 1, 1, 0, 0, 0)
    runtime = (int(runtime_timedelta.total_seconds() * 1e6)) + runtime_timedelta.microseconds
    return f"test{runtime}{randint:04}"
