from dbt_common.events.event_manager_client import cleanup_event_logger
import pytest


@pytest.fixture
def clean_up_logging() -> None:
    """
    This fixture ensures that the logging infrastructure does not accidentally
    reuse streams configured on previous test runs, which might now be closed.
    It should be run before (and so included as a parameter by) any other fixture
    which runs functions that might fire events.
    """
    cleanup_event_logger()
