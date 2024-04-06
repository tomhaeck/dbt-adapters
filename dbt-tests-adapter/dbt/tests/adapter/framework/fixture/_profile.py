import os
from typing import Any, Dict
import yaml

# TODO: refactor to avoid using private attributes
from _pytest.compat import LEGACY_PATH
import pytest


DictConfig = Dict[str, Any]


@pytest.fixture
def profiles_root(tmpdir_factory) -> LEGACY_PATH:
    return tmpdir_factory.mktemp("profile")


@pytest.fixture
def profiles_yml(profiles_root: LEGACY_PATH, dbt_profile_data: DictConfig) -> DictConfig:
    os.environ["DBT_PROFILES_DIR"] = str(profiles_root)
    with open(os.path.join(profiles_root, "profiles.yml"), "w", encoding="utf-8") as fp:
        fp.write(yaml.safe_dump(dbt_profile_data))
    yield dbt_profile_data
    del os.environ["DBT_PROFILES_DIR"]


@pytest.fixture
def profile_user(dbt_profile_target) -> str:
    return dbt_profile_target["user"]


@pytest.fixture
def dbt_profile_data(
        unique_schema: str,
        dbt_profile_target: DictConfig,
        profiles_config_update: DictConfig,
) -> DictConfig:
    """
    The profile dictionary, used to write out `profiles.yml`.

    It will pull in updates from two separate sources, the 'profile_target' and 'profiles_config_update'.
    The second one is useful when using alternative targets, etc.
    """
    target = dbt_profile_target
    target["schema"] = unique_schema
    profile = {
        "test": {
            "outputs": {
                "default": target,
            },
            "target": "default",
        },
    }
    profile.update(profiles_config_update)
    return profile


@pytest.fixture
def profiles_config_update() -> DictConfig:
    """
    This fixture can be overridden in a project.
    The data provided in this fixture will be merged into the default project dictionary via a python 'update'.
    """
    return {}


@pytest.fixture
def dbt_profile_target() -> DictConfig:
    """
    Contains the default profile information for setting up different profiles.

    *Note:*
        Because we load the profile to create the adapter,
        this fixture can't be used to test vars and env_vars or errors.
        The profile must be written out after the test starts.
    """
    # TODO: move this into dbt-postgres, this should be {}
    return {
        "type": "postgres",
        "threads": 4,
        "host": "localhost",
        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
        "user": os.getenv("POSTGRES_TEST_USER", "root"),
        "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
    }
