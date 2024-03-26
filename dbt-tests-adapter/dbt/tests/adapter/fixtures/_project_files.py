from typing import Any, Dict, Union
import yaml

from _pytest.compat import LEGACY_PATH
import pytest

import _defaults
from services import write_file


DictConfig = Dict[str, Any]
YMALConfig = str
TestConfig = Union[DictConfig, YMALConfig]


@pytest.fixture
def project_root(tmpdir_factory) -> LEGACY_PATH:
    # tmpdir docs - https://docs.pytest.org/en/6.2.x/tmpdir.html
    project_root = tmpdir_factory.mktemp("project")
    print(f"\n=== Test project_root: {project_root}")
    yield project_root


@pytest.fixture
def project_files(
        project_root: LEGACY_PATH,
        dbt_project_yml: DictConfig,
        models: TestConfig,
        macros: TestConfig,
        snapshots: TestConfig,
        properties: TestConfig,
        seeds: TestConfig,
        tests: TestConfig,
        analyses: TestConfig,
        dependencies: TestConfig,
        packages: TestConfig,
        selectors: TestConfig,
) -> None:
    _write_project_files(project_root, "models", {**models, **properties})
    _write_project_files(project_root, "macros", macros)
    _write_project_files(project_root, "snapshots", snapshots)
    _write_project_files(project_root, "seeds", seeds)
    _write_project_files(project_root, "tests", tests)
    _write_project_files(project_root, "analyses", analyses)
    _write_project_files(project_root, project_root, {
        "dbt_project.yml": dbt_project_yml,
        "dependencies.yml": dependencies,
        "packages.yml": packages,
        "selectors.yml": selectors,
    })


def _write_project_files(
        project_root: LEGACY_PATH,
        dir_name: Union[str, LEGACY_PATH],
        file_dict: Dict[str, Union[TestConfig, Dict]],
) -> None:
    if dir_name != project_root:
        path = project_root.mkdir(dir_name)
    else:
        path = project_root
    if file_dict:
        _write_project_files_recursively(path, file_dict)


def _write_project_files_recursively(
        path: LEGACY_PATH, file_dict: Dict[str, Union[TestConfig, Dict]]
) -> None:
    for name, value in file_dict.items():
        if name.endswith(".yml") or name.endswith(".yaml"):
            if isinstance(value, str):
                data = value
            else:
                data = yaml.safe_dump(value)
            _utils.write_file(data, path, name)
        elif name.endswith((".sql", ".csv", ".md", ".txt", ".py")):
            write_file(value, path, name)
        else:
            _write_project_files_recursively(path.mkdir(name), value)


@pytest.fixture
def dbt_project_yml(project_root: LEGACY_PATH, project_config_update: TestConfig) -> DictConfig:
    """
    Combine `project_config_update` with `project_config` defaults.
    """
    project_config = {
        "name": _defaults.PROJECT_NAME,
        "profile": _defaults.PROJECT_PROFILE,
        "flags": {"send_anonymous_usage_stats": False},
    }
    if project_config_update:
        if isinstance(project_config_update, dict):
            project_config.update(project_config_update)
        elif isinstance(project_config_update, str):
            updates = yaml.safe_load(project_config_update)
            project_config.update(updates)
    return project_config


@pytest.fixture
def project_config_update() -> TestConfig:
    """
    Data used to update the `dbt_project_yml` config data.
    """
    return {}


@pytest.fixture
def models() -> TestConfig:
    return {}


@pytest.fixture
def macros() -> TestConfig:
    return {}


@pytest.fixture
def properties() -> TestConfig:
    return {}


@pytest.fixture
def seeds() -> TestConfig:
    return {}


@pytest.fixture
def snapshots() -> TestConfig:
    return {}


@pytest.fixture
def tests() -> TestConfig:
    return {}


@pytest.fixture
def analyses() -> TestConfig:
    return {}


@pytest.fixture
def dependencies() -> TestConfig:
    return {}


@pytest.fixture
def packages() -> TestConfig:
    return {}


@pytest.fixture
def selectors() -> TestConfig:
    return {}


@pytest.fixture
def test_config() -> TestConfig:
    """
    This fixture is for customizing tests that need overrides in adapter repos.
    For an example, refer to `dbt.tests.adapter.basic.test_base`.
    """
    return {}
