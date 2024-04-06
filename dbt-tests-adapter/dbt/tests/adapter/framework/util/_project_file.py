import json
import os
import shutil
from typing import Any, Dict, List, Optional, Union
import yaml

from dbt.adapters.protocol import RelationProtocol


# TODO: remove dependency on dbt-core
from dbt.contracts.graph.manifest import Manifest


def read_file(*paths: str) -> str:
    with open(os.path.join(*paths), "r") as fp:
        contents = fp.read()
    return contents


def write_file(contents: str, *paths: str) -> None:
    """
    Used in tests to write out the string contents of a file
    to a file in the project directory. We need to explicitly
    use encoding="utf-8" because otherwise on Windows
    we'll get codepage 1252 and things might break

    Args:
        contents: the file contents
        *paths: the path, as an iterable of strings
    """
    with open(os.path.join(*paths), "w", encoding="utf-8") as fp:
        fp.write(contents)


def copy_file(src_path: str, src: str, dest_path: str, dest: List[str]) -> None:
    """
    Copy files from `data_dir` to the project directory.

    Args:
        src_path: source root
        src: source file name
        dest_path: destination root
        dest: a list, so that it can take nested directories, like `models` etc.
    """
    shutil.copyfile(
        os.path.join(src_path, src),
        os.path.join(dest_path, *dest),
    )


def rm_file(*paths: str) -> None:
    """
    Remove a file from the project directory.

    Args:
        *paths: the path, as an iterable of strings
    """
    os.remove(os.path.join(*paths))


def file_exists(*paths: str) -> bool:
    """
    Checks if the file exists

    Args:
        *paths: the path, as an iterable of strings
    """
    return os.path.exists(os.path.join(*paths))


def mkdir(directory_path: str) -> None:
    try:
        os.makedirs(directory_path)
    except FileExistsError:
        raise FileExistsError(f"{directory_path} already exists.")


def rm_dir(directory_path: str) -> None:
    try:
        shutil.rmtree(directory_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"{directory_path} does not exist.")


def rename_dir(src_directory_path: str, dest_directory_path: str) -> None:
    os.rename(src_directory_path, dest_directory_path)


def get_manifest(project_root: str) -> Optional[Manifest]:
    """
    Get the manifest from the partial parsing file.

    Note: This uses an internal version of the manifest.

    Args:
        project_root:

    Returns: the manifest from the partial parsing file
    """
    path = os.path.join(project_root, "target", "partial_parse.msgpack")
    if os.path.exists(path):
        with open(path, "rb") as fp:
            manifest_mp = fp.read()
        return Manifest.from_msgpack(manifest_mp)
    return None


def get_run_results(project_root: str) -> Optional[Any]:
    """
    Get the contents of the `run_results.json` file.

    Args:
        project_root:

    Returns: the `run_results.json` file
    """
    path = os.path.join(project_root, "target", "run_results.json")
    if os.path.exists(path):
        with open(path) as run_result_text:
            return json.load(run_result_text)
    return None


def get_artifact(*paths: str) -> Any:
    """
    Get an artifact (usually from the target directory) such as
    `manifest.json` or `catalog.json`.

    Args:
        *paths: the path to the file as a tuple

    Returns: the contents of the file
    """
    return json.loads(read_file(*paths))


def write_artifact(dct: Any, *paths: str) -> None:
    """
    Write to an artifact (usually in the target directory) such as
    `manifest.json` or `catalog.json`.

    Args:
        dct: the contents to write to the file
        *paths: the path to the file as a tuple
    """
    write_file(json.dumps(dct), *paths)


def update_config_file(updates: Dict[Any, Any], *paths: str) -> None:
    current_yaml = read_file(*paths)
    config = yaml.safe_load(current_yaml)
    config.update(updates)
    new_yaml = yaml.safe_dump(config)
    write_file(new_yaml, *paths)


def write_config_file(data: Union[Dict[Any, Any], str], *paths: str) -> None:
    if isinstance(data, dict):
        data = yaml.safe_dump(data)
    write_file(data, *paths)


def get_project_config(project) -> Any:
    file_yaml = read_file(project.project_root, "dbt_project.yml")
    return yaml.safe_load(file_yaml)


def set_project_config(project, config: Union[Dict[Any, Any]]):
    config_yaml = yaml.safe_dump(config)
    write_file(config_yaml, project.project_root, "dbt_project.yml")


def get_model_file(project, relation: RelationProtocol) -> str:
    return read_file(project.project_root, "models", f"{relation.name}.sql")


def set_model_file(
        project, relation: RelationProtocol, model_sql: str
) -> None:
    write_file(model_sql, project.project_root, "models", f"{relation.name}.sql")
