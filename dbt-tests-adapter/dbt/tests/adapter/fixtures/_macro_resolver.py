from collections import defaultdict
from copy import deepcopy
from typing import Any, Dict, List, Optional, Set

import pytest

from dbt.adapters.contracts.macros import MacroResolverProtocol
from dbt.adapters.factory import get_adapter_package_names

from _macro import MacroProtocol


class MacroResolver:

    def __init__(
            self, macros: List[MacroProtocol], adapter_type: str, profile: Dict[str, Any]
    ) -> None:
        self.macros = macros
        self.adapter_type = adapter_type
        self.profile = profile
        self.macros_by_name: Dict[str, Set[MacroProtocol]] = defaultdict(set)
        for macro in self.macros:
            self.macros_by_name[macro.name].add(macro)
        self.macros_by_package: Dict[str, Dict[str, MacroProtocol]] = defaultdict(dict)

    def add_macro(self, source_file: SourceFile, macro: Macro):
        self.macros[macro.unique_id] = macro
        self.macros_by_name[macro.name].add(macro)
        self.macros_by_package[macro.package_name][macro.name] = macro
        source_file.macros.append(macro.unique_id)

    def find_macro_by_name(
        self, name: str, root_project_name: str, package: Optional[str]
    ) -> Optional[MacroProtocol]:
        all_macros = {
            macro for macro in self.macros_by_name.get(name, None)
            if macro.package_name == package or package is None
        }
        internal_packages = get_adapter_package_names(self.adapter_type)

        if project_macros := {macro for macro in all_macros if macro.package_name == root_project_name}:
            return project_macros.pop()
        elif internal_macros := {macro for macro in all_macros if macro.package_name in internal_packages}:
            return internal_macros.pop()
        elif all_macros:
            return all_macros.pop()
        return None

    def _macros(self) -> List[MacroProtocol]:
        @classmethod
        def load_macros(
                cls,
                root_config: RuntimeConfig,
                macro_hook: Callable[[Manifest], Any],
                base_macros_only=False,
        ) -> Manifest:
            with PARSING_STATE:
                # base_only/base_macros_only: for testing only,
                # allows loading macros without running 'dbt deps' first
                projects = root_config.load_dependencies(base_only=True)
                macro_manifest = self.create_macro_manifest(root_config, projects.values(), macro_hook)

            return macro_manifest
    def create_macro_manifest(self, root_config, projects, macro_hook):
        for project in projects:
            # what is the manifest passed in actually used for?
            macro_parser = MacroParser(project, self)
            for path in macro_parser.get_paths():
                source_file = load_source_file(path, ParseFileType.Macro, project.project_name, {})
                block = FileBlock(source_file)
                macro_parser.parse_file(block)
        macro_manifest = MacroManifest(self.manifest.macros)
        return macro_manifest

    def _load_dependencies(self) -> Mapping[str, "RuntimeConfig"]:
        if self.dependencies is None:
            all_projects = {self.project_name: self}
            internal_packages = get_include_paths(self.credentials.type)
            project_paths = itertools.chain(internal_packages)
            for project_name, project in self.load_projects(project_paths):
                all_projects[project_name] = project
            self.dependencies = all_projects
        return self.dependencies

    def load_projects(self, paths: Iterable[Path]) -> Iterator[Tuple[str, "RuntimeConfig"]]:
        for path in paths:
            project = self.new_project(str(path))
            yield project.project_name, project

    def new_project(self, project_root: str) -> "RuntimeConfig":
        # copy profile
        profile = Profile(**self.profile)

        # load the new project and its packages. Don't pass cli variables.
        renderer = DbtProjectYamlRenderer(profile)
        project = Project.from_project_root(
            project_root,
            renderer,
            verify_version=bool(getattr(self.args, "VERSION_CHECK", True)),
        )

        runtime_config = self.from_parts(
            project=project,
            profile=profile,
            args=deepcopy(self.args),
        )
        # force our quoting back onto the new project.
        runtime_config.quoting = deepcopy(self.quoting)
        return runtime_config


@pytest.fixture
def macro_resolver() -> MacroResolverProtocol:
    macro_resolver = MacroResolver()
    yield macro_resolver
