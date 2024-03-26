from dataclasses import dataclass
import os
from typing import Iterable, List

from dbt_common.clients import jinja
from dbt_common.clients.system import find_matching
from dbt_common.utils import MACRO_PREFIX
import jinja2

from dbt.parser.search import FileBlock

from _macro import Macro, MacroProtocol


@dataclass
class FilePath:
    searched_path: str
    relative_path: str
    modification_time: float
    project_root: str

    @property
    def search_key(self) -> str:
        # TODO: should this be project name + path relative to project root?
        return self.absolute_path

    @property
    def full_path(self) -> str:
        # useful for symlink preservation
        return os.path.join(self.project_root, self.searched_path, self.relative_path)

    @property
    def absolute_path(self) -> str:
        return os.path.abspath(self.full_path)

    @property
    def original_file_path(self) -> str:
        return os.path.join(self.searched_path, self.relative_path)


class MacroParser:

    def __init__(self, project: Project, manifest: Manifest) -> None:
        self.project_root = project.project_root
        self.project_name = project.project_name
        self.project_macro_paths = project.macro_paths
        self.manifest = manifest

    def get_paths(self) -> List[FilePath]:
        return [
            FilePath(
                searched_path=result["searched_path"],
                relative_path=result["relative_path"],
                modification_time=result["modification_time"],
                project_root=self.project_root,
            )
            for result in find_matching(self.project_root, self.project_macro_paths, "[!.#~]*.sql")
        ]

    def parse_file(self, block: FileBlock):
        for macro in self.parse_macros(block.file.contents):
            self.manifest.add_macro(block.file, macro)

    def parse_macros(self, raw_code: str) -> Iterable[MacroProtocol]:
        blocks: List[jinja.BlockTag] = [
            block
            for block in jinja.extract_toplevel_blocks(
                raw_code,
                allowed_blocks={"macro", "materialization", "test", "data_test"},
                collect_raw_data=False,
            )
            if isinstance(block, jinja.BlockTag)
        ]

        for block in blocks:
            ast = jinja.parse(block.full_block)

            if (
                isinstance(ast, jinja2.nodes.Template)
                and hasattr(ast, "body")
                and len(ast.body) == 1
                and isinstance(ast.body[0], jinja2.nodes.Macro)
            ):
                # If the top level node in the Template is a Macro, things look
                # good and this is much faster than traversing the full ast, as
                # in the following else clause. It's not clear if that traversal
                # is ever really needed.
                macro = ast.body[0]
            else:
                macro_nodes = list(ast.find_all(jinja2.nodes.Macro))
                macro = macro_nodes[0]

            if not macro.name.startswith(MACRO_PREFIX):
                continue

            name: str = macro.name.replace(MACRO_PREFIX, "")
            yield Macro(
                package_name=self.project_name,
                name=name,
                macro_sql=block.full_block,
            )
