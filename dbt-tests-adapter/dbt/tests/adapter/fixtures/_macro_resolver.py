from typing import Callable, Dict, List, Mapping, Optional

from dbt_common.clients.jinja import MacroProtocol
import pytest

from dbt.adapters.contracts.macros import MacroResolverProtocol


class MacroResolver:

    def __init__(self):
        self.macros: Mapping[str, MacroProtocol] = {}
        self.adapter_type = ""
        self._macros_by_name = {}

    @property
    def macros_by_name(self) -> Dict[str, List[MacroProtocol]]:
        if self._macros_by_name is None:
            self._macros_by_name = self._build_macros_by_name(self.macros)
        return self._macros_by_name

    @staticmethod
    def _build_macros_by_name(macros: List[MacroProtocol]) -> Dict[str, List[MacroProtocol]]:
        macros_by_name: Dict[str, List[MacroProtocol]] = {}
        for macro in macros:
            if macro.name not in macros_by_name:
                macros_by_name[macro.name] = []
            macros_by_name[macro.name].append(macro)
        return macros_by_name

    def find_macro_by_name(
        self, name: str, root_project_name: str, package: Optional[str]
    ) -> Optional[MacroProtocol]:
        """Find a macro in the graph by its name and package name, or None for
        any package. The root project name is used to determine priority:
         - locally defined macros come first
         - then imported macros
         - then macros defined in the root project
        """
        filter: Optional[Callable[[MacroCandidate], bool]] = None
        if package is not None:

            def filter(candidate: MacroCandidate) -> bool:
                return package == candidate.macro.package_name

        candidates: CandidateList = self._find_macros_by_name(
            name=name,
            root_project_name=root_project_name,
            filter=filter,
        )

        return candidates.last()

    def _find_macros_by_name(
        self,
        name: str,
        root_project_name: str,
        filter: Optional[Callable[[MacroCandidate], bool]] = None,
    ) -> CandidateList:
        """Find macros by their name."""
        # avoid an import cycle
        from dbt.adapters.factory import get_adapter_package_names

        candidates: CandidateList = CandidateList()

        macros_by_name = self.macros_by_name
        if name not in macros_by_name:
            return candidates

        packages = set(get_adapter_package_names(self.adapter_type))
        for macro in macros_by_name[name]:
            candidate = MacroCandidate(
                locality=_get_locality(macro, root_project_name, packages),
                macro=macro,
            )
            if filter is None or filter(candidate):
                candidates.append(candidate)

        return candidates


@pytest.fixture
def macro_resolver() -> MacroResolverProtocol:
    macro_resolver = MacroResolver()
    yield macro_resolver
