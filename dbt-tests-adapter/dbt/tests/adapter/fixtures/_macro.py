from dataclasses import dataclass

from dbt_common.clients.jinja import MacroProtocol as _MacroProtocol


# TODO: move into dbt_common or dbt_adapters
class MacroProtocol(_MacroProtocol):
    package_name: str


@dataclass
class Macro:  # implements the above augmented MacroProtocol
    package_name: str
    name: str
    macro_sql: str
