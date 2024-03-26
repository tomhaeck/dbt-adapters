from typing import Any, Dict, Optional

from dbt_common.clients.jinja import MacroProtocol


from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.context.providers import MacroContext, OperationProvider
from dbt.parser.manifest import ManifestLoader


class MacroResolver(ManifestLoader):
    @classmethod
    def load_macros(cls, config, set_query_header, *args, **kwargs):
        return super().load_macros(config, set_query_header, base_macros_only=True)


def generate_runtime_macro_context(
    macro: MacroProtocol,
    config: RuntimeConfig,
    manifest: Manifest,
    package_name: Optional[str],
) -> Dict[str, Any]:
    ctx = MacroContext(macro, config, manifest, OperationProvider(), package_name)
    return ctx.to_dict()
