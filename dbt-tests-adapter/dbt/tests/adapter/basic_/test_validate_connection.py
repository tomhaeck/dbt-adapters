from dbt.adapters.protocol import AdapterProtocol


class BaseValidateConnection:
    @staticmethod
    def debug_query(adapter: AdapterProtocol):
        yield adapter.debug_query()

    def test_validate_connection(self, adapter: AdapterProtocol):
        with adapter.connection_named("debug"):
            self.debug_query(adapter)
