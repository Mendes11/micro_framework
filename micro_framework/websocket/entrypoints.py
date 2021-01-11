from micro_framework.entrypoints import Entrypoint
from micro_framework.exceptions import MaxConnectionsReached
from micro_framework.websocket.manager import WebSocketManager


class WebSocketEntrypoint(Entrypoint):
    manager = WebSocketManager()

    async def call_route(self, entry_id, *args, _meta=None, **kwargs):
        if self.runner.available_workers <= 0:
            raise MaxConnectionsReached("No Available Workers")
        return await super(WebSocketEntrypoint, self).call_route(
            entry_id, *args, _meta=_meta, **kwargs
        )

    async def setup(self):
        await super(WebSocketEntrypoint, self).setup()
        await self.manager.add_entrypoint(self)
