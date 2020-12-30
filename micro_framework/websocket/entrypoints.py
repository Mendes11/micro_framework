from micro_framework.entrypoints import Entrypoint
from micro_framework.exceptions import FrameworkException, MaxConnectionsReached
from micro_framework.websocket.manager import WebSocketManager


class WebSocketEntrypoint(Entrypoint):
    manager = WebSocketManager()

    async def handle_message(self, websocket, *args, **kwargs):
        """
        Called by the manager to handle the received message from a connected
        client.

        :param websocket:
        :param message:
        :return:
        """
        if self.runner.available_workers <= 0:
            raise MaxConnectionsReached("No Available Workers")
        await self.call_route(websocket, *args, **kwargs)

    async def on_finished_route(self, entry_id, worker):
        await self.manager.send_to_client(
            entry_id, data=worker.result, exception=worker.exception
        )

    async def on_failure(self, entry_id):
        exception = FrameworkException("RPC Server failed unexpectedly.")
        await self.manager.send_to_client(
            entry_id, data=None, exception=exception
        )

    async def setup(self):
        await super(WebSocketEntrypoint, self).setup()
        await self.manager.add_entrypoint(self)
