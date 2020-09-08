from functools import partial

from micro_framework.entrypoints import Entrypoint
from micro_framework.exceptions import FrameworkException
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
        self.runner.event_loop.run_in_executor(
            self.runner.extension_spawner,
            func=partial(self.call_route, websocket, *args, **kwargs)
        )

    def on_finished_route(self, entry_id, worker):
        self.manager.send_to_client(
            entry_id, data=worker.result, exception=worker.exception
        )

    def on_failure(self, entry_id):
        exception = FrameworkException("RPC Server failed unexpectedly.")
        self.manager.send_to_client(
            entry_id, data=None, exception=exception
        )

    def setup(self):
        super(WebSocketEntrypoint, self).setup()
        self.manager.add_entrypoint(self)
