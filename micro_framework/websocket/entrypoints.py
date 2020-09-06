import inspect
from functools import partial

from micro_framework.entrypoints import Entrypoint
from micro_framework.websocket.manager import WebSocketManager


class BaseWebSocketEntrypoint(Entrypoint):
    manager = WebSocketManager()

    async def handle_message(self, websocket, *args, **kwargs):
        """
        Called by the manager to handle the received message from a connected
        client.

        :param websocket:
        :param message:
        :return:
        """
        func = partial(self.call_route, websocket, *args, **kwargs)
        self.runner.event_loop.run_in_executor(
            self.runner.extension_spawner, func
        )

    def on_finished_route(self, entry_id, worker):
        # TODO Handle exception
        message = {'result': worker.result}
        self.manager.send_to_client(entry_id, message)

    def setup(self):
        super(BaseWebSocketEntrypoint, self).setup()
        self.manager.add_entrypoint(self)
