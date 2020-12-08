import asyncio
import logging

from micro_framework.rpc import AsyncRPCManagerMixin, format_rpc_response
from micro_framework.websocket.metrics import active_connections
from micro_framework.websocket.server import WebSocketServer

logger = logging.getLogger(__name__)


class WebSocketManager(AsyncRPCManagerMixin, WebSocketServer):
    """
    AsyncRPCManager implementation using WebSockets.

    It starts a WebSocketServer that listens to connections and handles
    messages using the format expected by RPCManagerMixin.

    """
    def __init__(self):
        super(WebSocketManager, self).__init__()
        self.started = False

    def setup(self):
        logger.debug("Setup WebSocket Manager.")

        self.ip = self.runner.config['WEBSOCKET_IP']
        self.port = self.runner.config["WEBSOCKET_PORT"]

    def start(self):
        if not self.started:
            logger.debug("Starting WebSocket Manager.")
            self.runner.spawn_async_extension(
                self, self.serve(self.ip, self.port)
            )
            self.started = True

    def stop(self):
        if self.started:
            self.runner.event_loop.call_soon_threadsafe(self.server.close())

    async def register_client(self, websocket):
        active_connections.inc()
        await super(WebSocketManager, self).register_client(websocket)
    
    async def unregister_client(self, websocket):
        active_connections.dec()
        await super(WebSocketManager, self).unregister_client(websocket)
    
    async def message_received(self, websocket, message):
        response = await self.consume_message(websocket, message)
        if response:
            await self.send(websocket, response)

    async def call_entrypoint(self, websocket, entrypoint, *args, **kwargs):
        await entrypoint.handle_message(websocket, *args, **kwargs)

    def send_to_client(self, websocket, data, exception=None):
        """
        Send back to the client outside the event-loop.
        This method is intended to be used by the entrypoint when called
        after a worker has finished.

        :param websocket: Client Connection
        :param message: Response
        :return:
        """
        message = format_rpc_response(data, exception)
        # Re-enter the event loop to send the response to the client.
        task = asyncio.run_coroutine_threadsafe(
            self.send(websocket, message), loop=self.runner.event_loop
        )
        task.result() # Wait it to send
