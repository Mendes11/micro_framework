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
    def setup(self):
        logger.debug("Setup WebSocket Manager.")

        self.ip = self.runner.config['WEBSOCKET_IP']
        self.port = self.runner.config["WEBSOCKET_PORT"]

    def start(self):
        logger.debug("Starting WebSocket Manager.")
        self.event_loop = self.runner.event_loop
        self.start_server(self.event_loop, self.ip, self.port)
    
    async def register_client(self, websocket):
        active_connections.inc()
        return super(WebSocketManager, self).register_client(websocket)
    
    async def unregister_client(self, websocket):
        active_connections.dec()
        return super(WebSocketManager, self).unregister_client(websocket)
    
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
        asyncio.run_coroutine_threadsafe(
            self.send(websocket, message), self.event_loop
        )
