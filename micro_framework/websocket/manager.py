import json
import logging

import nest_asyncio

from micro_framework.rpc import RPCManagerMixin, AsyncRPCManagerMixin
from micro_framework.websocket.server import WebSocketServer

logger = logging.getLogger(__name__)


class WebSocketManager(AsyncRPCManagerMixin, WebSocketServer):
    def setup(self):
        logger.debug("Setup WebSocket Manager.")

        self.ip = self.runner.config.get('WEBSOCKET_IP', '0.0.0.0')
        self.port = self.runner.config.get("WEBSOCKET_PORT", 8765)
        self.event_loop = self.runner.event_loop

        # Patching event_loop to allow the call in send_to_client from the
        # executor that returned.
        nest_asyncio.apply(self.event_loop)

    def start(self):
        logger.debug("Starting WebSocket Manager.")
        self.start_server(self.event_loop, self.ip, self.port)
        logger.info(f"Started WebSocketManager at {self.ip}:{self.port}")

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
        message = self.format_rpc_response(data, exception)
        # Re-enter the event loop to send the response to the client.
        self.event_loop.run_until_complete(self.send(websocket, message))
