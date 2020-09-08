import json
import logging

import websockets

logger = logging.getLogger(__name__)


class WebSocketServer:
    """
    Asyncio WebSocket Server.

    It is started by calling the start_server method and passing a running
    event_loop.

    """

    def __init__(self):
        self.clients = set()

    async def register_client(self, websocket):
        logger.debug(f"Client {websocket} connected.")
        self.clients.add(websocket)

    async def unregister_client(self, websocket):
        logger.debug(f"Client {websocket} disconnected.")
        self.clients.remove(websocket)

    async def listener(self, websocket, path):
        logger.debug(f"Received new Client: {websocket}.")
        await self.register_client(websocket)

        try:
            async for message in websocket:
                logger.debug(f"New message from {websocket}: {message}")
                await self.message_received(websocket, message)

        finally:
            await self.unregister_client(websocket)

    async def send(self, websocket, message):
        await websocket.send(message)

    async def message_received(self, websocket, message):
        pass

    def start_server(self, event_loop, ip, port):
        """
        Starts the WebSocket Server inside the event_loop.
        :param event_loop:
        :param str ip: IP address to bind the server at.
        :param port: Port to bind the server at.
        """
        self.server = websockets.serve(self.listener, ip, port)
        event_loop.run_until_complete(self.server)
        logger.info(f"WebSocketManager listening at {self.ip}:{self.port}")
