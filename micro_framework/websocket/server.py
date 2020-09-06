import json
import logging

import websockets

logger = logging.getLogger(__name__)


class WebSocketServer:
    def __init__(self):
        self.clients = set()

    async def register_client(self, websocket):
        self.clients.add(websocket)

    async def unregister_client(self, websocket):
        logger.info(f"Client {websocket} disconnected.")
        self.clients.remove(websocket)

    async def listener(self, websocket, path):
        logger.info(f"Received new Client: {websocket}.")
        await self.register_client(websocket)

        try:
            await websocket.send(json.dumps({'status': 'connected'}))

            async for message in websocket:
                await self.message_received(websocket, message)

        finally:
            await self.unregister_client(websocket)

    async def send(self, websocket, message):
        await websocket.send(message)

    async def message_received(self, websocket, message):
        pass

    def start_server(self, event_loop, ip, port):
        self.server = websockets.serve(self.listener, ip, port)
        event_loop.run_until_complete(self.server)
