import websockets
from websocket import create_connection

from micro_framework.rpc import RPCClient


class WebSocketClient(RPCClient):
    """
    Implements a WebSocket Client as a context manager.

    This client is a synchronous client and using it as a context manager
    you won't have to remember closing the connection.

    Usage:

    with WebSocketClient(address, port) as client:
        client.send(message)
        message = client.recv()

    """

    def __init__(self, address, port, timeout=None):
        self.address = address
        self.port = port
        self.uri = f"ws://{address}:{port}"
        self.timeout = timeout

    def __enter__(self):
        self.connection = create_connection(self.uri, timeout=self.timeout)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def send(self, message):
        self.connection.send(message)

    def recv(self):
        return self.connection.recv()


class AsyncWebSocketClient(websockets.connect):
    """
    Asyncio WebSocket Client

    Since it is an asyncio client, it needs to be ran inside an event-loop.

    Usage:
    async with AsyncWebSocketClient(addr, port) as client:
        await client.send(message)
        response = await client.recv(message)

    """

    def __init__(self, addr, port):
        self.uri = f"ws://{addr}:{port}"
        super(AsyncWebSocketClient, self).__init__(self.uri)
