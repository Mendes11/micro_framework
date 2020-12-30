import websockets
from websocket import create_connection, WebSocket

from micro_framework.exceptions import FrameworkException
from micro_framework.rpc import RPCClient, RPCConnection

class ServerDisconnected(FrameworkException):
    pass


class WSConnection(WebSocket):
    def recv(self):
        res = super(WSConnection, self).recv()
        if not res and not self.connected:
            raise ServerDisconnected(
                f"Server has unexpectedly disconnected"
            )
        return res

    def send(self, payload, *args, **kwargs):
        length = super(WSConnection, self).send(payload, *args, **kwargs)
        if not length and not self.connected:
            raise ServerDisconnected(
                f"Server has unexpectedly disconnected"
            )
        return length


class WebSocketConnection(RPCConnection):
    def __init__(self, uri, timeout=None):
        self.uri = uri
        self.timeout = timeout

    def __enter__(self):
        self.connection = create_connection(
            self.uri, timeout=self.timeout, class_=WSConnection
        )
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
