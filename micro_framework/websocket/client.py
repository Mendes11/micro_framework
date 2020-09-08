import websockets
from websocket import create_connection

from micro_framework.rpc import RPCClient, RPCConnection


class WebSocketConnection(RPCConnection):
    def __init__(self, uri, timeout=None):
        self.uri = uri
        self.timeout = timeout

    def __enter__(self):
        self.connection = create_connection(self.uri, timeout=self.timeout)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
