from micro_framework.rpc import RPCDependency
from micro_framework.websocket.client import WebSocketConnection


class WebSocketRPCClient(RPCDependency):
    connection_class = WebSocketConnection

    def __init__(self, address, port, timeout=None):
        self.address = address
        self.port = port
        self.timeout = timeout

    def get_connection_kwargs(self):
        return {
            'uri': f"ws://{self.address}:{self.port}", 'timeout': self.timeout
        }
