from micro_framework.rpc.dependencies import RPCDependency
from micro_framework.websocket.connectors import WebSocketConnector
from micro_framework.websocket.rpc import WSRPCProxy


class WebSocketRPCClient(RPCDependency):
    connector_class = WebSocketConnector
    proxy_class = WSRPCProxy

    def __init__(self, address, port, timeout=None):
        self.address = address
        self.port = port
        self.timeout = timeout

    def get_connector_kwargs(self):
        return {
            'uri': f"ws://{self.address}:{self.port}", 'timeout': self.timeout
        }
