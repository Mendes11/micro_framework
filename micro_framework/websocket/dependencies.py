from micro_framework.dependencies import Dependency
from micro_framework.rpc.dependencies import RPCDependencyMixin
from micro_framework.websocket.connectors import WebSocketConnector
from micro_framework.websocket.rpc import WSRPCServiceProxy


class WebSocketRPCClient(RPCDependencyMixin, Dependency):
    connector_class = WebSocketConnector
    proxy_class = WSRPCServiceProxy

    def __init__(self, address, port, timeout=None):
        self.address = address
        self.port = port
        self.timeout = timeout

    def get_connector_kwargs(self):
        return {
            'uri': f"ws://{self.address}:{self.port}", 'timeout': self.timeout
        }
