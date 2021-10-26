from micro_framework.rpc import RPCServiceProvider
from micro_framework.websocket.connectors import WebSocketConnector
from micro_framework.websocket.rpc import WSRPCProxy


class WebSocketRPCClient(RPCServiceProvider):
    connector_class = WebSocketConnector
    proxy_class = WSRPCProxy

    def __init__(self, address, port, timeout=None):
        self.address = address
        self.port = port
        self.timeout = timeout

        service_name = f"{address}:{port}"
        super(WebSocketRPCClient, self).__init__(service_name)

    def get_connector_kwargs(self):
        return {
            'uri': f"ws://{self.address}:{self.port}", 'timeout': self.timeout
        }
